######################################################
#
# Powershell Runbook Orchestrator (rev1.91)
#
# Velostrata Ltd.
#
######################################################

[CmdletBinding()]
param (
	[Parameter(Mandatory=$True,Position=1,ParameterSetName='DR')]	
    [switch]$DR,

	[Parameter(Mandatory=$False,ParameterSetName='StartMigration')]
    [Parameter(Mandatory=$False,ParameterSetName='RunInCloud')]
    [Parameter(Mandatory=$False,ParameterSetName='DR')]	
    [switch]$WriteIsolation,
    
    [Parameter(Mandatory=$True,Position=1,ParameterSetName='RunInCloud')]	
    [switch]$RunInCloud,
    
    [Parameter(Mandatory=$True,Position=1,ParameterSetName='StartMigration')]	
    [switch]$StartMigration,
    
    [Parameter(Mandatory=$True,Position=1,ParameterSetName='CreateLinkedClone')]	
    [switch]$CreateLinkedClone,

    [Parameter(Mandatory=$True,Position=1,ParameterSetName='DeleteLinkedClone')]	
    [switch]$DeleteLinkedClone,

    [Parameter(Mandatory=$True,Position=1,ParameterSetName='PrepareToDetach')]	
    [switch]$PrepareToDetach,

    [Parameter(Mandatory=$True,Position=1,ParameterSetName='Detach')]	
    [switch]$Detach,

    [Parameter(Mandatory=$True,Position=1,ParameterSetName='RunOnPrem')]	
    [switch]$RunOnPrem,

    [Parameter(Mandatory=$True,ParameterSetName='CreateLinkedClone')]	
    [Parameter(Mandatory=$True,ParameterSetName='DeleteLinkedClone')]	
    [string]$Prefix,
	
    [Parameter(Mandatory=$True,ParameterSetName='DR')]	
    [Parameter(Mandatory=$True,ParameterSetName='StartMigration')]
    [Parameter(Mandatory=$True,ParameterSetName='PrepareToDetach')]	
    [Parameter(Mandatory=$True,ParameterSetName='Detach')]
    [Parameter(Mandatory=$True,ParameterSetName='RunOnPrem')]
    [Parameter(Mandatory=$True,ParameterSetName='RunInCloud')]	
    [Parameter(Mandatory=$True,ParameterSetName='CreateLinkedClone')]	
    [string]$CSVFile,
 
    [Parameter(Mandatory=$True,ParameterSetName='DR')]	
    [Parameter(Mandatory=$True,ParameterSetName='StartMigration')]	
    [Parameter(Mandatory=$True,ParameterSetName='PrepareToDetach')]
    [Parameter(Mandatory=$True,ParameterSetName='Detach')]
    [Parameter(Mandatory=$True,ParameterSetName='RunOnPrem')]
    [Parameter(Mandatory=$True,ParameterSetName='RunInCloud')]	
    [Parameter(Mandatory=$True,ParameterSetName='CreateLinkedClone')]	
    [Parameter(Mandatory=$True,ParameterSetName='DeleteLinkedClone')]
    [string]$CredsXML,
    
    [Parameter(Mandatory=$True,Position=1,ParameterSetName='SaveCreds')]
    [switch]$SaveCreds,
    [Parameter(Mandatory=$True,ParameterSetName='SaveCreds')]
    [string]$vCenterServer,
	[Parameter(Mandatory=$True,ParameterSetName='SaveCreds')]	
    [string]$vCenterUser,
	[Parameter(Mandatory=$True,ParameterSetName='SaveCreds')]	
    [securestring]$vCenterPassword,
    [Parameter(Mandatory=$True,ParameterSetName='SaveCreds')]	
    [string]$VelostrataManager, 
    [Parameter(Mandatory=$True,ParameterSetName='SaveCreds')]	
    [string]$VelostrataSubscriptionId

 )
 
Import-Module VMware.VimAutomation.Core
Import-Module Velostrata.PowerShell.VMware

set-PowerCLIConfiguration -invalidCertificateAction "ignore" -confirm:$false | Out-Null
$Host.PrivateData.VerboseForegroundColor="Gray"

# Parameters --------------------------------------------------------------------------------
$outputPath = (Split-Path $MyInvocation.MyCommand.Path)
$maxParallelJobsPerCE = 2

# VC top logical folder to use
if($DR) { 
    $targetFolder = "Velos-DR" 
} elseif (($CreateLinkedClone) -or ($DeleteLinkedClone)) {
    $targetFolder = "Velos-Clone"
} else {
    $targetFolder = ""
}

# Job ScriptBlock ---------------------------------------------------------------------------
$run_one = {
######################################################
# Move a single VM to cloud 
######################################################

param (
    [string]$velosMgr,
    [securestring]$mgrPwd,
    [string]$vmName,
    [string]$vmId,
    [string]$dcId,
    [string]$ceName,
    [string]$instanceType,
    [string]$edgeNode,
    [string]$subnetId,
    [string]$securityGroupId,
    [string]$staticIp,
    [string]$resourceGroupId,
    [int]$port,
    [int]$minutes,
    [bool]$isWriteIsolation
 )


Import-Module 'Velostrata.PowerShell.VMware'


function Log ($text) {
    $VerbosePreference = ‘continue’
    $stamp = (Get-Date).ToString("HH:mm:ss.fff")
    Write-Verbose "$stamp | Job: $vmName | $text"
    $VerbosePreference = ‘SilentlyContinue’
}

function TestPort($address, $testPort)
{
    $test = New-Object System.Net.Sockets.TcpClient;
    Try
    {
        Log ( "Connecting to " + $address + ":" + $testPort)
        $test.Connect($address, $testPort)
        return $true
    }
    Catch
    {
        $Error.Clear()
        return $false
    }
    Finally
    {
        $test.Dispose()
    }
}

function CheckVmStopped($vmId)
{
    $vm = Get-VelosVm $vmId
    return (($vm) -and ($vm.PowerState -eq "Stopped"))
}

function WaitForPort($vm, $port, $minutes)
{
    $address = $vm.CloudInfo.privateIpAddress
    $timeout = (Get-Date).AddMinutes($minutes)
    $rebootRetries = 3
    Log("Starting probe")
    while ($timeout -gt (Get-Date)) {
        $rc = TestPort $address $Port

        if ($rc) {
            Log ( "Port check passed")
            return
        } else {
            Log ( "Port check failed - will retry")
            if(($rebootRetries -gt 0 ) -and (CheckVmStopped $vm.Id)) {
                Log ( "Rebooting cloud instance")
                Start-VelosVm -Id $vm.Id | Out-Null
                $rebootRetries -= 1     
            }
            Start-Sleep -s 30
        }
    }
    Write-Error "Timeout waiting for port"
}

function CheckSuccess($stage)
{
    if ($Error.Count -gt 0)
    {
        Log( $stage+" error: " + $Error[0])
        #set job result to failed
        write-output $false
        log("Stage: "+ $stage + " Failed!")
        exit
    } 
}


Log ( "Started")
Connect-VelostrataManager $velosMgr -Username 'apiuser' -Password $mgrPwd
CheckSuccess "Connect"
Log ( "Connected to Velostrata Manager")

$ce = Get-VelosCe -DatacenterId $dcId -Name $ceName
CheckSuccess "Get CE"

# select storage policy
$storagePolicy = if($isWriteIsolation) { "WriteIsolation" } else { "WriteBack" } 
Log("Using storage policy: " + $storagePolicy)

# Check if already in cloud but stopped, then start otherwise move to cloud
if(CheckVmStopped($vmId)){
  Log("VM stopped in cloud. Starting it.")
  $cloudVM = Start-VelosVm -Id $vmId
  CheckSuccess "Start VM in cloud"
} else {
	# run-in-cloud 
	Log("Moving to Cloud")
	if($ce.CloudProvider -eq "Aws"){
        $cloudVM = Move-VelosVm -Id $vmId -Destination Cloud -CloudExtension $ce -StoragePolicy $storagePolicy -InstanceType $instanceType -EdgeNode $edgeNode -SubnetId $subnetId -SecurityGroupIds $securityGroupId -StaticAddress $staticIp
    } else {
        $cloudVM = Move-VelosVm -Id $vmId -Destination Cloud -CloudExtension $ce -StoragePolicy $storagePolicy -InstanceType $instanceType -EdgeNode $edgeNode -SubnetId $subnetId -SecurityGroupIds $securityGroupId -StaticAddress $staticIp -ResourceGroupId $resourceGroupId
    }
	CheckSuccess "Move to cloud"
}

# probe VM 
if ($port) {
    WaitForPort $cloudVM $port $minutes
    CheckSuccess "Reachability"
}

# job finished
Log("Completed successfully")
write-output $true

}
# END Job ScriptBlock-------------------------------------------------------------------------------
$Return_One_From_Cloud = {

######################################################
# Return a single VM to Premises 
######################################################

param (
	[string]$vc,
	[string]$vcUser,
	[securestring]$vcPwd,
	[string]$velosMgr,
	[securestring]$mgrPwd,
    [string]$vmId,
	[int]$port,
    [int]$minutes,
	[string]$datacenter,
	[string]$vcFolder
 )

 Import-Module 'Velostrata.PowerShell.VMware'
 Add-PSSnapin "VMware.VimAutomation.Core" | Out-Null
 function CheckSuccess($stage)
{
    if ($Error.Count -gt 0)
    {
        Log( $stage+" error: " + $Error[0])
        #set job result to failed
        write-output $false
        log("Stage: "+ $stage + " Failed!")
        exit
    } 
}

 function TestPort($address, $testPort)
{
    $test = New-Object System.Net.Sockets.TcpClient;
    Try
    {
        Log ( "Connecting to " + $address + ":" + $testPort)
        $test.Connect($address, $testPort)
        return $true
    }
    Catch
    {
        $Error.Clear()
        return $false
    }
    Finally
    {
        $test.Dispose()
    }
}

function WaitForPort($vm, $port, $minutes)
{
    $address = $vm | select @{N="IP Address";E={@($_.guest.IPAddress[0])}}
	$address = $address.'IP Address'
    $timeout = (Get-Date).AddMinutes($minutes)
    $rebootRetries = 3
    Log("VM: $vm, Starting port probe")
    while ($timeout -gt (Get-Date)) {
        $rc = TestPort $address $Port
		
        if ($rc) {
            Log ( "VM: $vm, Port check passed")
            return
        } else {
             Log ( "Port check failed - will retry")
            Start-Sleep -s 30
        }
		
    }
    Write-Error "Timeout waiting for port"
}

function Log ($text) {
    $VerbosePreference = ‘continue’
    $stamp = (Get-Date).ToString("HH:mm:ss.fff")
    Write-Verbose "$stamp | Job: $vmName | $text"
    $VerbosePreference = ‘SilentlyContinue’
}

function GetAddress($vm) {
	$address = $vm | select @{N="IP Address";E={@($_.guest.IPAddress[0])}}
	$address = $address.'IP Address'
	return $address
}

function WaitForAddress($vm) { 
	$timeout = (Get-Date).AddMinutes(15)
    Log("VM: $vm Starting ip address probe ")
    while (!(GetAddress($vm)) -and $timeout -gt (Get-Date)) {
		Start-Sleep -Seconds 5
		$vm = Get-VM -Id $vm.Id -ErrorAction SilentlyContinue
	}
	if ((Get-Date) -gt $timeout){
		Write-Error "Timeout waiting for ip address"
	}
	return $vm
}

Function Start_vSphereVM($vm){
		try {
			 if (($vm) -and ($vm.PowerState -eq "PoweredOff")) {
				Log("Starting VM: "+$vm.Name)
				$vm = $vm | Start-VM -ErrorAction SilentlyContinue
			} elseif(!$vm) {
					Log("VM: "+$vm.name+" was not found in vCenter.")
			} else {
					Log("Skipping start for VM: "+$vm.name+" with PowerState: "+$vm.PowerState)
			}
		} catch {
			Log("Could not start VM: "+$vm.name + ", reason: "+$_)
		}
		
		return WaitForAddress($vm)
}

Log ( "Started")
Connect-VelostrataManager $velosMgr -Username 'apiuser' -Password $mgrPwd
CheckSuccess "Connect mgmt"
Log ( "Connected to Velostrata Manager")
$vcCred = New-Object System.Management.Automation.PsCredential($vcUser, $vcPwd)
Connect-VIServer -Server $vc -Credential $vcCred -errorAction Stop
CheckSuccess "Connect vc"
Log ( "Connected to vc")

# run-on-prem 
$vm = Get-VM -Id $vmId -ErrorAction SilentlyContinue
Log("Run on prem " + $vm.name)
Move-VelosVm -Id $vmId -Destination Origin -confirm:$false -ErrorAction Stop
CheckSuccess "Run on prem"
$vm = Start_vSphereVM ($vm)
CheckSuccess "Start vm"
					
# probe VM 
if ($port -and $vm) {
    WaitForPort $vm $port $minutes
    CheckSuccess "Reachability"
}

# job finished
Log("Completed successfully")
write-output $true

}
# END Job ScriptBlock-------------------------------------------------------------------------------

# Functions ---------------------------------------------------------------------------------


function Log ($text) {
    $stamp = (Get-Date).ToString("HH:mm:ss.fff")
    $VerbosePreference = 'continue'
    Write-verbose "$stamp | $text" 
    $VerbosePreference = 'SilentlyContinue'
}

function Save_Creds(){
    if($VelostrataSubscriptionId) {
        $subscriptionId = ConvertTo-SecureString $VelostrataSubscriptionId -AsPlainText -Force
    }
    $creds = @{}
    $creds.Add('vCenterServer',$vCenterServer)
    $creds.Add('vCenterUser',$vCenterUser)
    $creds.Add('vCenterPassword',$vCenterPassword)
    $creds.Add('VelostrataManager',$VelostrataManager)
    $creds.Add('subscriptionId',$subscriptionId)

    $creds | Export-Clixml -Path ($PSScriptRoot + "\creds@$vCenterServer.xml")
}

function ReadCreds(){
    return Import-Clixml -Path $CredsXML -ErrorAction Stop
}

function JobWriteVerbose ($job) {
        if(!$job) {return}
        foreach($line in $job.ChildJobs[0].verbose.ReadAll()){      
            $VerbosePreference = 'continue'
            write-verbose $line 
            $VerbosePreference = 'SilentlyContinue'
        }
}

function count-CeTasksForType ([string]$ceId, [string]$taskType) {
		$vmsInGroup = $vmRows | % { Get-VM -name $_.VMName }
		$runningTasksIdsInCe = $vmsInGroup | % { Get-Velostask -vm $_  -ErrorAction SilentlyContinue } | ? { $_.Type -eq $taskType -and $_.State -eq "Running" }
		$runningTasksIdsInCe = $runningTasks | ? { Get-VelosVm -id $_.EntityId | ? { $_.CloudExtensionId -eq $ceId } } | % { $_.Id }
		$runningTasksIdsInCe = $runningTasksIdsInCe | ? {$_}
        return  $runningTasksIdsInCe.Length
}

function ChangeUUID ($vm){
 Log("Changing UUID for: "+ $vm)
 Log("Current InstanceUuid: " + ( $vm | get-view | % { $_.config.InstanceUuid }))
 $guid=[guid]::NewGuid()
 $spec = New-Object VMware.Vim.VirtualMachineConfigSpec
 $spec.InstanceUuid = $guid
 $vm.extensiondata.ReconfigVM($spec)
 Log("Updated InstanceUuid: " + ( $vm | get-view | % { $_.config.InstanceUuid }))
}

function ImportRunbook()
{
	Log("Reading runbook from CSV File...")
	# define table 
	$table = New-Object system.Data.DataTable "Imported runbook"
    $table.columns.add((New-Object system.Data.DataColumn RunGroup,([int])))
    $table.columns.add((New-Object system.Data.DataColumn VMName,([string])))
    $table.columns.add((New-Object system.Data.DataColumn NumCPU,([int])))
    $table.columns.add((New-Object system.Data.DataColumn MemoryMB,([int])))
    $table.columns.add((New-Object system.Data.DataColumn NumDisks,([int])))
    $table.columns.add((New-Object system.Data.DataColumn OS,([string])))
    $table.columns.add((New-Object system.Data.DataColumn VCFolder,([string])))
    $table.columns.add((New-Object system.Data.DataColumn VMXPath,([string])))
    $table.columns.add((New-Object system.Data.DataColumn TargetDatacenter,([string])))
    $table.columns.add((New-Object system.Data.DataColumn TargetESXCluster,([string])))
    $table.columns.add((New-Object system.Data.DataColumn CloudExtension,([string])))
    $table.columns.add((New-Object system.Data.DataColumn CloudEdgeNode,([string])))
    $table.columns.add((New-Object system.Data.DataColumn CloudInstanceType,([string])))
    $table.columns.add((New-Object system.Data.DataColumn CloudSubnet,([string])))
    $table.columns.add((New-Object system.Data.DataColumn CloudSecurityGroup,([string])))
    $table.columns.add((New-Object system.Data.DataColumn CloudStaticIP,([string])))
    $table.columns.add((New-Object system.Data.DataColumn CloudResourceGroupId,([string])))
    $table.columns.add((New-Object system.Data.DataColumn CloudStorageAccount,([string])))
    $table.columns.add((New-Object system.Data.DataColumn ProbeTCPPort,([int])))
    $table.columns.add((New-Object system.Data.DataColumn ProbeWaitMinutes,([int])))
    $table.columns.add((New-Object system.Data.DataColumn BlockOnFailure,([boolean])))

	try {
		$csv = import-CSV -Path $CSVFile -ErrorAction Stop
	} catch {
		Log("Error importing CSV file. Aborting")
		Exit
	}

	# Populate table row per CSV lines
	foreach ($line in $csv)
	{
		$row = $table.NewRow()
		$row.RunGroup = $line.RunGroup
		$row.VMName = $line.VMName
		$row.NumCPU = $line.NumCPU
		$row.MemoryMB = $line.MemoryMB
        $row.NumDisks = $line.NumDisks
		$row.OS = $line.OS
        $row.VCFolder = $line.VCFolder
		$row.VMXPath = $line.VMXPath
		$row.TargetDatacenter = $line.TargetDataCenter
		$row.TargetESXCluster = $line.TargetESXCluster
		$row.CloudExtension = $line.CloudExtension
		$row.CloudEdgeNode = $line.CloudEdgeNode
		$row.CloudInstanceType = $line.CloudInstanceType
		$row.CloudSubnet = $line.CloudSubnet
		$row.CloudSecurityGroup = $line.CloudSecurityGroup
		$row.CloudStaticIP = $line.CloudStaticIP
        $row.CloudResourceGroupId = $line.CloudResourceGroupId
        $row.CloudStorageAccount = $line.CloudStorageAccount
        $row.ProbeTCPPort =  $line.ProbeTCPPort
		$row.ProbeWaitMinutes = $line.ProbeWaitMinutes
        $row.BlockOnFailure = $line.BlockOnFailure
		$table.Rows.Add($row)

	}
	Log("Runbook rows found: " + $table.rows.count)
	Return $table
}
 
function getFolderFromPath ($datacenterName, $FolderPath) {
        try {
            $dcObj = get-datacenter -Name $vmRow.TargetDatacenter -ErrorAction Stop
        } catch {
          Log("[getFolderFromPath] Datacenter:" + $datacenterName + " not found!")
          Return $null
        }
             
        $parentFolder = $dcObj | get-Folder -Name "vm" -NoRecursion # top VM folder

        if (!$FolderPath) {
            # return special top vm folder
            return $parentFolder
        }

        # validate VC Folder path exists and retrieve folder Id
        foreach ($folderName in ($FolderPath -split "/")){
  
                if(!$parentFolder) {break}
                $folderObj = $parentFolder | get-Folder -Name $folderName -NoRecursion -ErrorAction SilentlyContinue
                if(!$folderObj) {
                    Log("[getFolderFromPath] Folder not found: "+ $folderName + "in path:" + $FolderPath)
                    break
                } else {
                    $parentFolder = $folderObj
                }
        } 
        return $folderObj
}


function CreateVCFolder($RootFolder, $dcObj, $vmVCFolder){
        if($RootFolder) { $FolderPath = $RootFolder + "/" + $vmVCFolder} else { $FolderPath = $vmVCFolder }
        $parentFolder = $dcObj | get-Folder -Name "vm" -NoRecursion # top VM folder
        # validate VC Folder path and create missing as needed
        Log("Validating VC Folder: " + $FolderPath +", Datacenter: "+$dcObj.Name)
        foreach ($folderName in ($FolderPath -split "/")){
  
                if(!$parentFolder -or !$folderName) {break}
                $folderObj = $parentFolder | get-Folder -Name $folderName -NoRecursion -ErrorAction SilentlyContinue
                if(!$folderObj) {
                    # create folder 
                    Log("Creating folder: "+ $folderName)
                    $parentFolder = New-Folder -Location $parentFolder -Name $folderName -ErrorAction SilentlyContinue
                } else {
                    $parentFolder = $folderObj
                }
        }
        return $ParentFolder
}

function Register_VMs($RootFolder)
{
	# loop to register VMs in target VC 
	foreach($vmRow in $vmRows){
        try {
            $dcObj = get-datacenter -Name $vmRow.TargetDatacenter -ErrorAction Stop
        } catch {
          Log("Registration Failed for: " + $vmRow.VMName + ". Datacenter:" + $vmRow.TargetDatacenter + " not found!")
          if($vmRow.BlockOnFailure) { $blockNext = $true } 
          Continue
        }
                
        $parentFolder = CreateVCFolder $RootFolder $dcObj $vmRow.VCFolder

        if (!$parentFolder) {
            Log("VC folder error for " + $vmObj.Name + ". Skipping registration.")
            if($vmRow.BlockOnFailure) { $blockNext = $true } 
            Continue
        }

        # check if VM already registered in vCenter
        $vmObj = get-vm -Name $vmRow.VMName -Location $parentFolder -ErrorAction SilentlyContinue

        if($vmObj) {
            # already registered --> process next VM
			Log($vmObj.Name + " already registered. Skipping registration")
		    Continue
         }          

        # select ESX host to register VM with
        if($vmRow.TargetESXCluster) {
            $vmHost = (Get-Cluster $vmRow.TargetESXCluster | Get-VMHost | Get-Random)
        } else {
            $vmHost = ($dcObj | Get-VMHost | Get-Random)
        }
	    
        # register VM
	    Log("Registering " + $vmRow.VMName + " in VMHost: " + $vmHost.Name)
		try {
			$vmObj = New-VM -Name $vmRow.VMName -VMHost $vmHost -VMFilePath $vmRow.VMXPath -Location $parentFolder -ErrorAction Stop
			# registration succeeded
			## workaround to 1.3 UUID tracking bug
            ChangeUUID($vmObj)
		} catch {
          Log("Registration Failed for: " + $vmRow.VMName + " in VMHost: " + $vmHost.Name)
          if($vmRow.BlockOnFailure) { $blockNext = $true } 
        }
    }
    return $blockNext
}

function Get_Job_Runtime_Log(){
    # get job verbose stream during run
    While ($runningJobs = Get-Job | where {($_ -in $jobs) -and ($_.State -eq "Running")})
    {  
       foreach ($job in $runningJobs){
            JobWriteVerbose $job
            Start-Sleep -Seconds 3
       }
    }
}

function RunInCloud_Jobs($queue)
{
	$jobs = @()

    while($queue.Count -gt 0){
            # collect background job logs for interactive response
            if($jobs) {
                start-sleep -S 10
                foreach( $job in $jobs ) {
                    JobWriteVerbose $job
                } 
            }

            $vmRow = $queue.Dequeue();
            # input validations
            $dc = get-Datacenter -Name $vmRow.TargetDatacenter
            if(!$dc) {
                # dc not found
                if($vmRow.BlockOnFailure) { $blockNext = $true }
                continue 
            }

            $ce = Get-VelosCe -DatacenterId $dc.Id -Name $vmRow.CloudExtension
            if(!$ce) {
                # ce not found
                Log("CloudExtension: "+$vmRow.CloudExtension + " not found. Skipping run in cloud.")
                if($vmRow.BlockOnFailure) { $blockNext = $true }
                continue 
            }

            if ($targetFolder) { 
                if($vmRow.VCFolder) { 
                    $targetFolderPath = $targetFolder + "/" + $vmRow.VCFolder 
                } else {
                    $targetFolderPath = $targetFolder
                }
            } else { 
                $targetFolderPath = $vmRow.VCFolder 
            }
            $folderObj = getFolderFromPath $vmRow.TargetDatacenter $targetFolderPath
            $vm = get-VM -Name $vmRow.VMName -Location $folderObj -ErrorAction SilentlyContinue
            if (!$vm) { 
                # vm not registered in vc
                Log("VM: "+ $vmRow.VMName + " is not registered in target VC folder. Skipping run in cloud.")
                if($vmRow.BlockOnFailure) { $blockNext = $true }
                continue 
            } else {
                # vm is active locally in DR scenario                
                if (($vm.PowerState -eq "PoweredOn") -and ($DR)) {
                  Log("VM: "+ $vm.Name + " is already active. Skipping run in cloud.")
                  continue
                } elseif (($vm | get-velosvm | where {$_.PowerState -ne "Stopped"})) {
                  # vm is already active in cloud
                  Log("VM: "+ $vm.Name + " is already active in cloud. Skipping run in cloud.")
                  continue
                }
            }

            # throttle jobs for ce
            if ((count-CeTasksForType $ce.CloudExtensionId "RunVmInCloud") -ge $maxParallelJobsPerCE){
                Log("Throttling CloudExtension: "+ $ce.Name)
                $queue.Enqueue($vmRow)
                continue
            }
        
            # submit run in cloud job
	    	Log("Starting job for VM: " + $vmRow.VMName)
         
 	    	$jobs += Start-Job -Name $vmRow.VMName -ScriptBlock $run_one -ArgumentList $creds.get_item('VelostrataManager'), $creds.get_item('subscriptionId'), $vmRow.VMName, $vm.Id, $dc.Id, $vmRow.CloudExtension, $vmRow.CloudInstanceType, $vmRow.CloudEdgeNode, $vmRow.CloudSubnet, $vmRow.CloudSecurityGroup, $vmRow.CloudStaticIp, $vmRow.CloudResourceGroupId, $vmRow.ProbeTCPPort, $vmRow.ProbeWaitMinutes, $WriteIsolation
    }
    Return $jobs
}

function RunInCloud_VMs(){
        # prepare job queue
        $queue = New-Object System.Collections.Queue
        foreach ($vmRow in $vmRows){
            $queue.Enqueue($vmRow)
        }

        $blockNext = $false

        # submit run-in-cloud jobs
        $jobs = RunInCloud_Jobs $queue
    
	    Log("Waiting for jobs to complete...")
        # get log from running jobs until no more running jobs
        Get_Job_Runtime_Log

        # get job results
 	    foreach ($job in $jobs){
            # wait for job to end
            wait-job $job | Out-Null
            
            # log remaining output
            JobWriteVerbose $job
		    
            # collect job result
            [bool]$result = $job.ChildJobs[0].output.readall() | select -Last 1
        
            Log($job.Name + " job result is: "+ $result) 

            # check if need to block next Run Group
            $vmRow = ($vmRows | where-object {$_.VMName -eq $job.Name })
            if(($vmRow.BlockOnFailure -eq $true) -and !$result) {$blockNext = $true}
       	
            # clean job buffers
            Remove-Job $job
	    }
    return $blockNext
}

function RunOnPrem_Jobs()
{
	$jobs = @()
	foreach ($vmRow in $vmRows){
		# submit run in cloud job
		Log("Starting job for VM: " + $vmRow.VMName)
		if ($targetFolder) { 
			if($vmRow.VCFolder) { 
				$targetFolderPath = $targetFolder + "/" + $vmRow.VCFolder 
			} else {
				$targetFolderPath = $targetFolder
			}
		} else { 
			$targetFolderPath = $vmRow.VCFolder 
		}
        $folderObj = getFolderFromPath $vmRow.TargetDatacenter $targetFolderPath
		$vm = get-VM -Name $vmRow.VMName -Location $folderObj -ErrorAction SilentlyContinue
		try {
				$velosVM = $vm | Get-VelosVm 
				if (($velosVM) -and ($velosVM.MigrationState -ne "Detaching" -and $velosVM.MigrationState -ne "Detached" -and $velosVM.MigrationState -ne "CleaningUp")) {
					Log("Starting Run On Prem for VM: "+$vm.Name)
					$jobs += Start-Job -Name $vmRow.VMName -ScriptBlock $Return_One_From_Cloud -ArgumentList $creds.get_item('vCenterServer'), $creds.get_item('vCenterUser'), $creds.get_item('vCenterPassword'), $creds.get_item('VelostrataManager'), $creds.get_item('subscriptionId'), $vm.Id, $vmRow.ProbeTCPPort, $vmRow.ProbeWaitMinutes, $vmRow.TargetDatacenter, $vmRow.VCFolder
					
				} elseif(!$velosVM) {
					Log("VM: "+$vmRow.VMName+" is already on prem.")
				} 
				else{ 
					Log("Could not Run On Prem for VM: "+$vmRow.VMName+ ", reason: vm state is " + $velosVM.MigrationState)
				}
			} catch {
				Log("Could not Run On Prem for VM: "+$vmRow.VMName + ", reason: "+$_)
			}
    }
    Return $jobs
}

function Run_OnPrem(){
        
		$jobs = RunOnPrem_Jobs
        
        $blockNext = $false

    
	    Log("Waiting for jobs to complete...")
        # get log from running jobs until no more running jobs
        Get_Job_Runtime_Log

        # get job results
 	    foreach ($job in $jobs){
            # wait for job to end
            wait-job $job | Out-Null
            
            # log remaining output
            JobWriteVerbose $job
		    
            # collect job result
            [bool]$result = $job.ChildJobs[0].output.readall() | select -Last 1
        
            Log($job.Name + " job result is: "+ $result) 

            # check if need to block next Run Group
            $vmRow = ($vmRows | where-object {$_.VMName -eq $job.Name })
            if(($vmRow.BlockOnFailure -eq $true) -and !$result) {$blockNext = $true}
       	
            # clean job buffers
            Remove-Job $job
	    }
		
    return $blockNext
}

function Delete_LinkedClones(){
 # cleanup linked clones with specified prefix
 $blockNext=$false

 # check if clones found for prefix
 $clones = get-vm -Location $targetFolder -ErrorAction SilentlyContinue | where {$_.Name -like "$Prefix-*"} 
 if (!($clones)) { 
    Log("No clones found for prefix: "+$Prefix)
    $blockNext =$true 
    return $blockNext
 }

 # abort if clones are in cloud    
 if (($clones | get-velosVM)) {
    Log("Clones for prefix: "+$Prefix+" are in cloud. Aborting delete.")
    $blockNext =$true 
    return $blockNext
}

 # clones found. Find parent and delete clone and backing snapshot
 foreach ($clone in $clones) {
   Log("Get parent VM for clone: "+$clone.Name)
   $parentVMId = Get-Annotation -CustomAttribute ParentVMId -Entity $clone -ErrorAction SilentlyContinue | %{$_.Value}
   $parentVM = Get-VM -Id $parentVMId -errorAction SilentlyContinue
   if ($parentVM) {
     Log("Found parent VM: "+$parentVM.Name)
     Log("Deleting Linked Clone: "+$clone.Name)
     try {
        Remove-VM -DeletePermanently -VM $clone -Confirm:$false -errorAction Stop | out-null
     } catch {
       Log("Failed to delete linked clone VM: "+$clone.Name+". Reason: "+$_)
       $blockNext=$true
       # skip snapshot delete if cannot delete clone
       continue
     }
     $oOriginVMSnapshotName = "Base-snap-for-clone-$Prefix"
     #check if snapshot for clone exists, and delete it
     $oSnapshot = get-snapshot -VM $parentVM | where {$_.Name -eq $oOriginVMSnapshotName} 
     if($oSnapshot) {
        try {
            Log("Deleting backing snapshot for prefix: "+$Prefix)
            remove-snapshot -snapshot $oSnapshot -confirm:$false -errorAction stop | out-null
        } catch {
            Log("Cannot remove backing snapshot for prefix: "+$Prefix)
            $blockNext=$true
        }

     } else {
        Log("Backing snapshot not found for prefix: $Prefix, skipping.")
     }
   }
 }
 return $blockNext
}

function Create_LinkedClones(){
    # create customAttribute for tracking parent VM
    $BlockNext= $False
    if (!(Get-CustomAttribute -Name ParentVMId -ErrorAction SilentlyContinue)) {
        try {
            New-CustomAttribute -TargetType VirtualMachine -Name ParentVMId -ErrorAction Stop
        } catch {
            Log("Failed to create custom attribute for backing snapshot tracking, aborting linked clone creation. Reason: "+$_)
            $BlockNext = $true
            Return $BlockNext
        }
    }

     # create linked clones for the runbook VMs
     foreach($vmRow in $vmRows){
         $folderObj = getFolderFromPath $vmRow.TargetDatacenter $vmRow.VCFolder
         $vm = get-VM -Name $vmRow.VMName -Location $folderObj -ErrorAction SilentlyContinue
         if ($vm) {
            $oOriginVMSnapshotName = "Base-snap-for-clone-$Prefix"
            #check if snapshot for clone already exists, and reuse
            $oSnapshot = get-snapshot -VM $vm | where {$_.Name -eq $oOriginVMSnapshotName}
            if (!$oSnapshot) {
                   #snapshot for clone doesn't exist, create a new one 
                   Log("VM: "+$vm.Name + "- Creating snapshot for linked clone with prefix: "+$Prefix)
                   $oSnapShot=New-Snapshot -VM $vm.Name -Name $oOriginVMSnapshotName -Description "Snapshot for linked clone with prefix: $Prefix" -Quiesce
            } else {
                Log("VM: "+$vm.Name + "- found existing snapshot for linked clone with prefix: "+$Prefix)
            }
            
            # Validate datacenter parameter
            $dc = get-Datacenter -Name $vmRow.TargetDatacenter
            if(!$dc) {
                # dc not found
                Log("Datacenter not found: "+$vmRow.TargetDatacenter)
                if($vmRow.BlockOnFailure) { $blockNext = $true }
                continue 
            }
            # create or validate target VC folder for clone
            $cloneFolder = CreateVCFolder $targetFolder $dc $vmRow.VCFolder
            # skip to process next VM if linked clone exists
            if (Get-VM -Name ($Prefix+"-"+$vm.Name) -Location $cloneFolder -ErrorAction SilentlyContinue) {
             Log("Linked clone already exists for VM: "+$vm.name+", with prefix: "+$Prefix)
             continue
            }
            # create linked clone in clone folder
            Log("Creating linked clone for VM: "+$vm.name+", with prefix: "+$Prefix)
            try{
              # get datastore
              $datastore = get-datastore -vm $vm | select -First 1
              $VMclone = New-VM -Name ($Prefix+"-"+$vm.Name) -VM $vm.Name -Datastore $datastore.Name -Location $cloneFolder -ResourcePool Resources -LinkedClone -ReferenceSnapshot $oOriginVMSnapshotName -Confirm:$False -ErrorAction Stop 
              Set-Annotation -CustomAttribute ParentVMId -Value $vm.Id -Entity $VMclone -ErrorAction Stop | Out-Null
            } catch {
              Log("Failed to create linked clone for VM: "+$vm.name+", reason: "+$_);
              if($vmRow.BlockOnFailure) { $blockNext = $true }
              continue 
            }

        } else {
              Log("VM: "+ $vmRow.VMName + "was not found")
              if($vmRow.BlockOnFailure) { $blockNext = $true }
        }
     }
     return $blockNext
}

Function Start_Migration(){
# start storage migration of runbook VMs that are in cloud, and cached-on-demand 
        foreach($vmRow in $vmRows){
            $folderObj = getFolderFromPath $vmRow.TargetDatacenter $vmRow.VCFolder
            $vm = get-VM -Name $vmRow.VMName -Location $folderObj -ErrorAction SilentlyContinue
            try {
                $velosVM = $vm | Get-VelosVm 
                if (($velosVM) -and ($velosVM.MigrationState -eq "CacheOnDemand")) {
                    Log("Starting storage migration for VM: "+$vm.Name)
                    Start-VelosStorageMigration $vm.Id -ErrorAction Stop | Out-Null
                } else {
                    Log("Skipping storage migration for VM: "+$vmRow.VMName)
                }
            } catch {
                Log("Could not start storage migration for VM: "+$vmRow.VMName + ", reason: "+$_)
            }
        }
}

Function Prepare_Detach(){
# start PrepareToDetach of runbook VMs that are in cloud and are FullyCached 
        foreach($vmRow in $vmRows){
            $folderObj = getFolderFromPath $vmRow.TargetDatacenter $vmRow.VCFolder
            $vm = get-VM -Name $vmRow.VMName -Location $folderObj -ErrorAction SilentlyContinue
            try {
                $velosVM = $vm | Get-VelosVm 
                if (($velosVM) -and ($velosVM.MigrationState -eq "FullyCached")) {
                    Log("Starting Prepare-To-Detach for VM: "+$vm.Name)
                    if ($velosVM.CloudInfo.cloudProvider -eq "Aws") {
                      $task = $vm | Start-VelosPrepareToDetach -StorageSpec gp2 -ErrorAction Stop 
                    } else {
                      $task = $vm | Start-VelosPrepareToDetach -StorageSpec $vmRow.CloudStorageAccount -ErrorAction Stop 
                    }
                    if(($task) -and ($task.state -eq "Failed")){
                        Log("Prepare-to-Detach task ($task.Id) failed. More info: $task.ErrorMessage")
                    }
                } elseif(!$velosVM) {
                        Log("VM: "+$vmRow.VMName+" is not in cloud.")
                } else {
                        Log("Skipping Prepare-To-Detach for VM: "+$vmRow.VMName+" with state: "+$velosVM.MigrationState)
                }
            } catch {
                Log("Could not start Prepare-To-Detach for VM: "+$vmRow.VMName + ", reason: "+$_)
            }
        }
}

Function Run_Detach(){
# start Detach of runbook VMs that are in cloud and are ReadyToDetach
        
        # prepare detach task queue
        $queue = New-Object System.Collections.Queue
        $blockNext = $false
        foreach($vmRow in $vmRows){
            $folderObj = getFolderFromPath $vmRow.TargetDatacenter $vmRow.VCFolder
            $vm = get-VM -Name $vmRow.VMName -Location $folderObj -ErrorAction SilentlyContinue
            try {
                $velosVM = $vm | Get-VelosVm 
                if (($velosVM) -and ($velosVM.MigrationState -eq "ReadyToDetach")) {
                    Log("Starting Detach for VM: "+$vm.Name)
                    $task = $vm | Start-VelosDetach -InstanceType $velosVM.CloudInfo.type.name -ErrorAction Stop 
                    $task = $task | get-velostask
                    if(($task) -and ($task.state -eq "Failed")){
                        Log("Detach task ($task.Id) failed. More info: $task.ErrorMessage")
                        if($vmRow.BlockOnFailure) { $blockNext = $true }
                    } elseif ($task) {
                        Log("Adding Detach task ("+$task.Id+") to wait queue.")
                        $queue.Enqueue($task)
                    }
                } elseif(!$velosVM) {
                        Log("VM: "+$vmRow.VMName+" is not in cloud.")
                } else {
                        Log("Skipping Detach for VM: "+$vmRow.VMName+" with state: "+$velosVM.MigrationState)
                        if($velosVM.MigrationState -eq "Detaching") {
                            #if detach in progress, add to watch queue
                            $task = $velosVM.TaskIds | %{get-velostask $_} | where {$_.Type -eq "DetachVM"} 
                            if($task) {
                                Log("Adding Detach task ("+$task.Id+") to wait queue.")
                                $queue.Enqueue($task)
                            }
                        }
                }
            } catch {
                Log("Could not start Detach for VM: "+$vmRow.VMName + ", reason: "+$_)
            }
        }
        
        # wait for detach tasks to end
        if($queue.Count -gt 0) {Log("Waiting for Detach tasks to complete...")}
        while($queue.Count -gt 0){
                $task = $queue.Dequeue()
                #get updated task status
                $task = $task | get-velostask

                $attemptRestart = $false
                $taskTerminated = $false
                $state = $task.State.ToString().ToLower()

                switch($state){
                  "succeeded" {
                        Log("Detach task for VM: "+ (get-velosvm -id $task.EntityId).VM.Name + " completed in: "+(new-timespan -start $task.StartTime -end $task.EndTime).TotalMinutes+" minutes")
                  }
                  "failed" {$taskTerminated = $true }
                  "cancelled" {$taskTerminated = $true }
                  default {
                        $queue.Enqueue($task)
                        start-sleep -S 5
                  }
                }
                
                if($taskTerminated) {
                        $velosVM = get-velosvm -id $task.EntityId
                        Log("Detach task "+$state+" for VM: "+ (get-velosvm -id $task.EntityId).VM.Name)
                        
                        $vmRow = ($vmRows | where-object {$_.VMName -eq "$velosVM.VM.Name" })                       
                        
                        # drain tasks in progress (e.g. rollback) 
                        Log("Waiting for outstanding tasks to complete on VM: "+ $velosVM.VM.Name)
                        while($velosVM.taskIds){
                          Start-Sleep -S 5
                          $velosVM = $velosVM | get-velosVM
                        }

                        # restart VM if stopped (e.g. after cancel or rollback)
                        if(($attemptRestart) -and ($velosVM.PowerState -eq "Stopped")) {                                                  
                            Log("Attempting to restart VM: "+ $velosVM.VM.Name+" in cloud after "+$state +" detach")
                            try {
                                $velosVM | Start-VelosVm -ErrorAction Stop | Out-Null
                            } catch {
                                Log("Could not restart VM: "+$vmRow.VMName + ", reason: "+$_)
                                if($vmRow.BlockOnFailure) { $blockNext = $true } 
                            }
                        }
                }
    }
    return $blockNext
}




Function Stop_vSphereVMs(){
       foreach($vmRow in $vmRows){
            $folderObj = getFolderFromPath $vmRow.TargetDatacenter $vmRow.VCFolder
            $vm = get-VM -Name $vmRow.VMName -Location $folderObj -ErrorAction SilentlyContinue
            try {
                 if (($vm) -and ($vm.PowerState -eq "PoweredOn")) {
                    Log("Stopping VM: "+$vm.Name)
                    $vm | Stop-VM -Confirm:$false -ErrorAction SilentlyContinue| Out-Null
                } elseif(!$vm) {
                        Log("VM: "+$vmRow.VMName+" was not found in vCenter.")
                } else {
                        Log("Skipping stop for VM: "+$vmRow.VMName+" with PowerState: "+$vm.PowerState)
                }
            } catch {
                Log("Could not stop VM: "+$vmRow.VMName + ", reason: "+$_)
            }
        }
}

Function Start_vSphereVMs(){
       foreach($vmRow in $vmRows){
            $folderObj = getFolderFromPath $vmRow.TargetDatacenter $vmRow.VCFolder
            $vm = get-VM -Name $vmRow.VMName -Location $folderObj -ErrorAction SilentlyContinue
            try {
                 if (($vm) -and ($vm.PowerState -eq "PoweredOff")) {
                    Log("Starting VM: "+$vm.Name)
                    $vm | Start-VM -ErrorAction SilentlyContinue| Out-Null
                } elseif(!$vm) {
                        Log("VM: "+$vmRow.VMName+" was not found in vCenter.")
                } else {
                        Log("Skipping start for VM: "+$vmRow.VMName+" with PowerState: "+$vm.PowerState)
                }
            } catch {
                Log("Could not start VM: "+$vmRow.VMName + ", reason: "+$_)
            }
        }
}

Function Stop_VelosVMs(){
       foreach($vmRow in $vmRows){
            $folderObj = getFolderFromPath $vmRow.TargetDatacenter $vmRow.VCFolder
            $vm = get-VM -Name $vmRow.VMName -Location $folderObj -ErrorAction SilentlyContinue
            try {
                $velosVM = $vm | Get-VelosVm 
                if (($velosVM) -and ($velosVM.PowerState -eq "Running")) {
                    Log("Stopping VM: "+$vm.Name)
                    $vm | Stop-VelosVm -ErrorAction SilentlyContinue| Out-Null
                } elseif(!$velosVM) {
                        Log("VM: "+$vmRow.VMName+" is not in cloud.")
                } else {
                        Log("Skipping stop for VM: "+$vmRow.VMName+" with PowerState: "+$velosVM.PowerState)
                }
            } catch {
                Log("Could not stop VM: "+$vmRow.VMName + ", reason: "+$_)
            }
        }
}
#--------------------------------------------------------------------------------------------
# Main orchestration - top level activity dispatcher        ---------------------------------
#--------------------------------------------------------------------------------------------

# if SaveCreds switch specified - save creds to file
if($SaveCreds){
    Save_Creds
    exit
}

# prepare log
$start = get-date -Format "yyyy-MM-dd-HHmm_ss"
try {
Start-Transcript -Path ($outputPath + "\velos-orchestrator-$start.log")

$ActionDescription = if ($DR) {"Disaster Recovery"} 
                     elseif ($RunInCloud) {"Run In Cloud"} 
                     elseif ($StartMigration) {"Start Migration"} 
                     elseif ($CreateLinkedClone) {"Create Linked Clones"} 
                     elseif ($DeleteLinkedClone) {"Delete Linked Clones"}
                     elseif ($PrepareToDetach) {"Prepare To Detach"}
                     elseif ($Detach) {"Detach"}

if ($WriteIsolation) {
    $ActionDescription = $ActionDescription + ", WriteIsolation Mode"
}

Log("Velostrata Orchestrator - processing action: "+$ActionDescription)

# Get logon credentials
$creds = @{}
$creds = ReadCreds

$vcCred = New-Object System.Management.Automation.PsCredential($creds.get_item('vcenterUser'), $creds.get_item('vcenterPassword'))

# Disconnect any existing VC sessions
if ($DefaultVIServers.Count) {
    Log("Disconnect existing vCenter connections...")
    Disconnect-VIServer -Server * -Force -Confirm:$false
}

# Connect to VC
try {
    Log("Connecting to vCenter: "+ $creds.get_item('vCenterServer'))
    $VCconn = Connect-VIServer -Server $creds.get_item('vCenterServer') -Credential $vcCred -errorAction Stop
} catch {
    Log("Unable to connect to vCenter - " + $_)
    Exit
}


if (($DR) -or ($RunInCloud) -or ($StartMigration) -or ($DeleteLinkedClone)) { 
# Connect to Velostrata Manager
try {
    Log("Connecting to Velostrata Manager: "+$creds.get_item('VelostrataManager'))
    Connect-VelostrataManager $creds.get_item('VelostrataManager') -Username 'apiuser' -Password $creds.get_item('subscriptionId') -errorAction Stop | Out-Null
} catch {
    Log("Unable to connect to Velostrata Manager - " + $_)
    Exit
}
}

$unorderedRun = $false
# Import runbook from CSV file
if (!($DeleteLinkedClone)) {
  $runbook = ImportRunbook
 
  # get Run Groups
  Log("Retrieving Run Groups. (RunGroup -1 will be ignored if exists)")
  $RunGroups = $runbook.RunGroup | Select -Unique | where {$_ -ne -1} | Sort
} else {
  # generic run group for unordered actions
  $unorderedRun = $true
}

# determine orchestrator activity type - ordered or unordered
if($unorderedRun){
    # perform generic actions on runbook VMs without dependencies

    # get list of runbook VMs, ignoring group -1
    $vmRows = @($runbook | Where-Object {$_.RunGroup -ne -1})

    # deleteCreate LinkedClone Action handler
    if ($DeleteLinkedClone) {
      $blockNext = $blockNext -or [bool](Delete_LinkedClones)
    }
   
} else {
# process Run Groups in reverse order. Non blocking on individual failure
Log(">> Launching Reverse-order Actions") 
foreach ($RunGroup in ($RunGroups | Sort -Descending)) {
	$vmRows = @($runbook | Where-Object {$_.RunGroup -eq $RunGroup})
	Log("Processing Run Group: "+ $RunGroup +", VMs: " + $vmRows.count)

    if($RunInCloud -or $StartMigration){
      Stop_vSphereVMs
    }

    if($Detach -or $RunOnPrem){
      Stop_VelosVMs
    }
}

# process Run Groups sequentially in ascending order
Log(">> Launching Ordered Actions:") 
foreach ($RunGroup in $RunGroups) {
    $blockNext = $false
	$vmRows = @($runbook | Where-Object {$_.RunGroup -eq $RunGroup})
	Log("Processing Run Group: "+ $RunGroup +", VMs: " + $vmRows.count)

    # Create LinkedClone Action handler
    if ($CreateLinkedClone) {
      $blockNext = $blockNext -or [bool](Create_LinkedClones)
    }

    # for DR action, import VMs into vCenter
    if ($DR) {
        $blockNext = $blockNext -or [bool](Register_VMs($targetFolder))
    }
 
    # Run In Cloud for actions: DR, RunInCloud, StartMigration
    if (($DR) -or ($RunInCloud) -or ($StartMigration)) {
        $blockNext = $blockNext -or [bool](RunInCloud_VMs)
    }

    # For StartMigration action - Start storage migration if VM is in cloud and in CacheOnDemand state
    # storage migration while in cloud is non blocking for run-group processing and can be rerun to complete coverage of runbook VMs.
    if($StartMigration) {
        Start_Migration
    }

    # Prepare to Detach will operate on VMs that are in FullyCached state in cloud. This action is non-blocking for run-group processing and can be rerun to complete coverage of runbook VMs.
    if($PrepareToDetach) {
        Prepare_Detach
    }

    # Start detach will operate on VMs that are ReadyToDetach in cloud. 
    if($Detach) {
        $blockNext = $blockNext -or [bool](Run_Detach)
        ## TODO: Detach task monitoring and failure handling (blocking)
    }

    if($RunOnPrem) {
         $blockNext = $blockNext -or [bool](Run_OnPrem)
    }

	Log("Done with Run Group: " + $RunGroup )
    # don't process next Run Group if blocking issue occured
    if($blockNext) {
        Log("Blocking issue occured. Stopping.")
        break
    }
}
}

# Disconnect vCenter session
Log("Disconnecting from vCenter: "+$creds.get_item('vCenterServer'))
Disconnect-VIServer -Server $VCconn -Confirm:$false
} Finally {
Stop-Transcript
}