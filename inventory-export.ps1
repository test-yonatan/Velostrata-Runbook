######################################################
#
# Export A Specified VMWare Virtual Datacenter Inventory to CSV File (rev1.8)
#
# Velostrata ltd.
#
######################################################
[CmdletBinding()]
param (
	[Parameter(Mandatory=$True,Position=1)]
    [string]$vCenterServer,
	[Parameter(Mandatory=$True,Position=2)]	
    [string]$DatacenterName,
	[Parameter(Mandatory=$True,Position=3)]	
    [string]$vCenterUser,
	[Parameter(Mandatory=$True,Position=4)]	
    [securestring]$vCenterPassword
 )
 
Import-Module VMware.VimAutomation.Core
set-PowerCLIConfiguration -invalidCertificateAction "ignore" -confirm:$false

# Parameters --------------------------------------------------------------------------------
$vc = $vCenterServer
$dc = $DatacenterName
$outputPath = (Split-Path $MyInvocation.MyCommand.Path)

# Functions ---------------------------------------------------------------------------------
 
function Log ($text) {
    $stamp = (Get-Date).ToString("HH:mm:ss.fff")
    $VerbosePreference = 'continue'
    Write-Verbose "$stamp | $text"
    $VerbosePreference = 'SilentlyContinue'
}
 
function getFolderPath($vm){
    if (!$vm -or ($vm.Folder -eq "vm")) { return "" }
    $folder = get-folder -Id $vm.FolderId

    while($folder.name -ne "vm") {
        if(!$FolderPath) {
            $FolderPath = $folder.name 
        } else {
            $FolderPath = $folder.name + "/" + $FolderPath
        }
        $folder = get-folder -Id $folder.ParentId
    }

    return $FolderPath
}
# Business part of script -------------------------------------------------------------------
$start = get-date -Format "yyyy-MM-dd-HHmm_ss"
Start-Transcript -Path ($outputPath + "\inventory-export-$start.log")
 
# Get logon credentials
try {
    $cred = $null
    $cred = New-Object System.Management.Automation.PsCredential($vCenterUser, $vCenterPassword)
} catch {
    if (!$cred) {$cred = Get-Credential}
}
 
# Disconnect any existing VC sessions
if ($DefaultVIServers.Count) {
    Log("Disconnect existing vCenter connections...")
    Disconnect-VIServer -Server * -Force -Confirm:$false
}

# Connect to VC
try {
    Log("Connecting to vCenter: $vc")
    $VCconn = Connect-VIServer -Server $vc -Credential $cred -errorAction Stop
} catch {
    Log("Unable to connect to vCenter - " + $_)
    Exit
}

Log("Creating DR inventory table...")
 
$table = New-Object system.Data.DataTable "Inventory for $dc@$vc"
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

# Get list of VMs to DR
$VMs = Get-VM -Location (Get-Datacenter $dc) | Sort

# Build table row per VM
foreach ($vm in $vms)
{
	$row = $table.NewRow()
    $row.RunGroup = -1
	$row.VMName = $vm.Name
	$row.NumCPU = $vm.NumCPU
    $row.MemoryMB = $vm.MemoryMB
    $row.NumDisks = @($vm | Get-HardDisk).count
	$row.OS = ("" + ($vm | get-view | %{$_.config.GuestFullName}))
    if ($row.OS -eq "") { $row.OS = "Unknown" }
    $row.VCFolder = getFolderPath($vm)
    $row.VMXPath = $vm.ExtensionData.Config.Files.VmPathName
	$row.TargetDatacenter = $dc
	$row.TargetESXCluster = ""
	$row.CloudExtension = ""
    $row.CloudEdgeNode = ""
    $row.CloudInstanceType = ""
    $row.CloudSubnet = ""
    $row.CloudSecurityGroup = ""
    $row.CloudStaticIP = ""
    $row.CloudResourceGroupId = ""
    $row.CloudStorageAccount = ""
    $row.ProbeTCPPort =  if ($row.OS -ne "Unknown") {if ($row.OS -like "*Windows*") { 3389 } else { 22 } } else { 0 }
    $row.ProbeWaitMinutes = 15
    $row.BlockOnFailure = $false
 	$table.Rows.Add($row)
	Log("Added row for VM: $vm")
}

# Export table to CSV
$outputCSVFile =  ($outputPath + "\inventory-$dc@$vc-$start.csv")
Log("Exporting table to CSV file: $outputCSVFile")
$table | Export-Csv -Path $outputCSVFile -NoTypeInformation 

# Disconnect vCenter session
Log("Disconnecting from vCenter: $vc")
Disconnect-VIServer -Server $VCconn -Confirm:$false
Stop-Transcript