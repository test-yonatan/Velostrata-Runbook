# Velostrata-Runbook
The Velostrata Runbook Automation Tool simplifies the migration of multiple VMs simultaneously.

Using the tool to test and perform your migration reduces both the complexity and risk associated with a migration from on-premise to the cloud. The automation tool is useful for business areas such as IT services, engineering, and dev tests.

The automation tool is ideally suited to migrate the following types of environments:

Complex applications that run on multiple VMs, multi-tier environments, with no database, but with elements that have state, such as Load Balancing, and access data.
Production applications with databases, that communicate with other systems, for example, supply chain, CRM, ticketing, inventory, publishing and collaboration systems.
There are several actions/scenarios supported by the tool:

Exporting inventory for configuration definition.
Migration testing including creating local linked clones and running them in cloud.
Migration including running the workloads in cloud, migrating storage, preparing for cache detach, and detaching.
Moving back to on-premises, that is, move VMs that are running in cloud back to their on-premises location
The Velostrata Runbook Automation Tool is built as a PowerShell script and can be downloaded here. here

The automation actions run in a sequence as defined, and are all re-entrant, that is, if there is any failure in the process, you can fix the problem and restart the script again.

Full documentation is available here: http://docs.velostrata.com/m/60079


- testing - my new contribution
