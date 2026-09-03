# Backup and recovery expectations

What the platform team has committed to, and what the estate actually does. This
is a description, not a procedure that runs from this repository: no backup job
is defined here, and nothing in `infrastructure/` executes.

## Objectives

| Database | RPO | RTO | Owner |
| --- | --- | --- | --- |
| `WideWorldImporters` (OLTP) | 15 minutes | 1 hour | application team |
| `WideWorldImportersDW` | 4 hours | 8 hours | data platform team |
| `WideWorldImporters_Staging` | not backed up | 8 hours (rebuild) | data platform team |
| `SSISDB` | 24 hours | 4 hours | data platform team |
| `msdb` on the ETL host | 24 hours | 4 hours | data platform team |
| Oracle `WWIGERP` | 15 minutes | 2 hours | ERP team |
| Landing zone shares | 24 hours | 24 hours | infrastructure team |

The warehouse RTO of eight hours is the number the business agreed to; it is
also, not coincidentally, roughly how long a full restore of a 2.1 TB database
from the SAN share takes. Nobody has asked for it to be shorter since.

## Schedules

- **OLTP**: full weekly (Sunday 20:00), differential nightly, log every 15
  minutes. Run by the application team's own maintenance solution, not by
  anything in `sqlserver/agent`.
- **Warehouse**: full weekly (Saturday 20:00, before the weekly maintenance
  job), differential nightly at 20:00, log hourly. The `FACT_HISTORY` filegroup
  is read-only between closes and is backed up only when it is made writable, so
  a full backup during a close is substantially larger than one outside it.
- **Staging**: not backed up. It is rebuildable from the sources, and a 900 GB
  backup of data that is truncated nightly was judged not worth the window. The
  consequence is that a staging loss means a full re-extract, including the
  Oracle GL extract, which needs an out-of-window slot from the ERP team.
- **SSISDB**: full nightly. The catalogue master key is backed up separately by
  the DBA team and is required for a restore to be usable - a restore without it
  gives a catalogue whose sensitive parameters cannot be decrypted.
- **msdb**: full nightly. In practice the faster recovery path for the job
  definitions is to re-run `sqlserver/agent/90_install_all_agent_jobs.sql`,
  which is one of the reasons those scripts are idempotent.

## Disaster recovery

Log shipping from `sqlprod-dw` to DC2 every 15 minutes, with a 30-minute restore
delay so that a logical error can be caught before it reaches the standby. There
is no automatic failover: a DR invocation is a manual, rehearsed procedure of
about four hours, most of which is repointing the ETL host and the reporting
gateway.

The ETL host itself has no DR copy. Recovery is a rebuild: install the
prerequisites listed for `wwi-deploy01` in `infrastructure/servers.yaml`, restore
`SSISDB` and the catalogue master key, then run the deployment scripts. That
path has never been exercised end to end.

## Restore expectations that are known to be untested

These are written down here rather than assumed:

- A point-in-time restore of the warehouse to mid-close has never been
  attempted. The read-only `FACT_HISTORY` filegroup makes it more complicated
  than the runbook implies.
- A staging rebuild after a total loss depends on an out-of-window Oracle
  extract slot that has never been requested.
- The SSISDB restore path assumes the catalogue master key backup is current.
  There is no automated check that it is.
- DR failover of the ETL host has never been rehearsed, only documented.

## What recovery means for the ETL control framework

After any restore, the control tables and the data can disagree:

- `etl.Watermark` may point past the restored data, which causes the next
  incremental load to skip rows silently. The runbook step is to reset the
  affected watermarks by hand before enabling the jobs; there is no automation
  for it, and it has been missed before.
- `etl.Batch` rows can be left in a running state by the outage itself. The
  health-check job reports these as stuck batches within 30 minutes of the jobs
  being re-enabled.
- `etl.RowCountAudit` variance for the first run after a restore is expected to
  be large. Suppressing that alert for one run is a manual step.

The dependency of a restore on hand-maintained watermark resets is one of the
clearest operational costs in the estate.
