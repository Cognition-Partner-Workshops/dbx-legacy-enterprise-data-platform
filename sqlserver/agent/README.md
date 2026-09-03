# SQL Agent jobs

The estate's schedule. Every job here drives one of the `Master_*` orchestration
packages declared in `config/estate-catalog.yaml`, or one of the maintenance,
recovery and monitoring packages around them.

Nothing in this directory has been run. The scripts write rows into `msdb` only;
they do not need the SQL Agent service to be running when they are installed,
and they make no attempt to start a job.

## Install

```
sqlcmd -S "$SQLSERVER_HOST,$SQLSERVER_PORT" -b -I \
       -v EnvironmentCode="DEV" StagingDatabase="..." ... \
       -i sqlserver/agent/90_install_all_agent_jobs.sql
```

In practice the agent stage is invoked from `deployment/deploy-all.sh` or
`deployment/deploy-all.ps1`, which resolve every sqlcmd variable from the
environment variables documented in `config/README.md`.

## What gets created

| Script | Job | Category | Schedule |
| --- | --- | --- | --- |
| `00_create_job_categories.sql` | - | eight `WWI *` categories | - |
| `01_create_operators.sql` | - | operators `WWI ETL On-Call`, `WWI Platform DBA`, `WWI Finance Systems` | - |
| `02_create_proxies_and_credentials.sql` | - | proxies `WWI_SSIS_Proxy`, `WWI_FileOps_Proxy` | - |
| `10_job_WWI_Daily_ETL.sql` | WWI - Daily ETL | WWI ETL - Nightly | daily 01:30 |
| `11_job_WWI_Hourly_Incremental.sql` | WWI - Hourly Incremental | WWI ETL - Intraday | hourly 05:00-23:00 |
| `12_job_WWI_Intraday_Inventory.sql` | WWI - Intraday Inventory Refresh | WWI ETL - Intraday | every 20 min 05:00-22:00 |
| `13_job_WWI_Customer_Sync.sql` | WWI - Customer Master Sync | WWI ETL - Nightly | daily 00:15 |
| `14_job_WWI_File_Ingestion.sql` | WWI - File Ingestion | WWI ETL - Intraday | every 15 min |
| `15_job_WWI_Weekly_Reference_Load.sql` | WWI - Weekly Reference Refresh | WWI ETL - Reference | Sunday 03:00 |
| `16_job_WWI_Weekly_Maintenance.sql` | WWI - Weekly Maintenance | WWI Platform - Maintenance | Saturday 22:00 |
| `17_job_WWI_Month_End.sql` | WWI - Month End | WWI ETL - Period Close | daily 04:00, calendar-gated, ships disabled |
| `18_job_WWI_Finance_Close.sql` | WWI - Finance Close | WWI ETL - Period Close | monthly day 1 05:00, ships disabled |
| `19_job_WWI_Reject_Reprocess.sql` | WWI - Reject Reprocessing | WWI ETL - Recovery | every 4 hours |
| `20_job_WWI_Control_History_Purge.sql` | WWI - Control History Purge | WWI Platform - Maintenance | Sunday 05:00 |
| `21_job_WWI_Health_Check.sql` | WWI - ETL Health Check | WWI Platform - Monitoring | every 30 minutes |

The two period-close jobs are created disabled on purpose: the fiscal calendar
rows for the year have to be loaded before a close can be allowed to fire.

## Conventions the scripts follow

- **Idempotent.** A job script drops and re-adds its own job so step ordering
  and failure branching cannot drift. Schedules are shared objects and are
  created only when absent, then attached.
- **Failure branching.** Load jobs end with a "mark batch failed" step and a
  notification step. Steps use `@on_fail_step_id` to jump into that branch
  rather than simply quitting, so a failure always lands in `etl.ErrorLog`.
- **Retries.** Retry counts and intervals reflect the failure mode, not a house
  default: two 15-minute retries for the nightly Oracle extract (listener
  bounce), three 2-minute retries for the hourly OLTP read (replica failover),
  four 60-minute retries on the finance FX gate (waiting for a rate publication).
- **Output logging.** Every step writes an output file under `$(AgentLogRoot)`;
  retention for that directory is defined in `infrastructure/storage-layout.yaml`.
- **Proxies.** SSIS steps run as `WWI_SSIS_Proxy`; file-system steps run as
  `WWI_FileOps_Proxy`. Only the credential and proxy *names* appear in this
  repository. Secrets are injected by the deployment driver from the environment
  variables listed in `config/README.md`.
- **Ownership.** Jobs are owned by `sa` so that a leaver's account cannot orphan
  the schedule - a decision made after exactly that happened, and one a modern
  estate would revisit.
