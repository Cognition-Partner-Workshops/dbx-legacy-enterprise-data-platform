# Runbook: running the estate

The daily operation of the estate: what runs when, what to look at, and what to
do when something fails. Sources are `sqlserver/agent/`, the control framework
in `sqlserver/control/`, and `config/estate-catalog.yaml`.

**Nothing here has been run.** There is no execution history behind this
document; it describes what the checked-in job definitions and packages would
do. Every threshold and duration below comes from a script, not from a
measurement.

## The day

| Time (server local) | Job | Master | Cadence |
| --- | --- | --- | --- |
| 00:00, every 15 min | WWI - File Ingestion | `Master_File_Ingestion` | continuous |
| 00:00, every 30 min | WWI - ETL Health Check | - | continuous |
| 00:10, every 4 hours | WWI - Reject Reprocessing | - | recovery |
| 00:15 | WWI - Customer Master Sync | `Master_Customer_Sync` | daily |
| 01:30 | WWI - Daily ETL | `Master_Daily_ETL` | daily, the main run |
| 04:00 | WWI - Month End | `Master_Month_End` | daily, calendar-gated, **disabled** |
| 05:00 monthly day 1 | WWI - Finance Close | `Master_Finance_Close` | monthly, **disabled** |
| 05:00-23:00 hourly | WWI - Hourly Incremental | `Master_Hourly_Incremental` | intraday |
| 05:00-22:00, every 20 min | WWI - Intraday Inventory Refresh | `Master_Intraday_Inventory` | intraday |
| 05:00 Sunday | WWI - Control History Purge | - | maintenance |
| 03:00 Sunday | WWI - Weekly Reference Refresh | `Master_Weekly_Reference_Load` | weekly |
| 22:00 Saturday | WWI - Weekly Maintenance | `Master_Weekly_Maintenance` | weekly |

The two period-close jobs are created disabled deliberately: the fiscal calendar
rows for the year must be loaded before a close is allowed to fire.

The 01:30 nightly is the run that matters. It executes 192 packages and it
overlaps the 00:00 file ingestion and the 00:10 reject reprocessing, which is a
known collision - the reject reprocessor writes into tables the nightly is
reading. Nothing prevents this.

## What a run looks like in the control tables

Every run is one `etl.Batch` row, opened by `etl.usp_StartBatch` at the top of
the master and closed by `etl.usp_EndBatch`. Each section of the master is an
`etl.BatchStep`. Each package execution is an `etl.PackageExecution` row with
timings, statuses, and row counts, opened by `etl.usp_LogPackageStart`.

Because the control framework is deployed into both the staging database and the
warehouse, a nightly produces a batch in each, correlated only by business date
and batch name. Nothing enforces the correlation, so when you are chasing a run
you have to look in both.

## Monitoring

Seven views, in the order an operator uses them:

| View | Question |
| --- | --- |
| `etl.vw_BatchStatus` | did last night finish, and when |
| `etl.vw_RecentErrors` | what failed |
| `etl.vw_PackageExecutionHistory` | what has this package been doing lately |
| `etl.vw_SlowPackages` | why is the window overrunning |
| `etl.vw_RowCountReconciliation` | do the hop counts agree |
| `etl.vw_RejectSummary` | what left the pipeline |
| `etl.vw_WatermarkStatus` | are the incrementals advancing |

The health-check job runs every 30 minutes and is what turns those views into a
notification. Operators are `WWI ETL On-Call`, `WWI Platform DBA` and
`WWI Finance Systems`.

The deeper queries in `validation/runtime/` cover the same ground with more
detail and have never been run.

## Morning check

1. `etl.vw_BatchStatus` - one `Succeeded` batch for last night's business date,
   in **both** databases.
2. `etl.vw_RejectSummary` - reject volume in line with the last week. A spike
   in one object is usually a source change; a spike everywhere is usually a
   reference load that did not run.
3. `etl.vw_WatermarkStatus` - nothing locked, nothing stale. A watermark left
   locked by a failed run silently stops that incremental load, and the load
   still reports success.
4. `Fact.Fact Load Hold` - the held count should be falling, not accumulating.
   Rows here are invisible to every report until the rekey load releases them.
5. `etl.vw_SlowPackages` - the nightly must clear before the 05:00 intraday
   jobs start, or they queue behind it.

## When a package fails

The Agent jobs branch on failure rather than quitting: a failed step jumps to a
"mark batch failed" step and then to a notification step, so a failure always
lands in `etl.ErrorLog` and the batch is closed as `Failed` rather than left
`Running`. A batch left `Running` means the master itself died, and that is
worse - nothing will close it and the next night's run has to be told to adopt
or ignore it (`@AllowAdoptRunning` on `etl.usp_StartBatch`).

Restart is by step. `etl.Batch.RestartFromStep` records where to resume; the
master reads it. There is no automatic resume: a human sets it and re-runs the
job.

Retries are per-failure-mode, from `sqlserver/agent/`:

| Job | Retry | Because |
| --- | --- | --- |
| Daily ETL, Oracle extract steps | 2 x 15 min | listener bounces |
| Hourly Incremental, OLTP read | 3 x 2 min | replica failover |
| Finance Close, FX gate | 4 x 60 min | waiting for a rate publication |

## Common failures

| Symptom | Usual cause | Action |
| --- | --- | --- |
| Batch stuck `Running` for hours | master died without `usp_EndBatch` | close it by hand, then restart from `RestartFromStep` |
| Incremental load succeeds but loads nothing | watermark locked or not advancing | check `etl.vw_WatermarkStatus`, unlock, re-run |
| Fact row counts far below source | dimension lookups failing to unknown member | `Fact.Fact Load Hold` and the `-1` key counts |
| Reference-dependent loads all wrong | weekly reference load did not run | re-run `Master_Weekly_Reference_Load`, then the dependent loads |
| Aggregate disagrees with the fact | rebuild ran before the fact finished | re-run the aggregate |
| Reporting amounts wrong for one region | missing FX rate; the loads default the rate to 1 | check `[FX Rate Source Code]` coverage for that region |
| File ingestion silently doing nothing | landing-zone path or permissions | `WWI_LANDING_ROOT`, then the `WWI_FileOps_Proxy` |

## Reprocessing rejects

`WWI - Reject Reprocessing` runs every four hours and pushes rows from
`etl.RejectedRecord` back through the screens. It is the half of a data-flow
cycle that only stays acyclic because it runs in a different window from the
screens themselves - see `docs/dependency-maps/etl-dependency-map.md`. Running
it during the nightly is the way to create the loop.

Rejects not reprocessed within three days should be looked at by hand; nothing
escalates them.

## Month end and close

Both close jobs ship disabled. The sequence is: load the fiscal calendar for the
period, confirm `GL_PERIOD_STATUS` on the Oracle side, enable the job, let it
run, disable it again. `Master_Finance_Close` waits on an FX rate publication
and retries hourly four times; if the rate never publishes, the close does not
run, and the close is not restartable from the middle without a human deciding
which steps already committed.

## Housekeeping

- `WWI - Control History Purge` (Sunday 05:00) trims the control tables.
  Retention is in `infrastructure/storage-layout.yaml`.
- `WWI - Weekly Maintenance` (Saturday 22:00) does indexes and statistics.
- Agent step output files land under `$(AgentLogRoot)`; retention for that
  directory is also in `infrastructure/storage-layout.yaml` and is not enforced
  by anything in this repository.
