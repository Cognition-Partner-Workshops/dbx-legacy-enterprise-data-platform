# Monitoring and alerting

The estate is monitored through the `etl` control framework's own views rather
than through anything the ETL tooling provides. That is deliberate: the views are
the only place where the Oracle extract, the SSIS packages and the SQL Agent
schedule can be seen as one run.

Nothing here executes. This is the definition the monitoring collector and the
health-check job are built against.

## Sources

| Source | What it gives |
| --- | --- |
| `etl.vw_BatchStatus` | current and recent batches, their state and elapsed time |
| `etl.vw_PackageDurations` | per-package duration against its own history |
| `etl.vw_RowCountVariance` | loaded row counts against the prior run |
| `etl.vw_ErrorsLast7Days` | `etl.ErrorLog` rolled up by package and step |
| `etl.vw_WatermarkCurrent` | every watermark and how far behind it is |
| `etl.vw_DataQualityFailures` | failing rules from `etl.DataQualityResult` |
| `msdb.dbo.sysjobhistory` | job-level outcome, for the jobs that fail before they reach a batch |

The last row matters: a job that fails at step 1 never writes an `etl.Batch`
row, so a monitor built only on the control views would see nothing at all. That
gap was found the hard way.

## Alerts

| # | Condition | Source | Severity | Route | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | nightly batch not complete by 05:30 UTC | `etl.vw_BatchStatus` | high | WWI ETL On-Call | 05:30 is when NA finance starts reading |
| 2 | batch running > 3x its median duration | `etl.vw_PackageDurations` | high | WWI ETL On-Call | stuck rather than slow, in practice |
| 3 | any `etl.ErrorLog` row with severity >= 16 | `etl.vw_ErrorsLast7Days` | high | WWI ETL On-Call | |
| 4 | row-count variance outside tolerance on a fact load | `etl.vw_RowCountVariance` | high | WWI ETL On-Call | tolerance is per-fact, in `etl.DataQualityRule` |
| 5 | watermark more than 6 hours behind | `etl.vw_WatermarkCurrent` | high | WWI ETL On-Call | the OLTP change-tracking retention is 7 days, so 6 hours is early warning, not urgency |
| 6 | hourly incremental failed 3 times consecutively | `msdb.dbo.sysjobhistory` | high | WWI ETL On-Call | the job disables its own schedule at this point |
| 7 | quarantine count for a run above threshold | `etl.RejectedRecord` | medium | WWI ETL On-Call | threshold is `MaxRejectPercent` |
| 8 | reference data stale beyond the regional window | `ref` staleness checks | medium | WWI Finance Systems | NA 7 days, EU 3 days, APAC 14 days - see below |
| 9 | FX rates incomplete at close time | close gate step | high | WWI Finance Systems | blocks the close job rather than failing it |
| 10 | data quality rule failing for 3 consecutive runs | `etl.vw_DataQualityFailures` | medium | WWI Data Stewards | a single failure is noise |
| 11 | free space below 15% on a data or log drive | `MNT_Check_DiskSpace` | medium | WWI Platform DBA | |
| 12 | `DBCC CHECKDB` reporting any error | weekly maintenance job | critical | WWI Platform DBA | |
| 13 | log shipping restore more than 60 minutes behind | DR standby | medium | WWI Platform DBA | |
| 14 | SSIS catalogue operation retention job not run in 7 days | `SSISDB.catalog.operations` | low | WWI Platform DBA | |

## Regional divergence in alerting

The estate does not alert the same way for the three regions, because the
underlying obligations differ:

- **EU**: reference staleness is a three-day window and is treated as a
  compliance issue, not an ETL one - a stale VAT rate means invoices are being
  raised at the wrong rate. It pages the finance systems team, not the ETL
  on-call. EU consent-enforcement failures in the customer sync job are the only
  data-quality failure that alerts on the first occurrence.
- **NA**: a seven-day window on state and county tax tables, alerting to the ETL
  on-call as an ordinary load problem. NA tolerates a stale rate for a day
  because the correction is applied in the billing system downstream.
- **APAC**: a fourteen-day window on GST classes, and no alert at all for
  cross-border customer flagging - it is reviewed in the quarterly
  reconciliation instead. The APAC bank file feed has its own alert because the
  WAN hop to Singapore fails often enough to be routine.

## Routing

Alerts are delivered by SQL Agent operator notification through the SMTP relay
described in `infrastructure/network.yaml`. There is no paging integration; the
on-call rota is a distribution list that forwards to a phone. Three operators
exist:

| Operator | Receives |
| --- | --- |
| `WWI ETL On-Call` | load failures, batch and watermark alerts, reject thresholds |
| `WWI Platform DBA` | maintenance, integrity, space and DR alerts |
| `WWI Finance Systems` | close gates, FX completeness, EU reference staleness |

## The health-check job

`WWI - ETL Health Check` runs every 30 minutes and covers conditions 1, 2, 3, 5
and 11. It exists because the monitoring collector polls hourly and, at one
point, a nightly failure was not noticed until the following morning. It
duplicates part of the collector's coverage on purpose.

## Known gaps

- Nothing monitors the Oracle side directly. If the ERP extract is slow because
  of something happening on the ERP instance, the estate sees only a long-running
  package.
- The collector reads from the warehouse copy of the `etl` tables and the health
  check reads from the staging copy. They can disagree, and reconciling them by
  hand is a routine part of investigating an incident.
- There is no alert for a job that is *disabled*. The two close jobs ship
  disabled by design, so a job that someone disabled during an incident and
  forgot to re-enable looks exactly the same.
