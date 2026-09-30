# SSIS → Databricks migration — group 10/10 `platform_control`

Orchestration, control framework, generic DQ engine, error handling and maintenance for the
WideWorldImporters SSIS estate, re-implemented as Databricks-native Lakeflow Jobs + PySpark library +
Delta control tables.

| | |
|---|---|
| Landing schema | `otterorders_migration.ssis_platform_control` (36 Delta control/DQ/error tables + volume `landing`) |
| Evidence | `otterorders_migration.evidence.recon_results` (`branch = 'ssis_platform_control'`) |
| Workspace root | `/Workspace/Shared/ssis_migration/platform_control` (target `dev`) |
| Jobs | 31 jobs, all prefixed `ssis_platform_control_` (9 masters, 19 standalone packages, `setup`, `recon`, `e2e_smoke`) |
| Branch / PR | `devin/ssis-migration_platform_control` → `main` |

## Package → artifact mapping

`unit_type = ssis_package`, `harness_version = ssis-migration-v1`. Verdicts are those written by the
last `ssis_platform_control_recon` run (see [Evidence](#evidence)).

| Package | Load type | Legacy source → target | Databricks artifact | Verdict |
|---|---|---|---|---|
| `Master_Daily_ETL` | orchestration | SQL Agent `WWI_Daily_ETL` 01:30 | `resources/master_daily_etl.job.yml` (256 tasks) + `orchestration.py` | PARTIAL |
| `Master_Hourly_Incremental` | orchestration | SQL Agent hourly 05–23 | `resources/master_hourly_incremental.job.yml` (61 tasks) | PARTIAL |
| `Master_Intraday_Inventory` | orchestration | SQL Agent every 20 min 05–22 | `resources/master_intraday_inventory.job.yml` (38 tasks) | PARTIAL |
| `Master_Customer_Sync` | orchestration | SQL Agent 00:15 | `resources/master_customer_sync.job.yml` (50 tasks) | PARTIAL |
| `Master_File_Ingestion` | orchestration | SQL Agent every 15 min | `resources/master_file_ingestion.job.yml` (33 tasks) | PARTIAL |
| `Master_Finance_Close` | orchestration | SQL Agent day 1 05:00 | `resources/master_finance_close.job.yml` (33 tasks) | PARTIAL |
| `Master_Month_End` | orchestration | SQL Agent 04:00 + `ref.FiscalCalendar` gate | `resources/master_month_end.job.yml` (43 tasks) + `fiscalCalendarGate` | PARTIAL |
| `Master_Weekly_Maintenance` | orchestration | SQL Agent Sat 22:00 | `resources/master_weekly_maintenance.job.yml` (29 tasks) | PARTIAL |
| `Master_Weekly_Reference_Load` | orchestration | SQL Agent Sun 03:00 | `resources/master_weekly_reference_load.job.yml` (37 tasks) | PARTIAL |
| `DQ_Rule_Engine` | quality_screen | `etl.DataQualityRule` → `etl.DataQualityResult` | `quality.runRuleEngine` → `etl_data_quality_result` | FAIL (fixed, not re-run) |
| `DQ_Threshold_Gate` | quality_screen | `etl.RowCountAudit` → `etl.ReconciliationResult` | `quality.thresholdGate` → `etl_reconciliation_result` | PARTIAL |
| `DQ_Referential_Screen` | quality_screen | `stg.OrderLine`/`stg.SaleLine` → `err.RejectedLookupFailure` | `quality.referentialScreen` → `err_rejected_lookup_failure` | FAIL (fixed, not re-run) |
| `DQ_Reject_Reprocess` | quality_screen | `err.RejectedLookupFailure` → `stg.OrderLine` | `quality.rejectReprocess` → `stg_order_line_replay` | PARTIAL |
| `DQ_File_Screen` | quality_screen | `raw.FilePartnerSales` → `err.RejectedFileRow` | `quality.fileScreen` → `err_rejected_file_row` | FAIL (fixed, not re-run) |
| `ING_FILE_QuarantineMalformed` | file_ingest | `quarantine/*` → `err.RejectedFileRow` | `quality.quarantineSweep` over volume `landing/quarantine` | PARTIAL |
| `ERR_Handle_PackageFailure` | utility | `err.*` → `etl.ErrorLog` | `errors.handlePackageFailure` (+ job `on_failure` notifications) | NOT_APPLICABLE |
| `ERR_Notify_Operations` | utility | `etl.ErrorLog` → e-mail | `errors.notifyOperations` → `etl_operator_notification` + job email notifications | NOT_APPLICABLE |
| `ERR_Quarantine_BadFiles` | utility | file share → `quarantine/` | `errors.quarantineBadFiles` (volume move + `work_bad_file_queue`) | NOT_APPLICABLE |
| `ERR_Reconcile_RowCounts` | utility | `etl.RowCountAudit` → `work.RowCountReconciliation` | `errors.reconcileRowCounts` → `work_row_count_reconciliation` | PARTIAL |
| `ERR_Retry_FailedSteps` | utility | `etl.BatchStep` → `etl.BatchStepRerunRequest` | `errors.retryFailedSteps` + Lakeflow task `max_retries` | NOT_APPLICABLE |
| `ERR_Route_RejectedRows` | utility | `err.*` → `file:errors` + escalation | `errors.routeRejectedRows` → `work_reject_routing_history`, `errors/` volume folder | PARTIAL |
| `MNT_Archive_ProcessedFiles` | utility | file share `processed/` → `archive/` | `maintenance.archiveProcessedFiles` over the volume | NOT_APPLICABLE |
| `MNT_Check_DiskSpace` | utility | `xp_fixeddrives` | `maintenance.checkDiskSpace` (volume + Delta table size) | NOT_APPLICABLE |
| `MNT_Purge_ControlHistory` | utility | `etl.*` retention delete | `maintenance.purgeControlHistory` → `etl_purge_audit` | NOT_APPLICABLE |
| `MNT_Purge_StagingHistory` | utility | `stg.*` retention delete | `maintenance.purgeStagingHistory` (register-driven) | NOT_APPLICABLE |
| `MNT_Rebuild_Indexes` | utility | `ALTER INDEX … REBUILD/REORGANIZE` | `maintenance.rebuildIndexes` = `OPTIMIZE` + `VACUUM` (Predictive Optimization on UC) | NOT_APPLICABLE |
| `MNT_Update_Statistics` | utility | `UPDATE STATISTICS` | `maintenance.updateStatistics` = `ANALYZE TABLE … COMPUTE STATISTICS` when Delta history shows enough writes | NOT_APPLICABLE |
| `MNT_Validate_Configuration` | utility | `etl.Configuration` checks | `maintenance.validateConfiguration` → `etl_configuration_check` / `etl_preflight_result` | NOT_APPLICABLE |

## Layout

```
databricks.yml               bundle (target dev → /Workspace/Shared/ssis_migration/platform_control)
config/orchestration-plan.json   scoped copy of ssis/orchestration-plan.json (authoritative DAG)
config/sibling_groups.json       package → sibling group slug (for ssis_<slug>_<package> job names)
resources/*.job.yml          GENERATED by tools/generate_jobs.py — 9 master jobs, packages.job.yml, platform.job.yml
src/platform_control/        library (camelCase functions, snake_case tables)
  config.py  tables.py  control.py   PlatformConfig, 36 control-table DDL, ControlFramework lifecycle API
  rules.py                   pure rules (retry classification, thresholds, SSIS expression evaluator, file row parsing…)
  quality.py  errors.py  maintenance.py   DQ_*, ERR_*, MNT_* package logic
  orchestration.py           MasterPlan (plan parser) + Orchestrator (precedence-edge runtime, sibling dispatch)
  runners.py                 package registry: runStandalone / runPackage for the 19 non-master packages
  recon.py                   evidence builder + writer (re-runnable)
  seed.py  files.py  spark.py   rule/config seeding from legacy ref tables, volume file ops, local Spark
notebooks/                   thin entry points: setup, orchestration_task, run_package, recon
tests/                       pytest on local Spark + Delta (rules, control lifecycle, DQ/ERR, MNT, orchestration, recon)
samples/landing/             sample partner-sales + quarantine files uploaded to the volume by setup
tools/generate_jobs.py       plan → Lakeflow Jobs YAML
```

## How it runs

1. `ssis_platform_control_setup` — creates the 36 control tables in the schema, the `landing` volume
   (`processed/ archive/ errors/ quarantine/`), seeds `etl_data_quality_rule`, `etl_configuration`,
   `etl_reconciliation_exemption` … from `wwi_legacy_staging.etl.*` / `ref.*` via federation and uploads
   the sample files. Idempotent.
2. `ssis_platform_control_Master_*` — one Lakeflow Job per master package, generated from the plan:
   * every plan node is a task (`setup` → `node`/`start_phase` → one `child` task per child package →
     `end_phase` → … → `finalize`); dependencies are the plan edges with `run_if: ALL_DONE` so the
     *runtime* applies the exact SSIS precedence semantics (Success / Failure / Completion + the
     `@[User::…]` expressions, evaluated by a safe AST evaluator in `rules.evaluateSsisExpression`);
   * node status, variables (`ExtractAttempt`, `FailedObjectCount`, `NightlyBatchRunning`, …) and
     child outcomes are persisted in `etl_orchestration_variable`, so tasks are stateless and the job
     can be repaired/re-run from any task;
   * our own packages run in-process through `runners.py`; sibling packages are dispatched by name
     `ssis_<slug>_<package>` through the Jobs API (`databricksSiblingRunner`). If the sibling job is
     not deployed yet the child is recorded `Unresolved`, a `Warning` is logged, and the batch ends
     `SucceededWithWarnings` (`sibling_dispatch_on_missing=skip`; set to `fail` for production);
   * schedules are the recovered SQL Agent schedules (UTC, paused in `dev`), `max_concurrent_runs: 1`,
     queueing, task `max_retries` on child tasks (ERR_Retry_FailedSteps semantics), job-level
     `email_notifications.on_failure` (ERR_Notify_Operations semantics);
   * `Master_Month_End` keeps the SQL Agent step-01 gate: it only starts a batch when today is a
     `PeriodCloseDate` in `ref.FiscalCalendar` (`AgentGateOverride=true` bypasses it).
3. `ssis_platform_control_<package>` — 19 standalone jobs (one per non-master package) that open a
   batch, run the package via `runners.runStandalone`, and close the batch. `parameters_json` carries
   the SSIS `$Package::` parameters.
4. `ssis_platform_control_e2e_smoke` — setup → every owned package standalone → recon, the run used to
   produce the evidence.
5. `ssis_platform_control_recon` — rebuilds one evidence row per owned package (28) and appends them
   to `otterorders_migration.evidence.recon_results` with a single `run_id`.

## Design decisions

* **PySpark library + thin notebooks, not DLT.** These packages are control-flow heavy (loops, retries,
  conditional edges, side effects on control tables); Declarative Pipelines have no equivalent for
  batch/step bookkeeping or for dispatching sibling jobs, whereas Jobs + Delta tables map 1:1.
* **Plan is the source of truth.** `resources/*.job.yml` are generated from `config/orchestration-plan.json`
  and `tests/test_orchestration.py::testGeneratedResourcesMatchPlan` fails if the YAML drifts from the plan.
  The `.dtsx` for the masters were only used to confirm parameters/variables and edge expressions.
* **Precedence semantics at runtime, not in `run_if`.** Lakeflow `run_if` cannot express SSIS
  expression constraints (`@[User::ExtractAttempt] <= @[$Package::MaxExtractAttempts]`) or
  "Failure" edges combined with expressions, so every task is `ALL_DONE` and `Orchestrator.shouldRun`
  decides `Skipped` vs run from the persisted upstream statuses/variables. Skipped tasks finish green.
* **Control tables mirror `sqlserver/control/*.sql`** (snake_cased: `etl_batch`, `etl_batch_step`,
  `etl_package_execution`, `etl_watermark`, `etl_error_log`, `etl_row_count_audit`, `err_rejected_*`,
  `work_*` …); identities are generated with `max(id)+1` inside a single writer (jobs are serialized
  by `max_concurrent_runs: 1`).
* **DQ rules are data, not code.** `etl_data_quality_rule` (seeded from the 29 legacy
  `wwi_legacy_staging.etl.DataQualityRule` rows) drives `runRuleEngine`; rule expressions are translated
  with `rules.translateRuleExpression`, which resolves `schema.table` to our tables/federated tables and
  rejects anything that looks like a statement (`;`, comments, DDL/DML keywords) — no free SQL concatenation.
* **Thresholds / gates** (`DQ_Threshold_Gate`, `DQ_File_Screen`, referential screen) keep the legacy
  constants (`FILE_SCREEN_WARN/FAIL_MALFORMED_PCT`, tolerance rows, `etl_reconciliation_exemption`) and
  raise `GateFailure` → step `Failed` → `ERR_Handle_PackageFailure` classification (`RETRYABLE_ERROR_CODES`).
* **Files** live in UC volume `otterorders_migration.ssis_platform_control.landing`; `files.FileOps`
  gives the same API locally (temp dir) and on Databricks (`/Volumes/...`).

## Deliberate deviations from SSIS

* Row-by-row `Lookup` components and OLE DB batch sizes are replaced by set-based joins; ordering of
  reject rows therefore differs (checksums are order-independent).
* `MNT_Rebuild_Indexes` / `MNT_Update_Statistics` have no Delta equivalent of fragmentation %; the
  decision uses Delta history (rows written since last ANALYZE, file count) and `OPTIMIZE`/`VACUUM`
  (Predictive Optimization does this automatically on UC managed tables — the job is kept for parity).
* `MNT_Check_DiskSpace` (`xp_fixeddrives`) reports volume + table bytes and the configured thresholds
  instead of drive letters.
* `ERR_Notify_Operations` writes `etl_operator_notification` (de-duplicated per batch/severity) and relies
  on Lakeflow job `email_notifications` for delivery instead of Database Mail.
* `ERR_Retry_FailedSteps`: the sweep still marks steps/rerun requests, but the actual re-execution is the
  Lakeflow task `max_retries` + the plan's explicit `*Retry` phases.
* Sibling packages are run as separate Lakeflow jobs (`ssis_<slug>_<package>`) rather than child
  `Execute Package Task`s; the estate note in the plan already executed cross-project packages one
  `dtexec` each, so this preserves the boundary.
* Cross-group control data (`wwi_legacy_staging.etl.*`, `ref.FiscalCalendar`, `stg.StockItem` …) is read
  through federation; nothing is written outside our schema and `evidence`.

## Evidence

Written by `src/platform_control/recon.py` (`runRecon`) — one `run_id` per run, 28 rows, every row has
`row_count` + `checksum` checks. Semantics:

* Control/DQ packages that populate a table in our schema are compared against the legacy
  `wwi_legacy_staging.<schema>.<Table>` via `count` and `sum(xxhash64(business cols))`. The legacy
  err/DQ/result tables are **empty on the shared host** (the estate's SSIS run never populated them), so
  the comparison falls back to a source-derived expectation (`"baseline":"source_derived"`) computed with
  the package's own rules over the federated inputs → `PARTIAL`, never `PASS`.
* Master packages are reconciled structurally: plan nodes/edges/child packages vs generated tasks/
  dependencies (`row_count` = node count, `checksum` = hash of edge set) plus a check that the deployed
  job exists → `PARTIAL` (their "output" is control rows, not data).
* Utility packages (ERR_Handle/Notify/Quarantine/Retry, all `MNT_*`) → `NOT_APPLICABLE` with the
  Databricks-native equivalent in `summary`.

### Latest evidence run (kept as the PR's evidence — see "Open questions")

| | |
|---|---|
| `run_id` | `05e91654-ebea-4a29-8e7a-d46d2516481a` |
| `run_at` | 2026-09-30 20:45 UTC |
| `git_sha` | `f131fbb97850857c991105e7f41cba3897ee1fbf` (the deployed commit that produced the run; later commits in this PR are fixes + docs, see below) |
| Produced by | job `ssis_platform_control_e2e_smoke` run `562253133667727` (setup → 19 standalone packages → recon), 16/19 packages succeeded |
| Rows | 28 (one per owned package) |

| Verdict | Count | Packages |
|---|---|---|
| `NOT_APPLICABLE` | 11 | ERR_Handle_PackageFailure, ERR_Notify_Operations, ERR_Quarantine_BadFiles, ERR_Retry_FailedSteps, MNT_* (7) |
| `PARTIAL` | 14 | Master_* (9, structural), DQ_Threshold_Gate, DQ_Reject_Reprocess, ERR_Reconcile_RowCounts, ERR_Route_RejectedRows, ING_FILE_QuarantineMalformed (source-derived baselines match) |
| `FAIL` | 3 | DQ_Rule_Engine, DQ_Referential_Screen, DQ_File_Screen |
| `PASS` | 0 | every legacy DQ/ERR output table is empty on the host, so `PASS` is unreachable by contract |

FAIL causes (one line each; all three are fixed in the PR head but could not be re-run, see below):

* `DQ_Rule_Engine` — package ran fine (29 rules evaluated) but the source-derived checksum hit
  `ARITHMETIC_OVERFLOW`: `SUM(xxhash64(...))` overflows BIGINT under ANSI mode on Databricks (local
  Spark is non-ANSI, so tests passed). Fixed: the accumulator is now `SUM(CAST(xxhash64(...) AS DECIMAL(38,0)))`.
* `DQ_Referential_Screen` (and `DQ_Reject_Reprocess`'s first attempt) — legacy `stg.OrderLine` /
  `stg.SaleLine` / `stg.StockItem` carry `*BusinessKey` columns, not the OLTP `StockItemId` /
  `OrderLineId` the screen assumed (`UNRESOLVED_COLUMN StockItemId`). Fixed: the screen resolves
  `OrderLineBusinessKey` / `StockItemBusinessKey` / `SaleBusinessKey` / `TransactionCurrencyCode` first
  and falls back to the id names.
* `DQ_File_Screen` — first attempt failed with `PERSIST TABLE is not supported on serverless compute`
  (a `DataFrame.cache()`); the retry then failed with `FAILED_JDBC.CONNECTION` because the legacy SQL
  Server host had just been stopped by the operator. Fixed: the `cache()` is gone.

The three `FAIL` rows also carry `FAILED_JDBC.CONNECTION` errors on their legacy `row_count` /
`checksum` checks: the operator stopped the legacy hosts while the recon task was running, so the
second half of the run could not read `wwi_legacy_staging`. `recon.py` now emits an explicit
`{"check":"source_unavailable","pass":false}` in that situation.

## Sibling jobs referenced by the master jobs

Job names follow `ssis_<slug>_<package>` with slugs from `config/sibling_groups.json`
(`customer_engagement, customer_party, finance, logistics_returns, procurement, product_inventory,
ref_calendar, sales_o2c, sales_performance`). None of them existed in the workspace at deploy time;
the master jobs still deploy and run — each unresolved child is logged as a `Warning` in
`etl_error_log` and skipped (`sibling_dispatch_on_missing=skip`). The full list is in the task
descriptions of each `resources/master_*.job.yml` (`sibling job ssis_…`).

## Local development

```bash
pip install -r requirements-dev.txt          # pyspark 3.5.3, delta-spark 3.2.1, pytest, ruff, pyyaml
ruff check src tests tools notebooks
python -m pytest tests -q                     # local Spark + Delta (~10 min)
python tools/generate_jobs.py                 # regenerate resources/ after touching the plan
export DATABRICKS_AUTH_TYPE=pat DATABRICKS_HOST="$DATABRICKS_DEMO_HOST" DATABRICKS_TOKEN="$DATABRICKS_DEMO_TOKEN"
databricks bundle validate -t dev && databricks bundle deploy -t dev
databricks bundle run ssis_platform_control_setup -t dev
databricks bundle run ssis_platform_control_e2e_smoke -t dev
databricks bundle run ssis_platform_control_recon -t dev
```

If Maven Central is unreachable, drop `delta-spark_2.12-3.2.1.jar`, `delta-storage-3.2.1.jar`,
`antlr4-runtime-4.9.3.jar` into `~/spark_jars` (or `PLATFORM_CONTROL_DELTA_JARS`) — `spark.py` picks
them up.

## Open questions / not done

* **Legacy hosts stopped before re-verification.** The operator shut the SQL Server / Oracle EC2 hosts
  down at ~20:50 UTC on 2026-09-30, minutes after the e2e run above. The fixes for the three FAILs
  (overflow-safe checksum, business-key referential screen, no `cache()` on serverless) are in this
  PR, pass the local suite (41 tests), and are deployed to the workspace, but the packages and the recon
  could not be re-run against the sources. Re-run `ssis_platform_control_e2e_smoke` (or
  `databricks bundle run ssis_platform_control_recon -t dev --var git_sha=<head>`) once the hosts are
  back; a run with the hosts down records every data package as `FAIL` / `source_unavailable`, which is
  why the 20:45 run was kept as the evidence of record rather than overwritten. Consequently the
  evidence `git_sha` (`f131fbb`) is the deployed code commit, not the PR head.
* **Sibling jobs are not deployed.** The nine master jobs reference 176 `ssis_<slug>_<package>` jobs
  owned by the other nine sessions. None existed at deploy time; the master jobs deploy (their tasks are
  our own notebooks that dispatch by job *name* at runtime) and unresolved siblings are logged as
  `Unresolved` and skipped (`sibling_dispatch_on_missing=skip`). Set it to `fail` once the estate is
  complete. No master job was run end-to-end on the workspace for that reason; the orchestration
  runtime is covered by the local suite (happy path, failure/retry path, Daily-ETL stand-down, Month-End
  calendar gate, unresolved siblings).
* **`PASS` is unreachable for this group.** All legacy DQ/ERR output tables
  (`etl.DataQualityResult`, `etl.ReconciliationResult`, `err.RejectedLookupFailure`,
  `err.RejectedFileRow`, `work.RejectRoutingHistory`) are empty on the host, so the contract forces
  `PARTIAL` (source-derived baseline) at best.
* **Delta identity.** OSS Delta 3.2 (local tests) has no identity columns, so surrogate ids are
  time-ordered BIGINTs allocated in `ControlFramework.nextId` (`unix_micros << 10 | rand`). The first
  e2e run exposed that `MAX(id)+1` collides when 19 tasks insert concurrently; that is why ids are large.
* **Notifications** are job-level e-mail (`${var.operator_email}`) plus `etl_operator_notification`
  rows; there is no SMTP/Teams delivery from inside a task.
* **`databricks bundle validate` warnings**: several jobs per `resources/*.job.yml` (generated
  files; intentional) and the writable `/Workspace/Shared/...` root (mandated by the brief).
* **Bundle host** is hard-coded in `databricks.yml` because the CLI rejects interpolation on
  `workspace.host`; override with `--var`/profile when re-targeting.
