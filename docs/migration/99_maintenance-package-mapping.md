# 99_maintenance (WWI_Maintenance) -> Databricks package mapping

Legacy inputs: `ssis/99_maintenance/build_maintenance_packages.py` and the seven emitted `MNT_*.dtsx`;
operations tables in `sqlserver/control/07_tables_operations.sql`; control purge in
`sqlserver/control/procedures/etl.usp_PurgeControlHistory.sql`; weekly orchestration in
`ssis/00_orchestration/build_orchestration_packages.py::build_master_weekly_maintenance`.

Output: bundle `databricks/99_maintenance/` (job `wwi_99_maintenance`), shared helpers in
`databricks/99_maintenance/src/maintenance_lib.py`, reconciliation notebook in
`databricks/99_maintenance/validation/`, pytest suite in `databricks/99_maintenance/tests/`.

## 1. Package -> notebook / task

| Legacy package (.dtsx) | Notebook | Task key | Legacy phase (Master_Weekly_Maintenance) | Depends on |
|---|---|---|---|---|
| MNT_Check_DiskSpace | `notebooks/MNT_Check_DiskSpace.py` | `MNT_Check_DiskSpace` | Pre Flight (10) | - |
| MNT_Validate_Configuration | `notebooks/MNT_Validate_Configuration.py` | `MNT_Validate_Configuration` | Pre Flight (10) | MNT_Check_DiskSpace |
| MNT_Purge_StagingHistory | `notebooks/MNT_Purge_StagingHistory.py` | `MNT_Purge_StagingHistory` | Purge (20) | MNT_Validate_Configuration |
| MNT_Purge_ControlHistory | `notebooks/MNT_Purge_ControlHistory.py` | `MNT_Purge_ControlHistory` | Purge (20) | MNT_Purge_StagingHistory |
| MNT_Rebuild_Indexes | `notebooks/MNT_Rebuild_Indexes.py` | `MNT_Rebuild_Indexes` | Index Maintenance (30) | `Should_Rebuild_Indexes` (condition task, outcome true) |
| MNT_Update_Statistics | `notebooks/MNT_Update_Statistics.py` | `MNT_Update_Statistics` | Statistics (40) | MNT_Rebuild_Indexes + Should_Rebuild_Indexes, `run_if: ALL_DONE` |
| MNT_Archive_ProcessedFiles | `notebooks/MNT_Archive_ProcessedFiles.py` | `MNT_Archive_ProcessedFiles` | Archive (50) | MNT_Update_Statistics, `run_if: ALL_DONE` |

Every phase in the legacy master is `serialise=True`, so the job is a single chain. The legacy expression edge
`!@[User::SkipIndexRebuild] && @[User::MaintenanceWindowMinutes] >= 120` is split in two: the `SkipIndexRebuild`
half is the `Should_Rebuild_Indexes` condition task (job parameter), the window half runs inside
`MNT_Rebuild_Indexes` (exits `Skipped` and logs a `Succeeded` package execution with `rowsRead=0`). The legacy
"Completion" constraints (statistics after rebuild, archive after statistics) are `run_if: ALL_DONE`.

`Reject housekeeping` and `Maintenance notification` in the weekly master belong to projects 15 / 00 and are not
part of this job. The Saturday 22:00 schedule (`0 0 22 ? * SAT`, UTC) is declared on the job **paused** as a
reference; session 00's `Master_Weekly_Maintenance` job is the owner and triggers this job.

## 2. Source objects -> Delta targets

| Legacy object | Delta table | Written by | Notes |
|---|---|---|---|
| `etl.MaintenanceLog` | `etl.maintenance_log` | all | one row per executed OPTIMIZE / ANALYZE / summary |
| `etl.PurgeAudit` | `etl.purge_audit` | Purge_StagingHistory | one row per purged table |
| `etl.BatchArchive` | `etl.batch_archive` | Purge_ControlHistory | `BatchStatus` <- `etl.batch.Status`, `EndedAtUtc` <- `CompletedAtUtc` |
| `etl.PreflightResult` | `etl.preflight_result` | Check_DiskSpace | `CheckStatus` OK / WARNING / FAILED (legacy CRITICAL -> FAILED, per CHECK constraint) |
| `etl.BatchHold` | `etl.batch_hold` | Check_DiskSpace | `HoldReasonCode = 'DISK_SPACE'` |
| `etl.OperatorNotification` | `etl.operator_notification` | Validate_Configuration | `CONFIG_SUSPECT` warning |
| `etl.InboundFileRegister` | `etl.inbound_file_register` | Archive_ProcessedFiles (update) | stamps `IsArchived`, `ArchivePath`, `ArchivedAtUtc` |
| `etl.ArchiveExpiryList` | `etl.archive_expiry_list` | Archive_ProcessedFiles | files past `ArchiveRetentionDays` |
| `etl.LoadVolumeHistory` | `etl.load_volume_history` | read | growth projection input |
| `etl.StagingTableRegister` | `etl.staging_table_register` | read | optional; `SchemaName/TableName` are legacy names, mapped with `deltaName()` |
| `etl.RequiredConfigurationKey` | `etl.required_configuration_key` | read | |
| `etl.RegionPeriodStatus`, `etl.FxRateAvailability` | `etl.region_period_status`, `etl.fx_rate_availability` | read | plausibility checks |
| `etl.Configuration`, `etl.Batch`, `etl.BatchStep`, `etl.ErrorLog`, `etl.RejectedRecord` | `etl.configuration`, `etl.batch`, `etl.batch_step`, `etl.error_log`, `etl.rejected_record` | read / purged via `control.purgeControlHistory` | session 00 tables |
| `work.StagingPurgePlan` | `silver.work_staging_purge_plan` | Purge_StagingHistory | truncated per run |
| `work.IndexMaintenancePlan` | `silver.work_index_maintenance_plan` | Rebuild_Indexes | truncated per run |
| `work.StatisticsRefreshPlan` | `silver.work_statistics_refresh_plan` | Update_Statistics | truncated per run |
| `work.FileArchiveQueue` | `silver.work_file_archive_queue` | Archive_ProcessedFiles | truncated per run |
| `work.ConfigurationValidation` | `silver.work_configuration_validation` | Validate_Configuration | truncated per run |
| `work.VolumeSpaceCheck`, `work.VolumeGrowthProjection`, `work.VolumeShortfall` | `silver.work_volume_space_check`, `silver.work_volume_growth_projection`, `silver.work_volume_shortfall` | Check_DiskSpace | truncated per run |
| `raw.*`, `stg.*`, `err.*`, `work.*` (purge scope) | `bronze.raw_*`, `silver.stg_*`, `silver.err_*`, `silver.work_*` | Purge_StagingHistory (DELETE + VACUUM) | discovered from `information_schema.tables` |
| `sys.dm_db_index_physical_stats`, `sys.indexes` | `information_schema.tables/columns` + `DESCRIBE DETAIL` | Rebuild_Indexes | |
| `sys.dm_db_stats_properties` | `DESCRIBE HISTORY` operationMetrics | Update_Statistics | |
| `sys.master_files`, `sys.dm_os_volume_stats` | `DESCRIBE DETAIL sizeInBytes` per schema + `dbutils.fs.ls` of the landing Volume | Check_DiskSpace | |
| `C:\WWI\<ENV>\inbound\processed`, `...\archive\<yyyy>\<MM>` | `${landingVolumePath}/inbound/processed`, `${landingVolumePath}/archive/<yyyy>/<MM>` | Archive_ProcessedFiles | `landingVolumePath` default `/Volumes/${catalog}/bronze/landing` |

The `etl.*` operations tables and `silver.work_*` plan tables are created with `CREATE TABLE IF NOT EXISTS`
(legacy PascalCase columns, `GENERATED ALWAYS AS IDENTITY` for the identity keys) so the job works before/after
session 00 lays them down; if session 00 ships them with the same columns the statements are no-ops.

## 3. SSIS components -> Spark constructs

| Package | SSIS component | Spark / Databricks construct |
|---|---|---|
| Purge_StagingHistory | Execute SQL `Build Purge Plan` (StagingTableRegister + retention CASE) | `maintenance_lib.buildStagingPurgePlan` (information_schema discovery, register override, `retentionDaysFor`) -> `silver.work_staging_purge_plan` |
| | Foreach `Purge Each Staging Table` with chunked dynamic `DELETE TOP (@Chunk)` | one `DELETE FROM <table> WHERE <predicate>` per table (`purgePredicate`: load-date column when present, else `BatchId IN (batches older than cutoff)`) |
| | `Record Purge Audit` | `INSERT INTO etl.purge_audit` |
| | (none) | `VACUUM <table> [RETAIN n HOURS]` after each delete (see section 6) |
| Purge_ControlHistory | `Archive Old Batches` | `INSERT INTO etl.batch_archive ... NOT EXISTS` |
| | `Purge Batch Steps`, `Purge Batches`, `Purge Old Errors`, `Purge Resolved Rejects`, `Purge Control History Proc` | single `control.purgeControlHistory(...)` call (child-before-parent, unresolved rejects and watermarks retained) |
| Rebuild_Indexes | `Survey Fragmentation` (dm_db_index_physical_stats, page_count > 1000) | `listTables` + `DESCRIBE DETAIL` -> `fragmentationPercent` (small-file ratio vs 128 MB target), tables < 8 MB skipped |
| | `REORGANIZE` / `REBUILD` decision | `planOptimizeAction`: REORGANIZE -> `OPTIMIZE t`; REBUILD -> `OPTIMIZE t ZORDER BY (...)` (`zorderColumnsFor`), or plain `OPTIMIZE` when the table has liquid clustering |
| | Foreach `Maintain Rowstore Indexes` with per-index TRY/CATCH and `MaxDurationMinutes` deadline | loop largest-first, per-statement try/except logged to `etl.maintenance_log`, `deadlineReached` |
| | `Integration.usp_RebuildColumnstoreIndexes` | covered by the same loop over `gold.fact_*` (columnstore = Delta file layout) |
| Update_Statistics | `Build Statistics Plan` (modification_counter >= threshold) | `modifiedRowsSince(DESCRIBE HISTORY, last ANALYZE recorded in etl.maintenance_log)` |
| | `UPDATE STATISTICS ... WITH FULLSCAN` (Dim%) / `SAMPLE n PERCENT` | `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS` (gold.dim_*) / `ANALYZE TABLE ... COMPUTE STATISTICS` |
| Archive_ProcessedFiles | expression `archive\YEAR\MM`, File System `Create Archive Folder` | `archiveFolder()`, `dbutils.fs.mkdirs` |
| | `Build Archive Queue` from InboundFileRegister | `INSERT INTO silver.work_file_archive_queue` (ProcessingStatus `'Loaded'`, see section 5) |
| | Foreach File `Archive Each File` (move + `Stamp Register`) | `dbutils.fs.ls` / `fileIsSettled` / `dbutils.fs.mv` + `UPDATE etl.inbound_file_register` |
| | `Record Expired Archives` | `INSERT INTO etl.archive_expiry_list ... NOT EXISTS`; optional `dbutils.fs.rm` when `DeleteExpiredArchives=True` |
| Validate_Configuration | `Check Required Keys` (LEFT JOIN Configuration) | `etl.required_configuration_key` x `etl.configuration` (`EnvironmentCode` row wins over `'ALL'`) -> `requiredKeyStatus` |
| | `Check Connection Placeholders` (10 hard-coded keys) | same 10 keys against `etl.configuration` + secret-scope check (`dbutils.secrets.list(RequiredSecretScope)`) + job-parameter check |
| | `Check Plausible Values` (region periods / FX) | `periodPlausibility`, `fxPlausibility` against `BusinessDate` |
| | `Raise Configuration Defect` (RAISERROR 50011 -> usp_LogError) | `control.logError(errorCode=50011)` then `ConfigurationDefectError` when `FailOnMissingKey` (fails the task/job) |
| | `Warn Suspect Values` | `INSERT INTO etl.operator_notification` |
| Check_DiskSpace | Data Flow `Survey Volumes` (derived column, conditional split, multicast, 3 OLE DB destinations) | Python over `DESCRIBE DETAIL` sums + `evaluateVolume` -> `silver.work_volume_space_check`, `etl.preflight_result`, `silver.work_volume_shortfall` |
| | `Project Growth` (LoadVolumeHistory, x1.33) | `projectedGrowthBytes` -> `silver.work_volume_growth_projection` |
| | `Raise Batch Hold` | `INSERT INTO etl.batch_hold ('DISK_SPACE')` when `HoldBatchOnShortfall` |
| | `Check Tempdb Headroom` | `TEMPDB_HEADROOM` row: silver budget minus silver usage vs `MinimumFreeGbTempdb` |
| all | `Log Package Start` / `Log Package Success` / `Log Row Count`, `OnError` (`Log Error`, `Mark Execution Failed`) | see section 4 |

## 4. Control framework -> `dbx_etl_common`

| Legacy call | `dbx_etl_common` call |
|---|---|
| `etl.usp_LogPackageStart @BatchId, @PackageName, 'WWI_Maintenance'` | `control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME, projectName="WWI_Maintenance", stepName=<phase>)` |
| `etl.usp_LogPackageEnd ... 'Succeeded'` with row counters | `control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=..., rowsDeleted=..., ...)` |
| `OnError`: `etl.usp_LogError` + `etl.usp_LogPackageEnd ... 'Failed'` | `except Exception: control.logError(...); control.logPackageEnd(status="Failed"); raise` |
| `etl.usp_LogRowCount` | `control.logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=, targetRowCount=, deleteRowCount=, ...)` |
| `etl.usp_PurgeControlHistory @RetentionDays` | `control.purgeControlHistory(spark, catalog, retentionDays=None, executionHistoryMonths=13, errorHistoryMonths=6, rejectHistoryMonths=3, qualityHistoryMonths=13, whatIf=DryRun)` (months = `round(days / 30.4375)`) |
| `etl.usp_GetConfiguration` | `control.getConfiguration(spark, catalog, "MAINT_STORAGE_BUDGET_GB_<NAME>", environmentCode=...)` (storage budgets) |
| SSIS parameters | `params.getJobParams(dbutils)` for the six shared job parameters; package parameters via `maintenance_lib.widgetOr` |
| three-part names | `naming.table(catalog, schema, table)` |

`BatchId = "0"` (standalone run) is passed as `None` so `etl.package_execution.BatchId` stays NULL, as in the legacy
default `BatchId = 0` runs without a master batch.

Library reference: the job declares a serverless environment whose dependency is `${var.dbx_etl_common_wheel}`
(default `../common/dist/dbx_etl_common-0.1.0-py3-none-any.whl`, i.e. session 00's build output). Notebooks
import `from dbx_etl_common import control, naming, params` and add `../src` to `sys.path` for `maintenance_lib`.
If session 00 ships a different wheel file name, override the variable (`-var="dbx_etl_common_wheel=..."`).

## 5. Parameters

Job parameters (all strings): `BatchId` ("0"), `BusinessDate` (`${var.businessDate}`), `ReloadFullHistory`
("False", unused here), `EnvironmentCode` (`${var.environmentCode}`), `RestartFromStep` ("", the legacy master's
restart handling belongs to session 00), `catalog` (`${var.catalog}`), plus the weekly-master variables
`SkipIndexRebuild` ("False"), `MaintenanceWindowMinutes` ("240"), `DryRun` ("False"), `landingVolumePath`.

| Package | Legacy parameter (default) | Databricks task parameter | Behaviour |
|---|---|---|---|
| Purge_StagingHistory | DefaultRetentionDays (90) | same | fallback retention; `raw_fin*` 2555 d, `*partner*` 365 d, `work_*` 14 d |
| | DeleteChunkRows (50000) | same | accepted, not applicable: Delta `DELETE` is one transaction |
| | DryRun (False) | job `DryRun` | counts instead of deleting; no VACUUM |
| | - | `RunVacuum` (True), `VacuumRetentionHours` (0 = table default) | see section 6 |
| Purge_ControlHistory | BatchRetentionDays (400) / ErrorRetentionDays (180) / RejectRetentionDays (90) | same | converted to 13 / 6 / 3 months for `purgeControlHistory` |
| Rebuild_Indexes | ReorganiseThresholdPercent (10) / RebuildThresholdPercent (30) | same | thresholds on the small-file percentage |
| | MaxDurationMinutes (180) | same | deadline = `min(MaxDurationMinutes, MaintenanceWindowMinutes)` |
| | OnlineRebuild (False) | same | accepted, not applicable: `OPTIMIZE` is always online |
| | - | `Schemas` (bronze,silver,gold) | |
| Update_Statistics | ModificationThresholdRows (5000) | same | rows modified since last ANALYZE (Delta history) |
| | SamplePercent (20) | same | accepted, not applicable: `ANALYZE TABLE` has no sample clause |
| Archive_ProcessedFiles | ArchiveRetentionDays (730) / MinimumFileAgeHours (6) | same | |
| | - | `DeleteExpiredArchives` (False) | legacy could not delete; opt-in physical delete of listed files |
| Validate_Configuration | EnvironmentCode (DEV) | job `EnvironmentCode` | |
| | FailOnMissingKey (True) | same | raise -> task fails -> downstream purge does not run |
| | - | `RequiredSecretScope` (wwi), `RequiredSecretKeys` | secret keys the extract sessions reference as `{{secrets/wwi/<key>}}` |
| Check_DiskSpace | MinimumFreePercent (15) / MinimumFreeGbTempdb (50) / HoldBatchOnShortfall (True) | same | |
| | - | `StorageBudgetGb` (1024) | budget per schema / volume when no `MAINT_STORAGE_BUDGET_GB_<NAME>` configuration row exists |

## 6. VACUUM and `delta.deletedFileRetentionDuration`

`MNT_Purge_StagingHistory` frees space in two steps, unlike SQL Server where `DELETE` releases pages directly:

1. `DELETE FROM <table> WHERE <age predicate>` rewrites the affected files and logs the old ones as removed.
   Storage is **not** reclaimed yet; time travel and concurrent readers still see the old files.
2. `VACUUM <table>` physically removes files no longer referenced by any version older than the table's
   `delta.deletedFileRetentionDuration` (default `interval 7 days`). Reclaim therefore lags the purge by that
   retention, and cutting it below 7 days (`RETAIN n HOURS` with `n < retention`) requires disabling
   `spark.databricks.delta.retentionDurationCheck.enabled`, which this job **never** does: when
   `VacuumRetentionHours` is below the table setting the notebook logs the conflict to `etl.maintenance_log` and
   runs `VACUUM` with the table default instead.
3. Implications: (a) `DryRun` never vacuums; (b) tables the reconciliation notebook or restart logic may need to
   time-travel over should keep >= 7 days; (c) if a longer window is needed for a table, set
   `ALTER TABLE ... SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')` and the purge
   honours it automatically (`deletedFileRetentionHours` parses the property); (d) `VACUUM` also removes files
   written by failed/aborted writes, so it doubles as the tempdb/log cleanup the legacy package relied on
   SQL Server for; (e) `VACUUM` is not run on gold tables (not in purge scope) — OPTIMIZE'd gold tables keep
   their pre-OPTIMIZE files until a separate VACUUM policy, an open question below.

## 7. Not migrated / needs decision

| Item | Reason / proposal |
|---|---|
| Chunked deletes (`DeleteChunkRows`) | Delta has no `DELETE TOP`; one transactional delete per table. Parameter kept for compatibility, no effect. |
| `REBUILD ... ONLINE`, `SORT_IN_TEMPDB`, `MAXDOP`, fill factor | no equivalent; `OPTIMIZE` is online. `OnlineRebuild` accepted and ignored. |
| Fragmentation measure | `avg_fragmentation_in_percent` replaced by the small-file ratio (`1 - avgFileSize / 128 MB`), computed from `DESCRIBE DETAIL`. Thresholds keep their legacy defaults; **tune after the first runs**. |
| ZORDER keys | derived by rule (`zorderColumnsFor`: facts -> `*DateKey`/`BusinessDate` + `BatchId`; dims -> `WWI*ID` + `ValidFrom`; staging -> `BatchId`, `BusinessDate`). Tables session 00/other sessions create with liquid clustering (`CLUSTER BY`) are just `OPTIMIZE`d. **Decision:** should the per-table keys live in an `etl.configuration` row / register instead of the rule? |
| `SAMPLE n PERCENT` statistics | `ANALYZE TABLE` has no sample clause; non-dimension tables get table-level statistics, dims get `FOR ALL COLUMNS`. |
| Last-statistics-refresh tracking | Delta does not record ANALYZE in the table history; the last refresh is the newest `etl.maintenance_log` row for the statement. |
| `MNT_Check_DiskSpace` volumes / tempdb | cloud object storage has no fixed volume. Each schema and the landing Volume is measured against a **budget** (`MAINT_STORAGE_BUDGET_GB_<NAME>` configuration or `StorageBudgetGb`); the tempdb check becomes `TEMPDB_HEADROOM` on the silver (work) budget. **Decision:** confirm budgets per environment or drop the hold behaviour. |
| Legacy `PreflightResult` severities | legacy wrote `CRITICAL`, which the table's CHECK constraint rejects; mapped to `FAILED`. |
| Archive queue filter `ProcessingStatus = 'Processed'` | legacy value never satisfied the CHECK constraint (`Received/Validated/Loaded/Failed/Quarantined`), so the queue was always empty and the package archived every file in the processed folder. Migrated with `'Loaded'`, and the folder scan (age-based) is kept so the behaviour is a superset of legacy. **Confirm.** |
| Physical deletion of expired archives | legacy only listed them (`ArchiveExpiryList`); `DeleteExpiredArchives=False` keeps that. Set True to delete from the Volume. |
| Connection placeholders | the ten `ORACLE_*`/`SQLSERVER_*` keys are still checked in `etl.configuration`; passwords are validated as secret-scope keys (`RequiredSecretKeys`). **Decision:** final list of `wwi` scope keys once the extract sessions land. |
| Plausibility as-of date | legacy used `SYSUTCDATETIME()`; migrated to `BusinessDate` so a re-run for a past date is reproducible. |
| `Master_Weekly_Maintenance` schedule, `SkipIndexRebuild` / `MaintenanceWindowMinutes` derivation, reject housekeeping, notification | owned by session 00 / project 15. Schedule declared paused here as reference only. |
| `Integration.usp_RebuildColumnstoreIndexes` | no separate step: `gold.fact_*` are in the OPTIMIZE loop. |
| `etl.*` operations tables DDL | created idempotently here with legacy columns; if session 00's `07_tables_operations` port differs (identity vs explicit ids), adopt theirs. |
| Gold-table VACUUM policy | not in the legacy scope (SQL Server had nothing to reclaim); proposal: add gold to `RunVacuum` scope once a retention policy is agreed. |

## 8. Validation / deployment

```bash
cd databricks/99_maintenance
python3 -m py_compile notebooks/*.py src/*.py validation/*.py
DELTA_JARS=<delta-spark,delta-storage,antlr4 jars> python3 -m pytest tests -q   # or let delta-spark resolve from Maven
databricks bundle validate -t dev
# deploy (not done from the migration session): databricks bundle deploy -t dev && databricks bundle run wwi_99_maintenance -t dev
```

Reconciliation: `validation/MNT_Reconcile_Maintenance.py` counts and hashes every operations / plan / staging
table (`sum(xxhash64(concat_ws('|', ...)))` excluding identity and timestamp columns), compares with a baseline
from `BaselineTable` (Delta: `ObjectName, RowCount, RowHash[, BatchId]`) or `BaselineJson`, logs each table via
`control.logRowCount` and raises on mismatch. The SQL Server side of the baseline comes from
`validation/runtime/01_control_framework_health.sql` and `02_row_count_reconciliation.sql`.
