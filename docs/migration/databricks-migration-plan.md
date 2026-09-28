# Databricks migration plan

How the WideWorldImporters legacy estate - Oracle ERP, SQL Server OLTP, the
SQL Server staging and warehouse databases, 205 SSIS packages and the SQL Agent
schedule - moves to Databricks, pipeline by pipeline, without changing what the
business sees.

This is a design and a backlog, not a deployment. Nothing in this repository
has ever run (see `docs/known-unvalidated-items.md`), so every "current
behaviour" statement below is read from the checked-in definitions:
`ssis/orchestration-plan.json`, the package spec modules, `sqlserver/control/`,
the `Integration.usp_*` procedures and the inventories under `docs/inventories/`.
Where the target needs a fact this repository does not hold (cloud, catalog
names, storage, credentials, engine choice) it is listed in section 2 as a
decision to confirm and referenced as `D<n>` elsewhere.

Companion documents:

| Document | Holds |
| --- | --- |
| `docs/migration/child-session-backlog.md` | the seven child sessions (A-G), their prompts, acceptance criteria and the parent orchestration contract |
| `docs/migration/package-mapping-appendix.md` | every package mapped to its ingest target or Databricks task, generated from the plan and inventories by `tools/migration/build_package_mapping.py` |

Sections:

1. [Scope and principles](#1-scope-and-principles)
2. [Decisions to confirm](#2-decisions-to-confirm)
3. [Target architecture](#3-target-architecture)
4. [Source connectivity and ingestion](#4-source-connectivity-and-ingestion)
5. [Control framework replacement](#5-control-framework-replacement)
6. [Orchestration: one workflow per master](#6-orchestration-one-workflow-per-master)
7. [Transformation translation rules](#7-transformation-translation-rules)
8. [Regional rules module](#8-regional-rules-module)
9. [Known hazards: replicate, fix or defer](#9-known-hazards-replicate-fix-or-defer)
10. [Target schema and reporting layer](#10-target-schema-and-reporting-layer)
11. [Validation, parallel run and cutover](#11-validation-parallel-run-and-cutover)
12. [Phasing and dependency order](#12-phasing-and-dependency-order)

---

## 1. Scope and principles

### In scope

- The nine `Master_*` packages and the 196 packages they execute (150 under
  the nightly, the rest under the weekly, monthly and intraday masters -
  counts from `docs/inventories/ssis-packages.csv`).
- The `etl` control framework deployed twice today (staging and warehouse).
- The warehouse load procedures the packages wrap (`Integration.usp_*`, 37
  dimension and 33 fact/aggregate procedures) and the staging procedures
  (`stg.*`, `work.*`, `ref.*`).
- The SQL Agent jobs in `sqlserver/agent/` (nine masters plus the reject
  reprocessor, control-history purge and health check).
- The 16 `Report.vw_*` views the Power BI dashboards read.

### Out of scope

- Changing the Oracle ERP or the SQL Server OLTP application. They remain
  sources; only the way they are read changes.
- Re-modelling the dimensional warehouse. Dimension, fact and aggregate grain
  and column sets are preserved so the dashboards keep working (section 10).
- Data generation (`generators/`) and the SSIS deployment tooling
  (`deployment/`, `tools/ssisgen`); those are legacy build artefacts.

### Principles

1. **Same answers first, better answers second.** The migration is judged by
   parity against the legacy outputs. Every behavioural change is a listed
   hazard decision (section 9), never an incidental side effect.
2. **One control record per run.** The dual `etl` deployment goes; a single
   Delta job-control schema is the only thing that knows a run happened.
3. **The plan is the graph.** `ssis/orchestration-plan.json` is the source of
   truth for phase order, stream counts and precedence expressions, and the
   Databricks workflows are generated from it, not hand-drawn. That keeps the
   nine workflows regenerable the way the nine masters are today.
4. **Regional logic lives in one place.** The four copies of the NA/EU/APAC
   branch collapse into one module (section 8) that every layer calls.
5. **Idempotent by construction.** Every task can be re-run for the same
   batch and business date and produce the same tables, because every write
   is a `MERGE`, a replace-where on the batch/window partition, or an
   append keyed on `(batch_id, task_name)`.
6. **Generated from inventories.** Package-to-target mappings and the
   workflow definitions are derived from the checked-in inventories so the
   child sessions do not each re-read 205 `.dtsx` files.

---

## 2. Decisions to confirm

Open items the repository cannot settle. The design carries a working
assumption for each so the child sessions can start; confirming or changing a
decision changes configuration, not design.

| Id | Decision | Working assumption | Why it matters |
| --- | --- | --- | --- |
| D1 | Cloud provider and region(s) | Single cloud, single region hosting all three regional ledgers; storage account/bucket per environment | Storage paths, network path to Oracle/SQL Server, Auto Loader notification mode, private link |
| D2 | Unity Catalog layout | One catalog per environment (`wwi_dev`, `wwi_test`, `wwi_prod`), schemas per layer: `landing`, `bronze`, `stg`, `work`, `err`, `ref`, `dim`, `fact`, `agg`, `mart_finance`, `mart_sales`, `mart_inventory`, `mart_procurement`, `mart_customer360`, `report`, `ctl` | Every table name in this plan is written `{catalog}.{schema}.{table}`; a different layout is a rename |
| D3 | Oracle connectivity | JDBC over the Oracle thin driver, reading the existing `WWI_*.V_*_EXTRACT` views with the same `?`-bound watermark predicates the packages use today | Log-based CDC (Lakeflow Connect / partner CDC) is an option for `WWI_MDM` and `WWI_FIN`; `WWI_AUDIT.CHANGE_LOG` already gives delete detection so JDBC is sufficient for parity |
| D4 | SQL Server connectivity | JDBC to the OLTP `vw_*` extract views plus `CHANGETABLE` for the two change-tracked objects, exactly as `generate_sqlserver_extracts.py` does | Lakeflow Connect SQL Server connector is the managed alternative; parity first, then switch object by object |
| D5 | File landing | Cloud object storage mirroring `config/landing-zone.yaml` (`inbound/…`, `archive/…`, `quarantine/…`), mounted as a Unity Catalog external volume; Auto Loader in file-notification mode | The partner SFTP/share drop point moves or is synced to storage; decides whether `Master_File_Ingestion` stays a 15-minute poll or becomes continuous |
| D6 | Workflow engine | Lakeflow Jobs (Databricks Workflows) deployed as Declarative Automation Bundles, one job per master, `for_each`/task groups for streams | Alternative: an external scheduler (Airflow/ADF) calling jobs. The control-layer contract in section 5 is engine-neutral |
| D7 | Compute | Serverless jobs compute for orchestration and SQL; classic job clusters only where a JDBC driver or library needs it | Cost and the serverless restrictions on JDBC drivers |
| D8 | Secrets | Databricks secret scopes named per `etl.Configuration` keys `OraclePasswordSecretName`, `SqlServerPasswordSecretName`; service principal per environment | Replaces the SSIS proxy accounts and `config/.env.example` |
| D9 | Power BI connection | Dashboards repoint to a Databricks SQL warehouse via the Power BI connector, reading `{catalog}.report.*` views that keep the `Report.vw_*` names and columns | Alternative is Direct Lake / import; column names and types must not change |
| D10 | `Master_Intraday_Inventory` cadence | Every 20 minutes 05:00-22:00 (the Agent schedule in `sqlserver/agent/12_job_WWI_Intraday_Inventory.sql`) | The master's own description says "every two hours during warehouse operating hours". The Agent job is authoritative for behaviour; confirm intent |
| D11 | Hazard dispositions | As tabled in section 9 | Each row is a business decision; the table records the recommended default |
| D12 | Parallel-run length and acceptance thresholds | Two full month-ends plus one finance close in parallel; tolerances from `etl.Configuration` (`ReconAbsoluteTolerance` 0, `ReconPercentTolerance` 0.0, `MaxRejectPercent` 1 in PROD) | Section 11 |
| D13 | Retention | Keep the legacy values: control 400 days, rejects 180 days, snapshots 36 months, file archive 400 days, quarantine 90 days | Delta `VACUUM`/retention and the purge tasks in `Master_Weekly_Maintenance` |
| D14 | Reporting currency and unknown-member key | `USD`; unknown member `-1`, not-applicable `-2` (the dimension seeds), noting `etl.Configuration.UnknownMemberKey` says `0` | Static finding: the configuration row and the seeded dimensions disagree; the dimensions win |

---

## 3. Target architecture

```
  Oracle WWIGERP            SQL Server WideWorldImporters        Partner / carrier / treasury files
  (JDBC or CDC, D3)         (JDBC or Lakeflow Connect, D4)       (object storage + Auto Loader, D5)
        |                              |                                  |
        v                              v                                  v
  {catalog}.landing        raw file copies in an external volume  (inbound/, archive/, quarantine/)
  {catalog}.bronze         one Delta table per raw.* table, source-shaped, append-only, _ingest metadata
        |
        v
  {catalog}.stg            conformed, typed, deduplicated   (stg.*  -> stg.*)
  {catalog}.work           multi-pass scratch, batch-partitioned (work.* -> work.*)
  {catalog}.err            rejects by domain (err.* -> err.*), plus ctl.rejected_record
  {catalog}.ref            reference snapshots and the effective-dated rules (ref.*)
        |
        v
  {catalog}.dim            24 dimensions, SCD1/SCD2 preserved  (Dimension.* -> dim.*)
  {catalog}.fact           20 facts incl. accumulating snapshots and Fact Load Hold
  {catalog}.agg            12 aggregates
  {catalog}.mart_*         finance, sales, inventory, procurement, customer360 (ssis/10-14)
  {catalog}.report         16 views with the Report.vw_* names, gated by report.publish_state
        |
        v
  Power BI (WWI-SalesOrders.pbix, WWIDW-Sales.pbix) via Databricks SQL warehouse (D9)

  {catalog}.ctl            the job-control layer (section 5) - written by the parent workflow,
                           updated by every task, read by operators and by Session G reconciliation
```

Layer rules:

| Legacy | Target | Rule |
| --- | --- | --- |
| `raw.*` (34 tables) | `bronze.*` | Same column set plus `_source_system`, `_batch_id`, `_task_execution_id`, `_extract_window_from`, `_extract_window_to`, `_ingested_at_utc`, `_source_file` (files). Append-only; a re-run of the same window is a `replace where _batch_id = ? and _extract_window_from = ?` |
| `stg.*` (35) | `stg.*` | Same grain and columns. Written by `MERGE` on the natural key or replace-where on `_batch_id`, matching the legacy `truncate_reload` vs `incremental_append` load type per package |
| `work.*` (12) | `work.*` | Partitioned by `batch_id`; rebuilt for the batch, never truncated globally (the legacy `work_rebuild` truncates in place, which is the reason the 01:30/00:10 collision exists - section 9) |
| `err.*` (10) | `err.*` | Same shape plus `batch_id`, `task_execution_id`; one row per rejected source row |
| `ref.*` | `ref.*` | Effective-dated; the regional rules module (section 8) reads only from here |
| `Dimension.*` (24) | `dim.*` | Surrogate keys, `-1`/`-2` reserved members, both Type 2 mechanisms on `dim.customer` preserved (section 7.5) |
| `Fact.*` (20) | `fact.*` | Same grain; partition by the date key the load windows on; `fact.fact_load_hold` keeps its payload as a `STRING` (JSON) instead of XML |
| `Aggregate.*` (12) | `agg.*` | Rebuilt in full or by window exactly as the `Integration.usp_RefreshAggregate*` procedure does |
| `Report.vw_*` (16) | `report.vw_*` | Views over `agg`/`fact`/`dim` that read `report.publish_state` so an unpublished day is invisible, as `Integration.usp_PublishReportingLayer` does with `Report.PublishState` |
| `etl.*` (x2) | `ctl.*` (x1) | Section 5 |

Naming: Delta identifiers cannot carry spaces or brackets, so `Dimension.Stock Item`
becomes `dim.stock_item` and `[Tax Regime Code]` becomes `tax_regime_code`. The
`report.vw_*` views alias every column back to its bracketed display name so the
`.pbix` files bind unchanged; the alias map is generated by Session D from
`sqlserver/warehouse/**` and checked by Session G.

---

## 4. Source connectivity and ingestion

The 22 Oracle, 21 SQL Server and 7 file packages of `ssis/01`-`03` become
ingest tasks driven by a table, not 50 notebooks. Session B owns this section;
the full package-by-package table is `docs/migration/package-mapping-appendix.md`
(sections A1-A3).

### 4.1 Common shape of an ingest task

Each extract package today does the same eight things (`tools/ssisgen/patterns.py`):
log package start, `etl.usp_GetWatermark`, read the source with `?`-bound
window parameters, land into `raw.*`, `etl.usp_LogRowCount`, `etl.usp_SetWatermark`,
log success or failure. The target keeps the sequence as one generic notebook
`ingest/run_extract` parameterised from `ctl.extract_definition`:

| Column | Populated from |
| --- | --- |
| `package_name` | `docs/inventories/ssis-packages.csv` |
| `source_system_code` | `etl.SourceSystem` seed (`ORA_ERP`, `ORA_ERP_NA/EU/AP`, `WWI_OLTP`, `WWI_WEB`, `PARTNER_FL`, `CARRIER_FL`, `FX_FEED`) |
| `source_object` | `docs/inventories/source-target-map.csv` |
| `source_query` | the package's source SQL (with its `?` placeholders renamed `:watermark_from`, `:watermark_to`) |
| `load_type` | `full`, `incremental_timestamp`, `incremental_key`, `date_window`, `file_ingest` |
| `watermark_column`, `watermark_type` | `etl.Watermark` seed: `Timestamp`, `NumericKey`, `DateWindow` |
| `lookback` | `etl.Watermark.LookbackMinutes` / lookback days as seeded |
| `bronze_table` | `raw.<Table>` renamed to `bronze.<table>` |
| `delete_detection` | `WWI_AUDIT.CHANGE_LOG` (Oracle objects that record it) or `CHANGETABLE` (two OLTP objects) |
| `is_retryable` | `1` for source connectivity/timeout errors, as `ERR_Retry_FailedSteps` filters today |

The task body:

```
exec = ctl.start_task(batch_id, step_name, package_name)          # claims the step, returns task_execution_id
win  = ctl.get_watermark(source_system_code, source_object,
                         reload_full_history)                      # [from, to) with lookback applied, row locked
df   = read_source(definition, win)                                # spark.read.format("jdbc") or Auto Loader
df   = with_ingest_metadata(df, exec, win)
write_bronze(df, definition.bronze_table, exec)                    # replace-where on (_batch_id, window) -> idempotent
ctl.log_row_count(exec, source_count=source_side_count(definition, win), target_count=df.count())
ctl.set_watermark(source_system_code, source_object, win.to, exec) # refuses rewind unless allow_rewind
ctl.end_task(exec, "Succeeded")
```

Failure paths call `ctl.end_task(exec, "Failed", error, is_retryable)`; the
watermark is not advanced, so the next attempt re-reads the same window.

### 4.2 Watermark strategies

| Legacy load type | Packages | Target read | Window semantics to preserve |
| --- | --- | --- | --- |
| `full` | 9 Oracle, 7 SQL Server | Single JDBC read, land as a new `_batch_id` partition; `stg` load then does `truncate_reload` semantics with `INSERT OVERWRITE` | No watermark row; row-count audit compares source `COUNT(*)` to landed count |
| `incremental_timestamp` | 7 Oracle, 1 SQL Server (`EXT_SQL_StockItems` on temporal `ValidFrom`) | JDBC read with `>= :from and < :to`, `to` = source clock read at task start | `etl.usp_GetWatermark` subtracts `LookbackMinutes` from the stored value; late updates therefore re-land and `stg` must `MERGE`, never append |
| `incremental_key` | 4 Oracle, 13 SQL Server | Read source `MAX(key)` first, then `> :from and <= :to`; partition the JDBC read on the key column with `numPartitions` = the package's stream share | The source maximum is read *before* the extract so a mid-run insert is not skipped (`generate_sqlserver_extracts.py`); keep that order |
| `date_window` | `EXT_ORA_FxRateDaily`, `EXT_ORA_GlJournalLine`, `EXT_SQL_WebSessions` | Window `[business_date - lookback_days, business_date]`; replace-where on the date range in bronze | Re-runnable for a given window by design; `ReloadFullHistory=True` widens `from` to `WatermarkEpoch` (1900-01-01) |
| `file_ingest` | 7 | Auto Loader (`cloudFiles`) per feed with `pathGlobFilter` from the pattern in `config/landing-zone.yaml` | The watermark is the Auto Loader checkpoint plus `ctl.file_register`; section 4.5 |

Source-side filtering (`WHERE` pushed into the Oracle/SQL query) is kept in
`source_query`; Spark's JDBC reader pushes the bound window predicate down.
Denormalising joins stay in the source query for parity; Session G's
reconciliation compares landed row counts against the source-side count per
window, which is what `etl.RowCountAudit` records today.

Delete detection: `WWI_AUDIT.CHANGE_LOG` rows (`PKG_EXTRACT_CONTROL.mark_changes_extracted`)
and SQL Server `CHANGETABLE` rows land into `bronze.<table>_deletes` with the
same window; the `stg` merge applies them as `WHEN MATCHED AND _op = 'D' THEN
DELETE` or a soft-delete flag where the legacy `stg` table carries one.

### 4.3 Oracle (`ssis/01_oracle_extract`, 22 packages)

- Connection: JDBC to `WWIGERP` (D3) using the `WWI_EXTRACT` account the
  packages use, reading the `WWI_*.V_*_EXTRACT` views where the package does
  (`EXT_ORA_Geography` reads `WWI_REF.V_GEOGRAPHY_EXTRACT`) and tables elsewhere.
- Fetch size from `etl.Configuration.OracleFetchArraySize` (10000); command
  timeout `SourceQueryTimeoutSeconds` (3600).
- Ledger-scoped objects (`AP_*`, `GL_JOURNAL_LINE`, `AP_AGING_SNAPSHOT`) carry a
  `LEDGER_CODE`/region column; the source system code becomes `ORA_ERP_NA|EU|AP`
  per row so bronze carries the region without re-deriving it.
- `PKG_EXTRACT_CONTROL.begin_extract/end_extract/fail_extract` are still called
  through JDBC so the Oracle-side `EXTRACT_CONTROL` audit keeps recording;
  dropping that is a post-cutover clean-up, not a migration change.
- Lineage item L1: `docs/inventories/source-target-map.csv` maps
  `EXT_ORA_CodeTranslation` to `raw.OracleCustomerMaster` and
  `EXT_ORA_ProductHierarchy` to `raw.OracleProductMaster`. The inventory is
  generated, so this is the recorded lineage and it is carried as-is in the
  appendix, flagged for Session B to confirm against the `.dtsx` destination
  before the bronze table is named.

### 4.4 SQL Server OLTP (`ssis/02_sqlserver_extract`, 21 packages)

- Connection: JDBC to `WideWorldImporters` (D4), the `Integration.vw_*` extract
  views, `READ COMMITTED SNAPSHOT` as the packages assume.
- `EXT_SQL_StockItems` is the one temporal incremental (`ValidFrom` with lookback).
- `EXT_SQL_WebSessions` (`Ecommerce.WebSessions`) is the one date-window extract
  and the only `WWI_WEB` source; it is also the largest object, so it gets its
  own stream in the nightly and is a candidate for Lakeflow Connect first.
- Count discrepancy (static): the generator docstring and
  `docs/inventories/ssis-packages.csv` hold twenty-two SQL Server extract
  packages; `ssis/orchestration-plan.json` dispatches twenty-one.
  `EXT_SQL_LoyaltyLedger` (`Loyalty.LoyaltyPointsLedger` -> `raw.SqlLoyaltyLedger`)
  is the one no master reaches. `Fact.Loyalty Points` needs it, so the appendix
  marks it "not reached by any master - confirm" and Session B treats it as in
  scope for the nightly SQL extract phase pending that confirmation.

### 4.5 Files (`ssis/03_file_ingestion`, 7 packages)

`config/landing-zone.yaml` moves to object storage under one external volume
(D5) with the same relative layout, so the legacy folder names survive as
paths:

| Feed | Landing path and pattern | Format quirks to preserve | Bronze | Legacy package |
| --- | --- | --- | --- | --- |
| Partner sales NA | `inbound/partner/na/partner_sales_na_{yyyyMMdd}_{seq3}.csv` | windows-1252, US dates, `T` footer carrying the row count | `bronze.file_partner_sales` (`region='NA'`) | `ING_FILE_PartnerSales_NA` |
| Partner sales EU | `inbound/partner/eu/partner_sales_eu_{yyyyMMdd}_{seq3}.csv` | UTF-8, day-first dates, `9` footer carrying the amount sum | `bronze.file_partner_sales` (`region='EU'`) | `ING_FILE_PartnerSales_EU` |
| Partner sales APAC | `inbound/partner/apac/partner_sales_apac_{yyyyMMdd}_{seq3}.txt` | ISO-8859-1 although the spec says UTF-8, tab, no header; transcode, do not "fix" | `bronze.file_partner_sales` (`region='APAC'`) | `ING_FILE_PartnerSales_APAC` |
| Carrier scans | `inbound/carrier/carrier_scan_{yyyyMMdd}_{seq3}.csv` | local timestamps with no offset - site timezone applied from `dim.warehouse_site` | `bronze.file_carrier_scan` | `ING_FILE_CarrierScan` |
| Supplier catalog | `inbound/supplier/supplier_catalog_{yyyyMMdd}_{seq3}.psv` | pipe, ISO-8859-1, `TRL` footer | `bronze.file_supplier_catalog` | `ING_FILE_SupplierCatalog` |
| FX override | `inbound/treasury/fx_override_{yyyyMMdd}_{seq3}.csv` | checksum line | `bronze.file_fx_override` | `ING_FILE_FxOverride` |
| Malformed | `quarantine/{feed}/{yyyyMMdd}/{file}` | - | `err.rejected_file_row` | `ING_FILE_QuarantineMalformed` |

Design:

- **Auto Loader per feed**, `cloudFiles.format = text` for the two footer/
  checksum feeds so the footer can be validated before parsing, `csv` with
  `encoding` set for the rest. Schema is declared, not inferred (the packages
  carry the design-time column metadata); `rescuedDataColumn` captures
  anything that does not fit.
- **File register** (`ctl.file_register`): one row per file with feed, name,
  size, checksum, `received_at`, `status` (`Received`, `Screened`, `Staged`,
  `Archived`, `Quarantined`), attempts, and the footer/checksum verdict. This is
  the `etl.FileRegister` replacement and the source for the "Count Expected
  Files Missing" gate (`Ingest.ExpectedFile.%` configuration keys).
- **Structural screen** (`DQ_File_Screen`): row count vs `T`/`TRL` footer,
  amount sum vs `9` footer, checksum vs the FX checksum line, column count,
  encoding. A failed screen rejects the *file* to quarantine and logs the
  reason; a failed row goes to `err.rejected_file_row`. The generator comments
  are explicit that a structural failure must not fail the whole package.
- **Quarantine** (`ERR_Quarantine_BadFiles`): after `QuarantineAfterAttempts`
  (2) failures the file is copied to `quarantine/{feed}/{yyyyMMdd}/` and the
  register row is set `Quarantined`; retention 90 days.
- **Archive** (`MNT_Archive_ProcessedFiles`): successfully staged files move to
  `archive/{feed}/{yyyy}/{MM}/`; retention 400 days. The legacy service
  account was never granted delete on the archive share and produced a
  deletion list instead; the target lifecycle policy on the storage prefix
  replaces that list (decision folded into D13).
- **Raw payload retention**: bronze keeps the file bytes' path and checksum,
  not the bytes; the archived object is the payload of record.
- **Missing-file notification**: the gate compares `ctl.file_register` for the
  last 24 hours against the expected-file configuration; with
  `RequirePartnerFiles=False` the run proceeds and ends
  `SucceededWithWarnings` after `ERR_Notify_Operations`, exactly the plan's
  two edges.

---

## 5. Control framework replacement

Session A owns this section. It replaces `sqlserver/control/` - deployed today
to both `WideWorldImporters_Staging` and `WideWorldImportersDW`, so every
nightly run leaves two `etl.Batch` rows correlated only by
`(BatchName, BusinessDate)` - with one Delta schema, `{catalog}.ctl`, and one
Python module, `wwi_control`, that every task imports.

### 5.1 Schema

| Table | Replaces | Key | Notes |
| --- | --- | --- | --- |
| `ctl.source_system` | `etl.SourceSystem` | `source_system_code` | Seeded from `03_seed_control_data.sql` (11 codes incl. regional ledgers and their timezones) |
| `ctl.batch` | `etl.Batch` (x2) | `batch_id` (UUID) | `batch_name`, `batch_type` (`Daily`, `Intraday`, `Monthly`, `Reference`, `Maintenance`, `FileIngestion`), `business_date`, `environment_code`, `status`, `started_at_utc`, `ended_at_utc`, `restart_from_step`, `parent_run_id` (the workflow run id), `notes` |
| `ctl.batch_step` | `etl.BatchStep` | `(batch_id, step_name)` | `sequence`, `status`, `attempt_number`, `is_retryable`, `started/ended_at_utc`, `owner_task_run_id`. One row per phase node in the plan |
| `ctl.task_execution` | `etl.PackageExecution` | `task_execution_id` | `batch_id`, `step_name`, `package_name`, `attempt_number`, `status`, `rows_read/written/rejected`, `error_message`, `task_run_id`, `notebook_path` |
| `ctl.watermark` | `etl.Watermark` | `(source_system_code, object_name)` | `watermark_type`, `watermark_value`, `previous_watermark_value`, `lookback_minutes`, `lookback_days`, `locked_by_task_execution_id`, `locked_at_utc`, `last_advanced_by_task_execution_id` |
| `ctl.watermark_history` | (none - new) | append | Every advance, refused rewind and full-reload override; needed because Delta rows cannot be row-locked and operators need to see who moved what |
| `ctl.row_count_audit` | `etl.RowCountAudit` | append | `task_execution_id`, `object_name`, `source_row_count`, `target_row_count`, `rejected_row_count`, `variance`, `within_tolerance` |
| `ctl.reconciliation_exemption` | `etl.ReconciliationExemption` | `object_name` | The ten seeded exemptions (`work.CustomerDedup`, the SCD2 dimensions, the aggregates, `Fact.Stock Holding`, `Fact.Order Fulfilment`) |
| `ctl.reconciliation_result` | `etl.ReconciliationResult` | append | Output of the `reconcile` plan node and of `ERR_Reconcile_RowCounts` |
| `ctl.error_log` | `etl.ErrorLog` | append | `source_name`, `severity`, `error_message`, `error_payload` (JSON), `batch_id`, `task_execution_id` |
| `ctl.rejected_record` | `etl.RejectedRecord` | `rejected_record_id` | `batch_id`, `object_name`, `reject_stage` (`Extract`, `Stage`, `Quality`, `Dimension`, `Fact`), `reject_reason_code`, `source_key`, `payload` (JSON), `attempt_count`, `is_reprocessed`, `reprocessed_batch_id`, `expires_at_utc` |
| `ctl.dq_rule`, `ctl.dq_result` | `04_tables_data_quality.sql`, `05_seed_data_quality_rules.sql` | rule code / append | Rule definitions seeded from the legacy seed; results per batch, rule and object |
| `ctl.configuration` | `etl.Configuration` | `(configuration_key, environment_code)` | Seeded values from `03_seed_control_data.sql`; resolution order `environment_code` then `ALL`, as the plan's `ORDER BY EnvironmentCode DESC` does |
| `ctl.file_register` | `etl.FileRegister` and the operations tables | `file_register_id` | Section 4.5 |
| `ctl.notification` | `etl.NotificationQueue` | append | Written by `ERR_Notify_Operations`; delivered by the workflow's notification destinations, not by Database Mail |
| `ctl.retry_queue` | the `ERR_Retry_FailedSteps` query over `etl.BatchStep` | view | `SELECT ... FROM ctl.batch_step WHERE batch_id = ? AND status = 'Failed' AND is_retryable AND attempt_number < max_attempts` |
| `ctl.fact_rekey_queue` | `work.FactRekeyQueue`, `Fact.Fact Load Hold` | `queue_id` | Late-arriving members and held early-arriving facts; read by `DIM_Rekey_LateArriving`, `FACT_Apply_Corrections`, `DQ_Reject_Reprocess` |
| `ctl.batch_archive`, `ctl.error_log_archive` | `etl.*Archive` | append | Target of `MNT_Purge_ControlHistory`; alternatively Delta time travel plus a `retention` table property - Session A picks one |

All `ctl` tables are Delta with change data feed enabled so Session G can audit
every status transition without a trigger.

### 5.2 Module API (`wwi_control`)

```
start_batch(batch_name, batch_type, business_date, environment_code,
            allow_adopt_running=False, restart_from_step=None, notes=None) -> Batch
    # etl.usp_StartBatch: refuses a second Running batch for the same
    # (batch_name, business_date) unless allow_adopt_running; adoption
    # appends "adopted by run <run_id>" to notes and re-uses the batch_id.
end_batch(batch_id, force_status=None)
    # etl.usp_EndBatch: Succeeded if every step Succeeded/Skipped, Failed if
    # any Failed, else SucceededWithWarnings; force_status overrides
    # (the plan's End Batch With Warnings node).
claim_step(batch_id, step_name, sequence) -> BatchStep
    # idempotent upsert; if restart_from_step is set and sequence < the
    # restart step's sequence, returns status Skipped without running.
complete_step(batch_id, step_name, status)                      # Succeeded | Failed | Skipped
start_task(batch_id, step_name, package_name) -> TaskExecution  # attempt_number = prior + 1
end_task(task_execution_id, status, error=None, is_retryable=None, rows=None)
get_watermark(source_system_code, object_name, reload_full_history=False,
              task_execution_id=None) -> Window(from, to)
set_watermark(source_system_code, object_name, to, task_execution_id, allow_rewind=False)
log_row_count(task_execution_id, object_name, source_count, target_count, rejected=0)
log_error(source_name, severity, message, payload=None, batch_id=None, task_execution_id=None)
reject(batch_id, task_execution_id, object_name, stage, reason_code, source_key, payload)
assert_row_count_reconciliation(batch_id, raise_on_failure) -> ReconciliationResult
    # etl.usp_AssertRowCountReconciliation with the tolerance keys and exemptions
config(key, environment_code) -> str
retryable_steps(batch_id, max_attempts) -> list[BatchStep]
```

The module is a Python wheel deployed with the bundle (D6) and also exposed as
Databricks SQL functions/procedures for the pure-SQL tasks (aggregates, views).

### 5.3 Semantics to preserve

| Legacy behaviour | Where | Target |
| --- | --- | --- |
| Single running batch per `(batch_name, business_date)`; adoption on recovery | `etl.usp_StartBatch` `@AllowAdoptRunning` | `start_batch` does a `MERGE` into `ctl.batch` under a Delta transaction; concurrent parent runs see the conflict and the loser reads the winner's `batch_id`. Adoption is a parent-job parameter, set by the operator on a manual re-run |
| Parent creates the batch; children only update steps/tasks | Master packages create; child packages `usp_StartPackage/usp_EndPackage` | The parent workflow's first task calls `start_batch` and publishes `batch_id` as a task value; every child task receives it as a parameter and never creates a batch |
| `RestartFromStep` | Master parameter, `etl.usp_GetBatchStepStatus` | `claim_step` skips steps ordered before the named step and marks them `Skipped`; the parent's task graph is unchanged, so restart is a re-run of the same job with two parameters (`BatchId` of the failed run, `RestartFromStep`) |
| Bounded retry | `MaxExtractAttempts` (3), `ERR_Retry_FailedSteps.MaxRetryAttempts` (3), backoff grows with attempt | Task-level `max_retries` in the job definition for transient failures **and** a `retry_driver` task after each extract phase that calls `retryable_steps` and re-dispatches the failed packages in a serial stream with `BaseBackoffSeconds * attempt` - the plan's `Increment Extract Attempt -> Extract Retry` edge |
| Watermark window `[from, to)` with lookback; row created on first use from `WatermarkEpoch` | `etl.usp_GetWatermark` | Same; `to` is read from the source clock (`SYSTIMESTAMP` / `SYSUTCDATETIME()`) or source `MAX(key)` at task start |
| Lock while extracting | `UPDLOCK` in `usp_GetWatermark` | `locked_by_task_execution_id` set in the same `MERGE`; a second claimant with a different task fails fast with `WatermarkLocked`; locks expire after `SourceQueryTimeoutSeconds` |
| Previous value kept; rewind refused with a warning | `etl.usp_SetWatermark` | `previous_watermark_value` plus `ctl.watermark_history`; rewind requires `allow_rewind=True`, is logged `Warning`, and only `Master_Weekly_Reference_Load` (`Force Full Reload`) and an operator run pass it |
| Row-count audit and reconciliation gate | `etl.usp_LogRowCount`, `etl.usp_AssertRowCountReconciliation`, exemptions | Same tolerances, same exemption list; the `reconcile` plan node keeps its `raise_on_failure` flag per master (1 for Daily and Month End, 0 for Finance Close) |
| Health check every 30 minutes: stuck batches, duration outliers, error/reject rates, watermark drift | `sqlserver/agent/21_job_WWI_Health_Check.sql` | A `ctl_health_check` job on the same cadence writing to `ctl.error_log` and the notification destination; the SQL ports directly |
| Control history purge, watermarks never purged | `MNT_Purge_ControlHistory`, `20_job_WWI_Control_History_Purge.sql` | `ctl_purge` task in `Master_Weekly_Maintenance`: archive then delete beyond `ControlTableRetentionDays` (400) / `RejectRetentionDays` (180); `ctl.watermark` excluded |

### 5.4 Exactly-once boundaries

- **Bronze**: at-least-once from the source, exactly-once in the table -
  every write is `replace where (_batch_id, _extract_window_from)` or an
  Auto Loader checkpointed append, so a retried task overwrites its own
  partial write.
- **Silver/gold**: exactly-once per `(batch_id, business_date)` because every
  loader is a `MERGE` on the natural or surrogate key or a replace-where on the
  load window; running a step twice for the same batch is a no-op.
- **Control**: `ctl.task_execution` gets a new row per attempt (that is the
  audit trail); `ctl.batch_step` is upserted; `ctl.watermark` moves forward
  only after the bronze write commits.
- **Notifications**: at-least-once; `ctl.notification` is idempotent on
  `(batch_id, step_name, notification_kind)`.

### 5.5 Structured error and reject payloads

`ctl.error_log.error_payload` and `ctl.rejected_record.payload` are JSON with
a fixed envelope (`schema_version`, `source_system_code`, `object_name`,
`source_key`, `columns` (the offending row), `rule_code`, `attempt`,
`task_execution_id`). The legacy `err.*` tables keep their per-domain shape;
`ctl.rejected_record` is the steward's single queue, as
`ERR_Route_RejectedRows` intends.

---

## 6. Orchestration: one workflow per master

Session F owns this section. Each `Master_*` package becomes one parent job
(D6) generated from `ssis/orchestration-plan.json` by a builder that plays the
role `build_orchestration_packages.py` plays today.

### 6.1 Mapping rules from the plan to a job

| Plan element | Job element |
| --- | --- |
| root `parameters` | job parameters with the same names and defaults (`BatchId`, `BusinessDate`, `EnvironmentCode`, `ReloadFullHistory`, `MaxParallelStreams`, `MaxExtractAttempts`, `RestartFromStep`, plus the master-specific ones) |
| `batch_start` node | task `start_batch` -> `wwi_control.start_batch`; publishes `batch_id` as a task value; `BatchId != 0` means adopt/restart |
| `phase` node with `children` and `streams` | a task group: `claim_step` -> `for_each` over the child package list with `concurrency = min(streams, MaxParallelStreams)` -> `complete_step`. Children in the same phase run in parallel up to the stream count; the legacy package assigns children to streams round-robin, which a `for_each` reproduces |
| `phase` node with `in_package_children` | same, but the child is a notebook task in the parent bundle rather than a separate job |
| `control` node, `query` | a SQL task against `ctl.*`, result published as a task value (`PeriodStatus`, `RunningBatches`, `MissingFileCount`, `UnmappedCodeCount`, `DedupeCandidateCount`, `CorrectionCount`, `SubledgerVariance`, `OnHandVariance`) |
| `control` node, `expression` | a Python task mutating a task value (`ExtractAttempt`) |
| `reconcile` node | `assert_row_count_reconciliation(batch_id, raise_on_failure)` |
| `batch_end` node | `end_batch`; `force_status` carried through |
| edge `Success` | `depends_on` with `run_if: ALL_SUCCESS` |
| edge `Completion` | `depends_on` with `run_if: ALL_DONE` |
| edge `Failure` | `depends_on` with `run_if: ALL_FAILED` (or `AT_LEAST_ONE_FAILED` where the source has several predecessors) |
| edge with `expr` | an `if/else` condition task evaluating the expression over task values; the plan's `Test-PlanConditionalEdge.ps1` rule - an edge is taken only when the outcome *and* the expression hold - is preserved by chaining the condition after the `run_if` |
| `MaxParallelStreams` | the ceiling for every `for_each` concurrency in the job |
| `RestartFromStep` | see 5.3; the graph does not change, skipped steps finish in seconds |
| SQL Agent schedule | job schedule (cron) with the same times and enabled flags; the Agent job's own retry (`@retry_attempts = 2, @retry_interval = 15` on the nightly) becomes job-level `max_retries` on the `start_batch` root |
| SQL Agent failure step (`etl.usp_LogError` + notify) | job notification destination plus the `ERR_Notify_Operations` task the plan already includes |

Generic child job `run_package`: one job definition, parameterised by
`package_name`, that looks the package up in `ctl.package_registry` (generated
from `docs/inventories/ssis-packages.csv`) and runs the mapped notebook. Every
package in the appendix has a notebook path there. This keeps the parent job
graph identical to the plan's and lets a package be re-run alone by hand, as
`dtexec` allowed.

### 6.2 The nine masters

Schedules are from `sqlserver/agent/1x_job_*.sql`; they are checked-in intent,
not observed runs.

#### Master_Daily_ETL - nightly 01:30, `Daily`, 150 packages

Phase sequence, streams and the tasks that carry the loop-breakers:

```
start_batch
-> Extract Oracle       (22 pkgs, 4 streams)   -| failure -> Increment Extract Attempt
-> Extract SQL Server   (21 pkgs, 3 streams)   -|   -> Extract Retry (ERR_Retry_FailedSteps, serial)
                                                     while ExtractAttempt <= MaxExtractAttempts
-> File Screen (DQ_File_Screen) -> File Quarantine (ERR_Quarantine_BadFiles)
-> Stage Load           (24 pkgs, 4 streams)
-> Stage Work Tables    (4 pkgs: CustomerDedup, ProductCrosswalk, PaymentMatch, InventoryPosition)
-> Data Quality         (7 screens, 2 streams)
-> Referential Screen   (DQ_Referential_Screen)
-> Reject Routing       (ERR_Route_RejectedRows)
-> Referential Rescreen (DQ_Referential_Screen, second run)      <- loop-breaker, see below
-> Reference Refresh
-> Dimensions           (15 pkgs, 3 streams)
-> Facts                (23 pkgs, 4 streams)
-> Aggregates
-> Sales Mart | Inventory Mart | Procurement Mart (parallel groups)
-> Customer 360 Build -> Customer 360 Publish
-> Publish Reporting Layer (AGG_Publish_ReportingLayer)
-> Reconcile Row Counts (raise_on_failure = 1)
-> end_batch  |  failure path: ERR_Notify_Operations -> end_batch(Failed)
```

- **Loop-breaker.** The dependency map shows a cycle through
  `DQ_Referential_Screen -> ERR_Route_RejectedRows -> DQ_Referential_Screen`.
  It is intentional and bounded: the screen runs exactly twice per batch,
  the second pass is scoped `WHERE batch_id = ? AND is_reprocessed = 0`, and
  rows still failing after the rescreen stay in `ctl.rejected_record` for
  `DQ_Reject_Reprocess` (up to five retries over thirty days). In the job this
  is two distinct tasks, `referential_screen` and `referential_rescreen`, with
  no edge back - the graph is acyclic by construction and the bound is
  structural.
- **Extract retry.** `retry_driver` runs after both extract phases with
  `run_if: AT_LEAST_ONE_FAILED`, reads `retryable_steps(batch_id,
  MaxExtractAttempts)`, re-dispatches serially with backoff, increments
  `ExtractAttempt`; when attempts are exhausted the run takes the failure path.
  This also fixes the orphaned-retry finding in the dependency map: today
  `ERR_Retry_FailedSteps` is reachable only from the 00:10 Agent job.
- **Ordering guarantees carried into the graph**: Customer 360 build before
  publish; every fact, aggregate and mart before `publish_reporting_layer`;
  the reconcile node after publish so a reconciliation failure fails the batch
  but does not un-publish (the publish task itself flips
  `report.publish_state` only after its own validation - section 10).
- **Concurrency**: the job has `max_concurrent_runs = 1`; the hourly and
  intraday jobs check `ctl.batch` for a running `Daily` batch (below).

#### Master_Customer_Sync - daily 00:15, `Daily`

```
start_batch -> Customer Extract (5 pkgs, 2 streams; retry loop on the two Oracle customer extracts)
-> Customer Stage (3) -> Identity Resolution (STG_Work_CustomerDedup)
-> Count Merge Candidates (DUP_CANDIDATE rejects for work.CustomerDedup)
     > 500 -> Customer Sync Escalation (ERR_Route_RejectedRows, ERR_Notify_Operations) -> end_batch
     else  -> Customer Screen (DQ_Customer_Screen, DQ_Referential_Screen)
-> Regional Customer Dimensions (DIM_NA/EU/APAC_Load_Customer, 3 streams)
-> Customer Support Dimensions (CustomerCategory, CustomerSegment, City)
-> Customer Mart Build (C360_Build_CustomerProfile, C360_Build_LoyaltyOverlay)
-> Customer Mart Publish (C360_Publish_Segments) -> end_batch
```

Parameters `SuppressConsentWithdrawn` (True) and `DedupeThresholdScore` (85)
are job parameters passed to the dedup notebook and to the regional rules
module (section 8). Note the 00:15 start precedes the 01:30 nightly, which
re-runs the customer extracts with the same watermarks; the lookback makes
that harmless but it doubles the Oracle read - hazard H6.

#### Master_Hourly_Incremental - hourly 05:00-23:00, `Intraday`

The first task runs the plan's query (`COUNT(*) FROM ctl.batch WHERE batch_type =
'Daily' AND status = 'Running'`); with `SkipWhenNightlyRunning=True` and a
running nightly the job logs `Information` and ends without a batch. Otherwise:
Incremental Extract (orders, invoices, shipments) -> Incremental Stage ->
Intraday Screen -> Intraday Facts (`Completion` edge: facts load even if the
screen rejected rows) -> Intraday Aggregate (`AGG_Refresh_DailySalesSummary`)
-> end_batch; failure -> Intraday Failure Notice.

The Agent job's self-disabling step (three failures in four hours disables
the schedule) becomes a job-level alert plus a `ctl.configuration` flag
`Hourly.Paused` the first task honours; pausing a Databricks schedule from
inside a run is possible via the Jobs API but an explicit flag is auditable.

#### Master_Intraday_Inventory - every 20 minutes 05:00-22:00 (D10), `Intraday`

Inventory Extract (StockItems, StockMovements, StockTransfers; retry on
StockMovements) -> Inventory Stage (incl. `STG_Work_InventoryPosition`) ->
Inventory Facts (`FACT_Load_Movement`, `FACT_Load_StockHolding`) -> Inventory
Marts (StockTransfer, CycleCountVariance, `INV_Reconcile_OnHand`) -> Read On
Hand Variance -> if `PickingWindowOpen && variance <= OnHandVarianceTolerance`
(25) -> Replenishment -> Inventory Health -> end_batch; if variance exceeds
tolerance -> On Hand Variance Escalation. `PickingWindowOpen` is a job
parameter today; the target reads it from `ctl.configuration` keyed by site
timezone so the 20-minute schedule does not need two job definitions.

#### Master_Finance_Close - monthly, day 1 05:00, `Monthly` - **Agent job disabled**

```
start_batch -> Read Period Status (ctl.configuration 'Finance.PeriodStatus')
   == Closed -> Close Escalation -> end_batch(SucceededWithWarnings)
   != Closed -> Subledger Loads (ApAging, WithholdingTax, CostAllocation; 2 streams)
             -> FX Revaluation -> General Ledger (FIN_Load_GlPostings)
             -> Subledger Tie Out (FIN_Reconcile_SubledgerToGl)
             -> Read Subledger Variance (row-count variance on Fact.Payment, Fact.GL Posting)
                  == 0 or AllowCloseWithVariance -> Close Aggregates (2) -> Period Lock -> reconcile(0) -> end_batch
                  else -> Close Escalation -> end_batch(SucceededWithWarnings)
```

`FIN_Close_PeriodLock` locks the three ledgers separately (NA and EU calendar
month, APAC 4-4-5), so the gate reads and writes `GL_PERIOD_STATUS` per ledger
through the fiscal-calendar interface of the regional module. The Databricks
job is created with `pause_status: PAUSED` to mirror the disabled Agent job;
enabling it is an operational decision, not a migration step.

#### Master_Month_End - daily 04:00 schedule, `Monthly` - **Agent job disabled**, calendar-gated in the job step

Count Pending Corrections -> (>0) Fact Corrections (`FACT_Apply_Corrections`,
`FACT_Dedup_Sale`) -> Sales Aggregates (4, 2 streams) -> Operations Aggregates
(3, 3 streams) | Finance Aggregates -> Customer Aggregates (2) -> Customer Mart
Refresh (`C360_Build_RollingMetrics`, `C360_Build_ChurnFlags`) -> Customer
Segment Publish -> Month End Housekeeping -> reconcile(1) -> end_batch. The
Agent job step evaluates the calendar (first business day) before executing;
the job keeps a `calendar_gate` first task reading `dim.date`/`dim.fiscal_calendar`
and is deployed paused. Section 9 (H9) records that this flow is defined but
never enabled.

#### Master_Weekly_Reference_Load - Sunday 03:00, `Reference`

`Force Full Reload` (sets `ReloadFullHistory` for the reference extracts and
passes `allow_rewind`) -> Geography And Sites -> Currency And Terms (3 streams)
| Commercial Codes (6 pkgs, 2 streams) -> Calendar -> Code Translation
(`REF_Load_CodeTranslation`, `REF_Load_UnknownMembers`) -> Count Unmapped Codes
-> (0) Late Arriving Rekey (`DIM_Rekey_LateArriving`) -> end_batch; (>0) Unmapped
Code Escalation -> `FailOnCodeMapGap` ? Failed : SucceededWithWarnings. This is
the only master allowed to move a watermark backwards.

#### Master_Weekly_Maintenance - Saturday 22:00, `Maintenance`

Pre Flight (`MNT_Check_DiskSpace` -> storage/quota check, `MNT_Validate_Configuration`)
-> Purge (staging history, control history) -> if `!SkipIndexRebuild &&
MaintenanceWindowMinutes >= 120` Index Maintenance else skip -> Statistics ->
Archive (`MNT_Archive_ProcessedFiles`) -> Reject Housekeeping
(`ERR_Route_RejectedRows`, `ERR_Reconcile_RowCounts`) -> end_batch. The SQL
Server actions map to Delta maintenance: `MNT_Rebuild_Indexes` and
`Integration.usp_RebuildColumnstoreIndexes` -> `OPTIMIZE` (with liquid
clustering or `ZORDER` on the fact date keys), `MNT_Update_Statistics` ->
`ANALYZE TABLE ... COMPUTE STATISTICS`, purge -> `VACUUM` with the D13
retention; `MaintenanceWindowMinutes` bounds the `OPTIMIZE` task's timeout.

#### Master_File_Ingestion - every 15 minutes, `FileIngestion`

Count Expected Files Missing -> (0 or `!RequirePartnerFiles`) Partner Drops (3
streams) -> Carrier And Catalog (2 streams) -> Quarantine
(`ING_FILE_QuarantineMalformed`, `ERR_Quarantine_BadFiles`) -> File Screen ->
File Stage (`STG_Load_PartnerSale`, `STG_Load_Currency`) -> Archive Files ->
end_batch; missing files -> Missing File Notice -> end_batch(SucceededWithWarnings).
With Auto Loader the 15-minute poll can become a continuous or
`availableNow` trigger (D5); the batch and file-register semantics are the
same either way.

### 6.3 Operational jobs outside the masters

| Legacy Agent job | Target |
| --- | --- |
| `WWI - Reject Reprocess` every 4 h from 00:10 (`DQ_Reject_Reprocess`, `ERR_Retry_FailedSteps`, `ERR_Route_RejectedRows`) | `ops_reject_reprocess` job; disposition of the 01:30 collision is hazard H1 |
| `WWI - Control History Purge` Sunday 05:00 | folded into `Master_Weekly_Maintenance` purge task; a standalone job is kept only if D13 wants a different cadence |
| `WWI - Health Check` every 30 min | `ops_health_check` job (section 5.3) |
| Operators, categories, proxies | job notification destinations and a service principal per environment (D8) |

### 6.4 Concurrency rules

- `Master_Daily_ETL`, `Master_Month_End`, `Master_Finance_Close`: `max_concurrent_runs = 1` each, and mutually exclusive through `start_batch` (same `batch_type` family, same business date).
- `Master_Hourly_Incremental` and `Master_Intraday_Inventory` stand down when a `Daily` batch is running (the hourly does so today; the intraday inventory does not, and the plan adds the same check - H7).
- `Master_File_Ingestion` runs concurrently with everything; it writes only bronze, `stg.partner_sale`, `stg.currency` and the file register.
- `ops_reject_reprocess` must not run while `Master_Daily_ETL` is in its Stage/DQ phases (H1).

---

## 7. Transformation translation rules

Sessions C, D and E apply these rules; the rules are written once here so the
three sessions produce code that looks the same.

### 7.1 SSIS data-flow components to PySpark

| SSIS component (as emitted by `tools/ssisgen`) | Target | Rule |
| --- | --- | --- |
| OLE DB / ADO.NET / Oracle source with `?` parameters | `spark.read.format("jdbc")` with the query as `dbtable` subquery and bound window values | Keep the SQL text; only the placeholder syntax changes. Source-side `WHERE` stays server-side |
| Flat File source (`ING_FILE_*`) | Auto Loader | Section 4.5; column list from the package's design-time metadata |
| OLE DB destination fast-load into `raw.*`/`stg.*` | `df.write.format("delta")` with `replaceWhere` on the batch/window, or `DeltaTable.merge` | `DefaultBatchSize` is irrelevant; commit granularity is the task |
| Derived Column | `withColumn` / `selectExpr` | Expressions that encode a regional branch (`REGION == "EU" ? … : …`) are replaced by a call into the regional module (section 8), never re-typed inline |
| Lookup (full cache) against `ref.*`/`dim.*` | broadcast `join` on the lookup key; no-match rows routed by `left_anti`/`when(col.isNull())` | Lookup "no match -> redirect row" becomes a reject write to `err.*` and `ctl.reject`; "no match -> unknown member" becomes `coalesce(key, lit(-1))` |
| Lookup (partial/no cache) | ordinary join; if the lookup table is large, `MERGE`-style join on the natural key | |
| Conditional Split | `filter` per output; the default output is `filter(~any_of_the_others)` | Every output of the split is materialised as a named DataFrame so reject counts are auditable |
| Union All | `unionByName(allowMissingColumns=False)` | Column order differences are a legacy hazard the explicit name match removes |
| Sort + remove duplicates | `Window.partitionBy(key).orderBy(deterministic columns)` + `row_number() == 1` | The ordering must be fully deterministic (add the source key as the last sort column) or parity tests will flap |
| Aggregate | `groupBy().agg()` | |
| Multicast | reuse the same DataFrame (cache if written twice) | |
| Row Count into a variable, then `etl.usp_LogRowCount` | `ctl.log_row_count(...)` with `df.count()` on the written DataFrame, or the `MERGE` metrics (`numTargetRowsInserted/Updated/Deleted`) from the Delta commit | Prefer commit metrics: they are exact and free |
| Script component | Python UDF only when a `pandas_udf` or SQL expression cannot express it | Each is listed in the appendix as `manual_review` |
| Execute SQL Task calling `stg.usp_*` / `Integration.usp_*` | the procedure is *re-implemented* as a notebook (sections 7.3-7.6); the notebook is called from the task | The T-SQL body is the specification; the procedure is not ported as SQL scripting except for aggregates and views |
| Execute SQL Task calling `etl.usp_*` | `wwi_control.*` | Section 5 |
| Foreach Loop over files | Auto Loader input list / `for_each` task | |
| Precedence constraints with expressions | condition tasks | Section 6.1 |
| Event handlers (`OnError` -> `etl.usp_LogError`) | `try/except` in the notebook shell that every package notebook uses (`wwi_control.task_context`) | |

Every notebook has the same skeleton:

```
with wwi_control.task_context(batch_id, step_name, package_name) as t:   # start_task / end_task / log_error
    definition = registry.lookup(package_name)
    inputs = read_inputs(definition, t)          # bronze/stg/ref tables filtered to the batch or window
    outputs = transform(inputs, t, rules)        # pure function of inputs + regional rules + configuration
    write_outputs(outputs, definition, t)        # MERGE / replaceWhere, then row counts from commit metrics
```

`transform` is a pure function so it can be unit-tested on small DataFrames
without Delta or a workspace.

### 7.2 Idempotency and partitioning

| Layer | Idempotency key | Partition / clustering |
| --- | --- | --- |
| bronze | `(_batch_id, _extract_window_from)` replace-where; Auto Loader checkpoint for files | `_ingested_date`; cluster by the watermark column |
| stg (`truncate_reload`) | `INSERT OVERWRITE` whole table per batch | none needed; small |
| stg (`incremental_append`) | `MERGE` on the natural key, `WHEN MATCHED AND source is newer` | cluster by natural key |
| work | replace-where `batch_id = ?` | `batch_id` |
| err | append with `(batch_id, task_execution_id, source_key)` uniqueness | `batch_id` |
| dim (SCD1) | `MERGE` on natural key | none |
| dim (SCD2) | `MERGE` on `(natural_key, is_current)` with the expire-and-insert pattern | none |
| fact (incremental) | delete-by-window then insert, or `MERGE` on the degenerate key, per the legacy procedure | date key of the window |
| fact (accumulating snapshot) | `MERGE` on the process key (order line, shipment line, PO line) | first-milestone date key |
| fact (periodic snapshot) | replace-where `snapshot_date_key = ?` | `snapshot_date_key` |
| fact (rebuild-in-full) | `INSERT OVERWRITE` | month key |
| agg | replace-where on the refreshed window or full overwrite, per procedure | as the procedure |
| report | views; `report.publish_state` row per business date | - |

### 7.3 Staging (`ssis/04_staging`, 28 packages, Session C)

- 12 `truncate_reload` packages -> overwrite `stg.*` from the latest bronze
  batch partition; 12 `incremental_append` -> `MERGE`; 4 `work_rebuild` ->
  dedicated notebooks below. The spec module `build_staging_packages.py` is the
  column contract; the generator's `Column` declarations become the PySpark
  `StructType` per table, produced by a script, not by hand.
- Type conformance (`stg.*` typed vs `raw.*` strings) uses `try_cast`; a
  failed cast is a reject with `reason_code = 'TYPE_CONVERSION'`, matching the
  legacy error output of the OLE DB destination.
- Work tables:

| Legacy | Specification | Target notebook |
| --- | --- | --- |
| `STG_Work_CustomerDedup` -> `stg.usp_DeduplicateCustomer` | Three match rules in priority order (`EXACT_TAXNUM`, `NAME_POSTAL`, `NAME_FUZZY` on the first 12 standardised characters plus country, only when neither has a tax number); survivorship score = source rank (`ORA_ERP` 30, `WWI_OLTP` 20, `WWI_WEB` 10) + 2 per populated significant attribute + 10 for recency; EU explicit opt-in never loses; candidates below `DedupeThresholdScore` (85) are rejected `DUP_CANDIDATE` for stewardship | `work/customer_dedup`: three self-joins on standardised keys, `Window` over the match group ordered by score desc with deterministic tie-break, survivor plus `work.customer_dedup_map(survivor_key, merged_key, rule, score)`; consent rule and standardisation come from the regional module |
| `STG_Work_ProductCrosswalk` | Joins Oracle `PRODUCT_MASTER`, OLTP `StockItems` and the supplier catalog feed on SKU/barcode/supplier reference; unmatched products get an inferred crosswalk row | `work/product_crosswalk`: sequential left joins, `coalesce` priority, unmatched -> `ctl.reject('CROSSWALK_UNMATCHED')` and inferred member (7.5) |
| `STG_Work_PaymentMatch` | Matches receipts to invoices per regional instrument (NA lockbox, EU SEPA structured reference, APAC per-bank formats) with unmatched EU references to an unallocated bucket, not the reject queue | `work/payment_match`: matcher per instrument selected by the regional module; the "unallocated bucket" is a `stg.cash_allocation` row with `allocation_status='UNALLOCATED'` |
| `STG_Work_InventoryPosition` | Rebuilds the on-hand position by site and item from movements and transfers for the intraday cycle | `work/inventory_position`: window sum over movements ordered by movement key, partition by batch; consumed by `FACT_Load_StockHolding` and `INV_Reconcile_OnHand` |

### 7.4 Data quality, referential screening and rejects (`ssis/05`, `06`, `15`, Session C)

- `ctl.dq_rule` seeds the rules from `05_seed_data_quality_rules.sql`; each
  `DQ_*_Screen` notebook applies the rules for its object as boolean columns,
  writes `ctl.dq_result` (rule, passed, failed, sampled keys) and rejects failing
  rows to `err.*` plus `ctl.rejected_record`. `MaxRejectPercent` (5 / PROD 1)
  fails the task when exceeded, exactly as `etl.usp_AssertRejectThreshold`.
- `DQ_Referential_Screen` checks every foreign key in the batch against `dim`
  (current rows) and `ref`; failures are `LOOKUP_FAILURE` rejects.
  `ERR_Route_RejectedRows` copies `err.*` rows for the batch to the error
  volume path and registers them; the rescreen re-evaluates only
  `is_reprocessed = 0` rejects of this batch (section 6.2).
- `DQ_Reject_Reprocess` (the 4-hourly job) replays `LOOKUP_FAILURE` rejects
  whose dimension member has since arrived, at most five attempts over thirty
  days, into `stg.order_line` and marks them reprocessed; aged-out rows stay
  for stewardship.
- `DQ_Threshold_Gate` -> `assert_row_count_reconciliation`.
- Reference (`ssis/06_reference_data`, 14 `full_refresh` packages) -> `ref.*`
  overwritten from bronze; `REF_Load_CodeTranslation` -> `ref.code_crosswalk`
  with `ref.usp_ReportUnmappedCodes` becoming a view `ref.vw_unmapped_source_code`
  and the `CODE_UNMAPPED` reject; `REF_Load_UnknownMembers` ->
  `Integration.usp_EnsureUnknownMembers` as `dim/ensure_unknown_members`.

### 7.5 Dimensions (`ssis/07_dimensions`, 15 packages; `Integration.usp_MigrateStaged*`, Session D)

- **Surrogate keys**: `Integration.usp_AllocateDimensionKeyRange` allocates
  ranges from `Dimension.KeyRegistry`; the target keeps a `dim.key_registry`
  table and allocates a range per task with a `MERGE` (a monotonically
  increasing `BIGINT` per dimension, never `monotonically_increasing_id`, so
  keys are stable across re-runs). Reserved `-1` Unknown and `-2` Not
  Applicable per dimension (D14).
- **SCD2** (11 packages): the `MERGE` pattern - match on natural key where
  `is_current`, `WHEN MATCHED AND type2_hash <> source_hash` -> expire
  (`valid_to = business_date`, `is_current = false`) then insert the new
  version; `WHEN MATCHED AND type1_hash <> source_hash` -> update in place on
  all versions. Same-day changes use the `effective_sequence` column
  `usp_MigrateStagedCustomerDataV2` keeps.
- **`dim.customer` specifics**: Type 2 on trading name, bill-to, category,
  buying group, postal code, city, tax registration, credit-limit band; Type 1
  on contact, consent, phone, website, standard discount. Loaded by three
  regional packages (`DIM_NA/EU/APAC_Load_Customer`) that today diverge in the
  derived columns; the target is one notebook parameterised by region with the
  differences in the regional module (postal standardisation, consent
  filtering with `SuppressConsentWithdrawn`, retention anonymisation).
- **SCD1** (3): plain `MERGE`.
- **Inferred members** (`usp_InsertInferredMember`, `usp_EnrichInferredMembers`):
  a fact referencing an unknown natural key inserts a row with
  `is_inferred = true` and the natural key only; the next dimension load
  enriches it in place (Type 1 for the first arrival, then normal SCD2).
- **Early-arriving facts**: `Fact.Fact Load Hold` holds rows for
  `EarlyArrivingFactHoldDays` (3) before the inferred member is created;
  target `ctl.fact_rekey_queue` with `hold_until`.
- **Late-arriving rekey** (`DIM_Rekey_LateArriving`, `usp_RekeyLateArrivingDimensions`):
  within `LateArrivingDimensionDays` (7) re-points facts from the inferred key
  to the real key; abandons after the retry limit with a `ctl.error_log`
  warning. Target: a `MERGE` from the queue into each fact on the degenerate
  key, per fact, in the weekly reference job and the daily dimensions phase.
- Bridges (`Customer Buying Group`, `Employee Territory`), the junk dimension
  (`Order Status`), the mini-dimension (`Customer Demographic`), `Date`/`Time`
  population and role-playing date views port as SQL notebooks; they are
  deterministic rebuilds.

### 7.6 Facts (`ssis/08_facts`, 23 packages; `Integration.usp_LoadFact*`, Session D)

| Pattern | Facts | Legacy rule to preserve | Target |
| --- | --- | --- | --- |
| Incremental, delete-by-window then insert | `Fact.Sale` (three regional packages + `FACT_Dedup_Sale`), `Fact.Order`, `Fact.Purchase`, `Fact.Purchase Receipt`, `Fact.Return`, `Fact.Credit Note`, `Fact.Customer Transaction`, `Fact.Supplier Ledger`, `Fact.Loyalty Points`, `Fact.Web Session`, `Fact.Movement`, `Fact.GL Posting`, `Fact.Promotion Eligibility`, `Fact.Salesperson Territory Coverage` | The window is the extract window plus back-dating allowance (late invoice posting); rows outside the window are untouched | `replaceWhere` on the date-key window, one commit per fact per batch |
| Correction by reversal/restatement | `Fact.Sale` | A restated source invoice line produces a `REV` row (negated original) and a `RES` row; the original row is kept for finance audit | `FACT_Apply_Corrections` -> `fact/apply_corrections` reads `ctl.fact_rekey_queue` and appends the pair; never updates |
| Correction in place | `Fact.Payment`, `Fact.Order` | `restatement_version` incremented on the same row (credit control re-allocates cash for weeks) | `MERGE` on the degenerate key with `WHEN MATCHED` update |
| Accumulating snapshot | `Fact.Shipment` (milestone date keys and lag measures as tracking events arrive), `Fact.Order Fulfilment` (order/pick/despatch/delivery/invoice/cash milestones, open-milestone count), `Fact.Procure To Pay` (requisition/approval/PO/receipt/invoice/payment, one row per PO line) | One row per process instance, updated as milestones land; exempt from row-count reconciliation | `MERGE` on the process key: `WHEN MATCHED` set only the milestone columns that are null or later, recompute lags; `WHEN NOT MATCHED` insert. The join across the six milestone sources is done once per batch over the *open* rows plus the batch's new rows, not the whole fact |
| Periodic snapshot | `Fact.Stock Holding` (daily), `Fact.Daily Snapshots`, `Fact.Monthly Snapshots`, `Fact.Monthly Customer Balance` | Row count independent of source; `SnapshotRetentionMonths` (36) | replace-where on the snapshot date key; retention enforced by the maintenance purge |
| Rebuild in full | `Fact.Sales Margin` | Derived from sale, movement/cost, credit note and return; cost basis differs by region (NA weighted average, EU FIFO, APAC standard cost + PPV) and is recorded in `cost_basis_code`; the price/volume/mix/cost bridge absorbs the residual into mix | `INSERT OVERWRITE` per period from the four source facts; the cost-basis method is a regional-module interface (`cost_basis(region)`), the bridge maths ports as-is |
| Deduplication | `FACT_Dedup_Sale` -> `usp_DeduplicateFactSale` | Removes duplicate sale lines introduced by the three regional loaders overlapping on cross-region customers | `Window` over the degenerate key ordered deterministically; run after the three regional loads in the Facts phase and again in Month End |

Fact loads always join to `dim` on `(natural_key, valid_from <= event_date < valid_to)`
for SCD2 dimensions and to the current row for SCD1, exactly as the procedures
do; the join is a `range join` hint on the validity interval.

### 7.7 Aggregates and marts (`ssis/09`-`14`, Sessions D and E)

- 12 `Integration.usp_RefreshAggregate*` -> SQL notebooks in `agg`, rebuilt for
  the window the procedure rebuilds (daily sales: business date; monthly sales
  and margin analysis: the month; customer 360 and rolling 12 month: full;
  finance close summary: the accounting period per ledger).
- `AGG_Publish_ReportingLayer` -> section 10.
- Marts (`business_rule` load type, 28 packages) -> one notebook each in
  `mart_<domain>`, reading `fact`/`dim`/`agg` and writing mart tables.
  Finance keeps the close gate (`FIN_*` only run inside `Master_Finance_Close`,
  ledger period status from the regional fiscal interface); Customer 360 keeps
  the produce/consume split - `C360_Build_*` write `mart_customer360.*` tables,
  `C360_Publish_Segments` reads them and writes the published segment table
  (EU customers without marketing consent land in the suppressed segment);
  `SLS_Export_PartnerFeed` and `PRC_Export_SupplierStatement` write outbound
  files to the outbound volume with the EU personal-data stripping from the
  regional module.

---

## 8. Regional rules module

Today the NA/EU/APAC branch is written at least four times: Oracle
`WWI_FIN.PKG_TAX` / `WWI_REF.PKG_FX` / `WWI_REF.PKG_CODE_TRANSLATION`, the
`stg.usp_*` procedures, the SSIS derived-column expressions in the regional
packages (`DIM_{NA,EU,APAC}_Load_Customer`, `FACT_{NA,EU,APAC}_Load_Sale`,
`ING_FILE_PartnerSales_*`) and the warehouse procedures
(`usp_RefreshAggregateCustomer360` privacy rules, `usp_LoadFactPayment`
instruments, `usp_RefreshAggregateMarginAnalysis` cost basis,
`FIN_Close_PeriodLock` calendars). The target has one module, `wwi_rules`,
owned by Session C (built early because B, D and E call it) with one
configuration table set in `ref`.

### 8.1 Configuration

| Table | Content | Source of the seed |
| --- | --- | --- |
| `ref.region` | `NA`, `EU`, `APAC`; default currency, ledger code, source system code, timezone | `ref.usp_LoadRegion`, `etl.SourceSystem` |
| `ref.tax_jurisdiction`, `ref.tax_rate` | jurisdiction resolution and rates, effective-dated, `tax_regime_code` (`SALES_USE`, `VAT`, `GST_INCLUSIVE`), reverse-charge flag, withholding flag | `ref.usp_LoadTaxJurisdiction`, `EXT_ORA_TaxRate`, `PKG_TAX` |
| `ref.fx_rate_daily`, `ref.fx_override` | rates by pair and effective date, month-end rate flag, override precedence | `EXT_ORA_FxRateDaily`, `ING_FILE_FxOverride`, `PKG_FX` |
| `ref.fiscal_calendar` | per ledger: calendar month (NA, EU) or 4-4-5 (APAC); period status | `Dimension.Fiscal Calendar`, `GL_PERIOD_STATUS` |
| `ref.postal_format_rule` | postal/address shape per country | `ref.usp_LoadPostalFormatRule` |
| `ref.consent_policy`, `ref.retention_policy` | consent required (APAC explicit, EU explicit for marketing), retention days, anonymisation columns | hard-coded in `usp_RefreshAggregateCustomer360`, `C360_Publish_Segments`, `SLS_Export_PartnerFeed` |
| `ref.code_crosswalk` | source code -> conformed code, per source system and code set, effective-dated | `WWI_REF.CODE_TRANSLATION`, `ref.usp_LoadCodeCrosswalk` |
| `ref.cost_basis_policy` | `NA` weighted average, `EU` FIFO, `APAC` standard cost + PPV | `usp_RefreshAggregateMarginAnalysis` |
| `ref.payment_instrument_policy` | lockbox / SEPA / APAC bank formats and the unallocated-bucket rule | `usp_LoadFactPayment`, `STG_Work_PaymentMatch` |
| `ref.rule_version` | `rule_set_version`, `effective_from`, `effective_to`, approved by | new |

All rows carry `effective_from`/`effective_to` and `rule_set_version`; a
transformation asks for the rules "as at" the business date (or as at the
event date for facts) so a re-run for an old date reproduces the old answer.

### 8.2 Interfaces

```
rules = wwi_rules.for_batch(business_date, rule_set_version=None)   # None -> current

rules.tax.determine(df, region_col, jurisdiction_cols, amount_col, event_date_col)
    # adds tax_regime_code, tax_rate, tax_amount, is_reverse_charge, is_price_inclusive,
    # withholding_amount (APAC); mirrors PKG_TAX.determine_tax / resolve_jurisdiction /
    # resolve_rate / is_reverse_charge / validate_vat_id
rules.fx.convert(df, amount_col, from_ccy_col, event_date_col, to_ccy='USD', month_end=False)
    # PKG_FX.get_rate / month_end_rate / round_to_minor_unit; override feed precedence;
    # check_rate_freshness -> FxRateTolerancePercent warning
rules.fiscal.period_for(df, ledger_col, date_col)  /  rules.fiscal.period_status(ledger, period)
    # calendar month vs 4-4-5; GL_PERIOD_STATUS read/write for FIN_Close_PeriodLock
rules.address.standardise(df, region_col, address_cols)
    # postal shape per country; used by dedup and dim.customer
rules.consent.filter(df, region_col, consent_cols, suppress_consent_withdrawn: bool)
    # SuppressConsentWithdrawn job parameter; APAC no-consent == opted out
rules.retention.apply(df, region_col, retention_date_col)
    # EU blanking + anonymised flag, APAC shorter window, NA keep
rules.codes.translate(df, source_system_col, code_set, code_col, on_unmapped='reject'|'passthrough'|'unknown')
    # PKG_CODE_TRANSLATION.translate / reverse_translate; unmapped handling is hazard H3
rules.cost_basis(region) -> 'WAVG' | 'FIFO' | 'STD_PPV'
rules.payments.matcher(region) -> callable
rules.dedup.threshold -> DedupeThresholdScore (job parameter, default 85)
```

Precedence and fallback are explicit and identical everywhere: (1) an
override row (FX override feed, manual jurisdiction override) as at the date,
(2) the region-specific rule, (3) the `GLOBAL` rule, (4) the configured
`on_unmapped` action. There is no implicit "else NA" branch; a row whose
region is not one of the three is a `REGION_UNKNOWN` reject, which is exactly
the failure mode `validation/runtime/04_regional_divergence.sql` was written
to catch.

### 8.3 Tests

- Unit tests per interface on hand-built DataFrames (one case per region per
  rule, plus boundary dates around an effective-date change and a 4-4-5 period
  edge).
- Parity tests: the 04_regional_divergence queries ported to Databricks SQL and
  run against the legacy and target outputs during the parallel run
  (Session G).
- Contract test: every notebook that references a region literal (`'EU'`)
  outside `wwi_rules` fails a lint rule; this is how the four copies stay
  collapsed.

---

## 9. Known hazards: replicate, fix or defer

Recommended dispositions (D11). "Replicate" means the target reproduces the
legacy behaviour and the parity tests expect it; "Fix" means the target
behaviour changes and the parity tests carry an approved difference;
"Defer" means replicate now with a follow-up ticket.

| Id | Hazard | Current behaviour (static) | Impact | Proposed target | Compatibility | Validation | Owner | Decision needed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| H1 | 01:30 nightly vs 00:10 four-hourly reject reprocessor | `WWI - Reject Reprocess` runs 00:10, 04:10, 08:10 …; the 04:10 run overlaps the nightly's Stage/DQ phases; `DQ_Reject_Reprocess` replays into `stg.OrderLine` while `STG_Load_OrderLine`/`work_rebuild` truncate and reload it, and `ERR_Retry_FailedSteps` can re-run extract steps the nightly is retrying | Lost or duplicated replays, double extract attempts, non-deterministic reject counts | **Fix**: `ops_reject_reprocess.start_batch` refuses to start while a `Daily` batch is `Running` (same check the hourly does) and re-queues; `work.*` is batch-partitioned so a replay can never truncate the nightly's scratch | Replay timing shifts by up to one nightly duration; reject aging (five attempts / thirty days) unchanged | Parallel run: replay counts per reject equal between estates over 30 days; a synthetic overlap test proves the stand-down | F (gate), C (replay) | Confirm fix over replicate |
| H2 | Dual control identities | `etl` is deployed to both databases; each nightly writes a staging `etl.Batch` and a warehouse `etl.Batch`, correlated only by `(BatchName, BusinessDate)`; `20_job_WWI_Control_History_Purge.sql` purges both | Reconciliation across the two is by convention; restart logic reads whichever database the package connects to | **Fix**: one `ctl.batch` per run (section 5); the legacy pair is mapped to it during parallel run by `(batch_name, business_date)` for comparison only | None for consumers; operators lose the two-row view and gain one | Session G's control-framework health queries (`01_control_framework_health.sql`) ported to `ctl` show one row per run | A | None - inherent to the design |
| H3 | Incomplete `CODE_TRANSLATION` fallthrough | `PKG_CODE_TRANSLATION.translate` returns the source value when no mapping exists; downstream `CASE` branches then default (often to the NA branch); `REF_Load_CodeTranslation` counts `CODE_UNMAPPED` rejects and `FailOnCodeMapGap` decides whether the weekly fails | Silent mis-coding, discovered only by `ref.vw_UnmappedSourceCode` | **Fix, gated**: `rules.codes.translate(on_unmapped=...)` defaults to `reject` in the weekly reference load (so `FailOnCodeMapGap` semantics hold) and to `unknown` (-1 / `UNMAPPED`) in fact and dimension loads, never `passthrough`; a `passthrough` mode exists only for parallel-run comparison | Rows that today land with a source code will land as `UNMAPPED`; downstream reports show an `UNMAPPED` bucket instead of a wrong bucket | Parallel run diff of every crosswalk-driven column; count of `UNMAPPED` per code set trended to zero as mappings are added | C | Confirm `unknown` over `passthrough` in facts; agree the mapping backlog owner |
| H4 | Merged/retired parties in `MDM_MERGE_HISTORY` without fact repair | Oracle records party merges; `stg.usp_DeduplicateCustomer` produces a survivor map per batch; no procedure rewrites existing fact rows from the retired party key to the survivor; `Dimension.Customer` keeps both members | Customer 360 and segment publish split one customer across two keys; history is not comparable after a merge | **Defer** (replicate now): keep both keys and the map; add `work.customer_dedup_map` as a persisted, effective-dated bridge and a `report.vw_Customer360` option to roll retired keys up to the survivor. A fact rekey job is specified as a follow-up because rewriting `Fact.Sale` history conflicts with the reversal-row audit rule | No change at cutover; the roll-up view is additive | Parity of Customer 360 rows per key; count of retired keys with post-merge facts reported weekly | D (bridge), E (view) | Whether finance accepts rekeyed history or the roll-up view |
| H5 | `EXT_ORA_CodeTranslation` and `EXT_ORA_ProductHierarchy` land into another object's `raw` table per the inventory (L1) | Recorded lineage: `raw.OracleCustomerMaster` and `raw.OracleProductMaster` | Bronze table naming and reconciliation | Replicate the recorded lineage in the appendix, flagged; Session B confirms against the `.dtsx` before naming bronze | - | Source-to-target coverage check | B | None unless the `.dtsx` disagrees |
| H6 | `Master_Customer_Sync` at 00:15 and `Master_Daily_ETL` at 01:30 both extract the Oracle customer objects | Same watermarks, so the nightly re-reads the lookback window | Double Oracle read, benign | Replicate | - | Row counts | F | None |
| H7 | `Master_Intraday_Inventory` has no nightly stand-down | Only the hourly checks for a running `Daily` batch | Intraday movement fact loads can overlap the nightly Facts phase | **Fix**: same stand-down check as the hourly | Intraday refresh pauses during the nightly window | Overlap test | F | Confirm |
| H8 | Hourly job disables its own schedule after three failures in four hours | `sp_update_job @enabled = 0` inside the failure step | Silent outage until an operator re-enables | Replicate as an explicit `ctl.configuration` pause flag plus alert | Same operational effect, visible in `ctl` | Fault-injection test | F | None |
| H9 | `Master_Month_End` and `Master_Finance_Close` Agent jobs are disabled | `@enabled = 0` in `17_` and `18_`; the packages and calendar gate exist | Month-end aggregates and the close have never been scheduled to run | Replicate: jobs deployed paused; enabling is a business decision | - | Dry run in TEST against a synthetic period | F, E | Whether to enable at cutover |
| H10 | `UnknownMemberKey` configuration says `0`, dimension seeds use `-1`/`-2` | Two definitions of "unknown" | A loader reading the configuration key would mis-key | Fix: `-1`/`-2` (D14), configuration row corrected | None visible | Unit test | A, D | Confirm D14 |
| H11 | Snapshot and accumulating facts are exempt from row-count reconciliation by name (`etl.ReconciliationExemption`) | Exemptions are correct but broad | A genuinely wrong snapshot passes the gate | Replicate the exemption list; add a snapshot-specific check (row count vs `dim.warehouse_site x dim.stock_item` active pairs) as a warning | None | Session G check | G | None |
| H12 | Cadence conflict for `Master_Intraday_Inventory` | Agent: every 20 min; plan description: every two hours | Wrong compute profile or missed refresh | Follow the Agent schedule (D10) | - | - | F | Confirm D10 |

---

## 10. Target schema and reporting layer

### 10.1 Layer mapping

| Legacy database.schema | Objects | Target schema | Session |
| --- | --- | --- | --- |
| `WideWorldImporters_Staging.raw` | 34 tables | `bronze` | B |
| `…Staging.stg` | 35 | `stg` | C |
| `…Staging.work` | 12 | `work` | C |
| `…Staging.err` | 10 | `err` | C |
| `…Staging.ref` | 12 tables + `ref.usp_Load*` | `ref` | C |
| `…Staging.etl` and `WideWorldImportersDW.etl` | control | `ctl` | A |
| `WideWorldImportersDW.Dimension` | 24 dimensions, 2 bridges, key registry, unknown members, role-playing date views | `dim` | D |
| `…DW.Fact` | 20 facts incl. `Fact Load Hold`, daily/monthly snapshots | `fact` | D |
| `…DW.Aggregate` | 12 | `agg` | D |
| `…DW.Integration` (procedures, `vw_*`) | 70 procedures | notebooks under `dim/`, `fact/`, `agg/`; `Integration.vw_*` extract views stay on the source | D |
| `ssis/10_finance` outputs | 7 | `mart_finance` | E |
| `ssis/11_sales` | 6 | `mart_sales` (+ outbound partner feed volume) | E |
| `ssis/12_inventory` | 5 | `mart_inventory` | E |
| `ssis/13_procurement` | 5 (+ supplier statement volume) | `mart_procurement` | E |
| `ssis/14_customer_360` | 5 | `mart_customer360` | E |
| `…DW.Report` | 16 views + `Report.PublishState` | `report` | D (views), G (compatibility) |

### 10.2 Reporting layer and Power BI

`Integration.usp_PublishReportingLayer` refreshes the aggregates in dependency
order, evaluates the publish rules, and flips `Report.PublishState` for the
business date only when every rule passes, so BI never sees a partial day.
The target keeps that contract:

- `report.publish_state (business_date, is_published, published_at_utc, batch_id, rule_results)`.
- Every `report.vw_*` view joins to `report.publish_state` (or filters on the
  latest published date) so an unpublished batch is invisible even though the
  `agg`/`fact` tables already hold its rows.
- The publish task is the last data task in `Master_Daily_ETL`, after every
  fact, aggregate and mart; it writes the rule results to
  `ctl.reconciliation_result` and only then upserts `publish_state`.
- Views alias columns to the bracketed legacy names (`[Sale Key]` -> `` `Sale Key` ``)
  and keep the `Report.vw_*` names as `report.vw_*`; the `.pbix` data source
  changes from SQL Server to the Databricks connector (D9) with no query change.
  Where Power BI reads `Dimension.*`/`Fact.*` directly (the sample
  `WWIDW-Sales.pbix` does), compatibility views in `report` expose them under
  the legacy names too.
- `AGG_Publish_ReportingLayer`'s dependency-ordered refresh becomes the task
  dependency order of the Aggregates phase; the publish task itself only
  validates and flips state.

---

## 11. Validation, parallel run and cutover

Session G owns the detail (`child-session-backlog.md`, Session G). Summary:

1. **Static**: `validation/static/run_all_checks.py` and
   `validation/checks/*` keep guarding the legacy definitions; a new
   `check_migration_coverage.py` asserts every package in
   `docs/inventories/ssis-packages.csv` has a row in the appendix with a
   notebook path and a target table.
2. **Runtime checks ported**: `validation/runtime/01`-`04` rewritten for `ctl`,
   `dim`, `fact` and the regional module; `ERR_Reconcile_RowCounts` and
   `etl.usp_AssertRowCountReconciliation` are the same function
   (`assert_row_count_reconciliation`), so the reconciliation that runs inside
   every batch is the reconciliation the acceptance gate reads.
3. **Source-to-target reconciliation**: per bronze table and window, source
   count vs landed count (from `ctl.row_count_audit`); per `stg`/`dim`/`fact`
   table, legacy row count and hash of the business columns vs target for the
   same business date, using `docs/inventories/source-target-map.csv` to pair
   objects.
4. **Regional parity**: the 04 queries as Databricks SQL, run on both estates,
   compared per region.
5. **Parallel run** (D12): both estates run from the same sources for the
   agreed window; the legacy remains the system of record; daily diff report
   from Session G's notebooks; a hazard-approved difference list (H1, H3, H7,
   H10) is the only permitted variance.
6. **Acceptance gates**: zero unexplained row-count variance across
   `ReconAbsoluteTolerance`/`ReconPercentTolerance`; every `report.vw_*` view
   matches the legacy view for the last published date; every Power BI page
   renders with equal totals; watermark positions reconcile per object;
   `ctl` health check clean for seven consecutive days.
7. **Cutover**: freeze legacy watermarks, final parallel diff, switch the
   Power BI data sources, disable the Agent jobs, enable the Databricks
   schedules with the legacy cadences. **Rollback**: re-enable the Agent jobs
   and repoint Power BI; the legacy estate has kept running so no data
   back-fill is needed within the parallel-run window.
8. **Operational sign-off**: runbook for each of the nine jobs (restart from
   step, adopt running batch, force full reload, manual replay), the health
   check dashboard on `ctl`, and on-call notification routing.

---

## 12. Phasing and dependency order

```
Wave 0  Decisions D1-D9 confirmed; catalog/schemas/volumes created; source connectivity proven with one JDBC read each
Wave 1  Session A (control)  ──┐
        Session C part 1 (wwi_rules + ref seeds) ──┐   both are libraries the rest import
Wave 2  Session B (ingestion)  ─┐                 │
        Session C part 2 (stg/work/DQ) ─ needs A, rules, and B's bronze contract
Wave 3  Session D (dim/fact/agg) ─ needs C's stg/work/ref contract
Wave 4  Session E (marts) ─ needs D
Wave 5  Session F (workflows) ─ can start from the plan in Wave 1 and wire tasks as B-E land notebooks
Wave 6  Session G (validation) ─ static coverage from Wave 1; runtime reconciliation from Wave 3; parallel run after Wave 5
```

Contracts between sessions are table schemas and the two module APIs
(`wwi_control`, `wwi_rules`), which is why A and the rules half of C go first
and why every session's acceptance criteria include "contract tests pass
against the published schema". The appendix and the package registry are the
shared index the parent session uses to dispatch and to check completeness.
