# 05_data_quality (WWI_DataQuality) -> Databricks package mapping

Session 05 of the SSIS -> Databricks migration. Source: `ssis/05_data_quality/` (generator
`build_quality_packages.py`, 10 `DQ_*.dtsx`), rules seeded by
`sqlserver/control/05_seed_data_quality_rules.sql` into the tables of `04_tables_data_quality.sql`.
Output: bundle `databricks/05_data_quality/` (job `wwi_05_data_quality`) and this document.

Design choice: **quarantine-enabled notebooks, not a Lakeflow Declarative Pipeline**, for every screen.
The legacy screens do not materialise a cleaned table: they *read* a staged/bronze table already loaded by
the `STG_*` / `ING_FILE_*` projects, route offenders to `err.*` tables with reason codes, write measure
rows to `etl.DataQualityResult`, log rejects through `etl.usp_LogRejectedRecordSet` and raise warning /
failure gates on package variables. `@dlt.expect_or_drop` would need a new managed target table per
screen (changing the estate's table contract) and cannot express ordered conditional-split routing,
lookup error outputs, aggregate-based checks (supplier duplicate groups, invoice tax variance per invoice)
or the `FailedRuleCount` / `MeasuredValue` gate variables. The rule text is therefore executed
unchanged as Spark SQL predicates against the Delta objects (see "Rule text") and the screens replicate
each data flow with DataFrame ops.

## 1. Package -> notebook / task

| Package (task_key) | Notebook | Job phase (orchestration-plan step) | depends_on | Purpose |
|---|---|---|---|---|
| `DQ_File_Screen` | `notebooks/DQ_File_Screen.py` | File Screen | – | Structural screen of `raw.FilePartnerSales` (delimiter count, unparsable date / amount, high-bit chars) -> `err.RejectedFileRow` |
| `DQ_Rule_Engine` | `notebooks/DQ_Rule_Engine.py` | Data Quality | DQ_File_Screen | Evaluates every active `etl.DataQualityRule` -> `etl.DataQualityResult`; `FailedRuleCount` gates |
| `DQ_Customer_Screen` | `notebooks/DQ_Customer_Screen.py` | Data Quality | DQ_Rule_Engine | `stg.Customer` null / consent / credit / country screens -> `err.RejectedCustomer` |
| `DQ_Supplier_Screen` | `notebooks/DQ_Supplier_Screen.py` | Data Quality | DQ_Rule_Engine | Normalised tax-id duplicate groups -> `err.RejectedSupplier` |
| `DQ_OrderLine_Screen` | `notebooks/DQ_OrderLine_Screen.py` | Data Quality | DQ_Rule_Engine, DQ_Customer_Screen | Quantity / price / amount screens, customer lookup error output -> `err.RejectedOrderLine`, `err.RejectedLookupFailure` |
| `DQ_InvoiceLine_Screen` | `notebooks/DQ_InvoiceLine_Screen.py` | Data Quality | DQ_Rule_Engine | Tax recomputation and per-invoice variance -> `err.RejectedInvoiceLine` |
| `DQ_Payment_Screen` | `notebooks/DQ_Payment_Screen.py` | Data Quality | DQ_Rule_Engine, DQ_Supplier_Screen | Unmatched / orphan payment screens against `work.PaymentMatched` -> `err.RejectedPayment` |
| `DQ_Threshold_Gate` | `notebooks/DQ_Threshold_Gate.py` | Data Quality | all five `DQ_*_Screen` (run_if ALL_DONE) | Batch reject rate, control-total reconciliation, scorecard; publishes `gatePassed` task value |
| `DQ_Gate_Check` (condition_task) | – | Data Quality | DQ_Threshold_Gate | `{{tasks.DQ_Threshold_Gate.values.gatePassed}} == "true"` – the `RejectedRowCount` gate of the plan |
| `DQ_Referential_Screen` | `notebooks/DQ_Referential_Screen.py` | Referential Screen | DQ_Gate_Check (outcome true) | Staging -> reference / dimension joins -> `err.RejectedLookupFailure` |
| `DQ_Reject_Reprocess` | `notebooks/DQ_Reject_Reprocess.py` | Reject Routing (+ Referential Rescreen) | DQ_Referential_Screen | Replays resolvable lookup rejects into `stg.OrderLine`, marks rejects RESOLVED / ABANDONED / RETRY, re-runs the referential screen with `ScreenScope=Referential Rescreen` |

Edges come from `docs/inventories/package-dependencies.csv` (rows 19-21: Referential -> Reprocess ->
OrderLine / Referential; 139-143: ING_FILE_* -> DQ_File_Screen; 416-452: STG_* -> screens) and the
phase order of `ssis/orchestration-plan.json` (File Screen -> Data Quality -> Referential Screen ->
Reject Routing -> Referential Rescreen). Screens that share a staging object in the legacy plan run in
parallel; the two extra edges (Customer -> OrderLine, Supplier -> Payment) keep the reject tables of the
parent entity populated before the child screen so the lookup error outputs see the same picture the
legacy serial master saw. Upstream cross-project edges (`STG_*`, `ING_FILE_*`) are wired by session 00's
master job; the file-screen task is the entry point of this job.

Other masters reuse packages under different step names (`Intraday Screen`, `Customer Screen`); those
steps are honoured by `RestartFromStep` only for this job's own step names (see §6).

## 2. Objects: legacy -> Delta

| Legacy | Delta (`${catalog}.`…) | Used by |
|---|---|---|
| `raw.FilePartnerSales` | `bronze.raw_file_partner_sales` | File screen (source) |
| `stg.Customer` | `silver.stg_customer` | Customer screen, rules |
| `stg.Supplier` | `silver.stg_supplier` | Supplier screen, rules |
| `stg.Order`, `stg.OrderLine` | `silver.stg_order`, `silver.stg_order_line` | OrderLine screen, referential, reprocess (MERGE target), rules |
| `stg.Sale`, `stg.SaleLine` | `silver.stg_sale`, `silver.stg_sale_line` | InvoiceLine screen, referential, rules |
| `stg.Payment`, `work.PaymentMatched` | `silver.stg_payment`, `silver.work_payment_matched` | Payment screen, rules |
| `stg.StockItem`, `stg.SalesTerritory` | `silver.stg_stock_item`, `silver.stg_sales_territory` | Referential screen, reprocess |
| `ref.Country`, `ref.Currency`, `ref.PackageType` | `silver.ref_country`, `silver.ref_currency`, `silver.ref_package_type` | Customer screen, referential screen (see §7 for `ref.PackageType`) |
| `err.RejectedCustomer` / `RejectedSupplier` / `RejectedOrderLine` / `RejectedInvoiceLine` / `RejectedPayment` / `RejectedFileRow` | `silver.err_rejected_customer` … `silver.err_rejected_file_row` | quarantine targets of the screens |
| `err.RejectedLookupFailure` | `silver.err_rejected_lookup_failure` | OrderLine lookup miss, referential screen, reprocess |
| `err.RejectedConstraintViolation` | `silver.err_rejected_constraint_violation` | rule engine (rules that cannot be evaluated), threshold gate (reconciliation breaches) |
| `etl.DataQualityRule` / `DataQualityRuleException` / `DataQualityResult` | `etl.data_quality_rule` / `etl.data_quality_rule_exception` / `etl.data_quality_result` | rule engine, every screen (measure rows), threshold gate |
| `etl.RowCountAudit` | `etl.row_count_log` (contract name; `etl.row_count_audit` accepted as fallback) | threshold gate |
| `etl.ReconciliationResult` | `etl.reconciliation_result` | threshold gate |
| `etl.PackageExecution`, `etl.ErrorLog`, `etl.RejectedRecord` | `etl.package_execution`, … | via `dbx_etl_common.control` |

Every reference goes through `dq_quality.naming_map.deltaTable(catalog, legacyName)` which applies the
mapping above with `dbx_etl_common.naming.table`; there is no hard-coded catalog.

## 3. SSIS component -> Spark construct

| SSIS construct (generator helper) | Spark implementation |
|---|---|
| `Execute SQL Task` `etl.usp_LogPackageStart/End` (`build_quality_package`) | `control.packageRun(...)` context manager (`rowsRead/Inserted/Rejected` set from counts) |
| `record_measure(...)` (`INSERT etl.DataQualityResult` + `SELECT @Out = ...`) | `delta_io.measureRow` + `delta_io.writeResults` (`replaceWhere BatchId AND RuleCode IN (...)`), measure computed with DataFrame aggregates |
| `register_rejects(err_table, ...)` (`etl.usp_LogRejectedRecordSet`) | `delta_io.registerRejects` -> `control.logRejectedRecordSet` once per `RejectReasonCode` |
| `raise_gate(name, expression, severity)` (SSIS expression precedence constraint + `usp_LogError`) | `gates.Gate(name, description, severity, predicate, legacyExpression)` + `gates.applyGates` – warnings logged as `etl.error_log` Warning rows, failures logged then raised (`DataQualityGateError`) |
| `OLE DB Source` with `WHERE BatchId = ?` | `spark.table(...).filter(F.col("BatchId") == batchId)` |
| `Derived Column` | `screens.derive*Flags` (`withColumn` chains, same expressions incl. `Y/N` flags) |
| `Lookup` (error output "Redirect") | left join + `isNull` split (`screens.screenOrderLine`, `screens.screenReferential*`) -> separate error branch |
| `Conditional Split` (ordered outputs + default) | `screens.conditionalSplit` – first-match semantics reproduced with cumulative negation |
| `Aggregate` + `Sort` | `groupBy().agg()` (sort is not needed for Spark aggregates) |
| `Union All` of reject branches | `screens.unionAll` after `_tagBranches` (adds `RejectBranch`) |
| `Row Count` -> `User::RowsRead/RowsInserted/RowsRejected` | `df.count()` assigned to `run.rows*` + `control.logRowCount` |
| `OLE DB Destination` `[err].[…]` | `delta_io.writeRejects` -> Delta `replaceWhere("BatchId = … AND RejectReasonCode IN (...)")` (idempotent re-runs) |
| `OLE DB Command` (`UPDATE err.RejectedLookupFailure … SET ReprocessStatusCode`) | Delta `MERGE` in `DQ_Reject_Reprocess` |
| `Execute Package Task` (Threshold Gate -> master) / Rescreen | job `condition_task` + `dbutils.notebook.run("./DQ_Referential_Screen", …, {"ScreenScope": "Referential Rescreen"})` |
| package variables `User::MeasuredValue`, `FailedRuleCount`, `RejectedRowCount`, `QualityScore` | Python locals, plus `dbutils.jobs.taskValues.set` so the job's condition task and downstream tasks can read them |
| `$Package::ReloadFullHistory` | `p["reloadFullHistory"]`: screens always filter by `BatchId`; reprocess widens the reject window when true |

### Rule engine (DQ_Rule_Engine)

1. `control.evaluateDataQualityRules(spark, catalog, batchId, packageExecutionId)` runs the shared
   re-implementation of `etl.usp_EvaluateDataQualityRules` over the active rule set.
2. Rules without a usable result (`MeasuredValue < 0` / `NotEvaluated` / no row) are re-run by
   `dq_quality.rules`: `SELECT CAST(COUNT(*) AS DECIMAL(18,4)) FROM <delta object> AS t WHERE <RuleExpression>`
   – the exact legacy dynamic SQL, with only `LTRIM(RTRIM(x))` -> `TRIM(x)`, `N'…'` -> `'…'`,
   `ISNULL` -> `COALESCE`, `GETUTCDATE()` -> `current_timestamp()`, `LEN` -> `LENGTH`, and
   `schema.Table` references rewritten to their Delta names (`rules.translateRuleExpression`). The
   rule text stored in `etl.data_quality_rule` is *not* modified; the translation happens at execution
   time only. A rule that still fails records `MeasuredValue = -1`, `ResultStatus = NotEvaluated`, a
   Warning in `etl.error_log` and a `DQ_RULE_NOT_EVALUATED` row in `err.RejectedConstraintViolation`
   (legacy `TRY … CATCH` in `Evaluate Each Configured Rule`).
3. `ResultStatus` = `Passed` / `Failed` (`FAIL` severity) / `Warned` (any other severity) when
   `MeasuredValue > ThresholdValue`; `FailedRuleCount` = count of batch results whose `MeasuredValue >
   ThresholdValue` (`Count Failing Rules`). Gates: Warning `> 0`, Failure `> 10`.
4. The `DFT Rule Inventory` data flow (severity classification + per-group aggregate) is persisted as
   `DQ_RULE_INVENTORY_<group>_<severity>` result rows.

### Rule text

`src/dq_quality/legacy_rules.py` carries the 29 seeded rules verbatim (RuleCode, group, object, name,
expression, severity, threshold, region, source system) as a code-reviewed reference and test fixture;
the runtime source of truth remains the `etl.data_quality_rule` Delta table that session 00 seeds from
`05_seed_data_quality_rules.sql`. The screens implement the *data-flow* checks of each `.dtsx`
(derived-column expressions listed in the notebook headers) and then call
`control.evaluateDataQualityRules(..., ruleGroupCode=<GROUP>)` so the seeded rule group of the same
entity is also evaluated for the batch.

### Threshold gate (DQ_Threshold_Gate)

* `etl.row_count_log` rows of the batch -> `gate_math.computeControlTotals` (per object
  `SourceRowCount - TargetRowCount - RejectRowCount` = `VarianceRowCount`; `BREACH` when non-zero,
  reason `DQ_RECON_BREACH`) -> `etl.reconciliation_result` (`ReconciliationName = DQ_CONTROL_TOTALS`)
  and `err.RejectedConstraintViolation` for breaches.
* `DQ_BATCH_REJECT_RATE` = `100 * sum(RejectRowCount) / sum(SourceRowCount)`;
  `DQ_SCORECARD` = `100 - avg(100 if MeasuredValue > ThresholdValue else 0)` (legacy SQL).
* Thresholds: `etl.configuration` `WarnRejectPercent` (default 2), `MaxRejectPercent` (seeded 5 / PROD 1),
  `MinQualityScore` (default 90) per `EnvironmentCode`; defaults are the legacy constants.
* `control.assertRowCountReconciliation(..., raiseOnFailure=False)` is combined with the local breach
  count; gates: Warning reject rate > warn %, Failure reject rate > max %, Failure any reconciliation
  breach, Failure score < min score. The notebook always publishes task values first, so
  `DQ_Gate_Check` can still evaluate them when the gate fails.

### Referential screen / reprocess

* Order lines -> `stg.StockItem` (`StockItemBusinessKey`), `ref.PackageType` (`PackageTypeCode`);
  sale lines -> `stg.StockItem`, `ref.Currency` (`TransactionCurrencyCode`), `stg.SalesTerritory`
  (`RegionCode`). Misses -> `err.RejectedLookupFailure` (`LookupName`, `LookupKeyValue`,
  `ReprocessStatusCode = NEW`) and `control.logRejectedRecordSet(rejectStage="Referential")`.
  Gates: Warning orphan count > 0, Failure > 1000.
* Reprocess: open `err.RejectedLookupFailure` rows for `stg.OrderLine` (`NEW`/`RETRY`, attempts < 5,
  age <= 30 days); rows whose stock item now resolves are MERGEd into `silver.stg_order_line` and
  marked `RESOLVED`; aged-out rows `ABANDONED`; the rest `RETRY` with `ReprocessAttemptCount + 1`. Then
  the referential screen is re-run in scope `Referential Rescreen`. Gate: Warning if failed rules remain.

## 4. Control framework calls

| Legacy procedure | `dbx_etl_common` call |
|---|---|
| `etl.usp_LogPackageStart` / `usp_LogPackageEnd` / `usp_LogError` on failure | `control.packageRun` |
| `etl.usp_LogRowCount` | `control.logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount, targetRowCount, insertRowCount, rejectRowCount)` |
| `etl.usp_LogRejectedRecordSet` | `control.logRejectedRecordSet(..., rejectStage="Quality" / "Extract" / "Referential" / "RuleEngine" / "Reconciliation", rejectReasonCode=<code>, businessKeyColumn=<key>)` |
| `etl.usp_LogError` (gate warnings) | `control.logError(..., errorSeverity="Warning", errorCode=50000/50001, sourceComponent=<gate name>)` |
| `etl.usp_EvaluateDataQualityRules` | `control.evaluateDataQualityRules` |
| `etl.usp_GetConfiguration` | `control.getConfiguration` (`MaxRejectPercent`, `WarnRejectPercent`, `MinQualityScore`) |
| `etl.usp_AssertRowCountReconciliation` | `control.assertRowCountReconciliation` |

Import boilerplate in every notebook: `from dbx_etl_common import control, params, naming` and
`p = params.getJobParams(dbutils)`. The wheel is installed through the serverless environment
`dq_serverless` (`dependencies: - ${var.dbx_etl_common_wheel}`, default
`/Workspace/Shared/wwi/libs/dbx_etl_common-0.1.0-py3-none-any.whl`) – override the variable with the
path session 00 publishes.

## 5. Parameters

Job parameters (all string): `BatchId` (`"0"`), `BusinessDate` (`${var.businessDate}`),
`ReloadFullHistory` (`"False"`), `EnvironmentCode` (`${var.environmentCode}`), `RestartFromStep` (`""`),
`catalog` (`${var.catalog}` = `wwi_${bundle.target}`). Extra notebook parameters: `ScreenScope`
(`DQ_Referential_Screen`: `Referential Screen` | `Referential Rescreen`), `BaselineTable` /
`BaselineJson` (validation notebook).

## 6. Restart / idempotency

* `RestartFromStep` uses the plan's step names for this project (`File Screen`, `Data Quality`,
  `Referential Screen`, `Reject Routing`, `Referential Rescreen`); a task whose step precedes the
  requested one exits with `skipped:` (legacy `ExecutionMode`/step filter of the master).
* Every write is scoped to the batch: `replaceWhere` on `BatchId` (+ reason codes / rule codes), Delta
  `MERGE` for `stg.OrderLine` replay and reject-status updates. Re-running a task for the same
  `BatchId` yields identical tables.

## 7. Not migrated / needs decision

| Item | Why | Proposed handling |
|---|---|---|
| `ref.PackageType` lookup (`DQ_Referential_Screen`) | No `ref.PackageType` DDL exists under `sqlserver/staging/tables/` (the legacy lookup references a table the estate never ships). | Notebook uses `silver.ref_package_type` when present, otherwise logs a Warning and treats staged codes as known (no false orphans). Decision: ship the reference table (session 02/REF) or drop the lookup. |
| `stg.SalesTerritory` lookup (sale lines) | `stg.Sale` carries no `SalesTerritoryCode`; the legacy join keyed on it. | The Delta screen resolves the sale `RegionCode` against `stg.SalesTerritory.RegionCode`. Confirm the intended key. |
| `etl.RowCountAudit` vs `etl.row_count_log` | Contract names the Delta table `etl.row_count_log`; legacy table is `RowCountAudit`. | Gate reads `row_count_log`, falls back to `row_count_audit`. Confirm with session 00. |
| `WarnRejectPercent` / `MinQualityScore` configuration keys | Legacy thresholds 2 and 90 are hard-coded package expressions; only `MaxRejectPercent` is seeded. | Keys are read from `etl.configuration` with the legacy constants as defaults; seeding them is optional. |
| `Intraday Screen` / `Customer Screen` masters | Reuse `DQ_OrderLine_Screen`, `DQ_InvoiceLine_Screen`, `DQ_Customer_Screen`, `DQ_Referential_Screen` under other step names. | Session 00's master jobs should call this job's tasks (or `run_job_task` with `RestartFromStep=""`). |
| `ERR_Route_RejectedRows` (Reject Routing phase) | Belongs to `WWI_ErrorHandling` (another session). | Not in this job; the master orders it after `DQ_Reject_Reprocess`. |
| Rule expressions using T-SQL-only syntax | Translation covers the seeded 29 rules; unforeseen T-SQL in newly added rules falls back to `MeasuredValue = -1`, `NotEvaluated` (legacy behaviour). | Author new rules as Spark-SQL compatible predicates. |
| `validation/static/run_all_checks.py` `forbidden-content` | The repo-wide check fails on the words "Databricks" / "dbutils" in *any* file, including the mandated `# Databricks notebook source` header; `validation/` is read-only for this session. | Decision for the owner of `validation/`: skip `databricks/` and `docs/migration/` in `check_no_forbidden_content` (baseline `main` passes; all deep checks pass with this change). |
| DLT expectations | Not used (see design choice above). | – |

## 8. Validation

* `python3 -m py_compile databricks/05_data_quality/notebooks/*.py …/src/dq_quality/*.py …/validation/*.py`
* `cd databricks/05_data_quality && python3 -m pytest tests -q` – 42 tests over the rule translation,
  status derivation, ordered conditional splits, lookups, file-row reconstruction, reprocess routing,
  gate math and the reject registration (uses a test-only fake of `dbx_etl_common` in `tests/fakes/`).
* `cd databricks/05_data_quality && databricks bundle validate -t dev` (and `-t prod`).
* `validation/DQ_Reconciliation.py`: per target table row count + `sum(xxhash64(...))` for the batch,
  compared with the SQL Server baseline supplied as a Delta table (`BaselineTable`) or JSON
  (`BaselineJson`); outcome logged with `control.logRowCount`.
