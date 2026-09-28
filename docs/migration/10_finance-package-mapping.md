# 10_finance (WWI_Finance) -> `databricks/10_finance` package mapping

Session 10 of the SSIS -> Databricks migration. Source: `ssis/10_finance/` (7 `FIN_*` packages,
generator `build_finance_packages.py`). Target: Asset Bundle `wwi_10_finance`, job `wwi_10_finance`,
one notebook per package, shared finance helpers in `databricks/10_finance/src/`.

## 1. Packages -> notebooks / tasks

| Legacy package | Notebook (task_key = package name) | Master_Finance_Close phase | Targets written |
|---|---|---|---|
| `FIN_Load_ApAging` | `notebooks/FIN_Load_ApAging.py` | 10 Subledger Loads (stream 1) | `gold.fact_payment` (AP aging rows), `gold.agg_ap_aging_summary`, `silver.work_ap_aging_staging`, `silver.err_ap_aging_reject` |
| `FIN_Load_WithholdingTax` | `notebooks/FIN_Load_WithholdingTax.py` | 10 Subledger Loads (stream 2) | `gold.fact_payment` (withholding columns), `silver.work_withholding_tax_line`, `silver.err_withholding_tax_reject`, `silver.work_withholding_certificate_queue` |
| `FIN_Load_CostAllocation` | `notebooks/FIN_Load_CostAllocation.py` | 10 Subledger Loads (stream 2) | `gold.agg_finance_close_summary`, `silver.work_cost_allocation_result` |
| `FIN_Currency_Revaluation` | `notebooks/FIN_Currency_Revaluation.py` | 20 FX Revaluation | `gold.fact_payment` (revaluation columns), `silver.work_fx_revaluation_rate` |
| `FIN_Load_GlPostings` | `notebooks/FIN_Load_GlPostings.py` | 30 General Ledger | `gold.fact_gl_posting`, `silver.work_gl_held_line` |
| `FIN_Reconcile_SubledgerToGl` | `notebooks/FIN_Reconcile_SubledgerToGl.py` | 40 Subledger Tie Out | `etl.reconciliation_result`, `etl.row_count_log`, `etl.rejected_record` |
| `FIN_Close_PeriodLock` | `notebooks/FIN_Close_PeriodLock.py` | 60 Period Lock | `etl.period_lock`, `etl.configuration` (`Finance.PeriodStatus.<Ledger>` = Closed), `etl.batch` notes |
| (validation) | `validation/FIN_Reconcile_Baseline.py` | run ad hoc after a close | `etl.row_count_log` (baseline comparison) |

Job task edges (`resources/wwi_10_finance.yml`) follow the master-package phase order; the
`docs/inventories/package-dependencies.csv` edges (`FIN_Load_ApAging`, `FIN_Load_WithholdingTax`,
`FIN_Currency_Revaluation`, `FIN_Load_GlPostings` -> `FIN_Reconcile_SubledgerToGl`) are satisfied transitively:

```
FIN_Load_ApAging ─┐
FIN_Load_WithholdingTax ─┼─> FIN_Currency_Revaluation ─> FIN_Load_GlPostings ─> FIN_Reconcile_SubledgerToGl ─> FIN_Close_PeriodLock
FIN_Load_CostAllocation ─┘
```

Cross-project inputs (`STG_Load_ApInvoice`, `STG_Load_CostCenter`, `STG_Load_Currency`, `STG_Load_GlJournal`,
`FACT_Load_Payment`, `FACT_Load_GLPosting`) are owned by other bundles and become `depends_on`/`run_job_task`
edges in session 00's master job.

## 2. `Master_Finance_Close` cadence semantics (for session 00)

From `ssis/orchestration-plan.json`, root `Master_Finance_Close` (project `WWI_Orchestration`), **cadence monthly**,
described as "month-end close, gated on the accounting period being open, then subledger loads, FX revaluation,
GL, tie-out, aggregates, period lock". Root parameters: `BatchId`, `BusinessDate`, `EnvironmentCode`,
`ReloadFullHistory`, `MaxParallelStreams=4`, `MaxExtractAttempts=3`, `RestartFromStep`, `AccountingPeriod`,
`AllowCloseWithVariance="False"`.

| Seq | Phase | Group | Streams | Packages | In this bundle |
|---|---|---|---|---|---|
| 10 | Subledger Loads | Mart | 2 | `FIN_Load_ApAging`, `FIN_Load_WithholdingTax`, `FIN_Load_CostAllocation` | yes |
| 20 | FX Revaluation | Mart | 1 | `FIN_Currency_Revaluation` | yes |
| 30 | General Ledger | Mart | 1 | `FIN_Load_GlPostings` | yes |
| 40 | Subledger Tie Out | Mart | 1 | `FIN_Reconcile_SubledgerToGl` | yes |
| 50 | Close Aggregates | Aggregate | 1 | `AGG_Refresh_FinanceCloseSummary` (`WWI_Aggregates`) | **no** - session 08/09 |
| 60 | Period Lock | Mart | 1 | `FIN_Close_PeriodLock` | yes |
| 90 | Close Escalation | Maintenance | 1 | `ERR_*` escalation (`WWI_ErrorHandling`) | **no** - session 15 |

Conditional edges the master job must reproduce around this bundle:

* `Start Batch` -> `Read Period Status` (`Finance.PeriodStatus.<Ledger>` from `etl.configuration`):
  **open** -> Subledger Loads; **closed** -> Close Escalation (skip all finance loads).
  In this bundle every load task additionally calls `period_lock.assertPeriodOpen`, so a closed/locked
  period fails fast with `PeriodLockedError` even when run standalone.
* `Subledger Tie Out` -> `Read Variance`: `VarianceCount == 0 OR AllowCloseWithVariance` -> Close Aggregates;
  `VarianceCount > 0 AND NOT AllowCloseWithVariance` -> Close Escalation. Implemented inside
  `FIN_Reconcile_SubledgerToGl` as `control.assertRowCountTolerance(..., raiseOnFailure=not AllowCloseWithVariance)`;
  the task fails (routing to escalation in the master job) unless the override is set.
* `Close Aggregates` -> `Period Lock` -> `usp_AssertRowCountReconciliation` -> `End Batch`; escalation -> `End Batch`
  with `SucceededWithWarnings`. When `wwi_10_finance` runs standalone with `BatchId=0` it starts/adopts a
  `Master_Finance_Close` batch (`control.startBatch(..., batchType="Monthly", allowAdoptRunning=True)`) and
  `FIN_Close_PeriodLock` ends it; when the master job supplies `BatchId` the batch lifecycle is left to it.
* `RestartFromStep`: each task compares its phase sequence with the sequence of the named phase
  (`finance_common.shouldSkipForRestart`) and exits early with `SKIPPED (RestartFromStep)` when it is earlier.
* `MaxParallelStreams` is expressed by the three parallel phase-10 tasks; `MaxExtractAttempts` does not apply
  (no extract packages in WWI_Finance).

## 3. Source / target objects -> Delta tables

| Legacy object | Delta table | Role |
|---|---|---|
| `stg.ApInvoice` | `silver.stg_ap_invoice` | AP aging / withholding input (via `finance_sources.apInvoiceSql`) |
| `stg.ApInvoiceLine` | `silver.stg_ap_invoice_line` | withholding tax input |
| `stg.PaymentTerms` | `silver.stg_payment_terms` | discount-at-risk lookup |
| `stg.GlJournalLine` | `silver.stg_gl_journal_line` | GL postings input |
| `stg.FxRate` | `silver.stg_fx_rate` | FX revaluation input |
| `stg.CostCenter` | `silver.stg_cost_center` | cost allocation source pools / target validation |
| `stg.WithholdingTaxRate` (package SQL, no DDL in `sqlserver/staging`) | `silver.stg_withholding_tax_rate` | created by this bundle if absent |
| `stg.CostAllocationRule` / `stg.CostAllocationTarget` / `stg.CostCentreBalance` (package SQL, no DDL) | `silver.stg_cost_allocation_rule`, `silver.stg_cost_allocation_target`, `silver.stg_cost_centre_balance` | created by this bundle if absent |
| `Dimension.Supplier` | `gold.dim_supplier` | AP aging supplier lookup (owned by DIM session) |
| `Fact.Payment` | `gold.fact_payment` | AP aging rows, withholding, revaluation columns (minimum schema created if the FACT session has not) |
| `Fact.GL Posting` | `gold.fact_gl_posting` | GL postings |
| `Aggregate.Finance Close Summary` | `gold.agg_finance_close_summary` | cost allocation publish |
| `Aggregate.ApAgingSummary` (package-created) | `gold.agg_ap_aging_summary` | aging summary by ledger/bucket |
| `work.ApAgingStaging`, `work.GlHeldLine`, `work.CostAllocationResult`, `work.FxRevaluationRate`, `work.WithholdingTaxLine`, `work.WithholdingCertificateQueue` | `silver.work_*` (same snake_case) | package work tables, overwritten per batch (`replaceWhere BatchId`) |
| `err.ApAgingReject`, `err.WithholdingTaxReject` | `silver.err_ap_aging_reject`, `silver.err_withholding_tax_reject` | quarantine tables (+ `control.logRejectedRecordSet` into `etl.rejected_record`) |
| `etl.ReconciliationResult` | `etl.reconciliation_result` | tie-out results (PascalCase columns as in `sqlserver/control/06_tables_reconciliation.sql`) |
| new: period lock | `etl.period_lock` (`LedgerCode, AccountingPeriod, LockStatusCode, LockedByBatchId, LockedAt, LockedByPackage, Notes`) | dedicated lock table; `Finance.PeriodStatus.<Ledger>` configuration is also set to `Closed` for the legacy read path |
| `etl.Configuration` keys `Finance.OpenPeriod.<Ledger>`, `Finance.PeriodStatus.<Ledger>`, `Finance.KnownVariance.<Account>`, `Finance.ControlAccount.<Subledger>.<Ledger>` | `etl.configuration` | read with Spark SQL (`period_lock.openPeriodsFromConfiguration`, `knownVariancesFromConfiguration`, `delta_io.controlAccounts`) |

Column contract: the package SQL uses the WWI_Finance column names (`InvoiceAmount`, `PaidAmount`, `FunctionalDebitAmount`, ...).
`src/finance_sources.py` adapts the `silver.stg_*` schemas from `sqlserver/staging/tables/21_stg_tables_finance.sql`
(`AmountPaid AS PaidAmount`, `AccountedDebitAmount AS FunctionalDebitAmount`, `FiscalPeriodLabel AS AccountingPeriod`,
`IsPosted -> JournalStatusCode`, ...). Columns with no staging counterpart are typed NULLs and listed by
`finance_sources.unmappedColumns()` (see §6).

## 4. SSIS components -> Spark constructs

| Package | SSIS component | Spark construct (module.function) |
|---|---|---|
| all | Execute SQL `etl.usp_LogPackageStart/End`, `usp_LogError` (OnError handler) | `finance_common.PackageExecution` (wraps `control.logPackageStart`, `logPackageEnd`, `logError`) |
| all | `Execute SQL: Check period lock` (package-level gate) | `period_lock.assertPeriodOpen` (raises `PeriodLockedError`) |
| ApAging | OLE DB Source `AP_OPEN_ITEMS_SQL` + Conditional Split `Exclude paid / CANC,VOID,DRAFT` + disputed toggle | `finance_rules.apAgingOpenItems` (`where`, `IncludeDisputedInvoices`) |
| ApAging | Derived Column `AgingBucketCode`, `ReportableAmount` (NA/EU/APAC), `IsPastDue`, `AgingBucketSort`, `DiscountAtRisk` | `apAgingOpenItems` (`F.when` chains; payment terms `left join`) |
| ApAging | Lookup `Dimension.Supplier` (no-match -> error output) | `finance_rules.lookupSupplierKey` -> matched / `err_ap_aging_reject` (`SUPPLIER_NOT_FOUND`) |
| ApAging | OLE DB Destination `work.ApAgingStaging`; Execute SQL merge to `Fact.Payment`; Aggregate -> summary | `delta_io.replaceWhere` (work), `delta_io.mergeInto(gold.fact_payment, keys=[PaymentBusinessKey])`, `finance_rules.apAgingSummary` + `replaceWhere AccountingPeriod` |
| GlPostings | Execute SQL `Check journal balance` (fail unless `AllowUnbalancedJournals`) | `finance_rules.unbalancedJournalCount` -> `control.logError` + raise |
| GlPostings | OLE DB Source posted lines joined to `Finance.OpenPeriod.<Ledger>` | `finance_rules.glPostedLines(lines, openPeriods)` |
| GlPostings | Derived Column `Functional*` fallback, `NetAmount`, `PostingSide`, `SubledgerSourceKey`, `TaxRegimeCode` | `finance_rules.deriveGlPostingAttributes` |
| GlPostings | Conditional Split `PERIOD_NOT_OPEN` -> `work.GlHeldLine` + `usp_LogRejectedRecord` | `finance_rules.splitHeldLines`; held rows -> `silver.work_gl_held_line`, `control.logRejectedRecord(... "PERIOD_NOT_OPEN")` |
| GlPostings | OLE DB Destination / merge `Fact.GL Posting` | `delta_io.mergeInto(gold.fact_gl_posting, keys=[GlJournalLineId])` |
| Reconcile | Aggregate subledger (Fact.Payment by ledger/period/control account) + Aggregate GL + Merge Join (full outer) | `finance_rules.buildReconciliationSet` (`groupBy` + `full_outer` join, tolerance status) |
| Reconcile | Lookup `Finance.KnownVariance.<Account>` | `finance_rules.applyKnownExplanations` |
| Reconcile | OLE DB Destination `etl.ReconciliationResult`; `usp_LogRowCount`; `usp_LogRejectedRecordSet`; `usp_AssertRowCountTolerance` | delete-then-append per `BatchId`; `control.logRowCount("Finance.SubledgerToGl|<Ledger>|<Account>", sourceRowCount=subledger amount, targetRowCount=GL amount)`, `control.logRejectedRecordSet(... "SUBLEDGER_VARIANCE")`, `control.assertRowCountTolerance(scope="Finance.SubledgerToGl", absoluteTolerance=VarianceTolerance, percentTolerance=0, raiseOnFailure=not AllowCloseWithVariance)` |
| PeriodLock | Execute SQL `Count unexplained variances` -> fail (error 51001) | `unexplainedVariances(...).count()` -> `control.logError(errorCode="51001")` + raise |
| PeriodLock | Foreach ledger (`LedgerScope`) + APAC 4-4-5 period-end check + `UPDATE etl.Configuration ... Closed` + stamp batch | `period_lock.lockLedgers` (MERGE into `etl.period_lock`, MERGE `Finance.PeriodStatus.<Ledger>`), `period_lock.apac445PeriodEnded`, batch notes update |
| CostAllocation | OLE DB Source active rules by `AllocationRuleSet` + Sort `RuleSequence` + Foreach rule (cursor) | `finance_rules.activeRules`, `finance_rules.allocateCosts` (sequential loop so later rules consume earlier outputs) |
| CostAllocation | Derived Column proportional share; Row Count residual | `allocateCosts` (`DriverValue / sum(DriverValue) * pool`), `finance_rules.unallocatedResidual` -> `logRowCount` |
| CostAllocation | Aggregate by target cost centre / period -> `Aggregate.Finance Close Summary` | `finance_rules.summariseAllocationsByTarget` + `delta_io.mergeInto(gold.agg_finance_close_summary, keys=[CostCentreCode, AccountingPeriod])` |
| Revaluation | OLE DB Source latest rate <= `RevaluationDate` (`CLOSING`, `AVERAGE`) + Derived `InverseRate`, `IsTriangulated` | `finance_rules.closingRates` (window `row_number` per currency/type) |
| Revaluation | Execute SQL `Check missing closing rates` (fail when `FailOnMissingRate`, error 51010) | `finance_rules.missingClosingRateCurrencies` -> `control.logError(errorCode="51010")` + raise |
| Revaluation | OLE DB Command update open items (P&L=AVERAGE, else CLOSING, quote currency by region) | `finance_rules.revalueOpenItems` + `delta_io.mergeInto(gold.fact_payment, updateColumns=[Revalued*, UnrealisedGainLossAmount, ...])` |
| Withholding | OLE DB Source AP lines + Lookup withholding rates + Derived NA/EU/APAC rules, `NetPayableAmount`, `IsWithheld` | `finance_rules.withholdingLines` |
| Withholding | Conditional Split `JURISDICTION_UNMAPPED` -> `err.WithholdingTaxReject` | `finance_rules.splitUnmappedJurisdictions` + `control.logRejectedRecordSet` |
| Withholding | OLE DB Destination `work.WithholdingTaxLine`, merge to `Fact.Payment`, certificate queue (EU) | `replaceWhere`, `mergeInto(gold.fact_payment)`, `finance_rules.certificateQueue` -> `silver.work_withholding_certificate_queue` |

## 5. Control framework -> `dbx_etl_common`

| Legacy call | `dbx_etl_common` |
|---|---|
| `etl.usp_StartBatch` (`Master_Finance_Close`, Monthly) when `BatchId=0` | `control.startBatch(spark, catalog, "Master_Finance_Close", batchType="Monthly", businessDate=..., environmentCode=..., allowAdoptRunning=True)` (`finance_common.resolveBatchId`) |
| `etl.usp_EndBatch` (PeriodLock, only for an internally started batch) | `control.endBatch(spark, catalog, batchId)` |
| `etl.usp_LogPackageStart` / `usp_LogPackageEnd` | `control.logPackageStart(..., projectName="WWI_Finance", stepName=<phase>)`, `control.logPackageEnd(..., status, rowsRead, rowsInserted, rowsUpdated, rowsRejected)` via `finance_common.PackageExecution` |
| `etl.usp_LogError` (OnError handlers, 51001/51010 RAISERRORs) | `control.logError(...)` |
| `etl.usp_LogRowCount` | `control.logRowCount(...)` (every notebook logs its target; reconciliation logs one row per ledger/account) |
| `etl.usp_LogRejectedRecord` / `usp_LogRejectedRecordSet` | `control.logRejectedRecord` (GL held lines), `control.logRejectedRecordSet` (AP supplier rejects, withholding unmapped, subledger variances) |
| `etl.usp_AssertRowCountTolerance` | `control.assertRowCountTolerance(spark, catalog, batchId, scope=..., objectName=..., absoluteTolerance=VarianceTolerance, raiseOnFailure=not AllowCloseWithVariance)` |
| `etl.usp_GetConfiguration` (`Finance.*` keys) | Spark SQL over `etl.configuration` (`ConfigurationKey`, `EnvironmentCode`, `ConfigurationValue`) - set-based reads of many keys at once |
| `$Package::*` / `$Project::*` parameters | `params.getJobParams(dbutils)` + `finance_common.getFinanceParams(dbutils, businessDate)` |
| three-part table names | `naming.table(catalog, schema, table)` |

`dbx_etl_common` is consumed as a wheel: every task has `libraries: - whl: `../../common/dbx_etl_common/dist/*.whl``
(built by session 00). No code from `databricks/common` is copied; the
test-only fake lives in `databricks/10_finance/tests/fakes/dbx_etl_common/`. No `[dbx-migration 00]` PR was open
when this was written, so the imports follow the shared interface contract verbatim.

## 6. Parameters

Standard job parameters: `BatchId="0"`, `BusinessDate` (`${var.businessDate}`), `ReloadFullHistory="False"`,
`EnvironmentCode` (`${var.environmentCode}`), `RestartFromStep=""`, `catalog` (`${var.catalog}` = `wwi_${bundle.target}`).

WWI_Finance package parameters (job parameters with the legacy defaults; each notebook reads them as widgets):

| Parameter | Used by | Default | Legacy meaning |
|---|---|---|---|
| `AccountingPeriod` | all | `""` -> `yyyy-MM` of `BusinessDate` | period being closed |
| `AgingAsOfDate` | ApAging | `""` -> `BusinessDate` | aging as-of date |
| `IncludeDisputedInvoices` | ApAging | `False` | include on-hold/disputed invoices |
| `AllowUnbalancedJournals` | GlPostings | `False` | skip the journal balance check |
| `VarianceTolerance` | Reconcile | `1` | absolute tolerance per ledger/account |
| `AllowCloseWithVariance` | Reconcile, PeriodLock | `False` | master override: tie-out out-of-tolerance does not fail |
| `LedgerScope` | PeriodLock | `ALL` | lock all ledgers or one |
| `AllocationRuleSet` | CostAllocation | `STANDARD` | rule set code |
| `RevaluationDate` | Revaluation | `""` -> `BusinessDate` | rate cut-off |
| `FailOnMissingRate` | Revaluation | `True` | fail when an open-item currency has no CLOSING rate |
| `JurisdictionScope` | WithholdingTax | `ALL` | restrict to one jurisdiction |

Bundle variables: `catalog`, `warehouse_id`, `businessDate`, `environmentCode`, `accountingPeriod`,
`allowCloseWithVariance`, `varianceTolerance`, `ledgerScope`, `allocationRuleSet`, `failOnMissingRate`,
`jurisdictionScope`. Tasks run on serverless job compute (`environments` block). Targets `dev` (default,
`mode: development`) and `prod`.

## 7. Idempotency and quarantine

* Work tables: `replaceWhere BatchId = <batch>`; `gold.agg_ap_aging_summary`: `replaceWhere AccountingPeriod`.
* Facts / aggregates: Delta `MERGE` on the business key (`PaymentBusinessKey`, `GlJournalLineId`,
  `CostCentreCode+AccountingPeriod`); revaluation/withholding only update their own columns on `gold.fact_payment`.
* `etl.reconciliation_result`: rows of the same `BatchId` are deleted before the append.
* `etl.period_lock`: MERGE on `(LedgerCode, AccountingPeriod)`; every finance load calls `assertPeriodOpen`
  before writing, so reloads of a locked period fail with `PeriodLockedError` (unlock = delete/update the lock row,
  a deliberate manual step as in the legacy estate).
* Quarantine: `silver.err_ap_aging_reject`, `silver.err_withholding_tax_reject`, `silver.work_gl_held_line`
  plus `etl.rejected_record` through `control.logRejectedRecord*`.
* `ReloadFullHistory=True` drops the `LoadBatchId = BatchId` source filter (all staged rows), otherwise loads are
  batch-scoped as in the package SQL.

## 8. Not migrated / needs decision

1. **Repo static check.** `validation/static/run_all_checks.py` has a `forbidden-content` rule that rejects the words
   *Databricks* / *dbutils* / *Unity Catalog* in any `.py/.md/.yml` outside the legacy sample dirs. The mandatory
   `# Databricks notebook source` header trips it for every migration session, so the check reports failures only
   under `databricks/10_finance/**` and this document. `validation/` is read-only for this session; the carve-out
   for `databricks/**` and `docs/migration/**` must land via session 00 / the parent.
2. **Staging columns absent from `21_stg_tables_finance.sql`.** The package SQL reads `SupplierSiteCode`
   (`stg.ApInvoice`), `SupplierTaxRegistrationNumber` and `ServiceCategoryCode` (`stg.ApInvoiceLine`). They are typed
   NULLs in `finance_sources.py`, which makes every EU supplier fall to the *standard* withholding rate and every
   line to the unmapped-category path unless the STG session exposes them. Needs the silver owner to add them
   (or a mapping to `TaxCode` / supplier dimension).
3. **Finance reference tables with no DDL in `sqlserver/`** (`stg.WithholdingTaxRate`, `stg.CostAllocationRule`,
   `stg.CostAllocationTarget`, `stg.CostCentreBalance`): created by `finance_tables.ensureFinanceTables` with the
   columns the package SQL uses. Their loader is not in scope of any SSIS package -> needs an owner.
4. **Gold targets created with a minimum schema** (`gold.fact_payment`, `gold.fact_gl_posting`,
   `gold.agg_finance_close_summary`) when the FACT/AGG sessions have not created them yet; column names follow
   `sqlserver/warehouse/facts/*.sql` but surrogate keys the finance packages do not resolve (`SupplierKey` aside)
   are NULL. The finance notebooks enable Delta schema evolution (`autoMerge`) per session so the finance columns are
   added to the FACT session's tables.
5. **Phase 50 Close Aggregates and phase 90 Close Escalation** run between/after this bundle's tasks in the master
   job and are owned by sessions 08/09 and 15. Standalone, `FIN_Close_PeriodLock` runs straight after the tie-out.
6. **`Aggregate.ApAgingSummary`** has no DDL in `sqlserver/warehouse/aggregates`; migrated as `gold.agg_ap_aging_summary`
   keyed on `(LedgerCode, AccountingPeriod, AgingBucketCode)`.
7. **APAC 4-4-5 period end** is evaluated against `gold.dim_date` (`Fiscal Period 445` / `Is Fiscal Period End`) when
   the DIM session has created it; until then the APAC ledger locks on the calendar month like NA/EU.
8. **Re-running the same `BatchId`** appends duplicate `etl.rejected_record` rows unless session 00 de-duplicates in
   `logRejectedRecordSet`.
9. `warehouse_id` is declared for parity with the other bundles but unused (no SQL tasks).

## 9. Validation and deployment

```bash
# offline
cd databricks/10_finance
python3 -m py_compile notebooks/*.py src/*.py validation/*.py
python3 -m pytest tests -q                     # 27 tests, local PySpark (no Delta jars needed)
cd ../.. && python3 validation/checks/run_deep_checks.py

# bundle (needs DATABRICKS_AUTH_TYPE=pat DATABRICKS_HOST DATABRICKS_TOKEN)
cd databricks/10_finance
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev                # not run by this session
databricks bundle run -t dev wwi_10_finance --params BatchId=0,BusinessDate=2024-03-31,AccountingPeriod=2024-03
```

Baseline reconciliation: run `validation/FIN_Reconcile_Baseline.py` with `BaselineTable=<catalog>.etl.finance_baseline`
or `BaselineJson='[{"ObjectName":"Fact.GL Posting","ScopeKey":"<BatchId>","RowCount":123,"HashValue":456}]'`; it
writes `Baseline|<object>` / `BaselineHash|<object>` rows through `control.logRowCount` and fails on mismatch when
`FailOnMismatch=True`. SQL Server figures come from `validation/runtime/*.sql` (row counts) plus
`CHECKSUM_AGG(BINARY_CHECKSUM(...))` over the same column lists.
