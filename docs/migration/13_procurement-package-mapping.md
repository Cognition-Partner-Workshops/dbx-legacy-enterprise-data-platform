# 13_procurement (WWI_Procurement) — SSIS → Databricks package mapping

Source: `ssis/13_procurement/` (generator `build_procurement_packages.py`, 5 `PRC_*` packages, phase 92
"Procurement Mart", 2 streams, in `ssis/orchestration-plan.json`).
Target: bundle `databricks/13_procurement` (`wwi_13_procurement`), one job `wwi_13_procurement`.

## 1. Packages → notebooks / tasks

| Legacy package | Notebook | Job task_key | depends_on (job) | Legacy inventory edges (`package-dependencies.csv`) |
|---|---|---|---|---|
| `PRC_Load_PurchaseSpend` | `notebooks/PRC_Load_PurchaseSpend.py` | `PRC_Load_PurchaseSpend` | — | `STG_Load_PurchaseOrder` → this (other project; satisfied by the master job) |
| `PRC_Load_ReceiptMatching` | `notebooks/PRC_Load_ReceiptMatching.py` | `PRC_Load_ReceiptMatching` | — | `Master_Daily_ETL` execution edge only |
| `PRC_Load_SupplierScorecard` | `notebooks/PRC_Load_SupplierScorecard.py` | `PRC_Load_SupplierScorecard` | — | `STG_Load_Supplier` → this |
| `PRC_Load_ContractCompliance` | `notebooks/PRC_Load_ContractCompliance.py` | `PRC_Load_ContractCompliance` | `PRC_Load_SupplierScorecard` | `STG_Load_VendorContract` → this |
| `PRC_Export_SupplierStatement` | `notebooks/PRC_Export_SupplierStatement.py` | `PRC_Export_SupplierStatement` | — | `FACT_Load_SupplierTransaction` → this |
| *(new)* reconciliation | `validation/PRC_Reconcile_Procurement.py` | `PRC_Reconcile_Procurement` | all loads, `run_if: ALL_DONE` | — |

The inventory has no intra-project edge and the phase runs the five packages in two streams. The one
edge added inside the job (`ContractCompliance` after `SupplierScorecard`) comes from the data: the
package's `Publish Compliance To Scorecard` is an `UPDATE Aggregate.[Supplier Performance]` that only hits
rows the scorecard has written for the same supplier/month. Everything else runs in parallel.

## 2. Source / target objects → Delta tables

| Legacy object | Delta table | Written by | Idempotency |
|---|---|---|---|
| `stg.PurchaseOrder` | `${catalog}.silver.stg_purchase_order` | upstream (STG project) | read only |
| `stg.PurchaseOrderLine` | `silver.stg_purchase_order_line` | upstream | read only |
| `stg.Receipt` | `silver.stg_receipt` | upstream | read only |
| `stg.ApInvoiceLine` | `silver.stg_ap_invoice_line` | upstream | read only |
| `stg.VendorContract` | `silver.stg_vendor_contract` | upstream | read only |
| `stg.Supplier` | `silver.stg_supplier` | upstream | read only |
| `stg.MatchTolerance` | `silver.stg_match_tolerance` | upstream (see §6) | read only |
| `etl.SupplierScoringWeight` | `etl.supplier_scoring_weight` | session 00 (control) | read only |
| `Dimension.Supplier` | `gold.dim_supplier` | DIM project | read only (current-row lookup on `valid_to`) |
| `Fact.Supplier Transaction` | `gold.fact_supplier_transaction` | FACT project | read only |
| `work.PurchaseSpendLine` | `silver.work_purchase_spend_line` | PurchaseSpend | overwrite per run (legacy TRUNCATE) |
| `err.PurchaseSpendReject` | `silver.err_purchase_spend_reject` | PurchaseSpend | overwrite per run |
| `Fact.Purchase` (+ `Fact.Purchase.Extensions`) | `gold.fact_purchase` | PurchaseSpend | MERGE on `(purchase_order_number, purchase_order_line_number)` |
| `work.ReceiptMatch` | `silver.work_receipt_match` | ReceiptMatching | overwrite per run |
| `Fact.Purchase Receipt` | `gold.fact_purchase_receipt` | ReceiptMatching | MERGE on `(receipt_number, receipt_line_number, purchase_order_number, purchase_order_line_number)` |
| `work.SupplierScorecard` | `silver.work_supplier_scorecard` | SupplierScorecard | overwrite per run |
| `Aggregate.Supplier Performance` | `gold.agg_supplier_performance` | SupplierScorecard (MERGE), ContractCompliance (UPDATE matched) | MERGE on `(calendar_month, supplier_key, region_code)`; `calendar_month` = month of BusinessDate |
| `work.ContractCompliance` | `silver.work_contract_compliance` | ContractCompliance | overwrite per run |
| `work.SupplierStatementLine` | `silver.work_supplier_statement_line` | Export_SupplierStatement | overwrite per run |
| `work.SupplierStatementArchive` | `silver.work_supplier_statement_archive` | Export_SupplierStatement | `replaceWhere StatementPeriod = '<yyyy-MM>'` |
| `file:supplier_statement.csv` (`WWI_Archive_Files`, `$Project::ArchiveFileRoot`) | `${OutboundVolumePath}/supplier_statement_<yyyy-MM>.csv` + `${ArchiveVolumePath}/supplier_statement_<yyyy-MM>_batch<BatchId>.csv` | Export_SupplierStatement | file overwritten per period; archive copy per batch |
| `etl.RejectedRecord`, `etl.RowCountAudit`, `etl.PackageExecution`, `etl.ErrorLog` | `etl.rejected_record`, `etl.row_count_log`, `etl.package_execution`, `etl.error_log` | via `dbx_etl_common.control` only | owned by session 00 |

Gold column names are snake_case of the legacy warehouse column (`[Supplier Key]` → `supplier_key`,
`[WWI Supplier ID]` → `wwi_supplier_id`); silver `stg_*`/`work_*` columns keep the PascalCase names used by
the package SQL. Procurement-specific columns not in the legacy DDL (`spend_class_code`,
`contracted_amount`, `savings_amount`, `match_result_code`, `accrual_amount`, `supplier_score`,
`off_contract_spend_amount`, `leakage_amount`, `compliance_percent`, …) are added with Delta schema
evolution (`autoMerge`) on first write.

## 3. SSIS components → Spark constructs

All transformation logic lives in `src/procurement_lib/` as pure DataFrame functions (unit-tested); the
notebooks only wire tables, parameters and the control framework.

### PRC_Load_PurchaseSpend
| SSIS component | Type | Spark construct |
|---|---|---|
| `Log Package Start` / `Log Package Success` / `Log Error` / `Mark Execution Failed` (event handler) | Execute SQL (`etl.usp_LogPackageStart/End`, `usp_LogError`) | `control.packageRun(...)` context manager |
| `Truncate work_PurchaseSpendLine` | Execute SQL | `delta_io.overwriteTable` (overwrite instead of truncate+append) |
| `stg PurchaseOrderLine` (SPEND_SQL, joins `stg.PurchaseOrder`, LEFT `stg.VendorContract`, CASE for legacy contract ref, `LoadBatchId = ?`, excludes CANC/DRAFT) | OLE DB Source | `purchase_spend.buildPurchaseSpendLines` (`resolveContractNumber` = the CASE) |
| `Classify Spend` (PriceVariancePercent, SpendClassCode) | Derived Column | `purchase_spend.classifySpend` |
| `Derive Savings` (ContractedAmount, SavingsAmount) | Derived Column | `purchase_spend.deriveSavings` |
| `Lookup Supplier Key` (`Dimension.Supplier` current rows, no-match → redirect) | Lookup (RD) | `currentSupplierKeys` + `lookupSupplierKey` → `(matched, rejected)` |
| `work PurchaseSpendLine` | OLE DB Destination (fast load) | `overwriteTable(matched, silver.work_purchase_spend_line)` |
| `err PurchaseSpendReject` | reject destination | `overwriteTable(rejected, silver.err_purchase_spend_reject)` + `control.logRejectedRecordSet(...)` |
| `Count Rows Read` / `Count Rows Inserted` | Row Count | `DataFrame.count()` → `run.rowsRead/rowsInserted` |
| `Publish Purchase Spend` (`Integration.usp_PostPurchaseSpend`) | Execute SQL (proc **not in repo**) | `purchase_spend.toFactPurchase` + `delta_io.mergeInto(gold.fact_purchase)` — see §6 |
| `Measure Maverick Spend` | Execute SQL (result set → variables) | `purchase_spend.measureMaverickSpend` |
| `Log Row Counts` (`etl.usp_LogRowCount`, `Fact.Purchase`) | Execute SQL | `control.logRowCount(..., "Fact.Purchase", ...)` |

### PRC_Load_ReceiptMatching
| SSIS component | Type | Spark construct |
|---|---|---|
| `Truncate work_ReceiptMatch` | Execute SQL | `overwriteTable(silver.work_receipt_match)` |
| `stg Receipt` (RECEIPT_SQL: receipt ⋈ PO line ⋈ PO header, LEFT AP invoice line, LEFT `stg.MatchTolerance` by region+category, ISNULL defaults 2 / 3 / 1.00) | OLE DB Source | `receipt_matching.buildReceiptMatchInput` |
| `Derive Match Variances` (QuantityVariance, PriceVariance, IsInvoiced) | Derived Column | `deriveMatchVariances` |
| `Evaluate Match Result` (GRNI / MATCHED / QTYEXCEPT / PRICEEXCEPT, ReceiptAgeDays) | Derived Column | `evaluateMatchResult(df, businessDate)` — age vs BusinessDate instead of `GETDATE()` |
| `Split Match Outcome` (Matched / Grni / Exception) | Conditional Split | `splitMatchOutcome` → 3 filtered DataFrames |
| `Fact Purchase Receipt` (Matched branch) | branch destination | `toFactPurchaseReceipt(matched ⋈ supplier keys)` |
| `work ReceiptMatch` (Grni branch) / `Exceptions` (Exception branch → work) | branch destinations | whole evaluated set → `silver.work_receipt_match` (one table, `MatchResultCode` distinguishes branches) |
| `Count Matched` / `Count Rows Read` | Row Count | `count()` |
| `Accrue GRNI` (`INSERT Fact.[Purchase Receipt] ... WHERE ReceiptAgeDays <= ?`) | Execute SQL | `buildAccruals(grni, supplierKeys, AccrualCutoffDays)` unioned into the same MERGE (`match_result_code = 'ACCRUED'`) |
| `Raise Exceptions` (`INSERT etl.RejectedRecord`) | Execute SQL | `control.logRejectedRecordSet(spark, catalog, "stg.Receipt", exceptions, rejectStage="Mart", rejectReasonCode="MATCH_EXCEPTION", businessKeyColumn="ReceiptId")` |
| `Measure Match Outcomes` | Execute SQL | `measureMatchOutcomes` |
| `Log Row Counts` (`Fact.Purchase Receipt`) | Execute SQL | `control.logRowCount` |

### PRC_Load_SupplierScorecard
| SSIS component | Type | Spark construct |
|---|---|---|
| `Truncate work_SupplierScorecard` | Execute SQL | `overwriteTable(silver.work_supplier_scorecard)` |
| `Build Scorecard Measures` (`INSERT work.SupplierScorecard SELECT ... GROUP BY supplier, region` over `ScoringWindowDays`) | Execute SQL | `supplier_scorecard.buildScorecardMeasures(..., businessDate, windowDays)` — window ends at BusinessDate, not `SYSDATETIME()` |
| `work SupplierScorecard` source | OLE DB Source | the same DataFrame (cached) |
| `Lookup Scoring Weights` (`etl.SupplierScoringWeight`, ignore no-match) | Lookup (IG) | left join in `scoreSuppliers` |
| `Derive Percentages` (OnTime/Accuracy/Price/Quality %, default weights 0.4/0.2/0.2/0.2) | Derived Column | `scoreSuppliers` |
| `Derive Score Band` (SupplierScore, ScoreBandCode NODATA/A/B/C/D) | Derived Column | `scoreSuppliers` |
| `Aggregate Supplier Performance` | OLE DB Destination (append) | `toAggSupplierPerformance` + `mergeInto(gold.agg_supplier_performance, [calendar_month, supplier_key, region_code])` |
| `Count Unscored Suppliers` | Execute SQL | `countUnscoredSuppliers` |
| `Log Row Counts` (`Aggregate.Supplier Performance`) | Execute SQL | `control.logRowCount` |

### PRC_Load_ContractCompliance
| SSIS component | Type | Spark construct |
|---|---|---|
| `Truncate work_ContractCompliance` | Execute SQL | `overwriteTable(silver.work_contract_compliance)` |
| `Evaluate Contract Compliance` (INSERT work … CASE NON_PREFERRED / NO_CONTRACT / EXPIRED_CONTRACT / PRICE_LEAKAGE / COMPLIANT, LeakageAmount, `ComplianceWindowDays`, `PriceLeakageTolerance`) | Execute SQL | `contract_compliance.evaluateContractCompliance` (EXISTS → left-semi join flag) |
| `Measure Leakage` (LeakageAmount, ExpiredContractCount) | Execute SQL | `measureLeakage` |
| precedence `LeakageAmount > 0 \|\| ExpiredContractCount > 0` → `Publish Compliance To Scorecard` (`UPDATE Aggregate.[Supplier Performance]`) | Execute SQL, expression constraint | Python `if`, `summariseComplianceBySupplier` + `delta_io.updateMatched(gold.agg_supplier_performance, [calendar_month, supplier_key], [off_contract_spend_amount, leakage_amount, compliance_percent, ...])` |
| `Raise Non Compliant Lines` (`INSERT etl.RejectedRecord`) | Execute SQL | `nonCompliantLines` + `control.logRejectedRecordSet(..., "stg.VendorContract", rejectReasonCode="NON_COMPLIANT", businessKeyColumn="BusinessKey")` |
| `Log Row Counts` | Execute SQL | `control.logRowCount` |

### PRC_Export_SupplierStatement
| SSIS component | Type | Spark construct |
|---|---|---|
| `Truncate work_SupplierStatementLine` | Execute SQL | `overwriteTable(silver.work_supplier_statement_line)` |
| `Build Statement Lines` (`Fact.[Supplier Transaction]` ⋈ `Dimension.Supplier`, `FORMAT(date,'yyyy-MM') = @Period`, `ExcludeSelfBilling`, VAT only for EU) | Execute SQL | `supplier_statement.buildStatementLines` |
| `Compute Statement Balances` (`SUM() OVER (PARTITION BY SupplierId ORDER BY TransactionDate, SupplierStatementLineId)`) | Execute SQL | `computeRunningBalances` (Window) |
| `Build Statement File Name` (`"supplier_statement_" + @Period + ".csv"`) | Expression Task | `statementFileName` |
| `work SupplierStatementLine` source (ORDER BY SupplierId, TransactionDate) | OLE DB Source | `orderedStatementRows` |
| `Format Statement Row` (`RIGHT("0000000000"+SupplierId,10) + (DT_WSTR,10)Type + (DT_WSTR,30)Ref`, IncludesVatBlock) | Derived Column | `formatStatementRows` (`lpad` + `substring`, truncation without padding, as DT_WSTR does) |
| `Count Lines` | Row Count | `count()` |
| `Archive Statement` (`work.SupplierStatementArchive`, connection `WWI_Archive_Files`) | OLE DB Destination | `replaceWhere` into `silver.work_supplier_statement_archive` **and** CSV file on the outbound UC Volume + batch-suffixed copy on the archive Volume (the legacy flow never wrote an actual file — see §6) |
| `Count Statements` | Execute SQL | `countStatements` |
| `Log Row Counts` (`file:supplier_statement_<period>.csv`) | Execute SQL | `control.logRowCount` |

## 4. Control-framework calls → `dbx_etl_common`

| Legacy (T-SQL) | Notebook call |
|---|---|
| `etl.usp_LogPackageStart @BatchId, @PackageName, @PackageExecutionId OUTPUT` | `with control.packageRun(spark, catalog, batchId, "<PackageName>", projectName="WWI_Procurement", stepName="Procurement Mart") as run:` |
| `etl.usp_LogPackageEnd @Status='Succeeded', @RowsRead, @RowsInserted, @RowsUpdated, @RowsRejected` | `run.rowsRead / rowsInserted / rowsUpdated / rowsRejected` set inside the block; `packageRun` logs the end |
| `OnError` handler: `etl.usp_LogError` + `usp_LogPackageEnd @Status='Failed'` | `packageRun` on exception (`logError` + `Failed`, re-raised so the task fails) |
| `etl.usp_LogRowCount @ObjectName, @SourceRowCount, @TargetRowCount, @RejectRowCount` | `control.logRowCount(spark, catalog, run.packageExecutionId, objectName, sourceRowCount=, targetRowCount=, insertRowCount=, updateRowCount=, rejectRowCount=)` |
| `INSERT etl.RejectedRecord (...)` (ReceiptMatching, ContractCompliance) and the `err.*` redirect (PurchaseSpend) | `control.logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=, packageExecutionId=, sourceSystemCode="ORAERP", rejectStage="Mart", rejectReasonCode=, businessKeyColumn=)` |
| `$Package::BatchId` | `params.getJobParams(dbutils)["batchId"]` |
| table names | `naming.table(catalog, schema, table)` everywhere; nothing hard-coded |

The wheel is attached to every task as `libraries: - whl: `../../common/dbx_etl_common/dist/*.whl``
(session 00's build output) so the notebooks
`from dbx_etl_common import control, naming, params` exactly as the contract says. Local pytest uses the
fake in `tests/fakes/dbx_etl_common/` (same signatures, records calls). No session 00 PR was open when
this was written, so the imports follow the published interface contract verbatim.

## 5. Parameters

| Legacy parameter | Job parameter (string) | Default | Used by |
|---|---|---|---|
| `$Package::BatchId` | `BatchId` | `"0"` | all (batch scope `LoadBatchId = BatchId`, lineage columns) |
| — (`GETDATE()` / `SYSDATETIME()` in the packages) | `BusinessDate` | `${var.businessDate}` | receipt age, scoring / compliance windows, `calendar_month`, default `StatementPeriod` |
| `$Package::ReloadFullHistory` | `ReloadFullHistory` | `"False"` | accepted for contract compatibility; the marts are batch-scoped so it has no effect (see §6) |
| `$Project::EnvironmentCode` | `EnvironmentCode` | `${var.environmentCode}` | control framework |
| `RestartFromStep` | `RestartFromStep` | `""` | accepted; per-task restart is done with Databricks "repair run" |
| connection managers | `catalog` | `${var.catalog}` (= `wwi_${bundle.target}`) | all table names |
| `SpendCategoryScope` | `SpendCategoryScope` | `"ALL"` | PurchaseSpend (category filter; ALL = no filter) |
| `PriceVarianceTolerancePercent` | `PriceVarianceTolerancePercent` | `"5"` | PurchaseSpend (declared in the package but unused by its logic; logged only) |
| `AccrualCutoffDays` | `AccrualCutoffDays` | `"45"` | ReceiptMatching |
| `ScoringWindowDays` / `MinimumOrdersForScore` | same | `"90"` / `"5"` | SupplierScorecard |
| `ComplianceWindowDays` / `PriceLeakageTolerance` | same | `"30"` / `"2"` | ContractCompliance |
| `StatementPeriod` | `StatementPeriod` | `""` → month of BusinessDate (legacy `"1900-01"` yielded an empty file) | Export_SupplierStatement |
| `ExcludeSelfBilling` | `ExcludeSelfBilling` | `"True"` | Export_SupplierStatement |
| `$Project::ArchiveFileRoot` (`C:\WWI\DEV\archive`) | `OutboundVolumePath`, `ArchiveVolumePath` | `/Volumes/${catalog}/etl/outbound`, `/Volumes/${catalog}/etl/archive` | Export_SupplierStatement |
| — | `BaselineJson`, `BaselineTable`, `FailOnMismatch` | `""`, `""`, `"False"` | PRC_Reconcile_Procurement |

## 6. Not migrated / needs decision

1. **Package SQL vs. staging DDL column contract.** The package SQL (the spec migrated here) reads columns
   that do not exist in `sqlserver/staging/tables/21_stg_tables_finance.sql` (e.g. `pol.PurchaseOrderLineId`,
   `poh.SupplierId`, `pol.LegacyContractRef`, `pol.CategoryCode`, `vc.ContractPricePerOuter`, `vc.IsPreferred`,
   `r.LineNumber`, `ail.InvoicedOuters`, `stg.MatchTolerance`), while the DDL has `PurchaseOrderLineBusinessKey`,
   `SupplierBusinessKey`, `ReceivedQuantity`, … and no `stg.MatchTolerance` / `LegacyContractRef` at all. The
   notebooks implement the package contract (PascalCase columns listed in `src/procurement_lib/*.py`). **Decision:**
   whoever owns `silver.stg_*` (STG sessions) must either expose these columns (views are fine) or this
   project's source selects need a column-mapping pass once the real silver schema is known. Same for
   `gold.fact_supplier_transaction` (`transaction_type_code`, `transaction_reference`, `currency_code`, `tax_amount`
   are package names; the DDL has `Transaction Type Key`, `Transaction Currency Code`, tax split in two) and
   `gold.dim_supplier.is_self_billing` (not in `Dimension.Supplier.sql`).
2. **`Integration.usp_PostPurchaseSpend` is missing from `sqlserver/`.** The package calls it to move
   `work.PurchaseSpendLine` into `Fact.Purchase`; its body is unknown. Implemented as a MERGE on
   `(purchase_order_number, purchase_order_line_number)` carrying the legacy `Fact.Purchase` measures plus the
   spend-classification columns. `stock_item_key`, `vendor_contract_key`, `cost_center_key`, `warehouse_site_key`,
   `payment_terms_key` are **not** resolved (no lookup in the package); `wwi_stock_item_id` and
   `contract_number` are carried instead. Decision: confirm the proc's real behaviour or accept this shape.
3. **Wall-clock dates replaced by BusinessDate.** `GETDATE()` (receipt age) and `SYSDATETIME()` (scoring /
   compliance windows) are replaced by the `BusinessDate` job parameter so re-runs are deterministic. A
   catch-up run for an old BusinessDate therefore produces the figures *as of that date*, not as of today.
4. **Re-run semantics of `etl.rejected_record` rows.** The legacy packages appended reject rows on every run;
   `control.logRejectedRecordSet` does the same, so re-running a BatchId duplicates reject rows unless session 00's
   implementation de-duplicates on `(BatchId, ObjectName, BusinessKey)`. Facts / aggregates / work tables are idempotent.
5. **`Aggregate.Supplier Performance` grain.** Legacy appended one row per supplier per run with no month
   column in the package (the DDL grain is supplier × month × category). Migrated as `(calendar_month =
   month of BusinessDate, supplier_key, region_code)` with MERGE; category is not produced by the package.
6. **Statement file.** The legacy data flow ends in `work.SupplierStatementArchive` (an OLE DB destination on the
   `WWI_Archive_Files` connection); no flat-file destination existed, so there is no legacy byte-exact layout to
   reproduce. The export writes a header CSV with `StatementLineText` (the fixed-layout key from `Format Statement
   Row`) as the first column plus the statement columns (`STATEMENT_FILE_COLUMNS`), one file per period on the
   outbound Volume and a batch-suffixed archive copy. Decision: confirm column set / whether a header is wanted.
7. **`ReloadFullHistory` / `RestartFromStep`** are accepted but inert: the marts are scoped by `LoadBatchId`
   and by window, so a "full reload" is simply a run per historical BatchId / BusinessDate; per-task restarts use
   the job's repair-run.
8. **Repo offline check `validation/static/run_all_checks.py` fails on any `databricks/` output** (rule
   `forbidden-content` forbids the words *Databricks* / *dbutils* / *Unity Catalog* in every `.py/.md/.yml` outside
   the legacy sample dirs, and `# Databricks notebook source` is the mandatory notebook header). `validation/` is
   read-only for this session, so the carve-out for `databricks/**` and `docs/migration/**` has to land with
   session 00 / the parent (PR #18 shipped a similar carve-out on its branch). `run_deep_checks.py` passes.
9. **Local Delta MERGE test skipped offline.** `tests/test_reconciliation_and_delta_io.py::test_merge_into_is_idempotent`
   needs the delta-spark jars from Maven Central, which returned HTTP 429 from the build box; it self-skips when
   Delta is unavailable and runs wherever the jars are cached.
10. **`PriceVarianceTolerancePercent`** is declared by `PRC_Load_PurchaseSpend` but never referenced in its SQL or
    expressions; kept as a parameter and logged, not applied.
