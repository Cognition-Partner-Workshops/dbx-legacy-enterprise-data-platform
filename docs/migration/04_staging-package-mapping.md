# 04_staging (WWI_Staging) -> `databricks/04_staging` package mapping

Session 04 of the SSIS -> Databricks migration. Source project: `ssis/04_staging/` (generator
`ssis/04_staging/build_staging_packages.py`, 28 emitted `.dtsx`). Target: Databricks Asset Bundle
`wwi_04_staging` (`databricks/04_staging/databricks.yml`) with **one job `wwi_04_staging`** whose
28 tasks are keyed by legacy package name and run one source-format Python notebook each.

| Item | Value |
| --- | --- |
| Packages migrated | 28 (24 `STG_Load_*`, 4 `STG_Work_*`) |
| Bundle | `databricks/04_staging/databricks.yml` (`wwi_04_staging`, targets `dev` / `prod`) |
| Job | `databricks/04_staging/resources/wwi_04_staging.yml` |
| Notebooks | `databricks/04_staging/notebooks/<PackageName>.py` |
| Shared helpers | `databricks/04_staging/src/stg_common/` (`loader`, `expressions`, `transforms`, `refs`, `sources`, `work`) |
| Reconciliation | `databricks/04_staging/validation/STG_Reconcile_Staging.py` |
| Tests | `databricks/04_staging/tests/` (local PySpark + Delta; test-only fake of `dbx_etl_common` under `tests/fakes/`) |
| Control framework | `dbx_etl_common` (session 00, `databricks/common/dbx_etl_common`) consumed as a wheel library, never copied |

## 1. Parameters

Job parameters (all strings, names exactly as the naming contract) are resolved by
`params.getJobParams(dbutils)` from `dbx_etl_common`:

| Job parameter | Default | Typed key used by the notebooks |
| --- | --- | --- |
| `BatchId` | `"0"` | `p["batchId"]: int` — `0` means "start a batch" (`control.startBatch(..., batchName="WWI_Staging", batchType="Daily")`), otherwise the master job's batch is adopted |
| `BusinessDate` | `${var.businessDate}` (empty = today) | `p["businessDate"]: date` |
| `ReloadFullHistory` | `"False"` | `p["reloadFullHistory"]: bool` — watermark rewinds to the epoch |
| `EnvironmentCode` | `${var.environmentCode}` (`DEV`) | `p["environmentCode"]: str` |
| `RestartFromStep` | `""` | `p["restartFromStep"]: str` — a phase name (`Stage Load`, `Stage Work Tables`) or a package name; tasks ordered before it are skipped without a package-execution row (legacy `Master_Daily_ETL` restart semantics) |
| `catalog` | `${var.catalog}` = `wwi_${bundle.target}` | `p["catalog"]: str` — every table is `naming.table(catalog, schema, name)` |

Bundle variables: `catalog`, `warehouse_id`, `businessDate`, `environmentCode`, plus
job-cluster sizing. Legacy per-package parameters `SourceSystemCode`, `ObjectName`, `LookbackDays`
and the connection managers are constants inside each notebook (they were constants per package in
the generator too). Secrets are not needed: staging reads bronze Delta, not JDBC.

## 2. Package -> notebook / task, load semantics, dependencies

Load type is `docs/inventories/source-target-map.csv` / `ssis-packages.csv`; the "Delta write" column
is the `StagingRun` method that realises it. Every task runs on serverless job compute with the
`dbx_etl_common` wheel as an `environments` dependency. `depends_on` = the `stg.*` Lookup components inside the `.dtsx` plus
`docs/inventories/package-dependencies.csv`; the legacy orchestration plan ran the 24 loads as four
timing-ordered streams in phase "Stage Load" (seq 30) and the 4 work packages in "Stage Work Tables"
(seq 35) — the explicit edges make that ordering deterministic.

| Package (task_key) | Load type | Delta write | Watermark column | `depends_on` |
| --- | --- | --- | --- | --- |
| STG_Load_ApInvoice | incremental_append | `mergeByKey` (`InvoiceNumber`; `InvoiceNumber+LineNumber`) | `INVOICE_DT` | CostCenter, Currency, TaxAndTerms |
| STG_Load_CostCenter | truncate_reload | `truncateReload` | — | — |
| STG_Load_Currency | truncate_reload | `truncateReload` x2 | — | — |
| STG_Load_Customer | truncate_reload | `truncateReload` | — | — |
| STG_Load_CustomerAddress | truncate_reload | `truncateReload` | — | Geography |
| STG_Load_Employee | truncate_reload | `truncateReload` x2 | — | Currency |
| STG_Load_Geography | truncate_reload | `truncateReload` | — | PromotionAndTerritory |
| STG_Load_GlJournal | incremental_append | `mergeByKey` (`JournalId+LineNumber`) | `ACCOUNTING_DT` | — |
| STG_Load_LoyaltyLedger | incremental_append | `mergeByKey` (`LedgerEntryId`) | `EntryDate` | — |
| STG_Load_Order | incremental_append | `mergeByKey` (`OrderId`; `OrderLineId`) | `LastEditedWhen` | Customer |
| STG_Load_PartnerSale | incremental_append (per file batch) | `rebuildForBatch` | current `BatchId` file rows | — |
| STG_Load_Payment | incremental_append | `mergeByKey` (`PaymentId`) | `PAY_DT` | — |
| STG_Load_Product | truncate_reload | `truncateReload` | — | — |
| STG_Load_PromotionAndTerritory | truncate_reload | `truncateReload` x2 | — | — |
| STG_Load_PurchaseOrder | incremental_append | `mergeByKey` (`PurchaseOrderNumber`; `+LineNumber`) | `LAST_UPD_DT` | Currency, Work_ProductCrosswalk |
| STG_Load_ReturnAndCredit | incremental_append | `mergeByKey` (`ReturnLineId`; `CreditNoteId`) | `ReturnedWhen` / `IssuedWhen` | — |
| STG_Load_Sale | incremental_append | `mergeByKey` (`InvoiceId`; `InvoiceLineId`) | `LastEditedWhen` | Currency |
| STG_Load_Shipment | incremental_append | `mergeByKey` (`ShipmentId`; `ShipmentLineId`) | `DespatchedWhen` | — |
| STG_Load_StockItem | truncate_reload | `truncateReload` | — | — |
| STG_Load_StockMovement | incremental_append | `mergeByKey` (`StockItemTransactionId`) | `TransactionOccurredWhen` | — |
| STG_Load_Supplier | truncate_reload | `truncateReload` | — | TaxAndTerms |
| STG_Load_TaxAndTerms | truncate_reload | `truncateReload` x2 | — | — |
| STG_Load_VendorContract | truncate_reload | `truncateReload` | — | Currency, Supplier |
| STG_Load_WebSession | incremental_append | `mergeByKey` (`SessionId`) | `SessionStartWhen` | — |
| STG_Work_CustomerDedup | work_rebuild | `rebuildForBatch` x2 + `ref_source_key_crosswalk` retire/merge | — | Customer, CustomerAddress |
| STG_Work_InventoryPosition | work_rebuild | `rebuildForBatch` (90-day window) | — | StockItem, StockMovement |
| STG_Work_PaymentMatch | work_rebuild | `rebuildForBatch` | — | ApInvoice, Payment |
| STG_Work_ProductCrosswalk | work_rebuild | `rebuildForBatch` x2 | — | Product, StockItem |

Semantics of the three write shapes (`src/stg_common/loader.py`):

* `truncateReload` — `INSERT OVERWRITE` of the whole silver table (legacy `TRUNCATE TABLE` Execute SQL
  Task + OLE DB Destination). Re-running the same batch produces the same table.
* `mergeByKey` — the legacy `stg.usp_AppendIncremental_*` procedures do "delete the batch's keys then
  insert"; the Delta equivalent is `MERGE INTO ... WHEN MATCHED THEN UPDATE SET <cols> WHEN NOT
  MATCHED THEN INSERT`, deduplicated on the business key first (latest change stamp wins). Idempotent per
  `BatchId` and safe under the watermark lookback re-read.
* `rebuildForBatch` — `DELETE WHERE BatchId = :batchId` followed by an append (the work-table
  procedures' "rebuild for this batch" pattern); `STG_Work_InventoryPosition` deletes the rolling
  90-day window instead, as `work.usp_BuildInventoryPositionDaily` does.

## 3. Source -> target objects

Legacy name -> Delta name follows the naming contract (`raw.X` -> `${catalog}.bronze.raw_x`, `stg.X`
-> `${catalog}.silver.stg_x`, `work.X` -> `silver.work_x`, `err.X` -> `silver.err_x`, `ref.X` ->
`silver.ref_x`). Bronze extracts keep the extract sessions' column names; `src/stg_common/sources.py`
(`RAW_COLUMN_MAP`, `conformSource`) adds the package-vocabulary aliases the generator used (for example
`raw.OracleCostCenter.CC_CODE` = bronze `COST_CENTER_CD`) so the notebooks read like the `.dtsx`.

| Package | Bronze sources (`raw.*` -> `bronze.raw_*`) | Reference / silver lookups | Silver targets | Error tables |
| --- | --- | --- | --- | --- |
| STG_Load_ApInvoice | OracleApInvoiceHdr, OracleApInvoiceLine | ref_payment_terms, stg_cost_center, stg_fx_rate, stg_tax_rate | stg_ap_invoice, stg_ap_invoice_line | err_rejected_invoice_line, err_rejected_lookup_failure |
| STG_Load_CostCenter | OracleCostCenter | self (parent hierarchy) | stg_cost_center | err_rejected_lookup_failure |
| STG_Load_Currency | OracleCurrency, OracleFxRate, FileFxOverride | — | stg_currency, stg_fx_rate | err_rejected_constraint_violation |
| STG_Load_Customer | OracleCustomerMaster | ref_country | stg_customer | err_rejected_customer |
| STG_Load_CustomerAddress | OracleCustomerAddress | stg_geography | stg_customer_address | err_rejected_customer |
| STG_Load_Employee | SqlOrder, SqlPerson, SqlSalespersonQuota | stg_fx_rate (AVG) | stg_employee, stg_salesperson | err_rejected_constraint_violation |
| STG_Load_Geography | OracleGeography | stg_sales_territory | stg_geography | err_rejected_lookup_failure |
| STG_Load_GlJournal | OracleGlJournalLine | ref_gl_account, stg_fx_rate | stg_gl_journal_line | err_rejected_lookup_failure, err_rejected_constraint_violation |
| STG_Load_LoyaltyLedger | SqlLoyaltyLedger | ref_loyalty_tier, ref_code_crosswalk | stg_loyalty_ledger | err_rejected_lookup_failure |
| STG_Load_Order | SqlOrder, SqlOrderLine | stg_customer | stg_order, stg_order_line | err_rejected_order_line, err_rejected_lookup_failure |
| STG_Load_PartnerSale | FilePartnerSales | ref_country, ref_code_crosswalk (PARTNER_CUSTOMER) | stg_partner_sale | err_rejected_file_row |
| STG_Load_Payment | OracleApPayment | ref_code_crosswalk (PAYMENT_METHOD) | stg_payment | err_rejected_payment |
| STG_Load_Product | OracleProductMaster | ref_uom_conversion | stg_product | err_rejected_product |
| STG_Load_PromotionAndTerritory | SqlOrder, SqlPromotion, SqlSalesTerritory | ref_country, ref_code_crosswalk | stg_promotion, stg_sales_territory | err_rejected_constraint_violation, err_rejected_lookup_failure |
| STG_Load_PurchaseOrder | OraclePurchaseOrderHdr, OraclePurchaseOrderLine | stg_fx_rate, ref_uom_conversion, work_product_crosswalk | stg_purchase_order, stg_purchase_order_line | err_rejected_lookup_failure |
| STG_Load_ReturnAndCredit | SqlReturnLine, SqlCreditNote | ref_code_crosswalk (RETURN_REASON) | stg_return, stg_credit_note | err_rejected_constraint_violation |
| STG_Load_Sale | SqlInvoice, SqlInvoiceLine | stg_fx_rate (SPOT) | stg_sale, stg_sale_line | err_rejected_invoice_line |
| STG_Load_Shipment | SqlShipment, SqlShipmentLine | ref_carrier | stg_shipment, stg_shipment_line | err_rejected_lookup_failure |
| STG_Load_StockItem | SqlStockItem | — | stg_stock_item | err_rejected_constraint_violation |
| STG_Load_StockMovement | SqlStockMovement | ref_transaction_type, ref_uom_conversion | stg_stock_movement | err_rejected_constraint_violation |
| STG_Load_Supplier | OracleSupplierMaster | stg_payment_terms | stg_supplier | err_rejected_supplier |
| STG_Load_TaxAndTerms | OracleTaxRate, OraclePaymentTerms | ref_code_crosswalk (PAYMENT_TERMS) | stg_tax_rate, stg_payment_terms | err_rejected_constraint_violation |
| STG_Load_VendorContract | OracleVendorContract | stg_supplier, stg_fx_rate (CONTRACT) | stg_vendor_contract | err_rejected_supplier |
| STG_Load_WebSession | SqlWebSession | ref_country | stg_web_session | err_rejected_constraint_violation |
| STG_Work_CustomerDedup | — (reads silver) | stg_customer, stg_customer_address | work_customer_dedup, work_customer_address_standardized, ref_source_key_crosswalk | err_rejected_customer |
| STG_Work_InventoryPosition | — | stg_stock_item, stg_stock_movement | work_inventory_position_daily | err_rejected_constraint_violation |
| STG_Work_PaymentMatch | — | stg_payment, stg_ap_invoice | work_payment_matched | err_rejected_payment |
| STG_Work_ProductCrosswalk | FileSupplierCatalog | stg_product, stg_stock_item, ref_source_key_crosswalk | work_product_crosswalk, work_product_crosswalk_feed | err_rejected_lookup_failure |

Every reject also lands in `etl.rejected_record` through `control.logRejectedRecordSet` (one control
row per rejected business key — the behaviour the legacy `reject_sweep` cursor guaranteed).

## 4. SSIS construct -> Spark construct

| SSIS construct (from `build_staging_packages.py` / `.dtsx`) | Spark / `dbx_etl_common` equivalent |
| --- | --- |
| Package parameters + `User::PackageExecutionId`, `User::WatermarkFrom/To` variables | `params.getJobParams(dbutils)`; `StagingRun` attributes (`packageExecutionId`, `watermarkFrom`, `watermarkTo`) |
| `Log Package Start` (`etl.usp_LogPackageStart`) / `Log Package Success` / OnError handler (`usp_LogPackageEnd Failed` + `usp_LogError`) | `with control.packageRun(spark, catalog, batchId, packageName, projectName="WWI_Staging", stepName=...) as pkg:` — success/failure/logError on exit (`StagingRun.__enter__/__exit__`) |
| `Get Watermark` (`etl.usp_GetWatermark`) / `Set Watermark` (`etl.usp_SetWatermark`) | `control.getWatermark(...)` before the flow; `control.setWatermark(..., max(change stamp))` after a successful write, `watermarkTo` recorded on the package run |
| `Truncate <table>` Execute SQL Task | `StagingRun.truncateReload` (`INSERT OVERWRITE`) |
| OLE DB Source `SELECT ... FROM raw.X WHERE ChangeCol > ? AND ChangeCol <= ?` | `StagingRun.bronzeWatermarked(name, changeColumn)` (`applyWatermark` predicate, epoch on `reloadFullHistory`) |
| OLE DB Source `... WHERE BatchId = ?` (file feeds) | `StagingRun.bronze(name, currentBatchOnly=True)` |
| Derived Column (trim / upper / `(DT_*)` casts / `REPLACENULL` / `DATEADD`) | `withColumns` over `stg_common.expressions` (`safeDecimal`, `safeDate`, `standardizePostalCode`, `regionCase`, ...) and `stg_common.transforms.*` |
| Derived Column `hash_expression(...)` (pipe-joined `UPPER(TRIM(...))`) | `expressions.changeHash(*cols)` — the same pipe-joined `upper(trim(cast(col as string)))` string, NULL-propagating like SSIS concatenation |
| Lookup (full cache, `FailComponent` -> error output) | `loader.lookupLeft(df, ref, on=..., outputs=...)` + `splitByCondition` on the null output -> `rejectLookupFailures` |
| Lookup with *ignore failure* | `loader.lookupIgnore` (left join, `coalesce` to the package default, e.g. FX 1.0, UoM factor 1, region `ROW`) |
| Conditional Split (valid / reject branches) | `loader.splitByCondition(df, cond)` -> `(matched, unmatched)`; reject branch -> `StagingRun.rejectRows` / `rejectConstraint` |
| Union All (two inputs, e.g. FxRate + FileFxOverride) | `DataFrame.unionByName(..., allowMissingColumns=True)` |
| Sort + Aggregate / "survivorship" (keep latest per key) | `Window.partitionBy(key).orderBy(desc(stamp))` + `row_number() == 1` (`transforms.survivorshipDedupe`) |
| Row Count -> `Log Row Count` (`etl.usp_LogRowCount`) | `StagingRun` counters -> `control.logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount, targetRowCount, insertRowCount, ...)` |
| Error output -> `err.*` OLE DB Destination + `Register Rejects` cursor (`etl.usp_LogRejectedRecord` per row) | append to `silver.err_*` with `BatchId`, `RejectReasonCode`, `RecordPayload` (`expressions.jsonPayload`) and `control.logRejectedRecordSet(spark, catalog, objectName, rejectedDf, ...)` |
| `stg.usp_TranslateSourceCodes` | `refs.translateSourceCodes` (join to `ref_code_crosswalk` by code set + source system) |
| `stg.usp_ConvertCurrencyAmounts @RateTypeCode` | `refs.convertCurrencyAmounts` (effective-dated `stg_fx_rate` join, USD triangulation, `*Rate`, `*RateResolutionCode` columns) |
| `stg.usp_NormalizeCustomer` / `usp_NormalizeSupplier` / `usp_NormalizeAddress` / `usp_CleanStringBatch` | folded into the derived columns (`transforms.cleanseCustomer`, `deriveSupplierHash`, `standardizeAddress`, trimming helpers) |
| `stg.usp_AppendIncremental_*` (delete keys of the batch, insert) | `StagingRun.mergeByKey` (Delta `MERGE INTO`) |
| `stg.usp_TruncateAndReload_Geography` (hierarchy + surrogate key) | `transforms.cleanseGeography` + `truncateReload`; `GeographyKey` = `row_number()` over the region/country/state/city ordering (surrogate regenerated on every reload, as the procedure's identity reseed did) |
| `stg.usp_DeduplicateCustomer` (work.CustomerDedup) | `work.scoreCustomerSurvivorship` (EXACT_TAXNUM > NAME_POSTAL > NAME_FUZZY blocking, `SourceRank` ORA_ERP 30 / WWI_OLTP 20 / WWI_WEB 10 / other 5, completeness, recency, EU consent tie-break, lowest business key) + `ref_source_key_crosswalk` retirement |
| `work.usp_BuildProductCrosswalk` | `work.resolveProductCrosswalk` (MANUAL_XREF 100 > BARCODE 95 with ambiguity > NAME token overlap >= 0.80 within brand > UNMATCHED; `CandidateCount`, `ResolvedFlag`) |
| `work.usp_BuildInventoryPositionDaily` | `work.rollForwardInventory` (per item/warehouse day spine, `sum() over (rows unbounded preceding)`, `last(cost, ignorenulls)` carry-forward, 28-day days-of-cover, negative / break flags, outlier rejects) |
| `work.usp_MatchPaymentsToInvoices` | `work.allocatePayments` (pass 1 remittance reference, pass 2 EXACT_AMT incl. discount, pass 3 RESIDUAL oldest-first with EU 0.01 / APAC 0.5% / else 0.02 tolerance, UNAPPLIED remainder -> `err_rejected_payment`) |
| Precedence constraints / `Master_Daily_ETL` phases | job `depends_on` edges; `RestartFromStep` short-circuit in `StagingRun` |

## 5. Control-framework calls per notebook

Every notebook shares the same skeleton (generated header, see any `notebooks/STG_*.py`):

```python
# Databricks notebook source
from dbx_etl_common import control, naming, params
from stg_common.loader import StagingRun, ...
p = params.getJobParams(dbutils)
run = StagingRun(spark, dbutils, "<PackageName>", sourceSystemCode=..., objectName="stg.X", watermark=<bool>, jobParams=p)
with run:                       # control.packageRun(...) [+ control.startBatch when BatchId == 0]
    df = run.bronzeWatermarked(...) / run.bronze(...)         # control.getWatermark on __enter__
    ...transforms / lookups / splits...
    run.rejectRows(...) / run.rejectLookupFailures(...)       # err_* append + control.logRejectedRecordSet
    run.mergeByKey(...) / run.truncateReload(...) / run.rebuildForBatch(...)
run.finish()                    # control.logRowCount, control.setWatermark, packageRun exit -> logPackageEnd
```

`naming.table(catalog, "bronze"|"silver", name)` is the only way a table name is built. Nothing in
`databricks/common/` is copied; the tests use a minimal fake under `tests/fakes/dbx_etl_common/` that
implements only the functions above with the shipped session-00 signatures (PR
[#38](https://github.com/Cognition-Partner-Workshops/dbx-legacy-enterprise-data-platform/pull/38)
was checked: `params.getJobParams` widget names, `getWatermark` returning `(watermarkFrom,
watermarkTo)` strings and `logRejectedRecordSet` accepting an arbitrary reject frame all match).

## 6. Reconciliation (`validation/STG_Reconcile_Staging.py`)

For every silver table written by this job the notebook computes `RowCount` and an
order-independent `xxhash64` content hash (`sum(xxhash64(concat_ws('|', <sorted business columns>)))`,
runtime columns such as `BatchId`, `LoadedAt`, `PackageExecutionId` excluded), scoped to the batch /
business date where the table carries them. Baselines from SQL Server (`validation/runtime/*.sql`
ported to the same hash) are read from a Delta table (`baselineTable`) or a JSON parameter
(`baselineJson`); each table is classified `MATCH`, `COUNT_MISMATCH`, `HASH_MISMATCH`, `NO_BASELINE`
or `MISSING_TABLE` and written to `etl.row_count_log` through `control.logRowCount`
(`sourceRowCount` = baseline, `targetRowCount` = Delta). `failOnMismatch` raises after logging.

## 7. Validation performed (no workspace execution)

* `python3 -m py_compile` on all 28 notebooks + the reconciliation notebook.
* `pytest databricks/04_staging/tests` — transformation, work-algorithm and loader unit tests plus a
  smoke test that executes all 28 notebooks against empty Delta tables created from the legacy DDL
  (`tests/ddl_fixtures.py` parses `sqlserver/staging/tables/*.sql`).
* `databricks bundle validate -t dev` and `--strict` (PAT auth from the demo secrets).
* `python3 validation/checks/run_deep_checks.py` — passes.
* `python3 validation/static/run_all_checks.py` — see "needs decision" below.

## 8. Not migrated / needs decision

| Item | Status / why |
| --- | --- |
| `validation/static/run_all_checks.py` `forbidden-content` check | Fails on every file under `databricks/04_staging` (and will on every session's output) because it rejects the words `databricks` / `dbutils` in `.py`/`.md`/`.yml` anywhere in the repo. `validation/` is read-only for this session; PR [#18](https://github.com/Cognition-Partner-Workshops/dbx-legacy-enterprise-data-platform/pull/18) proposes a `MIGRATION_TARGET_DIRS` exemption. Decision: land that exemption (session 00 / parent) or accept the failure. |
| Watermark exchange with the master job | `RestartFromStep` and `BatchId` adoption follow the interface contract; `Master_Daily_ETL`'s four-stream parallelism is replaced by explicit task edges (same phase order, deterministic). |
| `STG_Load_PurchaseOrder` and `work.usp_BuildProductCrosswalk @OnlyMissing=1` | The legacy package rebuilt missing crosswalk rows inline before its own data flow. The notebook instead depends on the `STG_Work_ProductCrosswalk` task and reads `work_product_crosswalk` (empty-shaped frame if the table does not exist yet). Same rows resolved, no duplicated crosswalk logic. |
| Per-row reject cursor (`reject_sweep`) | Replaced by one set-based `control.logRejectedRecordSet` call per reject stream; still one `etl.rejected_record` row per rejected business key. |
| Bronze column vocabulary | Extract sessions (01/02/03) own `bronze.raw_*` schemas. `sources.RAW_COLUMN_MAP` aliases the columns the staging generator expected (`CC_CODE`, `CUST_CODE`, `SessionGuid`, ...); if an extract session ships different names the map is the single place to adjust. Columns the raw DDL never had (`REGION_CD` on currency, `COUNTRY_CD` on customer master, `NET_WEIGHT`/`WEIGHT_UOM_CD` on product master) are nulled and hit the package defaults. |
| `stg.usp_ConvertCurrencyAmounts` batching (`@BatchSize`, scratch table) | Set-based join in `refs.convertCurrencyAmounts`; the `RateResolutionCode` / `*Rate` audit columns are preserved, the T-SQL loop is not. |
| Fuzzy customer blocking (`NAME_FUZZY` rule) | Implemented as the procedure's blocking key `LEFT(normalised name, n) + country` (`fuzzyPrefixLength`), not SSIS Fuzzy Grouping (the package did not use that component), so no fidelity loss. |
| Wheel path | `libraries: - whl: `../../common/dbx_etl_common/dist/*.whl`` (session 00's build output). Session 00 must build the wheel before `bundle deploy`; alternative `%pip install` from the workspace path was not chosen. |
| Compute | Serverless job compute (`environments` block, `client: "3"`); the wheel is uploaded with the bundle and referenced as an environment dependency. The workspace is serverless-only. |
| Runtime reconciliation against SQL Server | Not executed (no workspace / SQL Server run allowed in this session); the notebook is ready once baseline figures are captured. |
