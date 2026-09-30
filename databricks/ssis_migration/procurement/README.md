# SSIS → Databricks migration — group `procurement` (procure-to-pay)

Databricks-native replacement for the 22 procurement SSIS packages of the WideWorldImporters estate
(suppliers, purchase orders, receipts, vendor contracts, supplier transactions, supplier catalog feed,
purchase facts, PRC_* marts, supplier statement export and the supplier-performance aggregate).

| item | value |
|---|---|
| landing schema | `otterorders_migration.ssis_procurement` (all bronze/silver/gold/ctl/err tables, volumes `landing` and `exports`) |
| evidence | `otterorders_migration.evidence.recon_results`, `branch = 'ssis_procurement'`, `actor = 'devin:ssis_procurement'` |
| bundle | `ssis_procurement` → `/Workspace/Shared/ssis_migration/procurement` (`databricks bundle deploy -t dev`) |
| job | `ssis_procurement_daily` (serverless; tasks `setup_landing → extract → stage → dimension → fact → mart → recon`) |
| sources | Lakehouse Federation only: `wwi_legacy_oracle` (wwi_mdm / wwi_proc / wwi_fin / wwi_ref), `wwi_legacy_oltp`, `wwi_legacy_dw`, `wwi_legacy_staging`; read-only `remote_query` for legacy names with spaces |

Implementation choice: **PySpark library + thin notebooks + DAB job** (one style for the whole group, following
`databricks/CONVENTIONS.md` of the sales-lakehouse branch). All logic lives in `src/procurement/`, the notebooks
only bootstrap `sys.path`, resolve widgets and call one entry point. Lakeflow Declarative Pipelines were not used
because half the packages are watermark/SCD/replay logic that needs explicit control-table reads and writes
(`ctl_watermark`, `ctl_batch`, `ctl_package_run`, `err_rejected_row` mirror `etl.*` / `err.*` on the host).

```
databricks.yml, resources/procurement_job.yml   bundle + job (job run id = SSIS batch id shared by all packages)
notebooks/_bootstrap.py                         sys.path + widgets
notebooks/setup_landing.py                      volumes, control tables, sample catalog files -> landing volume
notebooks/run_packages.py                       runs one layer (extract|stage|dimension|fact|mart) or a package list
notebooks/recon.py                              evidence writer (recon task)
src/procurement/config.py                       names, package inventory (22), SSIS project parameters
src/procurement/io.py                           Delta/federation IO, watermarks, batch/run log, rejects
src/procurement/extracts.py                     EXT_ORA_* / EXT_SQL_*        -> bronze_*
src/procurement/catalog_ingest.py               ING_FILE_SupplierCatalog     -> bronze_file_supplier_catalog
src/procurement/staging.py                      STG_Load_*                   -> silver_*
src/procurement/quality.py                      DQ_Supplier_Screen           -> silver_dq_supplier_result / err_rejected_row
src/procurement/dimensions.py                   DIM_Load_* (SCD)             -> gold_dim_*
src/procurement/facts.py                        FACT_Load_*                  -> gold_fact_*
src/procurement/marts.py                        PRC_*                        -> gold_fact_* / gold_agg_* / exports volume
src/procurement/aggregates.py                   AGG_Refresh_SupplierPerformance -> gold_agg_supplier_performance
src/procurement/recon.py                        22 ReconSpecs -> evidence.recon_results
src/procurement/pipeline.py                     package name -> entry point, layer order
samples/supplier_catalog/*.psv                  representative HDR/DTL/TRL feed files (one deliberately fails the footer check)
tests/                                          pytest on local Spark (14 tests)
```

## Package → artifact mapping

Verdicts are from the latest evidence run (see "Evidence" below). `PARTIAL` = the legacy target is empty on the
host, so the check is against a source-derived expectation (`"baseline":"source_derived"` in `checks`).

| # | package | load type | source (federated) | target Delta table | entry point | verdict |
|---|---|---|---|---|---|---|
| 1 | `EXT_ORA_SupplierMaster` | incremental_timestamp (`UPDATED_DT` watermark) | `wwi_legacy_oracle.wwi_mdm.supp_master`, `supp_certification` | `bronze_ora_supp_master` | `extracts.runSupplierMaster` | PARTIAL |
| 2 | `EXT_ORA_PurchaseOrderHdr` | incremental_timestamp | `wwi_proc.purchase_order_hdr`, `wwi_ref.fx_rate_daily` | `bronze_ora_purchase_order_hdr` | `extracts.runPurchaseOrderHdr` | PARTIAL |
| 3 | `EXT_ORA_PurchaseOrderLine` | incremental_key (`PO_LINE_ID`) | `wwi_proc.purchase_order_line` | `bronze_ora_purchase_order_line` | `extracts.runPurchaseOrderLine` | PARTIAL |
| 4 | `EXT_ORA_ReceiptLine` | incremental_key (`RECEIPT_LINE_ID`) | `wwi_proc.po_receipt_line` + `po_receipt_hdr` + PO line/hdr | `bronze_ora_receipt_line` | `extracts.runReceiptLine` | PARTIAL |
| 5 | `EXT_ORA_VendorContract` | full (truncate/reload) | `wwi_proc.vendor_contract`, `vendor_contract_line` | `bronze_ora_vendor_contract` | `extracts.runVendorContract` | PARTIAL |
| 6 | `EXT_SQL_SupplierTransactions` | incremental_key (`SupplierTransactionID`) | `wwi_legacy_oltp.Purchasing.SupplierTransactions` + `Suppliers` + `Application.TransactionTypes` | `bronze_sql_supplier_transaction` | `extracts.runSupplierTransactions` | PARTIAL |
| 7 | `ING_FILE_SupplierCatalog` | file_ingest | `/Volumes/otterorders_migration/ssis_procurement/landing/inbound/supplier/*.psv` | `bronze_file_supplier_catalog` (+ `ctl_landing_file`, `err_rejected_file_row`) | `catalog_ingest.runSupplierCatalog` | PARTIAL |
| 8 | `STG_Load_Supplier` | truncate_reload + survivorship | `bronze_ora_supp_master` | `silver_supplier` | `staging.runSupplier` | PARTIAL |
| 9 | `STG_Load_PurchaseOrder` | incremental_append | `bronze_ora_purchase_order_hdr/_line`, `product_master`, `product_uom_conv`, `payment_terms` | `silver_purchase_order`, `silver_purchase_order_line` | `staging.runPurchaseOrder` | PARTIAL |
| 10 | `STG_Load_VendorContract` | truncate_reload | `bronze_ora_vendor_contract`, `silver_supplier`, `fx_rate_daily` | `silver_vendor_contract` | `staging.runVendorContract` | PARTIAL |
| 11 | `DQ_Supplier_Screen` | quality_screen | `silver_supplier` | `silver_dq_supplier_result`, `err_rejected_row` (`err.RejectedSupplier`) | `quality.runSupplierScreen` | PARTIAL |
| 12 | `DIM_Load_Supplier` | hybrid SCD (Type 2 + Type 1 attributes) | `silver_supplier`, seeded from `wwi_legacy_dw.Dimension.Supplier` | `gold_dim_supplier` | `dimensions.runDimSupplier` | PARTIAL |
| 13 | `DIM_Load_VendorContract` | SCD2 (amendments) | `silver_vendor_contract`, `gold_dim_supplier` | `gold_dim_vendor_contract` | `dimensions.runDimVendorContract` | PARTIAL |
| 14 | `FACT_Load_Purchase` | incremental_fact (`LastEditedWhen` watermark, replay by PO line) | `wwi_legacy_oltp.Purchasing.PurchaseOrders/Lines`, `Warehouse.StockItems/PackageTypes`, `wwi_legacy_dw.Dimension.Supplier/Stock Item` | `gold_fact_purchase` (legacy grain, 8,367 rows) + `silver_purchase` / `gold_fact_purchase_p2p` (Oracle extension) | `facts.runFactPurchase` | see Evidence |
| 15 | `FACT_Load_PurchaseReceipt` | incremental_fact (`receipt_line_id` watermark) | `bronze_ora_receipt_line`, `silver_purchase_order(_line)`, `gold_dim_supplier` | `gold_fact_purchase_receipt` | `facts.runFactPurchaseReceipt` | PARTIAL |
| 16 | `FACT_Load_SupplierTransaction` | incremental_fact + accrual reversals | `bronze_sql_supplier_transaction`, `gold_dim_supplier` | `silver_supplier_transaction`, `gold_fact_supplier_transaction` | `facts.runFactSupplierTransaction` | PARTIAL |
| 17 | `PRC_Load_PurchaseSpend` | business_rule (rebuild) | `silver_purchase_order(_line)`, `silver_vendor_contract`, `silver_supplier` | `gold_fact_purchase_spend` | `marts.runPurchaseSpend` | PARTIAL |
| 18 | `PRC_Load_ReceiptMatching` | business_rule (rebuild) | `gold_fact_purchase_receipt`, `wwi_fin.ap_invoice_line/_hdr` | `gold_fact_receipt_matching` | `marts.runReceiptMatching` | PARTIAL |
| 19 | `PRC_Load_ContractCompliance` | business_rule (rebuild) | `gold_fact_purchase_spend`, `silver_vendor_contract` | `gold_agg_contract_compliance` | `marts.runContractCompliance` | PARTIAL |
| 20 | `PRC_Load_SupplierScorecard` | business_rule (rebuild) | receipts, matching, spend, `silver_supplier` | `gold_agg_supplier_scorecard` | `marts.runSupplierScorecard` | PARTIAL |
| 21 | `PRC_Export_SupplierStatement` | business_rule (outbound file) | `gold_fact_supplier_transaction`, `gold_dim_supplier` | `gold_supplier_statement` + `/Volumes/.../exports/supplier_statement/supplier_statement_<yyyyMM>.csv` | `marts.runSupplierStatement` | PARTIAL |
| 22 | `AGG_Refresh_SupplierPerformance` | aggregate_rebuild | gold facts/marts + `gold_dim_supplier` | `gold_agg_supplier_performance` | `aggregates.runAggregateSupplierPerformance` | PARTIAL |

Control / support tables: `ctl_batch`, `ctl_package_run`, `ctl_watermark`, `ctl_landing_file`, `err_rejected_row`,
`err_rejected_file_row`. Every package writes one `ctl_package_run` row per batch (rows read/inserted/rejected,
watermark window) — the `etl.PackageRun` equivalent.

## Legacy baseline facts that shape the verdicts

Profiled read-only through federation before implementing:

* `WideWorldImportersDW.Fact.Purchase` is populated (**8,367 rows**, order dates 2013-01-01 … 2016-05-31, 7 supplier
  keys, 227 stock-item keys, 2,074 POs). Every other procurement target on the host is empty:
  `raw.OracleSupplierMaster/PurchaseOrderHdr/PurchaseOrderLine/ReceiptLine/VendorContract`, `raw.FileSupplierCatalog`,
  `stg.Supplier/PurchaseOrder/PurchaseOrderLine/Receipt/VendorContract/Purchase/PurchaseReceipt/SupplierTransaction`,
  `err.RejectedSupplier`, `Dimension.[Vendor Contract]`, `Fact.[Purchase Receipt]`, `Fact.[Supplier Transaction]`,
  `Aggregate.[Supplier Performance]`, `etl.SupplierScoringWeight`.
* `raw.SqlInvoice` (the inventoried target of `EXT_SQL_SupplierTransactions`) holds 2,820 `Sales.Invoices` rows written by
  a sibling package, not supplier transactions — so there is no legacy baseline for that package either.
* `Dimension.Supplier` has 30 rows (13 WWI suppliers as versions + special members -2/-1/0). It is **seeded** into
  `gold_dim_supplier` with its surrogate keys preserved so `Fact.Purchase` keys resolve identically; Oracle suppliers
  (`wwi_mdm.supp_master`, 60 rows) are added as new business keys on top.
* No `supplier_catalog_*.psv` files exist in the repo or on the host; three representative files were generated from
  `config/landing-zone.yaml` / `tools/landing/render_feed_manifest.py` and committed under `samples/`.

Consequently only `FACT_Load_Purchase` can reach `PASS`; the other 21 packages are checked against a
source-derived expectation computed independently in `recon.py` and are `PARTIAL` by contract.

## Design decisions

* **Batch id** = Databricks job run id (`{{job.run_id}}`), written to `ctl_batch` and every row's `batch_id`, so a
  batch can be traced across all 22 packages like the SSIS `Master_Daily_ETL` batch.
* **Watermarks** live in `ctl_watermark` (`source_system`, `object_name`, `watermark_value`, kind `timestamp` | `key`),
  read at the start and advanced only after the successful write — the `etl.Watermark` / `etl.usp_SetWatermark`
  pattern. First run starts at `1900-01-01` / key `0` (full history), second run extracts only the delta.
* **Extracts are federated `SELECT`s** with the SSIS source query re-expressed in PySpark (status filters, dormant/deleted
  supplier exclusion, FX conversion via `fx_rate_daily` = `FN_CONVERT_AMOUNT`, receipt variance = `FN_RECEIPT_VARIANCE_PCT`)
  plus the SSIS audit trailer columns (`source_system_code`, `extracted_at_utc`, `package_execution_id`).
* **Staging** keeps the SSIS conditional-split semantics: PO lines with non-positive quantity/negative price go to
  `err_rejected_row` (`err.RejectedLookup`), contracts with inverted dates / unknown supplier / missing FX rate are
  rejected, supplier survivorship keeps the latest `UPDATED_DT` per supplier number and marks the losers
  `is_survivor_row = false` (`stg.usp_DeduplicateSupplier`). Missing payment terms default to `NET30`, tax ids are
  normalised (`upper`, strip `-`/space, missing → `NONE`).
* **DQ gate** implements the five screen rules in package order; rejects (`DUPLICATE_TAX_ID`, `MISSING_TAX_ID`) go to
  `err_rejected_row` with `reject_target = err.RejectedSupplier`, warnings stay as flags in `silver_dq_supplier_result`.
* **SCD**: `gold_dim_supplier` is the hybrid Type 2 (region, status, tier, payment terms …) / Type 1 (name, contact …)
  split of `usp_MigrateStagedSupplierDataV2`; `gold_dim_vendor_contract` is plain SCD2 keyed on `row_hash_type2` with
  `amendment_number`. Dimensions are rewritten as a whole through a `__work` table (`work.*` pattern) because they are
  tiny and serverless does not allow `cache()`.
* **Late-arriving / unknown members**: facts resolve the supplier key from the current dimension row and fall back to
  key `0` with `inferred_member_flag = true`; the P2P purchase fact holds rows without a resolvable supplier in
  `silver_purchase` with `dq_status_code = 'FAIL'` (the SSIS hold queue) instead of dropping them.
* **`FACT_Load_Purchase`** reproduces the legacy `Integration.GetPurchaseUpdates` → `MigrateStagedPurchaseData` lineage
  from WWI OLTP (`PurchaseOrderLines` × `StockItems` × `PackageTypes`, supplier and stock-item keys from the legacy
  dimensions with `Valid From <= order date < Valid To`) into `gold_fact_purchase`, and additionally builds the
  Oracle-sourced `gold_fact_purchase_p2p` (regional landed-cost rule, apportioned freight, three-way match state).
  Replays delete-and-reinsert the touched PO lines.
* **Supplier transactions** keep the "Post Accrual Reversals" step: every accrual without a reversal gets an `ACCREV` row
  with the negated amount.
* **Marts** are full rebuilds per run (as the PRC_* packages truncate/reload their targets); tolerances and windows are
  the SSIS project parameters (`config.PARAMS`: qty 2 %, price 1 %, GRNI 45 days, scorecard 90 days / 5 orders,
  compliance 365 days, dormant supplier 84 months).
* **Supplier statement export** is a Delta table (`gold_supplier_statement`) plus one CSV per statement period under
  the `exports` volume in the procurement schema (`file:supplier_statement.csv` equivalent).
* **Reconciliation** (`recon.py`): one `ReconSpec` per package with the business columns cast to common types on
  both sides, `row_count` and `checksum = sum(cast(xxhash64(cols) as decimal(38,0)))` (order independent, overflow safe
  under ANSI), an informational `legacy_target_row_count` check, and per-package extra checks (e.g. supplier key null
  rate, one current row per key, catalog quarantine outcome). Exactly 22 specs are asserted against the package inventory
  before anything is written; any exception inside a spec becomes a `FAIL` row, so no package is silently skipped.

## Deliberate deviations from the SSIS packages

| SSIS behaviour | Databricks behaviour | why |
|---|---|---|
| OLE DB destinations with `FastLoadMaxInsertCommitSize`, batch sizes, row-by-row Lookup components | set-based joins, Delta `overwrite` / `append` / `replaceWhere` | SSIS artefacts, no business meaning |
| `raw.*` truncate + staging tables on SQL Server | bronze Delta tables in the migration schema; `truncate_reload` packages overwrite, incremental ones append | same semantics, Delta-native |
| `Foreach File` loop moving files to `Processed`/`Quarantine` folders | `ctl_landing_file` records status per file; rows of quarantined files are not landed; files stay in the volume | volume is immutable input; status table is the audit trail |
| manifest says `header: true` for `supplier_catalog` but the layout is record-oriented (`HDR`/`DTL`/`TRL`) | parsed as record-oriented with no header row | the HDR record *is* the header; TRL row-count / checksum reconciliation implemented as in the package |
| Type 2 close uses `GETDATE()` at row time | `valid_to = asOf - 1 second`, one `asOf` per batch | deterministic replays |
| `Fact.Purchase` (SSIS) has no Oracle columns; `PRC_Load_PurchaseSpend` "extends" it in place | legacy grain kept exactly in `gold_fact_purchase`; Oracle P2P grain in `gold_fact_purchase_p2p`; spend in `gold_fact_purchase_spend` | keeps the legacy checksum reproducible while not losing the P2P attributes |
| `etl.SupplierScoringWeight` drives the scorecard | table is empty on the host → the package's regional default weights (`marts.REGION_WEIGHTS`: NA 40/25/20/15, EU 35/30/20/15, APAC 30/30/25/15 for delivery/quality/price/invoice) | documented default of the package |
| `EXT_SQL_SupplierTransactions` inventoried as writing `raw.SqlInvoice` | lands in `bronze_sql_supplier_transaction` | `raw.SqlInvoice` is a sibling package's (Sales invoices) table; sharing it would corrupt both |
| `Dimension.[Vendor Contract]` has no Oracle contract id | `gold_dim_vendor_contract.contract_business_key` added | needed to resolve the contract key on the P2P purchase fact without a name join |
| `EXT_ORA_SupplierMaster` selects `s.TAX_ID_MASKED`, which does not exist on the live `wwi_mdm.supp_master` (the package cannot run as written) | `tax_identifier = NVL(VAT_REG_NBR, TAX_ID_NBR)` as in Oracle's own `WWI_MDM.V_SUPPLIER_EXTRACT`; `TAX_ID_NBR` is null for every seeded supplier so the VAT/registration number is the effective identifier | without it every supplier is rejected `MISSING_TAX_ID` by `STG_Load_Supplier`'s conditional split and the whole supplier lineage is empty |
| index rebuilds / `UPDATE STATISTICS` post-steps | none (Predictive Optimization on Unity Catalog managed tables) | platform native |

## Evidence

Latest end-to-end run of `ssis_procurement_daily` (job `273029780406148`, run `18611557330537`, all seven tasks
`SUCCESS`) wrote evidence run `b300787d-c59b-44b4-ba89-89c6ba7fd2dd` (git_sha `9b592350d2a8`): 22 rows, one per
package — **PASS 1** (`FACT_Load_Purchase`, 8,367 rows and checksum equal to `wwi_legacy_dw.Fact.Purchase`),
**PARTIAL 21** (every other legacy target is empty on the host; row count and checksum match the source-derived
expectation in all 21), **FAIL 0**, **NOT_APPLICABLE 0**. Query:

```sql
WITH latest AS (SELECT run_id FROM otterorders_migration.evidence.recon_results
                WHERE branch = 'ssis_procurement' ORDER BY run_at DESC LIMIT 1)
SELECT unit, verdict, checks, summary
FROM otterorders_migration.evidence.recon_results JOIN latest USING (run_id) ORDER BY unit;
```

## How to run

```bash
export DATABRICKS_AUTH_TYPE=pat DATABRICKS_HOST="$DATABRICKS_DEMO_HOST" DATABRICKS_TOKEN="$DATABRICKS_DEMO_TOKEN"
cd databricks/ssis_migration/procurement
pip install pyspark==3.5.* pytest ruff pandas
ruff check src tests && python -m pytest -q                 # local Spark, no Databricks needed
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
databricks bundle run ssis_procurement_daily -t dev --params git_sha=$(git rev-parse HEAD)
```

`run_packages` accepts `packages = extract|stage|dimension|fact|mart|all|PKG_A,PKG_B` for partial replays.

## Open questions

1. `EXT_SQL_SupplierTransactions` → `raw.SqlInvoice` in `source-target-map.csv` looks like an inventory error (the table
   holds `Sales.Invoices`). Confirm the intended landing table before the legacy staging DB is decommissioned.
2. `wwi_fin.ap_invoice_line.po_line_id` / `receipt_line_id` are null for every row in the Oracle seed, so three-way
   matching always evaluates to `GRNI`. The logic is implemented and unit-tested; a populated AP feed is needed to
   exercise `MATCHED` / `*_EXCEPTION` on real data.
3. `product_uom_conv` and `supp_certification` are empty in Oracle; UOM conversion falls back to factor 1 and the
   certification join yields nulls (both handled, both untested against real data).
4. `Dimension.Supplier` contains version rows for the 13 WWI suppliers only; Oracle suppliers get new keys > 30. If the
   downstream sales/finance groups need a shared supplier key space, the seed strategy should be agreed across groups.
5. `TAX_ID_MASKED` (see deviations): confirm with the Oracle MDM owners whether the SSIS extract was meant to read a
   masked view column; the migration uses the unmasked registration number as the DQ duplicate key.
6. Statement period defaults to the latest month present in the transaction fact; the SSIS package took it from a
   project parameter that is not set in `WWI_DEV`.
