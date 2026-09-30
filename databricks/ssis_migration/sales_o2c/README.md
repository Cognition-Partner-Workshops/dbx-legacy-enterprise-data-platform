# SSIS → Databricks migration — group `sales_o2c` (Sales order-to-cash)

18 SSIS packages (`Master_Daily_ETL` children) re-implemented as PySpark library code + thin notebooks, deployed as a
Declarative Automation Bundle to `/Workspace/Shared/ssis_migration/sales_o2c`.

| | |
|---|---|
| Landing schema | `otterorders_migration.ssis_sales_o2c` |
| Evidence | `otterorders_migration.evidence.recon_results` (`branch = ssis_sales_o2c`, `actor = devin:ssis_sales_o2c`) |
| Jobs | `ssis_sales_o2c_daily_etl` (18 package tasks + `recon`), `ssis_sales_o2c_recon` (evidence only, re-runnable) |
| Sources | `wwi_legacy_oltp` (OLTP), `wwi_legacy_dw.Dimension.*` (sibling-owned dimensions, read only), `wwi_legacy_dw.Fact.*` / `wwi_legacy_staging.*` (recon baselines) |

## Package → Databricks artifact mapping

All packages run through `notebooks/run_package.py` → `src/sales_o2c/packages.py`; the table lists the module that holds the logic.

| Package | Load type | Source (legacy) | Target (Delta, `otterorders_migration.ssis_sales_o2c.`) | Module | Verdict |
|---|---|---|---|---|---|
| `EXT_SQL_Orders` | incremental_key (OrderID watermark) | `Sales.Orders` (+ Customers/SalesTerritories/SalesChannels/OrderLines pick summary) | `raw_sql_order` | `extract.py` | PARTIAL |
| `EXT_SQL_OrderLines` | incremental_key | `Sales.vw_OrderLineExtract` | `raw_sql_order_line` | `extract.py` | PARTIAL |
| `EXT_SQL_Invoices` | incremental_key | `Sales.vw_InvoiceExtract` | `raw_sql_invoice` | `extract.py` | PARTIAL |
| `EXT_SQL_InvoiceLines` | incremental_key | `Sales.InvoiceLines` (+ StockItems/PackageTypes/holdings) | `raw_sql_invoice_line` | `extract.py` | PARTIAL |
| `EXT_SQL_CustomerTransactions` | incremental_key | `Sales.CustomerTransactions` (+ TransactionTypes/PaymentMethods) | `raw_sql_customer_transaction` (inventory says `raw.SqlInvoice`, see deviations) | `extract.py` | PARTIAL |
| `STG_Load_Order` | incremental_append (timestamp watermark, MERGE) | `raw_sql_order`, `raw_sql_order_line` | `stg_order`, `stg_order_line`, `err_rejected_order_line`, `work_late_arriving_dimension_queue` | `staging.py` | PARTIAL |
| `STG_Load_Sale` | incremental_append | `raw_sql_invoice`, `raw_sql_invoice_line` | `stg_sale`, `stg_sale_line`, `err_rejected_invoice_line` | `staging.py` | PARTIAL |
| `DQ_OrderLine_Screen` | quality_screen | `stg_order_line` | `err_rejected_order_line` (+ `dq_status_code` stamp, `etl_data_quality_result`) | `dq.py` | PARTIAL |
| `DQ_InvoiceLine_Screen` | quality_screen | `stg_sale_line` | `err_rejected_invoice_line` (+ stamp, `etl_data_quality_result`) | `dq.py` | PARTIAL |
| `FACT_NA_Load_Sale` | incremental_fact (region = NA, sales tax, USD) | `stg_sale_line` + `Dimension.*` | `gold_fact_sale`, `work_sale_line_enriched`, `err_rejected_lookup_failure` | `fact_sale.py` | PASS |
| `FACT_EU_Load_Sale` | incremental_fact (region = EU, VAT / reverse charge, EUR) | `stg_sale_line` + `Dimension.*` | `gold_fact_sale` | `fact_sale.py` | PARTIAL |
| `FACT_APAC_Load_Sale` | incremental_fact (region = APAC, GST, AUD, July FY, 2.5 % rebate) | `stg_sale_line` + `Dimension.*` | `gold_fact_sale` | `fact_sale.py` | PARTIAL |
| `FACT_Dedup_Sale` | dedup (natural key, survivor = latest lineage then highest key) | `gold_fact_sale` | `gold_fact_sale`, `work_fact_sale_duplicate_archive` | `fact_sale.py` | PASS |
| `FACT_Load_Order` | incremental_fact (order-line grain, hold-and-retry) | `stg_order` + `stg_order_line` + `Dimension.*` | `gold_fact_order`, `work_order_line_enriched`, `work_late_arriving_dimension_queue`, `err_rejected_order_line` | `fact_order.py` | PASS |
| `FACT_Load_CustomerTransaction` | incremental_fact | `raw_sql_customer_transaction` → `stg_customer_transaction` | `gold_fact_customer_transaction`, `err_rejected_lookup_failure` | `fact_transaction.py` | PARTIAL |
| `FACT_Load_Transaction` | incremental_fact (AR + AP sub-ledgers) | `stg_customer_transaction` + `Purchasing.SupplierTransactions` → `stg_transaction` | `gold_fact_transaction`, `err_rejected_constraint_violation` | `fact_transaction.py` | PASS |
| `FACT_Load_DailySalesSnapshot` | snapshot_fact (delete window + rebuild) | `gold_fact_sale` + `Dimension.Date` | `gold_fact_daily_sales_snapshot`, `err_rejected_snapshot_row` | `snapshot_agg.py` | PARTIAL |
| `AGG_Refresh_DailySalesSummary` | aggregate_rebuild (window rebuild) | `gold_fact_sale` + `Dimension.Date` | `gold_agg_daily_sales_summary`, `err_rejected_summary_cell`, `etl_loss_making_day` | `snapshot_agg.py` | PARTIAL |

Control tables (all in the landing schema): `etl_watermark` (`etl.usp_Get/SetWatermark`), `etl_package_execution`
(`etl.usp_LogPackageStart/End`), `etl_data_quality_result`.

## Layout

```
databricks.yml                  bundle (target dev → /Workspace/Shared/ssis_migration/sales_o2c)
resources/*.job.yml             ssis_sales_o2c_daily_etl (18 tasks + recon), ssis_sales_o2c_recon
notebooks/run_package.py        thin notebook: widgets → RunContext → packages.executePackage
notebooks/recon.py              thin notebook: recon.runRecon → evidence.recon_results
src/sales_o2c/                  library (camelCase functions, snake_case tables/columns)
  config.py       constants, RunContext        tables.py     Delta MERGE/append/replaceWhere helpers
  watermark.py    numeric/timestamp watermarks runtime.py    package execution log wrapper
  extract.py      5 × EXT_SQL_*                staging.py    STG_Load_Order / STG_Load_Sale
  dq.py           DQ screens                   dimensions.py temporal (ValidFrom/ValidTo) lookups → key 0
  fact_sale.py    regional Fact.Sale + dedup   fact_order.py Fact.Order hold-and-retry
  fact_transaction.py  Fact.Customer Transaction + Fact.Transaction
  snapshot_agg.py Daily Sales Snapshot + Aggregate Daily Sales Summary
  recon.py        evidence writer (row_count + checksum per package)
tests/                          pytest on local Spark (28 tests: watermarks, DQ rules, regional tax/FX, dedup,
                                hold-and-retry, temporal lookups/unknown member, aging buckets, snapshot/aggregate ratios)
```

Run locally: `pip install pyspark==3.5.3 delta-spark==3.2.0 pytest ruff && ruff check . && pytest -q`.
Deploy/run: `databricks bundle validate --strict -t dev && databricks bundle deploy -t dev && databricks bundle run ssis_sales_o2c_daily_etl -t dev --params git_sha=<sha>,reload_full_history=true`.

## Design decisions

* **PySpark library + thin notebooks** (not DLT): the packages are watermark MERGE loads with hold/retry queues, archive-then-delete
  dedup and delete-window rebuilds — imperative semantics that map 1:1 onto Delta `MERGE`/`DELETE`/`replaceWhere`; a declarative
  pipeline would have to fake them. One notebook per package task keeps the SSIS package boundary visible in the job graph.
* **Watermarks** follow `etl.usp_GetWatermark/usp_SetWatermark`: `NumericKey` (defaults `0`) for the 5 extracts, `Timestamp`
  (defaults `1900-01-01`) for staging/facts; `reload_full_history=true` (SSIS `ReloadFullHistory`) ignores the stored value.
* **Idempotency**: raw/stg/fact tables are `MERGE`d on their business key; error tables append with `batch_id` so re-runs are auditable
  and recon reads the latest batch. Snapshot/aggregate use `replaceWhere` over the rebuild window.
* **Dimension lookups** read `wwi_legacy_dw.Dimension.*` with the legacy temporal rule `LastModified > ValidFrom AND LastModified <= ValidTo`
  and default to key `0` (the legacy `Integration.MigrateStaged*Data` behaviour). `FACT_Load_Order` uses *current* lookups first and holds
  rows whose customer/stock item is missing in `work_order_line_enriched` (3 retries, then unknown member `0`) as in the DTSX.
* **Regional Fact.Sale**: one implementation parameterised by `REGION_RULES` (NA sales tax/USD, EU VAT with reverse charge + Intrastat flag/EUR,
  APAC GST incl./excl., 2.5 % distributor rebate, July fiscal year/AUD). Region comes from `Sales.Customers.RegionCode` of the bill-to
  customer and defaults to `NA`, exactly as `stg.usp_LoadSale` does.
* **Recon** (`recon.py`) computes `row_count` and an order-independent `sum(xxhash64(business columns))` for each package. Populated legacy
  targets (`Fact.Sale`, `Fact.Order`, `Fact.Transaction`) are compared directly (`baseline: legacy_target`, PASS only when both match).
  Empty legacy targets are compared with an expectation derived from the legacy source with the package's own rules
  (`baseline: source_derived`, at best PARTIAL).

## Deliberate deviations from the SSIS packages

* **Hold-and-retry on a full-history reload**: `FACT_Load_Order` parks order lines whose customer/stock item is unresolved in
  `work_order_line_enriched` for up to 3 runs (SSIS behaviour). On `reload_full_history=true` there is no later run to retry into, so the
  retry budget is treated as exhausted and the 85,469 lines whose customer is absent from `Dimension.Customer` load with the unknown member
  (`customer_key = 0`) at once - which is exactly what the populated legacy `Fact.Order` contains. Incremental runs keep the 3-retry hold.
* **Duplicate SCD2 versions in the legacy dimensions**: `Dimension.City` / `Dimension.Employee` carry two rows with identical
  `[Valid From, Valid To)` for one business key. The SSIS lookup (cached, first match) resolved the lowest surrogate key; `asOfLookup`
  orders by `valid_from, dim_key` to reproduce that instead of picking an arbitrary duplicate.

| # | SSIS behaviour | Databricks behaviour | Why |
|---|---|---|---|
| 1 | `EXT_SQL_CustomerTransactions` is inventoried (and its DTSX destination is declared) as `raw.SqlInvoice`. | Lands in `raw_sql_customer_transaction`; `stg_customer_transaction` is conformed from it. | Generator defect: the legacy `stg.usp_ConformCustomerTransactionForFact` had to synthesise AR rows from `stg.Sale` because the real rows were never landed. The evidence row keeps `source_object = WideWorldImporters_Staging.raw.SqlInvoice` and explains this. |
| 2 | Row-by-row Lookup components with "ignore failure" and OLE DB fast-load batches. | Set-based broadcast/temporal joins, one `MERGE`. | SSIS artefact, no business meaning. |
| 3 | `FACT_Load_Transaction` routes rows failing `|excl + tax − total| > 0.01` to `err.RejectedConstraintViolation` only. | Rows are logged to `err_rejected_constraint_violation` **and loaded**. | The populated `Fact.Transaction` (99 585 rows) contains all 26 637 customer payments/credits and 366 supplier rows that fail this screen; rejecting them would break the required parity. |
| 4 | Snapshot / aggregate default windows are `GETDATE()`-anchored (yesterday −3 days, trailing 10 days). | Anchored on `MAX(invoice_date_key)` of `gold_fact_sale` unless `snapshot_date` / `agg_from_date` / `agg_to_date` are passed; `reload_full_history` rebuilds all dates. | WWI data ends in 2016 — a GETDATE() window is always empty on this baseline. |
| 5 | `AGG_Refresh_DailySalesSummary` reads from `Fact.Sale` in the DW; `FACT_Load_DailySalesSnapshot` from `stg.DailySalesSnapshot` (empty on the host, built by `stg.usp_BuildDailySalesSnapshot` from the same sale lines). | Both read `gold_fact_sale`. | Same data, one hop shorter; the legacy staging table was never populated. |
| 6 | AP side of `FACT_Load_Transaction` comes from the Purchasing extract (another group). | Read `wwi_legacy_oltp.Purchasing.SupplierTransactions` directly. | Cross-group dependency — read the legacy source rather than a sibling schema. |
| 7 | Legacy raw tables store every column as `nvarchar` and hold a 3 000-row synthetic sample (`OrderID 60000–62999`, `OrderLineID 1–999` repeated). | Typed Delta columns; full OLTP extract. | The legacy sample is not an extract of the OLTP, so EXT_* recon compares against the OLTP source (`source_derived`) and reports the legacy count as an informational check. |
| 8 | `Fact.Sale` `Bill To Customer Key` / `Total Dry Items` are SSIS lookups on `stg.Sale` header columns. | Same derivation (`Invoices.BillToCustomerID`, `Invoices.TotalDryItems`), read from the header row. | Verified column-by-column against the populated `Fact.Sale` (see recon checks). |

## Evidence summary (latest `ssis_sales_o2c_recon` run)

| Verdict | Packages |
|---|---|
| PASS (4) | `FACT_NA_Load_Sale`, `FACT_Dedup_Sale`, `FACT_Load_Order`, `FACT_Load_Transaction` - row count and business-column checksum equal the populated legacy `Fact.Sale` (228,265), `Fact.Order` (231,412) and `Fact.Transaction` (99,585) |
| PARTIAL (14) | the 5 `EXT_SQL_*` (legacy `raw.*` is a 3k-row synthetic sample, extract matched the OLTP source row-for-row), `STG_Load_*`, `DQ_*_Screen`, `FACT_Load_CustomerTransaction`, `FACT_Load_DailySalesSnapshot`, `AGG_Refresh_DailySalesSummary` (legacy targets empty, matched a source-derived expectation), `FACT_EU_Load_Sale` / `FACT_APAC_Load_Sale` (0 EU/APAC invoices in the OLTP baseline) |
| FAIL (0) | - |
| NOT_APPLICABLE (0) | - |

Query: `SELECT unit, verdict, checks FROM otterorders_migration.evidence.recon_results WHERE branch = 'ssis_sales_o2c' AND run_id = (SELECT run_id FROM otterorders_migration.evidence.recon_results WHERE branch = 'ssis_sales_o2c' ORDER BY run_at DESC LIMIT 1)`.

## Open questions / limitations

* `Sales.Customers.RegionCode` is `NULL` for every customer on the host, so every sale resolves to `NA`; `FACT_EU_Load_Sale` and
  `FACT_APAC_Load_Sale` therefore process 0 rows in this baseline and are `PARTIAL` (logic covered by unit tests only).
* `stg.FxRate`, `stg.TaxRate`, `ref.Currency`, `Sales.OrderDiscounts` are empty on the legacy host — FX defaults to 1 and the
  regional tax rate falls back to the invoice-line `TaxRate` (the legacy procedures use the same `COALESCE` defaults).
* `Fact.Customer Transaction`, `Fact.Daily Sales Snapshot` and `Aggregate.Daily Sales Summary` are empty on the host, so those packages
  can only be `PARTIAL` (source-derived expectation).
* Aging buckets in `gold_fact_customer_transaction` are relative to the run date (as in SSIS) and are excluded from the checksum.
* The bundle target does not use `mode: development` because that mode rejects the mandated shared `root_path`; the job gets an
  explicit `CAN_MANAGE` for `users` (the path is under `/Workspace/Shared`).
