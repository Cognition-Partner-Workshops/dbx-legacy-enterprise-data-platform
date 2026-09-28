# Sales ETL -> Databricks lakehouse: artifact mapping

Legacy sales pipeline (SQL Server OLTP + Oracle ERP/MDM/REF -> SSIS -> `stg.*`/`work.*` -> `Fact.*`/`Dimension.*` ->
`Aggregate.*` -> `Report.vw_*`) mapped to the Databricks implementation under `databricks/` (`sales_lakehouse` package,
thin notebooks, Declarative Automation Bundle jobs). Layer contract: `databricks/CONVENTIONS.md`. Run-book:
`databricks/README.md`.

Conventions used below:

* **Module** = `databricks/src/sales_lakehouse/<module>`; **Table** = `<catalog>.sales_<layer>.<table>` built with
  `cfg.fqn(layer, table)`; **Notebook** = `databricks/notebooks/...`; **Task** = task key in
  `databricks/resources/sales_lakehouse_pipeline_job.yml` (job `sales_lakehouse_pipeline`), which is also the stage name
  order used by `sales_lakehouse.orchestration.pipeline` locally.
* Rejected rows never disappear: every reject lands in `sales_quality.rejected_rows` (`rule_code`, `source_table`,
  `reason_text`, `row_json`, `batch_id`) via `common.quality.quarantine`. Soft screens set `dq_status_code = 'WARN'` and
  keep the row.
* Source rows are the ones in the *previous* workstream PR descriptions (#19 bronze, #20 silver rules, #24 silver
  customers/dims, #31 mock data, #32 gold facts, #35 gold aggregates/reporting/sales-ops, #42 silver transactions);
  this document consolidates them and adds the integration layer (orchestration, quality, validation).

## 1. Pipeline shape

| Stage (local `--stages` name / job task) | Module entry point | Writes | Replaces |
|---|---|---|---|
| `mock` (optional, `+mock`) | `mock_data.generate.generate` | `<mock_data_root>/sqlserver/<Schema>/<Table>.csv`, `oracle/<SCHEMA>/<TABLE>.csv`, `manifest.json` | production extracts (unreachable) |
| `bronze` / `ingest_bronze` | `bronze.ingest.run` | `sales_bronze.sqlserver_*`, `oracle_*` (59 tables), `sales_bronze._watermark`, `sales_quality.load_log` | `ssis/01_oracle_extract`, `ssis/02_sqlserver_extract`, `raw.*` |
| `silver_reference` / `silver_reference` | `silver.reference.run` | `ref_fx_rate`, `ref_tax_rate_na/eu/apac`, `dim_fiscal_calendar`, `dim_date` | `ssis/06_reference_data` (currency, date), Oracle `PKG_FX`/`PKG_TAX`/`FN_FISCAL_PERIOD` reference loads |
| `silver_party` / `silver_party_resolution` | `silver.party_resolution.run` | `party_resolution` | `WWI_MDM.PKG_CUSTOMER_MASTER` merge/survivor logic |
| `silver_customers` / `silver_customers` | `silver.customers.run` | `customer`, `dim_customer` (SCD2) | `STG_Load_Customer`, `STG_Work_CustomerDedup`, `DQ_Customer_Screen`, `stg.usp_*Customer*`, `Dimension.Customer` |
| `silver_dimensions` / `silver_dimensions` | `silver.dimensions.run` | `dim_sales_channel`, `dim_sales_territory`, `dim_salesperson`, `dim_buying_group`, `bridge_customer_buying_group` | `REF_Load_SalesChannel`, `STG_Load_PromotionAndTerritory`, `ssis/07_dimensions`, `Dimension.Sales *`, bridge |
| `silver_transactions` / `silver_transactions` | `silver.transactions.run` | `order`, `order_line`, `sale`, `sale_line`, `payment`, `payment_allocation`, `order_hold`, `order_amendment`, `backorder`, `quote`, `quote_line`, `late_arriving_dimension_queue` | `STG_Load_Order/Sale/Payment`, `STG_Work_PaymentMatch`, `DQ_OrderLine/InvoiceLine/Payment_Screen`, `stg.usp_AppendIncremental_*`, `work.usp_*` |
| `gold_facts` / `fact_*` (8 tasks) | `gold.facts.run` | `fact_sale`, `fact_order`, `fact_payment`, `fact_return`, `fact_credit_note`, `fact_sales_margin`, `fact_daily_sales_snapshot`, `fact_daily_backlog`, `fact_order_fulfilment` | `ssis/08_facts`, `Integration.usp_LoadFact*` |
| `gold_aggregates` / `gold_aggregates` | `gold.aggregates.run` | `agg_daily_sales`, `agg_monthly_sales`, `agg_regional_sales_performance`, `agg_customer_360`, `agg_customer_rolling_12_month`, `agg_product_performance`, `agg_monthly_margin_analysis` | `ssis/09_aggregates/AGG_Refresh_*`, `Integration.usp_RefreshAggregate*` |
| `gold_reporting` / `gold_reporting_views` | `gold.reporting.run` | `rpt_*` views (8) | `AGG_Publish_ReportingLayer`, `Integration.usp_PublishReportingLayer`, `Report.vw_*` |
| `sales_ops` / `gold_sales_ops` | `gold.sales_ops.run` | `agg_quota_attainment`, `agg_commission`, partner feed CSV + manifest | `ssis/11_sales` |
| `month_end` (optional, `+month_end`) / job `sales_lakehouse_month_end` | `gold.aggregates.closeMonthlyPeriod` per region | `agg_monthly_sales.period_closed_flag` | `Master_Month_End` |
| `quality` / `quality_checks` | `quality.checks.run` | `sales_quality.check_results` | `err.*` tables, `DQ_Threshold_Gate`, `DQ_Referential_Screen`, SSIS row-count audit |
| `validation` / `validate_against_mock` | `quality.validate_mock.validate` | console table, non-zero exit on failure | (new) reconciliation of gold against the mock manifest |

Dependency order (both `pipeline.STAGES` and the job `depends_on` chains): `bronze` -> `silver_reference` and
`silver_party` -> `silver_customers` -> `silver_dimensions` -> `silver_transactions` (needs reference + dimensions) ->
`fact_sale`, `fact_order` -> `fact_payment` (sale), `fact_return` (sale), `fact_credit_note` (sale) -> `fact_sales_margin`
(return + credit note), `fact_daily_snapshots` (order + return + credit note), `fact_order_fulfilment` (order + payment)
-> `gold_aggregates` -> `gold_reporting_views`, `gold_sales_ops` -> `quality_checks` -> `validate_against_mock`.

### Job design decision

`resources/sales_lakehouse_pipeline_job.yml` is the deployable replacement for the SSIS master packages:

| SSIS master package | Lakehouse |
|---|---|
| `Master_Daily_ETL` | job `sales_lakehouse_pipeline` (21 serverless notebook tasks, `batch_id = {{job.run_id}}` on every task, schedule 02:00 UTC, paused until enabled) |
| `Master_Hourly_Incremental` | same job. The hourly package differs from the daily one only by watermark scope; `bronze.ingest` owns the per-table watermark (`sales_bronze._watermark`), so scheduling the same job more often is the incremental run |
| `Master_Month_End` | job `sales_lakehouse_month_end` (`run_pipeline.py` with `stages = gold_aggregates,gold_reporting,month_end`, `close_month` widget; blank = previous month) |
| `Master_Customer_Sync`, `Master_Weekly_Reference_Load` | covered by the `silver_*` tasks of the daily job (no separate cadence) |
| `Master_File_Ingestion`, `Master_Finance_Close`, `Master_Intraday_Inventory`, `Master_Weekly_Maintenance` | out of the sales scope (finance / inventory / maintenance) |

The per-layer jobs the workstreams added (`bronze_job.yml`, `silver_reference_job.yml`, `silver_dimensions_job.yml`,
`silver_transactions_job.yml`, `gold_facts_job.yml`, `gold_reporting_job.yml`) are **kept unchanged** for re-running a
single layer during development; they are not scheduled. All notebook paths and job names from the workstreams are
unchanged; the integration adds `notebooks/90_orchestration/run_pipeline.py` and
`notebooks/95_validation/validate_against_mock.py`.

## 2. SSIS packages

### 2.1 `ssis/02_sqlserver_extract` (and `01_oracle_extract`)

| Package | Module / function | Table | Notebook | Task |
|---|---|---|---|---|
| `EXT_SQL_Orders`, `EXT_SQL_OrderLines`, `EXT_SQL_Invoices`, `EXT_SQL_InvoiceLines`, `EXT_SQL_CustomerTransactions`, `EXT_SQL_CreditNotes`, `EXT_SQL_Returns`, `EXT_SQL_Shipments`, `EXT_SQL_ShipmentLines` (numeric-key incrementals) | `bronze.ingest.loadTable`; mode/watermark from `bronze.generate_registry.SOURCE_SPECS` (`legacyPackage` recorded per `SourceTable`) | `sales_bronze.sqlserver_sales_orders`, `..._order_lines`, `..._invoices`, `..._invoice_lines`, `..._customer_transactions`, `sqlserver_returns_*`, `sqlserver_shipping_*` | `10_bronze/ingest_bronze.py` | `ingest_bronze` |
| `EXT_SQL_People`, `EXT_SQL_Cities`, `EXT_SQL_CustomerSegments`, `EXT_SQL_PaymentMethods`, `EXT_SQL_Promotions`, `EXT_SQL_SalesTerritories`, `EXT_SQL_TransactionTypes`, `EXT_SQL_StockItems` (full / timestamp) | same (`full`, `StockItems` = `ValidFrom` watermark, 240 min lookback) | `sales_bronze.sqlserver_application_*`, `sqlserver_sales_*`, `sqlserver_warehouse_stock_items` | same | same |
| `EXT_SQL_LoyaltyLedger`, `EXT_SQL_StockMovements`, `EXT_SQL_StockTransfers`, `EXT_SQL_SupplierTransactions`, `EXT_SQL_WebSessions` | not sales scope; the tables are landed when a CSV exists (registry is DDL-driven) but no silver consumer | - | - | - |
| `EXT_ORA_*` (Oracle MDM/REF/FIN) | `bronze.ingest.loadTable` (`CUST_MASTER` = `UPDATED_DT` watermark 120 min; `FX_RATE_DAILY` = `RATE_DT` date window) | `sales_bronze.oracle_wwi_mdm_*`, `oracle_wwi_ref_*`, `oracle_wwi_fin_*` | same | same |
| `Integration.ChangeTrackingWatermark` (control) | `bronze.ingest.applyWatermark` / `updateWatermark` | `sales_bronze._watermark` | same | same |
| package run/row-count logging | `bronze.ingest` -> `load_log` row per table | `sales_quality.load_log` | same | same |

### 2.2 `ssis/04_staging` + `ssis/05_data_quality` (staging + screens)

| Package | Module / function | Table | Notebook | Task |
|---|---|---|---|---|
| `STG_Load_Customer`, `STG_Work_CustomerDedup`, `DQ_Customer_Screen` | `silver.customers.conformCustomers`, `quarantineCustomers` (`CUST_MISSING_NAME`, `CUST_BAD_COUNTRY`, `CUST_BAD_REGION`, `CUST_BAD_CREDIT`, `CUST_NO_CONSENT`), `latestSourceVersion`, `applySurvivorship`, `translateCodes` | `sales_silver.customer` | `20_silver/customers.py` | `silver_customers` |
| `STG_Load_CustomerAddress` | folded into `customers.conformCustomers` (address/postal fields on `customer`) | `sales_silver.customer` | same | same |
| `STG_Load_Order`, `DQ_OrderLine_Screen` | `silver.transactions.conformOrders`, `conformOrderLines`, `conformOrderHolds`, `conformOrderAmendments`, `conformBackorders`; hard rejects `ORDER_MISSING_KEY`, `OL_ORPHAN_HEADER`, `OL_BAD_NUMERIC`, `OL_NEG_QTY` | `order`, `order_line`, `order_hold`, `order_amendment`, `backorder` | `20_silver/silver_transactions.py` | `silver_transactions` |
| `STG_Load_Sale`, `DQ_InvoiceLine_Screen` | `transactions.conformSales`, `conformSaleLines`; rejects `SALE_MISSING_KEY`, `SL_ORPHAN_HEADER`, `SL_BAD_NUMERIC`, `DUP_SALE_LINE` | `sale`, `sale_line` | same | same |
| `STG_Load_Payment`, `STG_Work_PaymentMatch`, `DQ_Payment_Screen` | `transactions.conformPayments`, `allocatePayments`, `silver.payment_matching.matchPaymentsToInvoices` (`REMIT_REF` 100 / `EXACT_AMT` 90 / `RESIDUAL` 65 / `UNAPPLIED`); reject `PAY_MISSING_KEY` | `payment`, `payment_allocation` | same | same |
| `STG_Load_PromotionAndTerritory` | `silver.dimensions.buildDimSalesTerritory`, `buildDimSalesChannel` (promotions: bronze only) | `dim_sales_territory`, `dim_sales_channel` | `20_silver/dimensions.py` | `silver_dimensions` |
| `STG_Load_Employee` | `silver.dimensions.buildSalespersonSource` + `mergeScd2` | `dim_salesperson` | same | same |
| `STG_Load_Currency`, `STG_Load_TaxAndTerms` | `silver.reference.buildRefFxRate`, `buildRefTaxRateNa/Eu/Apac` | `ref_fx_rate`, `ref_tax_rate_na`, `ref_tax_rate_eu`, `ref_tax_rate_apac` | `20_silver/build_reference.py` | `silver_reference` |
| `STG_Load_ReturnAndCredit` | read directly from bronze by `gold.fact_return` / `gold.fact_credit_note` (no silver staging table) | `fact_return`, `fact_credit_note` | `30_gold/fact_return.py`, `fact_credit_note.py` | `fact_return`, `fact_credit_note` |
| `STG_Load_Shipment` | bronze `sqlserver_shipping_*` read by `gold.fact_order_fulfilment` | `fact_order_fulfilment` | `30_gold/fact_order_fulfilment.py` | `fact_order_fulfilment` |
| `STG_Load_Product`, `STG_Load_StockItem`, `STG_Work_ProductCrosswalk` | out of the sales workstreams (stock item dim is read as optional `silver.dim_stock_item` by gold) | - | - | - |
| `STG_Load_ApInvoice`, `STG_Load_CostCenter`, `STG_Load_GlJournal`, `STG_Load_LoyaltyLedger`, `STG_Load_PartnerSale`, `STG_Load_PurchaseOrder`, `STG_Load_Supplier`, `STG_Load_VendorContract`, `STG_Load_WebSession`, `STG_Load_StockMovement`, `STG_Work_InventoryPosition`, `DQ_Supplier_Screen`, `DQ_File_Screen` | finance / procurement / inventory - out of scope | - | - | - |
| `DQ_Referential_Screen`, `DQ_Threshold_Gate`, `DQ_Rule_Engine` | `quality.checks.referentialIntegrity`, `reconcileBronze/Silver/Gold`, `duplicateKeys`, `amountGuards` -> `check_results` (report only, no row drops) | `sales_quality.check_results` | `90_orchestration/run_pipeline.py` (`stages=quality`) | `quality_checks` |
| `DQ_Reject_Reprocess` | not reproduced: quarantined rows are re-read from `rejected_rows.row_json` manually (open question) | - | - | - |

### 2.3 `ssis/06_reference_data`

| Package | Module / function | Table | Notebook | Task |
|---|---|---|---|---|
| `REF_Load_Currency` | `silver.reference.buildRefFxRate` (`derivation_code` DIRECT / INVERSE / TRIANGULATED) | `ref_fx_rate` | `20_silver/build_reference.py` | `silver_reference` |
| `REF_Load_DateDimension` | `silver.reference.buildDimDate`, `buildDimFiscalCalendar` | `dim_date`, `dim_fiscal_calendar` | same | same |
| `REF_Load_CodeTranslation` | `silver.customers.translateCodes` reading `oracle_wwi_ref_code_translation` (region-specific > global, latest effective) | `customer.*_is_translated` flags | `20_silver/customers.py` | `silver_customers` |
| `REF_Load_SalesChannel` | `silver.dimensions.buildDimSalesChannel` | `dim_sales_channel` | `20_silver/dimensions.py` | `silver_dimensions` |
| `REF_Load_UnknownMembers` | `-1` member rows created by each dim builder (`customers.loadDimCustomer`, `dimensions.*`) and `gold.fact_support.UNKNOWN_KEY` | every `dim_*` | same | same |
| `REF_Load_PaymentMethod`, `REF_Load_ReturnReason`, `REF_Load_TransactionType` | code values carried on the transaction rows (`payment.payment_method_code`, `fact_return.return_reason_code`); untranslated codes pass through | - | - | - |
| `REF_Load_Carrier`, `REF_Load_CostCenter`, `REF_Load_Geography`, `REF_Load_LoyaltyTier`, `REF_Load_PaymentTerms`, `REF_Load_WarehouseSite` | out of scope (gold reads `dim_warehouse_site` etc. as optional) | - | - | - |

### 2.4 `ssis/08_facts`

| Package | Module / function | Table | Notebook | Task |
|---|---|---|---|---|
| `FACT_NA_Load_Sale`, `FACT_EU_Load_Sale`, `FACT_APAC_Load_Sale` | one loader `gold.fact_sale.run` (`buildFactSale`); regional tax/FX/fiscal via `gold.rules_adapter` -> `silver.rules.tax.applyTax`, `fx.applyFx`, `fiscal.resolveFiscalPeriod` | `sales_gold.fact_sale` | `30_gold/fact_sale.py` | `fact_sale` |
| `FACT_Dedup_Sale` | `fact_sale.buildFactSale` -> `fact_support.dedupeLatest`; losers `FACT_SALE_DUP` | `fact_sale`, `rejected_rows` | same | same |
| `FACT_Load_Order` | `gold.fact_order.run` (`buildFactOrder`, `_withFulfilmentFlags`, backorder/hold joins; `FACT_ORDER_ZERO_QTY`) | `fact_order` | `30_gold/fact_order.py` | `fact_order` |
| `FACT_Load_Payment` | `gold.fact_payment.run` (`buildFactPayment`, `writeFactPayment`; `FACT_PAYMENT_OVER_ALLOC`, `|UNALLOC` rows) | `fact_payment` | `30_gold/fact_payment.py` | `fact_payment` |
| `FACT_Load_Return` | `gold.fact_return.run` (`mergeAccumulating`, `finalizeReturn`) | `fact_return` | `30_gold/fact_return.py` | `fact_return` |
| `FACT_Load_CreditNote` | `gold.fact_credit_note.run` (`FACT_CREDIT_NOTE_UNAPPROVED`) | `fact_credit_note` | `30_gold/fact_credit_note.py` | `fact_credit_note` |
| `FACT_Load_DailySalesSnapshot` | `gold.fact_daily_snapshots.runSalesSnapshot` / `runBacklog` (`replaceWhere snapshot_date_key`) | `fact_daily_sales_snapshot`, `fact_daily_backlog` | `30_gold/fact_daily_snapshots.py` | `fact_daily_snapshots` |
| `FACT_Load_OrderFulfilment` | `gold.fact_order_fulfilment.run` (`finalizeFulfilment`) | `fact_order_fulfilment` | `30_gold/fact_order_fulfilment.py` | `fact_order_fulfilment` |
| (margin rebuild step of `Master_Daily_ETL`) | `gold.fact_sales_margin.run` (full rebuild; cost from `oracle_wwi_mdm_product_master.UNIT_COST_STD`) | `fact_sales_margin` | `30_gold/fact_sales_margin.py` | `fact_sales_margin` |
| `FACT_Apply_Corrections` | credit-note invoices load as reversing rows (`correction_type_code = 'REV'`) inside `fact_sale`; no separate correction pass | `fact_sale` | `30_gold/fact_sale.py` | `fact_sale` |
| `FACT_Load_CustomerTransaction`, `FACT_Load_Transaction` | AR ledger rows are the source of `silver.payment` (`conformPayments`), not a separate fact | `payment`, `fact_payment` | - | - |
| `FACT_Load_Shipment` | shipment milestones folded into `fact_order_fulfilment` (despatch/delivery dates) | `fact_order_fulfilment` | - | - |
| `FACT_Load_DailyInventorySnapshot`, `FACT_Load_GLPosting`, `FACT_Load_LoyaltyPoints`, `FACT_Load_Movement`, `FACT_Load_Purchase*`, `FACT_Load_StockHolding`, `FACT_Load_Supplier*`, `FACT_Load_WebSession` | out of scope | - | - | - |

### 2.5 `ssis/09_aggregates`

| Package | Module / function | Table | Notebook | Task |
|---|---|---|---|---|
| `AGG_Refresh_DailySalesSummary` | `gold.aggregates.buildDailySales` | `agg_daily_sales` | `30_gold/gold_aggregates.py` | `gold_aggregates` |
| `AGG_Refresh_MonthlySalesSummary` | `aggregates.buildMonthlySales` / `closeMonthlyPeriod` | `agg_monthly_sales` | same / `90_orchestration/run_pipeline.py` (`month_end`) | `gold_aggregates` / job `sales_lakehouse_month_end` |
| `AGG_Refresh_RegionalSalesPerformance` | `aggregates.buildRegionalSalesPerformance` | `agg_regional_sales_performance` | same | same |
| `AGG_Refresh_Customer360`, `AGG_Refresh_CustomerRolling12Month` | `aggregates.buildCustomer360`, `buildCustomerRolling12Month` | `agg_customer_360`, `agg_customer_rolling_12_month` | same | same |
| `AGG_Refresh_ProductPerformance` | `aggregates.buildProductPerformance` | `agg_product_performance` | same | same |
| `AGG_Refresh_MonthlyMarginAnalysis` | `aggregates.buildMonthlyMarginAnalysis` | `agg_monthly_margin_analysis` | same | same |
| `AGG_Publish_ReportingLayer` | `gold.reporting.run` (`CREATE OR REPLACE VIEW rpt_*`) | `rpt_*` | `30_gold/gold_reporting_views.py` | `gold_reporting_views` |
| `AGG_Refresh_DailyInventoryHealth`, `DeliveryPerformanceSummary`, `FinanceCloseSummary`, `PromotionEffectiveness`, `SupplierPerformance` | out of scope | - | - | - |

### 2.6 `ssis/11_sales`

| Package | Module / function | Table / file | Notebook | Task |
|---|---|---|---|---|
| `SLS_Load_QuotaAttainment` | `gold.sales_ops.buildQuotaAttainment` (`QUOTA_UNRESOLVED_KEY`) | `agg_quota_attainment` | `30_gold/gold_sales_ops.py` | `gold_sales_ops` |
| `SLS_NA_Load_Commission`, `SLS_EU_Load_Commission`, `SLS_APAC_Load_Commission` | `sales_ops.buildCommission(region, ...)` (`COMM_NON_COMMISSIONABLE_CHANNEL`, `COMM_NO_PLAN`, `COMM_MISSING_FX`) | `agg_commission` (`region_code`, `commission_basis`) | same | same |
| `SLS_Export_PartnerFeed` | `gold.partner_feed.exportPartnerFeed(spark, cfg, outDir, region)` | `<partner_feed_dir>/partner_feed_<REGION>_<yyyyMMdd>.csv` + `.manifest.json` | same | same |
| `SLS_Load_PromotionRedemption` | not reproduced (promotion facts out of scope; `total_discount_amount_local` carried on `order`) | - | - | - |

### 2.7 `ssis/00_orchestration`

See "Job design decision" above. `orchestration-plan.json` precedence is encoded in `pipeline.STAGES[*].dependsOn` and the
job `depends_on` lists; `Project.params` / `*.conmgr` connection managers are replaced by the bundle variables
`catalog`, `mock_data_root` and the job parameters `batch_id`, `partner_feed_dir`, `close_month`.

## 3. Stored procedures

### 3.1 `sqlserver/staging/procedures`

| Procedure | Module / function | Table | Task |
|---|---|---|---|
| `err.usp_LogRejectedRows` | `common.quality.quarantine` | `sales_quality.rejected_rows` | every task |
| `err.usp_PurgeRejectedRows` | not reproduced (Delta retention / `VACUUM` instead) | - | - |
| `stg.usp_AppendIncremental_OrderLine` | `silver.transactions.conformOrderLines` + `common.tables.mergeByKey` | `order_line` | `silver_transactions` |
| `stg.usp_AppendIncremental_SaleLine` | `transactions.conformSales` / `conformSaleLines` + `mergeByKey` | `sale`, `sale_line` | same |
| `stg.usp_AppendIncremental_Payment` | `transactions.conformPayments` + `mergeByKey` | `payment` | same |
| `stg.usp_AppendIncremental_Shipment`, `_StockMovement` | out of scope | - | - |
| `stg.usp_ConformCustomerTransactionForFact` (terms, AR sign) | `transactions.conformPayments` | `payment` | same |
| `stg.usp_ConformTransactionForFact`, `stg.usp_ConformOrderFulfilmentForFact` | `transactions.conformOrders`, `conformOrderLines`, `conformOrderHolds`; `gold.fact_order_fulfilment` | `order`, `order_hold`, `fact_order_fulfilment` | `silver_transactions`, `fact_order_fulfilment` |
| `stg.usp_ConformDailySalesSnapshotForFact` | `gold.fact_daily_snapshots.buildDailySalesSnapshot` | `fact_daily_sales_snapshot` | `fact_daily_snapshots` |
| `stg.usp_ConformCityForDimension`, `_CustomerCategoryForDimension`, `_CustomerSegmentForDimension` | `silver.customers.conformCustomers` (category / segment / city attributes on `customer` -> `dim_customer`) | `dim_customer` | `silver_customers` |
| `stg.usp_ConvertCurrencyAmounts` | `silver.rules.fx.applyFx` (NA 7-day fallback, EU prior day unbounded, APAC month anchor, `DEFAULT_1`) | `fact_*._usd` / `_reporting` columns | `fact_*` |
| `stg.usp_DeduplicateCustomer` | `customers.latestSourceVersion`, `applySurvivorship` | `customer` | `silver_customers` |
| `stg.usp_DeduplicateOrderLine` | `silver.dedup.rankExactCopies`, `flagRekeyDuplicates` (`DUP_ORDER_LINE`) | `order_line`, `rejected_rows` | `silver_transactions` |
| `stg.usp_NormalizeAddress`, `stg.usp_NormalizeCustomer` | `customers.standardizeName`, `normalizeTaxNumber` | `customer` | `silver_customers` |
| `stg.usp_TranslateSourceCodes` | `customers.translateCodes` | `customer` | same |
| `stg.usp_TruncateAndReload_Customer` | `customers.conformCustomers` + `quarantineCustomers` | `customer` | same |
| `stg.usp_TruncateAndReload_Geography/Product/Receipt/Supplier`, `stg.usp_Conform*Purchase*/GlPosting/LoyaltyPoints/Movement/StockHolding/Supplier*`, `stg.usp_DeduplicateSupplier`, `stg.usp_NormalizeSupplier` | out of scope | - | - |
| `work.usp_MatchPaymentsToInvoices` | `silver.payment_matching.matchPaymentsToInvoices`, `summarisePayments` | `payment_allocation`, `payment` | `silver_transactions` |
| `work.usp_QueueLateArrivingDimensions` | `silver.late_arriving.flagMissingReferences`, `collectMissing`, `mergeQueue` | `late_arriving_dimension_queue` | same |
| `work.usp_BuildInventoryPositionDaily`, `work.usp_BuildProductCrosswalk` | out of scope | - | - |

### 3.2 `sqlserver/procedures/facts`

| Procedure | Module / function | Table | Task |
|---|---|---|---|
| `Integration.usp_LoadFactSale` | `gold.fact_sale.run` | `fact_sale` | `fact_sale` |
| `Integration.usp_DeduplicateFactSale` | `fact_sale` -> `dedupeLatest` (`FACT_SALE_DUP`) | `fact_sale` | `fact_sale` |
| `Integration.usp_LoadFactOrder` | `gold.fact_order.run` | `fact_order` | `fact_order` |
| `Integration.usp_LoadFactPayment` | `gold.fact_payment.run` | `fact_payment` | `fact_payment` |
| `Integration.usp_LoadFactReturn` | `gold.fact_return.run` | `fact_return` | `fact_return` |
| `Integration.usp_LoadFactCreditNote` | `gold.fact_credit_note.run` | `fact_credit_note` | `fact_credit_note` |
| `Integration.usp_LoadFactDailySalesSnapshot` | `gold.fact_daily_snapshots.run` | `fact_daily_sales_snapshot`, `fact_daily_backlog` | `fact_daily_snapshots` |
| `Integration.usp_LoadFactOrderFulfilment` | `gold.fact_order_fulfilment.run` | `fact_order_fulfilment` | `fact_order_fulfilment` |
| `Integration.usp_ApplyFactCorrections` | reversing rows in `fact_sale` (`correction_type_code`), payment `restatement_version` | `fact_sale`, `fact_payment` | `fact_sale`, `fact_payment` |
| `Integration.usp_RekeyLateArrivingDimensions` | `late_arriving.mergeQueue` (`resolved_batch_id`) + gold SCD2 lookups re-run on MERGE (`fact_support.lookupScd2Key`) | `late_arriving_dimension_queue`, facts | `silver_transactions`, `fact_*` |
| `Integration.usp_PopulateDateDimensionRange` | `silver.reference.buildDimDate` (NA 4-4-5, EU calendar, APAC Apr-Mar) | `dim_date`, `dim_fiscal_calendar` | `silver_reference` |
| `Integration.usp_RefreshAggregateDailySales`, `_MonthlySales`, `_RegionalSales`, `_Customer360`, `_ProductPerformance`, `_MarginAnalysis` | `gold.aggregates.build*` | `agg_*` | `gold_aggregates` |
| `Integration.usp_PublishReportingLayer` | `gold.reporting.run` | `rpt_*` | `gold_reporting_views` |
| `Integration.usp_RebuildColumnstoreIndexes` | not needed (Delta; `OPTIMIZE` if wanted) | - | - |
| `Integration.usp_LoadFactDailyInventorySnapshot`, `_GlPosting`, `_LoyaltyPoints`, `_Movement`, `_Purchase*`, `_Shipment`, `_StockHolding`, `_SupplierPayment`, `_WebSession`, `usp_RefreshAggregateDeliveryPerformance/FinanceClose/InventoryHealth/PromotionEffectiveness/SupplierPerformance` | out of scope | - | - |

## 4. Warehouse tables, aggregates and views

### 4.1 Dimensions (`sqlserver/warehouse/dimensions`, `wwi-dw-ssdt`)

| Legacy | Lakehouse table | Module | Task |
|---|---|---|---|
| `Dimension.Customer` (+ `90_unknown_members`) | `sales_silver.dim_customer` (hybrid SCD2, `is_current`, `-1`) | `silver.customers.loadDimCustomer` | `silver_customers` |
| `Dimension.Sales Channel` | `dim_sales_channel` | `silver.dimensions.buildDimSalesChannel` | `silver_dimensions` |
| `Dimension.Sales Territory` | `dim_sales_territory` | `dimensions.buildDimSalesTerritory` | same |
| `Dimension.Salesperson` | `dim_salesperson` (SCD2) | `dimensions.buildSalespersonSource` + `mergeScd2` | same |
| `Dimension.Buying Group`, `Dimension.Customer Buying Group Bridge` | `dim_buying_group`, `bridge_customer_buying_group` | `dimensions.buildDimBuyingGroup`, `buildBridge` | same |
| `Dimension.Date`, `Dimension.Fiscal Calendar`, `91_date_role_playing_views` | `dim_date`, `dim_fiscal_calendar` (role-playing = `*_date_key` columns on facts) | `silver.reference` | `silver_reference` |
| `Dimension.Currency`, `Dimension.Tax Jurisdiction` | `ref_fx_rate`, `ref_tax_rate_na/eu/apac` | `silver.reference` | same |
| `Dimension.Customer Category / Segment / Demographic`, `Dimension.City`, `Dimension.Geography`, `Dimension.Country`, `Dimension.Region` | attributes flattened onto `customer` / `dim_customer` (`customer_category_name`, segment, city, country, region) | `silver.customers` | `silver_customers` |
| `Dimension.Stock Item`, `Product *`, `Employee`, `Payment Method`, `Payment Terms`, `Promotion`, `Return Reason`, `Order Status Junk`, `Time`, `Transaction Type`, `Carrier`, `Warehouse Site`, `Cost Center`, `GL Account`, `Loyalty Tier`, `Supplier *`, `Vendor Contract`, `Employee Territory Bridge` | not built by the sales workstreams; gold reads `dim_stock_item`, `dim_warehouse_site`, `dim_return_reason` etc. as optional (`gold.inputs.readOrEmpty`, `-1`/NULL when absent) | - | - |

### 4.2 Facts (`sqlserver/warehouse/facts`)

| Legacy | Lakehouse table | Grain / write | Task |
|---|---|---|---|
| `Fact.Sale` + `Fact.Sale.Extensions` | `sales_gold.fact_sale` | invoice line, `sale_line_business_key`, MERGE | `fact_sale` |
| `Fact.Order` + `Fact.Order.Extensions` | `fact_order` | order line, MERGE | `fact_order` |
| `Fact.Payment` | `fact_payment` | allocation (+ `<receipt>\|UNALLOC`), conditional MERGE + `restatement_version` | `fact_payment` |
| `Fact.Return` | `fact_return` | return line, accumulating MERGE | `fact_return` |
| `Fact.Credit Note` | `fact_credit_note` | credit note, accumulating MERGE | `fact_credit_note` |
| `Fact.Sales Margin` | `fact_sales_margin` | invoice line, full rebuild | `fact_sales_margin` |
| `Fact.Daily Snapshots` | `fact_daily_sales_snapshot`, `fact_daily_backlog` | snapshot date x grain, `replaceWhere` | `fact_daily_snapshots` |
| `Fact.Order Fulfilment` | `fact_order_fulfilment` | order, accumulating MERGE | `fact_order_fulfilment` |
| `Fact.Fact Load Hold` | `silver.order_hold` + `fact_order.hold_*` flags | - | `silver_transactions`, `fact_order` |
| `Fact.Customer Transaction`, `Fact.Transaction.Extensions` | `silver.payment` (AR ledger) -> `fact_payment` | - | `silver_transactions`, `fact_payment` |
| `Fact.Monthly Snapshots`, `Fact.Salesperson Territory Coverage`, `Fact.Promotion Eligibility`, `Fact.GL Posting`, `Fact.Loyalty Points`, `Fact.Movement`, `Fact.Purchase*`, `Fact.Procure To Pay`, `Fact.Shipment`, `Fact.Stock Holding`, `Fact.Supplier Ledger`, `Fact.Web Session` | out of scope | - | - |

### 4.3 Aggregates (`sqlserver/warehouse/aggregates`)

| Legacy | Lakehouse table | Task |
|---|---|---|
| `Aggregate.[Daily Sales Summary]` | `agg_daily_sales` (date x stock item x territory x channel) | `gold_aggregates` |
| `Aggregate.[Monthly Sales Summary]` | `agg_monthly_sales` (month x customer x territory x channel; `period_closed_flag`) | `gold_aggregates`, month-end job |
| `Aggregate.[Regional Sales Performance]` | `agg_regional_sales_performance` | `gold_aggregates` |
| `Aggregate.[Customer 360]`, `[Customer Rolling 12 Month]` (Customer Summaries) | `agg_customer_360`, `agg_customer_rolling_12_month` | same |
| `Aggregate.[Product Performance]` | `agg_product_performance` | same |
| `Aggregate.[Monthly Margin Analysis]` | `agg_monthly_margin_analysis` | same |
| `Aggregate.[Quota Attainment]` / `[Commission]` (sales-ops outputs) | `agg_quota_attainment`, `agg_commission` | `gold_sales_ops` |
| `Aggregate.Delivery Performance Summary`, `Finance Close Summary`, `Promotion Effectiveness`, `Supplier Performance` | out of scope | - |

### 4.4 `Report.vw_*` views (`sqlserver/views`)

| Legacy view | Lakehouse view (`sales_gold`) | SQL builder (`gold.reporting`) |
|---|---|---|
| `Report.vw_DailySalesTrend` | `rpt_daily_sales_trend` | `dailySalesTrendSql` |
| `Report.vw_SalesByCustomerMonth` | `rpt_sales_by_customer_month` | `salesByCustomerMonthSql` |
| `Report.vw_SalesByProductMonth` | `rpt_sales_by_product_month` | `salesByProductMonthSql` |
| `Report.vw_SalesByTerritoryMonth` | `rpt_sales_by_territory_month` | `salesByTerritoryMonthSql` |
| `Report.vw_OrderToCashCycle` | `rpt_order_to_cash_cycle` | `orderToCashCycleSql` |
| `Report.vw_MarginByProductCategory` | `rpt_margin_by_product_category` | `marginByProductCategorySql` |
| `Report.vw_Customer360` | `rpt_customer_360` | `customer360Sql` |
| `Report.vw_CustomerChurnRisk` | `rpt_customer_churn_risk` | `customerChurnRiskSql` |
| `Report.vw_ApAgingCurrent`, `vw_FinanceCloseStatus`, `vw_InventoryHealthCurrent`, `vw_LoyaltyProgramSummary`, `vw_PromotionRoi`, `vw_ReturnsRateByCategory`, `vw_SupplierOnTimeDelivery`, `vw_SupplierSpendYtd` | out of scope | - |

`tests/test_gold_reporting_views.py` parses each legacy `.sql` select list and asserts the `rpt_*` column list matches.

## 5. Oracle FX / tax / fiscal reference

| Legacy | Lakehouse | Module |
|---|---|---|
| `WWI_REF.PKG_FX` (`get_rate` direct -> inverse -> triangulated, `backoff_days`) | `ref_fx_rate` (`currency_code`, `rate_date`, `rate_to_usd`, `rate_type_code`, `effective_date`, `rate_source_code`, `region_code`, `derivation_code`, `is_interpolated`) | `silver.reference.buildRefFxRate`, `silver.rules.fx.applyFx` |
| `WWI_FIN.PKG_TAX` (`determine_tax`), `WWI_FIN.FN_TAX_AMOUNT` | `ref_tax_rate_na` (component chain, `combined_tax_rate`), `ref_tax_rate_eu` (`is_reduced_rate`, `is_reverse_charge_eligible`), `ref_tax_rate_apac` (`gst_rate`, `is_price_inclusive`) | `silver.reference.buildRefTaxRate*`, `silver.rules.tax.applyTax` |
| `WWI_FIN.FN_CONVERT_AMOUNT` | `applyFx` `<col>_usd` outputs | `silver.rules.fx` |
| `WWI_REF.FN_FISCAL_PERIOD`, `WWI_REF.CALENDAR_FISCAL` (`NA_CAL` / `EU_CAL445` / `APAC_APR`) | `dim_fiscal_calendar` (`NA445` / `EUCAL` / `APACJUN`), `dim_date` | `silver.rules.fiscal.resolveFiscalPeriod`, `na445Parts`, `eucalParts`, `apacjunParts` |
| `WWI_REF.PKG_CODE_TRANSLATION`, `FN_TRANSLATE_CODE`, `CODE_TRANSLATION` | `customer.*_is_translated`; untranslated values pass through | `silver.customers.translateCodes` |
| `WWI_MDM.PKG_CUSTOMER_MASTER`, `PARTY_XREF`, `MDM_MERGE_HISTORY` | `party_resolution` (`DIRECT`, `MERGED`, `MERGED_MULTI_HOP`, `RETIRED_NO_SURVIVOR`, `MISSING_XREF`, `DUPLICATE_XREF`) | `silver.party_resolution` |
| `WWI_MDM.PKG_PRODUCT_MASTER` (`UNIT_COST_STD`) | read from bronze `oracle_wwi_mdm_product_master` by margin | `gold.fact_sales_margin.costFromProductMaster` |
| `WWI_AUDIT.PKG_DATA_QUALITY`, `PKG_EXTRACT_CONTROL` | `sales_quality.load_log`, `check_results`, `sales_bronze._watermark` | `bronze.ingest`, `quality.checks` |

Gold consumes the silver reference tables **as defined by `silver.reference`** (the integration deleted the WS6
`rules_adapter` fallback and its `(from_currency_code, to_currency_code, rate)` shape; see changelog).

## 6. Deliberate behavioural reproductions

Each is marked `# LEGACY QUIRK` in code and covered by a test.

**Extract / bronze**
- Per-table load mode and watermark column mirror the extract package descriptions (numeric-key incrementals for
  orders/lines/invoices/lines/AR/returns/credit notes/shipments; `StockItems.ValidFrom` 240 min and `CUST_MASTER.UPDATED_DT`
  120 min lookbacks; `FX_RATE_DAILY.RATE_DT` date window). Overlap rows land again and are de-duplicated in silver.
- Strict-greater-than watermark: a same-`RATE_DT` FX row arriving late is not re-extracted (legacy blind spot).
- Columns in a CSV but not in the DDL are not landed; DDL columns absent from the CSV land as NULL.
- Same-batch re-run of an incremental table replays its original window (`previous_watermark_value`).

**Customers / party / dimensions**
- `ChannelStatus` overload: `PILOT` channels are `is_commissionable = false` but `is_orderable = true`; `CLOSED` is not
  orderable. `MARKETPLACE_NO_COMMISSION` rows load with WARN.
- Buying-group bridge factors re-normalised only when off by > 0.000001; a three-way equal split lands on `0.999999` and
  is quarantined `BRIDGE_ALLOCATION_SUM`, exactly like the legacy load. Implicit `1.0` membership row for customers with a
  buying group and no membership row.
- Untranslated codes pass through the source value with `*_is_translated = false`; stale codes are used as-is.
- Regional consent: NA opt-out (NULL consentable), EU opt-in (missing opt-in + date -> reject `CUST_NO_CONSENT`), APAC
  follows EU rules for JP/AU only.
- Duplicate xref chosen deterministically (active, latest `UPDATED_DT`, lowest `PARTY_XREF_ID`) and tagged
  `DUPLICATE_XREF`; retired party without merge record keeps its id (`RETIRED_NO_SURVIVOR`); missing xref -> `-1`; merge
  chains bounded at 10 hops with a cycle guard.
- Dedup losers stay in `customer` as `is_survivor_row = false` / WARN.
- Territory unknown parent -> NULL rather than reject; inactive territories retired, not deleted.

**Transactions (silver)**
- Business keys `<SourceSystemCode>|<KEY>` / `|<LINE>`; `ORA_ERP_NA/EU/AP` -> `ORA_ERP`, `WWI_WEB` -> `WWI_OLTP`; Oracle ids
  left-padded to 10; embedded `|` -> `/`; blank key part -> hard reject.
- Order-line dedup: exact re-extraction copies grouped by business key, latest `LastEditedWhen` / `_load_ts` wins and
  earlier copies -> `DUP_ORDER_LINE`; re-key candidates (same order, item, qty, price) keep the highest line number and the
  losers stay as WARN `is_duplicate_loser`.
- `FulfilmentFlags` parsed in both formats (pipe letters and comma words); malformed -> `fulfilment_flags_malformed`,
  WARN, never rejected.
- Payment matching precedence: explicit allocations + remittance reference (`REMIT_REF`, 100) -> exact amount + customer
  within regional tolerance (`EXACT_AMT`, 90) -> FIFO residual (`RESIDUAL`, 65) -> remainder `UNAPPLIED` (on-account).
- Late-arriving customer / stock item / salesperson / channel: queued, fact row still lands with WARN, gold attaches `-1`.
- Header defaults from the staging packages (salesperson `-1`, `ExpectedDeliveryDate = OrderDate + 3`, PO upper-cased,
  package `EACH`, delivery `UNKNOWN`, due date by regional terms, value date +2 EU / +1 elsewhere, AR receipts absolute).
- Deletion logs soft-delete only (`is_deleted`, `deleted_batch_id`).

**Tax / FX / fiscal**
- Missing FX rate -> `fx_rate_to_reporting = 1.0`, `fx_rate_source_code = 'DEFAULT_1'`, WARN; never NULL, never rejected.
  Same currency -> `1.0` / `SAME_CCY`.
- Effective-date conventions: NA = transaction date else most recent prior rate within 7 days; EU = invoice date else most
  recent prior day (unbounded retry); APAC = rate effective on the 1st of the transaction month, carried forward 7 days.
- APAC GST-inclusive: `net = truncate(gross / (1 + rate), 2)`, `tax = gross - net`, dropped fraction in
  `tax_residual_local` (`GST_INCLUSIVE_RESIDUAL`). Oracle `PKG_TAX` rounds; the spec and SSIS truncate -> truncate.
- EU reverse charge with NULL VAT registration still loads: `tax_treatment_code = REVERSE_CHARGE_NO_VATREG`, WARN - but only
  when neither the invoice `CustomerTaxNumber` nor the customer master carries a registration. Silver backfills an
  invoice-level NULL from `Sales.Customers.TaxRegistrationNumber`, as `FACT_EU_Load_Sale` did through its `stg.Customer`
  lookup, so the mock's `EU_REVERSE_CHARGE_NULL_VAT` invoices land as plain `REVERSE_CHARGE` with zero tax.
- Missing tax rate = 0 % with WARN `TAX_RATE_MISSING`.
- Cross-region aggregates and quota/commission use the NA 4-4-5 calendar (`naFiscalPeriodFor`), regardless of the row's
  own regional calendar; the row-level `fiscal_period_key` stays regional.

**Gold facts**
- Credit-note invoices load as reversing sale rows (`correction_type_code = 'REV'`); nothing is netted at load.
- Zero-quantity order lines are quotation artefacts -> `FACT_ORDER_ZERO_QTY`; `quantity_open` stored (`ordered - picked`).
- Over-allocation rejected at receipt level with 0.01 tolerance; unallocated cash is a valid `|UNALLOC` row; payment
  restatement updates in place and bumps `restatement_version`.
- Sales margin is a full rebuild every run; standard cost treated as effective for all history; missing cost keeps
  `cost_usd = NULL` and `margin_status_code = 'COST_MISSING'`.
- Daily snapshot running totals stored, not derived; EU distributor channels excluded by hard-coded channel keys (7, 8,
  12); backlog re-evaluates every open line every day.
- Returns negative; statutory windows EU 14d from delivery, APAC per return reason (default 7), NA 30d from invoice;
  return-backed credit notes excluded from `fact_credit_note`; unapproved notes > 1,000 quarantined.
- Fulfilment: cash-applied date = latest allocation; stalled thresholds NA 30 / EU 45 / APAC 60 days; completed cycles
  frozen unless `reopenClosedRows=True`.

**Aggregates / reporting / sales ops**
- Prior-year daily comparison is 364 days back (same weekday); daily proc does not filter reversal rows, the monthly /
  regional / customer procs do; closed periods never refreshed; missing month-average FX -> 1.0 (not NULL).
- `vw_DailySalesTrend` `Total Discount` double counts promotion discount; product ABC thresholds 80 / 95 %.
- Quota: zero quota -> `attainment_pct = 0`, band `NOQUOTA`; commission bands are inclusive upper bounds, band 3 applies
  above; NULL attainment pays band 1; PILOT channels excluded and quarantined `COMM_NON_COMMISSIONABLE_CHANNEL`.
- NA commission: USD lines only, house accounts 50 %, accelerator above quota. EU: VAT-exclusive basis in EUR at the
  month-average rate, one per-line statutory cap (the "three-band" ladder is a *rate* ladder, see open questions),
  cash-basis countries count cleared payments only. APAC: GST-exclusive, period-average FX, missing rate ->
  `COMM_MISSING_FX` reject, team split factor.
- Partner feed: EU customers without sharing consent are redacted then dropped (`EU_CONSENT_N` absent from the EU feed).

**Integration / quality**
- Quality checks report (`PASS` / `WARN` / `FAIL`) and never drop or move rows; silver/gold batch reconciliation is a WARN
  because MERGE targets legitimately carry more rows than one batch's source (history, soft deletes).

## 7. Open questions

Collected from every workstream; the default currently implemented is stated first.

**Silver customers x gold facts**
0. `dim_customer` initial versions take `valid_from` from the source `ValidFrom` (`Sales.Customers` temporal start = last edit), so the
   as-of lookup in `fact_sale` (`Integration.usp_LoadFactSale` semantics: `Invoice Date >= Valid From AND < Valid To`) returns `-1`
   for invoices dated before the customer's latest edit (2,105 of 13,815 mock invoice lines; `RI_UNKNOWN_MEMBER` WARN). Legacy had
   `Customers_Archive` history for those dates; the mock has none. Options: seed the first version from `1900-01-01` (legacy
   `usp_EnsureUnknownMembers` style), or fall back to the current row as `usp_LoadFactOrder` / `usp_LoadFactPayment` do.

**Bronze**
1. Customer-master watermark column: `UPDATED_DT` (DDL) vs `LAST_UPDATE_DT` (`EXT_ORA_CustomerMaster`). `UPDATED_DT`.
2. CSV columns not in the DDL are dropped (logged) rather than landed with schema drift.
3. Delete detection from `WWI_AUDIT.CHANGE_LOG` / SQL Server change tracking is not reproduced; `Sales.OrderDeletionLog`
   and `Integration.DeletedRowLog` are landed as normal tables and applied as soft deletes by silver.
4. `bronze.ingest.run` raises after the loop if any table FAILED (partial loads still committed and logged).
5. Silver references four bronze names that are **not** in the 59-table registry and are read as optional (empty when
   absent): `sqlserver_integration_deleted_row_log` (WS4 soft deletes - only `sqlserver_sales_order_deletion_log` is
   exercised), `sqlserver_ref_buying_group_membership` (WS3 bridge - built from implicit 1.0 rows instead),
   `sqlserver_sales_quotes` (WS4 reads `sqlserver_sales_quote_headers` first) and `oracle_wwi_mdm_product_master` (WS6
   margin cost; the mock does emit `WWI_MDM.PRODUCT_MASTER`, so adding it to the registry would populate `cost_usd` -
   left out of the DDL-derived registry on purpose, decision for the product owner).

**Silver customers / dims**
6. Buying-group membership has no OLTP table; `sqlserver_ref_buying_group_membership` is optional, `buying_group_code` is
   derived from the upper-cased name.
7. Salesperson dimension is built from `Application.People` + `SalesTeamMembers` + `SalesTeams`; commission currency /
   quota amount are not in the OLTP extensions so `QUOTA_NO_CURRENCY` has no equivalent.
8. Code-translation table is `oracle_wwi_ref_code_translation` (the brief said `code_xlat`).

**Silver transactions**
9. Exact-copy dedup tie-breaker is `_load_ts` (legacy used staging identity).
10. `REMIT_REF` matching shares precedence tier 1 with explicit `PaymentAllocations` rows.
11. `Sales.Invoices.AmountOutstanding` is carried but the matcher works from gross minus known allocations; gold
    (`fact_payment`) trusts the matcher.
12. `source_system_code` keeps the bronze label (`SQLSERVER_WWI_OLTP`) while business keys use the legacy code
    (`WWI_OLTP`) - both WS3 and WS4 do this; changing it is one constant in each.
13. On-account payments: the mock (and CHECK constraint) has no `ONACCOUNT` status; generated as `UNAPPLIED` with zero
    allocations, which the matcher treats as on-account.

**Rules**
14. `usp_ConvertCurrencyAmounts` uses a PERIOD_END rate for EU lines in a closed GL period; no period-status feed, so EU
    always uses invoice-date / prior-day.
15. NA sales tax: `usp_LoadFactSale` copies the OLTP tax amount, SSIS/`PKG_TAX` recompute. Recomputed.
16. Only `NL_ICA` is flagged reverse-charge in the seed; per-line eligibility comes from the caller's `is_reverse_charge`.
17. Credit-note tax regime: `Returns.CreditNotes.TaxRegimeCode` is the coarse family (`SALESTAX/VAT/GST/...`), not the
    order's regime; `fact_credit_note` joins back to the original sale for the regime.

**Gold facts**
18. ERP cost table: legacy used `PRODUCT_COST` history; only `PRODUCT_MASTER.UNIT_COST_STD` exists. Standard cost for all
    history; `silver.ref_product_cost` honoured if provided.
19. Snapshot dates per batch default to the distinct invoice dates of the batch's `fact_sale` rows (falls back to today).
20. `cash_applied_date` uses the receipt date of the latest allocation.

**Aggregates / sales ops**
21. NA quota basis: SSIS sums `ExtendedPrice + TaxAmount` (gross), the contract says invoiced net USD. Net implemented
    (`measure_basis_code = INVOICED`).
22. EU cap shape: the package has one statutory cap per line; the plan's three bands ladder the *rate*. Implemented as
    written.
23. `vw_SalesByCustomerMonth` YTD is tie-order dependent when two calendar months share a fiscal period (as in legacy).
24. `fact_loyalty_points`, `fact_web_session`, `fact_customer_balance`, `ref_sales_budget`, `dim_warehouse_site`,
    `dim_stock_item` are optional inputs; their measures are 0 / NULL until those domains land.

**Mock data**
25. `Application.Cities` rows are synthetic one-per-customer cities (no `StateProvinces` / `Countries` chain).
26. `--as-of` defaults to today; tests pin it. Consider pinning in the job parameters for byte-identical reruns.

**Integration**
27. `DQ_Reject_Reprocess` (re-feeding corrected rejects) has no lakehouse equivalent yet; rows are re-read from
    `rejected_rows.row_json`.
28. `sales_lakehouse_pipeline` runs bronze in full/incremental according to the watermarks; there is no separate
    "hourly" job. If a lighter hourly path is wanted, run the same job with `stages` narrowed.
29. `PARTY_XREF.SOURCE_SYS_CD`: the mock hub (and its `SOURCE_SYSTEM_REF` seed) labels the OLTP `WWI_SQL`, the legacy
    dedup proc ranks `WWI_OLTP`, the generator contract said `WWIOLTP`. `party_resolution` accepts all three; confirm the
    code the real MDM hub emits so the list can be narrowed.
30. Reverse-charge invoices whose own `CustomerTaxNumber` is NULL are silently backfilled from the customer master (see
    section 6). Should the invoice-level gap be a WARN in its own right (it was not in the legacy EU load)?
31. `rpt_order_to_cash_cycle.order_line` is NULL: `fact_order_fulfilment` is order-grain (legacy `vw_OrderToCashCycle`
    exposed a line number from the order-line fact). Add a line-grain fulfilment fact if reports need it.

## 8. Changelog - integration changes to workstream code

Kept minimal; each workstream's tests stay green.

| Area | Change | Why |
|---|---|---|
| `gold/rules_adapter.py` | Deleted the fallback tax/FX/fiscal implementations and the `USING_FALLBACK` flag; the adapter now only calls `silver.rules.tax/fx/fiscal` and renames outputs for gold | silver rules are always present on the integration branch (task 1) |
| `tests/gold_facts_fixtures.py`, `tests/test_gold_facts_*.py`, `tests/gold_reporting_fixtures.py` | `ref_fx_rate` seeded with the `silver.reference` contract (`currency_code, rate_date, rate_to_usd, rate_type_code, effective_date, rate_source_code, region_code`) instead of `(from_currency_code, to_currency_code, region_code, effective_date, rate, rate_source_code)`; `ref_tax_rate_*`, `dim_fiscal_calendar`, `dim_date` checked against the same source | fixtures encoded a shape the silver layer never produced |
| `tests/test_gold_reporting_views.py::test_rpt_sales_by_territory_month_budget_status` | expected `translation_difference` changed from `-54.0` to `-1.0` | the fixture's monthly average rate is now derived from the daily `ref_fx_rate` quotes (silver contract) rather than a hand-typed `rate` column; the arithmetic of the view is unchanged |
| `silver/rules/fx.py::applyFx` | rate lookup keyed on `(currency, region, anchor_date)` instead of a `monotonically_increasing_id()` row id | the row id is not stable across plan re-evaluation; credit-note FX came back as `DEFAULT_1` although a rate existed |
| `gold/fact_sale.py`, `gold/fact_order.py` | header `customer_business_key` aliased before the line/header join and coalesced afterwards; `fact_sale` also exposes `tax_residual_local` | WS4 writes `customer_business_key` on both header and line tables -> `AMBIGUOUS_REFERENCE` on real silver output (fixtures had it only on the header); residual is needed for `GST_INCLUSIVE_RESIDUAL` validation |
| `gold/fact_sale.py`, `fact_order.py`, `fact_payment.py`, `fact_return.py`, `fact_credit_note.py`, `fact_order_fulfilment.py`, `sales_ops.py`, `aggregates.py`, `partner_feed.py`, `inputs.py`, `reporting.py` | readers standardised on the WS4 silver names via `pickColumn` / `optionalColumn`: `currency_code` (was `transaction_currency_code`), `*_amount_local` (was unsuffixed), `allocated_amount_local`; `inputs.readDimCustomer` conforms WS3 `dim_customer` (`is_current`, `customer_name`, `customer_category_name`, `customer_business_key`) to the gold reader names; `reporting.SqlResolver.currentRow` picks `is_current` / `is_current_row` | gold was written against contract fixtures before WS3/WS4 existed; silver follows CONVENTIONS so gold adapts |
| `silver/transactions.py::customerContext` | customers deduplicated to one row per `_cust_key` (latest `LastEditedWhen` / `_load_ts` / `ExtractedRowVersion` wins) via `latestExtractPerKey` | the `DUPLICATE_CUSTOMER_EXTRACT` mock rows (5 customers extracted twice) fanned every order / invoice / payment / quote of those customers out to two silver rows (128 duplicate `order`, 114 `sale`, 88 `payment` business keys in both the local and the workspace run) |
| `gold/inputs.py::conformDimSalesperson` / `readDimSalesperson`, `fact_sale.py`, `fact_order.py`, `fact_credit_note.py`, `sales_ops.py` | `silver.dim_salesperson` (WS3) exposes `wwi_employee_id`, not the `salesperson_business_key` / `wwi_person_id` the gold readers were written against; the adapter derives `WWI_OLTP|<PersonID>` with `sourceSystemKey` (same key WS4 stamps on the facts) | `UNRESOLVED_COLUMN salesperson_business_key` on real silver |
| `gold/fact_sale.py` | `promotion_business_key` read with `optionalColumn` | WS4 `sale_line` carries no promotion key (legacy `Sales.InvoiceLines` has none either); the fact keeps the column as NULL -> `-1` |
| `gold/reporting.py::SqlResolver.firstCol`, `gold/partner_feed.py` | views pick the silver dimension attribute name (`sales_territory_name`, `sales_channel_name`, `customer_name`, `customer_category_name`, `partner_identifier`) with the legacy `Dimension.*` names as first candidates | WS6 views/feed were written against legacy-shaped fixtures (`sales_territory`, `sales_channel`, `customer`, `partner_name`) |
| `common/spark.py::getSpark` | honours `SALES_LAKEHOUSE_WAREHOUSE_DIR` (persistent warehouse + Derby metastore) when no explicit `warehouseDir` is passed | lets the CLI, the validation module and ad-hoc probes share one local metastore across processes |
| `notebooks/20_silver/{party_resolution,customers,dimensions}.py` | added the `batch_id` widget and pass it into `PipelineConfig` | every task of the end-to-end job shares `{{job.run_id}}` |
| `bronze/ingest.py`, `bronze/registry.py`, `common/config.py`, `mock_data/*`, `silver/transactions.py`, tests | ruff findings (`I001`, `RUF100`, `UP`, `B`, `E501`) fixed; `databricks/ruff.toml` added (line length 140, `E,F,I,B,UP`, notebook/test ignores) | `ruff check databricks` clean |
| `silver/party_resolution.py::WWI_OLTP_SOURCE_SYSTEM_CODES` | `WWI_SQL` accepted as an OLTP source-system code next to `WWI_OLTP` / `WWIOLTP` | the mock `PARTY_XREF` rows carry `WWI_SQL`, so every customer resolved as `MISSING_XREF` (`erp_party_id = -1`) and the `XREF_RETIRED_*` edge cases never reached a survivor |
| `gold/reporting.py::orderToCashCycleSql` | `order_line` read through `firstCol` (NULL when absent) | `fact_order_fulfilment` is order-grain and has no `order_line_number`; the view failed with `UNRESOLVED_COLUMN` on real data (the fixture had the column) |
| `quality/validate_mock.py::checkReverseChargeNullVat` | expectation relaxed from "every line is `REVERSE_CHARGE_NO_VATREG`" to "every line is reverse-charged with zero tax", reporting how many are `NO_VATREG` | silver backfills the invoice-level NULL from the customer master (legacy `FACT_EU_Load_Sale` behaviour), see section 6 / open question 30 |
| `quality/validate_mock.py::checkXrefResolution` | `XREF_RETIRED_NO_MERGE` read from `silver.party_resolution.resolution_status_code` (falls back to `silver.customer`) | the planted customer is also an EU `CUSTOMER_NO_CONSENT` reject, so it is absent from `silver.customer`; the resolution table is the record of the walk (`RETIRED_NO_SURVIVOR`, WARN) |
| `gold/reporting.py::orderToCashCycleSql` | milestone lag / value / SLA columns resolved through `firstCol` (`order_to_pick_days`, `invoice_value_reporting`, ...; `pick_sla_breach_flag`, `perfect_order_flag` NULL when absent) | the fixture used `*_lag_days` / `invoiced_value_reporting` names the real `fact_order_fulfilment` does not have |
| `ruff.toml` | `E501` ignored for `bronze/registry.py` | the file is generated and `test_generator_output_is_checked_in` compares it byte for byte with `generate_registry.render` |
| new | `orchestration/pipeline.py`, `notebooks/90_orchestration/run_pipeline.py`, `resources/sales_lakehouse_pipeline_job.yml`, `quality/checks.py`, `quality/validate_mock.py`, `notebooks/95_validation/validate_against_mock.py`, tests for each | deliverables A-D |
