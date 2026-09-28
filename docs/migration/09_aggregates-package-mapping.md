# 09_aggregates — SSIS `WWI_Aggregates` -> Databricks bundle `wwi_09_aggregates`

Source: `ssis/09_aggregates/` (13 `AGG_*` packages emitted by `build_aggregate_packages.py`), the aggregate DDL in
`sqlserver/warehouse/aggregates/`, the `Report.vw_*` views in `sqlserver/views/` and
`sqlserver/procedures/facts/Integration.usp_PublishReportingLayer.sql`.
Target: `databricks/09_aggregates/` — one job `wwi_09_aggregates`, one notebook per package (task_key = package name),
shared helpers in `src/`, reconciliation in `validation/`, pytest suite in `tests/`.

## 1. Package -> notebook / task

| # | Legacy package | Notebook / task_key | Target (legacy -> Delta) | Refresh strategy (why) |
|---|---|---|---|---|
| 1 | `AGG_Refresh_DailySalesSummary` | `notebooks/AGG_Refresh_DailySalesSummary.py` | `Aggregate.Daily Sales Summary` -> `gold.agg_daily_sales_summary` | **Windowed** `replaceWhere sales_date BETWEEN RefreshFromDate AND RefreshToDate` (default `BusinessDate-3 .. BusinessDate`, the legacy *Init Refresh Window* trailing-3-day re-state). Not an MV: the package only re-states a window and applies a small-cell suppression reject. |
| 2 | `AGG_Refresh_MonthlySalesSummary` | `notebooks/AGG_Refresh_MonthlySalesSummary.py` | `Aggregate.Monthly Sales Summary` -> `gold.agg_monthly_sales_summary` | **Windowed** `replaceWhere calendar_month` for `AccountingPeriodCode` and `RebuildPriorPeriods` preceding months (legacy *Init Refresh Period* + `DELETE ... WHERE CalendarMonth BETWEEN`). |
| 3 | `AGG_Refresh_DailyInventoryHealth` | `notebooks/AGG_Refresh_DailyInventoryHealth.py` | `Aggregate.Daily Inventory Health` -> `gold.agg_daily_inventory_health` | **Windowed** `replaceWhere snapshot_date` (daily snapshot window, `StockOutThreshold` parameter). |
| 4 | `AGG_Refresh_MonthlyMarginAnalysis` | `notebooks/AGG_Refresh_MonthlyMarginAnalysis.py` | `Aggregate.Monthly Margin Analysis` -> `gold.agg_monthly_margin_analysis` | **Windowed** `replaceWhere calendar_month`; `ApportionPurchaseVariance` toggles PPV apportionment exactly as the legacy derived column. |
| 5 | `AGG_Refresh_Customer360` | `notebooks/AGG_Refresh_Customer360.py` | `Aggregate.Customer 360` -> `gold.agg_customer_360` | **Full rebuild** (atomic Delta `overwrite`). The legacy package truncates and reloads the whole customer grain (`AsAtDate`, `IncludeInactiveCustomers`). Implemented as an overwrite notebook rather than `CREATE OR REPLACE MATERIALIZED VIEW` because the package (a) rejects inactive customers into `etl.rejected_record`, (b) needs `refresh_batch_id`/`refreshed_datetime` stamps for the publish staleness gate, and (c) the parent job must be able to run it on serverless job compute without a SQL warehouse; MV-ready SQL is isolated in `agg_sql.customer360Sql` should the platform team prefer an MV + `REFRESH` task later. |
| 6 | `AGG_Refresh_CustomerRolling12Month` | `notebooks/AGG_Refresh_CustomerRolling12Month.py` | `Aggregate.Customer Rolling 12 Month` -> `gold.agg_customer_rolling_12_month` | **Windowed** `replaceWhere calendar_month` over the trailing `RollingMonths` (12) months ending at the accounting period — the legacy rolling-12 delete/reload. Window functions compute rolling 12/3-month revenue, trend and consecutive inactive months. |
| 7 | `AGG_Refresh_ProductPerformance` | `notebooks/AGG_Refresh_ProductPerformance.py` | `Aggregate.Product Performance` -> `gold.agg_product_performance` | **Windowed** `replaceWhere calendar_month`; ABC class from cumulative revenue share (`AbcThresholdA/B`), velocity band, days-of-cover, return rate. |
| 8 | `AGG_Refresh_SupplierPerformance` | `notebooks/AGG_Refresh_SupplierPerformance.py` | `Aggregate.Supplier Performance` -> `gold.agg_supplier_performance` | **Windowed** `replaceWhere calendar_month`; on-time / in-full / quality measures. |
| 9 | `AGG_Refresh_RegionalSalesPerformance` | `notebooks/AGG_Refresh_RegionalSalesPerformance.py` | `Aggregate.Regional Sales Performance` -> `gold.agg_regional_sales_performance` | **Windowed** `replaceWhere calendar_month`; FX to `ReportingCurrency` via `silver.ref_fx_rate_monthly`, budget variance via `silver.ref_sales_budget`, regional fiscal calendar from `gold.dim_date`. |
| 10 | `AGG_Refresh_FinanceCloseSummary` | `notebooks/AGG_Refresh_FinanceCloseSummary.py` | `Aggregate.Finance Close Summary` -> `gold.agg_finance_close_summary` | **Windowed** `replaceWhere (fiscal_year, fiscal_period)` for the accounting period (+ prior periods); `MaterialityAmount` drives the control-variance reject; closed periods are re-stated only when explicitly requested through `RebuildPriorPeriods`. |
| 11 | `AGG_Refresh_PromotionEffectiveness` | `notebooks/AGG_Refresh_PromotionEffectiveness.py` | `Aggregate.Promotion Effectiveness` -> `gold.agg_promotion_effectiveness` | **Windowed** `replaceWhere promotion_key IN (promotions active in the period)`; `BaselineWeeks` pre-period baseline for uplift. |
| 12 | `AGG_Refresh_DeliveryPerformanceSummary` | `notebooks/AGG_Refresh_DeliveryPerformanceSummary.py` | `Aggregate.Delivery Performance Summary` -> `gold.agg_delivery_performance_summary` | **Windowed** `replaceWhere iso_week_start_date` for the ISO weeks touched by the refresh window; `OnTimeGraceHours`. |
| 13 | `AGG_Publish_ReportingLayer` | `notebooks/AGG_Publish_ReportingLayer.py` | `Report.vw_*` -> `gold.rpt_*` views, `Report.PublishState` -> `gold.rpt_publish_state`, `Integration.ReportingPublication` -> `silver.int_reporting_publication` | `CREATE OR REPLACE VIEW` per report (atomic repoint, no drop window), gated by the legacy publish rules; see §4. |

Why no `CREATE OR REPLACE MATERIALIZED VIEW`: only `AGG_Refresh_Customer360` is a true full rebuild, and it carries
reject routing and control stamps an MV cannot express (see row 5). All other packages re-state a bounded window
(daily / accounting period / rolling 12 / ISO week), for which Delta `replaceWhere` is the idempotent equivalent of the
legacy `DELETE ... WHERE <window>` + `INSERT`. Re-running the same `BatchId`/window yields identical rows (verified in
`tests/test_refresh_package.py::test_rerun_same_window_is_idempotent_and_keeps_other_windows`).

## 2. Source objects (legacy -> Delta)

Resolved once per notebook via `agg_common.resolveTables(catalog, naming.table)`; nothing is hard-coded.

| Package | Reads (gold / silver) |
|---|---|
| DailySalesSummary | `fact_sale`, `fact_return`, `dim_stock_item` |
| DailyInventoryHealth | `fact_daily_inventory_snapshot`, `fact_stock_movement`, `dim_stock_item` |
| MonthlySalesSummary | `fact_sale`, `fact_return`, `fact_credit_note`, `dim_date` |
| MonthlyMarginAnalysis | `fact_sales_margin`, `dim_stock_item`, `dim_date` |
| Customer360 | `fact_sale`, `fact_return`, `fact_customer_payment`, `fact_loyalty_points`, `fact_monthly_customer_balance`, `fact_web_session`, `dim_customer` |
| CustomerRolling12Month | `fact_sale`, `fact_return`, `fact_customer_payment`, `fact_loyalty_points`, `fact_web_session`, `dim_date` |
| ProductPerformance | `fact_sale`, `fact_return`, `fact_daily_inventory_snapshot`, `dim_stock_item` |
| SupplierPerformance | `fact_purchase`, `fact_purchase_receipt`, `fact_supplier_payment`, `dim_stock_item` |
| RegionalSalesPerformance | `fact_sale`, `dim_date`, `silver.ref_fx_rate_monthly`, `silver.ref_sales_budget` |
| FinanceCloseSummary | `fact_gl_posting`, `fact_monthly_ap_aging`, `fact_monthly_ar_aging`, `dim_gl_account` |
| PromotionEffectiveness | `fact_sale`, `fact_return`, `fact_loyalty_points`, `fact_promotion_eligibility`, `dim_promotion`, `dim_stock_item` |
| DeliveryPerformanceSummary | `fact_shipment` |
| Publish_ReportingLayer | all `gold.agg_*`, `silver.int_reporting_publication`, `etl.watermark` (via `control.getWatermark`), dims/facts listed in §4 |

`Fact.X` -> `gold.fact_x`, `Dimension.X` -> `gold.dim_x`, `ref.X` -> `silver.ref_x`, `Integration.X` -> `silver.int_x`,
`Aggregate.X` -> `gold.agg_x`, `Report.X` -> `gold.rpt_x`.

## 3. SSIS component -> Spark construct

| SSIS component (every `AGG_Refresh_*`) | Spark / `dbx_etl_common` |
|---|---|
| Package parameters (`RefreshFromDate`, `AccountingPeriodCode`, `RebuildPriorPeriods`, thresholds …) | job parameters -> `dbutils.widgets`; `1900-01-01` / `1900-01` sentinels mean *not supplied* (`agg_common.dailyWindow` / `periodWindow`) |
| `Log Package Start` / `Log Package End` / OnError handler | `control.packageRun(...)` context manager (start, end with row counts, `logError` + `Failed` on exception) |
| `Init Refresh Window` / `Init Refresh Period` (Execute SQL) | `agg_common.dailyWindow`, `periodWindow`, `RefreshWindow.sqlLiteral` |
| `Delete Refresh Window` (Execute SQL `DELETE … WHERE <window>`) + `Load Aggregate` (OLE DB destination) | one Delta write with `.option("replaceWhere", predicate)` (`agg_common.overwriteWindow`) — atomic delete+insert |
| `Truncate Aggregate` + full reload (Customer360) | `df.write.mode("overwrite")` (`agg_common.overwriteAll`) |
| Data Flow source (`SELECT … FROM Fact.* JOIN Dimension.*` with `GROUP BY`) | `agg_sql.<package>Sql(...)` Spark SQL with the same grain / measures (`AGGREGATE_KEY_COLUMNS` documents the grain) |
| Derived Column (ratios, ABC class, on-time flag, FX, PPV apportionment …) | expressions inside the same SQL (`CASE`, window functions) |
| Conditional Split -> reject output (`Small cell`, `No receipts`, `Unassigned territory`, …) | `RefreshSpec.rejectCondition` -> `control.logRejectedRecordSet(objectName, rejectedDf, rejectReasonCode=…, businessKeyColumn="agg_business_key")` |
| Row Count transforms (`RowsRead`, `RowsInserted`, `RowsRejected`) | `RefreshResult` counts -> `control.logRowCount(...)` and `run.rows*` |
| `Reconcile Row Counts` (`RowsRead = RowsInserted + RowsRejected` audit) | `agg_common.assertAggregateReconciliation` + `control.assertRowCountTolerance(scope="OBJECT", absoluteTolerance=rejects)` |
| `Set Watermark` | `control.setWatermark(sourceSystemCode="WWIDW", objectName="Aggregate.<name>", watermarkTo=…)` |
| Refresh stamps (`RefreshBatchId`, `RefreshedDateTime`) | `refresh_batch_id`, `refreshed_datetime` columns added in `agg_package.buildRefreshFrame` |

## 4. `AGG_Publish_ReportingLayer`

`Integration.usp_PublishReportingLayer` behaviours and their targets:

| Legacy step | Databricks |
|---|---|
| `@SkipRefresh = 0` -> call every `Aggregate.usp_Refresh*` in order | replaced by the job DAG: the publish task `depends_on` the daily aggregates and the tail of the month-end chain; `run_if: NONE_FAILED` lets it publish when the month-end branch is skipped by `Should_Run_MonthEnd_Aggregates` |
| Publication list `Integration.ReportingPublication` (group, sequence, enabled) | `silver.int_reporting_publication` (`PublicationGroupCode`, `PublishSequence`, `IsEnabled`, `LastPublishedAt`, `LastPublishedByExecutionId`) -> `etl.configuration` key `ReportingPublicationList.<group>` -> `ReportingPublicationList` -> default order (`rpt_views.DEFAULT_PUBLICATION_ORDER`) |
| Staleness check on aggregate refresh timestamps | `control.getWatermark("WWIDW", "Aggregate.<name>")` per required aggregate; `> MaxStalenessHours` (26) -> `logRejectedRecord(AGG_STALE_AT_PUBLICATION)`; raises when `FailOnStaleSource=True` |
| Publish rules: yesterday has daily sales; yesterday has inventory snapshot; no EU customer past retention un-anonymised | `agg_publish.evaluatePublishRules` (same three rules, same reason codes) |
| `@ForcePublish` -> `Report.PublishState` = `OK` / `FORCED` | `agg_publish.publishDecision` -> `gold.rpt_publish_state` (`publish_scope_code = 'DW'`, single row upsert) |
| Publish = swap report views | `CREATE OR REPLACE VIEW gold.rpt_* AS …` in publication order (`rpt_views.viewDdl`) — atomic repoint |
| Rebuild reporting indexes | `OPTIMIZE gold.agg_*` for the aggregates the published views read (`OptimizeAggregates`) |
| `usp_LogRowCount('Report.PublishState')` | `control.logRowCount(...)` per published view + publish state |

Report views migrated (16 = every `sqlserver/views/Report.vw_*.sql`):

| `Report.*` | `gold.rpt_*` | reads |
|---|---|---|
| vw_DailySalesTrend | rpt_daily_sales_trend | agg_daily_sales_summary, dim_sales_channel, dim_sales_territory |
| vw_InventoryHealthCurrent | rpt_inventory_health_current | agg_daily_inventory_health, dim_product_category, dim_warehouse_site |
| vw_SalesByCustomerMonth | rpt_sales_by_customer_month | agg_monthly_sales_summary, dim_customer |
| vw_SalesByProductMonth | rpt_sales_by_product_month | agg_product_performance, dim_stock_item, dim_product_category |
| vw_SalesByTerritoryMonth | rpt_sales_by_territory_month | agg_regional_sales_performance, dim_sales_territory, dim_sales_channel |
| vw_MarginByProductCategory | rpt_margin_by_product_category | agg_monthly_margin_analysis, dim_product_category, dim_sales_territory |
| vw_Customer360 | rpt_customer_360 | agg_customer_360, agg_customer_rolling_12_month, dim_customer_segment, dim_loyalty_tier |
| vw_CustomerChurnRisk | rpt_customer_churn_risk | agg_customer_360, agg_customer_rolling_12_month |
| vw_ReturnsRateByCategory | rpt_returns_rate_by_category | agg_product_performance, fact_return, dim_stock_item, dim_product_category |
| vw_PromotionRoi | rpt_promotion_roi | agg_promotion_effectiveness, dim_promotion, dim_product_category, dim_sales_channel |
| vw_SupplierSpendYtd | rpt_supplier_spend_ytd | agg_supplier_performance, dim_supplier |
| vw_SupplierOnTimeDelivery | rpt_supplier_on_time_delivery | fact_purchase_receipt, dim_supplier |
| vw_FinanceCloseStatus | rpt_finance_close_status | agg_finance_close_summary |
| vw_ApAgingCurrent | rpt_ap_aging_current | fact_monthly_ap_aging, dim_supplier |
| vw_LoyaltyProgramSummary | rpt_loyalty_program_summary | fact_loyalty_points, dim_loyalty_tier |
| vw_OrderToCashCycle | rpt_order_to_cash_cycle | fact_order_fulfilment, dim_customer, dim_sales_territory, dim_warehouse_site |

## 5. Control framework usage (`dbx_etl_common`)

`params.getJobParams`, `naming.table`, `control.packageRun`, `control.logRowCount`, `control.logRejectedRecord`,
`control.logRejectedRecordSet`, `control.getWatermark`, `control.setWatermark`, `control.getConfiguration`,
`control.assertRowCountTolerance`. The wheel is attached to the job as a serverless environment dependency
(`${var.dbx_etl_common_wheel}`, default `../common/dist/dbx_etl_common-0.1.0-py3-none-any.whl` from session 00); the
notebooks add `../src` to `sys.path` for the project helpers. `tests/fakes/dbx_etl_common/` is a call-recording fake
used only by pytest.

## 6. Job parameters

Common: `BatchId`, `BusinessDate`, `ReloadFullHistory`, `EnvironmentCode`, `RestartFromStep`, `catalog`.
Project: `RunMonthEndAggregates` (condition task gating the accounting-period chain), `RefreshFromDate`,
`RefreshToDate`, `AccountingPeriodCode`, `RebuildPriorPeriods`, `PublicationGroupCode`, `ForcePublish`.
Package-level defaults from the .dtsx are notebook widgets: `StockOutThreshold=0`, `OnTimeGraceHours=12`,
`AsAtDate`, `IncludeInactiveCustomers=0`, `RollingMonths=12`, `AbcThresholdA=80`, `AbcThresholdB=95`,
`ApportionPurchaseVariance=True`, `ReportingCurrency=USD`, `MaterialityAmount=100`, `BaselineWeeks=8`,
`MaxStalenessHours=26`, `FailOnStaleSource=True`, `OptimizeAggregates=True`.

## 7. Validation

`validation/AGG_Reconcile_Aggregates.py`: for every `gold.agg_*` (and optionally every `gold.rpt_*`) computes row count,
`SUM(xxhash64(concat_ws('|', <sorted columns minus refresh stamps>)))` and the sum of the key measures per period
(`sales_date` / `snapshot_date` / `calendar_month` / `fiscal_period` / `iso_week_start_date`, plus a `*` total) and compares
with the SQL Server baseline supplied as `BaselineJson` or Delta table `etl.agg_reconciliation_baseline`
(`ObjectName, PeriodValue, RowCount, RowHash, MeasureName, MeasureValue`). Results -> `control.logRowCount`
(`SourceRowCount` = baseline, `TargetRowCount` = Databricks, `RejectRowCount` = failed metrics).

## 8. Not migrated / needs decision

| Item | Why / decision needed |
|---|---|
| **Materialized views** | Deliberately not used (see §1). If the platform prefers `CREATE OR REPLACE MATERIALIZED VIEW gold.agg_customer_360` + a `REFRESH` SQL task on a warehouse, `agg_sql.customer360Sql` is the SELECT; the reject routing of inactive customers would then have to move to a DQ rule. |
| **Repo static check `forbidden-content`** | `validation/static/run_all_checks.py` forbids the words *Databricks* / *dbutils* / *Unity Catalog* anywhere outside the legacy sample dirs, so the mandatory `# Databricks notebook source` header and `dbutils` calls in every session's `databricks/<NN>/` tree fail it (33 hits here, all `forbidden-content`; the same holds for the already-open session PRs). Every other check passes, and `run_deep_checks.py` passes. Session 00 / the parent session should add `databricks/` and `docs/migration/` to the check's exclusions (validation/ is read-only for this session). |
| **Publication sources for fact-backed reports** | `rpt_supplier_on_time_delivery`, `rpt_ap_aging_current`, `rpt_loyalty_program_summary`, `rpt_order_to_cash_cycle` read facts directly; they have no aggregate freshness gate (as in the legacy procedure). |
| **`Report.PublishState` history** | The legacy table keeps one row per scope; `gold.rpt_publish_state` mirrors that (upsert). If an audit trail per publication is wanted, append to `etl.row_count_log` (already logged) or add a history table. |
| **Index rebuilds** | `OPTIMIZE` replaces `ALTER INDEX … REBUILD`; `ZORDER` columns are not specified (Liquid clustering / column choice is a platform decision). |
| **Live SQL execution** | The SQL was parsed by Spark and exercised against synthetic Delta tables for `agg_daily_sales_summary`; column names for the other facts/dims follow `sqlserver/warehouse/*` snake_cased and must be confirmed against the dimension/fact sessions' Delta schemas before the first deploy. |
| **Retention / anonymisation rule** | The EU-retention publish rule reads `agg_customer_360.anonymised_flag` / `retention_expiry_date`, which are derived from `dim_customer`; the anonymisation itself remains session 14 (`C360_*`). |
