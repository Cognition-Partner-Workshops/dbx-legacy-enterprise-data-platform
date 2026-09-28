"""Gold report views replacing ``Report.vw_*`` (column-for-column, snake_case).

Legacy view                      -> gold view
Report.vw_DailySalesTrend        -> rpt_daily_sales_trend
Report.vw_SalesByCustomerMonth   -> rpt_sales_by_customer_month
Report.vw_SalesByProductMonth    -> rpt_sales_by_product_month
Report.vw_SalesByTerritoryMonth  -> rpt_sales_by_territory_month
Report.vw_OrderToCashCycle       -> rpt_order_to_cash_cycle
Report.vw_MarginByProductCategory-> rpt_margin_by_product_category
Report.vw_Customer360            -> rpt_customer_360
Report.vw_CustomerChurnRisk      -> rpt_customer_churn_risk

Every view is created with ``CREATE OR REPLACE VIEW`` over the gold aggregate
tables and silver dimensions.  Dimensions that are optional for the sales
domain (stock item, product category, customer segment, loyalty tier,
warehouse site) are joined when present and projected as NULL otherwise, so
the column contract holds even before those workstreams land.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import tableExists

RPT_DAILY_SALES_TREND = "rpt_daily_sales_trend"
RPT_SALES_BY_CUSTOMER_MONTH = "rpt_sales_by_customer_month"
RPT_SALES_BY_PRODUCT_MONTH = "rpt_sales_by_product_month"
RPT_SALES_BY_TERRITORY_MONTH = "rpt_sales_by_territory_month"
RPT_ORDER_TO_CASH_CYCLE = "rpt_order_to_cash_cycle"
RPT_MARGIN_BY_PRODUCT_CATEGORY = "rpt_margin_by_product_category"
RPT_CUSTOMER_360 = "rpt_customer_360"
RPT_CUSTOMER_CHURN_RISK = "rpt_customer_churn_risk"

REPORT_VIEWS: dict[str, str] = {
    "Report.vw_DailySalesTrend": RPT_DAILY_SALES_TREND,
    "Report.vw_SalesByCustomerMonth": RPT_SALES_BY_CUSTOMER_MONTH,
    "Report.vw_SalesByProductMonth": RPT_SALES_BY_PRODUCT_MONTH,
    "Report.vw_SalesByTerritoryMonth": RPT_SALES_BY_TERRITORY_MONTH,
    "Report.vw_OrderToCashCycle": RPT_ORDER_TO_CASH_CYCLE,
    "Report.vw_MarginByProductCategory": RPT_MARGIN_BY_PRODUCT_CATEGORY,
    "Report.vw_Customer360": RPT_CUSTOMER_360,
    "Report.vw_CustomerChurnRisk": RPT_CUSTOMER_CHURN_RISK,
}


@dataclass(frozen=True)
class SqlResolver:
    """Builds table references / optional joins against the configured catalog."""

    spark: SparkSession
    cfg: PipelineConfig

    def table(self, layer: str, table: str) -> str:
        return self.cfg.fqn(layer, table)

    def exists(self, layer: str, table: str) -> bool:
        return tableExists(self.spark, self.cfg.fqn(layer, table))

    def optionalJoin(self, layer: str, table: str, alias: str, condition: str) -> str:
        if not self.exists(layer, table):
            return ""
        return f"LEFT JOIN {self.table(layer, table)} AS {alias} ON {condition}"

    def optionalCol(self, layer: str, table: str, alias: str, column: str, sqlType: str = "STRING") -> str:
        if not self.exists(layer, table):
            return f"CAST(NULL AS {sqlType})"
        return f"{alias}.{column}"


def dailySalesTrendSql(r: SqlResolver) -> str:
    return f"""
CREATE OR REPLACE VIEW {r.table('gold', RPT_DAILY_SALES_TREND)} AS
WITH rolled AS (
    SELECT d.sales_date, d.region_code, d.sales_territory_key, d.sales_channel_key,
           MAX(d.fiscal_year) AS fiscal_year, MAX(d.fiscal_period) AS fiscal_period,
           SUM(d.invoice_count) AS invoice_count, SUM(d.line_count) AS line_count,
           SUM(d.distinct_customer_count) AS customer_count_sum_of_items,
           SUM(d.quantity_sold_base_uom) AS quantity_sold, SUM(d.gross_sales_amount) AS gross_sales,
           SUM(d.line_discount_amount) AS line_discount, SUM(d.promotion_discount_amount) AS promotion_discount,
           SUM(d.net_sales_amount) AS net_sales, SUM(d.tax_amount) AS tax, SUM(d.freight_amount) AS freight,
           SUM(d.cost_of_sales_amount) AS cost_of_sales, SUM(d.gross_margin_amount) AS gross_margin,
           SUM(d.returns_amount) AS returns, SUM(d.net_sales_amount_reporting) AS net_sales_reporting,
           SUM(d.prior_year_net_sales) AS prior_year_net_sales
    FROM {r.table('gold', 'agg_daily_sales')} AS d
    GROUP BY d.sales_date, d.region_code, d.sales_territory_key, d.sales_channel_key
)
SELECT
    r.sales_date                                         AS sales_date,
    dayofweek(r.sales_date)                              AS day_of_week,
    r.fiscal_year                                        AS fiscal_year,
    r.fiscal_period                                      AS fiscal_period,
    r.region_code                                        AS region,
    terr.sales_territory                                 AS territory,
    ch.sales_channel                                     AS channel,
    r.invoice_count                                      AS invoices,
    r.line_count                                         AS lines,
    r.quantity_sold                                      AS quantity_sold,
    r.gross_sales                                        AS gross_sales,
    r.line_discount + COALESCE(r.promotion_discount, 0)  AS total_discount,
    r.net_sales                                          AS net_sales,
    r.net_sales_reporting                                AS net_sales_reporting_currency,
    r.tax                                                AS indirect_tax,
    r.freight                                            AS freight,
    r.cost_of_sales                                      AS cost_of_sales,
    r.gross_margin                                       AS gross_margin,
    ROUND(100.0 * r.gross_margin / NULLIF(r.net_sales, 0), 2) AS margin_pct,
    r.returns                                            AS returns,
    r.prior_year_net_sales                               AS prior_year_net_sales,
    ROUND(100.0 * (r.net_sales - r.prior_year_net_sales) / NULLIF(r.prior_year_net_sales, 0), 2)
                                                         AS prior_year_growth_pct,
    LAG(r.net_sales, 7) OVER (PARTITION BY r.sales_territory_key, r.sales_channel_key ORDER BY r.sales_date)
                                                         AS net_sales_same_day_last_week,
    AVG(r.net_sales) OVER (PARTITION BY r.sales_territory_key, r.sales_channel_key ORDER BY r.sales_date
                           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS seven_day_moving_average,
    SUM(r.net_sales_reporting) OVER (PARTITION BY r.sales_territory_key, r.sales_channel_key, r.fiscal_year
                                     ORDER BY r.sales_date ROWS UNBOUNDED PRECEDING) AS fiscal_year_to_date_revenue,
    RANK() OVER (PARTITION BY r.sales_date, r.region_code ORDER BY r.net_sales_reporting DESC)
                                                         AS rank_in_region_that_day
FROM rolled AS r
LEFT JOIN {r.table('silver', 'dim_sales_territory')} AS terr ON terr.sales_territory_key = r.sales_territory_key
LEFT JOIN {r.table('silver', 'dim_sales_channel')} AS ch ON ch.sales_channel_key = r.sales_channel_key
"""


def salesByCustomerMonthSql(r: SqlResolver) -> str:
    return f"""
CREATE OR REPLACE VIEW {r.table('gold', RPT_SALES_BY_CUSTOMER_MONTH)} AS
SELECT
    m.customer_key                                       AS customer_key,
    c.customer                                           AS customer_name,
    c.category                                           AS customer_category,
    m.region_code                                        AS region,
    m.fiscal_calendar_code                               AS fiscal_calendar,
    m.fiscal_year                                        AS fiscal_year,
    m.fiscal_period                                      AS fiscal_period,
    CASE m.region_code
        WHEN 'NA' THEN CONCAT('FY', m.fiscal_year, ' P', LPAD(m.fiscal_period, 2, '0'))
        WHEN 'EU' THEN CONCAT('FY', m.fiscal_year, '/', RIGHT(CAST(m.fiscal_year + 1 AS STRING), 2),
                              ' P', LPAD(m.fiscal_period, 2, '0'))
        ELSE CONCAT('FY', m.fiscal_year, ' P', LPAD(m.fiscal_period, 2, '0'), ' (13P)')
    END                                                  AS fiscal_period_label,
    m.calendar_month                                     AS calendar_month,
    m.order_count                                        AS orders,
    m.invoice_count                                      AS invoices,
    m.quantity_sold_base_uom                             AS units,
    m.gross_revenue                                      AS gross_revenue,
    m.discount_given                                     AS discount_given,
    m.net_revenue                                        AS net_revenue_local,
    m.net_revenue_reporting                              AS net_revenue,
    m.credit_notes_reporting                             AS credit_notes,
    m.returns_reporting                                  AS returns,
    m.net_revenue_after_credits                          AS net_revenue_after_credits,
    m.gross_margin_reporting                             AS gross_margin,
    CASE WHEN COALESCE(m.net_revenue_reporting, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * m.gross_margin_reporting / m.net_revenue_reporting, 2) END AS margin_pct,
    m.average_order_value                                AS average_order_value,
    m.prior_period_net_revenue                           AS prior_period_net_revenue,
    m.period_over_period_percent                         AS period_on_period_pct,
    m.prior_year_net_revenue                             AS prior_year_net_revenue,
    m.year_over_year_percent                             AS year_on_year_pct,
    m.rolling_3_period_net_revenue                       AS rolling_3_period_revenue,
    SUM(m.net_revenue_reporting) OVER (PARTITION BY m.customer_key, m.fiscal_year ORDER BY m.fiscal_period
                                       ROWS UNBOUNDED PRECEDING) AS year_to_date_revenue,
    CASE WHEN m.period_closed_flag THEN 'Closed' ELSE 'Period To Date' END AS period_status,
    m.refreshed_datetime                                 AS data_as_of
FROM {r.table('gold', 'agg_monthly_sales')} AS m
LEFT JOIN {r.table('silver', 'dim_customer')} AS c ON c.customer_key = m.customer_key AND c.is_current_row
"""


def salesByProductMonthSql(r: SqlResolver) -> str:
    return f"""
CREATE OR REPLACE VIEW {r.table('gold', RPT_SALES_BY_PRODUCT_MONTH)} AS
SELECT
    p.calendar_month                                     AS calendar_month,
    p.stock_item_key                                     AS stock_item_key,
    {r.optionalCol('silver', 'dim_stock_item', 'si', 'stock_item')}       AS product,
    {r.optionalCol('silver', 'dim_stock_item', 'si', 'brand')}            AS brand,
    {r.optionalCol('silver', 'dim_stock_item', 'si', 'size')}             AS size,
    {r.optionalCol('silver', 'dim_product_category', 'cat', 'product_category')} AS category,
    p.region_code                                        AS region,
    p.units_sold_base_uom                                AS units_sold,
    p.net_revenue_reporting                              AS net_revenue,
    p.gross_margin_reporting                             AS gross_margin,
    p.margin_percent                                     AS margin_pct,
    p.discount_depth_percent                             AS discount_depth_pct,
    p.average_selling_price                              AS average_selling_price,
    p.average_unit_cost                                  AS average_unit_cost,
    p.units_returned                                     AS units_returned,
    p.return_rate_percent                                AS return_rate_pct,
    p.inventory_turns                                    AS inventory_turns,
    p.stockout_days                                      AS stockout_days,
    p.lost_sales_estimate_reporting                      AS estimated_lost_sales,
    p.sell_through_percent                               AS sell_through_pct,
    p.distinct_customer_count                            AS buying_customers,
    p.abc_class                                          AS abc_class,
    p.xyz_class                                          AS xyz_class,
    CONCAT(p.abc_class, '/', COALESCE(p.xyz_class, '?'))  AS abc_xyz,
    p.prior_month_abc_class                              AS prior_month_abc,
    CASE WHEN p.prior_month_abc_class IS NULL THEN 'New'
         WHEN p.abc_class < p.prior_month_abc_class THEN 'Promoted'
         WHEN p.abc_class > p.prior_month_abc_class THEN 'Demoted'
         ELSE 'Stable' END                               AS class_movement,
    p.rank_in_category_by_revenue                        AS category_revenue_rank,
    p.rank_in_category_by_margin                         AS category_margin_rank,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY p.region_code, p.calendar_month
                                 ORDER BY p.net_revenue_reporting DESC) <= 500 THEN 1 ELSE 0 END AS top_500_flag,
    prev.net_revenue_reporting                           AS prior_month_net_revenue,
    CASE WHEN COALESCE(prev.net_revenue_reporting, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * (p.net_revenue_reporting - prev.net_revenue_reporting) / prev.net_revenue_reporting, 2)
    END                                                  AS month_on_month_pct,
    p.new_product_flag                                   AS new_product_flag,
    p.discontinued_flag                                  AS discontinued_flag,
    p.refreshed_datetime                                 AS data_as_of
FROM {r.table('gold', 'agg_product_performance')} AS p
{r.optionalJoin('silver', 'dim_stock_item', 'si', 'si.stock_item_key = p.stock_item_key')}
{r.optionalJoin('silver', 'dim_product_category', 'cat', 'cat.product_category_key = p.product_category_key')}
LEFT JOIN {r.table('gold', 'agg_product_performance')} AS prev
    ON prev.stock_item_key = p.stock_item_key AND prev.region_code = p.region_code
   AND prev.calendar_month = add_months(p.calendar_month, -1)
"""


def salesByTerritoryMonthSql(r: SqlResolver) -> str:
    return f"""
CREATE OR REPLACE VIEW {r.table('gold', RPT_SALES_BY_TERRITORY_MONTH)} AS
SELECT
    r.calendar_month                                     AS calendar_month,
    r.fiscal_year                                        AS fiscal_year,
    r.fiscal_period                                      AS fiscal_period,
    r.fiscal_calendar_code                               AS fiscal_calendar,
    r.region_code                                        AS region,
    t.sales_territory                                    AS territory,
    ch.sales_channel                                     AS channel,
    r.local_currency_code                                AS local_currency,
    r.order_count                                        AS orders,
    r.invoice_count                                      AS invoices,
    r.active_customer_count                              AS active_customers,
    r.active_salesperson_count                           AS active_salespeople,
    r.net_sales_local                                    AS net_sales_local,
    r.net_sales_daily_rate                               AS net_sales,
    r.net_sales_monthly_average_rate                     AS net_sales_at_average_rate,
    r.translation_difference                             AS translation_difference,
    r.gross_margin_reporting                             AS gross_margin,
    r.margin_percent                                     AS margin_pct,
    r.sales_tax_collected                                AS sales_tax_collected,
    r.vat_output_amount                                  AS vat_output,
    r.vat_reverse_charge_amount                          AS vat_reverse_charge,
    r.gst_collected                                      AS gst_collected,
    r.gst_free_sales                                     AS gst_free_sales,
    r.sales_tax_collected + r.vat_output_amount + r.gst_collected AS indirect_tax_collected,
    r.budget_net_sales_reporting                         AS budget,
    r.budget_variance_reporting                          AS budget_variance,
    r.budget_attainment_percent                          AS budget_attainment_pct,
    CASE WHEN r.budget_attainment_percent IS NULL THEN 'No Budget'
         WHEN r.budget_attainment_percent >= 100 THEN 'On Or Above'
         WHEN r.budget_attainment_percent >= 90 THEN 'Within 10%'
         ELSE 'Below' END                                AS budget_status,
    r.prior_year_net_sales                               AS prior_year_net_sales,
    r.year_over_year_percent                             AS year_on_year_pct,
    r.year_to_date_net_sales                             AS year_to_date_net_sales,
    r.rank_in_region_by_sales                            AS rank_in_region,
    r.refreshed_datetime                                 AS data_as_of
FROM {r.table('gold', 'agg_regional_sales_performance')} AS r
LEFT JOIN {r.table('silver', 'dim_sales_territory')} AS t ON t.sales_territory_key = r.sales_territory_key
LEFT JOIN {r.table('silver', 'dim_sales_channel')} AS ch ON ch.sales_channel_key = r.sales_channel_key
"""


def orderToCashCycleSql(r: SqlResolver) -> str:
    fact = r.table("gold", "fact_order_fulfilment")
    return f"""
CREATE OR REPLACE VIEW {r.table('gold', RPT_ORDER_TO_CASH_CYCLE)} AS
WITH cohort AS (
    -- median completed cycle in the same region over the trailing six months of order dates
    SELECT f.order_fulfilment_key, percentile(CAST(c.order_to_cash_cycle_days AS DOUBLE), 0.5) AS median_cycle_days
    FROM {fact} AS f
    JOIN {fact} AS c
      ON c.region_code = f.region_code AND c.cycle_complete_flag
     AND c.order_date_key >= add_months(f.order_date_key, -6) AND c.order_date_key <= f.order_date_key
    GROUP BY f.order_fulfilment_key
)
SELECT
    f.order_number                                       AS order_number,
    f.order_line_number                                  AS order_line,
    f.invoice_number                                     AS invoice_number,
    f.despatch_note_number                               AS despatch_note,
    f.region_code                                        AS region,
    cust.customer                                        AS customer,
    terr.sales_territory                                 AS territory,
    {r.optionalCol('silver', 'dim_warehouse_site', 'site', 'warehouse_site')} AS warehouse_site,
    f.order_date_key                                     AS order_date,
    f.allocation_date_key                                AS allocated,
    f.pick_date_key                                      AS picked,
    f.pack_date_key                                      AS packed,
    f.despatch_date_key                                  AS despatched,
    f.delivery_date_key                                  AS delivered,
    f.invoice_date_key                                   AS invoiced,
    f.cash_applied_date_key                              AS cash_applied,
    f.order_to_pick_lag_days                             AS order_to_pick_days,
    f.pick_to_despatch_lag_days                          AS pick_to_despatch_days,
    f.despatch_to_delivery_lag_days                      AS despatch_to_delivery_days,
    f.delivery_to_invoice_lag_days                       AS delivery_to_invoice_days,
    f.invoice_to_cash_lag_days                           AS invoice_to_cash_days,
    f.order_to_cash_cycle_days                           AS order_to_cash_days,
    f.service_target_days                                AS service_target_days,
    f.quantity_ordered                                   AS quantity_ordered,
    f.quantity_despatched                                AS quantity_despatched,
    f.quantity_invoiced                                  AS quantity_invoiced,
    f.order_value_reporting                              AS order_value,
    f.invoiced_value_reporting                           AS invoiced_value,
    f.cash_applied_reporting                             AS cash_applied_value,
    f.order_value_reporting - COALESCE(f.cash_applied_reporting, 0) AS value_still_in_pipeline,
    f.pipeline_status_code                               AS pipeline_status,
    f.open_milestone_count                               AS open_milestones,
    f.pick_sla_breach_flag                               AS pick_sla_breach,
    f.delivery_sla_breach_flag                           AS delivery_sla_breach,
    f.perfect_order_flag                                 AS perfect_order_flag,
    f.cancelled_flag                                     AS cancelled_flag,
    f.cycle_complete_flag                                AS cycle_complete_flag,
    cohort.median_cycle_days                             AS cohort_median_cycle_days,
    f.order_to_cash_cycle_days - cohort.median_cycle_days AS variance_to_cohort_median,
    CASE WHEN NOT COALESCE(f.cycle_complete_flag, false) THEN 'In Flight'
         WHEN f.order_to_cash_cycle_days <= f.service_target_days THEN 'Within Target'
         WHEN f.order_to_cash_cycle_days <= f.service_target_days + 10 THEN 'Marginal'
         ELSE 'Breached' END                             AS cycle_assessment,
    f.last_milestone_update                              AS last_milestone_update
FROM {fact} AS f
LEFT JOIN {r.table('silver', 'dim_customer')} AS cust ON cust.customer_key = f.customer_key AND cust.is_current_row
LEFT JOIN {r.table('silver', 'dim_sales_territory')} AS terr ON terr.sales_territory_key = f.sales_territory_key
{r.optionalJoin('silver', 'dim_warehouse_site', 'site', 'site.warehouse_site_key = f.warehouse_site_key')}
LEFT JOIN cohort ON cohort.order_fulfilment_key = f.order_fulfilment_key
"""


def marginByProductCategorySql(r: SqlResolver) -> str:
    return f"""
CREATE OR REPLACE VIEW {r.table('gold', RPT_MARGIN_BY_PRODUCT_CATEGORY)} AS
SELECT
    m.calendar_month                                     AS calendar_month,
    m.fiscal_year                                        AS fiscal_year,
    m.fiscal_period                                      AS fiscal_period,
    m.region_code                                        AS region,
    {r.optionalCol('silver', 'dim_product_category', 'cat', 'product_category')} AS category,
    t.sales_territory                                    AS territory,
    m.cost_basis_code                                    AS cost_basis,
    m.quantity_sold_base_uom                             AS units,
    m.net_revenue_reporting                              AS net_revenue,
    m.cost_of_sales_reporting                            AS cost_of_sales,
    m.standard_cost_reporting                            AS standard_cost,
    m.purchase_price_variance                            AS purchase_price_variance,
    m.freight_cost_reporting                             AS freight,
    m.rebate_accrual_reporting                           AS rebate_accrual,
    m.gross_margin_reporting                             AS gross_margin,
    m.contribution_margin_reporting                      AS contribution_margin,
    m.margin_percent                                     AS margin_pct,
    CASE WHEN m.cost_basis_code = 'STD' THEN m.standard_margin_percent ELSE NULL END AS standard_margin_pct,
    m.prior_period_margin_percent                        AS prior_period_margin_pct,
    ROUND(m.margin_percent - COALESCE(m.prior_period_margin_percent, m.margin_percent), 2) AS margin_point_movement,
    m.price_effect_amount                                AS price_effect,
    m.volume_effect_amount                               AS volume_effect,
    m.mix_effect_amount                                  AS mix_effect,
    m.cost_effect_amount                                 AS cost_effect,
    ROUND(m.gross_margin_reporting - COALESCE(m.price_effect_amount, 0) - COALESCE(m.volume_effect_amount, 0)
          - COALESCE(m.mix_effect_amount, 0) - COALESCE(m.cost_effect_amount, 0), 2) AS bridge_residual,
    m.negative_margin_line_count                         AS negative_margin_lines,
    CASE WHEN m.margin_percent < 0 THEN 'Loss Making'
         WHEN m.margin_percent < 15 THEN 'Thin'
         WHEN m.margin_percent < 35 THEN 'Normal'
         ELSE 'Strong' END                               AS margin_band,
    m.refreshed_datetime                                 AS data_as_of
FROM {r.table('gold', 'agg_monthly_margin_analysis')} AS m
{r.optionalJoin('silver', 'dim_product_category', 'cat', 'cat.product_category_key = m.product_category_key')}
LEFT JOIN {r.table('silver', 'dim_sales_territory')} AS t ON t.sales_territory_key = m.sales_territory_key
"""


def customer360Sql(r: SqlResolver) -> str:
    return f"""
CREATE OR REPLACE VIEW {r.table('gold', RPT_CUSTOMER_360)} AS
SELECT
    c.customer_key                                       AS customer_key,
    CASE WHEN c.anonymised_flag THEN '(anonymised)' ELSE c.customer_name END AS customer,
    CASE WHEN c.anonymised_flag OR NOT c.marketing_consent_flag THEN NULL ELSE c.primary_contact_email END
                                                         AS contact_email,
    c.region_code                                        AS region,
    {r.optionalCol('silver', 'dim_customer_segment', 'seg', 'customer_segment')} AS segment,
    {r.optionalCol('silver', 'dim_loyalty_tier', 'tier', 'loyalty_tier')}        AS loyalty_tier,
    c.tenure_months                                      AS tenure_months,
    c.first_order_date                                   AS first_order,
    c.last_order_date                                    AS last_order,
    c.days_since_last_order                              AS days_since_last_order,
    c.lifetime_order_count                               AS lifetime_orders,
    c.lifetime_net_revenue                               AS lifetime_revenue,
    c.lifetime_gross_margin                              AS lifetime_margin,
    CASE WHEN COALESCE(c.lifetime_net_revenue, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * c.lifetime_gross_margin / c.lifetime_net_revenue, 2) END AS lifetime_margin_pct,
    c.lifetime_returns_amount                            AS lifetime_returns,
    c.average_order_value                                AS average_order_value,
    c.average_days_to_pay                                AS average_days_to_pay,
    c.current_balance_reporting                          AS current_balance,
    c.overdue_balance_reporting                          AS overdue_balance,
    c.credit_limit_reporting                             AS credit_limit,
    c.credit_utilisation_percent                         AS credit_utilisation_pct,
    c.loyalty_point_balance                              AS loyalty_points,
    c.web_session_count_90_day                           AS web_sessions_90_day,
    c.rfm_score                                          AS rfm_score,
    c.churn_risk_score                                   AS churn_risk_score,
    c.churn_risk_band                                    AS churn_risk_band,
    roll.rolling_12_month_revenue                        AS rolling_12_month_revenue,
    roll.rolling_12_month_margin                         AS rolling_12_month_margin,
    roll.rolling_3_month_revenue                         AS rolling_3_month_revenue,
    roll.revenue_trend_percent                           AS revenue_trend_pct,
    roll.consecutive_inactive_months                     AS consecutive_inactive_months,
    RANK() OVER (PARTITION BY c.region_code ORDER BY c.lifetime_net_revenue DESC) AS rank_by_lifetime_revenue,
    NTILE(10) OVER (PARTITION BY c.region_code ORDER BY roll.rolling_12_month_revenue DESC) AS revenue_decile,
    c.marketing_consent_flag                             AS marketing_consent_flag,
    c.retention_expiry_date                              AS retention_expiry,
    c.anonymised_flag                                    AS anonymised_flag,
    c.refreshed_datetime                                 AS data_as_of
FROM {r.table('gold', 'agg_customer_360')} AS c
LEFT JOIN {r.table('gold', 'agg_customer_rolling_12_month')} AS roll
    ON roll.customer_key = c.customer_key AND roll.month_offset = 0
{r.optionalJoin('silver', 'dim_customer_segment', 'seg', 'seg.customer_segment_key = c.customer_segment_key')}
{r.optionalJoin('silver', 'dim_loyalty_tier', 'tier', 'tier.loyalty_tier_key = c.loyalty_tier_key')}
"""


def customerChurnRiskSql(r: SqlResolver) -> str:
    roll = r.table("gold", "agg_customer_rolling_12_month")
    joins = "\n".join(
        f"LEFT JOIN {roll} AS m{n} ON m{n}.customer_key = c.customer_key AND m{n}.month_offset = {n}"
        for n in (0, 1, 2, 3, 6, 12)
    )
    return f"""
CREATE OR REPLACE VIEW {r.table('gold', RPT_CUSTOMER_CHURN_RISK)} AS
SELECT
    c.customer_key                                       AS customer_key,
    CASE WHEN c.anonymised_flag THEN '(anonymised)' ELSE c.customer_name END AS customer,
    c.region_code                                        AS region,
    c.churn_risk_score                                   AS churn_risk_score,
    c.churn_risk_band                                    AS churn_risk_band,
    c.rfm_score                                          AS rfm_score,
    c.days_since_last_order                              AS days_since_last_order,
    c.tenure_months                                      AS tenure_months,
    c.lifetime_net_revenue                               AS lifetime_revenue,
    c.average_order_value                                AS average_order_value,
    c.loyalty_point_balance                              AS loyalty_points_at_risk,
    c.overdue_balance_reporting                          AS overdue_balance,
    m0.net_revenue_reporting                             AS revenue_current_month,
    m1.net_revenue_reporting                             AS revenue_month_minus_1,
    m2.net_revenue_reporting                             AS revenue_month_minus_2,
    m3.net_revenue_reporting                             AS revenue_month_minus_3,
    m6.net_revenue_reporting                             AS revenue_month_minus_6,
    m12.net_revenue_reporting                            AS revenue_month_minus_12,
    m0.rolling_12_month_revenue                          AS rolling_12_month_revenue,
    m0.rolling_3_month_revenue                           AS rolling_3_month_revenue,
    m0.revenue_trend_percent                             AS revenue_trend_pct,
    m0.consecutive_inactive_months                       AS consecutive_inactive_months,
    CASE c.region_code
        WHEN 'NA' THEN CASE WHEN NOT c.marketing_consent_flag THEN 0 ELSE 1 END
        WHEN 'EU' THEN CASE WHEN c.anonymised_flag THEN 0
                            WHEN NOT c.marketing_consent_flag THEN 0
                            WHEN c.retention_expiry_date < current_date() THEN 0
                            ELSE 1 END
        ELSE CASE WHEN NOT c.marketing_consent_flag THEN 0
                  WHEN c.retention_expiry_date < date_add(current_date(), 30) THEN 0
                  ELSE 1 END
    END                                                  AS contactable_flag,
    CASE WHEN c.churn_risk_score >= 80 AND c.lifetime_net_revenue >= 100000 THEN 'P1 - High Value At Risk'
         WHEN c.churn_risk_score >= 80 THEN 'P2 - At Risk'
         WHEN c.churn_risk_score >= 55 THEN 'P3 - Watch'
         ELSE 'P4 - Monitor Only' END                    AS worklist_priority,
    c.refreshed_datetime                                 AS data_as_of
FROM {r.table('gold', 'agg_customer_360')} AS c
{joins}
WHERE c.churn_risk_score >= 40
"""


VIEW_BUILDERS = (
    dailySalesTrendSql,
    salesByCustomerMonthSql,
    salesByProductMonthSql,
    salesByTerritoryMonthSql,
    orderToCashCycleSql,
    marginByProductCategorySql,
    customer360Sql,
    customerChurnRiskSql,
)


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    resolver = SqlResolver(spark, cfg)
    for builder in VIEW_BUILDERS:
        spark.sql(builder(resolver))
