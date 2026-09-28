"""``gold.rpt_*`` view definitions mirroring ``sqlserver/views/Report.*.sql``.

Legacy report column aliases (``[Margin %]``, ``[Net Sales (Reporting
Currency)]``) are snake_cased (``margin_percent``, ``net_sales_reporting_currency``)
because ``%``, ``(`` and ``)`` are not valid Unity Catalog column characters.
Every view keeps the legacy row grain, joins and derived expressions.
"""
from __future__ import annotations

import re

from typing import Callable, Dict, List

Tables = Dict[str, str]

# Legacy Report.* name -> gold.rpt_* name (naming contract: snake_case, vw_ prefix dropped)
REPORT_VIEWS: Dict[str, str] = {
    "Report.vw_DailySalesTrend": "rpt_daily_sales_trend",
    "Report.vw_InventoryHealthCurrent": "rpt_inventory_health_current",
    "Report.vw_SalesByCustomerMonth": "rpt_sales_by_customer_month",
    "Report.vw_SalesByProductMonth": "rpt_sales_by_product_month",
    "Report.vw_SalesByTerritoryMonth": "rpt_sales_by_territory_month",
    "Report.vw_MarginByProductCategory": "rpt_margin_by_product_category",
    "Report.vw_Customer360": "rpt_customer_360",
    "Report.vw_CustomerChurnRisk": "rpt_customer_churn_risk",
    "Report.vw_ReturnsRateByCategory": "rpt_returns_rate_by_category",
    "Report.vw_PromotionRoi": "rpt_promotion_roi",
    "Report.vw_SupplierSpendYtd": "rpt_supplier_spend_ytd",
    "Report.vw_SupplierOnTimeDelivery": "rpt_supplier_on_time_delivery",
    "Report.vw_FinanceCloseStatus": "rpt_finance_close_status",
    "Report.vw_ApAgingCurrent": "rpt_ap_aging_current",
    "Report.vw_LoyaltyProgramSummary": "rpt_loyalty_program_summary",
    "Report.vw_OrderToCashCycle": "rpt_order_to_cash_cycle",
}

# Aggregates each report depends on (legacy object names) - drives the staleness gate.
REPORT_SOURCES: Dict[str, List[str]] = {
    "rpt_daily_sales_trend": ["Aggregate.Daily Sales Summary"],
    "rpt_inventory_health_current": ["Aggregate.Daily Inventory Health"],
    "rpt_sales_by_customer_month": ["Aggregate.Monthly Sales Summary"],
    "rpt_sales_by_product_month": ["Aggregate.Product Performance"],
    "rpt_sales_by_territory_month": ["Aggregate.Regional Sales Performance"],
    "rpt_margin_by_product_category": ["Aggregate.Monthly Margin Analysis"],
    "rpt_customer_360": ["Aggregate.Customer 360", "Aggregate.Customer Rolling 12 Month"],
    "rpt_customer_churn_risk": ["Aggregate.Customer 360", "Aggregate.Customer Rolling 12 Month"],
    "rpt_returns_rate_by_category": ["Aggregate.Product Performance"],
    "rpt_promotion_roi": ["Aggregate.Promotion Effectiveness"],
    "rpt_supplier_spend_ytd": ["Aggregate.Supplier Performance"],
    "rpt_supplier_on_time_delivery": [],
    "rpt_finance_close_status": ["Aggregate.Finance Close Summary"],
    "rpt_ap_aging_current": [],
    "rpt_loyalty_program_summary": [],
    "rpt_order_to_cash_cycle": [],
}


def _pct(numerator: str, denominator: str) -> str:
    return f"ROUND(100.0 * {numerator} / NULLIF({denominator}, 0), 2)"


def dailySalesTrend(t: Tables) -> str:
    return f"""
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
    FROM {t['agg_daily_sales_summary']} AS d
    GROUP BY d.sales_date, d.region_code, d.sales_territory_key, d.sales_channel_key
)
SELECT
    r.sales_date, DAYOFWEEK(r.sales_date) AS day_of_week, r.fiscal_year, r.fiscal_period,
    r.region_code AS region, terr.sales_territory AS territory, ch.sales_channel AS channel,
    r.invoice_count AS invoices, r.line_count AS lines, r.quantity_sold, r.gross_sales,
    r.line_discount + COALESCE(r.promotion_discount, 0) AS total_discount,
    r.net_sales, r.net_sales_reporting AS net_sales_reporting_currency, r.tax AS indirect_tax, r.freight,
    r.cost_of_sales, r.gross_margin,
    {_pct('r.gross_margin', 'r.net_sales')} AS margin_percent,
    r.returns, r.prior_year_net_sales,
    {_pct('(r.net_sales - r.prior_year_net_sales)', 'r.prior_year_net_sales')} AS prior_year_growth_percent,
    LAG(r.net_sales, 7) OVER (PARTITION BY r.sales_territory_key, r.sales_channel_key ORDER BY r.sales_date)
        AS net_sales_same_day_last_week,
    AVG(r.net_sales) OVER (PARTITION BY r.sales_territory_key, r.sales_channel_key ORDER BY r.sales_date
                           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS seven_day_moving_average,
    SUM(r.net_sales_reporting) OVER (PARTITION BY r.sales_territory_key, r.sales_channel_key, r.fiscal_year
                                     ORDER BY r.sales_date ROWS UNBOUNDED PRECEDING) AS fiscal_year_to_date_revenue,
    RANK() OVER (PARTITION BY r.sales_date, r.region_code ORDER BY r.net_sales_reporting DESC) AS rank_in_region_that_day
FROM rolled AS r
LEFT JOIN {t['dim_sales_territory']} AS terr ON terr.sales_territory_key = r.sales_territory_key
LEFT JOIN {t['dim_sales_channel']} AS ch ON ch.sales_channel_key = r.sales_channel_key
"""


def inventoryHealthCurrent(t: Tables) -> str:
    return f"""
SELECT
    h.snapshot_date, h.region_code AS region, ws.warehouse_site, cat.product_category AS category,
    h.sku_count AS skus, h.sku_stocked_count AS skus_in_stock, h.stockout_sku_count AS skus_out_of_stock,
    h.below_reorder_sku_count AS skus_below_reorder, h.excess_sku_count AS skus_in_excess,
    h.slow_moving_sku_count AS skus_slow_moving, h.quarantined_sku_count AS skus_quarantined,
    h.total_quantity_on_hand AS quantity_on_hand, h.total_stock_value_reporting AS stock_value,
    h.excess_stock_value_reporting AS excess_stock_value, h.obsolescence_provision_amount AS obsolescence_provision,
    CASE WHEN COALESCE(h.total_stock_value_reporting, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * h.obsolescence_provision_amount / h.total_stock_value_reporting, 2) END AS provision_percent_of_stock,
    h.average_days_of_cover AS days_of_cover, h.service_level_percent, h.inventory_turns_annualised AS inventory_turns,
    h.days_inventory_outstanding, h.stockout_rate_percent, lw.stockout_rate_percent AS stockout_rate_percent_last_week,
    ROUND(h.stockout_rate_percent - COALESCE(lw.stockout_rate_percent, h.stockout_rate_percent), 2) AS stockout_rate_movement,
    ROUND(h.total_stock_value_reporting - COALESCE(lw.total_stock_value_reporting, h.total_stock_value_reporting), 2) AS stock_value_movement,
    CASE WHEN h.stockout_rate_percent > 5 THEN 'RED' WHEN h.stockout_rate_percent > 2 THEN 'AMBER' ELSE 'GREEN' END AS availability_rag,
    CASE WHEN h.average_days_of_cover > 180 THEN 'Overstocked' WHEN h.average_days_of_cover < 14 THEN 'Exposed' ELSE 'Balanced' END AS cover_assessment,
    h.intraday_refresh_count AS intraday_refreshes, h.refreshed_datetime AS data_as_of
FROM {t['agg_daily_inventory_health']} AS h
INNER JOIN (SELECT warehouse_site_key, MAX(snapshot_date) AS latest_snapshot
            FROM {t['agg_daily_inventory_health']} GROUP BY warehouse_site_key) AS latest
    ON latest.warehouse_site_key = h.warehouse_site_key AND latest.latest_snapshot = h.snapshot_date
LEFT JOIN {t['agg_daily_inventory_health']} AS lw
    ON lw.warehouse_site_key = h.warehouse_site_key AND lw.product_category_key <=> h.product_category_key
   AND lw.snapshot_date = DATE_SUB(h.snapshot_date, 7)
LEFT JOIN {t['dim_warehouse_site']} AS ws ON ws.warehouse_site_key = h.warehouse_site_key
LEFT JOIN {t['dim_product_category']} AS cat ON cat.product_category_key = h.product_category_key
"""


def salesByCustomerMonth(t: Tables) -> str:
    return f"""
SELECT
    m.customer_key, c.customer AS customer_name, c.category AS customer_category, m.region_code AS region,
    m.fiscal_calendar_code AS fiscal_calendar, m.fiscal_year, m.fiscal_period,
    CASE m.region_code
        WHEN 'NA' THEN CONCAT('FY', CAST(m.fiscal_year AS STRING), ' P', LPAD(CAST(m.fiscal_period AS STRING), 2, '0'))
        WHEN 'EU' THEN CONCAT('FY', CAST(m.fiscal_year AS STRING), '/', RIGHT(CAST(m.fiscal_year + 1 AS STRING), 2),
                              ' P', LPAD(CAST(m.fiscal_period AS STRING), 2, '0'))
        ELSE CONCAT('FY', CAST(m.fiscal_year AS STRING), ' P', LPAD(CAST(m.fiscal_period AS STRING), 2, '0'), ' (13P)')
    END AS fiscal_period_label,
    m.calendar_month, m.order_count AS orders, m.invoice_count AS invoices, m.quantity_sold_base_uom AS units,
    m.gross_revenue, m.discount_given, m.net_revenue AS net_revenue_local, m.net_revenue_reporting AS net_revenue,
    m.credit_notes_reporting AS credit_notes, m.returns_reporting AS returns, m.net_revenue_after_credits,
    m.gross_margin_reporting AS gross_margin,
    CASE WHEN COALESCE(m.net_revenue_reporting, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * m.gross_margin_reporting / m.net_revenue_reporting, 2) END AS margin_percent,
    m.average_order_value, m.prior_period_net_revenue, m.period_over_period_percent AS period_on_period_percent,
    m.prior_year_net_revenue, m.year_over_year_percent AS year_on_year_percent,
    m.rolling_3_period_net_revenue AS rolling_3_period_revenue,
    SUM(m.net_revenue_reporting) OVER (PARTITION BY m.customer_key, m.fiscal_year ORDER BY m.fiscal_period
                                       ROWS UNBOUNDED PRECEDING) AS year_to_date_revenue,
    CASE WHEN m.period_closed_flag THEN 'Closed' ELSE 'Period To Date' END AS period_status,
    m.refreshed_datetime AS data_as_of
FROM {t['agg_monthly_sales_summary']} AS m
LEFT JOIN {t['dim_customer']} AS c ON c.customer_key = m.customer_key
"""


def salesByProductMonth(t: Tables) -> str:
    return f"""
SELECT
    p.calendar_month, p.stock_item_key, si.stock_item AS product, si.brand, si.size, cat.product_category AS category,
    p.region_code AS region, p.units_sold_base_uom AS units_sold, p.net_revenue_reporting AS net_revenue,
    p.gross_margin_reporting AS gross_margin, p.margin_percent, p.discount_depth_percent, p.average_selling_price,
    p.average_unit_cost, p.units_returned, p.return_rate_percent, p.inventory_turns, p.stockout_days,
    p.lost_sales_estimate_reporting AS estimated_lost_sales, p.sell_through_percent,
    p.distinct_customer_count AS buying_customers, p.abc_class, p.xyz_class,
    CONCAT(p.abc_class, '/', COALESCE(p.xyz_class, '?')) AS abc_xyz, p.prior_month_abc_class AS prior_month_abc,
    CASE WHEN p.prior_month_abc_class IS NULL THEN 'New'
         WHEN p.abc_class < p.prior_month_abc_class THEN 'Promoted'
         WHEN p.abc_class > p.prior_month_abc_class THEN 'Demoted' ELSE 'Stable' END AS class_movement,
    p.rank_in_category_by_revenue AS category_revenue_rank, p.rank_in_category_by_margin AS category_margin_rank,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY p.region_code, p.calendar_month ORDER BY p.net_revenue_reporting DESC) <= 500
         THEN 1 ELSE 0 END AS top_500_flag,
    prev.net_revenue_reporting AS prior_month_net_revenue,
    CASE WHEN COALESCE(prev.net_revenue_reporting, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * (p.net_revenue_reporting - prev.net_revenue_reporting) / prev.net_revenue_reporting, 2) END
        AS month_on_month_percent,
    p.new_product_flag, p.discontinued_flag, p.refreshed_datetime AS data_as_of
FROM {t['agg_product_performance']} AS p
LEFT JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = p.stock_item_key
LEFT JOIN {t['dim_product_category']} AS cat ON cat.product_category_key = p.product_category_key
LEFT JOIN {t['agg_product_performance']} AS prev
    ON prev.stock_item_key = p.stock_item_key AND prev.region_code = p.region_code
   AND prev.calendar_month = ADD_MONTHS(p.calendar_month, -1)
"""


def salesByTerritoryMonth(t: Tables) -> str:
    return f"""
SELECT
    r.calendar_month, r.fiscal_year, r.fiscal_period, r.fiscal_calendar_code AS fiscal_calendar, r.region_code AS region,
    tr.sales_territory AS territory, ch.sales_channel AS channel, r.local_currency_code AS local_currency,
    r.order_count AS orders, r.invoice_count AS invoices, r.active_customer_count AS active_customers,
    r.active_salesperson_count AS active_salespeople, r.net_sales_local, r.net_sales_daily_rate AS net_sales,
    r.net_sales_monthly_average_rate AS net_sales_at_average_rate, r.translation_difference,
    r.gross_margin_reporting AS gross_margin, r.margin_percent, r.sales_tax_collected, r.vat_output_amount AS vat_output,
    r.vat_reverse_charge_amount AS vat_reverse_charge, r.gst_collected, r.gst_free_sales,
    r.sales_tax_collected + r.vat_output_amount + r.gst_collected AS indirect_tax_collected,
    r.budget_net_sales_reporting AS budget, r.budget_variance_reporting AS budget_variance, r.budget_attainment_percent,
    CASE WHEN r.budget_attainment_percent IS NULL THEN 'No Budget'
         WHEN r.budget_attainment_percent >= 100 THEN 'On Or Above'
         WHEN r.budget_attainment_percent >= 90 THEN 'Within 10%' ELSE 'Below' END AS budget_status,
    r.prior_year_net_sales, r.year_over_year_percent AS year_on_year_percent, r.year_to_date_net_sales,
    r.rank_in_region_by_sales AS rank_in_region, r.refreshed_datetime AS data_as_of
FROM {t['agg_regional_sales_performance']} AS r
LEFT JOIN {t['dim_sales_territory']} AS tr ON tr.sales_territory_key = r.sales_territory_key
LEFT JOIN {t['dim_sales_channel']} AS ch ON ch.sales_channel_key = r.sales_channel_key
"""


def marginByProductCategory(t: Tables) -> str:
    return f"""
SELECT
    m.calendar_month, m.fiscal_year, m.fiscal_period, m.region_code AS region, cat.product_category AS category,
    tr.sales_territory AS territory, m.cost_basis_code AS cost_basis, m.quantity_sold_base_uom AS units,
    m.net_revenue_reporting AS net_revenue, m.cost_of_sales_reporting AS cost_of_sales,
    m.standard_cost_reporting AS standard_cost, m.purchase_price_variance, m.freight_cost_reporting AS freight,
    m.rebate_accrual_reporting AS rebate_accrual, m.gross_margin_reporting AS gross_margin,
    m.contribution_margin_reporting AS contribution_margin, m.margin_percent,
    CASE WHEN m.cost_basis_code = 'STD' THEN m.standard_margin_percent ELSE NULL END AS standard_margin_percent,
    m.prior_period_margin_percent,
    ROUND(m.margin_percent - COALESCE(m.prior_period_margin_percent, m.margin_percent), 2) AS margin_point_movement,
    m.price_effect_amount AS price_effect, m.volume_effect_amount AS volume_effect, m.mix_effect_amount AS mix_effect,
    m.cost_effect_amount AS cost_effect,
    ROUND(m.gross_margin_reporting - COALESCE(m.price_effect_amount, 0) - COALESCE(m.volume_effect_amount, 0)
          - COALESCE(m.mix_effect_amount, 0) - COALESCE(m.cost_effect_amount, 0), 2) AS bridge_residual,
    m.negative_margin_line_count AS negative_margin_lines,
    CASE WHEN m.margin_percent < 0 THEN 'Loss Making' WHEN m.margin_percent < 15 THEN 'Thin'
         WHEN m.margin_percent < 35 THEN 'Normal' ELSE 'Strong' END AS margin_band,
    m.refreshed_datetime AS data_as_of
FROM {t['agg_monthly_margin_analysis']} AS m
LEFT JOIN {t['dim_product_category']} AS cat ON cat.product_category_key = m.product_category_key
LEFT JOIN {t['dim_sales_territory']} AS tr ON tr.sales_territory_key = m.sales_territory_key
"""


def customer360(t: Tables) -> str:
    return f"""
SELECT
    c.customer_key,
    CASE WHEN c.anonymised_flag THEN '(anonymised)' ELSE c.customer_name END AS customer,
    CASE WHEN c.anonymised_flag OR NOT COALESCE(c.marketing_consent_flag, FALSE) THEN NULL ELSE c.primary_contact_email END AS contact_email,
    c.region_code AS region, seg.customer_segment AS segment, tier.loyalty_tier, c.tenure_months,
    c.first_order_date AS first_order, c.last_order_date AS last_order, c.days_since_last_order,
    c.lifetime_order_count AS lifetime_orders, c.lifetime_net_revenue AS lifetime_revenue,
    c.lifetime_gross_margin AS lifetime_margin,
    CASE WHEN COALESCE(c.lifetime_net_revenue, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * c.lifetime_gross_margin / c.lifetime_net_revenue, 2) END AS lifetime_margin_percent,
    c.lifetime_returns_amount AS lifetime_returns, c.average_order_value, c.average_days_to_pay,
    c.current_balance_reporting AS current_balance, c.overdue_balance_reporting AS overdue_balance,
    c.credit_limit_reporting AS credit_limit, c.credit_utilisation_percent, c.loyalty_point_balance AS loyalty_points,
    c.web_session_count_90_day AS web_sessions_90_day, c.rfm_score, c.churn_risk_score, c.churn_risk_band,
    roll.rolling_12_month_revenue, roll.rolling_12_month_margin, roll.rolling_3_month_revenue,
    roll.revenue_trend_percent, roll.consecutive_inactive_months,
    RANK() OVER (PARTITION BY c.region_code ORDER BY c.lifetime_net_revenue DESC) AS rank_by_lifetime_revenue,
    NTILE(10) OVER (PARTITION BY c.region_code ORDER BY roll.rolling_12_month_revenue DESC) AS revenue_decile,
    c.marketing_consent_flag, c.retention_expiry_date AS retention_expiry, c.anonymised_flag,
    c.refreshed_datetime AS data_as_of
FROM {t['agg_customer_360']} AS c
LEFT JOIN {t['agg_customer_rolling_12_month']} AS roll ON roll.customer_key = c.customer_key AND roll.month_offset = 0
LEFT JOIN {t['dim_customer_segment']} AS seg ON seg.customer_segment_key = c.customer_segment_key
LEFT JOIN {t['dim_loyalty_tier']} AS tier ON tier.loyalty_tier_key = c.loyalty_tier_key
"""


def customerChurnRisk(t: Tables) -> str:
    roll = t['agg_customer_rolling_12_month']
    return f"""
SELECT
    c.customer_key,
    CASE WHEN c.anonymised_flag THEN '(anonymised)' ELSE c.customer_name END AS customer,
    c.region_code AS region, c.churn_risk_score, c.churn_risk_band, c.rfm_score, c.days_since_last_order,
    c.tenure_months, c.lifetime_net_revenue AS lifetime_revenue, c.average_order_value,
    c.loyalty_point_balance AS loyalty_points_at_risk, c.overdue_balance_reporting AS overdue_balance,
    m0.net_revenue_reporting AS revenue_current_month, m1.net_revenue_reporting AS revenue_month_minus_1,
    m2.net_revenue_reporting AS revenue_month_minus_2, m3.net_revenue_reporting AS revenue_month_minus_3,
    m6.net_revenue_reporting AS revenue_month_minus_6, m12.net_revenue_reporting AS revenue_month_minus_12,
    m0.rolling_12_month_revenue, m0.rolling_3_month_revenue, m0.revenue_trend_percent, m0.consecutive_inactive_months,
    CASE c.region_code
        WHEN 'NA' THEN CASE WHEN NOT COALESCE(c.marketing_consent_flag, FALSE) THEN 0 ELSE 1 END
        WHEN 'EU' THEN CASE WHEN c.anonymised_flag THEN 0
                            WHEN NOT COALESCE(c.marketing_consent_flag, FALSE) THEN 0
                            WHEN c.retention_expiry_date < CURRENT_DATE() THEN 0 ELSE 1 END
        ELSE CASE WHEN NOT COALESCE(c.marketing_consent_flag, FALSE) THEN 0
                  WHEN c.retention_expiry_date < DATE_ADD(CURRENT_DATE(), 30) THEN 0 ELSE 1 END
    END AS contactable_flag,
    CASE WHEN c.churn_risk_score >= 80 AND c.lifetime_net_revenue >= 100000 THEN 'P1 - High Value At Risk'
         WHEN c.churn_risk_score >= 80 THEN 'P2 - At Risk'
         WHEN c.churn_risk_score >= 55 THEN 'P3 - Watch' ELSE 'P4 - Monitor Only' END AS worklist_priority,
    c.refreshed_datetime AS data_as_of
FROM {t['agg_customer_360']} AS c
LEFT JOIN {roll} AS m0 ON m0.customer_key = c.customer_key AND m0.month_offset = 0
LEFT JOIN {roll} AS m1 ON m1.customer_key = c.customer_key AND m1.month_offset = 1
LEFT JOIN {roll} AS m2 ON m2.customer_key = c.customer_key AND m2.month_offset = 2
LEFT JOIN {roll} AS m3 ON m3.customer_key = c.customer_key AND m3.month_offset = 3
LEFT JOIN {roll} AS m6 ON m6.customer_key = c.customer_key AND m6.month_offset = 6
LEFT JOIN {roll} AS m12 ON m12.customer_key = c.customer_key AND m12.month_offset = 12
WHERE c.churn_risk_score >= 40
"""


def returnsRateByCategory(t: Tables) -> str:
    rate = "ROUND(100.0 * rb.credit_value / NULLIF(sb.net_revenue, 0), 2)"
    return f"""
WITH returns_by_month AS (
    SELECT TRUNC(r.return_date_key, 'MM') AS calendar_month, r.region_code, si.product_category_key,
           COUNT(*) AS return_line_count, COUNT(DISTINCT r.rma_number) AS rma_count,
           SUM(r.quantity_returned) AS quantity_returned, SUM(r.quantity_restocked) AS quantity_restocked,
           SUM(r.quantity_scrapped) AS quantity_scrapped, SUM(r.net_credit_amount_reporting) AS credit_value,
           SUM(r.restocking_fee_amount) AS restocking_fees, SUM(r.margin_reversed) AS margin_reversed,
           SUM(CASE WHEN r.faulty_goods_flag THEN r.net_credit_amount_reporting ELSE 0 END) AS faulty_goods_credit,
           SUM(CASE WHEN NOT COALESCE(r.within_statutory_window_flag, TRUE) THEN r.net_credit_amount_reporting ELSE 0 END) AS out_of_window_credit,
           AVG(CAST(r.days_since_invoice AS DECIMAL(9, 2))) AS average_days_to_return
    FROM {t['fact_return']} AS r
    INNER JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = r.stock_item_key
    GROUP BY TRUNC(r.return_date_key, 'MM'), r.region_code, si.product_category_key
),
sales_by_month AS (
    SELECT m.calendar_month, m.region_code, m.product_category_key,
           SUM(m.net_revenue_reporting) AS net_revenue, SUM(m.units_sold_base_uom) AS quantity_sold
    FROM {t['agg_product_performance']} AS m
    GROUP BY m.calendar_month, m.region_code, m.product_category_key
)
SELECT
    rb.calendar_month, rb.region_code AS region, cat.product_category AS category, rb.return_line_count AS return_lines,
    rb.rma_count, rb.quantity_returned, rb.quantity_restocked, rb.quantity_scrapped, rb.credit_value, rb.restocking_fees,
    rb.margin_reversed, rb.faulty_goods_credit, rb.out_of_window_credit, rb.average_days_to_return,
    sb.net_revenue, sb.quantity_sold,
    {rate} AS returns_rate_percent_value,
    ROUND(100.0 * rb.quantity_returned / NULLIF(sb.quantity_sold, 0), 2) AS returns_rate_percent_units,
    LAG({rate}, 1) OVER (PARTITION BY rb.region_code, rb.product_category_key ORDER BY rb.calendar_month) AS prior_month_returns_rate_percent,
    LAG({rate}, 12) OVER (PARTITION BY rb.region_code, rb.product_category_key ORDER BY rb.calendar_month) AS prior_year_returns_rate_percent,
    ROUND(AVG(100.0 * rb.credit_value / NULLIF(sb.net_revenue, 0)) OVER
          (PARTITION BY rb.region_code, rb.product_category_key ORDER BY rb.calendar_month
           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS three_month_average_rate_percent,
    ROUND(100.0 * rb.quantity_scrapped / NULLIF(rb.quantity_returned, 0), 2) AS scrap_rate_percent,
    RANK() OVER (PARTITION BY rb.calendar_month, rb.region_code ORDER BY rb.credit_value DESC) AS rank_by_credit_value
FROM returns_by_month AS rb
LEFT JOIN sales_by_month AS sb
    ON sb.calendar_month = rb.calendar_month AND sb.region_code = rb.region_code
   AND sb.product_category_key <=> rb.product_category_key
LEFT JOIN {t['dim_product_category']} AS cat ON cat.product_category_key = rb.product_category_key
"""


def promotionRoi(t: Tables) -> str:
    return f"""
SELECT
    p.promotion_code, promo.promotion_name AS promotion, p.region_code AS region, cat.product_category AS category,
    ch.sales_channel AS channel, p.promotion_start_date AS start_date, p.promotion_end_date AS end_date,
    DATEDIFF(p.promotion_end_date, p.promotion_start_date) + 1 AS promotion_days,
    p.baseline_start_date AS baseline_start, p.baseline_end_date AS baseline_end,
    p.eligible_customer_count AS eligible_customers, p.participating_customer_count AS participating_customers,
    p.eligible_not_purchased_count AS eligible_not_purchased, p.take_up_rate_percent AS take_up_percent,
    p.new_customer_count AS new_customers, p.reactivated_customer_count AS reactivated_customers,
    p.promoted_units_sold AS promoted_units, p.baseline_revenue_reporting AS baseline_revenue,
    p.promotion_revenue_reporting AS promotion_revenue, p.incremental_revenue_reporting AS incremental_revenue,
    p.cannibalised_revenue, p.discount_cost_reporting AS discount_cost, p.baseline_margin_reporting AS baseline_margin,
    p.promotion_margin_reporting AS promotion_margin, p.incremental_margin_reporting AS incremental_margin,
    p.loyalty_points_issued, p.return_rate_percent, p.roi_percent AS roi_percent_incremental_margin,
    CASE WHEN COALESCE(p.discount_cost_reporting, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * p.promotion_revenue_reporting / p.discount_cost_reporting, 2) END AS roi_percent_gross_2012_basis,
    CASE WHEN COALESCE(p.participating_customer_count, 0) = 0 THEN NULL
         ELSE ROUND(p.discount_cost_reporting / p.participating_customer_count, 2) END AS cost_per_participant,
    p.payback_achieved_flag, p.consent_restricted_flag,
    RANK() OVER (PARTITION BY p.region_code ORDER BY p.incremental_margin_reporting DESC) AS rank_by_incremental_margin,
    RANK() OVER (PARTITION BY p.region_code ORDER BY p.roi_percent DESC) AS rank_by_roi,
    CASE WHEN p.incremental_margin_reporting < 0 THEN 'Value Destroying'
         WHEN p.payback_achieved_flag THEN 'Repeatable' ELSE 'Review Before Repeat' END AS recommendation,
    p.refreshed_datetime AS data_as_of
FROM {t['agg_promotion_effectiveness']} AS p
LEFT JOIN {t['dim_promotion']} AS promo ON promo.promotion_key = p.promotion_key
LEFT JOIN {t['dim_product_category']} AS cat ON cat.product_category_key = p.product_category_key
LEFT JOIN {t['dim_sales_channel']} AS ch ON ch.sales_channel_key = p.sales_channel_key
"""


def supplierSpendYtd(t: Tables) -> str:
    spendYear = ("CASE sp.region_code WHEN 'EU' THEN CASE WHEN MONTH(sp.calendar_month) >= 4 THEN YEAR(sp.calendar_month) "
                 "ELSE YEAR(sp.calendar_month) - 1 END ELSE YEAR(sp.calendar_month) END")
    return f"""
WITH ytd AS (
    SELECT sp.supplier_key, sp.region_code, {spendYear} AS spend_year,
           SUM(sp.committed_spend_reporting) AS committed_spend, SUM(sp.recognised_spend_reporting) AS recognised_spend,
           SUM(sp.contract_covered_spend) AS contract_covered_spend, SUM(sp.maverick_spend_reporting) AS maverick_spend,
           SUM(sp.landed_cost_reporting) AS landed_cost, SUM(sp.freight_in_reporting) AS freight_in,
           SUM(sp.customs_duty_reporting) AS customs_duty, SUM(sp.price_variance_reporting) AS price_variance,
           SUM(sp.discount_captured_reporting) AS discount_captured, SUM(sp.discount_lost_reporting) AS discount_lost,
           SUM(sp.purchase_order_count) AS purchase_orders, SUM(sp.match_exception_count) AS match_exceptions,
           MAX(sp.landed_cost_basis_code) AS landed_cost_basis, MAX(sp.calendar_month) AS latest_month,
           MAX(sp.refreshed_datetime) AS refreshed_datetime
    FROM {t['agg_supplier_performance']} AS sp
    GROUP BY sp.supplier_key, sp.region_code, {spendYear}
)
SELECT
    ytd.spend_year, ytd.region_code AS region,
    CASE ytd.region_code WHEN 'EU' THEN 'April Year' ELSE 'Calendar Year' END AS year_basis,
    ytd.supplier_key, s.supplier, s.category AS supplier_category, s.payment_days AS payment_terms_days,
    ytd.committed_spend AS committed_spend_ytd, ytd.recognised_spend AS recognised_spend_ytd,
    ytd.landed_cost AS landed_cost_ytd, ytd.landed_cost_basis, ytd.freight_in AS freight_in_ytd,
    ytd.customs_duty AS customs_duty_ytd, ytd.contract_covered_spend, ytd.maverick_spend,
    CASE WHEN COALESCE(ytd.committed_spend, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * ytd.contract_covered_spend / ytd.committed_spend, 2) END AS contract_coverage_percent,
    ytd.price_variance AS purchase_price_variance, ytd.discount_captured AS early_settlement_captured,
    ytd.discount_lost AS early_settlement_lost, ytd.purchase_orders, ytd.match_exceptions,
    RANK() OVER (PARTITION BY ytd.region_code, ytd.spend_year ORDER BY ytd.recognised_spend DESC) AS spend_rank,
    SUM(ytd.recognised_spend) OVER (PARTITION BY ytd.region_code, ytd.spend_year ORDER BY ytd.recognised_spend DESC
                                    ROWS UNBOUNDED PRECEDING) AS cumulative_spend,
    ROUND(100.0 * SUM(ytd.recognised_spend) OVER (PARTITION BY ytd.region_code, ytd.spend_year ORDER BY ytd.recognised_spend DESC
                                                  ROWS UNBOUNDED PRECEDING)
          / NULLIF(SUM(ytd.recognised_spend) OVER (PARTITION BY ytd.region_code, ytd.spend_year), 0), 2) AS cumulative_spend_percent,
    ytd.latest_month AS latest_month_included, ytd.refreshed_datetime AS data_as_of
FROM ytd
LEFT JOIN {t['dim_supplier']} AS s ON s.supplier_key = ytd.supplier_key
"""


def supplierOnTimeDelivery(t: Tables) -> str:
    target = "CASE r.region_code WHEN 'NA' THEN 95.0 WHEN 'EU' THEN 97.0 ELSE 90.0 END"
    return f"""
WITH receipt_days AS (
    SELECT DISTINCT supplier_key, receipt_date_key FROM {t['fact_purchase_receipt']}
),
roll AS (
    -- OUTER APPLY equivalent: the rolling window depends only on supplier and receipt date
    SELECT d.supplier_key, d.receipt_date_key,
           COUNT(*) AS receipt_count_3m,
           SUM(CASE WHEN h.on_time_flag AND h.in_full_flag THEN 1 ELSE 0 END) AS otif_count_3m,
           AVG(CAST(h.lead_time_days AS DECIMAL(18, 2))) AS average_lead_time_3m
    FROM receipt_days AS d
    INNER JOIN {t['fact_purchase_receipt']} AS h
        ON h.supplier_key = d.supplier_key AND h.receipt_date_key <= d.receipt_date_key
       AND h.receipt_date_key > ADD_MONTHS(d.receipt_date_key, -3)
    GROUP BY d.supplier_key, d.receipt_date_key
)
SELECT
    r.receipt_date_key AS receipt_date, r.region_code AS region, r.supplier_key, s.supplier,
    r.purchase_order_number AS po_number, r.receipt_number, r.quantity_ordered_base_uom AS ordered_quantity,
    r.quantity_received_base_uom AS received_quantity, r.quantity_rejected_base_uom AS rejected_quantity,
    r.receipt_value_reporting AS received_value, r.lead_time_days, r.days_late_versus_promise AS days_late,
    r.on_time_flag, r.in_full_flag,
    CASE WHEN r.on_time_flag AND r.in_full_flag THEN 1 ELSE 0 END AS otif_flag,
    CASE WHEN r.days_late_versus_promise <= 0 THEN 'On Or Early' WHEN r.days_late_versus_promise <= 3 THEN '1-3 Days Late'
         WHEN r.days_late_versus_promise <= 7 THEN '4-7 Days Late' WHEN r.days_late_versus_promise <= 30 THEN '8-30 Days Late'
         ELSE 'Over 30 Days Late' END AS lateness_band,
    roll.receipt_count_3m AS receipts_last_3_months, roll.otif_count_3m AS otif_last_3_months,
    CASE WHEN COALESCE(roll.receipt_count_3m, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * roll.otif_count_3m / roll.receipt_count_3m, 2) END AS rolling_3_month_otif_percent,
    roll.average_lead_time_3m AS rolling_3_month_lead_time,
    {target} AS regional_otif_target_percent,
    CASE WHEN COALESCE(roll.receipt_count_3m, 0) = 0 THEN NULL
         WHEN 100.0 * roll.otif_count_3m / roll.receipt_count_3m < {target} THEN 1 ELSE 0 END AS below_target_flag,
    r.batch_id
FROM {t['fact_purchase_receipt']} AS r
LEFT JOIN {t['dim_supplier']} AS s ON s.supplier_key = r.supplier_key
LEFT JOIN roll ON roll.supplier_key = r.supplier_key AND roll.receipt_date_key = r.receipt_date_key
"""


def financeCloseStatus(t: Tables) -> str:
    w = "PARTITION BY fc.legal_entity_code, fc.account_group_code ORDER BY (fc.fiscal_year * 100) + fc.fiscal_period"
    return f"""
SELECT
    fc.fiscal_year, fc.fiscal_period, fc.legal_entity_code AS legal_entity, fc.region_code AS region,
    fc.account_group_code AS account_group, fc.ledger_currency_code AS ledger_currency, fc.period_end_date AS period_end,
    (fc.fiscal_year * 100) + fc.fiscal_period AS close_sequence,
    CASE fc.region_code WHEN 'NA' THEN 'January-December' WHEN 'EU' THEN 'April-March' ELSE 'July-June' END AS fiscal_calendar,
    fc.opening_balance_local, fc.period_debits_local, fc.period_credits_local, fc.closing_balance_local,
    fc.consolidation_rate, fc.closing_balance_reporting AS closing_balance, fc.sub_ledger_balance_reporting AS sub_ledger_balance,
    fc.sub_ledger_to_gl_difference, ABS(COALESCE(fc.sub_ledger_to_gl_difference, 0)) AS absolute_difference,
    fc.tolerance_amount AS tolerance, fc.within_tolerance_flag,
    ABS(COALESCE(fc.sub_ledger_to_gl_difference, 0)) - COALESCE(fc.tolerance_amount, 0) AS amount_over_tolerance,
    fc.manual_journal_count AS manual_journals, fc.manual_journal_value, fc.late_posting_count AS late_postings,
    fc.unposted_journal_count AS unposted_journals, fc.ar_balance_reporting AS ar_balance, fc.ap_balance_reporting AS ap_balance,
    fc.grni_accrual_reporting AS grni_accrual, fc.bad_debt_provision_reporting AS bad_debt_provision,
    fc.close_status_code AS close_status, fc.close_completed_datetime AS close_completed, fc.days_to_close,
    AVG(CAST(fc.days_to_close AS DECIMAL(9, 2))) OVER ({w} ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_12_period_days_to_close,
    LAG(fc.closing_balance_reporting, 1) OVER ({w}) AS prior_period_closing_balance,
    fc.closing_balance_reporting - LAG(fc.closing_balance_reporting, 1) OVER ({w}) AS movement_on_prior_period,
    CASE WHEN fc.close_status_code = 'CLOSED' AND fc.within_tolerance_flag THEN 'Clean Close'
         WHEN fc.close_status_code = 'CLOSED' THEN 'Closed With Variance'
         WHEN fc.unposted_journal_count > 0 THEN 'Blocked - Unposted Journals'
         WHEN NOT fc.within_tolerance_flag THEN 'Blocked - Reconciliation' ELSE 'In Progress' END AS close_assessment,
    fc.refreshed_datetime AS data_as_of
FROM {t['agg_finance_close_summary']} AS fc
"""


def apAgingCurrent(t: Tables) -> str:
    return f"""
SELECT
    ap.month_end_date_key AS month_end, ap.fiscal_year, ap.fiscal_period, ap.region_code AS region, ap.supplier_key,
    s.supplier, s.category AS supplier_category, ap.aging_bucket_code AS aging_bucket, ap.aging_bucket_sort_order AS bucket_order,
    ap.open_invoice_count AS open_invoices, ap.balance_transaction_currency, ap.balance_reporting AS balance,
    ap.not_yet_due_reporting AS not_yet_due, ap.balance_reporting - ap.not_yet_due_reporting AS overdue_balance,
    ap.blocked_for_payment_reporting AS blocked_for_payment, ap.discount_still_capturable, ap.discount_lost_to_date AS discount_lost,
    ap.grni_accrual_reporting AS grni_accrual, ap.recoverable_tax_reporting AS recoverable_tax, ap.days_payable_outstanding,
    ap.average_days_beyond_terms, ap.match_exception_count AS match_exceptions, ap.supplier_risk_rating_code AS supplier_risk_rating,
    SUM(ap.balance_reporting) OVER (PARTITION BY ap.region_code, ap.supplier_key) AS supplier_total_balance,
    ROUND(100.0 * ap.balance_reporting / NULLIF(SUM(ap.balance_reporting) OVER (PARTITION BY ap.region_code), 0), 2) AS percent_of_regional_ap,
    CASE WHEN ap.average_days_beyond_terms > 30 THEN 'Chronic Late Payer' WHEN ap.average_days_beyond_terms > 5 THEN 'Slipping'
         WHEN ap.average_days_beyond_terms < -5 THEN 'Paying Early' ELSE 'On Terms' END AS payment_behaviour
FROM {t['fact_monthly_ap_aging']} AS ap
INNER JOIN (SELECT region_code, MAX(month_end_date_key) AS latest_month_end FROM {t['fact_monthly_ap_aging']}
            WHERE snapshot_frozen_flag GROUP BY region_code) AS latest
    ON latest.region_code = ap.region_code AND latest.latest_month_end = ap.month_end_date_key
LEFT JOIN {t['dim_supplier']} AS s ON s.supplier_key = ap.supplier_key
WHERE ap.snapshot_frozen_flag
"""


def loyaltyProgramSummary(t: Tables) -> str:
    w = "PARTITION BY mm.region_code, mm.loyalty_tier_key ORDER BY mm.calendar_month"
    return f"""
WITH movement_month AS (
    SELECT TRUNC(lp.movement_date_key, 'MM') AS calendar_month, lp.region_code, lp.loyalty_tier_key,
           COUNT(*) AS movement_count, COUNT(DISTINCT lp.loyalty_account_number) AS active_accounts,
           COUNT(DISTINCT CASE WHEN lp.opt_out_flag THEN lp.loyalty_account_number END) AS opted_out_accounts,
           SUM(CASE WHEN lp.movement_type_code IN ('EARN', 'BONUS') THEN lp.points_delta ELSE 0 END) AS points_issued,
           SUM(CASE WHEN lp.movement_type_code = 'REDEEM' THEN -lp.points_delta ELSE 0 END) AS points_redeemed,
           SUM(CASE WHEN lp.movement_type_code = 'EXPIRE' THEN -lp.points_delta ELSE 0 END) AS points_expired,
           SUM(CASE WHEN lp.movement_type_code = 'ADJUST' THEN lp.points_delta ELSE 0 END) AS points_adjusted,
           SUM(lp.points_delta) AS net_points_movement, SUM(lp.point_liability_reporting) AS liability_movement,
           SUM(lp.redemption_value_amount) AS redemption_value, SUM(lp.breakage_amount) AS breakage_recognised,
           SUM(lp.qualifying_spend_amount) AS qualifying_spend,
           SUM(CASE WHEN lp.tier_change_flag THEN 1 ELSE 0 END) AS tier_changes
    FROM {t['fact_loyalty_points']} AS lp
    GROUP BY TRUNC(lp.movement_date_key, 'MM'), lp.region_code, lp.loyalty_tier_key
)
SELECT
    mm.calendar_month, mm.region_code AS region, tier.loyalty_tier, mm.movement_count AS movements,
    CASE WHEN mm.region_code = 'EU' THEN mm.active_accounts - mm.opted_out_accounts ELSE mm.active_accounts END AS reportable_members,
    mm.opted_out_accounts AS opted_out_members, mm.points_issued, mm.points_redeemed, mm.points_expired, mm.points_adjusted,
    mm.net_points_movement,
    SUM(mm.net_points_movement) OVER ({w} ROWS UNBOUNDED PRECEDING) AS closing_points_balance,
    SUM(mm.liability_movement) OVER ({w} ROWS UNBOUNDED PRECEDING) AS closing_liability,
    mm.redemption_value, mm.breakage_recognised, mm.qualifying_spend, mm.tier_changes,
    ROUND(100.0 * mm.points_redeemed / NULLIF(mm.points_issued, 0), 2) AS redemption_rate_percent,
    ROUND(100.0 * mm.points_expired / NULLIF(mm.points_issued, 0), 2) AS expiry_rate_percent,
    LAG(mm.points_issued, 12) OVER ({w}) AS points_issued_prior_year,
    CASE mm.region_code WHEN 'NA' THEN 'Full face value, annual breakage' WHEN 'EU' THEN 'Net of modelled breakage'
         ELSE 'Rolling 24 month expiry, local currency scheme' END AS liability_basis
FROM movement_month AS mm
LEFT JOIN {t['dim_loyalty_tier']} AS tier ON tier.loyalty_tier_key = mm.loyalty_tier_key
"""


def orderToCashCycle(t: Tables) -> str:
    return f"""
WITH cohort_keys AS (
    SELECT DISTINCT region_code, order_date_key FROM {t['fact_order_fulfilment']}
),
cohort AS (
    -- OUTER APPLY equivalent: the six-month regional median depends only on region and order date
    SELECT k.region_code, k.order_date_key,
           PERCENTILE(CAST(c.order_to_cash_cycle_days AS DOUBLE), 0.5) AS median_cycle_days
    FROM cohort_keys AS k
    INNER JOIN {t['fact_order_fulfilment']} AS c
        ON c.region_code = k.region_code AND c.cycle_complete_flag
       AND c.order_date_key >= ADD_MONTHS(k.order_date_key, -6) AND c.order_date_key <= k.order_date_key
    GROUP BY k.region_code, k.order_date_key
)
SELECT
    f.order_number, f.order_line_number AS order_line, f.invoice_number, f.despatch_note_number AS despatch_note,
    f.region_code AS region, cust.customer, terr.sales_territory AS territory, site.warehouse_site,
    f.order_date_key AS order_date, f.allocation_date_key AS allocated, f.pick_date_key AS picked, f.pack_date_key AS packed,
    f.despatch_date_key AS despatched, f.delivery_date_key AS delivered, f.invoice_date_key AS invoiced,
    f.cash_applied_date_key AS cash_applied, f.order_to_pick_lag_days AS order_to_pick_days,
    f.pick_to_despatch_lag_days AS pick_to_despatch_days, f.despatch_to_delivery_lag_days AS despatch_to_delivery_days,
    f.delivery_to_invoice_lag_days AS delivery_to_invoice_days, f.invoice_to_cash_lag_days AS invoice_to_cash_days,
    f.order_to_cash_cycle_days AS order_to_cash_days, f.service_target_days, f.quantity_ordered, f.quantity_despatched,
    f.quantity_invoiced, f.order_value_reporting AS order_value, f.invoiced_value_reporting AS invoiced_value,
    f.cash_applied_reporting AS cash_applied_value,
    f.order_value_reporting - COALESCE(f.cash_applied_reporting, 0) AS value_still_in_pipeline,
    f.pipeline_status_code AS pipeline_status, f.open_milestone_count AS open_milestones,
    f.pick_sla_breach_flag AS pick_sla_breach, f.delivery_sla_breach_flag AS delivery_sla_breach, f.perfect_order_flag,
    f.cancelled_flag, f.cycle_complete_flag, cohort.median_cycle_days AS cohort_median_cycle_days,
    f.order_to_cash_cycle_days - cohort.median_cycle_days AS variance_to_cohort_median,
    CASE WHEN NOT COALESCE(f.cycle_complete_flag, FALSE) THEN 'In Flight'
         WHEN f.order_to_cash_cycle_days <= f.service_target_days THEN 'Within Target'
         WHEN f.order_to_cash_cycle_days <= f.service_target_days + 10 THEN 'Marginal' ELSE 'Breached' END AS cycle_assessment,
    f.last_milestone_update
FROM {t['fact_order_fulfilment']} AS f
LEFT JOIN {t['dim_customer']} AS cust ON cust.customer_key = f.customer_key
LEFT JOIN {t['dim_sales_territory']} AS terr ON terr.sales_territory_key = f.sales_territory_key
LEFT JOIN {t['dim_warehouse_site']} AS site ON site.warehouse_site_key = f.warehouse_site_key
LEFT JOIN cohort ON cohort.region_code = f.region_code AND cohort.order_date_key = f.order_date_key
"""


VIEW_BUILDERS: Dict[str, Callable[[Tables], str]] = {
    "rpt_daily_sales_trend": dailySalesTrend,
    "rpt_inventory_health_current": inventoryHealthCurrent,
    "rpt_sales_by_customer_month": salesByCustomerMonth,
    "rpt_sales_by_product_month": salesByProductMonth,
    "rpt_sales_by_territory_month": salesByTerritoryMonth,
    "rpt_margin_by_product_category": marginByProductCategory,
    "rpt_customer_360": customer360,
    "rpt_customer_churn_risk": customerChurnRisk,
    "rpt_returns_rate_by_category": returnsRateByCategory,
    "rpt_promotion_roi": promotionRoi,
    "rpt_supplier_spend_ytd": supplierSpendYtd,
    "rpt_supplier_on_time_delivery": supplierOnTimeDelivery,
    "rpt_finance_close_status": financeCloseStatus,
    "rpt_ap_aging_current": apAgingCurrent,
    "rpt_loyalty_program_summary": loyaltyProgramSummary,
    "rpt_order_to_cash_cycle": orderToCashCycle,
}

DEFAULT_PUBLICATION_ORDER: List[str] = list(VIEW_BUILDERS.keys())


def resolvePublicationList(configuredList: str) -> List[str]:
    """``ReportingPublicationList`` is a comma/semicolon-separated list of legacy
    ``[Report].[vw_*]`` names or ``rpt_*`` names; blank means every view."""
    if configuredList is None or configuredList.strip() == "":
        return list(DEFAULT_PUBLICATION_ORDER)
    resolved: List[str] = []
    for raw in re.split(r"[,;]", configuredList):
        name = raw.strip().replace("[", "").replace("]", "")
        if not name:
            continue
        if name in REPORT_VIEWS:
            name = REPORT_VIEWS[name]
        elif name.startswith("vw_") and f"Report.{name}" in REPORT_VIEWS:
            name = REPORT_VIEWS[f"Report.{name}"]
        if name not in VIEW_BUILDERS:
            raise ValueError(f"Unknown reporting publication '{raw.strip()}'")
        if name not in resolved:
            resolved.append(name)
    return resolved


def viewDdl(catalog: str, viewName: str, selectSql: str, tableFn: Callable[[str, str, str], str]) -> str:
    fq = tableFn(catalog, "gold", viewName)
    return f"CREATE OR REPLACE VIEW {fq} AS\n{selectSql.strip()}"
