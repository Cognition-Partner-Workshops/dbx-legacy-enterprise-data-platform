"""Spark SQL builders for the gold.agg_* refreshes.

Each builder returns one SELECT whose column list is exactly the legacy
``Aggregate.*`` table (``sqlserver/warehouse/aggregates/*.sql``) in snake_case,
minus the IDENTITY surrogate and the refresh stamp (added by the notebook).
``t`` maps logical names (see ``agg_common.TABLES``) to resolved table or view
names so the same SQL runs against ``${catalog}.gold.*`` in a job and against
temp views in pytest.
"""
from __future__ import annotations

import datetime as dt
from typing import Dict, Sequence

from agg_common import RefreshWindow, addMonths, monthEnd, monthStart

Tables = Dict[str, str]


def _d(day: dt.date) -> str:
    return f"DATE'{day.isoformat()}'"


def _pct(numerator: str, denominator: str, scale: int = 4) -> str:
    return f"CASE WHEN COALESCE({denominator}, 0) = 0 THEN NULL ELSE ROUND(100.0 * {numerator} / {denominator}, {scale}) END"


def _ratio(numerator: str, denominator: str, scale: int = 4) -> str:
    return f"CASE WHEN COALESCE({denominator}, 0) = 0 THEN NULL ELSE ROUND({numerator} / {denominator}, {scale}) END"


ACTIVE_SALE_FILTER = "COALESCE(f.correction_type_code, 'ORIG') <> 'REV'"


# --------------------------------------------------------------------------
# AGG_Refresh_DailySalesSummary  (Integration.usp_RefreshAggregateDailySales)
# --------------------------------------------------------------------------
def dailySalesSummarySql(t: Tables, window: RefreshWindow) -> str:
    priorFrom = window.fromDate - dt.timedelta(days=364)
    priorTo = window.toDate - dt.timedelta(days=364)
    return f"""
WITH sale AS (
    SELECT
        f.invoice_date_key                                   AS sales_date,
        f.stock_item_key,
        si.product_category_key,
        f.sales_territory_key,
        f.sales_channel_key,
        f.region_code,
        MAX(f.fiscal_year)                                   AS fiscal_year,
        MAX(f.fiscal_period)                                 AS fiscal_period,
        COUNT(DISTINCT f.invoice_number)                     AS invoice_count,
        COUNT(*)                                             AS line_count,
        COUNT(DISTINCT f.customer_key)                       AS distinct_customer_count,
        SUM(f.quantity_base_uom)                             AS quantity_sold_base_uom,
        SUM(f.gross_amount)                                  AS gross_sales_amount,
        SUM(f.line_discount_amount)                          AS line_discount_amount,
        SUM(CASE WHEN f.promotion_key > 0 THEN f.line_discount_amount ELSE 0 END)
                                                             AS promotion_discount_amount,
        SUM(f.net_amount)                                    AS net_sales_amount,
        SUM(f.tax_amount)                                    AS tax_amount,
        SUM(COALESCE(f.freight_amount, 0))                   AS freight_amount,
        SUM(f.cost_of_sale_amount)                           AS cost_of_sales_amount,
        SUM(f.gross_margin_amount)                           AS gross_margin_amount,
        SUM(f.net_amount_reporting)                          AS net_sales_amount_reporting,
        COUNT(*)                                             AS source_row_count
    FROM {t['fact_sale']} AS f
    LEFT JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = f.stock_item_key
    WHERE {window.sqlLiteral('f.invoice_date_key')}
      AND {ACTIVE_SALE_FILTER}
    GROUP BY f.invoice_date_key, f.stock_item_key, si.product_category_key,
             f.sales_territory_key, f.sales_channel_key, f.region_code
),
ret AS (
    SELECT r.return_date_key, r.stock_item_key, r.sales_territory_key, r.region_code,
           SUM(r.net_credit_amount_reporting) AS returns_amount
    FROM {t['fact_return']} AS r
    WHERE {window.sqlLiteral('r.return_date_key')}
    GROUP BY r.return_date_key, r.stock_item_key, r.sales_territory_key, r.region_code
),
prior_year AS (
    SELECT f.invoice_date_key, f.stock_item_key, f.sales_territory_key, f.sales_channel_key, f.region_code,
           SUM(f.net_amount) AS prior_year_net_sales
    FROM {t['fact_sale']} AS f
    WHERE f.invoice_date_key BETWEEN {_d(priorFrom)} AND {_d(priorTo)}
      AND {ACTIVE_SALE_FILTER}
    GROUP BY f.invoice_date_key, f.stock_item_key, f.sales_territory_key, f.sales_channel_key, f.region_code
)
SELECT
    s.sales_date, s.stock_item_key, s.product_category_key, s.sales_territory_key, s.sales_channel_key,
    s.region_code, s.fiscal_year, s.fiscal_period, s.invoice_count, s.line_count, s.distinct_customer_count,
    s.quantity_sold_base_uom, s.gross_sales_amount, s.line_discount_amount, s.promotion_discount_amount,
    s.net_sales_amount, s.tax_amount, s.freight_amount, s.cost_of_sales_amount, s.gross_margin_amount,
    {_pct('s.gross_margin_amount', 's.net_sales_amount')}      AS margin_percent,
    COALESCE(ret.returns_amount, 0)                            AS returns_amount,
    s.net_sales_amount_reporting,
    py.prior_year_net_sales,
    {_pct('(s.net_sales_amount - py.prior_year_net_sales)', 'py.prior_year_net_sales')}
                                                               AS prior_year_variance_percent,
    s.source_row_count
FROM sale AS s
LEFT JOIN ret
    ON ret.return_date_key = s.sales_date AND ret.stock_item_key = s.stock_item_key
   AND ret.sales_territory_key = s.sales_territory_key AND ret.region_code = s.region_code
LEFT JOIN prior_year AS py
    ON py.invoice_date_key = DATE_SUB(s.sales_date, 364) AND py.stock_item_key = s.stock_item_key
   AND py.sales_territory_key = s.sales_territory_key AND py.region_code = s.region_code
   AND COALESCE(py.sales_channel_key, -1) = COALESCE(s.sales_channel_key, -1)
"""


# --------------------------------------------------------------------------
# AGG_Refresh_DailyInventoryHealth (Integration.usp_RefreshAggregateInventoryHealth)
# --------------------------------------------------------------------------
def dailyInventoryHealthSql(t: Tables, window: RefreshWindow, stockOutThreshold: float) -> str:
    movementFrom = window.fromDate - dt.timedelta(days=90)
    return f"""
WITH snap AS (
    SELECT
        f.snapshot_date_key                                          AS snapshot_date,
        f.warehouse_site_key,
        si.product_category_key,
        f.region_code,
        f.stock_item_key,
        f.quantity_on_hand,
        f.quantity_quarantined,
        f.reorder_level,
        f.days_of_cover,
        f.stock_value_reporting,
        f.excess_quantity,
        f.obsolescence_provision_amount,
        f.slow_moving_flag
    FROM {t['fact_daily_inventory_snapshot']} AS f
    LEFT JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = f.stock_item_key
    WHERE {window.sqlLiteral('f.snapshot_date_key')}
),
outbound AS (
    SELECT m.warehouse_site_key, si.product_category_key, m.region_code,
           SUM(ABS(m.quantity)) AS outbound_quantity_90d,
           SUM(ABS(m.cost_amount_reporting)) AS outbound_cost_90d
    FROM {t['fact_stock_movement']} AS m
    LEFT JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = m.stock_item_key
    WHERE m.movement_date_key BETWEEN {_d(movementFrom)} AND {_d(window.toDate)}
      AND m.movement_type_code IN ('ISSUE', 'SALE', 'TRANSFER_OUT')
    GROUP BY m.warehouse_site_key, si.product_category_key, m.region_code
),
agg AS (
    SELECT
        s.snapshot_date, s.warehouse_site_key, s.product_category_key, s.region_code,
        COUNT(DISTINCT s.stock_item_key)                                                  AS sku_count,
        COUNT(DISTINCT CASE WHEN s.quantity_on_hand > 0 THEN s.stock_item_key END)        AS sku_stocked_count,
        COUNT(DISTINCT CASE WHEN s.quantity_on_hand <= {stockOutThreshold} THEN s.stock_item_key END)
                                                                                          AS stockout_sku_count,
        COUNT(DISTINCT CASE WHEN s.quantity_on_hand < COALESCE(s.reorder_level, 0) THEN s.stock_item_key END)
                                                                                          AS below_reorder_sku_count,
        COUNT(DISTINCT CASE WHEN COALESCE(s.excess_quantity, 0) > 0 THEN s.stock_item_key END)
                                                                                          AS excess_sku_count,
        COUNT(DISTINCT CASE WHEN s.slow_moving_flag THEN s.stock_item_key END)            AS slow_moving_sku_count,
        COUNT(DISTINCT CASE WHEN COALESCE(s.quantity_quarantined, 0) > 0 THEN s.stock_item_key END)
                                                                                          AS quarantined_sku_count,
        SUM(s.quantity_on_hand)                                                           AS total_quantity_on_hand,
        SUM(s.stock_value_reporting)                                                      AS total_stock_value_reporting,
        SUM(CASE WHEN COALESCE(s.excess_quantity, 0) > 0 AND s.quantity_on_hand > 0
                 THEN s.stock_value_reporting * s.excess_quantity / s.quantity_on_hand ELSE 0 END)
                                                                                          AS excess_stock_value_reporting,
        SUM(CASE s.region_code
                WHEN 'EU'   THEN COALESCE(s.obsolescence_provision_amount, 0)
                WHEN 'APAC' THEN CASE WHEN s.slow_moving_flag THEN s.stock_value_reporting * 0.50
                                      ELSE COALESCE(s.obsolescence_provision_amount, 0) END
                ELSE CASE WHEN s.slow_moving_flag THEN s.stock_value_reporting * 0.25
                          ELSE COALESCE(s.obsolescence_provision_amount, 0) END END)
                                                                                          AS obsolescence_provision_amount,
        ROUND(AVG(CAST(s.days_of_cover AS DECIMAL(9, 2))), 2)                             AS average_days_of_cover
    FROM snap AS s
    GROUP BY s.snapshot_date, s.warehouse_site_key, s.product_category_key, s.region_code
)
SELECT
    a.snapshot_date, a.warehouse_site_key, a.product_category_key, a.region_code,
    a.sku_count, a.sku_stocked_count, a.stockout_sku_count, a.below_reorder_sku_count, a.excess_sku_count,
    a.slow_moving_sku_count, a.quarantined_sku_count, a.total_quantity_on_hand, a.total_stock_value_reporting,
    a.excess_stock_value_reporting, a.obsolescence_provision_amount, a.average_days_of_cover,
    {_pct('(a.sku_count - a.stockout_sku_count)', 'a.sku_count')}                          AS service_level_percent,
    {_ratio('(COALESCE(o.outbound_cost_90d, 0) * 365.0 / 90.0)', 'a.total_stock_value_reporting')}
                                                                                          AS inventory_turns_annualised,
    {_ratio('(a.total_stock_value_reporting * 90.0)', 'COALESCE(o.outbound_cost_90d, 0)', 2)}
                                                                                          AS days_inventory_outstanding,
    {_pct('a.stockout_sku_count', 'a.sku_count')}                                          AS stockout_rate_percent,
    CAST(1 AS INT)                                                                        AS intraday_refresh_count
FROM agg AS a
LEFT JOIN outbound AS o
    ON o.warehouse_site_key = a.warehouse_site_key
   AND COALESCE(o.product_category_key, -1) = COALESCE(a.product_category_key, -1)
   AND o.region_code = a.region_code
"""


# --------------------------------------------------------------------------
# AGG_Refresh_MonthlySalesSummary (Integration.usp_RefreshAggregateMonthlySales)
# --------------------------------------------------------------------------
REGIONAL_FISCAL_YEAR = """CASE f.region_code WHEN 'EU' THEN d.eu_fiscal_year WHEN 'APAC' THEN d.apac_fiscal_year ELSE d.na_fiscal_year END"""
REGIONAL_FISCAL_PERIOD = """CASE f.region_code WHEN 'EU' THEN d.eu_fiscal_period WHEN 'APAC' THEN d.apac_fiscal_period ELSE d.na_fiscal_period END"""
REGIONAL_FISCAL_CALENDAR = """CASE f.region_code WHEN 'EU' THEN 'APR12' WHEN 'APAC' THEN 'JUL13' ELSE 'JAN445' END"""


def monthlySalesSummarySql(t: Tables, window: RefreshWindow) -> str:
    # Two extra trailing months are read so prior-period / rolling-3 comparisons
    # and the prior-year month can be derived in one pass.
    readFrom = addMonths(monthStart(window.fromDate), -12)
    return f"""
WITH sale AS (
    SELECT
        TRUNC(f.invoice_date_key, 'MM')                        AS calendar_month,
        f.customer_key, f.sales_territory_key, f.customer_segment_key, f.sales_channel_key, f.region_code,
        MAX({REGIONAL_FISCAL_YEAR})                            AS fiscal_year,
        MAX({REGIONAL_FISCAL_PERIOD})                          AS fiscal_period,
        MAX({REGIONAL_FISCAL_CALENDAR})                        AS fiscal_calendar_code,
        COUNT(DISTINCT f.order_number)                         AS order_count,
        COUNT(DISTINCT f.invoice_number)                       AS invoice_count,
        SUM(f.quantity_base_uom)                               AS quantity_sold_base_uom,
        SUM(f.gross_amount)                                    AS gross_revenue,
        SUM(f.line_discount_amount)                            AS discount_given,
        SUM(CASE WHEN f.region_code = 'EU' AND f.tax_regime_code = 'RC'
                 THEN f.net_amount - COALESCE(f.tax_amount, 0) ELSE f.net_amount END)
                                                               AS net_revenue,
        SUM(f.net_amount_reporting)                            AS net_revenue_reporting,
        SUM(f.cost_of_sale_amount * COALESCE(f.fx_rate_to_reporting, 1.0))
                                                               AS cost_of_sales_reporting,
        SUM(f.gross_margin_amount * COALESCE(f.fx_rate_to_reporting, 1.0))
                                                               AS gross_margin_reporting
    FROM {t['fact_sale']} AS f
    INNER JOIN {t['dim_date']} AS d ON d.date = f.invoice_date_key
    WHERE f.invoice_date_key BETWEEN {_d(readFrom)} AND {_d(window.toDate)}
      AND {ACTIVE_SALE_FILTER}
    GROUP BY TRUNC(f.invoice_date_key, 'MM'), f.customer_key, f.sales_territory_key,
             f.customer_segment_key, f.sales_channel_key, f.region_code
),
ret AS (
    SELECT TRUNC(r.return_date_key, 'MM') AS calendar_month, r.customer_key, r.region_code,
           COUNT(DISTINCT r.rma_number) AS return_count,
           SUM(r.net_credit_amount_reporting) AS returns_reporting
    FROM {t['fact_return']} AS r
    WHERE r.return_date_key BETWEEN {_d(readFrom)} AND {_d(window.toDate)}
    GROUP BY TRUNC(r.return_date_key, 'MM'), r.customer_key, r.region_code
),
credit AS (
    SELECT TRUNC(c.credit_note_date_key, 'MM') AS calendar_month, c.customer_key, c.region_code,
           SUM(c.credit_amount_reporting) AS credit_notes_reporting
    FROM {t['fact_credit_note']} AS c
    WHERE c.credit_note_date_key BETWEEN {_d(readFrom)} AND {_d(window.toDate)}
    GROUP BY TRUNC(c.credit_note_date_key, 'MM'), c.customer_key, c.region_code
),
enriched AS (
    SELECT
        s.*,
        COALESCE(ret.return_count, 0)                          AS return_count,
        COALESCE(credit.credit_notes_reporting, 0)             AS credit_notes_reporting,
        COALESCE(ret.returns_reporting, 0)                     AS returns_reporting,
        LAG(s.net_revenue_reporting, 1) OVER w                 AS prior_period_net_revenue,
        LAG(s.net_revenue_reporting, 12) OVER w                AS prior_year_net_revenue,
        SUM(s.net_revenue_reporting) OVER (PARTITION BY s.customer_key, s.sales_territory_key,
            s.customer_segment_key, s.sales_channel_key, s.region_code ORDER BY s.calendar_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)          AS rolling_3_period_net_revenue
    FROM sale AS s
    LEFT JOIN ret    ON ret.calendar_month = s.calendar_month AND ret.customer_key = s.customer_key AND ret.region_code = s.region_code
    LEFT JOIN credit ON credit.calendar_month = s.calendar_month AND credit.customer_key = s.customer_key AND credit.region_code = s.region_code
    WINDOW w AS (PARTITION BY s.customer_key, s.sales_territory_key, s.customer_segment_key,
                 s.sales_channel_key, s.region_code ORDER BY s.calendar_month)
)
SELECT
    e.fiscal_year, e.fiscal_period, e.calendar_month, e.customer_key, e.sales_territory_key,
    e.customer_segment_key, e.sales_channel_key, e.region_code, e.fiscal_calendar_code,
    e.order_count, e.invoice_count, e.return_count, e.quantity_sold_base_uom, e.gross_revenue, e.discount_given,
    e.net_revenue, e.net_revenue_reporting, e.credit_notes_reporting, e.returns_reporting,
    e.net_revenue_reporting - e.credit_notes_reporting - e.returns_reporting  AS net_revenue_after_credits,
    e.cost_of_sales_reporting, e.gross_margin_reporting,
    {_ratio('e.net_revenue_reporting', 'e.order_count', 2)}                  AS average_order_value,
    e.prior_period_net_revenue, e.prior_year_net_revenue, e.rolling_3_period_net_revenue,
    {_pct('(e.net_revenue_reporting - e.prior_period_net_revenue)', 'e.prior_period_net_revenue')}
                                                                             AS period_over_period_percent,
    {_pct('(e.net_revenue_reporting - e.prior_year_net_revenue)', 'e.prior_year_net_revenue')}
                                                                             AS year_over_year_percent,
    CAST(FALSE AS BOOLEAN)                                                   AS period_closed_flag
FROM enriched AS e
WHERE e.calendar_month BETWEEN {_d(monthStart(window.fromDate))} AND {_d(window.toDate)}
"""


# --------------------------------------------------------------------------
# AGG_Refresh_MonthlyMarginAnalysis (Integration.usp_RefreshAggregateMarginAnalysis)
# --------------------------------------------------------------------------
def monthlyMarginAnalysisSql(t: Tables, window: RefreshWindow, apportionPurchaseVariance: bool) -> str:
    readFrom = addMonths(monthStart(window.fromDate), -1)
    ppvTerm = "COALESCE(m.purchase_price_variance, 0)" if apportionPurchaseVariance else "0"
    return f"""
WITH base AS (
    SELECT
        TRUNC(f.invoice_date_key, 'MM')                                  AS calendar_month,
        si.product_category_key, f.sales_territory_key, f.sales_channel_key, f.region_code,
        MAX({REGIONAL_FISCAL_YEAR})                                      AS fiscal_year,
        MAX({REGIONAL_FISCAL_PERIOD})                                    AS fiscal_period,
        CASE f.region_code WHEN 'EU' THEN 'FIFO' WHEN 'APAC' THEN 'STD' ELSE 'WAVG' END AS cost_basis_code,
        SUM(f.quantity_base_uom)                                         AS quantity_sold_base_uom,
        SUM(f.net_amount_reporting)                                      AS net_revenue_reporting,
        SUM(CASE f.region_code WHEN 'EU' THEN f.fifo_cost_reporting
                               WHEN 'APAC' THEN f.standard_cost_reporting
                               ELSE f.weighted_average_cost_reporting END)
                                                                         AS cost_of_sales_reporting,
        SUM(f.standard_cost_reporting)                                   AS standard_cost_reporting,
        SUM(f.purchase_price_variance_reporting)                         AS purchase_price_variance,
        SUM(COALESCE(f.freight_cost_reporting, 0))                       AS freight_cost_reporting,
        SUM(COALESCE(f.rebate_accrual_reporting, 0))                     AS rebate_accrual_reporting,
        SUM(CASE WHEN f.gross_margin_reporting < 0 THEN 1 ELSE 0 END)    AS negative_margin_line_count
    FROM {t['fact_sales_margin']} AS f
    INNER JOIN {t['dim_date']} AS d ON d.date = f.invoice_date_key
    LEFT JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = f.stock_item_key
    WHERE f.invoice_date_key BETWEEN {_d(readFrom)} AND {_d(window.toDate)}
    GROUP BY TRUNC(f.invoice_date_key, 'MM'), si.product_category_key, f.sales_territory_key,
             f.sales_channel_key, f.region_code
),
margin AS (
    SELECT m.*,
           m.net_revenue_reporting - m.cost_of_sales_reporting - {ppvTerm}    AS gross_margin_reporting,
           m.net_revenue_reporting - m.standard_cost_reporting                 AS standard_margin_reporting,
           m.net_revenue_reporting - m.cost_of_sales_reporting - {ppvTerm}
               - m.freight_cost_reporting - m.rebate_accrual_reporting         AS contribution_margin_reporting
    FROM base AS m
),
bridged AS (
    SELECT m.*,
           LAG(m.quantity_sold_base_uom)  OVER w AS prior_quantity,
           LAG(m.net_revenue_reporting)   OVER w AS prior_revenue,
           LAG(m.cost_of_sales_reporting) OVER w AS prior_cost,
           LAG(m.gross_margin_reporting)  OVER w AS prior_margin
    FROM margin AS m
    WINDOW w AS (PARTITION BY m.product_category_key, m.sales_territory_key, m.sales_channel_key, m.region_code
                 ORDER BY m.calendar_month)
)
SELECT
    b.fiscal_year, b.fiscal_period, b.calendar_month, b.product_category_key, b.sales_territory_key,
    b.sales_channel_key, b.region_code, b.cost_basis_code, b.quantity_sold_base_uom, b.net_revenue_reporting,
    b.cost_of_sales_reporting, b.standard_cost_reporting, b.purchase_price_variance, b.freight_cost_reporting,
    b.rebate_accrual_reporting, b.gross_margin_reporting, b.standard_margin_reporting, b.contribution_margin_reporting,
    {_pct('b.gross_margin_reporting', 'b.net_revenue_reporting')}    AS margin_percent,
    {_pct('b.standard_margin_reporting', 'b.net_revenue_reporting')} AS standard_margin_percent,
    -- price effect: (current price - prior price) * current volume
    CASE WHEN COALESCE(b.prior_quantity, 0) = 0 OR COALESCE(b.quantity_sold_base_uom, 0) = 0 THEN NULL
         ELSE ROUND((b.net_revenue_reporting / b.quantity_sold_base_uom - b.prior_revenue / b.prior_quantity)
                    * b.quantity_sold_base_uom, 2) END               AS price_effect_amount,
    -- volume effect: (current volume - prior volume) * prior unit margin
    CASE WHEN COALESCE(b.prior_quantity, 0) = 0 THEN NULL
         ELSE ROUND((b.quantity_sold_base_uom - b.prior_quantity) * (b.prior_margin / b.prior_quantity), 2) END
                                                                     AS volume_effect_amount,
    -- mix effect: residual of margin movement after price, volume and cost
    CASE WHEN COALESCE(b.prior_quantity, 0) = 0 OR COALESCE(b.quantity_sold_base_uom, 0) = 0 THEN NULL
         ELSE ROUND((b.gross_margin_reporting - b.prior_margin)
                    - ((b.net_revenue_reporting / b.quantity_sold_base_uom - b.prior_revenue / b.prior_quantity) * b.quantity_sold_base_uom)
                    - ((b.quantity_sold_base_uom - b.prior_quantity) * (b.prior_margin / b.prior_quantity))
                    + ((b.cost_of_sales_reporting / b.quantity_sold_base_uom - b.prior_cost / b.prior_quantity) * b.quantity_sold_base_uom), 2) END
                                                                     AS mix_effect_amount,
    -- cost effect: -(current unit cost - prior unit cost) * current volume
    CASE WHEN COALESCE(b.prior_quantity, 0) = 0 OR COALESCE(b.quantity_sold_base_uom, 0) = 0 THEN NULL
         ELSE ROUND(-1 * (b.cost_of_sales_reporting / b.quantity_sold_base_uom - b.prior_cost / b.prior_quantity)
                    * b.quantity_sold_base_uom, 2) END               AS cost_effect_amount,
    b.negative_margin_line_count,
    {_pct('b.prior_margin', 'b.prior_revenue')}                      AS prior_period_margin_percent
FROM bridged AS b
WHERE b.calendar_month BETWEEN {_d(monthStart(window.fromDate))} AND {_d(window.toDate)}
"""


# --------------------------------------------------------------------------
# AGG_Refresh_Customer360 (Integration.usp_RefreshAggregateCustomer360, profile pass)
# --------------------------------------------------------------------------
def customer360Sql(t: Tables, asAtDate: dt.date) -> str:
    webFrom = asAtDate - dt.timedelta(days=90)
    return f"""
WITH sale AS (
    SELECT f.customer_key,
           MIN(f.invoice_date_key)                                    AS first_order_date,
           MAX(f.invoice_date_key)                                    AS last_order_date,
           COUNT(DISTINCT f.order_number)                             AS lifetime_order_count,
           SUM(f.net_amount_reporting)                                AS lifetime_net_revenue,
           SUM(f.gross_margin_amount * COALESCE(f.fx_rate_to_reporting, 1.0)) AS lifetime_gross_margin
    FROM {t['fact_sale']} AS f
    WHERE f.invoice_date_key <= {_d(asAtDate)} AND {ACTIVE_SALE_FILTER}
    GROUP BY f.customer_key
),
channel AS (
    SELECT customer_key, sales_channel_key AS primary_sales_channel_key
    FROM (
        SELECT f.customer_key, f.sales_channel_key,
               ROW_NUMBER() OVER (PARTITION BY f.customer_key ORDER BY SUM(f.net_amount_reporting) DESC, f.sales_channel_key) AS rn
        FROM {t['fact_sale']} AS f
        WHERE f.invoice_date_key <= {_d(asAtDate)} AND {ACTIVE_SALE_FILTER}
        GROUP BY f.customer_key, f.sales_channel_key
    ) WHERE rn = 1
),
ret AS (
    SELECT r.customer_key, SUM(r.net_credit_amount_reporting) AS lifetime_returns_amount
    FROM {t['fact_return']} AS r WHERE r.return_date_key <= {_d(asAtDate)} GROUP BY r.customer_key
),
pay AS (
    SELECT p.customer_key, MAX(p.payment_date_key) AS last_payment_date,
           AVG(CAST(p.days_to_pay AS DECIMAL(9, 2))) AS average_days_to_pay
    FROM {t['fact_customer_payment']} AS p WHERE p.payment_date_key <= {_d(asAtDate)} GROUP BY p.customer_key
),
bal AS (
    SELECT b.customer_key, b.current_balance_reporting, b.overdue_balance_reporting, b.credit_limit_reporting
    FROM (
        SELECT b.*, ROW_NUMBER() OVER (PARTITION BY b.customer_key ORDER BY b.month_end_date_key DESC) AS rn
        FROM {t['fact_monthly_customer_balance']} AS b WHERE b.month_end_date_key <= {_d(asAtDate)}
    ) AS b WHERE b.rn = 1
),
loyalty AS (
    SELECT lp.customer_key, SUM(lp.points_delta) AS loyalty_point_balance,
           MAX(lp.loyalty_tier_key) AS loyalty_tier_key
    FROM {t['fact_loyalty_points']} AS lp WHERE lp.movement_date_key <= {_d(asAtDate)} GROUP BY lp.customer_key
),
web AS (
    SELECT w.customer_key, COUNT(*) AS web_session_count_90_day
    FROM {t['fact_web_session']} AS w
    WHERE w.session_date_key BETWEEN {_d(webFrom)} AND {_d(asAtDate)} GROUP BY w.customer_key
),
profile AS (
    SELECT
        c.customer_key, c.customer_segment_key, c.sales_territory_key, l.loyalty_tier_key,
        ch.primary_sales_channel_key, c.region_code, c.customer AS customer_name,
        c.primary_contact_email, c.account_manager_employee_key,
        s.first_order_date, s.last_order_date, pay.last_payment_date,
        CAST(FLOOR(MONTHS_BETWEEN({_d(asAtDate)}, s.first_order_date)) AS INT)  AS tenure_months,
        COALESCE(s.lifetime_order_count, 0)                                     AS lifetime_order_count,
        COALESCE(s.lifetime_net_revenue, 0)                                     AS lifetime_net_revenue,
        COALESCE(s.lifetime_gross_margin, 0)                                    AS lifetime_gross_margin,
        COALESCE(ret.lifetime_returns_amount, 0)                                AS lifetime_returns_amount,
        {_ratio('s.lifetime_net_revenue', 's.lifetime_order_count', 2)}          AS average_order_value,
        pay.average_days_to_pay,
        bal.current_balance_reporting, bal.overdue_balance_reporting, bal.credit_limit_reporting,
        {_pct('bal.current_balance_reporting', 'bal.credit_limit_reporting')}    AS credit_utilisation_percent,
        COALESCE(l.loyalty_point_balance, 0)                                    AS loyalty_point_balance,
        COALESCE(web.web_session_count_90_day, 0)                               AS web_session_count_90_day,
        DATEDIFF({_d(asAtDate)}, s.last_order_date)                             AS days_since_last_order,
        c.marketing_consent_flag,
        c.retention_expiry_date,
        COALESCE(c.is_erased, FALSE)                                            AS is_erased
    FROM {t['dim_customer']} AS c
    LEFT JOIN sale AS s      ON s.customer_key = c.customer_key
    LEFT JOIN channel AS ch  ON ch.customer_key = c.customer_key
    LEFT JOIN ret            ON ret.customer_key = c.customer_key
    LEFT JOIN pay            ON pay.customer_key = c.customer_key
    LEFT JOIN bal            ON bal.customer_key = c.customer_key
    LEFT JOIN loyalty AS l   ON l.customer_key = c.customer_key
    LEFT JOIN web            ON web.customer_key = c.customer_key
    WHERE c.is_current AND c.customer_key > 0
),
scored AS (
    SELECT p.*,
        NTILE(5) OVER (ORDER BY COALESCE(p.days_since_last_order, 99999) DESC)  AS recency_score,
        NTILE(5) OVER (ORDER BY p.lifetime_order_count)                          AS frequency_score,
        NTILE(5) OVER (ORDER BY p.lifetime_net_revenue)                          AS monetary_score
    FROM profile AS p
),
rfm AS (
    SELECT s.*,
        ROUND(LEAST(100.0, GREATEST(0.0,
              COALESCE(s.days_since_last_order, 730) / 7.3
              + CASE WHEN s.overdue_balance_reporting > 0 THEN 10 ELSE 0 END
              - s.web_session_count_90_day * 0.5)), 4)                          AS churn_risk_score
    FROM scored AS s
)
SELECT
    r.customer_key, r.customer_segment_key, r.sales_territory_key, r.loyalty_tier_key, r.primary_sales_channel_key,
    r.region_code,
    CASE WHEN anonymise THEN NULL ELSE r.customer_name END                      AS customer_name,
    CASE WHEN anonymise THEN NULL ELSE r.primary_contact_email END              AS primary_contact_email,
    r.account_manager_employee_key, r.first_order_date, r.last_order_date, r.last_payment_date, r.tenure_months,
    r.lifetime_order_count, r.lifetime_net_revenue, r.lifetime_gross_margin, r.lifetime_returns_amount,
    r.average_order_value, r.average_days_to_pay, r.current_balance_reporting, r.overdue_balance_reporting,
    r.credit_limit_reporting, r.credit_utilisation_percent, r.loyalty_point_balance, r.web_session_count_90_day,
    r.days_since_last_order, r.churn_risk_score,
    CASE WHEN r.churn_risk_score >= 80 THEN 'HIGH' WHEN r.churn_risk_score >= 55 THEN 'MEDIUM'
         WHEN r.churn_risk_score >= 40 THEN 'WATCH' ELSE 'LOW' END              AS churn_risk_band,
    CONCAT(CAST(r.recency_score AS STRING), CAST(r.frequency_score AS STRING), CAST(r.monetary_score AS STRING))
                                                                                AS rfm_score,
    CASE WHEN anonymise THEN FALSE ELSE r.marketing_consent_flag END            AS marketing_consent_flag,
    r.retention_expiry_date,
    anonymise                                                                   AS anonymised_flag,
    r.is_erased,
    r.days_since_last_order > 730                                               AS is_inactive
FROM (
    SELECT r.*,
           (r.is_erased OR (r.region_code IN ('EU', 'APAC') AND r.retention_expiry_date < {_d(asAtDate)}))
                                                                                AS anonymise
    FROM rfm AS r
) AS r
"""


# --------------------------------------------------------------------------
# AGG_Refresh_CustomerRolling12Month (rolling pass of usp_RefreshAggregateCustomer360)
# --------------------------------------------------------------------------
def customerRolling12MonthSql(t: Tables, rollingFrom: dt.date, asAtMonth: dt.date, rollingMonths: int) -> str:
    # Rolling-12 windows need 11 months of history before the first rebuilt month.
    readFrom = addMonths(rollingFrom, -11)
    return f"""
WITH months AS (
    SELECT DISTINCT TRUNC(d.date, 'MM') AS calendar_month
    FROM {t['dim_date']} AS d
    WHERE d.date BETWEEN {_d(readFrom)} AND {_d(monthEnd(asAtMonth))}
),
customers AS (
    SELECT DISTINCT f.customer_key, f.region_code
    FROM {t['fact_sale']} AS f
    WHERE f.invoice_date_key BETWEEN {_d(readFrom)} AND {_d(monthEnd(asAtMonth))} AND {ACTIVE_SALE_FILTER}
),
grid AS (
    SELECT c.customer_key, c.region_code, m.calendar_month FROM customers AS c CROSS JOIN months AS m
),
sale AS (
    SELECT TRUNC(f.invoice_date_key, 'MM') AS calendar_month, f.customer_key, f.region_code,
           COUNT(DISTINCT f.order_number)                                AS order_count,
           SUM(f.net_amount_reporting)                                   AS net_revenue_reporting,
           SUM(f.gross_margin_amount * COALESCE(f.fx_rate_to_reporting, 1.0)) AS gross_margin_reporting,
           COUNT(DISTINCT f.stock_item_key)                              AS distinct_product_count
    FROM {t['fact_sale']} AS f
    WHERE f.invoice_date_key BETWEEN {_d(readFrom)} AND {_d(monthEnd(asAtMonth))} AND {ACTIVE_SALE_FILTER}
    GROUP BY TRUNC(f.invoice_date_key, 'MM'), f.customer_key, f.region_code
),
ret AS (
    SELECT TRUNC(r.return_date_key, 'MM') AS calendar_month, r.customer_key, SUM(r.net_credit_amount_reporting) AS returns_reporting
    FROM {t['fact_return']} AS r WHERE r.return_date_key BETWEEN {_d(readFrom)} AND {_d(monthEnd(asAtMonth))}
    GROUP BY TRUNC(r.return_date_key, 'MM'), r.customer_key
),
pay AS (
    SELECT TRUNC(p.payment_date_key, 'MM') AS calendar_month, p.customer_key, SUM(p.amount_reporting) AS cash_received_reporting
    FROM {t['fact_customer_payment']} AS p WHERE p.payment_date_key BETWEEN {_d(readFrom)} AND {_d(monthEnd(asAtMonth))}
    GROUP BY TRUNC(p.payment_date_key, 'MM'), p.customer_key
),
loyalty AS (
    SELECT TRUNC(lp.movement_date_key, 'MM') AS calendar_month, lp.customer_key,
           SUM(CASE WHEN lp.movement_type_code IN ('EARN', 'BONUS') THEN lp.points_delta ELSE 0 END) AS loyalty_points_earned,
           SUM(CASE WHEN lp.movement_type_code = 'REDEEM' THEN -lp.points_delta ELSE 0 END)       AS loyalty_points_redeemed
    FROM {t['fact_loyalty_points']} AS lp WHERE lp.movement_date_key BETWEEN {_d(readFrom)} AND {_d(monthEnd(asAtMonth))}
    GROUP BY TRUNC(lp.movement_date_key, 'MM'), lp.customer_key
),
web AS (
    SELECT TRUNC(w.session_date_key, 'MM') AS calendar_month, w.customer_key, COUNT(*) AS web_session_count
    FROM {t['fact_web_session']} AS w WHERE w.session_date_key BETWEEN {_d(readFrom)} AND {_d(monthEnd(asAtMonth))}
    GROUP BY TRUNC(w.session_date_key, 'MM'), w.customer_key
),
monthly AS (
    SELECT g.customer_key, g.calendar_month, g.region_code,
           COALESCE(s.order_count, 0)              AS order_count,
           COALESCE(s.net_revenue_reporting, 0)    AS net_revenue_reporting,
           COALESCE(s.gross_margin_reporting, 0)   AS gross_margin_reporting,
           COALESCE(ret.returns_reporting, 0)      AS returns_reporting,
           COALESCE(pay.cash_received_reporting, 0) AS cash_received_reporting,
           COALESCE(s.distinct_product_count, 0)   AS distinct_product_count,
           COALESCE(l.loyalty_points_earned, 0)    AS loyalty_points_earned,
           COALESCE(l.loyalty_points_redeemed, 0)  AS loyalty_points_redeemed,
           COALESCE(web.web_session_count, 0)      AS web_session_count
    FROM grid AS g
    LEFT JOIN sale AS s    ON s.calendar_month = g.calendar_month AND s.customer_key = g.customer_key AND s.region_code = g.region_code
    LEFT JOIN ret          ON ret.calendar_month = g.calendar_month AND ret.customer_key = g.customer_key
    LEFT JOIN pay          ON pay.calendar_month = g.calendar_month AND pay.customer_key = g.customer_key
    LEFT JOIN loyalty AS l ON l.calendar_month = g.calendar_month AND l.customer_key = g.customer_key
    LEFT JOIN web          ON web.calendar_month = g.calendar_month AND web.customer_key = g.customer_key
),
rolled AS (
    SELECT m.*,
           SUM(m.net_revenue_reporting)  OVER (PARTITION BY m.customer_key, m.region_code ORDER BY m.calendar_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_12_month_revenue,
           SUM(m.gross_margin_reporting) OVER (PARTITION BY m.customer_key, m.region_code ORDER BY m.calendar_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_12_month_margin,
           SUM(m.net_revenue_reporting)  OVER (PARTITION BY m.customer_key, m.region_code ORDER BY m.calendar_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)  AS rolling_3_month_revenue,
           SUM(m.net_revenue_reporting)  OVER (PARTITION BY m.customer_key, m.region_code ORDER BY m.calendar_month ROWS BETWEEN 5 PRECEDING AND 3 PRECEDING)  AS previous_3_month_revenue,
           m.order_count = 0                                                                 AS inactive_month_flag,
           SUM(CASE WHEN m.order_count > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY m.customer_key, m.region_code ORDER BY m.calendar_month ROWS UNBOUNDED PRECEDING) AS activity_group
    FROM monthly AS m
)
SELECT
    r.customer_key,
    CAST(MONTHS_BETWEEN({_d(asAtMonth)}, r.calendar_month) AS INT)           AS month_offset,
    r.calendar_month, r.region_code, r.order_count, r.net_revenue_reporting, r.gross_margin_reporting,
    r.returns_reporting, r.cash_received_reporting, r.distinct_product_count, r.loyalty_points_earned,
    r.loyalty_points_redeemed, r.web_session_count, r.rolling_12_month_revenue, r.rolling_12_month_margin,
    r.rolling_3_month_revenue,
    {_pct('(r.rolling_3_month_revenue - r.previous_3_month_revenue)', 'r.previous_3_month_revenue')}
                                                                             AS revenue_trend_percent,
    r.inactive_month_flag,
    CAST(CASE WHEN r.inactive_month_flag
              THEN ROW_NUMBER() OVER (PARTITION BY r.customer_key, r.region_code, r.activity_group ORDER BY r.calendar_month)
                   - CASE WHEN r.activity_group > 0 THEN 1 ELSE 0 END
              ELSE 0 END AS INT)                                             AS consecutive_inactive_months
FROM rolled AS r
WHERE r.calendar_month BETWEEN {_d(rollingFrom)} AND {_d(monthEnd(asAtMonth))}
  AND CAST(MONTHS_BETWEEN({_d(asAtMonth)}, r.calendar_month) AS INT) BETWEEN 0 AND {rollingMonths}
"""


# --------------------------------------------------------------------------
# AGG_Refresh_ProductPerformance (Integration.usp_RefreshAggregateProductPerformance)
# --------------------------------------------------------------------------
def productPerformanceSql(t: Tables, window: RefreshWindow, abcThresholdA: float, abcThresholdB: float) -> str:
    readFrom = addMonths(monthStart(window.fromDate), -1)
    return f"""
WITH sale AS (
    SELECT TRUNC(f.invoice_date_key, 'MM') AS calendar_month, f.stock_item_key, f.region_code,
           SUM(f.quantity_base_uom)                   AS units_sold_base_uom,
           SUM(f.net_amount_reporting)                AS net_revenue_reporting,
           SUM(f.gross_margin_amount * COALESCE(f.fx_rate_to_reporting, 1.0)) AS gross_margin_reporting,
           SUM(f.gross_amount)                        AS gross_amount,
           SUM(f.line_discount_amount)                AS discount_amount,
           SUM(f.cost_of_sale_amount * COALESCE(f.fx_rate_to_reporting, 1.0)) AS cost_reporting,
           COUNT(DISTINCT f.customer_key)             AS distinct_customer_count
    FROM {t['fact_sale']} AS f
    WHERE f.invoice_date_key BETWEEN {_d(readFrom)} AND {_d(window.toDate)} AND {ACTIVE_SALE_FILTER}
    GROUP BY TRUNC(f.invoice_date_key, 'MM'), f.stock_item_key, f.region_code
),
ret AS (
    SELECT TRUNC(r.return_date_key, 'MM') AS calendar_month, r.stock_item_key, r.region_code,
           SUM(r.quantity_returned) AS units_returned
    FROM {t['fact_return']} AS r WHERE r.return_date_key BETWEEN {_d(readFrom)} AND {_d(window.toDate)}
    GROUP BY TRUNC(r.return_date_key, 'MM'), r.stock_item_key, r.region_code
),
inv AS (
    SELECT TRUNC(i.snapshot_date_key, 'MM') AS calendar_month, i.stock_item_key, i.region_code,
           AVG(i.stock_value_reporting)                                       AS average_stock_value_reporting,
           COUNT(DISTINCT CASE WHEN i.quantity_on_hand <= 0 THEN i.snapshot_date_key END) AS stockout_days,
           COUNT(DISTINCT i.snapshot_date_key)                                AS snapshot_days
    FROM {t['fact_daily_inventory_snapshot']} AS i
    WHERE i.snapshot_date_key BETWEEN {_d(readFrom)} AND {_d(window.toDate)}
    GROUP BY TRUNC(i.snapshot_date_key, 'MM'), i.stock_item_key, i.region_code
),
base AS (
    SELECT s.calendar_month, s.stock_item_key, si.product_category_key, s.region_code, si.primary_supplier_key,
           s.units_sold_base_uom, s.net_revenue_reporting, s.gross_margin_reporting,
           {_pct('s.gross_margin_reporting', 's.net_revenue_reporting')}    AS margin_percent,
           {_pct('s.discount_amount', 's.gross_amount')}                    AS discount_depth_percent,
           COALESCE(ret.units_returned, 0)                                   AS units_returned,
           {_pct('COALESCE(ret.units_returned, 0)', 's.units_sold_base_uom')} AS return_rate_percent,
           {_ratio('s.net_revenue_reporting', 's.units_sold_base_uom')}      AS average_selling_price,
           {_ratio('s.cost_reporting', 's.units_sold_base_uom')}             AS average_unit_cost,
           inv.average_stock_value_reporting,
           {_ratio('s.cost_reporting', 'inv.average_stock_value_reporting')} AS inventory_turns,
           {_ratio('(inv.average_stock_value_reporting * COALESCE(inv.snapshot_days, 30))', 's.cost_reporting', 2)}
                                                                             AS days_inventory_outstanding,
           COALESCE(inv.stockout_days, 0)                                    AS stockout_days,
           CASE WHEN COALESCE(inv.snapshot_days, 0) = 0 THEN NULL
                ELSE ROUND(s.net_revenue_reporting / inv.snapshot_days * inv.stockout_days, 2) END
                                                                             AS lost_sales_estimate_reporting,
           {_pct('s.units_sold_base_uom', '(s.units_sold_base_uom + COALESCE(inv.average_stock_value_reporting, 0) / NULLIF(s.cost_reporting / NULLIF(s.units_sold_base_uom, 0), 0))')}
                                                                             AS sell_through_percent,
           s.distinct_customer_count,
           si.first_sold_date, si.is_discontinued
    FROM sale AS s
    LEFT JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = s.stock_item_key
    LEFT JOIN ret ON ret.calendar_month = s.calendar_month AND ret.stock_item_key = s.stock_item_key AND ret.region_code = s.region_code
    LEFT JOIN inv ON inv.calendar_month = s.calendar_month AND inv.stock_item_key = s.stock_item_key AND inv.region_code = s.region_code
),
ranked AS (
    SELECT b.*,
           SUM(b.net_revenue_reporting) OVER (PARTITION BY b.calendar_month, b.region_code, b.product_category_key
                ORDER BY b.net_revenue_reporting DESC, b.stock_item_key ROWS UNBOUNDED PRECEDING) AS cumulative_revenue,
           SUM(b.net_revenue_reporting) OVER (PARTITION BY b.calendar_month, b.region_code, b.product_category_key) AS category_revenue,
           RANK() OVER (PARTITION BY b.calendar_month, b.region_code, b.product_category_key ORDER BY b.net_revenue_reporting DESC) AS rank_in_category_by_revenue,
           RANK() OVER (PARTITION BY b.calendar_month, b.region_code, b.product_category_key ORDER BY b.gross_margin_reporting DESC) AS rank_in_category_by_margin,
           STDDEV_POP(b.units_sold_base_uom) OVER (PARTITION BY b.stock_item_key, b.region_code ORDER BY b.calendar_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS units_stddev,
           AVG(b.units_sold_base_uom) OVER (PARTITION BY b.stock_item_key, b.region_code ORDER BY b.calendar_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS units_mean
    FROM base AS b
),
classed AS (
    SELECT r.*,
           CASE WHEN COALESCE(r.category_revenue, 0) <= 0 THEN 'C'
                WHEN r.cumulative_revenue <= {abcThresholdA} * r.category_revenue THEN 'A'
                WHEN r.cumulative_revenue <= {abcThresholdB} * r.category_revenue THEN 'B'
                ELSE 'C' END AS abc_class,
           CASE WHEN COALESCE(r.units_mean, 0) = 0 THEN 'Z'
                WHEN r.units_stddev / r.units_mean <= 0.5 THEN 'X'
                WHEN r.units_stddev / r.units_mean <= 1.0 THEN 'Y'
                ELSE 'Z' END AS xyz_class
    FROM ranked AS r
)
SELECT
    c.calendar_month, c.stock_item_key, c.product_category_key, c.region_code, c.primary_supplier_key,
    c.units_sold_base_uom, c.net_revenue_reporting, c.gross_margin_reporting, c.margin_percent, c.discount_depth_percent,
    c.units_returned, c.return_rate_percent, c.average_selling_price, c.average_unit_cost, c.average_stock_value_reporting,
    c.inventory_turns, c.days_inventory_outstanding, c.stockout_days, c.lost_sales_estimate_reporting, c.sell_through_percent,
    c.distinct_customer_count, c.abc_class, c.xyz_class,
    LAG(c.abc_class) OVER (PARTITION BY c.stock_item_key, c.region_code ORDER BY c.calendar_month) AS prior_month_abc_class,
    c.rank_in_category_by_revenue, c.rank_in_category_by_margin,
    COALESCE(c.first_sold_date >= ADD_MONTHS(c.calendar_month, -3), FALSE) AS new_product_flag,
    COALESCE(c.is_discontinued, FALSE)                                    AS discontinued_flag
FROM classed AS c
WHERE c.calendar_month BETWEEN {_d(monthStart(window.fromDate))} AND {_d(window.toDate)}
"""


# --------------------------------------------------------------------------
# AGG_Refresh_SupplierPerformance (Integration.usp_RefreshAggregateSupplierPerformance)
# --------------------------------------------------------------------------
def supplierPerformanceSql(t: Tables, window: RefreshWindow) -> str:
    ytdFrom = dt.date(monthStart(window.fromDate).year - 1, 1, 1)
    return f"""
WITH purchase AS (
    SELECT TRUNC(p.order_date_key, 'MM') AS calendar_month, p.supplier_key, si.product_category_key, p.region_code,
           MAX(p.vendor_contract_key)                                   AS vendor_contract_key,
           COUNT(DISTINCT p.purchase_order_number)                      AS purchase_order_count,
           COUNT(*)                                                     AS purchase_line_count,
           SUM(p.ordered_value_reporting)                               AS committed_spend_reporting,
           SUM(CASE WHEN p.vendor_contract_key > 0 THEN p.ordered_value_reporting ELSE 0 END) AS contract_covered_spend,
           SUM(CASE WHEN COALESCE(p.vendor_contract_key, 0) <= 0 THEN p.ordered_value_reporting ELSE 0 END) AS maverick_spend_reporting
    FROM {t['fact_purchase']} AS p
    LEFT JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = p.stock_item_key
    WHERE p.order_date_key BETWEEN {_d(ytdFrom)} AND {_d(window.toDate)}
    GROUP BY TRUNC(p.order_date_key, 'MM'), p.supplier_key, si.product_category_key, p.region_code
),
receipt AS (
    SELECT TRUNC(r.receipt_date_key, 'MM') AS calendar_month, r.supplier_key, si.product_category_key, r.region_code,
           COUNT(*)                                                     AS receipt_count,
           SUM(r.receipt_value_reporting)                               AS recognised_spend_reporting,
           SUM(COALESCE(r.freight_in_reporting, 0))                     AS freight_in_reporting,
           SUM(COALESCE(r.customs_duty_reporting, 0))                   AS customs_duty_reporting,
           SUM(CASE WHEN r.on_time_flag THEN 1 ELSE 0 END)              AS on_time_receipt_count,
           SUM(CASE WHEN r.in_full_flag THEN 1 ELSE 0 END)              AS in_full_receipt_count,
           SUM(CASE WHEN r.on_time_flag AND r.in_full_flag THEN 1 ELSE 0 END) AS otif_receipt_count,
           AVG(CAST(GREATEST(r.days_late_versus_promise, 0) AS DECIMAL(9, 2))) AS average_days_late,
           AVG(CAST(r.lead_time_days AS DECIMAL(9, 2)))                 AS average_lead_time_days,
           STDDEV_POP(CAST(r.lead_time_days AS DECIMAL(9, 2)))          AS lead_time_variability_days,
           SUM(r.quantity_rejected_base_uom)                            AS rejected_quantity,
           SUM(r.quantity_received_base_uom)                            AS received_quantity,
           SUM(CASE WHEN r.match_exception_flag THEN 1 ELSE 0 END)      AS match_exception_count,
           SUM(COALESCE(r.price_variance_reporting, 0))                 AS price_variance_reporting
    FROM {t['fact_purchase_receipt']} AS r
    LEFT JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = r.stock_item_key
    WHERE r.receipt_date_key BETWEEN {_d(ytdFrom)} AND {_d(window.toDate)}
    GROUP BY TRUNC(r.receipt_date_key, 'MM'), r.supplier_key, si.product_category_key, r.region_code
),
payment AS (
    SELECT TRUNC(sp.payment_date_key, 'MM') AS calendar_month, sp.supplier_key, sp.region_code,
           SUM(COALESCE(sp.discount_captured_reporting, 0))             AS discount_captured_reporting,
           SUM(COALESCE(sp.discount_lost_reporting, 0))                 AS discount_lost_reporting,
           AVG(CAST(sp.days_beyond_terms AS DECIMAL(9, 2)))             AS average_days_beyond_terms
    FROM {t['fact_supplier_payment']} AS sp
    WHERE sp.payment_date_key BETWEEN {_d(ytdFrom)} AND {_d(window.toDate)}
    GROUP BY TRUNC(sp.payment_date_key, 'MM'), sp.supplier_key, sp.region_code
),
grain AS (
    SELECT calendar_month, supplier_key, product_category_key, region_code FROM purchase
    UNION
    SELECT calendar_month, supplier_key, product_category_key, region_code FROM receipt
),
base AS (
    SELECT g.calendar_month, g.supplier_key, g.product_category_key, g.region_code,
           p.vendor_contract_key,
           COALESCE(p.purchase_order_count, 0)        AS purchase_order_count,
           COALESCE(p.purchase_line_count, 0)         AS purchase_line_count,
           COALESCE(r.receipt_count, 0)               AS receipt_count,
           COALESCE(p.committed_spend_reporting, 0)   AS committed_spend_reporting,
           COALESCE(r.recognised_spend_reporting, 0)  AS recognised_spend_reporting,
           COALESCE(p.contract_covered_spend, 0)      AS contract_covered_spend,
           COALESCE(p.maverick_spend_reporting, 0)    AS maverick_spend_reporting,
           COALESCE(r.freight_in_reporting, 0)        AS freight_in_reporting,
           COALESCE(r.customs_duty_reporting, 0)      AS customs_duty_reporting,
           CASE g.region_code
                WHEN 'EU'   THEN COALESCE(r.recognised_spend_reporting, 0) + COALESCE(r.freight_in_reporting, 0) + COALESCE(r.customs_duty_reporting, 0)
                WHEN 'APAC' THEN COALESCE(r.recognised_spend_reporting, 0) + COALESCE(r.freight_in_reporting, 0) + COALESCE(r.customs_duty_reporting, 0) + COALESCE(r.price_variance_reporting, 0)
                ELSE COALESCE(r.recognised_spend_reporting, 0) + COALESCE(r.freight_in_reporting, 0) END
                                                      AS landed_cost_reporting,
           CASE g.region_code WHEN 'EU' THEN 'CIF' WHEN 'APAC' THEN 'DDP' ELSE 'FOB' END AS landed_cost_basis_code,
           COALESCE(r.on_time_receipt_count, 0)       AS on_time_receipt_count,
           {_pct('r.on_time_receipt_count', 'r.receipt_count')}   AS on_time_percent,
           {_pct('r.in_full_receipt_count', 'r.receipt_count')}   AS in_full_percent,
           {_pct('r.otif_receipt_count', 'r.receipt_count')}      AS on_time_in_full_percent,
           r.average_days_late, r.average_lead_time_days, r.lead_time_variability_days,
           COALESCE(r.rejected_quantity, 0)           AS rejected_quantity,
           {_pct('r.rejected_quantity', 'r.received_quantity')}   AS quality_reject_rate_percent,
           COALESCE(r.match_exception_count, 0)       AS match_exception_count,
           COALESCE(r.price_variance_reporting, 0)    AS price_variance_reporting,
           COALESCE(pay.discount_captured_reporting, 0) AS discount_captured_reporting,
           COALESCE(pay.discount_lost_reporting, 0)   AS discount_lost_reporting,
           pay.average_days_beyond_terms
    FROM grain AS g
    LEFT JOIN purchase AS p ON p.calendar_month = g.calendar_month AND p.supplier_key = g.supplier_key
                           AND COALESCE(p.product_category_key, -1) = COALESCE(g.product_category_key, -1) AND p.region_code = g.region_code
    LEFT JOIN receipt AS r  ON r.calendar_month = g.calendar_month AND r.supplier_key = g.supplier_key
                           AND COALESCE(r.product_category_key, -1) = COALESCE(g.product_category_key, -1) AND r.region_code = g.region_code
    LEFT JOIN payment AS pay ON pay.calendar_month = g.calendar_month AND pay.supplier_key = g.supplier_key AND pay.region_code = g.region_code
),
scored AS (
    SELECT b.*,
           SUM(b.recognised_spend_reporting) OVER (
               PARTITION BY b.supplier_key, b.product_category_key, b.region_code,
                            CASE WHEN b.region_code = 'EU' AND MONTH(b.calendar_month) < 4 THEN YEAR(b.calendar_month) - 1 ELSE YEAR(b.calendar_month) END
               ORDER BY b.calendar_month ROWS UNBOUNDED PRECEDING)                       AS year_to_date_spend_reporting,
           CASE WHEN b.receipt_count = 0 THEN NULL
                WHEN b.on_time_in_full_percent >= 95 AND COALESCE(b.quality_reject_rate_percent, 0) <= 1 THEN 'A'
                WHEN b.on_time_in_full_percent >= 85 THEN 'B'
                WHEN b.on_time_in_full_percent >= 70 THEN 'C'
                ELSE 'D' END                                                              AS scorecard_rating_code,
           RANK() OVER (PARTITION BY b.calendar_month, b.region_code, b.product_category_key
                        ORDER BY b.recognised_spend_reporting DESC)                       AS rank_in_category_by_spend
    FROM base AS b
)
SELECT
    s.calendar_month, s.supplier_key, s.product_category_key, s.region_code, s.vendor_contract_key,
    s.purchase_order_count, s.purchase_line_count, s.receipt_count, s.committed_spend_reporting, s.recognised_spend_reporting,
    s.year_to_date_spend_reporting, s.contract_covered_spend, s.maverick_spend_reporting, s.freight_in_reporting,
    s.customs_duty_reporting, s.landed_cost_reporting, s.landed_cost_basis_code, s.on_time_receipt_count, s.on_time_percent,
    s.in_full_percent, s.on_time_in_full_percent, s.average_days_late, s.average_lead_time_days, s.lead_time_variability_days,
    s.rejected_quantity, s.quality_reject_rate_percent, s.match_exception_count, s.price_variance_reporting,
    s.discount_captured_reporting, s.discount_lost_reporting, s.average_days_beyond_terms, s.scorecard_rating_code,
    s.rank_in_category_by_spend
FROM scored AS s
WHERE s.calendar_month BETWEEN {_d(monthStart(window.fromDate))} AND {_d(window.toDate)}
"""


# --------------------------------------------------------------------------
# AGG_Refresh_RegionalSalesPerformance (Integration.usp_RefreshAggregateRegionalSales)
# --------------------------------------------------------------------------
def regionalSalesPerformanceSql(t: Tables, window: RefreshWindow, reportingCurrency: str) -> str:
    # Prior-year and year-to-date comparisons need the preceding 23 months.
    readFrom = addMonths(monthStart(window.fromDate), -23)
    return f"""
WITH sale AS (
    SELECT
        TRUNC(f.invoice_date_key, 'MM')                        AS calendar_month,
        f.region_code, f.sales_territory_key, f.sales_channel_key,
        MAX({REGIONAL_FISCAL_YEAR})                            AS fiscal_year,
        MAX({REGIONAL_FISCAL_PERIOD})                          AS fiscal_period,
        MAX({REGIONAL_FISCAL_CALENDAR})                        AS fiscal_calendar_code,
        CASE f.region_code WHEN 'EU' THEN 'EUR' WHEN 'APAC' THEN 'AUD' ELSE 'USD' END AS local_currency_code,
        COUNT(DISTINCT f.order_number)                         AS order_count,
        COUNT(DISTINCT f.invoice_number)                       AS invoice_count,
        COUNT(DISTINCT f.customer_key)                         AS active_customer_count,
        COUNT(DISTINCT f.salesperson_key)                      AS active_salesperson_count,
        SUM(f.net_amount)                                      AS net_sales_local,
        SUM(f.net_amount_reporting)                            AS net_sales_daily_rate,
        SUM(f.net_amount * COALESCE(fx.monthly_average_rate, 1.0)) AS net_sales_monthly_average_rate,
        SUM(f.gross_margin_amount * COALESCE(f.fx_rate_to_reporting, 1.0)) AS gross_margin_reporting,
        SUM(f.gross_margin_amount)                             AS gross_margin_local,
        SUM(CASE WHEN f.region_code = 'NA' THEN f.tax_amount ELSE 0 END)                                            AS sales_tax_collected,
        SUM(CASE WHEN f.region_code = 'EU' AND COALESCE(f.tax_regime_code, 'STD') = 'STD' THEN f.tax_amount ELSE 0 END) AS vat_output_amount,
        SUM(CASE WHEN f.region_code = 'EU' AND f.tax_regime_code = 'RC' THEN f.tax_amount ELSE 0 END)                AS vat_reverse_charge_amount,
        SUM(CASE WHEN f.region_code = 'APAC' AND COALESCE(f.tax_regime_code, 'GST') <> 'GSTFREE' THEN f.tax_amount ELSE 0 END) AS gst_collected,
        SUM(CASE WHEN f.region_code = 'APAC' AND f.tax_regime_code = 'GSTFREE' THEN f.net_amount ELSE 0 END)          AS gst_free_sales
    FROM {t['fact_sale']} AS f
    INNER JOIN {t['dim_date']} AS d ON d.date = f.invoice_date_key
    LEFT JOIN {t['ref_fx_rate_monthly']} AS fx
        ON fx.currency_code = f.transaction_currency_code
       AND fx.rate_month = TRUNC(f.invoice_date_key, 'MM')
       AND fx.reporting_currency_code = '{reportingCurrency}'
    WHERE f.invoice_date_key BETWEEN {_d(readFrom)} AND {_d(window.toDate)}
      AND {ACTIVE_SALE_FILTER}
    GROUP BY TRUNC(f.invoice_date_key, 'MM'), f.region_code, f.sales_territory_key, f.sales_channel_key
),
budget AS (
    SELECT b.budget_month AS calendar_month, b.region_code, b.sales_territory_key, b.sales_channel_key,
           SUM(b.budget_net_sales_reporting) AS budget_net_sales_reporting
    FROM {t['ref_sales_budget']} AS b
    GROUP BY b.budget_month, b.region_code, b.sales_territory_key, b.sales_channel_key
),
enriched AS (
    SELECT s.*,
           s.net_sales_daily_rate - s.net_sales_monthly_average_rate         AS translation_difference,
           {_pct('s.gross_margin_local', 's.net_sales_local', 2)}            AS margin_percent,
           bud.budget_net_sales_reporting,
           LAG(s.net_sales_daily_rate, 12) OVER (PARTITION BY s.region_code, s.sales_territory_key, s.sales_channel_key ORDER BY s.calendar_month)
                                                                             AS prior_year_net_sales,
           SUM(s.net_sales_daily_rate) OVER (PARTITION BY s.region_code, s.sales_territory_key, s.sales_channel_key, s.fiscal_year
                                             ORDER BY s.calendar_month ROWS UNBOUNDED PRECEDING)
                                                                             AS year_to_date_net_sales
    FROM sale AS s
    LEFT JOIN budget AS bud ON bud.calendar_month = s.calendar_month AND bud.region_code = s.region_code
                           AND bud.sales_territory_key = s.sales_territory_key
                           AND COALESCE(bud.sales_channel_key, -1) = COALESCE(s.sales_channel_key, -1)
)
SELECT
    e.fiscal_year, e.fiscal_period, e.calendar_month, e.region_code, e.sales_territory_key, e.sales_channel_key,
    e.fiscal_calendar_code, e.local_currency_code, e.order_count, e.invoice_count, e.active_customer_count,
    e.active_salesperson_count, e.net_sales_local, e.net_sales_daily_rate, e.net_sales_monthly_average_rate,
    e.translation_difference, e.gross_margin_reporting, e.margin_percent, e.sales_tax_collected, e.vat_output_amount,
    e.vat_reverse_charge_amount, e.gst_collected, e.gst_free_sales, e.budget_net_sales_reporting,
    e.net_sales_daily_rate - e.budget_net_sales_reporting                     AS budget_variance_reporting,
    {_pct('e.net_sales_daily_rate', 'e.budget_net_sales_reporting')}          AS budget_attainment_percent,
    e.prior_year_net_sales,
    {_pct('(e.net_sales_daily_rate - e.prior_year_net_sales)', 'e.prior_year_net_sales')} AS year_over_year_percent,
    e.year_to_date_net_sales,
    RANK() OVER (PARTITION BY e.calendar_month, e.region_code ORDER BY e.net_sales_daily_rate DESC) AS rank_in_region_by_sales
FROM enriched AS e
WHERE e.calendar_month BETWEEN {_d(monthStart(window.fromDate))} AND {_d(window.toDate)}
"""


# --------------------------------------------------------------------------
# AGG_Refresh_FinanceCloseSummary (Integration.usp_RefreshAggregateFinanceClose)
# --------------------------------------------------------------------------
def financeCloseSummarySql(t: Tables, window: RefreshWindow, materialityAmount: float) -> str:
    return f"""
WITH periods AS (
    SELECT DISTINCT gl.fiscal_year, gl.fiscal_period
    FROM {t['fact_gl_posting']} AS gl
    WHERE {window.sqlLiteral('gl.posting_date_key')}
),
gl AS (
    SELECT gl.fiscal_year, gl.fiscal_period, gl.legal_entity_code, gl.region_code, a.account_group_code,
           MAX(gl.ledger_currency_code)                                 AS ledger_currency_code,
           MAX(gl.period_end_date)                                      AS period_end_date,
           SUM(gl.debit_amount_local)                                   AS period_debits_local,
           SUM(gl.credit_amount_local)                                  AS period_credits_local,
           MAX(gl.consolidation_rate)                                   AS consolidation_rate,
           COUNT(DISTINCT CASE WHEN gl.manual_journal_flag THEN gl.journal_number END) AS manual_journal_count,
           SUM(CASE WHEN gl.manual_journal_flag THEN ABS(gl.debit_amount_local - gl.credit_amount_local) ELSE 0 END) AS manual_journal_value,
           SUM(CASE WHEN gl.posting_date_key > gl.period_end_date THEN 1 ELSE 0 END) AS late_posting_count,
           SUM(CASE WHEN NOT COALESCE(gl.posted_flag, TRUE) THEN 1 ELSE 0 END)     AS unposted_journal_count,
           MAX(gl.close_status_code)                                    AS close_status_code,
           MAX(gl.close_completed_datetime)                             AS close_completed_datetime
    FROM {t['fact_gl_posting']} AS gl
    INNER JOIN periods AS p ON p.fiscal_year = gl.fiscal_year AND p.fiscal_period = gl.fiscal_period
    INNER JOIN {t['dim_gl_account']} AS a ON a.gl_account_key = gl.gl_account_key
    GROUP BY gl.fiscal_year, gl.fiscal_period, gl.legal_entity_code, gl.region_code, a.account_group_code
),
opening AS (
    SELECT gl.fiscal_year, gl.fiscal_period, gl.legal_entity_code, gl.region_code, a.account_group_code,
           SUM(gl.debit_amount_local - gl.credit_amount_local) AS opening_balance_local
    FROM {t['fact_gl_posting']} AS gl
    INNER JOIN {t['dim_gl_account']} AS a ON a.gl_account_key = gl.gl_account_key
    INNER JOIN periods AS p ON gl.fiscal_year * 100 + gl.fiscal_period < p.fiscal_year * 100 + p.fiscal_period
    GROUP BY gl.fiscal_year, gl.fiscal_period, gl.legal_entity_code, gl.region_code, a.account_group_code
),
ar AS (
    SELECT ar.fiscal_year, ar.fiscal_period, ar.legal_entity_code, ar.region_code,
           SUM(ar.balance_reporting) AS ar_balance_reporting,
           SUM(COALESCE(ar.bad_debt_provision_reporting, 0)) AS bad_debt_provision_reporting
    FROM {t['fact_monthly_ar_aging']} AS ar
    GROUP BY ar.fiscal_year, ar.fiscal_period, ar.legal_entity_code, ar.region_code
),
ap AS (
    SELECT ap.fiscal_year, ap.fiscal_period, ap.legal_entity_code, ap.region_code,
           SUM(ap.balance_reporting) AS ap_balance_reporting,
           SUM(COALESCE(ap.grni_accrual_reporting, 0)) AS grni_accrual_reporting
    FROM {t['fact_monthly_ap_aging']} AS ap
    GROUP BY ap.fiscal_year, ap.fiscal_period, ap.legal_entity_code, ap.region_code
),
base AS (
    SELECT g.*,
           COALESCE(o.opening_balance_local, 0)                                                       AS opening_balance_local,
           COALESCE(o.opening_balance_local, 0) + g.period_debits_local - g.period_credits_local      AS closing_balance_local,
           CASE g.account_group_code WHEN 'AR' THEN ar.ar_balance_reporting WHEN 'AP' THEN ap.ap_balance_reporting ELSE NULL END
                                                                                                      AS sub_ledger_balance_reporting,
           CASE g.region_code WHEN 'EU' THEN LEAST({materialityAmount}, 1000.0)
                              WHEN 'APAC' THEN LEAST({materialityAmount}, 2500.0)
                              ELSE {materialityAmount} END                                            AS tolerance_amount,
           ar.ar_balance_reporting, ap.ap_balance_reporting, ap.grni_accrual_reporting, ar.bad_debt_provision_reporting
    FROM gl AS g
    LEFT JOIN opening AS o ON o.fiscal_year = g.fiscal_year AND o.fiscal_period = g.fiscal_period AND o.legal_entity_code = g.legal_entity_code
                          AND o.region_code = g.region_code AND o.account_group_code = g.account_group_code
    LEFT JOIN ar ON ar.fiscal_year = g.fiscal_year AND ar.fiscal_period = g.fiscal_period AND ar.legal_entity_code = g.legal_entity_code AND ar.region_code = g.region_code
    LEFT JOIN ap ON ap.fiscal_year = g.fiscal_year AND ap.fiscal_period = g.fiscal_period AND ap.legal_entity_code = g.legal_entity_code AND ap.region_code = g.region_code
)
SELECT
    b.fiscal_year, b.fiscal_period, b.legal_entity_code, b.region_code, b.account_group_code, b.ledger_currency_code,
    b.period_end_date, b.opening_balance_local, b.period_debits_local, b.period_credits_local, b.closing_balance_local,
    ROUND(b.closing_balance_local * COALESCE(b.consolidation_rate, 1.0), 2)                          AS closing_balance_reporting,
    b.consolidation_rate, b.sub_ledger_balance_reporting,
    CASE WHEN b.sub_ledger_balance_reporting IS NULL THEN NULL
         ELSE ROUND(b.sub_ledger_balance_reporting - b.closing_balance_local * COALESCE(b.consolidation_rate, 1.0), 2) END
                                                                                                     AS sub_ledger_to_gl_difference,
    b.tolerance_amount,
    CASE WHEN b.sub_ledger_balance_reporting IS NULL THEN TRUE
         ELSE ABS(b.sub_ledger_balance_reporting - b.closing_balance_local * COALESCE(b.consolidation_rate, 1.0)) <= b.tolerance_amount END
                                                                                                     AS within_tolerance_flag,
    b.manual_journal_count, b.manual_journal_value, b.late_posting_count, b.unposted_journal_count,
    b.ar_balance_reporting, b.ap_balance_reporting, b.grni_accrual_reporting, b.bad_debt_provision_reporting,
    b.close_status_code, b.close_completed_datetime,
    CASE WHEN b.close_completed_datetime IS NULL THEN NULL ELSE DATEDIFF(CAST(b.close_completed_datetime AS DATE), b.period_end_date) END
                                                                                                     AS days_to_close
FROM base AS b
"""


# --------------------------------------------------------------------------
# AGG_Refresh_PromotionEffectiveness (Integration.usp_RefreshAggregatePromotionEffectiveness)
# --------------------------------------------------------------------------
def promotionEffectivenessSql(t: Tables, window: RefreshWindow, baselineWeeks: int) -> str:
    return f"""
WITH promo AS (
    SELECT p.promotion_key, p.promotion_code, p.region_code, p.start_date AS promotion_start_date, p.end_date AS promotion_end_date,
           DATE_SUB(p.start_date, 1 + DATEDIFF(p.end_date, p.start_date))  AS baseline_start_date,
           DATE_SUB(p.start_date, 1)                                        AS baseline_end_date,
           DATE_SUB(p.start_date, 7 * {baselineWeeks})                      AS baseline_weeks_start_date
    FROM {t['dim_promotion']} AS p
    WHERE p.promotion_key > 0
      AND p.end_date >= {_d(window.fromDate)} AND p.start_date <= {_d(window.toDate)}
),
promoted AS (
    SELECT f.promotion_key, si.product_category_key, f.sales_channel_key, f.region_code,
           COUNT(DISTINCT f.customer_key)                                   AS participating_customer_count,
           SUM(f.quantity_base_uom)                                         AS promoted_units_sold,
           SUM(f.net_amount_reporting)                                      AS promotion_revenue_reporting,
           SUM(f.line_discount_amount * COALESCE(f.fx_rate_to_reporting, 1.0)) AS discount_cost_reporting,
           SUM(f.gross_margin_amount * COALESCE(f.fx_rate_to_reporting, 1.0)) AS promotion_margin_reporting
    FROM {t['fact_sale']} AS f
    INNER JOIN promo AS p ON p.promotion_key = f.promotion_key
    LEFT JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = f.stock_item_key
    WHERE f.invoice_date_key BETWEEN p.promotion_start_date AND p.promotion_end_date AND {ACTIVE_SALE_FILTER}
    GROUP BY f.promotion_key, si.product_category_key, f.sales_channel_key, f.region_code
),
baseline AS (
    SELECT p.promotion_key, si.product_category_key, f.sales_channel_key, f.region_code,
           SUM(f.net_amount_reporting)                                      AS baseline_revenue_reporting,
           SUM(f.gross_margin_amount * COALESCE(f.fx_rate_to_reporting, 1.0)) AS baseline_margin_reporting
    FROM {t['fact_sale']} AS f
    INNER JOIN promo AS p ON p.region_code = f.region_code
    LEFT JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = f.stock_item_key
    WHERE f.invoice_date_key BETWEEN p.baseline_start_date AND p.baseline_end_date AND {ACTIVE_SALE_FILTER}
      AND EXISTS (SELECT 1 FROM {t['fact_sale']} AS ps
                  WHERE ps.promotion_key = p.promotion_key AND ps.stock_item_key = f.stock_item_key)
    GROUP BY p.promotion_key, si.product_category_key, f.sales_channel_key, f.region_code
),
customer_history AS (
    SELECT f.promotion_key, f.customer_key, MIN(f.invoice_date_key) AS first_promo_order_date
    FROM {t['fact_sale']} AS f
    INNER JOIN promo AS p ON p.promotion_key = f.promotion_key
    WHERE f.invoice_date_key BETWEEN p.promotion_start_date AND p.promotion_end_date AND {ACTIVE_SALE_FILTER}
    GROUP BY f.promotion_key, f.customer_key
),
customer_class AS (
    SELECT ch.promotion_key,
           SUM(CASE WHEN prior.customer_key IS NULL THEN 1 ELSE 0 END)            AS new_customer_count,
           SUM(CASE WHEN prior.customer_key IS NOT NULL AND prior.last_order_date < DATE_SUB(ch.first_promo_order_date, 180) THEN 1 ELSE 0 END)
                                                                                  AS reactivated_customer_count
    FROM customer_history AS ch
    LEFT JOIN (
        SELECT f.customer_key, ch2.promotion_key, MAX(f.invoice_date_key) AS last_order_date
        FROM {t['fact_sale']} AS f
        INNER JOIN customer_history AS ch2 ON ch2.customer_key = f.customer_key
        WHERE f.invoice_date_key < ch2.first_promo_order_date AND {ACTIVE_SALE_FILTER}
        GROUP BY f.customer_key, ch2.promotion_key
    ) AS prior ON prior.customer_key = ch.customer_key AND prior.promotion_key = ch.promotion_key
    GROUP BY ch.promotion_key
),
eligible AS (
    SELECT e.promotion_key, e.region_code,
           COUNT(DISTINCT e.customer_key)                                                           AS eligible_customer_count,
           COUNT(DISTINCT CASE WHEN e.region_code = 'EU' AND NOT COALESCE(e.marketing_consent_flag, FALSE) THEN e.customer_key END)
                                                                                                    AS consent_restricted_count
    FROM {t['fact_promotion_eligibility']} AS e
    GROUP BY e.promotion_key, e.region_code
),
ret AS (
    SELECT r.promotion_key, si.product_category_key, r.region_code, SUM(r.quantity_returned) AS units_returned
    FROM {t['fact_return']} AS r
    LEFT JOIN {t['dim_stock_item']} AS si ON si.stock_item_key = r.stock_item_key
    WHERE r.promotion_key > 0
    GROUP BY r.promotion_key, si.product_category_key, r.region_code
),
loyalty AS (
    SELECT lp.promotion_key, SUM(CASE WHEN lp.movement_type_code IN ('EARN', 'BONUS') THEN lp.points_delta ELSE 0 END) AS loyalty_points_issued
    FROM {t['fact_loyalty_points']} AS lp WHERE lp.promotion_key > 0 GROUP BY lp.promotion_key
)
SELECT
    p.promotion_key, pr.product_category_key, pr.sales_channel_key, p.region_code, p.promotion_code,
    p.promotion_start_date, p.promotion_end_date, p.baseline_start_date, p.baseline_end_date,
    COALESCE(el.eligible_customer_count, 0)                                                  AS eligible_customer_count,
    pr.participating_customer_count,
    {_pct('pr.participating_customer_count', 'el.eligible_customer_count')}                  AS take_up_rate_percent,
    GREATEST(COALESCE(el.eligible_customer_count, 0) - pr.participating_customer_count, 0)  AS eligible_not_purchased_count,
    COALESCE(cc.new_customer_count, 0)                                                       AS new_customer_count,
    COALESCE(cc.reactivated_customer_count, 0)                                               AS reactivated_customer_count,
    pr.promoted_units_sold,
    COALESCE(b.baseline_revenue_reporting, 0)                                                AS baseline_revenue_reporting,
    pr.promotion_revenue_reporting,
    pr.promotion_revenue_reporting - COALESCE(b.baseline_revenue_reporting, 0)               AS incremental_revenue_reporting,
    pr.discount_cost_reporting,
    pr.promotion_margin_reporting,
    COALESCE(b.baseline_margin_reporting, 0)                                                 AS baseline_margin_reporting,
    pr.promotion_margin_reporting - COALESCE(b.baseline_margin_reporting, 0)                 AS incremental_margin_reporting,
    GREATEST(COALESCE(b.baseline_revenue_reporting, 0) - pr.promotion_revenue_reporting, 0) AS cannibalised_revenue,
    {_pct('COALESCE(ret.units_returned, 0)', 'pr.promoted_units_sold')}                      AS return_rate_percent,
    COALESCE(l.loyalty_points_issued, 0)                                                     AS loyalty_points_issued,
    {_pct('(pr.promotion_margin_reporting - COALESCE(b.baseline_margin_reporting, 0))', 'pr.discount_cost_reporting')}
                                                                                             AS roi_percent,
    (pr.promotion_margin_reporting - COALESCE(b.baseline_margin_reporting, 0)) >= pr.discount_cost_reporting AS payback_achieved_flag,
    COALESCE(el.consent_restricted_count, 0) > 0                                             AS consent_restricted_flag
FROM promoted AS pr
INNER JOIN promo AS p ON p.promotion_key = pr.promotion_key
LEFT JOIN baseline AS b ON b.promotion_key = pr.promotion_key AND COALESCE(b.product_category_key, -1) = COALESCE(pr.product_category_key, -1)
                        AND COALESCE(b.sales_channel_key, -1) = COALESCE(pr.sales_channel_key, -1) AND b.region_code = pr.region_code
LEFT JOIN customer_class AS cc ON cc.promotion_key = pr.promotion_key
LEFT JOIN eligible AS el ON el.promotion_key = pr.promotion_key AND el.region_code = pr.region_code
LEFT JOIN ret ON ret.promotion_key = pr.promotion_key AND COALESCE(ret.product_category_key, -1) = COALESCE(pr.product_category_key, -1) AND ret.region_code = pr.region_code
LEFT JOIN loyalty AS l ON l.promotion_key = pr.promotion_key
"""


# --------------------------------------------------------------------------
# AGG_Refresh_DeliveryPerformanceSummary (Integration.usp_RefreshAggregateDeliveryPerformance)
# --------------------------------------------------------------------------
def deliveryPerformanceSummarySql(t: Tables, window: RefreshWindow, onTimeGraceHours: float) -> str:
    return f"""
WITH ship AS (
    SELECT
        DATE_SUB(f.delivered_date_key, (DAYOFWEEK(f.delivered_date_key) + 5) % 7) AS iso_week_start_date,
        f.carrier_key, f.warehouse_site_key, f.sales_territory_key, f.region_code, f.service_level_code,
        COUNT(DISTINCT f.consignment_number)                                     AS consignment_count,
        SUM(COALESCE(f.package_count, 1))                                        AS package_count,
        SUM(CASE WHEN f.delivered_date_key IS NOT NULL THEN 1 ELSE 0 END)        AS delivered_count,
        SUM(CASE WHEN f.delivered_datetime <= f.promised_datetime + INTERVAL {int(onTimeGraceHours * 60)} MINUTES THEN 1 ELSE 0 END)
                                                                                 AS on_time_count,
        SUM(CASE WHEN f.delivered_datetime >  f.promised_datetime + INTERVAL {int(onTimeGraceHours * 60)} MINUTES THEN 1 ELSE 0 END)
                                                                                 AS late_count,
        SUM(COALESCE(f.failed_attempt_count, 0))                                 AS failed_attempt_count,
        SUM(CASE WHEN f.damaged_flag THEN 1 ELSE 0 END)                          AS damaged_count,
        SUM(CASE WHEN f.lost_flag THEN 1 ELSE 0 END)                             AS lost_count,
        AVG(CAST(f.dispatch_to_delivery_days AS DECIMAL(9, 2)))                  AS average_transit_days,
        AVG(CAST(f.customs_hold_days AS DECIMAL(9, 2)))                          AS average_customs_hold_days,
        AVG(CAST(f.pick_to_despatch_days AS DECIMAL(9, 2)))                      AS average_pick_to_despatch_days,
        SUM(f.shipment_weight_kg)                                                AS total_weight_kg,
        SUM(COALESCE(f.chargeable_weight_kg, f.shipment_weight_kg))              AS chargeable_weight_kg,
        SUM(f.freight_charge_reporting)                                          AS freight_cost_reporting,
        SUM(COALESCE(f.fuel_surcharge_reporting, 0))                             AS fuel_surcharge_reporting,
        SUM(COALESCE(f.duty_and_clearance_reporting, 0))                         AS duty_and_clearance_reporting
    FROM {t['fact_shipment']} AS f
    WHERE {window.sqlLiteral('f.delivered_date_key')}
    GROUP BY DATE_SUB(f.delivered_date_key, (DAYOFWEEK(f.delivered_date_key) + 5) % 7),
             f.carrier_key, f.warehouse_site_key, f.sales_territory_key, f.region_code, f.service_level_code
),
rated AS (
    SELECT s.*,
           {_pct('s.on_time_count', 's.delivered_count')}                                         AS on_time_percent,
           {_pct('(s.delivered_count - s.failed_attempt_count)', 's.delivered_count')}            AS first_attempt_success_percent,
           {_pct('s.damaged_count', 's.consignment_count')}                                       AS damage_rate_percent,
           {_ratio('s.freight_cost_reporting', 's.consignment_count')}                            AS cost_per_consignment,
           {_ratio('s.freight_cost_reporting', 's.chargeable_weight_kg')}                         AS cost_per_chargeable_kg,
           CASE s.region_code WHEN 'EU' THEN 97.0 WHEN 'APAC' THEN 90.0 ELSE 95.0 END              AS sla_target_percent
    FROM ship AS s
)
SELECT
    r.iso_week_start_date, r.carrier_key, r.warehouse_site_key, r.sales_territory_key, r.region_code, r.service_level_code,
    r.consignment_count, r.package_count, r.delivered_count, r.on_time_count, r.late_count, r.failed_attempt_count,
    r.damaged_count, r.lost_count, r.on_time_percent, r.first_attempt_success_percent, r.damage_rate_percent,
    r.average_transit_days, r.average_customs_hold_days, r.average_pick_to_despatch_days, r.total_weight_kg,
    r.chargeable_weight_kg, r.freight_cost_reporting, r.fuel_surcharge_reporting, r.duty_and_clearance_reporting,
    r.cost_per_consignment, r.cost_per_chargeable_kg, r.sla_target_percent,
    COALESCE(r.on_time_percent < r.sla_target_percent, FALSE)                                    AS sla_breach_flag,
    CASE WHEN r.region_code = 'EU' AND r.on_time_percent < r.sla_target_percent
         THEN ROUND(r.freight_cost_reporting * 0.05, 2) ELSE 0 END                               AS service_credit_reporting
FROM rated AS r
"""


AGGREGATE_KEY_COLUMNS: Dict[str, Sequence[str]] = {
    "agg_daily_sales_summary": ("sales_date", "stock_item_key", "sales_territory_key", "sales_channel_key", "region_code"),
    "agg_daily_inventory_health": ("snapshot_date", "warehouse_site_key", "product_category_key", "region_code"),
    "agg_monthly_sales_summary": ("calendar_month", "customer_key", "sales_territory_key", "customer_segment_key", "sales_channel_key", "region_code"),
    "agg_monthly_margin_analysis": ("calendar_month", "product_category_key", "sales_territory_key", "sales_channel_key", "region_code"),
    "agg_customer_360": ("customer_key",),
    "agg_customer_rolling_12_month": ("customer_key", "calendar_month", "region_code"),
    "agg_product_performance": ("calendar_month", "stock_item_key", "region_code"),
    "agg_supplier_performance": ("calendar_month", "supplier_key", "product_category_key", "region_code"),
    "agg_regional_sales_performance": ("calendar_month", "region_code", "sales_territory_key", "sales_channel_key"),
    "agg_finance_close_summary": ("fiscal_year", "fiscal_period", "legal_entity_code", "region_code", "account_group_code"),
    "agg_promotion_effectiveness": ("promotion_key", "product_category_key", "sales_channel_key", "region_code"),
    "agg_delivery_performance_summary": ("iso_week_start_date", "carrier_key", "warehouse_site_key", "sales_territory_key", "region_code", "service_level_code"),
}
