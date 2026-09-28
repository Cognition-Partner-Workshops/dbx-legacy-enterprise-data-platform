"""Gold aggregate tables replacing the legacy ``Aggregate.*`` refresh procedures.

Legacy artifact                                        -> gold table
Integration.usp_RefreshAggregateDailySales             -> agg_daily_sales
Integration.usp_RefreshAggregateMonthlySales           -> agg_monthly_sales
Integration.usp_RefreshAggregateRegionalSales          -> agg_regional_sales_performance
Integration.usp_RefreshAggregateCustomer360            -> agg_customer_360 + agg_customer_rolling_12_month
Integration.usp_RefreshAggregateProductPerformance     -> agg_product_performance   (needed by rpt_sales_by_product_month)
Integration.usp_RefreshAggregateMarginAnalysis         -> agg_monthly_margin_analysis (needed by rpt_margin_by_product_category)

Refresh semantics
-----------------
The legacy procedures delete a trailing window / one calendar month and
re-insert it.  The window exists only to bound runtime on SQL Server: every
row is a deterministic function of the facts, so the lakehouse rebuilds each
table in full with ``overwriteTable`` (idempotent on re-run).  The one piece
of state that is not derivable from the facts is ``period_closed_flag`` on
agg_monthly_sales (legacy skips closed periods on refresh): closed rows are
carried over from the previous version of the table and open rows rebuilt.
Customer 360 is TRUNCATE + rebuild in the legacy too.
"""
from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import overwriteTable, readTable, tableExists
from sales_lakehouse.gold.inputs import (
    DIM_STOCK_ITEM_SCHEMA,
    FACT_CUSTOMER_BALANCE_SCHEMA,
    FACT_LOYALTY_POINTS_SCHEMA,
    FACT_RETURN_SCHEMA,
    FACT_WEB_SESSION_SCHEMA,
    LEGACY_FISCAL_CALENDAR_LABEL,
    LEGACY_REGION_CURRENCY,
    SALES_BUDGET_SCHEMA,
    monthIndex,
    monthlyAverageFxRate,
    notReversal,
    readDimCustomer,
    readOrEmpty,
    safeDivide,
    safePercent,
    withNaFiscalPeriod,
)

AGG_DAILY_SALES = "agg_daily_sales"
AGG_MONTHLY_SALES = "agg_monthly_sales"
AGG_REGIONAL_SALES_PERFORMANCE = "agg_regional_sales_performance"
AGG_CUSTOMER_360 = "agg_customer_360"
AGG_CUSTOMER_ROLLING_12_MONTH = "agg_customer_rolling_12_month"
AGG_PRODUCT_PERFORMANCE = "agg_product_performance"
AGG_MONTHLY_MARGIN_ANALYSIS = "agg_monthly_margin_analysis"

PRIOR_YEAR_DAY_OFFSET = 364  # LEGACY QUIRK: same weekday last year, not 365 days


def _labelCase(regionCol: str, mapping: dict[str, str], default: str) -> F.Column:
    expr = F.lit(default)
    for code, label in mapping.items():
        expr = F.when(F.col(regionCol) == code, F.lit(label)).otherwise(expr)
    return expr


def _refreshColumns(df: DataFrame, batchId: int) -> DataFrame:
    return df.withColumn("refresh_batch_id", F.lit(batchId).cast("bigint")).withColumn(
        "refreshed_datetime", F.current_timestamp()
    )


# --------------------------------------------------------------------------- daily
def buildDailySales(
    sale: DataFrame, ret: DataFrame, stockItem: DataFrame, fiscalCalendar: DataFrame, batchId: int
) -> DataFrame:
    """Aggregate.Daily Sales Summary: sales date x stock item x territory x channel (x region).

    # LEGACY QUIRK: the daily proc does NOT filter reversal corrections (the
    # monthly/regional/customer procs do), so REV rows net against their
    # originals inside the day instead of being excluded.
    """
    items = stockItem.select("stock_item_key", F.col("product_category_key").alias("item_category_key"))
    s = sale.join(items, "stock_item_key", "left")
    s = withNaFiscalPeriod(s, "invoice_date_key", fiscalCalendar)
    grp = s.groupBy(
        F.col("invoice_date_key").alias("sales_date"),
        "stock_item_key",
        F.coalesce(F.col("item_category_key"), F.lit(0)).alias("product_category_key"),
        "sales_territory_key",
        "sales_channel_key",
        "region_code",
    ).agg(
        F.max("na_fiscal_year").alias("fiscal_year"),
        F.max("na_fiscal_period").alias("fiscal_period"),
        F.countDistinct("invoice_number").alias("invoice_count"),
        F.count(F.lit(1)).alias("line_count"),
        F.countDistinct("customer_key").alias("distinct_customer_count"),
        F.sum("quantity_base_uom").alias("quantity_sold_base_uom"),
        F.sum("gross_amount").alias("gross_sales_amount"),
        F.sum("line_discount_amount").alias("line_discount_amount"),
        F.sum(
            F.when(F.col("promotion_key") > 0, F.col("line_discount_amount")).otherwise(F.lit(0))
        ).alias("promotion_discount_amount"),
        F.sum("net_amount").alias("net_sales_amount"),
        F.sum("tax_amount").alias("tax_amount"),
        F.sum(F.coalesce(F.col("freight_amount"), F.lit(0))).alias("freight_amount"),
        F.sum("cost_of_sale_amount").alias("cost_of_sales_amount"),
        F.sum("gross_margin_amount").alias("gross_margin_amount"),
        F.sum("net_amount_reporting").alias("net_sales_amount_reporting"),
        F.count(F.lit(1)).alias("source_row_count"),
    )
    grp = grp.withColumn(
        "margin_percent", safePercent(F.col("gross_margin_amount"), F.col("net_sales_amount"))
    )
    # LEGACY QUIRK: returns are looked up per (date, item, territory) only, so the
    # same returns amount repeats on every channel/region row of that key.
    returns = ret.groupBy(
        F.col("return_date_key").alias("sales_date"), "stock_item_key", "sales_territory_key"
    ).agg(F.sum(F.abs(F.col("net_credit_amount"))).alias("returns_amount"))
    grp = grp.join(returns, ["sales_date", "stock_item_key", "sales_territory_key"], "left").withColumn(
        "returns_amount", F.coalesce(F.col("returns_amount"), F.lit(0))
    )
    priorYear = grp.groupBy(
        F.date_add(F.col("sales_date"), PRIOR_YEAR_DAY_OFFSET).alias("sales_date"),
        "stock_item_key",
        "sales_territory_key",
        "sales_channel_key",
    ).agg(F.sum("net_sales_amount").alias("prior_year_net_sales"))
    grp = grp.join(
        priorYear, ["sales_date", "stock_item_key", "sales_territory_key", "sales_channel_key"], "left"
    )
    grp = grp.withColumn(
        "prior_year_variance_percent",
        safePercent(
            F.col("net_sales_amount") - F.col("prior_year_net_sales"), F.col("prior_year_net_sales")
        ),
    )
    return _refreshColumns(grp, batchId).select(
        "sales_date", "stock_item_key", "product_category_key", "sales_territory_key",
        "sales_channel_key", "region_code", "fiscal_year", "fiscal_period", "invoice_count",
        "line_count", "distinct_customer_count", "quantity_sold_base_uom", "gross_sales_amount",
        "line_discount_amount", "promotion_discount_amount", "net_sales_amount", "tax_amount",
        "freight_amount", "cost_of_sales_amount", "gross_margin_amount", "margin_percent",
        "returns_amount", "net_sales_amount_reporting", "prior_year_net_sales",
        "prior_year_variance_percent", "refresh_batch_id", "refreshed_datetime", "source_row_count",
    )


# --------------------------------------------------------------------------- monthly
def buildMonthlySales(
    sale: DataFrame,
    creditNote: DataFrame,
    ret: DataFrame,
    fiscalCalendar: DataFrame,
    batchId: int,
    existing: DataFrame | None = None,
) -> DataFrame:
    """Aggregate.Monthly Sales Summary: fiscal period x customer x territory x segment x channel x region."""
    s = withNaFiscalPeriod(notReversal(sale), "invoice_date_key", fiscalCalendar).withColumn(
        "calendar_month", F.trunc(F.col("invoice_date_key"), "month")
    )
    grp = s.groupBy(
        "calendar_month", "customer_key", "sales_territory_key", "customer_segment_key",
        "sales_channel_key", "region_code",
    ).agg(
        F.max("na_fiscal_year").alias("fiscal_year"),
        F.max("na_fiscal_period").alias("fiscal_period"),
        F.countDistinct("order_number").alias("order_count"),
        F.countDistinct("invoice_number").alias("invoice_count"),
        F.sum("quantity_base_uom").alias("quantity_sold_base_uom"),
        F.sum("gross_amount").alias("gross_revenue"),
        F.sum("line_discount_amount").alias("discount_given"),
        F.sum("net_amount").alias("net_revenue"),
        # LEGACY QUIRK: EU reverse-charge rows report net of the (self-assessed) tax amount.
        F.sum(
            F.when(
                (F.col("region_code") == "EU") & (F.col("tax_regime_code") == "RC"),
                F.col("net_amount_reporting") - F.col("tax_amount"),
            ).otherwise(F.col("net_amount_reporting"))
        ).alias("net_revenue_reporting"),
        F.sum(F.col("cost_of_sale_amount") * F.col("fx_rate_to_reporting")).alias(
            "cost_of_sales_reporting"
        ),
        F.sum(F.col("gross_margin_amount") * F.col("fx_rate_to_reporting")).alias(
            "gross_margin_reporting"
        ),
    )
    grp = grp.withColumn(
        "fiscal_calendar_code", _labelCase("region_code", LEGACY_FISCAL_CALENDAR_LABEL, "JUL13")
    )
    # LEGACY QUIRK: credit notes / returns are joined per customer x month (MAX),
    # so a customer split over several territory/channel rows carries the full
    # customer total on each row.
    credits = creditNote.groupBy(
        "customer_key", F.trunc(F.col("credit_note_date_key"), "month").alias("calendar_month")
    ).agg(
        F.round(
            F.sum(F.col("credit_excluding_tax") * F.coalesce(F.col("fx_rate_to_reporting"), F.lit(1.0))), 2
        ).alias("credit_notes_reporting")
    )
    returns = ret.groupBy(
        "customer_key", F.trunc(F.col("return_date_key"), "month").alias("calendar_month")
    ).agg(
        F.count(F.lit(1)).alias("return_count"),
        F.sum(F.abs(F.col("net_credit_amount_reporting"))).alias("returns_reporting"),
    )
    grp = (
        grp.join(credits, ["customer_key", "calendar_month"], "left")
        .join(returns, ["customer_key", "calendar_month"], "left")
        .withColumn("credit_notes_reporting", F.coalesce(F.col("credit_notes_reporting"), F.lit(0)))
        .withColumn("returns_reporting", F.coalesce(F.col("returns_reporting"), F.lit(0)))
        .withColumn("return_count", F.coalesce(F.col("return_count"), F.lit(0)))
    )
    grp = grp.withColumn(
        "net_revenue_after_credits",
        F.col("net_revenue_reporting") - F.col("credit_notes_reporting") - F.col("returns_reporting"),
    ).withColumn(
        "average_order_value", safeDivide(F.col("net_revenue_reporting"), F.col("order_count"))
    )
    # Comparatives: the legacy UPDATE joins the customer's prior-month row
    # regardless of territory/channel; with several rows the pick is arbitrary.
    # Summing per customer x month makes the same lookup deterministic.
    custMonth = grp.groupBy("customer_key", "calendar_month").agg(
        F.sum("net_revenue_reporting").alias("cm_net")
    )

    def shifted(months: int, alias: str) -> DataFrame:
        return custMonth.select(
            "customer_key",
            F.add_months(F.col("calendar_month"), months).alias("calendar_month"),
            F.col("cm_net").alias(alias),
        )

    grp = (
        grp.join(shifted(1, "prior_period_net_revenue"), ["customer_key", "calendar_month"], "left")
        .join(shifted(2, "prior_period_2_net_revenue"), ["customer_key", "calendar_month"], "left")
        .join(shifted(12, "prior_year_net_revenue"), ["customer_key", "calendar_month"], "left")
    )
    grp = (
        grp.withColumn(
            "rolling_3_period_net_revenue",
            F.col("net_revenue_reporting")
            + F.coalesce(F.col("prior_period_net_revenue"), F.lit(0))
            + F.coalesce(F.col("prior_period_2_net_revenue"), F.lit(0)),
        )
        .withColumn(
            "period_over_period_percent",
            safePercent(
                F.col("net_revenue_reporting") - F.col("prior_period_net_revenue"),
                F.col("prior_period_net_revenue"),
            ),
        )
        .withColumn(
            "year_over_year_percent",
            safePercent(
                F.col("net_revenue_reporting") - F.col("prior_year_net_revenue"),
                F.col("prior_year_net_revenue"),
            ),
        )
        .withColumn("period_closed_flag", F.lit(False))
        .drop("prior_period_2_net_revenue")
    )
    out = _refreshColumns(grp, batchId).select(*MONTHLY_COLUMNS)
    if existing is not None:
        # LEGACY QUIRK: closed periods are never refreshed; keep the frozen rows.
        closed = existing.filter(F.col("period_closed_flag") == True).select(*MONTHLY_COLUMNS)  # noqa: E712
        closedKeys = closed.select("calendar_month", "region_code").distinct()
        out = out.join(closedKeys, ["calendar_month", "region_code"], "left_anti").unionByName(closed)
    return out


MONTHLY_COLUMNS = [
    "fiscal_year", "fiscal_period", "calendar_month", "customer_key", "sales_territory_key",
    "customer_segment_key", "sales_channel_key", "region_code", "fiscal_calendar_code",
    "order_count", "invoice_count", "return_count", "quantity_sold_base_uom", "gross_revenue",
    "discount_given", "net_revenue", "net_revenue_reporting", "credit_notes_reporting",
    "returns_reporting", "net_revenue_after_credits", "cost_of_sales_reporting",
    "gross_margin_reporting", "average_order_value", "prior_period_net_revenue",
    "prior_year_net_revenue", "rolling_3_period_net_revenue", "period_over_period_percent",
    "year_over_year_percent", "period_closed_flag", "refresh_batch_id", "refreshed_datetime",
]


def closeMonthlyPeriod(spark: SparkSession, cfg: PipelineConfig, calendarMonth: dt.date, regionCode: str) -> None:
    """Freeze one calendar month / region (legacy month-end close sets [Period Closed Flag])."""
    spark.sql(
        f"UPDATE {cfg.fqn('gold', AGG_MONTHLY_SALES)} SET period_closed_flag = true "
        f"WHERE calendar_month = DATE'{calendarMonth.isoformat()}' AND region_code = '{regionCode}'"
    )


# --------------------------------------------------------------------------- regional
def buildRegionalSalesPerformance(
    sale: DataFrame,
    fxRate: DataFrame,
    budget: DataFrame,
    fiscalCalendar: DataFrame,
    batchId: int,
    reportingCurrency: str = "USD",
) -> DataFrame:
    """Aggregate.Regional Sales Performance: fiscal period x territory x channel x region."""
    s = withNaFiscalPeriod(notReversal(sale), "invoice_date_key", fiscalCalendar).withColumn(
        "calendar_month", F.trunc(F.col("invoice_date_key"), "month")
    )
    monthlyFx = monthlyAverageFxRate(fxRate, reportingCurrency)
    s = s.join(
        monthlyFx,
        (s["transaction_currency_code"] == monthlyFx["fx_from_currency_code"])
        & (s["calendar_month"] == monthlyFx["fx_rate_month"]),
        "left",
    )
    regime = F.coalesce(F.col("tax_regime_code"), F.lit("STD"))
    isNa = F.col("region_code") == "NA"
    isEu = F.col("region_code") == "EU"
    isApac = F.col("region_code") == "APAC"
    grp = s.groupBy("calendar_month", "region_code", "sales_territory_key", "sales_channel_key").agg(
        F.max("na_fiscal_year").alias("fiscal_year"),
        F.max("na_fiscal_period").alias("fiscal_period"),
        F.countDistinct("order_number").alias("order_count"),
        F.countDistinct("invoice_number").alias("invoice_count"),
        F.countDistinct("customer_key").alias("active_customer_count"),
        F.countDistinct("salesperson_key").alias("active_salesperson_count"),
        F.sum("net_amount").alias("net_sales_local"),
        F.sum("net_amount_reporting").alias("net_sales_daily_rate"),
        # LEGACY QUIRK: missing month-average rate defaults to 1.0 (wrong number, not NULL).
        F.round(F.sum(F.col("net_amount") * F.coalesce(F.col("monthly_average_rate"), F.lit(1.0))), 2).alias(
            "net_sales_monthly_average_rate"
        ),
        F.sum(F.col("gross_margin_amount") * F.col("fx_rate_to_reporting")).alias("gross_margin_reporting"),
        F.sum(F.when(isNa, F.col("tax_amount")).otherwise(F.lit(0))).alias("sales_tax_collected"),
        F.sum(F.when(isEu & (regime == "STD"), F.col("tax_amount")).otherwise(F.lit(0))).alias(
            "vat_output_amount"
        ),
        F.sum(F.when(isEu & (regime == "RC"), F.col("tax_amount")).otherwise(F.lit(0))).alias(
            "vat_reverse_charge_amount"
        ),
        F.sum(
            F.when(isApac & (F.coalesce(F.col("tax_regime_code"), F.lit("GST")) != "GSTFREE"), F.col("tax_amount"))
            .otherwise(F.lit(0))
        ).alias("gst_collected"),
        F.sum(F.when(isApac & (regime == "GSTFREE"), F.col("net_amount")).otherwise(F.lit(0))).alias(
            "gst_free_sales"
        ),
    )
    grp = (
        grp.withColumn("fiscal_calendar_code", _labelCase("region_code", LEGACY_FISCAL_CALENDAR_LABEL, "JUL13"))
        .withColumn("local_currency_code", _labelCase("region_code", LEGACY_REGION_CURRENCY, "AUD"))
        .withColumn(
            "translation_difference",
            F.round(F.col("net_sales_monthly_average_rate") - F.col("net_sales_daily_rate"), 2),
        )
        .withColumn("margin_percent", safePercent(F.col("gross_margin_reporting"), F.col("net_sales_daily_rate")))
    )
    bud = budget.select(
        "sales_territory_key",
        F.col("budget_month").alias("calendar_month"),
        F.col("budget_amount_reporting").alias("budget_net_sales_reporting"),
    )
    grp = grp.join(bud, ["sales_territory_key", "calendar_month"], "left")
    grp = grp.withColumn(
        "budget_variance_reporting", F.col("net_sales_daily_rate") - F.col("budget_net_sales_reporting")
    ).withColumn(
        "budget_attainment_percent", safePercent(F.col("net_sales_daily_rate"), F.col("budget_net_sales_reporting"))
    )
    priorYear = grp.groupBy(
        F.add_months(F.col("calendar_month"), 12).alias("calendar_month"), "sales_territory_key", "sales_channel_key"
    ).agg(F.sum("net_sales_daily_rate").alias("prior_year_net_sales"))
    grp = grp.join(priorYear, ["calendar_month", "sales_territory_key", "sales_channel_key"], "left")
    grp = grp.withColumn(
        "year_over_year_percent",
        safePercent(F.col("net_sales_daily_rate") - F.col("prior_year_net_sales"), F.col("prior_year_net_sales")),
    )
    ytdWindow = (
        Window.partitionBy("sales_territory_key", "sales_channel_key", F.year(F.col("calendar_month")))
        .orderBy("calendar_month")
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    )
    rankWindow = Window.partitionBy("region_code", "calendar_month").orderBy(F.col("net_sales_daily_rate").desc())
    grp = grp.withColumn("year_to_date_net_sales", F.sum("net_sales_daily_rate").over(ytdWindow)).withColumn(
        "rank_in_region_by_sales", F.row_number().over(rankWindow)
    )
    return _refreshColumns(grp, batchId).select(
        "fiscal_year", "fiscal_period", "calendar_month", "region_code", "sales_territory_key",
        "sales_channel_key", "fiscal_calendar_code", "local_currency_code", "order_count", "invoice_count",
        "active_customer_count", "active_salesperson_count", "net_sales_local", "net_sales_daily_rate",
        "net_sales_monthly_average_rate", "translation_difference", "gross_margin_reporting", "margin_percent",
        "sales_tax_collected", "vat_output_amount", "vat_reverse_charge_amount", "gst_collected",
        "gst_free_sales", "budget_net_sales_reporting", "budget_variance_reporting",
        "budget_attainment_percent", "prior_year_net_sales", "year_over_year_percent",
        "year_to_date_net_sales", "rank_in_region_by_sales", "refresh_batch_id", "refreshed_datetime",
    )


# --------------------------------------------------------------------------- customer 360
RETENTION_YEARS: dict[str, int] = {"EU": 7, "APAC": 3, "NA": 10}


def buildCustomerRolling12Month(sale: DataFrame, payment: DataFrame, ret: DataFrame, loyalty: DataFrame,
                                webSession: DataFrame, asOfDate: dt.date, batchId: int) -> DataFrame:
    """Aggregate.Customer Rolling 12 Month: customer x calendar month (months with activity only).

    # LEGACY QUIRK: rows exist only for months in which the customer bought, so
    # [Inactive Month Flag] / [Consecutive Inactive Months] can never be set
    # from this table alone; both are reproduced as 0.
    """
    s = notReversal(sale).withColumn("calendar_month", F.trunc(F.col("invoice_date_key"), "month"))
    sales = s.groupBy("customer_key", "calendar_month").agg(
        F.max("region_code").alias("region_code"),
        F.countDistinct("order_number").alias("order_count"),
        F.sum("net_amount_reporting").alias("net_revenue_reporting"),
        F.sum(F.col("gross_margin_amount") * F.col("fx_rate_to_reporting")).alias("gross_margin_reporting"),
        F.countDistinct("stock_item_key").alias("distinct_product_count"),
    )
    returns = ret.groupBy("customer_key", F.trunc(F.col("return_date_key"), "month").alias("calendar_month")).agg(
        F.sum(F.abs(F.col("net_credit_amount_reporting"))).alias("returns_reporting")
    )
    cash = payment.groupBy("customer_key", F.trunc(F.col("payment_date_key"), "month").alias("calendar_month")).agg(
        F.sum("allocated_amount_reporting").alias("cash_received_reporting")
    )
    points = loyalty.groupBy("customer_key", F.trunc(F.col("transaction_date_key"), "month").alias("calendar_month")).agg(
        F.sum("points_earned").alias("loyalty_points_earned"),
        F.sum("points_redeemed").alias("loyalty_points_redeemed"),
    )
    web = webSession.groupBy("customer_key", F.trunc(F.col("session_date_key"), "month").alias("calendar_month")).agg(
        F.sum("session_count").alias("web_session_count")
    )
    keys = ["customer_key", "calendar_month"]
    rolling = (
        sales.join(returns, keys, "left").join(cash, keys, "left").join(points, keys, "left").join(web, keys, "left")
    )
    for c in ("returns_reporting", "cash_received_reporting", "loyalty_points_earned", "loyalty_points_redeemed", "web_session_count"):
        rolling = rolling.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    asOfMonthIdx = F.lit(asOfDate.year * 12 + asOfDate.month)
    rolling = rolling.withColumn("month_idx", monthIndex(F.col("calendar_month"))).withColumn(
        "month_offset", (asOfMonthIdx - F.col("month_idx")).cast("int")
    )
    w12 = Window.partitionBy("customer_key").orderBy("month_idx").rangeBetween(-11, 0)
    w3 = Window.partitionBy("customer_key").orderBy("month_idx").rangeBetween(-2, 0)
    wPrev3 = Window.partitionBy("customer_key").orderBy("month_idx").rangeBetween(-5, -3)
    rolling = (
        rolling.withColumn("rolling_12_month_revenue", F.sum("net_revenue_reporting").over(w12))
        .withColumn("rolling_12_month_margin", F.sum("gross_margin_reporting").over(w12))
        .withColumn("rolling_3_month_revenue", F.sum("net_revenue_reporting").over(w3))
        .withColumn("prev_3_month_revenue", F.sum("net_revenue_reporting").over(wPrev3))
        .withColumn(
            "revenue_trend_percent",
            safePercent(F.col("rolling_3_month_revenue") - F.col("prev_3_month_revenue"), F.col("prev_3_month_revenue")),
        )
        .withColumn("inactive_month_flag", F.lit(False))
        .withColumn("consecutive_inactive_months", F.lit(0))
    )
    return _refreshColumns(rolling, batchId).select(
        "customer_key", "month_offset", "calendar_month", "region_code", "order_count", "net_revenue_reporting",
        "gross_margin_reporting", "returns_reporting", "cash_received_reporting", "distinct_product_count",
        "loyalty_points_earned", "loyalty_points_redeemed", "web_session_count", "rolling_12_month_revenue",
        "rolling_12_month_margin", "rolling_3_month_revenue", "revenue_trend_percent", "inactive_month_flag",
        "consecutive_inactive_months", "refresh_batch_id", "refreshed_datetime",
    )


def buildCustomer360(
    customer: DataFrame,
    sale: DataFrame,
    payment: DataFrame,
    ret: DataFrame,
    loyalty: DataFrame,
    webSession: DataFrame,
    balance: DataFrame,
    asOfDate: dt.date,
    batchId: int,
) -> DataFrame:
    """Aggregate.Customer 360: one row per current customer (TRUNCATE + rebuild)."""
    asOf = F.lit(asOfDate)
    cust = customer.filter(F.col("is_current_row") == True).select(  # noqa: E712
        "customer_key",
        F.col("customer_segment_key").alias("dim_customer_segment_key"),
        F.col("sales_territory_key").alias("dim_sales_territory_key"),
        "region_code",
        F.col("customer").alias("customer_name"),
        F.col("primary_contact_email").alias("dim_primary_contact_email"),
        "account_manager_employee_key",
        F.coalesce(F.col("credit_limit_amount"), F.lit(0)).alias("credit_limit_reporting"),
        F.col("marketing_consent_flag").alias("src_marketing_consent_flag"),
        "erasure_requested_on",
    )
    s = notReversal(sale)
    sales = s.groupBy("customer_key").agg(
        F.min("invoice_date_key").alias("first_order_date"),
        F.max("invoice_date_key").alias("last_order_date"),
        F.countDistinct("order_number").alias("lifetime_order_count"),
        F.sum("net_amount_reporting").alias("lifetime_net_revenue"),
        F.sum(F.col("gross_margin_amount") * F.col("fx_rate_to_reporting")).alias("lifetime_gross_margin"),
        F.max("customer_segment_key").alias("sale_customer_segment_key"),
        F.max("sales_territory_key").alias("sale_sales_territory_key"),
    )
    channelWindow = Window.partitionBy("customer_key").orderBy(F.col("channel_revenue").desc(), F.col("sales_channel_key"))
    primaryChannel = (
        s.groupBy("customer_key", "sales_channel_key")
        .agg(F.sum("net_amount_reporting").alias("channel_revenue"))
        .withColumn("rn", F.row_number().over(channelWindow))
        .filter(F.col("rn") == 1)
        .select("customer_key", F.col("sales_channel_key").alias("primary_sales_channel_key"))
    )
    pay = payment.groupBy("customer_key").agg(
        F.max("payment_date_key").alias("last_payment_date"),
        F.round(F.avg("days_to_pay"), 2).alias("average_days_to_pay"),
    )
    returns = ret.groupBy("customer_key").agg(F.sum(F.abs(F.col("net_credit_amount_reporting"))).alias("lifetime_returns_amount"))
    points = loyalty.groupBy("customer_key").agg(
        (F.sum("points_earned") - F.sum("points_redeemed")).alias("loyalty_point_balance"),
        F.max_by("loyalty_tier_key", "transaction_date_key").alias("loyalty_tier_key"),
    )
    web = webSession.filter(F.col("session_date_key") >= F.date_sub(asOf, 90)).groupBy("customer_key").agg(
        F.sum("session_count").alias("web_session_count_90_day")
    )
    latestBalance = balance.groupBy("customer_key").agg(
        F.max_by("current_balance_reporting", "balance_date_key").alias("current_balance_reporting"),
        F.max_by("overdue_balance_reporting", "balance_date_key").alias("overdue_balance_reporting"),
    )
    c = (
        cust.join(sales, "customer_key", "left")
        .join(primaryChannel, "customer_key", "left")
        .join(pay, "customer_key", "left")
        .join(returns, "customer_key", "left")
        .join(points, "customer_key", "left")
        .join(web, "customer_key", "left")
        .join(latestBalance, "customer_key", "left")
    )
    c = (
        c.withColumn("customer_segment_key", F.coalesce(F.col("sale_customer_segment_key"), F.col("dim_customer_segment_key"), F.lit(-1)))
        .withColumn("sales_territory_key", F.coalesce(F.col("sale_sales_territory_key"), F.col("dim_sales_territory_key"), F.lit(-1)))
        .withColumn("loyalty_tier_key", F.coalesce(F.col("loyalty_tier_key"), F.lit(-1)))
        .withColumn("primary_sales_channel_key", F.coalesce(F.col("primary_sales_channel_key"), F.lit(-1)))
        .withColumn("lifetime_order_count", F.coalesce(F.col("lifetime_order_count"), F.lit(0)))
        .withColumn("lifetime_net_revenue", F.coalesce(F.col("lifetime_net_revenue"), F.lit(0)))
        .withColumn("lifetime_gross_margin", F.coalesce(F.col("lifetime_gross_margin"), F.lit(0)))
        .withColumn("lifetime_returns_amount", F.coalesce(F.col("lifetime_returns_amount"), F.lit(0)))
        .withColumn("loyalty_point_balance", F.coalesce(F.col("loyalty_point_balance"), F.lit(0)))
        .withColumn("web_session_count_90_day", F.coalesce(F.col("web_session_count_90_day"), F.lit(0)))
        .withColumn("current_balance_reporting", F.coalesce(F.col("current_balance_reporting"), F.lit(0)))
        .withColumn("overdue_balance_reporting", F.coalesce(F.col("overdue_balance_reporting"), F.lit(0)))
        .withColumn(
            "tenure_months",
            F.when(F.col("first_order_date").isNull(), F.lit(None).cast("int")).otherwise(
                (F.lit(asOfDate.year * 12 + asOfDate.month) - monthIndex(F.col("first_order_date"))).cast("int")
            ),
        )
        .withColumn("average_order_value", safeDivide(F.col("lifetime_net_revenue"), F.col("lifetime_order_count")))
        .withColumn("credit_utilisation_percent", safePercent(F.col("current_balance_reporting"), F.col("credit_limit_reporting")))
        .withColumn("days_since_last_order", F.datediff(asOf, F.col("last_order_date")))
    )
    # Marketing consent is regional: EU explicit opt-in + no erasure, APAC explicit opt-in, NA opt-out.
    consent = F.col("src_marketing_consent_flag")
    c = c.withColumn(
        "marketing_consent_flag",
        F.when(F.col("region_code") == "APAC", F.coalesce(consent, F.lit(False)))
        .when(F.col("region_code") == "EU", F.coalesce(consent, F.lit(False)) & F.col("erasure_requested_on").isNull())
        .otherwise(F.coalesce(consent, F.lit(True))),
    )
    retention = F.lit(None).cast("date")
    for region, years in RETENTION_YEARS.items():
        retention = F.when(
            F.col("region_code") == region, F.add_months(F.coalesce(F.col("last_order_date"), asOf), 12 * years)
        ).otherwise(retention)
    c = c.withColumn("retention_expiry_date", retention)
    dslo = F.col("days_since_last_order")
    rfm = (
        F.when(dslo <= 30, 3).when(dslo <= 120, 2).otherwise(1)
        + F.when(F.col("lifetime_order_count") >= 50, 3).when(F.col("lifetime_order_count") >= 10, 2).otherwise(1)
        + F.when(F.col("lifetime_net_revenue") >= 250000, 3).when(F.col("lifetime_net_revenue") >= 25000, 2).otherwise(1)
    )
    churn = F.when(dslo.isNull(), F.lit(100)).otherwise(
        F.when(dslo > 365, 60).when(dslo > 180, 40).when(dslo > 90, 20).otherwise(0)
        + F.when(F.col("web_session_count_90_day") == 0, 15).otherwise(0)
        + F.when(F.col("credit_utilisation_percent") > 90, 10).otherwise(0)
        + F.when(
            F.col("lifetime_returns_amount")
            > 0.10 * F.when(F.col("lifetime_net_revenue") == 0, F.lit(None)).otherwise(F.col("lifetime_net_revenue")),
            15,
        ).otherwise(0)
    )
    c = c.withColumn("rfm_score", rfm.cast("string")).withColumn("churn_risk_score", churn.cast("decimal(5,2)"))
    c = c.withColumn(
        "churn_risk_band",
        F.when(F.col("churn_risk_score") >= 60, "HIGH").when(F.col("churn_risk_score") >= 30, "MEDIUM").otherwise("LOW"),
    )
    # LEGACY QUIRK: EU/APAC customers past retention are pseudonymised in place;
    # NA rows are kept verbatim (no retention regime in NA).
    expired = F.col("region_code").isin("EU", "APAC") & (F.col("retention_expiry_date") < asOf)
    c = (
        c.withColumn("anonymised_flag", expired)
        .withColumn("customer_name", F.when(expired, F.lit("REDACTED")).otherwise(F.col("customer_name")))
        .withColumn("primary_contact_email", F.when(expired, F.lit(None).cast("string")).otherwise(F.col("dim_primary_contact_email")))
        .withColumn("marketing_consent_flag", F.when(expired, F.lit(False)).otherwise(F.col("marketing_consent_flag")))
    )
    return _refreshColumns(c, batchId).select(
        "customer_key", "customer_segment_key", "sales_territory_key", "loyalty_tier_key", "primary_sales_channel_key",
        "region_code", "customer_name", "primary_contact_email", "account_manager_employee_key", "first_order_date",
        "last_order_date", "last_payment_date", "tenure_months", "lifetime_order_count", "lifetime_net_revenue",
        "lifetime_gross_margin", "lifetime_returns_amount", "average_order_value", "average_days_to_pay",
        "current_balance_reporting", "overdue_balance_reporting", "credit_limit_reporting",
        "credit_utilisation_percent", "loyalty_point_balance", "web_session_count_90_day", "days_since_last_order",
        "churn_risk_score", "churn_risk_band", "rfm_score", "marketing_consent_flag", "retention_expiry_date",
        "anonymised_flag", "refresh_batch_id", "refreshed_datetime",
    )


# --------------------------------------------------------------------------- product performance
def buildProductPerformance(sale: DataFrame, ret: DataFrame, stockItem: DataFrame, batchId: int) -> DataFrame:
    """Aggregate.Product Performance: calendar month x stock item x region.

    Inventory-derived measures (average stock value, turns, DIO, stockout days,
    lost sales, sell-through) come from Fact.Daily Inventory Snapshot, which is
    outside the sales domain; they are emitted as NULL / 0.
    """
    items = stockItem.select(
        "stock_item_key",
        F.col("product_category_key").alias("item_category_key"),
        F.col("primary_supplier_key").alias("item_supplier_key"),
        F.col("valid_from").alias("item_valid_from"),
        F.col("is_discontinued").alias("item_discontinued"),
    )
    s = notReversal(sale).join(items, "stock_item_key", "left").withColumn(
        "calendar_month", F.trunc(F.col("invoice_date_key"), "month")
    )
    grp = s.groupBy("calendar_month", "stock_item_key", "region_code").agg(
        F.coalesce(F.max("item_category_key"), F.lit(0)).alias("product_category_key"),
        F.coalesce(F.max("item_supplier_key"), F.lit(-1)).alias("primary_supplier_key"),
        F.sum("quantity_base_uom").alias("units_sold_base_uom"),
        F.sum("net_amount_reporting").alias("net_revenue_reporting"),
        F.sum(F.col("gross_margin_amount") * F.col("fx_rate_to_reporting")).alias("gross_margin_reporting"),
        safePercent(F.sum("gross_margin_amount"), F.sum("net_amount")).alias("margin_percent"),
        safePercent(F.sum("line_discount_amount"), F.sum("gross_amount")).alias("discount_depth_percent"),
        safeDivide(F.sum("net_amount"), F.sum("quantity_base_uom"), 4).alias("average_selling_price"),
        safeDivide(F.sum("cost_of_sale_amount"), F.sum("quantity_base_uom"), 4).alias("average_unit_cost"),
        F.countDistinct("customer_key").alias("distinct_customer_count"),
        F.max(F.when(F.col("item_valid_from") > F.add_months(F.col("calendar_month"), -6), True).otherwise(False)).alias(
            "new_product_flag"
        ),
        F.max(F.coalesce(F.col("item_discontinued"), F.lit(False))).alias("discontinued_flag"),
    )
    # LEGACY QUIRK: units returned are looked up per item x month across all regions.
    returns = ret.groupBy(F.trunc(F.col("return_date_key"), "month").alias("calendar_month"), "stock_item_key").agg(
        F.sum(F.abs(F.col("quantity_returned"))).alias("units_returned")
    )
    grp = grp.join(returns, ["calendar_month", "stock_item_key"], "left").withColumn(
        "units_returned", F.coalesce(F.col("units_returned"), F.lit(0))
    )
    grp = grp.withColumn("return_rate_percent", safePercent(F.col("units_returned"), F.col("units_sold_base_uom")))
    catWindow = Window.partitionBy("calendar_month", "product_category_key", "region_code")
    revRank = catWindow.orderBy(F.col("net_revenue_reporting").desc(), F.col("stock_item_key"))
    grp = (
        grp.withColumn("rank_in_category_by_revenue", F.row_number().over(revRank))
        .withColumn(
            "rank_in_category_by_margin",
            F.row_number().over(catWindow.orderBy(F.col("gross_margin_reporting").desc(), F.col("stock_item_key"))),
        )
        .withColumn("category_revenue", F.sum("net_revenue_reporting").over(catWindow))
        .withColumn(
            "cumulative_revenue", F.sum("net_revenue_reporting").over(revRank.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )
    )
    grp = grp.withColumn(
        "abc_class",
        F.when(F.col("category_revenue") == 0, "C")
        .when(F.col("cumulative_revenue") <= 0.80 * F.col("category_revenue"), "A")
        .when(F.col("cumulative_revenue") <= 0.95 * F.col("category_revenue"), "B")
        .otherwise("C"),
    )
    grp = grp.withColumn("month_idx", monthIndex(F.col("calendar_month")))
    itemWindow = Window.partitionBy("stock_item_key", "region_code").orderBy("month_idx")
    trailing = itemWindow.rangeBetween(-11, 0)
    grp = (
        grp.withColumn(
            "prior_month_abc_class",
            F.when(F.lag("month_idx").over(itemWindow) == F.col("month_idx") - 1, F.lag("abc_class").over(itemWindow)),
        )
        .withColumn("mean_units", F.avg("units_sold_base_uom").over(trailing))
        .withColumn("std_units", F.stddev_samp("units_sold_base_uom").over(trailing))
    )
    cv = F.col("std_units") / F.col("mean_units")
    grp = grp.withColumn(
        "xyz_class",
        F.when(F.col("mean_units").isNull() | (F.col("mean_units") == 0), "Z")
        .when(F.col("std_units").isNull(), "Z")
        .when(cv <= 0.25, "X")
        .when(cv <= 0.60, "Y")
        .otherwise("Z"),
    )
    nullDec = F.lit(None).cast("decimal(18,2)")
    grp = (
        grp.withColumn("average_stock_value_reporting", nullDec)
        .withColumn("inventory_turns", nullDec)
        .withColumn("days_inventory_outstanding", nullDec)
        .withColumn("stockout_days", F.lit(0))
        .withColumn("lost_sales_estimate_reporting", nullDec)
        .withColumn("sell_through_percent", nullDec)
    )
    return _refreshColumns(grp, batchId).select(
        "calendar_month", "stock_item_key", "product_category_key", "region_code", "primary_supplier_key",
        "units_sold_base_uom", "net_revenue_reporting", "gross_margin_reporting", "margin_percent",
        "discount_depth_percent", "units_returned", "return_rate_percent", "average_selling_price",
        "average_unit_cost", "average_stock_value_reporting", "inventory_turns", "days_inventory_outstanding",
        "stockout_days", "lost_sales_estimate_reporting", "sell_through_percent", "distinct_customer_count",
        "abc_class", "xyz_class", "prior_month_abc_class", "rank_in_category_by_revenue",
        "rank_in_category_by_margin", "new_product_flag", "discontinued_flag", "refresh_batch_id",
        "refreshed_datetime",
    )


# --------------------------------------------------------------------------- margin analysis
COST_BASIS_BY_REGION: dict[str, str] = {"NA": "WAVG", "EU": "FIFO", "APAC": "STD"}


def buildMonthlyMarginAnalysis(salesMargin: DataFrame, fiscalCalendar: DataFrame, batchId: int) -> DataFrame:
    """Aggregate.Monthly Margin Analysis: month x category x territory x channel x region.

    # LEGACY QUIRK: the price/volume/cost effects do not sum to the margin
    # movement; the residual is plugged into the mix effect.
    """
    m = withNaFiscalPeriod(salesMargin, "invoice_date_key", fiscalCalendar).withColumn(
        "calendar_month", F.trunc(F.col("invoice_date_key"), "month")
    )
    fx = F.col("fx_rate_to_reporting")
    stdCostRep = F.col("standard_cost_amount") * fx
    costRep = F.col("cost_of_sale_amount") * fx
    grp = m.groupBy("calendar_month", "product_category_key", "sales_territory_key", "sales_channel_key", "region_code").agg(
        F.max("na_fiscal_year").alias("fiscal_year"),
        F.max("na_fiscal_period").alias("fiscal_period"),
        F.sum("quantity_base_uom").alias("quantity_sold_base_uom"),
        F.sum("net_amount_reporting").alias("net_revenue_reporting"),
        F.sum(costRep).alias("cost_of_sales_reporting"),
        F.sum(stdCostRep).alias("standard_cost_reporting"),
        F.sum(costRep - stdCostRep).alias("purchase_price_variance"),
        F.sum(F.coalesce(F.col("freight_cost_amount"), F.lit(0))).alias("freight_cost_reporting"),
        F.sum(F.coalesce(F.col("rebate_accrual_amount"), F.lit(0))).alias("rebate_accrual_reporting"),
        F.sum("gross_margin_reporting").alias("gross_margin_reporting"),
        F.sum(F.col("net_amount_reporting") - stdCostRep).alias("standard_margin_reporting"),
        F.sum(
            F.col("gross_margin_reporting")
            - F.coalesce(F.col("freight_cost_amount"), F.lit(0))
            + F.coalesce(F.col("rebate_accrual_amount"), F.lit(0))
        ).alias("contribution_margin_reporting"),
        F.sum(F.when(F.col("gross_margin_reporting") < 0, 1).otherwise(0)).alias("negative_margin_line_count"),
    )
    grp = grp.withColumn("cost_basis_code", _labelCase("region_code", COST_BASIS_BY_REGION, "STD"))
    grp = grp.withColumn("margin_percent", safePercent(F.col("gross_margin_reporting"), F.col("net_revenue_reporting"))).withColumn(
        "standard_margin_percent", safePercent(F.col("standard_margin_reporting"), F.col("net_revenue_reporting"))
    )
    # LEGACY QUIRK: prior period is joined on category/territory/channel only (no region).
    prior = grp.groupBy(
        F.add_months(F.col("calendar_month"), 1).alias("calendar_month"), "product_category_key", "sales_territory_key", "sales_channel_key"
    ).agg(
        F.sum("quantity_sold_base_uom").alias("p_qty"),
        F.sum("net_revenue_reporting").alias("p_rev"),
        F.sum("cost_of_sales_reporting").alias("p_cost"),
        F.sum("gross_margin_reporting").alias("p_margin"),
        F.max("margin_percent").alias("prior_period_margin_percent"),
    )
    grp = grp.join(prior, ["calendar_month", "product_category_key", "sales_territory_key", "sales_channel_key"], "left")
    qty = F.col("quantity_sold_base_uom")
    pQty = F.coalesce(F.col("p_qty"), F.lit(0))
    unitPrice = F.when(qty == 0, F.lit(0)).otherwise(F.col("net_revenue_reporting") / qty)
    pUnitPrice = F.when(pQty == 0, F.lit(0)).otherwise(F.col("p_rev") / F.col("p_qty"))
    unitCost = F.when(qty == 0, F.lit(0)).otherwise(F.col("cost_of_sales_reporting") / qty)
    pUnitCost = F.when(pQty == 0, F.lit(0)).otherwise(F.col("p_cost") / F.col("p_qty"))
    pUnitMargin = F.when(pQty == 0, F.lit(0)).otherwise((F.col("p_rev") - F.col("p_cost")) / F.col("p_qty"))
    grp = (
        grp.withColumn("price_effect_amount", F.round((unitPrice - pUnitPrice) * qty, 2))
        .withColumn("volume_effect_amount", F.round((qty - pQty) * pUnitMargin, 2))
        .withColumn("cost_effect_amount", F.round((pUnitCost - unitCost) * qty, 2))
    )
    grp = grp.withColumn(
        "mix_effect_amount",
        F.round(
            F.col("gross_margin_reporting") - F.coalesce(F.col("p_margin"), F.lit(0))
            - F.coalesce(F.col("price_effect_amount"), F.lit(0))
            - F.coalesce(F.col("volume_effect_amount"), F.lit(0))
            - F.coalesce(F.col("cost_effect_amount"), F.lit(0)),
            2,
        ),
    )
    return _refreshColumns(grp, batchId).select(
        "fiscal_year", "fiscal_period", "calendar_month", "product_category_key", "sales_territory_key",
        "sales_channel_key", "region_code", "cost_basis_code", "quantity_sold_base_uom", "net_revenue_reporting",
        "cost_of_sales_reporting", "standard_cost_reporting", "purchase_price_variance", "freight_cost_reporting",
        "rebate_accrual_reporting", "gross_margin_reporting", "standard_margin_reporting",
        "contribution_margin_reporting", "margin_percent", "standard_margin_percent", "price_effect_amount",
        "volume_effect_amount", "mix_effect_amount", "cost_effect_amount", "negative_margin_line_count",
        "prior_period_margin_percent", "refresh_batch_id", "refreshed_datetime",
    )


# --------------------------------------------------------------------------- entry point
def run(spark: SparkSession, cfg: PipelineConfig, asOfDate: dt.date | None = None) -> None:
    asOf = asOfDate or dt.date.today()
    sale = readTable(spark, cfg, "gold", "fact_sale")
    payment = readTable(spark, cfg, "gold", "fact_payment")
    creditNote = readTable(spark, cfg, "gold", "fact_credit_note")
    salesMargin = readTable(spark, cfg, "gold", "fact_sales_margin")
    ret = readOrEmpty(spark, cfg, "gold", "fact_return", FACT_RETURN_SCHEMA)
    customer = readDimCustomer(spark, cfg)
    fiscalCalendar = readTable(spark, cfg, "silver", "dim_fiscal_calendar")
    fxRate = readTable(spark, cfg, "silver", "ref_fx_rate")
    stockItem = readOrEmpty(spark, cfg, "silver", "dim_stock_item", DIM_STOCK_ITEM_SCHEMA)
    budget = readOrEmpty(spark, cfg, "silver", "ref_sales_budget", SALES_BUDGET_SCHEMA)
    loyalty = readOrEmpty(spark, cfg, "gold", "fact_loyalty_points", FACT_LOYALTY_POINTS_SCHEMA)
    webSession = readOrEmpty(spark, cfg, "gold", "fact_web_session", FACT_WEB_SESSION_SCHEMA)
    balance = readOrEmpty(spark, cfg, "gold", "fact_customer_balance", FACT_CUSTOMER_BALANCE_SCHEMA)

    overwriteTable(buildDailySales(sale, ret, stockItem, fiscalCalendar, cfg.batchId), cfg.fqn("gold", AGG_DAILY_SALES), ["region_code"])
    monthlyFqn = cfg.fqn("gold", AGG_MONTHLY_SALES)
    existing = spark.table(monthlyFqn) if tableExists(spark, monthlyFqn) else None
    monthly = buildMonthlySales(sale, creditNote, ret, fiscalCalendar, cfg.batchId, existing)
    if existing is not None:
        monthly = monthly.localCheckpoint()  # materialise before overwriting the source table
    overwriteTable(monthly, monthlyFqn, ["region_code"])
    overwriteTable(
        buildRegionalSalesPerformance(sale, fxRate, budget, fiscalCalendar, cfg.batchId, cfg.reportingCurrency),
        cfg.fqn("gold", AGG_REGIONAL_SALES_PERFORMANCE),
        ["region_code"],
    )
    overwriteTable(
        buildCustomer360(customer, sale, payment, ret, loyalty, webSession, balance, asOf, cfg.batchId),
        cfg.fqn("gold", AGG_CUSTOMER_360),
        ["region_code"],
    )
    overwriteTable(
        buildCustomerRolling12Month(sale, payment, ret, loyalty, webSession, asOf, cfg.batchId),
        cfg.fqn("gold", AGG_CUSTOMER_ROLLING_12_MONTH),
    )
    overwriteTable(buildProductPerformance(sale, ret, stockItem, cfg.batchId), cfg.fqn("gold", AGG_PRODUCT_PERFORMANCE), ["region_code"])
    overwriteTable(
        buildMonthlyMarginAnalysis(salesMargin, fiscalCalendar, cfg.batchId), cfg.fqn("gold", AGG_MONTHLY_MARGIN_ANALYSIS), ["region_code"]
    )
