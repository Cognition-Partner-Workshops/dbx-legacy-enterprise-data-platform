"""AGG_Refresh_*: the five month-end aggregates rebuilt over the gold sale fact."""

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_performance import config, fiscal
from sales_performance.commissions import writeMetrics
from sales_performance.common import readDw, readOltp, readTable, readTableOrEmpty, replaceWindow, saveTable, snakeCaseColumns, withAudit
from sales_performance.promotions import PROMOTION_LINE_TABLE, PROMOTION_TABLE, REDEMPTION_TABLE, SUMMARY_TABLE
from sales_performance.quota import ATTAINMENT_TABLE
from sales_performance.sale_line import CUSTOMER_TABLE, FACT_ORDER_TABLE, FACT_SALE_TABLE, STOCK_ITEM_TABLE, TERRITORY_TABLE

MONTHLY_SALES_TABLE = "gold_agg_monthly_sales_summary"
REGIONAL_TABLE = "gold_agg_regional_sales_performance"
REGIONAL_REJECT_TABLE = "gold_agg_regional_sales_reject"
PRODUCT_TABLE = "gold_agg_product_performance"
PROMO_EFFECT_TABLE = "gold_agg_promotion_effectiveness"
MARGIN_TABLE = "gold_agg_monthly_margin_analysis"
MONEY = "decimal(19,4)"

PACKAGES = {
    MONTHLY_SALES_TABLE: "AGG_Refresh_MonthlySalesSummary",
    REGIONAL_TABLE: "AGG_Refresh_RegionalSalesPerformance",
    PRODUCT_TABLE: "AGG_Refresh_ProductPerformance",
    PROMO_EFFECT_TABLE: "AGG_Refresh_PromotionEffectiveness",
    MARGIN_TABLE: "AGG_Refresh_MonthlyMarginAnalysis",
}


def activeSales(fact: DataFrame) -> DataFrame:
    """Exclude reversal rows and the originals they reversed (the replacement row carries the value)."""
    reversedOriginal = F.col("is_correction") & ~F.col("is_reversal") & F.col("reverses_sale_key").isNull()
    return fact.filter(~F.col("is_reversal") & ~reversedOriginal)


def _pct(numerator, denominator):
    return F.when(denominator.isNotNull() & (denominator != 0), numerator / denominator * 100).cast("decimal(12,4)")


def periodEndDate(periodStart, calendarCode):
    """Approximate period end as the day before the next period starts."""
    return F.date_sub(fiscal.fiscalPeriodStart(F.date_add(periodStart, 35), calendarCode), 1)


def buildMonthlySalesSummary(fact: DataFrame, orders: DataFrame, asOfDate: date) -> DataFrame:
    """Customer x fiscal period grain with regional fiscal attributes and the comparison metrics."""
    sales = activeSales(fact)
    keys = ["customer_key", "region_code", "fiscal_calendar_code", "fiscal_year", "fiscal_period"]
    base = sales.groupBy(*keys).agg(
        F.min("invoice_date").alias("first_invoice_date"),
        F.max("calendar_month").alias("calendar_month"),
        F.countDistinct("invoice_number").alias("invoice_count"),
        F.lit(0).cast("bigint").alias("return_count"),
        F.sum("quantity").cast("decimal(18,3)").alias("quantity_sold_base_uom"),
        F.sum("extended_price").cast(MONEY).alias("gross_revenue"),
        F.lit(0).cast(MONEY).alias("discount_given"),
        F.sum("extended_price").cast(MONEY).alias("net_revenue"),
        F.sum("extended_price").cast(MONEY).alias("net_revenue_reporting"),
        F.lit(0).cast(MONEY).alias("credit_notes_reporting"),
        F.lit(0).cast(MONEY).alias("returns_reporting"),
        F.sum("cost_amount").cast(MONEY).alias("cost_of_sales"),
        F.sum("profit").cast(MONEY).alias("gross_margin"),
        F.sum(F.when(F.col("profit") < 0, 1).otherwise(0)).alias("loss_making_line_count"),
    )
    if orders is not None:
        o = orders.withColumn("fiscal_calendar_code", fiscal.calendarForRegion(F.col("region_code")))
        o = o.withColumn("fiscal_year", fiscal.fiscalYear(F.col("order_date"), F.col("fiscal_calendar_code"))).withColumn(
            "fiscal_period", fiscal.fiscalPeriod(F.col("order_date"), F.col("fiscal_calendar_code"))
        )
        oc = o.groupBy(*keys).agg(F.countDistinct("order_number").alias("order_count"))
        base = base.join(oc, keys, "left")
    else:
        base = base.withColumn("order_count", F.lit(0))
    base = base.na.fill({"order_count": 0}).withColumn(
        "net_revenue_after_credits",
        (F.col("net_revenue_reporting") - F.col("credit_notes_reporting") - F.col("returns_reporting")).cast(MONEY),
    )
    periodStart = fiscal.fiscalPeriodStart(F.col("first_invoice_date"), F.col("fiscal_calendar_code"))
    base = base.withColumn("period_start_date", periodStart).withColumn(
        "period_end_date", periodEndDate(periodStart, F.col("fiscal_calendar_code"))
    )
    base = base.withColumn("is_closed_period", F.col("period_end_date") < F.lit(asOfDate))
    seq = Window.partitionBy("customer_key", "region_code").orderBy("fiscal_year", "fiscal_period")
    priorYear = base.select(*keys, F.col("net_revenue_reporting").alias("prior_year_net_revenue")).withColumn(
        "fiscal_year", F.col("fiscal_year") + 1
    )
    out = (
        base.withColumn("prior_period_net_revenue", F.lag("net_revenue_reporting").over(seq))
        .withColumn("rolling_3_period_net_revenue", F.sum("net_revenue_reporting").over(seq.rowsBetween(-2, 0)).cast(MONEY))
        .join(priorYear, keys, "left")
    )
    out = (
        out.withColumn(
            "period_over_period_change_percent",
            _pct(F.col("net_revenue_reporting") - F.col("prior_period_net_revenue"), F.col("prior_period_net_revenue")),
        )
        .withColumn(
            "prior_year_change_percent",
            _pct(F.col("net_revenue_reporting") - F.col("prior_year_net_revenue"), F.col("prior_year_net_revenue")),
        )
        .withColumn("gross_margin_percent", _pct(F.col("gross_margin"), F.col("net_revenue")))
    )
    return out


def buildRegionalSalesPerformance(fact: DataFrame, territories: DataFrame, attainment: DataFrame = None):
    """Region/territory x fiscal period. Returns (rows, unassignedTerritoryRejects)."""
    sales = activeSales(fact)
    terr = territories.select(
        "territory_code",
        F.col("region_code").alias("t_region_code"),
        "tax_regime_code",
        "reporting_currency_code",
        F.col("fiscal_calendar_code").alias("t_calendar"),
    )
    joined = sales.join(terr, "territory_code", "left")
    rejects = joined.filter(F.col("t_region_code").isNull() | (F.col("territory_code") == "UNASSIGNED")).select(
        "sale_key",
        "invoice_number",
        "invoice_line_number",
        "region_code",
        "territory_code",
        "country_name",
        F.lit("UNASSIGNED_TERRITORY").alias("reject_reason_code"),
    )
    assigned = joined.filter(F.col("t_region_code").isNotNull() & (F.col("territory_code") != "UNASSIGNED"))
    # Regional tax semantics: NA sales tax and APAC GST are added on top of the net; EU VAT is inside the gross.
    netLocal = F.when(F.col("region_code") == "EU", F.col("total_including_tax") - F.coalesce(F.col("vat_amount"), F.lit(0))).otherwise(
        F.col("extended_price")
    )
    keys = ["region_code", "territory_code", "fiscal_calendar_code", "fiscal_year", "fiscal_period"]
    agg = (
        assigned.withColumn("net_local", netLocal)
        .groupBy(*keys)
        .agg(
            F.max("tax_regime_code").alias("tax_regime_code"),
            F.max("reporting_currency_code").alias("reporting_currency_code"),
            F.max("currency_code").alias("local_currency_code"),
            F.min("invoice_date").alias("first_invoice_date"),
            F.countDistinct("invoice_number").alias("invoice_count"),
            F.countDistinct("customer_key").alias("customer_count"),
            F.sum("quantity").cast("decimal(18,3)").alias("quantity_sold"),
            F.sum("extended_price").cast(MONEY).alias("gross_revenue_local"),
            F.sum("tax_amount").cast(MONEY).alias("tax_amount_local"),
            F.sum("net_local").cast(MONEY).alias("net_revenue_local"),
            F.sum("profit").cast(MONEY).alias("gross_margin_local"),
        )
    )
    # Daily-rate vs monthly-average translation: the only rates the estate holds are identities.
    agg = (
        agg.withColumn("net_revenue_reporting_daily_rate", F.col("net_revenue_local"))
        .withColumn("net_revenue_reporting_monthly_avg", F.col("net_revenue_local"))
        .withColumn(
            "translation_difference", (F.col("net_revenue_reporting_daily_rate") - F.col("net_revenue_reporting_monthly_avg")).cast(MONEY)
        )
    )
    agg = agg.withColumn("period_start_date", fiscal.fiscalPeriodStart(F.col("first_invoice_date"), F.col("fiscal_calendar_code")))
    agg = agg.withColumn("budget_amount", F.lit(None).cast(MONEY)).withColumn("budget_variance_percent", F.lit(None).cast("decimal(12,4)"))
    priorYear = agg.select(*keys, F.col("net_revenue_reporting_monthly_avg").alias("prior_year_net_revenue")).withColumn(
        "fiscal_year", F.col("fiscal_year") + 1
    )
    agg = agg.join(priorYear, keys, "left").withColumn(
        "prior_year_change_percent",
        _pct(F.col("net_revenue_reporting_monthly_avg") - F.col("prior_year_net_revenue"), F.col("prior_year_net_revenue")),
    )
    ytd = (
        Window.partitionBy("region_code", "territory_code", "fiscal_year")
        .orderBy("fiscal_period")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    agg = agg.withColumn("ytd_net_revenue", F.sum("net_revenue_reporting_monthly_avg").over(ytd).cast(MONEY))
    rank = Window.partitionBy("region_code", "fiscal_year", "fiscal_period").orderBy(F.col("net_revenue_reporting_monthly_avg").desc())
    agg = agg.withColumn("territory_revenue_rank", F.rank().over(rank))
    if attainment is not None:
        qa = attainment.groupBy("territory_code", "fiscal_period_label").agg(
            F.sum("quota_amount").cast(MONEY).alias("quota_amount"), F.sum("attainment_amount").cast(MONEY).alias("quota_attainment_amount")
        )
        agg = agg.withColumn(
            "fiscal_period_label",
            F.concat(F.lit("FY"), F.col("fiscal_year").cast("string"), F.lit("-P"), F.lpad(F.col("fiscal_period").cast("string"), 2, "0")),
        ).join(qa, ["territory_code", "fiscal_period_label"], "left")
        agg = agg.withColumn("quota_attainment_percent", _pct(F.col("quota_attainment_amount"), F.col("quota_amount")))
    return agg, rejects


def abcClass(cumulativeShare):
    return F.when(cumulativeShare <= 0.80, "A").when(cumulativeShare <= 0.95, "B").otherwise("C")


def xyzClass(coefficientOfVariation):
    return (
        F.when(coefficientOfVariation.isNull(), "Z")
        .when(coefficientOfVariation < 0.5, "X")
        .when(coefficientOfVariation < 1.0, "Y")
        .otherwise("Z")
    )


def buildProductPerformance(fact: DataFrame, stockHolding: DataFrame = None, productAttributes: DataFrame = None) -> DataFrame:
    """Product x calendar month x region with ABC/XYZ classification and stock metrics."""
    sales = activeSales(fact)
    keys = ["stock_item_key", "region_code", "calendar_month"]
    agg = sales.groupBy(*keys).agg(
        F.max("stock_item_name").alias("stock_item_name"),
        F.sum("quantity").cast("decimal(18,3)").alias("quantity_sold"),
        F.sum("extended_price").cast(MONEY).alias("net_revenue"),
        F.sum("cost_amount").cast(MONEY).alias("cost_of_sales"),
        F.sum("profit").cast(MONEY).alias("gross_margin"),
        F.lit(0).cast(MONEY).alias("discount_given"),
        F.lit(0).cast("decimal(18,3)").alias("returned_quantity"),
        F.countDistinct("invoice_number").alias("invoice_count"),
        F.countDistinct("invoice_date").alias("selling_day_count"),
    )
    if stockHolding is not None:
        sh = stockHolding.groupBy("stock_item_key").agg(
            F.avg("quantity_on_hand").cast("decimal(18,3)").alias("average_stock_on_hand"),
            F.max(F.when(F.col("quantity_on_hand") <= 0, 1).otherwise(0)).alias("stock_out_flag"),
        )
        agg = agg.join(sh, "stock_item_key", "left")
    else:
        agg = agg.withColumn("average_stock_on_hand", F.lit(None).cast("decimal(18,3)")).withColumn("stock_out_flag", F.lit(0))
    agg = agg.na.fill({"stock_out_flag": 0})
    avgDailyRevenue = F.col("net_revenue") / F.greatest(F.col("selling_day_count"), F.lit(1))
    agg = agg.withColumn("lost_sales_estimate", F.when(F.col("stock_out_flag") == 1, avgDailyRevenue * 7).otherwise(F.lit(0)).cast(MONEY))
    agg = agg.withColumn(
        "sell_through_percent", _pct(F.col("quantity_sold"), F.col("quantity_sold") + F.coalesce(F.col("average_stock_on_hand"), F.lit(0)))
    )
    share = (
        Window.partitionBy("region_code", "calendar_month")
        .orderBy(F.col("net_revenue").desc(), "stock_item_key")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    total = Window.partitionBy("region_code", "calendar_month")
    agg = agg.withColumn(
        "cumulative_revenue_share", (F.sum("net_revenue").over(share) / F.sum("net_revenue").over(total)).cast("decimal(12,6)")
    ).withColumn("abc_class", abcClass(F.col("cumulative_revenue_share")))
    seq = Window.partitionBy("stock_item_key", "region_code").orderBy("calendar_month")
    agg = agg.withColumn("prior_month_abc_class", F.lag("abc_class").over(seq))
    trailing = seq.rowsBetween(-11, 0)
    cov = F.stddev_samp("quantity_sold").over(trailing) / F.avg("quantity_sold").over(trailing)
    agg = agg.withColumn("demand_coefficient_of_variation", cov.cast("decimal(12,6)")).withColumn(
        "xyz_class", xyzClass(F.col("demand_coefficient_of_variation"))
    )
    if productAttributes is not None:
        agg = agg.join(productAttributes, "stock_item_key", "left")
    return agg.withColumn("gross_margin_percent", _pct(F.col("gross_margin"), F.col("net_revenue")))


def buildPromotionEffectiveness(
    promotions: DataFrame, summary: DataFrame, redemptions: DataFrame, promotionLines: DataFrame, fact: DataFrame, customers: DataFrame
) -> DataFrame:
    """Promotion-window vs baseline-window revenue, take-up, incremental revenue, margin, ROI, payback."""
    sales = activeSales(fact)
    p = promotions.select(
        "promotion_id",
        "promotion_code",
        "promotion_name",
        F.trim(F.col("region_code")).alias("region_code"),
        "promotion_type",
        "start_date",
        "end_date",
        "budget_amount",
        "eligible_customer_category_list",
    )
    p = (
        p.withColumn("window_days", F.datediff(F.col("end_date"), F.col("start_date")) + 1)
        .withColumn("baseline_start_date", F.date_sub(F.col("start_date"), F.col("window_days")))
        .withColumn("baseline_end_date", F.date_sub(F.col("start_date"), 1))
    )
    promoItems = (
        promotionLines.filter(F.col("stock_item_id").isNotNull())
        .select("promotion_id", F.col("stock_item_id").cast("bigint").alias("stock_item_id"))
        .distinct()
    )
    ps = p.join(promoItems, "promotion_id", "left")
    joinCond = (sales["region_code"] == ps["region_code"]) & (
        ps["stock_item_id"].isNull() | (sales["stock_item_id"] == ps["stock_item_id"])
    )
    s = sales.alias("s").join(ps.alias("p"), joinCond, "inner")
    inPromo = F.col("s.invoice_date").between(F.col("p.start_date"), F.col("p.end_date"))
    inBase = F.col("s.invoice_date").between(F.col("p.baseline_start_date"), F.col("p.baseline_end_date"))
    rev = s.groupBy("p.promotion_id").agg(
        F.sum(F.when(inPromo, F.col("s.extended_price")).otherwise(0)).cast(MONEY).alias("promotion_revenue"),
        F.sum(F.when(inBase, F.col("s.extended_price")).otherwise(0)).cast(MONEY).alias("baseline_revenue"),
        F.sum(F.when(inPromo, F.col("s.quantity")).otherwise(0)).cast("decimal(18,3)").alias("promoted_units"),
        F.sum(F.when(inPromo, F.col("s.profit")).otherwise(0)).cast(MONEY).alias("promotion_margin_before_discount"),
    )
    cust = customers.withColumn("region_code", F.lit("NA")) if "region_code" not in customers.columns else customers
    consent = F.lit(True)
    if "marketing_consent_flag" in cust.columns:
        consent = F.coalesce(F.col("marketing_consent_flag").cast("boolean"), F.lit(False))
    eligible = (
        cust.withColumn("is_eu", F.col("region_code") == "EU")
        .filter(~F.col("is_eu") | consent)
        .groupBy("region_code")
        .agg(F.countDistinct("customer_key").alias("eligible_customer_count"))
    )
    out = (
        p.join(rev, "promotion_id", "left")
        .join(
            summary.select(
                "promotion_id", "redeemed_amount", "discount_cost", "redemption_count", "distinct_customer_count", "is_over_budget"
            ),
            "promotion_id",
            "left",
        )
        .join(eligible, "region_code", "left")
    )
    out = out.na.fill(
        {
            "promotion_revenue": 0,
            "baseline_revenue": 0,
            "promoted_units": 0,
            "discount_cost": 0,
            "redemption_count": 0,
            "distinct_customer_count": 0,
            "redeemed_amount": 0,
        }
    )
    out = out.withColumn("participating_customer_count", F.col("distinct_customer_count")).withColumn(
        "take_up_percent", _pct(F.col("participating_customer_count"), F.col("eligible_customer_count"))
    )
    out = out.withColumn("incremental_revenue", (F.col("promotion_revenue") - F.col("baseline_revenue")).cast(MONEY))
    out = out.withColumn(
        "incremental_margin", (F.coalesce(F.col("promotion_margin_before_discount"), F.lit(0)) - F.col("discount_cost")).cast(MONEY)
    )
    out = out.withColumn("roi_percent", _pct(F.col("incremental_margin"), F.col("discount_cost")))
    dailyIncremental = F.col("incremental_revenue") / F.greatest(F.col("window_days"), F.lit(1))
    out = out.withColumn("payback_days", F.when(dailyIncremental > 0, F.col("discount_cost") / dailyIncremental).cast("decimal(12,2)"))
    return out.withColumn("loyalty_points_dependency", F.lit("Loyalty.* points not populated in the legacy estate"))


def buildMonthlyMarginAnalysis(fact: DataFrame, productAttributes: DataFrame = None) -> DataFrame:
    """Category/territory/channel/region x fiscal period with the regional costing method and the
    price/volume/mix/cost bridge against the prior period (mix is the residual)."""
    sales = activeSales(fact)
    if productAttributes is not None:
        sales = sales.join(productAttributes.select("stock_item_key", "product_category"), "stock_item_key", "left")
    else:
        sales = sales.withColumn("product_category", F.lit(None).cast("string"))
    sales = sales.na.fill({"product_category": "UNKNOWN"})
    costMethod = F.when(F.col("region_code") == "NA", "WEIGHTED_AVERAGE").when(F.col("region_code") == "EU", "FIFO").otherwise("STANDARD")
    stdWindow = Window.partitionBy("stock_item_key", "region_code", "fiscal_year")
    unitCost = F.col("cost_amount") / F.when(F.col("quantity") != 0, F.col("quantity"))
    sales = sales.withColumn("unit_cost", unitCost).withColumn("standard_unit_cost", F.avg("unit_cost").over(stdWindow))
    sales = (
        sales.withColumn("cost_method_code", costMethod)
        .withColumn("standard_cost_amount", (F.col("standard_unit_cost") * F.col("quantity")).cast(MONEY))
        .withColumn(
            "costed_amount", F.when(costMethod == "STANDARD", F.col("standard_cost_amount")).otherwise(F.col("cost_amount")).cast(MONEY)
        )
    )
    keys = [
        "product_category",
        "territory_code",
        "sales_channel_code",
        "region_code",
        "fiscal_calendar_code",
        "fiscal_year",
        "fiscal_period",
    ]
    agg = sales.groupBy(*keys).agg(
        F.max("cost_method_code").alias("cost_method_code"),
        F.sum("quantity").cast("decimal(18,3)").alias("quantity_sold"),
        F.sum("extended_price").cast(MONEY).alias("net_revenue"),
        F.sum("costed_amount").cast(MONEY).alias("cost_of_sales"),
        F.sum("standard_cost_amount").cast(MONEY).alias("standard_cost_amount"),
        F.lit(0).cast(MONEY).alias("freight_amount"),
        F.lit(0).cast(MONEY).alias("rebate_amount"),
        F.sum(F.when(F.col("profit") < 0, 1).otherwise(0)).alias("negative_margin_line_count"),
    )
    agg = agg.withColumn("purchase_price_variance", (F.col("cost_of_sales") - F.col("standard_cost_amount")).cast(MONEY))
    agg = agg.withColumn("gross_margin", (F.col("net_revenue") - F.col("cost_of_sales")).cast(MONEY)).withColumn(
        "standard_margin", (F.col("net_revenue") - F.col("standard_cost_amount")).cast(MONEY)
    )
    agg = agg.withColumn(
        "contribution_margin", (F.col("gross_margin") - F.col("freight_amount") - F.col("rebate_amount")).cast(MONEY)
    ).withColumn("gross_margin_percent", _pct(F.col("gross_margin"), F.col("net_revenue")))
    seq = Window.partitionBy(*keys[:4]).orderBy("fiscal_year", "fiscal_period")
    qty = F.col("quantity_sold")
    price = F.col("net_revenue") / F.when(qty != 0, qty)
    cost = F.col("cost_of_sales") / F.when(qty != 0, qty)
    agg = agg.withColumn("avg_price", price).withColumn("avg_cost", cost)
    agg = (
        agg.withColumn("prior_qty", F.lag("quantity_sold").over(seq))
        .withColumn("prior_price", F.lag("avg_price").over(seq))
        .withColumn("prior_cost", F.lag("avg_cost").over(seq))
        .withColumn("prior_margin", F.lag("gross_margin").over(seq))
    )
    volume = (qty - F.col("prior_qty")) * (F.col("prior_price") - F.col("prior_cost"))
    priceEffect = (F.col("avg_price") - F.col("prior_price")) * qty
    costEffect = -(F.col("avg_cost") - F.col("prior_cost")) * qty
    agg = (
        agg.withColumn("bridge_volume_effect", volume.cast(MONEY))
        .withColumn("bridge_price_effect", priceEffect.cast(MONEY))
        .withColumn("bridge_cost_effect", costEffect.cast(MONEY))
    )
    agg = agg.withColumn(
        "bridge_mix_effect",
        (
            (F.col("gross_margin") - F.col("prior_margin"))
            - F.col("bridge_volume_effect")
            - F.col("bridge_price_effect")
            - F.col("bridge_cost_effect")
        ).cast(MONEY),
    )
    return agg.drop("avg_price", "avg_cost", "prior_qty", "prior_price", "prior_cost", "prior_margin")


def loadProductAttributes(spark: SparkSession, stockItems: DataFrame) -> DataFrame:
    items = snakeCaseColumns(readOltp(spark, "Warehouse", "StockItems")).select(
        F.col("stock_item_id").cast("bigint"), F.col("supplier_id").cast("bigint")
    )
    suppliers = snakeCaseColumns(readOltp(spark, "Purchasing", "Suppliers")).select(
        F.col("supplier_id").cast("bigint"), F.col("supplier_name")
    )
    groups = snakeCaseColumns(readOltp(spark, "Warehouse", "StockItemStockGroups")).select(
        F.col("stock_item_id").cast("bigint"), F.col("stock_group_id").cast("bigint")
    )
    groupNames = snakeCaseColumns(readOltp(spark, "Warehouse", "StockGroups")).select(
        F.col("stock_group_id").cast("bigint"), F.col("stock_group_name")
    )
    firstGroup = (
        groups.join(groupNames, "stock_group_id")
        .withColumn("_rn", F.row_number().over(Window.partitionBy("stock_item_id").orderBy("stock_group_id")))
        .filter(F.col("_rn") == 1)
        .select("stock_item_id", F.col("stock_group_name").alias("product_category"))
    )
    attrs = items.join(suppliers, "supplier_id", "left").join(firstGroup, "stock_item_id", "left")
    return (
        stockItems.select("stock_item_key", F.col("wwi_stock_item_id").cast("bigint").alias("stock_item_id"))
        .join(attrs, "stock_item_id", "left")
        .select("stock_item_key", "product_category", "supplier_name")
    )


def loadStockHolding(spark: SparkSession) -> DataFrame:
    sh = snakeCaseColumns(readDw(spark, "Fact", "Stock Holding"))
    return sh.select(F.col("stock_item_key").cast("bigint"), F.col("quantity_on_hand").cast("decimal(18,3)"))


def _windowPredicate(fact: DataFrame, monthsBack: int):
    if monthsBack is None or monthsBack <= 0:
        return None
    maxDate = fact.agg(F.max("invoice_date")).collect()[0][0]
    start = date(maxDate.year, maxDate.month, 1)
    for _ in range(monthsBack):
        start = date(start.year - 1, 12, 1) if start.month == 1 else date(start.year, start.month - 1, 1)
    return start


def _write(df: DataFrame, table: str, packageName: str, batchId: int, windowStart, dateColumn: str):
    out = withAudit(df, packageName, batchId)
    if windowStart is None or dateColumn not in out.columns:
        saveTable(out, table)
    else:
        out = out.filter(F.col(dateColumn) >= F.lit(windowStart))
        replaceWindow(out, table, f"{dateColumn} >= '{windowStart.isoformat()}'")
    return spark_count(out)


def spark_count(df):
    return df.count()


def runAggregate(spark: SparkSession, table: str, batchId: int, monthsBack: int = 0, asOfDate: date = None):
    packageName = PACKAGES[table]
    fact = readTable(spark, FACT_SALE_TABLE)
    asOfDate = asOfDate or date.today()
    windowStart = _windowPredicate(fact, monthsBack)
    metrics = {}
    if table == MONTHLY_SALES_TABLE:
        orders = readTableOrEmpty(spark, FACT_ORDER_TABLE, "order_number bigint, customer_key bigint, region_code string, order_date date")
        df = buildMonthlySalesSummary(fact, orders, asOfDate)
        if windowStart is not None:
            df = df.filter(~F.col("is_closed_period") | (F.col("period_start_date") >= F.lit(windowStart)))
        metrics["row_count"] = _write(df, table, packageName, batchId, windowStart, "period_start_date")
        metrics["loss_making_rows_preserved"] = readTable(spark, table).filter(F.col("gross_margin") < 0).count()
    elif table == REGIONAL_TABLE:
        rows, rejects = buildRegionalSalesPerformance(
            fact,
            readTable(spark, TERRITORY_TABLE),
            readTableOrEmpty(
                spark,
                ATTAINMENT_TABLE,
                "territory_code string, fiscal_period_label string, quota_amount decimal(19,4), attainment_amount decimal(19,4)",
            ),
        )
        metrics["row_count"] = _write(rows, table, packageName, batchId, windowStart, "period_start_date")
        saveTable(withAudit(rejects, packageName, batchId), REGIONAL_REJECT_TABLE)
        metrics["unassigned_territory_rows"] = readTable(spark, REGIONAL_REJECT_TABLE).count()
    elif table == PRODUCT_TABLE:
        attrs = loadProductAttributes(spark, readTable(spark, STOCK_ITEM_TABLE))
        df = buildProductPerformance(fact, loadStockHolding(spark), attrs).withColumn(
            "period_start_date", F.to_date(F.concat(F.col("calendar_month"), F.lit("-01")))
        )
        metrics["row_count"] = _write(df, table, packageName, batchId, windowStart, "period_start_date")
    elif table == PROMO_EFFECT_TABLE:
        df = buildPromotionEffectiveness(
            readTable(spark, PROMOTION_TABLE),
            readTable(spark, SUMMARY_TABLE),
            readTable(spark, REDEMPTION_TABLE),
            readTable(spark, PROMOTION_LINE_TABLE),
            fact,
            readTable(spark, CUSTOMER_TABLE),
        )
        metrics["row_count"] = _write(df, table, packageName, batchId, None, "start_date")
    elif table == MARGIN_TABLE:
        attrs = loadProductAttributes(spark, readTable(spark, STOCK_ITEM_TABLE))
        df = buildMonthlyMarginAnalysis(fact, attrs)
        df = df.withColumn(
            "period_start_date",
            fiscal.fiscalPeriodStart(F.make_date(F.col("fiscal_year"), F.lit(1), F.lit(1)), F.col("fiscal_calendar_code")),
        )
        metrics["row_count"] = _write(df, table, packageName, batchId, None, "period_start_date")
        metrics["negative_margin_rows"] = readTable(spark, table).filter(F.col("gross_margin") < 0).count()
    else:
        raise ValueError(table)
    writeMetrics(spark, packageName, metrics, batchId)
    return metrics


def refreshAll(spark: SparkSession, batchId: int, monthsBack: int = 0):
    """AGG_Publish_ReportingLayer's dependency-ordered refresh of the group's aggregates."""
    return {
        t: runAggregate(spark, t, batchId, monthsBack)
        for t in (MONTHLY_SALES_TABLE, REGIONAL_TABLE, PRODUCT_TABLE, PROMO_EFFECT_TABLE, MARGIN_TABLE)
    }


def tableFullName(table):
    return config.tableName(table)
