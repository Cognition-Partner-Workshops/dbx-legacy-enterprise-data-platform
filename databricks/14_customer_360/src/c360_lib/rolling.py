"""C360_Build_RollingMetrics business rules (rolling window aggregate, APAC 4-4-5 realignment,
ratios, activity status, RFM deciles)."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

DEFAULT_WINDOW_MONTHS = 12
DEFAULT_INACTIVE_THRESHOLD_DAYS = 270
NO_ORDER_DAYS = 9999
FREQUENT_ORDER_COUNT = 12


def aggregateRollingWindow(factSale: DataFrame, dimCustomer: DataFrame, asOfDate, windowMonths: int = DEFAULT_WINDOW_MONTHS) -> DataFrame:
    """Execute SQL Task 'Aggregate Rolling Window'.

    Window = [asOf - windowMonths months, asOf]; per Customer Key / Region Code:
    COUNT(DISTINCT WWI Invoice ID), SUM(Total Excluding Tax),
    SUM(CASE WHEN Quantity < 0 THEN ABS(Total Excluding Tax) END), COUNT(DISTINCT Stock Item Key),
    MAX(Invoice Date Key). Dimension.Customer joined on Customer Key (all versions, as legacy).
    """
    today = F.lit(asOfDate).cast("date")
    windowStart = F.add_months(today, -int(windowMonths))
    sales = factSale.where(F.col("InvoiceDateKey") >= windowStart)
    cust = dimCustomer.select("CustomerKey", "RegionCode")
    return (
        sales.join(cust, "CustomerKey", "inner")
        .groupBy("CustomerKey", "RegionCode")
        .agg(
            F.countDistinct("WWIInvoiceID").alias("OrderCount"),
            F.sum("TotalExcludingTax").cast(DecimalType(18, 2)).alias("NetRevenue"),
            F.sum(F.when(F.col("Quantity") < 0, F.abs(F.col("TotalExcludingTax"))).otherwise(F.lit(0)))
             .cast(DecimalType(18, 2)).alias("ReturnAmount"),
            F.countDistinct("StockItemKey").alias("DistinctItemCount"),
            F.max("InvoiceDateKey").alias("LastOrderDate"),
        )
        .withColumn("WindowStartDate", windowStart)
        .withColumn("WindowEndDate", today)
        .select("CustomerKey", "RegionCode", "WindowStartDate", "WindowEndDate", "OrderCount",
                "NetRevenue", "ReturnAmount", "DistinctItemCount", "LastOrderDate")
    )


def latestCompleted445Period(fiscalCalendar445: DataFrame, asOfDate):
    """SELECT TOP (1) PeriodStartDate, PeriodEndDate FROM stg.FiscalCalendar445Period
    WHERE PeriodEndDate <= asOf ORDER BY PeriodEndDate DESC. Returns None when no period."""
    today = F.lit(asOfDate).cast("date")
    rows = (
        fiscalCalendar445.where(F.col("PeriodEndDate") <= today)
        .orderBy(F.col("PeriodEndDate").desc())
        .select("PeriodStartDate", "PeriodEndDate")
        .limit(1)
        .collect()
    )
    return rows[0] if rows else None


def realignApacWindow(metrics: DataFrame, period) -> DataFrame:
    """Execute SQL Task 'Realign APAC Window To 445': APAC rows take the latest completed
    4-4-5 period boundaries as their window. Only the window dates move; the aggregate
    values were computed over the calendar window (legacy behaviour, preserved)."""
    if period is None:
        return metrics
    isApac = F.col("RegionCode") == "APAC"
    return (
        metrics.withColumn("WindowStartDate", F.when(isApac, F.lit(period["PeriodStartDate"]).cast("date")).otherwise(F.col("WindowStartDate")))
        .withColumn("WindowEndDate", F.when(isApac, F.lit(period["PeriodEndDate"]).cast("date")).otherwise(F.col("WindowEndDate")))
    )


def deriveRollingMetrics(metrics: DataFrame, windowMonths: int = DEFAULT_WINDOW_MONTHS,
                         inactiveThresholdDays: int = DEFAULT_INACTIVE_THRESHOLD_DAYS) -> DataFrame:
    """Data Flow 'Derive Rolling Metrics': Derive Ratios -> Derive Activity Status."""
    dec = DecimalType(18, 2)
    daysSince = F.when(F.col("LastOrderDate").isNull(), F.lit(NO_ORDER_DAYS)).otherwise(
        F.datediff(F.col("WindowEndDate"), F.col("LastOrderDate")))
    return (
        metrics
        .withColumn("AverageBasketAmount",
                    F.when(F.col("OrderCount") == 0, F.lit(0).cast(dec))
                     .otherwise((F.col("NetRevenue") / F.col("OrderCount")).cast(dec)))
        .withColumn("ReturnRatePercent",
                    F.when(F.col("NetRevenue") == 0, F.lit(0).cast(dec))
                     .otherwise((F.col("ReturnAmount") * 100 / F.col("NetRevenue")).cast(dec)))
        .withColumn("DaysSinceLastOrder", daysSince.cast("int"))
        .withColumn("ActivityStatusCode",
                    F.when(F.col("DaysSinceLastOrder") > F.lit(int(inactiveThresholdDays)), F.lit("INACTIVE"))
                     .when(F.col("OrderCount") >= FREQUENT_ORDER_COUNT, F.lit("FREQUENT"))
                     .otherwise(F.lit("ACTIVE")))
        .withColumn("OrdersPerMonth", (F.col("OrderCount") / F.lit(int(windowMonths)).cast(dec)).cast(dec))
    )


def assignRfmDeciles(metrics: DataFrame) -> DataFrame:
    """Execute SQL Task 'Assign RFM Deciles': NTILE(10) per Region Code.
    Recency ORDER BY Days Since Last Order DESC; Frequency ORDER BY Order Count;
    Monetary ORDER BY Net Revenue. Ties are broken by CustomerKey for determinism (T-SQL NTILE
    tie order is undefined)."""
    byRegion = Window.partitionBy("RegionCode")
    return (
        metrics
        .withColumn("RecencyDecile", F.ntile(10).over(byRegion.orderBy(F.col("DaysSinceLastOrder").desc(), F.col("CustomerKey"))))
        .withColumn("FrequencyDecile", F.ntile(10).over(byRegion.orderBy(F.col("OrderCount"), F.col("CustomerKey"))))
        .withColumn("MonetaryDecile", F.ntile(10).over(byRegion.orderBy(F.col("NetRevenue"), F.col("CustomerKey"))))
    )
