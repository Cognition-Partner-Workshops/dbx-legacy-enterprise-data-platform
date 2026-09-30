"""SLS_Load_QuotaAttainment: regional attainment against Sales.SalesQuotas."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_performance import fiscal
from sales_performance.commissions import writeMetrics
from sales_performance.common import readTable, readTableOrEmpty, saveTable, withAudit
from sales_performance.sale_line import FACT_ORDER_TABLE, QUOTA_TABLE, SALE_LINE_TABLE

PACKAGE = "SLS_Load_QuotaAttainment"
ATTAINMENT_TABLE = "gold_quota_attainment"
CREDIT_NOTE_TABLE = "silver_credit_note"
MONEY = "decimal(19,4)"


def attainmentBand(quotaAmount, attainmentPercent):
    return (
        F.when(quotaAmount.isNull() | (quotaAmount == 0), "NOQUOTA")
        .when(attainmentPercent >= 120, "OVER120")
        .when(attainmentPercent >= 100, "AT")
        .when(attainmentPercent >= 90, "NEAR")
        .otherwise("UNDER")
    )


def regionalRevenue(lines: DataFrame, orders: DataFrame = None, creditNotes: DataFrame = None) -> DataFrame:
    """Per salesperson / territory / fiscal period revenue using the regional definitions."""
    active = lines.filter(~F.col("is_reversal"))
    na = active.filter(F.col("region_code") == "NA").withColumn(
        "measure", F.col("extended_price") + F.coalesce(F.col("tax_amount"), F.lit(0))
    )
    eu = active.filter(F.col("region_code") == "EU").withColumn("measure", F.col("net_amount"))
    keys = ["region_code", "territory_code", "salesperson_id", "fiscal_calendar_code", "fiscal_period_label"]
    frames = [
        na.groupBy(*keys).agg(F.sum("measure").cast(MONEY).alias("attainment_amount"), F.lit("NA_INVOICED_GROSS").alias("measure_code"))
    ]
    euAgg = eu.groupBy(*keys).agg(F.sum("measure").cast(MONEY).alias("attainment_amount"))
    if creditNotes is not None:
        cn = creditNotes.groupBy(*keys).agg(F.sum("credit_amount").cast(MONEY).alias("credit_amount"))
        euAgg = (
            euAgg.join(cn, keys, "left")
            .withColumn("attainment_amount", (F.col("attainment_amount") - F.coalesce(F.col("credit_amount"), F.lit(0))).cast(MONEY))
            .drop("credit_amount")
        )
    frames.append(euAgg.withColumn("measure_code", F.lit("EU_NET_AFTER_CREDITS")))
    if orders is not None:
        ap = orders.filter(F.col("region_code") == "APAC")
        calendar = F.lit(fiscal.CALENDAR_APAC)
        ap = ap.withColumn("fiscal_calendar_code", calendar).withColumn(
            "fiscal_period_label", fiscal.fiscalPeriodLabel(F.col("order_date"), calendar)
        )
        frames.append(
            ap.groupBy(*keys)
            .agg(F.sum("extended_price").cast(MONEY).alias("attainment_amount"))
            .withColumn("measure_code", F.lit("APAC_ORDER_INTAKE"))
        )
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


def calculateQuotaAttainment(revenue: DataFrame, quotas: DataFrame) -> DataFrame:
    q = quotas.select(
        F.col("salesperson_id").alias("q_salesperson_id"),
        F.col("territory_code").alias("q_territory_code"),
        F.col("fiscal_period_label").alias("q_period"),
        "quota_amount",
        "quota_currency_code",
        "stretch_quota_amount",
    )
    joined = revenue.join(
        q,
        (revenue["salesperson_id"] == q["q_salesperson_id"])
        & (revenue["territory_code"] == q["q_territory_code"])
        & (revenue["fiscal_period_label"] == q["q_period"]),
        "full_outer",
    )
    df = joined.select(
        F.coalesce(F.col("region_code"), F.substring(F.col("q_territory_code"), 1, 2)).alias("region_code"),
        F.coalesce(F.col("territory_code"), F.col("q_territory_code")).alias("territory_code"),
        F.coalesce(F.col("salesperson_id"), F.col("q_salesperson_id")).alias("salesperson_id"),
        F.col("fiscal_calendar_code"),
        F.coalesce(F.col("fiscal_period_label"), F.col("q_period")).alias("fiscal_period_label"),
        F.col("measure_code"),
        F.coalesce(F.col("attainment_amount"), F.lit(0)).cast(MONEY).alias("attainment_amount"),
        F.col("quota_amount"),
        F.col("stretch_quota_amount"),
        F.col("quota_currency_code"),
    )
    pct = (
        F.when(F.col("quota_amount").isNotNull() & (F.col("quota_amount") > 0), (F.col("attainment_amount") / F.col("quota_amount") * 100))
        .otherwise(F.lit(0))
        .cast("decimal(9,2)")
    )
    return (
        df.withColumn("attainment_percent", pct)
        .withColumn("attainment_band", attainmentBand(F.col("quota_amount"), F.col("attainment_percent")))
        .withColumn("is_missing_quota", F.col("quota_amount").isNull())
    )


def runQuotaAttainment(spark: SparkSession, batchId: int):
    lines = readTable(spark, SALE_LINE_TABLE)
    orders = readTableOrEmpty(
        spark,
        FACT_ORDER_TABLE,
        "region_code string, territory_code string, salesperson_id bigint, order_date date, extended_price decimal(19,4)",
    )
    quotas = readTable(spark, QUOTA_TABLE)
    creditNotes = readTableOrEmpty(
        spark,
        CREDIT_NOTE_TABLE,
        "region_code string, territory_code string, salesperson_id bigint, fiscal_calendar_code string, fiscal_period_label string, credit_amount decimal(19,4)",
    )
    revenue = regionalRevenue(lines, orders, creditNotes)
    attainment = withAudit(calculateQuotaAttainment(revenue, quotas), PACKAGE, batchId)
    saveTable(attainment, ATTAINMENT_TABLE)
    metrics = {
        "attainment_row_count": attainment.count(),
        "missing_quota_count": attainment.filter(F.col("is_missing_quota"))
        .select("territory_code", "salesperson_id", "fiscal_period_label")
        .distinct()
        .count(),
    }
    writeMetrics(spark, PACKAGE, metrics, batchId)
    return metrics
