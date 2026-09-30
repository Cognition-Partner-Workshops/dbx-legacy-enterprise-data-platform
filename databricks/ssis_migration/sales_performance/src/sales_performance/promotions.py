"""SLS_Load_PromotionRedemption: attribute redemptions to promotions and summarise them."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_performance.commissions import writeMetrics
from sales_performance.common import readOltp, readTable, saveTable, snakeCaseColumns, withAudit
from sales_performance.sale_line import SALE_LINE_TABLE

PACKAGE = "SLS_Load_PromotionRedemption"
PROMOTION_TABLE = "silver_promotion"
PROMOTION_LINE_TABLE = "silver_promotion_line"
REDEMPTION_TABLE = "gold_promotion_redemption"
SUMMARY_TABLE = "gold_promotion_summary"
MONEY = "decimal(19,4)"
SPILL_DAYS = {"NA": 30, "APAC": 14, "EU": 0}


def spillDays(regionCode, strictMode: bool):
    if strictMode:
        return F.lit(0)
    return F.when(regionCode == "NA", 30).when(regionCode == "APAC", 14).otherwise(0)


def attributeRedemptions(redemptions: DataFrame, promotions: DataFrame, promotionLines: DataFrame, strictMode: bool = False) -> DataFrame:
    """Classify each redemption as ATTRIBUTED (inside the promotion window), SPILL (inside the
    regional spill window after the end date) or OUTSIDE, and cost the discount."""
    firstLine = (
        promotionLines.withColumn(
            "_rn", F.row_number().over(Window.partitionBy("promotion_id").orderBy("line_sequence", "promotion_line_id"))
        )
        .filter(F.col("_rn") == 1)
        .select(
            "promotion_id", F.col("discount_percent").alias("line_discount_percent"), F.col("discount_amount").alias("line_discount_amount")
        )
    )
    p = promotions.select(
        "promotion_id",
        "promotion_code",
        F.trim(F.col("region_code")).alias("promotion_region_code"),
        "promotion_type",
        "start_date",
        "end_date",
        "budget_amount",
    ).join(firstLine, "promotion_id", "left")
    df = redemptions.join(p, "promotion_id", "left")
    windowEnd = F.date_add(F.col("end_date"), spillDays(F.col("promotion_region_code"), strictMode))
    redeemedDate = F.to_date(F.col("redeemed_when"))
    attribution = (
        F.when(F.col("promotion_code").isNull(), "UNKNOWN_PROMOTION")
        .when(redeemedDate.between(F.col("start_date"), F.col("end_date")), "ATTRIBUTED")
        .when((redeemedDate > F.col("end_date")) & (redeemedDate <= windowEnd), "SPILL")
        .otherwise("OUTSIDE")
    )
    redeemedAmount = F.coalesce(F.col("redeemed_amount"), F.col("discount_value"), F.lit(0)).cast(MONEY)
    discountCost = (
        F.when(
            F.upper(F.col("promotion_type")) == "PERCENTAGE", redeemedAmount * F.coalesce(F.col("line_discount_percent"), F.lit(0)) / 100
        )
        .when(F.upper(F.col("promotion_type")) == "FIXED", F.coalesce(F.col("line_discount_amount"), F.col("discount_value")))
        .otherwise(F.col("discount_value"))
    ).cast(MONEY)
    return df.select(
        "promotion_redemption_id",
        "promotion_id",
        "promotion_code",
        F.col("promotion_region_code").alias("region_code"),
        "promotion_type",
        "order_id",
        "customer_id",
        "coupon_code",
        redeemedDate.alias("redeemed_date"),
        "start_date",
        "end_date",
        windowEnd.alias("attribution_window_end"),
        attribution.alias("attribution_code"),
        redeemedAmount.alias("redeemed_amount"),
        discountCost.alias("discount_cost"),
        "currency_code",
        "redemption_status",
    )


def summarizePromotions(attributed: DataFrame, promotions: DataFrame) -> DataFrame:
    inWindow = attributed.filter(F.col("attribution_code").isin("ATTRIBUTED", "SPILL"))
    agg = inWindow.groupBy("promotion_id").agg(
        F.sum("redeemed_amount").cast(MONEY).alias("redeemed_amount"),
        F.sum("discount_cost").cast(MONEY).alias("discount_cost"),
        F.count("*").alias("redemption_count"),
        F.countDistinct("customer_id").alias("distinct_customer_count"),
        F.sum(F.when(F.col("attribution_code") == "ATTRIBUTED", 1).otherwise(0)).alias("attributed_redemption_count"),
        F.sum(F.when(F.col("attribution_code") == "SPILL", 1).otherwise(0)).alias("spill_redemption_count"),
    )
    return (
        promotions.select(
            "promotion_id",
            "promotion_code",
            "promotion_name",
            F.trim(F.col("region_code")).alias("region_code"),
            "promotion_type",
            "start_date",
            "end_date",
            "budget_amount",
            "budget_currency_code",
        )
        .join(agg, "promotion_id", "left")
        .select(
            "*",
        )
        .na.fill(
            {
                "redeemed_amount": 0,
                "discount_cost": 0,
                "redemption_count": 0,
                "distinct_customer_count": 0,
                "attributed_redemption_count": 0,
                "spill_redemption_count": 0,
            }
        )
        .withColumn("is_over_budget", F.col("budget_amount").isNotNull() & (F.col("discount_cost") > F.col("budget_amount")))
    )


def loadPromotionSources(spark: SparkSession):
    promotions = snakeCaseColumns(readOltp(spark, "Sales", "Promotions"))
    lines = snakeCaseColumns(readOltp(spark, "Sales", "PromotionLines"))
    redemptions = snakeCaseColumns(readOltp(spark, "Sales", "PromotionRedemptions"))
    orderAmounts = readTable(spark, SALE_LINE_TABLE).groupBy("invoice_number").agg(F.sum("extended_price").alias("redeemed_amount"))
    redemptions = redemptions.join(orderAmounts, redemptions["order_id"] == orderAmounts["invoice_number"], "left").drop("invoice_number")
    return promotions, lines, redemptions


def runPromotionRedemption(spark: SparkSession, batchId: int, strictMode: bool = False):
    promotions, lines, redemptions = loadPromotionSources(spark)
    saveTable(withAudit(promotions, PACKAGE, batchId), PROMOTION_TABLE)
    saveTable(withAudit(lines, PACKAGE, batchId), PROMOTION_LINE_TABLE)
    attributed = withAudit(attributeRedemptions(redemptions, promotions, lines, strictMode), PACKAGE, batchId).cache()
    saveTable(attributed, REDEMPTION_TABLE)
    summary = withAudit(summarizePromotions(attributed, promotions), PACKAGE, batchId)
    saveTable(summary, SUMMARY_TABLE)
    metrics = {
        "redemption_row_count": attributed.count(),
        "spill_redemption_count": attributed.filter(F.col("attribution_code") == "SPILL").count(),
        "over_budget_promotion_count": summary.filter(F.col("is_over_budget")).count(),
    }
    writeMetrics(spark, PACKAGE, metrics, batchId)
    return metrics
