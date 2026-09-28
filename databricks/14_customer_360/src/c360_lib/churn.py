"""C360_Build_ChurnFlags business rules (feature join, rule scores, risk band, outreach queue)."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from c360_lib.profile import activeCustomer

DEFAULT_HIGH_RISK_THRESHOLD = 60
DEFAULT_ORDER_GAP_MULTIPLIER = 2
MEDIUM_RISK_THRESHOLD = 30
RULE_SET_VERSION = "2019.3"
QUEUE_REASON_CHURN_HIGH = "CHURN_HIGH"


def buildChurnFeatureSet(rollingMetric: DataFrame, dimCustomer: DataFrame, salesSummary: DataFrame,
                         aggRolling12: DataFrame, asOfDate) -> DataFrame:
    """Reconstructed work.ChurnFeatureSet.

    - DaysSinceLastOrder / AverageBasketAmount / ReturnRatePercent: Customer360.CustomerRollingMetric
    - AverageOrderGapDays: (LastOrderDate - FirstOrderDate) / (LifetimeOrderCount - 1), 0 when < 2 orders
    - PriorAverageBasketAmount: Aggregate.Customer Rolling 12 Month, month offsets 12..23
      (the window immediately before the current rolling window): SUM(Net Revenue Reporting) / SUM(Order Count)
    - IsOnCreditHold: Dimension.Customer current row
    """
    dec = DecimalType(18, 2)
    cust = dimCustomer.where(activeCustomer(asOfDate)).select(
        "CustomerKey", F.col("WWICustomerID").alias("CustomerId"),
        F.coalesce(F.col("IsOnCreditHold").cast("boolean"), F.lit(False)).alias("IsOnCreditHold"))
    gap = salesSummary.select(
        "CustomerKey",
        F.when(F.coalesce(F.col("LifetimeOrderCount"), F.lit(0)) < 2, F.lit(0).cast(dec))
         .otherwise((F.datediff(F.col("LastOrderDate"), F.col("FirstOrderDate")) / (F.col("LifetimeOrderCount") - 1)).cast(dec))
         .alias("AverageOrderGapDays"))
    prior = (
        aggRolling12.where(F.col("MonthOffset").between(12, 23))
        .groupBy("CustomerKey")
        .agg((F.sum("NetRevenueReporting") / F.sum("OrderCount")).cast(dec).alias("PriorAverageBasketAmount"))
    )
    return (
        rollingMetric.select("CustomerKey", "RegionCode", "DaysSinceLastOrder", "AverageBasketAmount", "ReturnRatePercent")
        .join(cust, "CustomerKey", "inner")
        .join(gap, "CustomerKey", "left")
        .join(prior, "CustomerKey", "left")
        .withColumn("AverageOrderGapDays", F.coalesce(F.col("AverageOrderGapDays"), F.lit(0).cast(dec)))
        .withColumn("PriorAverageBasketAmount", F.coalesce(F.col("PriorAverageBasketAmount"), F.lit(0).cast(dec)))
        .select("CustomerKey", "CustomerId", "RegionCode", "DaysSinceLastOrder", "AverageOrderGapDays",
                "AverageBasketAmount", "PriorAverageBasketAmount", "ReturnRatePercent", "IsOnCreditHold")
    )


def buildChurnSource(featureSet: DataFrame, loyaltyOverlay: DataFrame) -> DataFrame:
    """OLE DB Source 'work ChurnFeatureSet' (LEFT JOIN work.LoyaltyOverlay, NONE defaults)."""
    lo = loyaltyOverlay.select("CustomerId", "TierCode", "PreviousTierCode")
    return (
        featureSet.join(lo, "CustomerId", "left")
        .withColumn("TierCode", F.coalesce(F.col("TierCode"), F.lit("NONE")))
        .withColumn("PreviousTierCode", F.coalesce(F.col("PreviousTierCode"), F.lit("NONE")))
        .withColumn("IsOnCreditHold", F.coalesce(F.col("IsOnCreditHold").cast("boolean"), F.lit(False)))
    )


def scoreChurnRisk(source: DataFrame, highRiskThreshold: int = DEFAULT_HIGH_RISK_THRESHOLD,
                   orderGapMultiplier=DEFAULT_ORDER_GAP_MULTIPLIER) -> DataFrame:
    """Derived Columns 'Apply Churn Rules' + 'Band Churn Risk'."""
    df = (
        source
        .withColumn("RuleOrderGapScore",
                    F.when((F.col("AverageOrderGapDays") > 0)
                           & (F.col("DaysSinceLastOrder") > F.col("AverageOrderGapDays") * F.lit(orderGapMultiplier)), 30).otherwise(0))
        .withColumn("RuleBasketDeclineScore",
                    F.when((F.col("PriorAverageBasketAmount") > 0)
                           & (F.col("AverageBasketAmount") < F.col("PriorAverageBasketAmount") * 0.7), 20).otherwise(0))
        .withColumn("RuleReturnsScore", F.when(F.col("ReturnRatePercent") > 15, 15).otherwise(0))
        .withColumn("RuleTierLapseScore",
                    F.when((F.col("TierCode") != F.col("PreviousTierCode")) & (F.col("PreviousTierCode") != "NONE"), 15).otherwise(0))
        .withColumn("RuleCreditHoldScore", F.when(F.col("IsOnCreditHold"), 25).otherwise(0))
    )
    total = (F.col("RuleOrderGapScore") + F.col("RuleBasketDeclineScore") + F.col("RuleReturnsScore")
             + F.col("RuleTierLapseScore") + F.col("RuleCreditHoldScore"))
    return (
        df.withColumn("ChurnScore", total.cast("int"))
        .withColumn("ChurnRiskBandCode",
                    F.when(total >= F.lit(int(highRiskThreshold)), "HIGH")
                     .when(total >= MEDIUM_RISK_THRESHOLD, "MEDIUM").otherwise("LOW"))
        .withColumn("RuleSetVersion", F.lit(RULE_SET_VERSION))
    )


def splitHighRisk(scored: DataFrame) -> DataFrame:
    """Conditional Split 'Split Risk Bands' -> HighRisk output (Other output is unused)."""
    return scored.where(F.col("ChurnRiskBandCode") == "HIGH")


def newOutreachQueueRows(highRisk: DataFrame, outreachQueue: DataFrame, queuedAtUtc=None) -> DataFrame:
    """Execute SQL Task 'Queue High Risk For Outreach' (INSERT ... WHERE NOT EXISTS same customer
    with QueueReasonCode CHURN_HIGH)."""
    ts = F.lit(queuedAtUtc).cast("timestamp") if queuedAtUtc is not None else F.current_timestamp()
    queued = outreachQueue.where(F.col("QueueReasonCode") == QUEUE_REASON_CHURN_HIGH).select("CustomerId").distinct()
    return (
        highRisk.select("CustomerId", "RegionCode").distinct()
        .join(queued, "CustomerId", "left_anti")
        .withColumn("QueueReasonCode", F.lit(QUEUE_REASON_CHURN_HIGH))
        .withColumn("QueuedAtUtc", ts)
    )
