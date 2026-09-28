"""SLS_Load_PromotionRedemption: attribute redemptions to promotions inside the
region's attribution window (NA +30 days, APAC +14 days, EU promotion window
only), summarise into Aggregate.Promotion Effectiveness, keep spill separately
and flag over-budget promotions.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sales_common import MONEY, resolveColumns

PROMOTION_COLUMNS = {
    "PromotionId": ["PromotionId", "PromotionBusinessKey", "PromotionCode"],
    "PromotionCode": ["PromotionCode"],
    "PromotionName": ["PromotionName"],
    "RegionCode": ["RegionCode"],
    "StartDate": ["StartDate"],
    "EndDate": ["EndDate"],
    "DiscountTypeCode": ["DiscountTypeCode", "PromotionTypeCode"],
    "DiscountValue": ["DiscountValue", "DiscountPercent", "DiscountAmount"],
    "BudgetAmount": ["BudgetAmount", "BudgetAmountUsd"],
    "LoadBatchId": ["BatchId", "LoadBatchId"],
}
REDEMPTION_COLUMNS = {
    "PromotionId": ["PromotionId", "PromotionBusinessKey", "PromotionCode"],
    "SaleLineId": ["SaleLineId", "SaleLineBusinessKey"],
    "InvoiceDate": ["InvoiceDate"],
    "CustomerId": ["CustomerId", "CustomerBusinessKey"],
    "StockItemId": ["StockItemId", "StockItemBusinessKey"],
    "RedeemedAmount": ["RedeemedAmount"],
    "RedemptionChannelCode": ["RedemptionChannelCode", "SalesChannelCode"],
}
ATTRIBUTION_MODES = ("REGIONAL", "STRICT")


def legacyPromotions(df: DataFrame) -> DataFrame:
    return resolveColumns(df, PROMOTION_COLUMNS, optional=("PromotionName",))


def legacyRedemptions(df: DataFrame) -> DataFrame:
    return resolveColumns(df, REDEMPTION_COLUMNS, optional=("StockItemId", "RedemptionChannelCode"))


def attributionEndDate():
    """CASE p.RegionCode WHEN 'NA' THEN EndDate+30 WHEN 'APAC' THEN EndDate+14 ELSE EndDate END."""
    return (F.when(F.col("RegionCode") == "NA", F.date_add(F.col("EndDate"), 30))
             .when(F.col("RegionCode") == "APAC", F.date_add(F.col("EndDate"), 14))
             .otherwise(F.col("EndDate")))


def joinRedemptions(promotions: DataFrame, redemptions: DataFrame) -> DataFrame:
    r = redemptions.withColumnRenamed("PromotionId", "r_PromotionId")
    return (promotions.join(r, promotions["PromotionId"] == r["r_PromotionId"], "inner")
                      .drop("r_PromotionId")
                      .withColumn("AttributionEndDate", attributionEndDate()))


def classifyAttribution(df: DataFrame, attributionMode: str) -> DataFrame:
    """Derived column 'Classify Attribution'."""
    mode = (attributionMode or "REGIONAL").upper()
    windowEnd = F.col("EndDate") if mode == "STRICT" else F.col("AttributionEndDate")
    return (df.withColumn("IsInWindow",
                          (F.col("InvoiceDate") >= F.col("StartDate")) & (F.col("InvoiceDate") <= windowEnd))
              .withColumn("DiscountCostAmount",
                          F.when(F.col("DiscountTypeCode") == "PCT",
                                 F.col("RedeemedAmount") * F.col("DiscountValue") / 100)
                           .otherwise(F.col("DiscountValue")).cast(MONEY)))


def splitSpill(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    attributed = df.where(F.coalesce(F.col("IsInWindow"), F.lit(False)))
    spill = df.where(~F.coalesce(F.col("IsInWindow"), F.lit(False)))
    return attributed, spill


def summarisePromotion(attributed: DataFrame) -> DataFrame:
    """Aggregate 'Summarise Promotion' grouped by PromotionId, PromotionCode, RegionCode."""
    return (attributed.groupBy("PromotionId", "PromotionCode", "RegionCode")
                      .agg(F.sum("RedeemedAmount").cast(MONEY).alias("RedeemedAmount"),
                           F.sum("DiscountCostAmount").cast(MONEY).alias("DiscountCostAmount"),
                           F.count("SaleLineId").alias("RedemptionCount"),
                           F.countDistinct("CustomerId").alias("RedeemingCustomerCount")))


def flagOverBudget(summary: DataFrame, promotions: DataFrame) -> DataFrame:
    """'Flag Over Budget Promotions': [Budget Status] = 'OVER' where discount cost > budget."""
    budgets = (promotions.select(F.col("PromotionId").alias("b_PromotionId"), "BudgetAmount")
                         .dropDuplicates(["b_PromotionId"]))
    return (summary.join(budgets, summary["PromotionId"] == budgets["b_PromotionId"], "left")
                   .drop("b_PromotionId")
                   .withColumn("BudgetStatus",
                               F.when(F.col("DiscountCostAmount") > F.col("BudgetAmount"), F.lit("OVER"))))


def toPromotionEffectiveness(flagged: DataFrame, batchId: int) -> DataFrame:
    return (flagged.withColumn("RefreshBatchId", F.lit(int(batchId)).cast("long"))
                   .withColumn("RefreshedDatetime", F.current_timestamp()))
