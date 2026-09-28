"""PRC_Load_SupplierScorecard: weighted supplier scorecard over a rolling window."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .common import MONEY, money, withBatchColumns, zeroMoney

BAND_NODATA = "NODATA"
BAND_A = "A"
BAND_B = "B"
BAND_C = "C"
BAND_D = "D"

DEFAULT_ON_TIME_WEIGHT = Decimal("0.4")
DEFAULT_ACCURACY_WEIGHT = Decimal("0.2")
DEFAULT_PRICE_WEIGHT = Decimal("0.2")
DEFAULT_QUALITY_WEIGHT = Decimal("0.2")

QUALITY_REJECT_CODE = "REJECT"


def buildScorecardMeasures(supplierDf: DataFrame, poHeaderDf: DataFrame, poLineDf: DataFrame,
                           receiptDf: DataFrame, apInvoiceLineDf: DataFrame,
                           businessDate: date, scoringWindowDays: int) -> DataFrame:
    """Execute SQL `Build Scorecard Measures` -> work.SupplierScorecard rows.

    Grain is supplier x region; the counts are over PO lines in the window (the legacy SUMs
    count lines, while OrderCount counts distinct purchase orders carrying a receipt).
    """
    windowStart = businessDate - timedelta(days=int(scoringWindowDays))
    s = supplierDf.alias("s")
    poh = poHeaderDf.alias("poh")
    pol = poLineDf.alias("pol")
    r = receiptDf.alias("r")
    ail = apInvoiceLineDf.alias("ail")

    joined = (
        s.join(poh, F.col("poh.SupplierId") == F.col("s.SupplierId"), "inner")
        .join(pol, F.col("pol.PurchaseOrderNumber") == F.col("poh.PurchaseOrderNumber"), "inner")
        .join(r, (F.col("r.PurchaseOrderNumber") == F.col("pol.PurchaseOrderNumber"))
              & (F.col("r.LineNumber") == F.col("pol.LineNumber")), "left")
        .join(ail, (F.col("ail.PurchaseOrderNumber") == F.col("pol.PurchaseOrderNumber"))
              & (F.col("ail.PurchaseOrderLineNumber") == F.col("pol.LineNumber")), "left")
        .where(F.col("poh.OrderDate") >= F.lit(windowStart))
    )
    invoicedPrice = F.coalesce(F.col("ail.InvoicedUnitPrice"), F.col("pol.ExpectedUnitPricePerOuter"))
    return joined.groupBy(F.col("s.SupplierId").alias("SupplierId"), F.col("s.RegionCode").alias("RegionCode")).agg(
        F.countDistinct(F.col("r.PurchaseOrderNumber")).alias("OrderCount"),
        F.sum(F.when(F.col("r.ReceivedAtUtc") <= F.col("pol.PromisedDate"), 1).otherwise(0)).alias("OnTimeCount"),
        F.sum(F.when(F.col("r.ReceivedOuters") == F.col("pol.OrderedOuters"), 1).otherwise(0)).alias("QuantityAccurateCount"),
        F.sum(F.when(F.abs(invoicedPrice - F.col("pol.ExpectedUnitPricePerOuter")) < 0.005, 1).otherwise(0)).alias("PriceAdherentCount"),
        F.sum(F.when(F.col("r.QualityStatusCode") == QUALITY_REJECT_CODE, 1).otherwise(0)).alias("QualityRejectCount"),
        F.sum(F.when(F.col("ail.ApInvoiceNumber").isNull(), 1).otherwise(0)).alias("InvoiceExceptionCount"),
        F.lit(int(scoringWindowDays)).alias("WindowDays"),
    )


def scoreSuppliers(measuresDf: DataFrame, weightsDf: DataFrame, minimumOrdersForScore: int) -> DataFrame:
    """Lookup `Lookup Scoring Weights` (ignore no-match) + the two derived-column components.

    Missing regional weights fall back to 0.4 / 0.2 / 0.2 / 0.2. The band is driven by on-time
    percent alone, exactly as the legacy expression does; the weighted score is carried alongside.
    """
    weights = weightsDf.select("RegionCode", "OnTimeWeight", "AccuracyWeight", "PriceWeight", "QualityWeight")
    df = measuresDf.join(weights, "RegionCode", "left")

    def pct(numerator):
        return F.when(F.col("OrderCount") == 0, zeroMoney()).otherwise(money(numerator * 100 / F.col("OrderCount")))

    df = (
        df.withColumn("OnTimePercent", pct(F.col("OnTimeCount")))
        .withColumn("AccuracyPercent", pct(F.col("QuantityAccurateCount")))
        .withColumn("PricePercent", pct(F.col("PriceAdherentCount")))
        .withColumn("QualityPercent", pct(F.col("OrderCount") - F.col("QualityRejectCount")))
    )
    score = (
        F.col("OnTimePercent") * F.coalesce(F.col("OnTimeWeight"), F.lit(DEFAULT_ON_TIME_WEIGHT))
        + F.col("AccuracyPercent") * F.coalesce(F.col("AccuracyWeight"), F.lit(DEFAULT_ACCURACY_WEIGHT))
        + F.col("PricePercent") * F.coalesce(F.col("PriceWeight"), F.lit(DEFAULT_PRICE_WEIGHT))
        + F.col("QualityPercent") * F.coalesce(F.col("QualityWeight"), F.lit(DEFAULT_QUALITY_WEIGHT))
    )
    band = (
        F.when(F.col("OrderCount") < F.lit(int(minimumOrdersForScore)), F.lit(BAND_NODATA))
        .when(F.col("OnTimePercent") >= 95, F.lit(BAND_A))
        .when(F.col("OnTimePercent") >= 85, F.lit(BAND_B))
        .when(F.col("OnTimePercent") >= 70, F.lit(BAND_C))
        .otherwise(F.lit(BAND_D))
    )
    return df.withColumn("SupplierScore", money(score)).withColumn("ScoreBandCode", band)


def countUnscoredSuppliers(measuresDf: DataFrame, minimumOrdersForScore: int) -> int:
    """Execute SQL `Count Unscored Suppliers`."""
    return measuresDf.where(F.col("OrderCount") < F.lit(int(minimumOrdersForScore))).count()


def toAggSupplierPerformance(scoredDf: DataFrame, supplierKeysDf: DataFrame, businessDate: date,
                             batchId: int, refreshedAtUtc: datetime | None = None) -> DataFrame:
    """Shape scored rows for the MERGE into gold.agg_supplier_performance (supplier x month grain)."""
    calendarMonth = date(businessDate.year, businessDate.month, 1)
    df = scoredDf.join(supplierKeysDf, "SupplierId", "inner")
    df = withBatchColumns(df, batchId, refreshedAtUtc)
    return df.select(
        F.lit(calendarMonth).alias("calendar_month"),
        F.col("SupplierKey").alias("supplier_key"),
        F.col("SupplierId").alias("wwi_supplier_id"),
        F.col("RegionCode").alias("region_code"),
        F.col("OrderCount").alias("purchase_order_count"),
        F.col("OnTimeCount").alias("on_time_receipt_count"),
        F.col("QuantityAccurateCount").alias("quantity_accurate_count"),
        F.col("PriceAdherentCount").alias("price_adherent_count"),
        F.col("QualityRejectCount").alias("quality_reject_count"),
        F.col("InvoiceExceptionCount").alias("match_exception_count"),
        F.col("WindowDays").alias("scoring_window_days"),
        F.col("OnTimePercent").cast("decimal(9,4)").alias("on_time_percent"),
        F.col("AccuracyPercent").cast("decimal(9,4)").alias("in_full_percent"),
        F.col("PricePercent").cast("decimal(9,4)").alias("price_adherence_percent"),
        F.col("QualityPercent").cast("decimal(9,4)").alias("quality_percent"),
        F.col("SupplierScore").alias("supplier_score"),
        F.col("ScoreBandCode").alias("scorecard_rating_code"),
        F.col("BatchId").alias("refresh_batch_id"),
        F.col("LoadedAtUtc").alias("refreshed_datetime"),
    )
