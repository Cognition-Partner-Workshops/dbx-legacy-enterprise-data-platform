"""AGG_Refresh_SupplierPerformance - monthly supplier performance aggregate (Aggregate.Supplier Performance)."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from procurement import io
from procurement.config import qualified
from procurement.dimensions import GOLD_DIM_SUPPLIER
from procurement.facts import GOLD_FACT_PURCHASE_P2P, GOLD_FACT_PURCHASE_RECEIPT
from procurement.marts import GOLD_AGG_CONTRACT_COMPLIANCE, GOLD_AGG_SUPPLIER_SCORECARD, GOLD_FACT_PURCHASE_SPEND, GOLD_FACT_RECEIPT_MATCHING

GOLD_AGG_SUPPLIER_PERFORMANCE = "gold_agg_supplier_performance"


def refreshSupplierPerformance(purchase: DataFrame, receipts: DataFrame, matching: DataFrame, spend: DataFrame, compliance: DataFrame,
                               scorecard: DataFrame, dimSupplier: DataFrame, batchId) -> DataFrame:
    """Integration.usp_RefreshAggregateSupplierPerformance: full rebuild per (calendar_month, supplier_key, region_code)."""
    p = purchase.groupBy(F.date_format("date_key", "yyyy-MM").alias("calendar_month"), "supplier_key", "region_code").agg(
        F.countDistinct("purchase_order_number").alias("purchase_order_count"), F.count("*").alias("purchase_line_count"),
        F.sum("extended_cost_reporting").alias("recognised_spend_reporting"), F.sum("freight_in_amount").alias("freight_in_reporting"),
        F.sum("customs_duty_amount").alias("customs_duty_reporting"), F.sum("landed_cost_reporting").alias("landed_cost_reporting"),
        F.sum(F.when(F.col("match_status_code").isin("QTY_VARIANCE", "TWO_WAY"), 1).otherwise(0)).alias("po_match_exception_count"),
    )
    r = receipts.groupBy(F.date_format("receipt_date_key", "yyyy-MM").alias("calendar_month"), "supplier_key", "region_code").agg(
        F.count("*").alias("receipt_count"), F.sum(F.when(F.col("on_time_flag"), 1).otherwise(0)).alias("on_time_receipt_count"),
        F.round(F.avg(F.when(F.col("on_time_flag"), 100.0).otherwise(0.0)), 2).alias("on_time_percent"),
        F.round(F.avg(F.when(F.col("in_full_flag"), 100.0).otherwise(0.0)), 2).alias("in_full_percent"),
        F.round(F.avg(F.when(F.col("on_time_flag") & F.col("in_full_flag"), 100.0).otherwise(0.0)), 2).alias("on_time_in_full_percent"),
        F.round(F.avg("days_late_versus_promise"), 2).alias("average_days_late"), F.round(F.avg("lead_time_days"), 2).alias("average_lead_time_days"),
        F.round(F.stddev_samp("lead_time_days"), 2).alias("lead_time_variability_days"), F.sum("quantity_rejected_base_uom").alias("rejected_quantity"),
        F.round(F.sum("quantity_rejected_base_uom") * 100.0 / F.sum("quantity_received_base_uom"), 2).alias("quality_reject_rate_percent"),
        F.sum("price_variance_amount").alias("price_variance_reporting"),
    )
    m = matching.groupBy(F.date_format("receipt_date_key", "yyyy-MM").alias("calendar_month"), "supplier_key", "region_code").agg(
        F.sum(F.when(F.col("match_status_code") != "MATCHED", 1).otherwise(0)).alias("match_exception_count"), F.sum("grni_accrual_amount_reporting").alias("grni_accrual_reporting"),
    )
    supplierKeys = dimSupplier.where("is_current_row").select("supplier_business_key", F.col("supplier_key").alias("_sk"))
    s = spend.join(supplierKeys, "supplier_business_key", "left").groupBy("calendar_month", F.coalesce(F.col("_sk"), F.lit(0)).alias("supplier_key"), "region_code").agg(
        F.sum("contract_covered_spend_usd").alias("contract_covered_spend"), F.sum(F.when(F.col("spend_class_code") == "MAVERICK", F.col("spend_amount_usd")).otherwise(0)).alias("maverick_spend_reporting"),
        F.sum("expected_rebate_usd").alias("discount_captured_reporting"),
    )
    c = compliance.join(supplierKeys, "supplier_business_key", "left").groupBy("calendar_month", F.coalesce(F.col("_sk"), F.lit(0)).alias("supplier_key"), "region_code").agg(
        F.max("compliance_code").alias("compliance_code"), F.max("contract_number").alias("contract_number"),
    )
    sc = scorecard.join(supplierKeys, "supplier_business_key", "left").select(F.coalesce(F.col("_sk"), F.lit(0)).alias("supplier_key"), "scorecard_rating_code", "scorecard_score").dropDuplicates(["supplier_key"])
    keys = p.select("calendar_month", "supplier_key", "region_code").union(r.select("calendar_month", "supplier_key", "region_code")).union(s.select("calendar_month", "supplier_key", "region_code")).distinct()
    j = keys.join(p, ["calendar_month", "supplier_key", "region_code"], "left").join(r, ["calendar_month", "supplier_key", "region_code"], "left") \
        .join(m, ["calendar_month", "supplier_key", "region_code"], "left").join(s, ["calendar_month", "supplier_key", "region_code"], "left") \
        .join(c, ["calendar_month", "supplier_key", "region_code"], "left").join(sc, "supplier_key", "left")
    ytd = Window.partitionBy(F.substring("calendar_month", 1, 4), "supplier_key", "region_code").orderBy("calendar_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    rank = Window.partitionBy("calendar_month", "region_code").orderBy(F.col("recognised_spend_reporting").desc_nulls_last())
    dec = lambda c: F.coalesce(F.col(c), F.lit(0)).cast("decimal(19,4)")  # noqa: E731
    return j.select(
        "calendar_month", "supplier_key", "region_code", "contract_number", F.coalesce(F.col("purchase_order_count"), F.lit(0)).alias("purchase_order_count"),
        F.coalesce(F.col("purchase_line_count"), F.lit(0)).alias("purchase_line_count"), F.coalesce(F.col("receipt_count"), F.lit(0)).alias("receipt_count"),
        dec("recognised_spend_reporting").alias("recognised_spend_reporting"),
        F.sum(F.coalesce(F.col("recognised_spend_reporting"), F.lit(0))).over(ytd).cast("decimal(19,4)").alias("year_to_date_spend_reporting"),
        dec("contract_covered_spend").alias("contract_covered_spend"), dec("maverick_spend_reporting").alias("maverick_spend_reporting"),
        dec("freight_in_reporting").alias("freight_in_reporting"), dec("customs_duty_reporting").alias("customs_duty_reporting"), dec("landed_cost_reporting").alias("landed_cost_reporting"),
        F.when(F.col("region_code") == "EU", "EXW_PLUS_FREIGHT_DUTY").when(F.col("region_code") == "APAC", "CIF").otherwise("EXW").alias("landed_cost_basis_code"),
        F.coalesce(F.col("on_time_receipt_count"), F.lit(0)).alias("on_time_receipt_count"), F.col("on_time_percent").cast("decimal(9,2)"), F.col("in_full_percent").cast("decimal(9,2)"),
        F.col("on_time_in_full_percent").cast("decimal(9,2)"), F.col("average_days_late").cast("decimal(9,2)"), F.col("average_lead_time_days").cast("decimal(9,2)"),
        F.col("lead_time_variability_days").cast("decimal(9,2)"), dec("rejected_quantity").alias("rejected_quantity"), F.col("quality_reject_rate_percent").cast("decimal(9,2)"),
        (F.coalesce(F.col("match_exception_count"), F.lit(0)) + F.coalesce(F.col("po_match_exception_count"), F.lit(0))).alias("match_exception_count"),
        dec("price_variance_reporting").alias("price_variance_reporting"), dec("grni_accrual_reporting").alias("grni_accrual_reporting"),
        dec("discount_captured_reporting").alias("discount_captured_reporting"), "compliance_code", "scorecard_rating_code", F.col("scorecard_score").cast("decimal(9,2)"),
        F.rank().over(rank).alias("rank_in_region_by_spend"), F.lit(int(batchId)).cast("long").alias("refresh_batch_id"), F.current_timestamp().alias("refreshed_datetime"),
    )


def runAggregateSupplierPerformance(spark, batchId):
    packageName = "AGG_Refresh_SupplierPerformance"
    out = refreshSupplierPerformance(
        spark.table(qualified(GOLD_FACT_PURCHASE_P2P)), spark.table(qualified(GOLD_FACT_PURCHASE_RECEIPT)), spark.table(qualified(GOLD_FACT_RECEIPT_MATCHING)),
        spark.table(qualified(GOLD_FACT_PURCHASE_SPEND)), spark.table(qualified(GOLD_AGG_CONTRACT_COMPLIANCE)), spark.table(qualified(GOLD_AGG_SUPPLIER_SCORECARD)),
        spark.table(qualified(GOLD_DIM_SUPPLIER)), batchId,
    )
    io.writeDelta(out, qualified(GOLD_AGG_SUPPLIER_PERFORMANCE))
    rows = spark.table(qualified(GOLD_AGG_SUPPLIER_PERFORMANCE)).count()
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=rows, rowsInserted=rows)
    return rows
