"""AGG_Refresh_DeliveryPerformanceSummary -> gold_agg_delivery_performance_summary (+ lane tier / thin-sample outputs).

The legacy package has two overlapping implementations that we preserve side by side:

* ``Integration.usp_RefreshAggregateDeliveryPerformance`` (the T-SQL that owns the DW table): ISO-week x carrier x
  warehouse site x sales territory x region x service level rebuild of the trailing ``@WeeksToRefresh`` weeks
  (delete + reinsert), regional on-time definition (APAC gets the customs-hold grace), regional SLA targets
  (NA 96 / EU 98 / other 92), breach flag, and the EU-only service credit (2 % of freight per point below target,
  capped at 10 %).
* The ``Rebuild Delivery Performance`` data flow inside the .dtsx: delivered-date x carrier x origin/destination lane
  grain with a service-tier band (PLATINUM/GOLD/SILVER/REVIEW), freight-per-kg and a "Reject Thin Sample" conditional
  split (fewer than 5 shipments -> quality-gate reject).  The DW table cannot hold those columns, so they land in
  ``gold_agg_delivery_performance_lane`` and the rejects in ``gold_agg_delivery_performance_thin_sample``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from logistics_returns.common import appendTable, logRowCount, overwriteTable, readTableOrEmpty, tableExists
from logistics_returns.config import RunContext, Tables

DEFAULT_WEEKS_TO_REFRESH = 6
THIN_SAMPLE_THRESHOLD = 5
EU_SERVICE_CREDIT_RATE_PER_POINT = 0.02
EU_SERVICE_CREDIT_CAP = 0.10
PACKAGE_NAME = "AGG_Refresh_DeliveryPerformanceSummary"
LEGACY_OBJECT = "Aggregate.Delivery Performance Summary"

AGGREGATE_GRAIN = [
    "iso_week_start_date", "iso_week_number", "carrier_key", "warehouse_site_key", "sales_territory_key",
    "region_code", "service_level_code",
]  # fmt: skip


@dataclass
class AggregateResult:
    rowsRead: int
    rowsDeletedForWindow: int
    rowsAggregated: int
    rowsRejected: int


def slaTargetPercent(regionCode: F.Column) -> F.Column:
    region = F.upper(regionCode)
    return F.when(region == "NA", F.lit(96.0)).when(region == "EU", F.lit(98.0)).otherwise(F.lit(92.0)).cast("decimal(9,4)")


def isoWeekStart(dateCol: F.Column) -> F.Column:
    """T-SQL ``DATEADD(DAY, 1 - DATEPART(WEEKDAY, d), d)`` with the default ``DATEFIRST 7`` (weeks start on Sunday)."""
    return F.date_sub(dateCol, F.dayofweek(dateCol) - 1)


def onTimeByRegion(regionCode: F.Column, delivered: F.Column, promised: F.Column, customsHoldDays: F.Column) -> F.Column:
    """Regional on-time definition: NA/EU strict against the promise, everyone else gets the customs hold as grace."""
    region = F.upper(regionCode)
    strict = F.when(delivered <= promised, 1).otherwise(0)
    withGrace = F.when(delivered <= F.date_add(promised, F.coalesce(customsHoldDays, F.lit(0))), 1).otherwise(0)
    return F.when(region.isin("NA", "EU"), strict).otherwise(withGrace)


def serviceCreditReporting(regionCode: F.Column, onTimePercent: F.Column, slaTarget: F.Column, freightCost: F.Column) -> F.Column:
    gap = (slaTarget - onTimePercent) * F.lit(EU_SERVICE_CREDIT_RATE_PER_POINT)
    rate = F.when(gap > F.lit(EU_SERVICE_CREDIT_CAP), F.lit(EU_SERVICE_CREDIT_CAP)).otherwise(gap)
    return (
        F.when((F.upper(regionCode) == "EU") & (onTimePercent < slaTarget), F.round(freightCost * rate, 2))
        .otherwise(F.lit(0))
        .cast("decimal(18,2)")
    )


def buildWeeklyDeliveryPerformance(factShipment: DataFrame, fromDate: date | None, batchId: int) -> DataFrame:
    """The proc's INSERT ... GROUP BY followed by its two UPDATE passes (ratios, then breach / service credit)."""
    sh = factShipment
    if fromDate is not None:
        sh = sh.where(F.col("despatch_date_key") >= F.lit(fromDate).cast("date"))
    delivered = F.col("delivery_confirmed_date_key")
    promised = F.col("promised_delivery_date_key")
    grouped = (
        sh.withColumn("iso_week_start_date", isoWeekStart(F.col("despatch_date_key")))
        .withColumn("iso_week_number", F.weekofyear(F.col("despatch_date_key")).cast("int"))
        .withColumn("service_level_code", F.coalesce(F.col("service_level_code"), F.lit("STD")))
        .groupBy(*AGGREGATE_GRAIN)
        .agg(
            F.countDistinct("despatch_note_number").cast("int").alias("consignment_count"),
            F.sum(F.coalesce(F.col("package_count"), F.lit(1))).cast("int").alias("package_count"),
            F.sum(F.when(delivered.isNotNull(), 1).otherwise(0)).cast("int").alias("delivered_count"),
            F.sum(onTimeByRegion(F.col("region_code"), delivered, promised, F.col("customs_hold_days")))
            .cast("int")
            .alias("on_time_count"),
            F.sum(F.when(delivered > promised, 1).otherwise(0)).cast("int").alias("late_count"),
            F.sum(F.coalesce(F.col("delivery_attempt_count"), F.lit(0))).cast("int").alias("failed_attempt_count"),
            F.sum(F.when(F.col("damaged_flag"), 1).otherwise(0)).cast("int").alias("damaged_count"),
            F.sum(F.when(F.col("shipment_status_code") == "LOST", 1).otherwise(0)).cast("int").alias("lost_count"),
            F.avg(F.col("despatch_to_delivery_lag_days").cast("decimal(18,2)"))
            .cast("decimal(9,2)")
            .alias("average_transit_days"),
            F.avg(F.coalesce(F.col("customs_hold_days"), F.lit(0)).cast("decimal(18,2)"))
            .cast("decimal(9,2)")
            .alias("average_customs_hold_days"),
            F.avg(F.col("pick_to_despatch_lag_days").cast("decimal(18,2)"))
            .cast("decimal(9,2)")
            .alias("average_pick_to_despatch_days"),
            F.sum(F.coalesce(F.col("total_weight_kg"), F.lit(0))).cast("decimal(18,3)").alias("total_weight_kg"),
            F.sum(F.coalesce(F.col("chargeable_weight_kg"), F.lit(0))).cast("decimal(18,3)").alias("chargeable_weight_kg"),
            F.sum(F.coalesce(F.col("freight_charge_reporting"), F.lit(0))).cast("decimal(18,2)").alias("freight_cost_reporting"),
            F.sum(F.coalesce(F.col("fuel_surcharge"), F.lit(0))).cast("decimal(18,2)").alias("fuel_surcharge_reporting"),
            F.sum(F.coalesce(F.col("duty_and_clearance_amount"), F.lit(0)))
            .cast("decimal(18,2)")
            .alias("duty_and_clearance_reporting"),
        )
    )
    consignments = F.col("consignment_count")
    deliveredCount = F.col("delivered_count")
    totalCost = F.col("freight_cost_reporting") + F.col("fuel_surcharge_reporting") + F.col("duty_and_clearance_reporting")
    ratios = (
        grouped.withColumn(
            "on_time_percent",
            F.when(deliveredCount == 0, F.lit(None))
            .otherwise(F.round(F.lit(100.0) * F.col("on_time_count") / deliveredCount, 2))
            .cast("decimal(9,4)"),
        )
        .withColumn(
            "first_attempt_success_percent",
            F.when(deliveredCount == 0, F.lit(None))
            .otherwise(F.round(F.lit(100.0) * (deliveredCount - F.col("failed_attempt_count")) / deliveredCount, 2))
            .cast("decimal(9,4)"),
        )
        .withColumn(
            "damage_rate_percent",
            F.when(consignments == 0, F.lit(None))
            .otherwise(F.round(F.lit(100.0) * F.col("damaged_count") / consignments, 2))
            .cast("decimal(9,4)"),
        )
        .withColumn(
            "cost_per_consignment",
            F.when(consignments == 0, F.lit(None)).otherwise(F.round(totalCost / consignments, 2)).cast("decimal(18,4)"),
        )
        .withColumn(
            "cost_per_chargeable_kg",
            F.when(F.coalesce(F.col("chargeable_weight_kg"), F.lit(0)) == 0, F.lit(None))
            .otherwise(
                F.round((F.col("freight_cost_reporting") + F.col("fuel_surcharge_reporting")) / F.col("chargeable_weight_kg"), 4)
            )
            .cast("decimal(18,4)"),
        )
        .withColumn("sla_target_percent", slaTargetPercent(F.col("region_code")))
    )
    return (
        ratios.withColumn("sla_breach_flag", F.coalesce(F.col("on_time_percent") < F.col("sla_target_percent"), F.lit(False)))
        .withColumn(
            "service_credit_reporting",
            serviceCreditReporting(
                F.col("region_code"), F.col("on_time_percent"), F.col("sla_target_percent"), F.col("freight_cost_reporting")
            ),
        )
        .withColumn("refresh_batch_id", F.lit(batchId).cast("long"))
        .withColumn("refreshed_datetime", F.current_timestamp())
    )


def serviceTier(shipmentCount: F.Column, onTimeCount: F.Column) -> F.Column:
    pct = onTimeCount.cast("decimal(9,4)") / shipmentCount.cast("decimal(9,4)") * 100
    return (
        F.when(shipmentCount == 0, F.lit("NODATA"))
        .when(pct >= 98, F.lit("PLATINUM"))
        .when(pct >= 95, F.lit("GOLD"))
        .when(pct >= 90, F.lit("SILVER"))
        .otherwise(F.lit("REVIEW"))
    )


def buildLaneDeliveryPerformance(
    factShipment: DataFrame,
    laneAttributes: DataFrame,
    fromDate: date | None,
    toDate: date | None,
    thinSampleThreshold: int = THIN_SAMPLE_THRESHOLD,
) -> tuple[DataFrame, DataFrame]:
    """The .dtsx data flow: delivered-date x carrier x lane; returns ``(kept, thin_sample_rejects)``.

    ``laneAttributes`` supplies ``despatch_note_number, carrier_code, origin_country_iso_code, destination_country_iso_code``
    (the DW fact carries surrogate keys only, so the lane comes from silver_shipment).
    """
    delivered = F.col("delivery_confirmed_date_key")
    sh = factShipment.where(delivered.isNotNull() & (F.col("shipment_status_code") == "DELIVERED"))
    if fromDate is not None:
        sh = sh.where(delivered >= F.lit(fromDate).cast("date"))
    if toDate is not None:
        sh = sh.where(delivered <= F.lit(toDate).cast("date"))
    joined = sh.alias("f").join(laneAttributes.alias("l"), "despatch_note_number", "left")
    daysLate = F.datediff(delivered, F.col("promised_delivery_date_key"))
    grouped = joined.groupBy(
        delivered.alias("delivered_date_key"),
        F.coalesce(F.col("l.carrier_code"), F.lit("UNKN")).alias("carrier_code"),
        F.col("l.origin_country_iso_code").alias("origin_country_iso_code"),
        F.col("l.destination_country_iso_code").alias("destination_country_iso_code"),
    ).agg(
        F.count(F.lit(1)).alias("shipment_count"),
        F.sum(F.when(F.coalesce(daysLate, F.lit(0)) <= 0, 1).otherwise(0)).cast("int").alias("on_time_shipment_count"),
        F.avg(F.col("despatch_to_delivery_lag_days").cast("decimal(9,2)")).cast("decimal(9,2)").alias("average_transit_days"),
        F.avg(F.col("customs_hold_days").cast("decimal(9,2)")).cast("decimal(9,2)").alias("average_customs_hold_days"),
        F.sum(F.col("freight_charge")).cast("decimal(18,2)").alias("freight_charge_amount"),
        F.sum(F.col("total_weight_kg")).cast("decimal(18,2)").alias("total_weight_kg"),
    )
    count = F.col("shipment_count")
    derived = (
        grouped.withColumn(
            "on_time_percent",
            F.when(count == 0, F.lit(0))
            .otherwise(F.col("on_time_shipment_count").cast("decimal(9,4)") / count.cast("decimal(9,4)") * 100)
            .cast("decimal(9,4)"),
        )
        .withColumn(
            "freight_per_kg",
            F.when(F.coalesce(F.col("total_weight_kg"), F.lit(0)) == 0, F.lit(0))
            .otherwise(F.col("freight_charge_amount") / F.col("total_weight_kg"))
            .cast("decimal(18,2)"),
        )
        .withColumn("is_cross_border_lane", F.col("origin_country_iso_code") != F.col("destination_country_iso_code"))
        .withColumn("service_tier_code", serviceTier(count, F.col("on_time_shipment_count")))
    )
    thin = count < F.lit(thinSampleThreshold)
    return derived.where(~thin), derived.where(thin).withColumn(
        "reject_reason", F.lit("Aggregate row failed the summary quality gate")
    )


def laneAttributesFromSilver(silver: DataFrame) -> DataFrame:
    ranked = silver.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("shipment_business_key").orderBy(F.col("loaded_at_utc").desc()))
    ).where(F.col("_rn") == 1)
    origin = F.upper(F.substring(F.col("ship_from_warehouse_code"), 1, 2))
    return ranked.select(
        F.col("shipment_business_key").alias("despatch_note_number"),
        F.col("carrier_code"),
        origin.alias("origin_country_iso_code"),
        F.upper(F.col("ship_to_country_code")).alias("destination_country_iso_code"),
    )


def refreshWindowStart(asOf: date, weeksToRefresh: int, reloadFullHistory: bool) -> date | None:
    if reloadFullHistory:
        return None
    return asOf - timedelta(weeks=weeksToRefresh)


def runAggRefreshDeliveryPerformance(ctx: RunContext, weeksToRefresh: int = DEFAULT_WEEKS_TO_REFRESH) -> AggregateResult:
    spark = ctx.spark
    target = ctx.table(Tables.goldAggDeliveryPerformanceSummary)
    fact = spark.table(ctx.table(Tables.goldFactShipment))
    fromDate = refreshWindowStart(ctx.startedAtUtc.date(), weeksToRefresh, ctx.reloadFullHistory)
    rowsRead = fact.where(F.col("despatch_date_key") >= F.lit(fromDate).cast("date")).count() if fromDate else fact.count()

    incoming = buildWeeklyDeliveryPerformance(fact, fromDate, ctx.batchId).cache()
    aggregated = incoming.count()
    deleted = 0
    if tableExists(spark, target) and fromDate is not None:
        existing = spark.table(target)
        window = F.col("iso_week_start_date") >= F.lit(fromDate).cast("date")
        deleted = existing.where(window).count()
        retained = existing.where(~window).select(*incoming.columns)
        overwriteTable(retained.unionByName(incoming), target)
    else:
        if tableExists(spark, target):
            deleted = spark.table(target).count()
        overwriteTable(incoming, target)

    silver = readTableOrEmpty(
        spark,
        ctx.table(Tables.silverShipment),
        T.StructType(
            [
                T.StructField("shipment_business_key", T.StringType()),
                T.StructField("carrier_code", T.StringType()),
                T.StructField("ship_from_warehouse_code", T.StringType()),
                T.StructField("ship_to_country_code", T.StringType()),
                T.StructField("loaded_at_utc", T.TimestampType()),
            ]
        ),
    )
    lanes, thin = buildLaneDeliveryPerformance(fact, laneAttributesFromSilver(silver), fromDate, None)
    laneTarget = ctx.table(Tables.goldAggDeliveryPerformanceLane)
    thinTarget = ctx.table(Tables.goldAggDeliveryPerformanceThinSample)
    stamp = [F.lit(ctx.batchId).cast("long").alias("refresh_batch_id"), F.current_timestamp().alias("refreshed_datetime")]
    if tableExists(spark, laneTarget) and fromDate is not None:
        keep = spark.table(laneTarget).where(F.col("delivered_date_key") < F.lit(fromDate).cast("date"))
        overwriteTable(keep.unionByName(lanes.select("*", *stamp), allowMissingColumns=True), laneTarget)
    else:
        overwriteTable(lanes.select("*", *stamp), laneTarget)
    thinRows = thin.select("*", *stamp).withColumn("package_execution_id", F.lit(ctx.packageExecutionId).cast("long"))
    rejected = thinRows.count()
    appendTable(thinRows, thinTarget)

    logRowCount(
        ctx,
        PACKAGE_NAME,
        Tables.goldAggDeliveryPerformanceSummary,
        {"rows_read": rowsRead, "rows_deleted": deleted, "rows_inserted": aggregated, "rows_rejected": rejected},
    )
    return AggregateResult(rowsRead, deleted, aggregated, rejected)
