"""``fact_daily_sales_snapshot`` and ``fact_daily_backlog`` - periodic snapshots keyed by
``snapshot_date_key``; each run replaces exactly the snapshot dates in the batch
(``replaceWhere``), the Delta equivalent of the legacy delete-by-window.

Replaces Integration.usp_LoadFactDailySalesSnapshot (both tables) and SSIS
FACT_Load_DailySalesSnapshot.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.gold.fact_support import (
    daysBetween,
    money,
    optionalColumn,
    readOptional,
    replaceSnapshotDates,
    withLoadMetadata,
)

SALES_TABLE = "fact_daily_sales_snapshot"
BACKLOG_TABLE = "fact_daily_backlog"
SNAPSHOT_COL = "snapshot_date_key"
SALES_GRAIN = ["salesperson_key", "sales_territory_key", "sales_channel_key", "customer_segment_key", "region_code"]
# LEGACY QUIRK: EU distributor-agreement channels are excluded from commissionable revenue by
# hard-coded channel keys because the agreement dimension was never built (usp_LoadFactDailySalesSnapshot).
EU_DISTRIBUTOR_CHANNEL_KEYS: tuple[int, ...] = (7, 8, 12)
STALE_THRESHOLD_DAYS = {"NA": 30, "EU": 45, "APAC": 60}


def _dateStrings(df: DataFrame, col: str) -> list[str]:
    return sorted(str(r[0]) for r in df.select(F.col(col).cast("date")).distinct().collect() if r[0] is not None)


def batchSnapshotDates(spark: SparkSession, cfg: PipelineConfig) -> list[str]:
    """Snapshot dates touched by this batch: invoice dates of fact_sale rows and order dates of
    fact_order rows loaded with ``cfg.batchId``. Falls back to today when the batch touched nothing."""
    dates: set[str] = set()
    sale = readOptional(spark, cfg, "gold", "fact_sale")
    if sale is not None:
        dates.update(_dateStrings(sale.filter(F.col("batch_id") == cfg.batchId), "invoice_date_key"))
    order = readOptional(spark, cfg, "gold", "fact_order")
    if order is not None:
        dates.update(_dateStrings(order.filter(F.col("batch_id") == cfg.batchId), "order_date_key"))
    return sorted(dates) or [str(date.today())]


def _commissionRate() -> Column:
    # LEGACY QUIRK: regional commission rate bands, applied after aggregation (usp_LoadFactDailySalesSnapshot).
    region = F.col("region_code")
    return (
        F.when((region == "NA") & (F.col("commissionable_revenue") > 50000), F.lit(0.030))
        .when(region == "NA", F.lit(0.020))
        .when((region == "EU") & (F.col("margin_percent") >= 30), F.lit(0.025))
        .when(region == "EU", F.lit(0.015))
        .when(region == "APAC", F.lit(0.018))
        .otherwise(F.lit(0.010))
        .cast("decimal(9,4)")
    )


def _adjustmentByDate(df: DataFrame | None, dateCol: str, amountCol: str, outCol: str) -> DataFrame | None:
    if df is None or dateCol not in df.columns or amountCol not in df.columns:
        return None
    keys = [k for k in ("salesperson_key", "sales_territory_key", "region_code") if k in df.columns]
    return df.groupBy(F.col(dateCol).cast("date").alias(SNAPSHOT_COL), *keys).agg(
        F.sum(amountCol).cast("decimal(19,4)").alias(outCol)
    )


def buildDailySalesSnapshot(
    cfg: PipelineConfig,
    factSale: DataFrame,
    snapshotDates: Sequence[str],
    factReturn: DataFrame | None = None,
    factCreditNote: DataFrame | None = None,
) -> DataFrame:
    s = factSale.withColumn(SNAPSHOT_COL, F.col("invoice_date_key").cast("date"))
    daily = s.groupBy(SNAPSHOT_COL, *SALES_GRAIN).agg(
        F.first("fiscal_year", ignorenulls=True).alias("fiscal_year"),
        F.first("fiscal_period", ignorenulls=True).alias("fiscal_period"),
        F.first(optionalColumn(s, "fiscal_calendar_code", "string"), ignorenulls=True).alias("fiscal_calendar_code"),
        F.countDistinct("invoice_number").cast("int").alias("invoice_count"),
        F.countDistinct("order_number").cast("int").alias("order_count"),
        F.countDistinct("customer_key").cast("int").alias("distinct_customer_count"),
        F.count("*").cast("int").alias("line_count"),
        F.sum("quantity").cast("decimal(18,4)").alias("quantity_sold"),
        F.sum("gross_amount").cast("decimal(19,4)").alias("gross_sales_amount"),
        F.sum("line_discount_amount").cast("decimal(19,4)").alias("discount_amount"),
        F.sum("net_amount").cast("decimal(19,4)").alias("net_sales_amount"),
        F.sum("tax_amount").cast("decimal(19,4)").alias("tax_amount"),
        F.sum(F.coalesce(F.col("freight_amount"), F.lit(0))).cast("decimal(19,4)").alias("freight_amount"),
        F.sum("cost_of_sale_amount").cast("decimal(19,4)").alias("cost_of_sales_amount"),
        F.sum("gross_margin_amount").cast("decimal(19,4)").alias("gross_margin_amount"),
        F.sum("net_amount_reporting").cast("decimal(19,4)").alias("net_sales_amount_reporting"),
        F.sum(
            F.when(
                (F.col("region_code") == "EU") & F.col("sales_channel_key").isin(list(EU_DISTRIBUTOR_CHANNEL_KEYS)),
                F.lit(0),
            ).otherwise(F.col("net_amount"))
        )
        .cast("decimal(19,4)")
        .alias("commissionable_revenue"),
    )
    daily = daily.withColumn(
        "margin_percent",
        F.when(
            F.col("net_sales_amount") != 0, F.round(F.col("gross_margin_amount") / F.col("net_sales_amount") * 100, 4)
        ).cast("decimal(9,4)"),
    ).withColumn(
        "average_order_value",
        F.when(F.col("invoice_count") > 0, money(F.col("net_sales_amount") / F.col("invoice_count"))),
    )
    for adj, dateCol, amountCol, outCol in (
        (factReturn, "return_date_key", "net_credit_amount_reporting", "returns_amount"),
        (factCreditNote, "credit_note_date_key", "credit_including_tax_reporting", "credit_note_amount"),
    ):
        a = _adjustmentByDate(adj, dateCol, amountCol, outCol)
        if a is None:
            daily = daily.withColumn(outCol, F.lit(0).cast("decimal(19,4)"))
        else:
            joinKeys = [c for c in a.columns if c != outCol]
            daily = daily.join(a, joinKeys, "left").withColumn(outCol, money(F.coalesce(F.col(outCol), F.lit(0))))
    # LEGACY QUIRK: running totals are stored, not derived, because bonuses were signed off
    # against the numbers as they stood on the day (Fact.Daily Snapshots header).
    mtd = (
        Window.partitionBy(*SALES_GRAIN, F.year(SNAPSHOT_COL), F.month(SNAPSHOT_COL))
        .orderBy(SNAPSHOT_COL)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    ytd = (
        Window.partitionBy(*SALES_GRAIN, F.year(SNAPSHOT_COL))
        .orderBy(SNAPSHOT_COL)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    daily = (
        daily.withColumn("month_to_date_net_sales", F.sum("net_sales_amount").over(mtd).cast("decimal(19,4)"))
        .withColumn("year_to_date_net_sales", F.sum("net_sales_amount").over(ytd).cast("decimal(19,4)"))
        .withColumn("month_to_date_margin", F.sum("gross_margin_amount").over(mtd).cast("decimal(19,4)"))
        .withColumn("commission_rate", _commissionRate())
        .withColumn("commission_accrued_amount", money(F.col("commissionable_revenue") * F.col("commission_rate")))
        .withColumn("quota_amount", F.lit(None).cast("decimal(19,4)"))
        .withColumn("quota_attainment_percent", F.lit(None).cast("decimal(9,4)"))
        .withColumn("bonus_locked_flag", F.lit(False))
        .withColumn("restated_flag", F.lit(False))
        .withColumn("lineage_key", F.lit(None).cast("bigint"))
    )
    daily = daily.filter(F.col(SNAPSHOT_COL).isin([F.lit(d).cast("date") for d in snapshotDates]))
    daily = daily.withColumn("daily_sales_snapshot_key", F.xxhash64(SNAPSHOT_COL, *SALES_GRAIN))
    return withLoadMetadata(daily, cfg)


def buildDailyBacklog(cfg: PipelineConfig, factOrder: DataFrame, snapshotDates: Sequence[str]) -> DataFrame:
    dates = [F.lit(d).cast("date") for d in snapshotDates]
    snap = F.explode(F.array(*dates)).alias(SNAPSHOT_COL)
    cancelled = F.upper(F.coalesce(F.col("order_status_code"), F.lit(""))).isin("CANCELLED", "CANC", "XX")
    # LEGACY QUIRK: backlog is evaluated against the CURRENT open quantity of the order line
    # (Fact.Order state), reproduced from usp_LoadFactDailySnapshot; a line "reappears every
    # night until it is fully despatched or cancelled".
    openLines = factOrder.filter((F.col("quantity_open") > 0) & ~cancelled)
    df = openLines.select("*", snap).filter(F.col("order_date_key") <= F.col(SNAPSHOT_COL))
    df = (
        df.withColumn("backlog_age_days", daysBetween("order_date_key", SNAPSHOT_COL))
        .withColumn("days_past_promise", daysBetween("promised_delivery_date_key", SNAPSHOT_COL))
        .withColumn("past_promise_flag", F.coalesce(F.col("days_past_promise") > 0, F.lit(False)))
        .withColumn("stock_constrained_flag", F.col("is_backordered"))
        .withColumn("credit_hold_constrained_flag", F.col("is_credit_hold"))
        .withColumn("expedite_requested_flag", F.lit(False))
        .withColumn(
            "backlog_aging_bucket",
            F.when(F.col("backlog_age_days") <= 7, F.lit("0-7"))
            .when(F.col("backlog_age_days") <= 30, F.lit("8-30"))
            .when(F.col("backlog_age_days") <= 60, F.lit("31-60"))
            .otherwise(F.lit("60+")),
        )
        .withColumn("open_margin_reporting", F.lit(None).cast("decimal(19,4)"))
        .withColumn("quantity_available_at_site", F.lit(None).cast("decimal(18,4)"))
        .withColumn("coverable_quantity", F.lit(None).cast("decimal(18,4)"))
    )
    out = df.select(
        F.xxhash64(SNAPSHOT_COL, "order_line_business_key").alias("daily_backlog_key"),
        F.col(SNAPSHOT_COL),
        F.col("order_line_business_key"),
        F.col("order_business_key"),
        F.col("order_date_key"),
        F.col("requested_delivery_date_key"),
        F.col("promised_delivery_date_key"),
        F.col("customer_key"),
        F.col("stock_item_key"),
        F.col("salesperson_key"),
        F.lit(-1).cast("bigint").alias("warehouse_site_key"),
        F.col("sales_territory_key"),
        F.col("sales_channel_key"),
        F.col("region_code"),
        F.col("order_number"),
        F.col("order_line_number"),
        F.col("backorder_reason_code"),
        F.col("quantity_ordered"),
        F.col("quantity_despatched").alias("quantity_despatched_to_date"),
        F.col("quantity_open"),
        F.col("quantity_available_at_site"),
        F.col("coverable_quantity"),
        F.col("open_value_reporting"),
        F.col("open_margin_reporting"),
        F.col("backlog_age_days"),
        F.col("days_past_promise"),
        F.col("past_promise_flag"),
        F.col("stock_constrained_flag"),
        F.col("credit_hold_constrained_flag"),
        F.col("expedite_requested_flag"),
        F.col("backlog_aging_bucket"),
        F.col("lineage_key"),
    )
    return withLoadMetadata(out, cfg)


def runSalesSnapshot(spark: SparkSession, cfg: PipelineConfig, snapshotDates: Sequence[str] | None = None) -> None:
    dates = list(snapshotDates) if snapshotDates else batchSnapshotDates(spark, cfg)
    df = buildDailySalesSnapshot(
        cfg,
        factSale=spark.table(cfg.fqn("gold", "fact_sale")),
        snapshotDates=dates,
        factReturn=readOptional(spark, cfg, "gold", "fact_return"),
        factCreditNote=readOptional(spark, cfg, "gold", "fact_credit_note"),
    )
    replaceSnapshotDates(df, cfg.fqn("gold", SALES_TABLE), SNAPSHOT_COL, dates)


def runBacklog(spark: SparkSession, cfg: PipelineConfig, snapshotDates: Sequence[str] | None = None) -> None:
    dates = list(snapshotDates) if snapshotDates else batchSnapshotDates(spark, cfg)
    df = buildDailyBacklog(cfg, spark.table(cfg.fqn("gold", "fact_order")), dates)
    replaceSnapshotDates(df, cfg.fqn("gold", BACKLOG_TABLE), SNAPSHOT_COL, dates)


def run(spark: SparkSession, cfg: PipelineConfig, snapshotDates: Sequence[str] | None = None) -> None:
    dates = list(snapshotDates) if snapshotDates else batchSnapshotDates(spark, cfg)
    runSalesSnapshot(spark, cfg, dates)
    runBacklog(spark, cfg, dates)
