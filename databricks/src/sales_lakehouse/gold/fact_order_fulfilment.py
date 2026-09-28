"""``fact_order_fulfilment`` - order-grain accumulating snapshot of the order-to-cash cycle
(``DeltaTable.merge`` on ``order_business_key``). Milestones are filled in as events arrive
and never erased; lags, pipeline status and stalled flags are recomputed on every merge.

Replaces Integration.usp_LoadFactOrderFulfilment and SSIS FACT_Load_OrderFulfilment.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import tableExists
from sales_lakehouse.gold.fact_support import (
    daysBetween,
    mergeAccumulating,
    money,
    optionalColumn,
    pickColumn,
    readOptional,
    surrogateKey,
    withLoadMetadata,
)

TABLE = "fact_order_fulfilment"
KEY_COLS = ["order_business_key"]
MILESTONES = [
    "order_date_key",
    "allocation_date_key",
    "pick_date_key",
    "pack_date_key",
    "despatch_date_key",
    "delivery_date_key",
    "invoice_date_key",
    "cash_applied_date_key",
    "cancellation_date_key",
]
# LEGACY QUIRK: regional "stalled" thresholds from usp_LoadFactOrderFulfilment.
STALLED_AFTER_DAYS = {"NA": 30, "EU": 45, "APAC": 60}
CANCELLED_STATUSES = ("CANCELLED", "CANC", "XX")


def _stalledThreshold() -> Column:
    region = F.col("region_code")
    return (
        F.when(region == "NA", F.lit(STALLED_AFTER_DAYS["NA"]))
        .when(region == "EU", F.lit(STALLED_AFTER_DAYS["EU"]))
        .otherwise(F.lit(STALLED_AFTER_DAYS["APAC"]))
        .cast("int")
    )


def finalizeFulfilment(df: DataFrame) -> DataFrame:
    asOf = F.current_date()
    status = (
        F.when(F.col("cancellation_date_key").isNotNull(), F.lit("CANCELLED"))
        .when(F.col("cash_applied_date_key").isNotNull(), F.lit("CASH"))
        .when(F.col("invoice_date_key").isNotNull(), F.lit("INVOICED"))
        .when(F.col("delivery_date_key").isNotNull(), F.lit("DELIVERED"))
        .when(F.col("despatch_date_key").isNotNull(), F.lit("DESPATCHED"))
        .when(F.col("pick_date_key").isNotNull(), F.lit("PICKED"))
        .otherwise(F.lit("ORDERED"))
    )
    openMilestones = sum(
        (
            F.when(F.col(c).isNull(), 1).otherwise(0)
            for c in (
                "pick_date_key",
                "despatch_date_key",
                "delivery_date_key",
                "invoice_date_key",
                "cash_applied_date_key",
            )
        ),
        F.lit(0),
    )
    stalledReason = (
        F.when(F.col("pick_date_key").isNull(), F.lit("NOPICK"))
        .when(F.col("despatch_date_key").isNull(), F.lit("NODESP"))
        .when(F.col("invoice_date_key").isNull(), F.lit("NOINV"))
        .otherwise(F.lit("NOCASH"))
    )
    df = (
        df.withColumn("order_to_pick_days", daysBetween("order_date_key", "pick_date_key"))
        .withColumn("pick_to_despatch_days", daysBetween("pick_date_key", "despatch_date_key"))
        .withColumn("despatch_to_delivery_days", daysBetween("despatch_date_key", "delivery_date_key"))
        .withColumn("delivery_to_invoice_days", daysBetween("delivery_date_key", "invoice_date_key"))
        .withColumn("invoice_to_cash_days", daysBetween("invoice_date_key", "cash_applied_date_key"))
        .withColumn("order_to_cash_cycle_days", daysBetween("order_date_key", "cash_applied_date_key"))
        .withColumn("pipeline_status_code", status)
        .withColumn("cycle_complete_flag", F.col("cash_applied_date_key").isNotNull())
        .withColumn("cancelled_flag", F.col("cancellation_date_key").isNotNull())
        .withColumn("open_milestone_count", openMilestones.cast("int"))
        .withColumn("service_target_days", _stalledThreshold())
        .withColumn("days_open", F.datediff(asOf, F.col("order_date_key")).cast("int"))
    )
    stalled = (
        (~F.col("cycle_complete_flag"))
        & (~F.col("cancelled_flag"))
        & (F.col("days_open") > F.col("service_target_days"))
    )
    return (
        df.withColumn("stalled_flag", F.coalesce(stalled, F.lit(False)))
        .withColumn("stalled_reason_code", F.when(F.col("stalled_flag"), stalledReason))
        .withColumn(
            "delivery_sla_breach_flag",
            F.coalesce(F.col("delivery_date_key") > F.col("promised_delivery_date_key"), F.lit(False)),
        )
        .withColumn("last_milestone_update", F.current_timestamp())
    )


def _orderSide(factOrder: DataFrame) -> DataFrame:
    cancelled = F.upper(F.coalesce(F.col("order_status_code"), F.lit(""))).isin(*CANCELLED_STATUSES)
    return factOrder.groupBy("order_business_key").agg(
        F.min("order_date_key").alias("order_date_key"),
        F.max("customer_key").alias("customer_key"),
        F.max("salesperson_key").alias("salesperson_key"),
        F.max("sales_channel_key").alias("sales_channel_key"),
        F.max("sales_territory_key").alias("sales_territory_key"),
        F.first("region_code", ignorenulls=True).alias("region_code"),
        F.first("order_number", ignorenulls=True).alias("order_number"),
        F.first("customer_business_key", ignorenulls=True).alias("customer_business_key"),
        F.first("transaction_currency_code", ignorenulls=True).alias("transaction_currency_code"),
        F.max("requested_delivery_date_key").alias("requested_delivery_date_key"),
        F.max("promised_delivery_date_key").alias("promised_delivery_date_key"),
        F.count("*").cast("int").alias("order_line_count"),
        F.sum("quantity_ordered").cast("decimal(18,4)").alias("quantity_ordered"),
        F.sum("quantity_despatched").cast("decimal(18,4)").alias("quantity_despatched"),
        F.sum("net_order_amount").cast("decimal(19,4)").alias("order_value"),
        F.sum("net_order_amount_reporting").cast("decimal(19,4)").alias("order_value_reporting"),
        # Pick is complete only when every line is picked: max(picked) over all lines, null if any line is open.
        F.when(F.max(F.when(F.col("picked_date_key").isNull(), 1).otherwise(0)) == 0, F.max("picked_date_key")).alias(
            "pick_date_key"
        ),
        F.max(F.when(cancelled, F.col("order_date_key"))).alias("_cancel_marker"),
        F.max(F.when(F.col("is_on_hold"), F.lit(True)).otherwise(F.lit(False))).alias("on_hold_flag"),
        F.max(F.when(F.col("is_backordered"), F.lit(True)).otherwise(F.lit(False))).alias("backordered_flag"),
    )


def buildFactOrderFulfilment(
    cfg: PipelineConfig,
    factOrder: DataFrame,
    sales: DataFrame | None,
    saleLines: DataFrame | None,
    shipments: DataFrame | None,
    factPayment: DataFrame | None,
) -> DataFrame:
    df = _orderSide(factOrder)
    if sales is not None:
        s = sales.select(
            F.col("order_business_key").alias("_s_order"),
            F.col("sale_business_key"),
            F.col("invoice_date").cast("date").alias("invoice_date"),
            optionalColumn(sales, "confirmed_delivery_utc", "timestamp").cast("date").alias("_confirmed_delivery"),
            F.coalesce(optionalColumn(sales, "is_credit_note", "boolean"), F.lit(False)).alias("_is_cn"),
        ).filter(~F.col("_is_cn"))
        if saleLines is not None:
            sl = saleLines.groupBy("sale_business_key").agg(
                F.sum("quantity").cast("decimal(18,4)").alias("_inv_qty"),
                F.sum(pickColumn(saleLines, ["net_line_amount_usd", "net_line_amount"], "decimal(19,4)"))
                .cast("decimal(19,4)")
                .alias("_inv_value"),
            )
            s = s.join(sl, "sale_business_key", "left")
        else:
            s = s.withColumn("_inv_qty", F.lit(None).cast("decimal(18,4)")).withColumn(
                "_inv_value", F.lit(None).cast("decimal(19,4)")
            )
        inv = s.groupBy("_s_order").agg(
            F.min("invoice_date").alias("invoice_date_key"),
            F.min("sale_business_key").alias("invoice_number"),
            F.min("_confirmed_delivery").alias("_confirmed_delivery"),
            F.sum("_inv_qty").cast("decimal(18,4)").alias("quantity_invoiced"),
            F.sum("_inv_value").cast("decimal(19,4)").alias("invoice_value_reporting"),
        )
        df = df.join(inv, df["order_business_key"] == inv["_s_order"], "left").drop("_s_order")
        if shipments is not None:
            sh = shipments.select(
                F.col("sale_business_key"),
                pickColumn(shipments, ["shipped_date", "despatch_date"], "date").alias("_shipped"),
                pickColumn(shipments, ["delivered_date_time_utc", "delivered_when_utc", "delivered_date"], "timestamp")
                .cast("date")
                .alias("_delivered"),
                pickColumn(shipments, ["shipment_reference", "shipment_business_key"], "string").alias("_ref"),
                pickColumn(shipments, ["carrier_code"], "string").alias("_carrier"),
            ).join(
                sales.select("sale_business_key", F.col("order_business_key").alias("_sh_order")), "sale_business_key"
            )
            ship = sh.groupBy("_sh_order").agg(
                F.min("_shipped").alias("despatch_date_key"),
                F.min("_delivered").alias("_ship_delivered"),
                F.min("_ref").alias("despatch_note_number"),
                F.min("_carrier").alias("carrier_code"),
            )
            df = df.join(ship, df["order_business_key"] == ship["_sh_order"], "left").drop("_sh_order")
        else:
            df = (
                df.withColumn("despatch_date_key", F.lit(None).cast("date"))
                .withColumn("_ship_delivered", F.lit(None).cast("date"))
                .withColumn("despatch_note_number", F.lit(None).cast("string"))
                .withColumn("carrier_code", F.lit(None).cast("string"))
            )
        df = df.withColumn(
            "delivery_date_key", F.coalesce(F.col("_ship_delivered"), F.col("_confirmed_delivery"))
        ).drop("_ship_delivered", "_confirmed_delivery")
    else:
        for c, t in (
            ("invoice_date_key", "date"),
            ("invoice_number", "string"),
            ("quantity_invoiced", "decimal(18,4)"),
            ("invoice_value_reporting", "decimal(19,4)"),
            ("despatch_date_key", "date"),
            ("delivery_date_key", "date"),
            ("despatch_note_number", "string"),
            ("carrier_code", "string"),
        ):
            df = df.withColumn(c, F.lit(None).cast(t))

    if factPayment is not None and "invoice_number" in factPayment.columns:
        # LEGACY QUIRK: cash-applied date is the LATEST allocation against the order's invoices
        # (usp_LoadFactOrderFulfilment TOP 1 ... ORDER BY DESC), not the first.
        pay = (
            factPayment.filter(F.col("invoice_number").isNotNull())
            .groupBy("invoice_number")
            .agg(
                F.max("payment_date_key").alias("cash_applied_date_key"),
                F.sum("allocated_amount_reporting").cast("decimal(19,4)").alias("cash_applied_reporting"),
                F.min("receipt_number").alias("receipt_number"),
            )
        )
        df = df.join(pay, "invoice_number", "left")
    else:
        df = (
            df.withColumn("cash_applied_date_key", F.lit(None).cast("date"))
            .withColumn("cash_applied_reporting", F.lit(None).cast("decimal(19,4)"))
            .withColumn("receipt_number", F.lit(None).cast("string"))
        )

    df = (
        df.withColumn("allocation_date_key", F.lit(None).cast("date"))
        .withColumn("pack_date_key", F.lit(None).cast("date"))
        # LEGACY QUIRK: WWI has no cancellation timestamp; a cancelled order is stamped with its
        # order date so it leaves the open pipeline, as the legacy proc did.
        .withColumn("cancellation_date_key", F.col("_cancel_marker"))
        .drop("_cancel_marker")
        .withColumn("order_value_reporting", money(F.col("order_value_reporting")))
    )
    out = df.select(
        surrogateKey("order_business_key").alias("order_fulfilment_key"),
        F.col("order_business_key"),
        *[F.col(m) for m in MILESTONES],
        F.col("requested_delivery_date_key"),
        F.col("promised_delivery_date_key"),
        F.col("customer_key"),
        F.col("salesperson_key"),
        F.col("sales_channel_key"),
        F.col("sales_territory_key"),
        F.lit(-1).cast("bigint").alias("warehouse_site_key"),
        F.lit(-1).cast("bigint").alias("carrier_key"),
        F.col("region_code"),
        F.col("order_number"),
        F.col("customer_business_key"),
        F.col("invoice_number"),
        F.col("despatch_note_number"),
        F.col("carrier_code"),
        F.col("receipt_number"),
        F.col("order_line_count"),
        F.col("quantity_ordered"),
        F.col("quantity_despatched"),
        F.col("quantity_invoiced"),
        F.col("transaction_currency_code"),
        F.col("order_value"),
        F.col("order_value_reporting"),
        F.col("invoice_value_reporting"),
        F.col("cash_applied_reporting"),
        F.col("on_hold_flag"),
        F.col("backordered_flag"),
    )
    return withLoadMetadata(out, cfg)


def writeFactOrderFulfilment(
    spark: SparkSession, cfg: PipelineConfig, df: DataFrame, reopenClosedRows: bool = False
) -> None:
    fqn = cfg.fqn("gold", TABLE)
    if not reopenClosedRows and tableExists(spark, fqn):
        # LEGACY QUIRK: once the cycle is complete (cash applied) the row is frozen and no longer
        # restated unless @ReopenClosedRows is set (usp_LoadFactOrderFulfilment).
        closed = spark.table(fqn).filter(F.col("cycle_complete_flag")).select(*KEY_COLS)
        df = df.join(closed, list(KEY_COLS), "left_anti")
    mergeAccumulating(spark, df, fqn, KEY_COLS, MILESTONES, finalizeFulfilment)


def run(spark: SparkSession, cfg: PipelineConfig, reopenClosedRows: bool = False) -> None:
    df = buildFactOrderFulfilment(
        cfg,
        factOrder=spark.table(cfg.fqn("gold", "fact_order")),
        sales=readOptional(spark, cfg, "silver", "sale"),
        saleLines=readOptional(spark, cfg, "silver", "sale_line"),
        shipments=readOptional(spark, cfg, "silver", "shipment"),
        factPayment=readOptional(spark, cfg, "gold", "fact_payment"),
    )
    writeFactOrderFulfilment(spark, cfg, df, reopenClosedRows)
