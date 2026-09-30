"""FACT_Load_Shipment, FACT_Load_OrderFulfilment (accumulating snapshots), FACT_Load_Return, FACT_Load_CreditNote
(incremental facts).

Each SSIS package is a Data Flow (stg.* source -> Lookups on the DW dimensions -> Derived Column -> Conditional
Split -> Fact.* | err.*) followed by the Integration.usp_LoadFact* procedure that applies the milestone / delete-and-
reload logic. The pure DataFrame functions here are the tested business rules; the ``runFact*`` functions wire them
to the landing tables.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from logistics_returns.common import (
    appendTable,
    ensureTable,
    getWatermark,
    logRowCount,
    overwriteTable,
    readLegacy,
    readTableOrEmpty,
    rowHash,
    setWatermark,
    snakeCaseColumns,
    tableExists,
)
from logistics_returns.config import SOURCE_SYSTEM_OLTP, UNKNOWN_MEMBER_KEY, RunContext, Tables
from logistics_returns.dimensions import (
    CARRIER,
    CUSTOMER,
    NOT_APPLICABLE_KEY,
    RETURN_REASON,
    SALES_TERRITORY,
    STOCK_ITEM,
    WAREHOUSE_SITE,
    attachKey,
    dimensionLookup,
    inferredMemberFlag,
    legacyFactSale,
    returnReasonWindows,
)
from logistics_returns.reference import (
    REGION_APAC,
    REGION_EU,
    REGION_NA,
    regionStalledThresholdDays,
)
from logistics_returns.staging import rejectRows

WATERMARK_TYPE = "Timestamp"
LINEAGE_KEY = 8  # logistics_returns package group
DIM_WEIGHT_DIVISOR = 167.0  # kg per m3 volumetric divisor used by usp_LoadFactShipment
CUSTOMS_HOLD_DAYS_THRESHOLD = 3
CREDIT_APPROVAL_THRESHOLD = 1000.0
CREDIT_SECOND_APPROVAL_THRESHOLD = 10000.0
NA_RESTOCKING_PERCENT = 15.0
APAC_RESTOCKING_FLAT_FEE = 5.0
NA_RETURN_WINDOW_DAYS = 30
EU_RETURN_WINDOW_DAYS = 14
APAC_DEFAULT_RETURN_WINDOW_DAYS = 7
FAULTY_REASON_CODES = ("DAMTR", "QUAL", "CONFORM", "RECALL", "DMG", "DAMG", "EXP")
SCRAP_REASON_CODES = ("DMG", "EXP", "DAMTR", "RECALL")


@dataclass
class FactResult:
    rowsRead: int
    rowsInserted: int
    rowsUpdated: int
    rowsRejected: int


def _load(ctx: RunContext, df: DataFrame) -> DataFrame:
    return (
        df.withColumn("lineage_key", F.lit(LINEAGE_KEY))
        .withColumn("batch_id", F.lit(ctx.batchId).cast("long"))
        .withColumn("load_datetime", F.lit(ctx.startedAtUtc).cast("timestamp"))
    )


# ---------------------------------------------------------------------------------------------
# FACT_Load_Shipment (accumulating snapshot)
# ---------------------------------------------------------------------------------------------

SCAN_EVENTS_SCHEMA = T.StructType(
    [
        T.StructField("shipment_reference", T.StringType()),
        T.StructField("tracking_number", T.StringType()),
        T.StructField("scan_event_code", T.StringType()),
        T.StructField("exception_reason_code", T.StringType()),
        T.StructField("scan_timestamp_utc", T.TimestampType()),
    ]
)


def summarizeScanEvents(scans: DataFrame) -> DataFrame:
    """Carrier events per shipment: collection, customs clearance, attempts, delivery, damage, loss."""
    code = F.upper(F.col("scan_event_code"))
    reason = F.upper(F.coalesce(F.col("exception_reason_code"), F.lit("")))
    day = F.to_date("scan_timestamp_utc")
    return (
        scans.withColumn("scan_key", F.coalesce(F.col("shipment_reference"), F.col("tracking_number")))
        .groupBy("scan_key")
        .agg(
            F.min(F.when(code.isin("COL", "PU", "PIC"), day)).alias("scan_collection_date"),
            F.min(F.when(code.isin("CUS", "CCL"), day)).alias("scan_customs_cleared_date"),
            F.min(F.when(code.isin("ATT", "DLV", "POD", "FLD"), day)).alias("scan_first_attempt_date"),
            F.min(F.when(code.isin("DLV", "POD"), day)).alias("scan_delivered_date"),
            F.sum(F.when(code.isin("ATT", "FLD") | (code.isin("DLV", "POD")), 1).otherwise(0))
            .cast("int")
            .alias("scan_attempt_count"),
            F.max(F.when((code == "DMG") | reason.isin("DAMAGED", "DMG"), True).otherwise(False)).alias("scan_damaged_flag"),
            F.max(F.when((code == "LST") | reason.isin("LOST", "LST"), True).otherwise(False)).alias("scan_lost_flag"),
            F.max("scan_timestamp_utc").alias("scan_last_event_utc"),
        )
    )


def shipmentStatus(deliveredKey: F.Column, lostFlag: F.Column, customsHoldDays: F.Column, attemptCount: F.Column) -> F.Column:
    return (
        F.when(lostFlag, F.lit("LOST"))
        .when(deliveredKey.isNotNull(), F.lit("DELIVERED"))
        .when(customsHoldDays > CUSTOMS_HOLD_DAYS_THRESHOLD, F.lit("CUSTOMS_HOLD"))
        .when(attemptCount > 0, F.lit("ATTEMPTED"))
        .otherwise(F.lit("IN_TRANSIT"))
    )


def buildShipmentFactRows(staged: DataFrame, lineCounts: DataFrame, scanSummary: DataFrame) -> DataFrame:
    """stg.Shipment (+ line counts, carrier scans) -> the Fact.Shipment column contract, before key lookups."""
    s = staged.alias("s")
    joined = s.join(lineCounts.alias("lc"), F.col("s.shipment_business_key") == F.col("lc.shipment_business_key"), "left").join(
        scanSummary.alias("sc"),
        (F.col("s.shipment_reference") == F.col("sc.scan_key")) | (F.col("s.shipment_id").cast("string") == F.col("sc.scan_key")),
        "left",
    )
    despatch = F.to_date("s.shipped_date_time_utc")
    delivered = F.coalesce(F.to_date("s.delivered_date_time_utc"), F.col("sc.scan_delivered_date"))
    promised = F.to_date("s.promised_delivery_utc")
    customsCleared = F.when(F.col("s.customs_required_flag") == "Y", F.coalesce(F.col("sc.scan_customs_cleared_date"), delivered))
    customsHold = F.when(customsCleared.isNotNull(), F.datediff(customsCleared, despatch)).otherwise(
        F.when((F.col("s.customs_required_flag") == "Y") & delivered.isNull(), F.datediff(F.current_date(), despatch))
    )
    lost = F.coalesce(F.col("sc.scan_lost_flag"), F.lit(False)) | (F.col("s.shipment_status_code") == "LOST")
    damaged = F.coalesce(F.col("sc.scan_damaged_flag"), F.lit(False))
    attempts = F.coalesce(F.col("sc.scan_attempt_count"), F.when(delivered.isNotNull(), F.lit(1)).otherwise(F.lit(0)))
    weight = F.col("s.total_weight_kg").cast("decimal(18,3)")
    volumetric = (F.col("s.total_volume_m3") * F.lit(DIM_WEIGHT_DIVISOR)).cast("decimal(18,3)")
    onTime = F.when(delivered.isNull() | promised.isNull(), F.lit(None).cast("boolean")).otherwise(delivered <= promised)
    fx = F.when(
        F.col("s.freight_charge_amount").isNotNull() & (F.col("s.freight_charge_amount") != 0),
        (F.col("s.freight_charge_amount_usd") / F.col("s.freight_charge_amount")).cast("decimal(18,8)"),
    )
    return joined.select(
        despatch.alias("despatch_date_key"),
        F.lit(None).cast("date").alias("order_date_key"),
        F.lit(None).cast("date").alias("picked_date_key"),
        F.lit(None).cast("date").alias("packed_date_key"),
        F.coalesce(F.col("sc.scan_collection_date"), despatch).alias("carrier_collection_date_key"),
        customsCleared.alias("customs_cleared_date_key"),
        F.coalesce(F.col("sc.scan_first_attempt_date"), delivered).alias("first_delivery_attempt_key"),
        delivered.alias("delivery_confirmed_date_key"),
        promised.alias("promised_delivery_date_key"),
        F.col("s.customer_id").alias("customer_natural_key"),
        F.col("s.carrier_code").alias("carrier_natural_key"),
        F.col("s.ship_from_warehouse_code").alias("warehouse_natural_key"),
        F.lit(None).cast("string").alias("sales_territory_natural_key"),
        F.col("s.region_code").alias("region_code"),
        F.col("s.shipment_business_key").alias("despatch_note_number"),
        F.lit(None).cast("string").alias("order_number"),
        F.col("s.sale_business_key").alias("invoice_number"),
        F.col("s.shipment_reference").alias("carrier_tracking_number"),
        F.col("s.customs_declaration_ref").alias("customs_declaration_number"),
        F.lit(None).cast("string").alias("incoterm_code"),
        F.lit(None).cast("string").alias("vessel_voyage_reference"),
        F.col("s.service_level_code").alias("service_level_code"),
        F.coalesce(F.col("lc.package_count"), F.lit(0)).cast("int").alias("package_count"),
        weight.alias("total_weight_kg"),
        F.greatest(
            F.coalesce(weight, F.lit(0).cast("decimal(18,3)")), F.coalesce(volumetric, F.lit(0).cast("decimal(18,3)"))
        ).alias("chargeable_weight_kg"),
        F.col("s.total_volume_m3").cast("decimal(18,3)").alias("total_volume_m3"),
        F.col("s.freight_charge_amount").cast("decimal(19,4)").alias("freight_charge"),
        F.lit(0).cast("decimal(19,4)").alias("fuel_surcharge"),
        F.lit(0).cast("decimal(19,4)").alias("duty_and_clearance_amount"),
        F.col("s.freight_charge_amount_usd").cast("decimal(19,4)").alias("freight_charge_reporting"),
        fx.alias("fx_rate_to_reporting"),
        F.lit(None).cast("int").alias("pick_to_despatch_lag_days"),
        F.when(delivered.isNotNull(), F.datediff(delivered, despatch)).alias("despatch_to_delivery_lag_days"),
        customsHold.cast("int").alias("customs_hold_days"),
        F.lit(None).cast("int").alias("order_to_delivery_lag_days"),
        attempts.cast("int").alias("delivery_attempt_count"),
        onTime.alias("on_time_delivery_flag"),
        damaged.alias("damaged_flag"),
        lost.alias("lost_in_transit_flag"),
        shipmentStatus(delivered, lost, F.coalesce(customsHold, F.lit(0)), attempts).alias("shipment_status_code"),
        (delivered.isNotNull() | lost).alias("milestone_complete_flag"),
        F.coalesce(F.col("sc.scan_last_event_utc"), F.col("s.delivered_date_time_utc"), F.col("s.shipped_date_time_utc")).alias(
            "last_milestone_update"
        ),
    )


MILESTONE_COLUMNS = [
    "order_date_key", "picked_date_key", "packed_date_key", "carrier_collection_date_key", "customs_cleared_date_key",
    "first_delivery_attempt_key", "delivery_confirmed_date_key", "promised_delivery_date_key",
]  # fmt: skip
SHIPMENT_FLAG_COLUMNS = ["damaged_flag", "lost_in_transit_flag"]


def mergeShipmentSnapshot(existing: DataFrame, incoming: DataFrame) -> DataFrame:
    """usp_LoadFactShipment: insert new despatch notes; for existing ones apply milestones without regressing.

    Milestone dates use ``COALESCE(existing, incoming)`` (a populated milestone is never overwritten), the damaged
    / lost flags are OR-ed, measures and status are recomputed from the merged milestones, and ``load_datetime`` /
    ``batch_id`` of the original insert are kept while ``last_milestone_update`` advances.
    """
    e = existing.alias("e")
    i = incoming.alias("i")
    key = "despatch_note_number"
    columns = incoming.columns
    matched = e.join(i, key, "inner")
    fresh = i.join(e.select(key), key, "left_anti")
    if matched.limit(1).count() == 0:
        return fresh.select(*columns).unionByName(existing.select(*columns))

    def pick(col: str) -> F.Column:
        if col in MILESTONE_COLUMNS:
            return F.coalesce(F.col(f"e.{col}"), F.col(f"i.{col}"))
        if col in SHIPMENT_FLAG_COLUMNS:
            return F.coalesce(F.col(f"e.{col}"), F.lit(False)) | F.coalesce(F.col(f"i.{col}"), F.lit(False))
        if col in ("batch_id", "load_datetime", "lineage_key", "natural_key_hash"):
            return F.col(f"e.{col}")
        if col == "last_milestone_update":
            return F.greatest(F.col(f"e.{col}"), F.col(f"i.{col}"))
        if col == "delivery_attempt_count":
            return F.greatest(F.col(f"e.{col}"), F.col(f"i.{col}"))
        return F.coalesce(F.col(f"i.{col}"), F.col(f"e.{col}"))

    merged = matched.select(F.col(f"e.{key}").alias(key), *[pick(c).alias(c) for c in columns if c != key])
    delivered = F.col("delivery_confirmed_date_key")
    despatch = F.col("despatch_date_key")
    merged = (
        merged.withColumn(
            "despatch_to_delivery_lag_days", F.when(delivered.isNotNull(), F.datediff(delivered, despatch)).cast("int")
        )
        .withColumn(
            "on_time_delivery_flag",
            F.when(delivered.isNull() | F.col("promised_delivery_date_key").isNull(), F.lit(None).cast("boolean")).otherwise(
                delivered <= F.col("promised_delivery_date_key")
            ),
        )
        .withColumn(
            "customs_hold_days",
            F.when(F.col("customs_cleared_date_key").isNotNull(), F.datediff(F.col("customs_cleared_date_key"), despatch))
            .otherwise(F.col("customs_hold_days"))
            .cast("int"),
        )
        .withColumn(
            "shipment_status_code",
            shipmentStatus(
                delivered,
                F.col("lost_in_transit_flag"),
                F.coalesce(F.col("customs_hold_days"), F.lit(0)),
                F.col("delivery_attempt_count"),
            ),
        )
        .withColumn("milestone_complete_flag", delivered.isNotNull() | F.col("lost_in_transit_flag"))
    )
    untouched = e.join(i.select(key), key, "left_anti").select(*columns)
    return fresh.select(*columns).unionByName(merged.select(*columns)).unionByName(untouched)


def _attachShipmentKeys(ctx: RunContext, rows: DataFrame) -> DataFrame:
    rows = attachKey(rows, dimensionLookup(ctx, CUSTOMER), "customer_natural_key", "customer_key")
    rows = attachKey(rows, dimensionLookup(ctx, CARRIER), "carrier_natural_key", "carrier_key")
    rows = attachKey(rows, dimensionLookup(ctx, WAREHOUSE_SITE), "warehouse_natural_key", "warehouse_site_key")
    rows = attachKey(
        rows,
        dimensionLookup(ctx, SALES_TERRITORY),
        "sales_territory_natural_key",
        "sales_territory_key",
        nullDefault=NOT_APPLICABLE_KEY,
    )
    rows = rows.withColumn("city_key", F.lit(UNKNOWN_MEMBER_KEY))
    return rows.withColumn("inferred_member_flag", inferredMemberFlag("customer_key", "carrier_key", "warehouse_site_key")).drop(
        "customer_natural_key", "carrier_natural_key", "warehouse_natural_key", "sales_territory_natural_key"
    )


def latestStagedShipments(silver: DataFrame) -> DataFrame:
    ranked = silver.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("shipment_business_key").orderBy(F.col("loaded_at_utc").desc(), F.col("row_hash"))
        ),
    )
    return ranked.where(F.col("_rn") == 1).drop("_rn")


def runFactLoadShipment(ctx: RunContext) -> FactResult:
    spark = ctx.spark
    packageName = "FACT_Load_Shipment"
    target = ctx.table(Tables.goldFactShipment)
    silver = spark.table(ctx.table(Tables.silverShipment))
    wmFrom = getWatermark(ctx, SOURCE_SYSTEM_OLTP, "Fact.Shipment", WATERMARK_TYPE)
    if wmFrom is not None:
        silver = silver.where(F.col("loaded_at_utc") > F.lit(wmFrom).cast("timestamp"))
    staged = latestStagedShipments(silver)
    rowsRead = staged.count()

    lines = readTableOrEmpty(
        spark, ctx.table(Tables.silverShipmentLine), T.StructType([T.StructField("shipment_business_key", T.StringType())])
    )
    lineCounts = lines.groupBy("shipment_business_key").agg(F.count(F.lit(1)).alias("package_count"))
    scans = readTableOrEmpty(spark, ctx.table(Tables.bronzeFileCarrierScan), SCAN_EVENTS_SCHEMA)
    incoming = _attachShipmentKeys(ctx, buildShipmentFactRows(staged, lineCounts, summarizeScanEvents(scans)))
    incoming = _load(ctx, incoming).withColumn(
        "natural_key_hash", rowHash(F.col("despatch_note_number"), F.lit(SOURCE_SYSTEM_OLTP))
    )

    existingCount = 0
    if tableExists(spark, target):
        existing = spark.table(target)
        existingCount = existing.count()
        snapshot = mergeShipmentSnapshot(
            existing, incoming.select(*existing.columns) if set(existing.columns) == set(incoming.columns) else incoming
        )
    else:
        snapshot = incoming
    total = snapshot.count()
    overwriteTable(snapshot, target)
    inserted = max(total - existingCount, 0)
    updated = rowsRead - inserted if rowsRead > inserted else 0

    newWatermark = spark.table(ctx.table(Tables.silverShipment)).agg(F.max("loaded_at_utc").cast("string")).collect()[0][0]
    if newWatermark:
        setWatermark(ctx, SOURCE_SYSTEM_OLTP, "Fact.Shipment", WATERMARK_TYPE, newWatermark)
    logRowCount(
        ctx,
        packageName,
        Tables.goldFactShipment,
        {"rows_read": rowsRead, "rows_inserted": inserted, "rows_updated": updated, "rows_total": total},
    )
    return FactResult(rowsRead, inserted, updated, 0)


# ---------------------------------------------------------------------------------------------
# FACT_Load_OrderFulfilment (accumulating snapshot, one row per order)
# ---------------------------------------------------------------------------------------------

PIPELINE_ORDER = ["ORDERED", "ALLOCATED", "PICKED", "DESPATCHED", "DELIVERED", "INVOICED", "CASH"]


def ordersFromLegacyFactOrder(factOrder: DataFrame) -> DataFrame:
    """usp_LoadFactOrderFulfilment step 1: one fulfilment row per order from Fact.Order (order-header grain)."""
    orderNumber = F.coalesce(F.col("Order Number"), F.col("WWI Order ID").cast("string"))
    return factOrder.groupBy(orderNumber.alias("order_number")).agg(
        F.max("WWI Order ID").cast("long").alias("wwi_order_id"),
        F.min(F.col("Order Date Key").cast("date")).alias("order_date_key"),
        F.min(F.col("Order Date Key").cast("date")).alias("allocation_date_key"),
        F.max(F.col("Picked Date Key").cast("date")).alias("pick_date_key"),
        F.max("Customer Key").cast("int").alias("customer_key"),
        F.lit(UNKNOWN_MEMBER_KEY).alias("stock_item_key"),
        F.max("Salesperson Key").cast("int").alias("salesperson_key"),
        F.coalesce(F.max("Sales Channel Key"), F.lit(UNKNOWN_MEMBER_KEY)).cast("int").alias("sales_channel_key"),
        F.coalesce(F.max("Sales Territory Key"), F.lit(NOT_APPLICABLE_KEY)).cast("int").alias("sales_territory_key"),
        F.coalesce(F.max("Region Code"), F.lit(REGION_NA)).alias("region_code"),
        F.count(F.lit(1)).cast("int").alias("order_line_count"),
        F.coalesce(F.max("Transaction Currency Code"), F.lit("USD")).alias("transaction_currency_code"),
        F.sum(F.coalesce(F.col("Quantity Ordered"), F.col("Quantity"))).cast("decimal(18,3)").alias("quantity_ordered"),
        F.sum(
            F.coalesce(F.col("Quantity Despatched"), F.when(F.col("Picked Date Key").isNotNull(), F.col("Quantity")).otherwise(0))
        )
        .cast("decimal(18,3)")
        .alias("quantity_despatched"),
        F.sum(F.coalesce(F.col("Net Order Amount Reporting"), F.col("Net Order Amount"), F.col("Total Excluding Tax")))
        .cast("decimal(19,4)")
        .alias("order_value_reporting"),
        F.max(F.col("Promised Delivery Date Key").cast("date")).alias("promised_delivery_date_key"),
        F.max(F.when(F.upper(F.col("Order Status Code")) == "CANCELLED", F.col("Order Date Key").cast("date"))).alias(
            "cancellation_date_key"
        ),
    )


def ordersFromLegacyStaging(stagingOrders: DataFrame) -> DataFrame:
    """stg.OrderFulfilment rows (the package's declared source) mapped onto the same intermediate shape."""
    o = snakeCaseColumns(stagingOrders)
    return o.select(
        F.col("order_number"),
        F.lit(None).cast("long").alias("wwi_order_id"),
        F.to_date("ordered_at").alias("order_date_key"),
        F.to_date("allocated_at").alias("allocation_date_key"),
        F.to_date("picked_at").alias("pick_date_key"),
        F.lit(UNKNOWN_MEMBER_KEY).alias("customer_key"),
        F.lit(UNKNOWN_MEMBER_KEY).alias("stock_item_key"),
        F.lit(UNKNOWN_MEMBER_KEY).alias("salesperson_key"),
        F.lit(UNKNOWN_MEMBER_KEY).alias("sales_channel_key"),
        F.lit(NOT_APPLICABLE_KEY).alias("sales_territory_key"),
        F.coalesce(F.col("region_code"), F.lit(REGION_NA)).alias("region_code"),
        F.lit(1).alias("order_line_count"),
        F.coalesce(F.col("transaction_currency"), F.lit("USD")).alias("transaction_currency_code"),
        F.lit(None).cast("decimal(18,3)").alias("quantity_ordered"),
        F.lit(None).cast("decimal(18,3)").alias("quantity_despatched"),
        F.coalesce(F.col("order_net_amount_usd"), F.col("order_net_amount")).cast("decimal(19,4)").alias("order_value_reporting"),
        F.lit(None).cast("date").alias("promised_delivery_date_key"),
        F.lit(None).cast("date").alias("cancellation_date_key"),
        F.to_date("invoiced_at").alias("stg_invoice_date_key"),
        F.to_date("cash_received_at").alias("stg_cash_applied_date_key"),
        F.col("invoiced_amount").cast("decimal(19,4)").alias("stg_invoiced_value"),
        F.col("cash_received_amount").cast("decimal(19,4)").alias("stg_cash_applied"),
    )


def invoiceMilestones(factSale: DataFrame, invoices: DataFrame) -> DataFrame:
    """Order -> invoice milestone (Fact.Sale joined to the order through Sales.Invoices.OrderID)."""
    inv = invoices.select(
        F.col("InvoiceID").cast("long").alias("invoice_id"), F.col("OrderID").cast("long").alias("wwi_order_id")
    )
    sale = factSale.groupBy(F.col("WWI Invoice ID").cast("long").alias("invoice_id")).agg(
        F.min(F.col("Invoice Date Key").cast("date")).alias("invoice_date_key"),
        F.min(F.col("Delivery Date Key").cast("date")).alias("sale_delivery_date_key"),
        F.max("Invoice Number").alias("invoice_number"),
        F.sum("Quantity").cast("decimal(18,3)").alias("quantity_invoiced"),
        F.sum(F.coalesce(F.col("Total Excluding Tax Reporting"), F.col("Total Excluding Tax")))
        .cast("decimal(19,4)")
        .alias("invoiced_value_reporting"),
    )
    return (
        sale.join(inv, "invoice_id", "inner")
        .groupBy("wwi_order_id")
        .agg(
            F.min("invoice_date_key").alias("invoice_date_key"),
            F.min("sale_delivery_date_key").alias("sale_delivery_date_key"),
            F.max(F.coalesce(F.col("invoice_number"), F.col("invoice_id").cast("string"))).alias("invoice_number"),
            F.sum("quantity_invoiced").alias("quantity_invoiced"),
            F.sum("invoiced_value_reporting").alias("invoiced_value_reporting"),
        )
    )


def cashMilestones(factPayment: DataFrame, invoiceNumbers: DataFrame) -> DataFrame:
    """Order -> cash milestone through Fact.Payment.Invoice Number (empty on the baseline)."""
    pay = factPayment.groupBy(F.col("Invoice Number").alias("invoice_number")).agg(
        F.max(F.col("Payment Date Key").cast("date")).alias("cash_applied_date_key"),
        F.max("Receipt Number").alias("receipt_number"),
        F.sum(F.coalesce(F.col("Allocated Amount Reporting"), F.col("Allocated Amount")))
        .cast("decimal(19,4)")
        .alias("cash_applied_reporting"),
    )
    return invoiceNumbers.join(pay, "invoice_number", "inner").select(
        "wwi_order_id", "cash_applied_date_key", "receipt_number", "cash_applied_reporting"
    )


def buildOrderFulfilmentRows(
    orders: DataFrame, shipments: DataFrame, invoices: DataFrame, cash: DataFrame, asOfDate: F.Column | None = None
) -> DataFrame:
    """Milestones -> lags, pipeline status, SLA breach, stalled and cycle-complete flags (usp_LoadFactOrderFulfilment)."""
    asOf = asOfDate if asOfDate is not None else F.current_date()
    o = orders.alias("o")
    sh = shipments.alias("sh")
    joined = (
        o.join(sh, F.col("o.order_number") == F.col("sh.order_number"), "left")
        .join(invoices.alias("inv"), F.col("o.wwi_order_id") == F.col("inv.wwi_order_id"), "left")
        .join(cash.alias("c"), F.col("o.wwi_order_id") == F.col("c.wwi_order_id"), "left")
    )
    stgInvoice = F.col("o.stg_invoice_date_key") if "stg_invoice_date_key" in orders.columns else F.lit(None).cast("date")
    stgCash = F.col("o.stg_cash_applied_date_key") if "stg_cash_applied_date_key" in orders.columns else F.lit(None).cast("date")
    stgInvoiced = F.col("o.stg_invoiced_value") if "stg_invoiced_value" in orders.columns else F.lit(None).cast("decimal(19,4)")
    stgCashValue = F.col("o.stg_cash_applied") if "stg_cash_applied" in orders.columns else F.lit(None).cast("decimal(19,4)")

    orderDate = F.col("o.order_date_key")
    pickDate = F.col("o.pick_date_key")
    despatchDate = F.coalesce(F.col("sh.despatch_date_key"), F.when(F.col("o.quantity_despatched") > 0, pickDate))
    deliveryDate = F.coalesce(F.col("sh.delivery_confirmed_date_key"), F.col("inv.sale_delivery_date_key"))
    invoiceDate = F.coalesce(F.col("inv.invoice_date_key"), stgInvoice)
    cashDate = F.coalesce(F.col("c.cash_applied_date_key"), stgCash)
    cancelled = F.col("o.cancellation_date_key").isNotNull()
    lastMilestone = F.greatest(
        orderDate,
        F.coalesce(F.col("o.allocation_date_key"), orderDate),
        F.coalesce(pickDate, orderDate),
        F.coalesce(despatchDate, orderDate),
        F.coalesce(deliveryDate, orderDate),
        F.coalesce(invoiceDate, orderDate),
        F.coalesce(cashDate, orderDate),
    )
    status = (
        F.when(cancelled, F.lit("CANCELLED"))
        .when(cashDate.isNotNull(), F.lit("CASH"))
        .when(invoiceDate.isNotNull(), F.lit("INVOICED"))
        .when(deliveryDate.isNotNull(), F.lit("DELIVERED"))
        .when(despatchDate.isNotNull(), F.lit("DESPATCHED"))
        .when(pickDate.isNotNull(), F.lit("PICKED"))
        .when(F.col("o.allocation_date_key").isNotNull(), F.lit("ALLOCATED"))
        .otherwise(F.lit("ORDERED"))
    )
    openMilestones = sum(
        F.when(m.isNull(), 1).otherwise(0)
        for m in (F.col("o.allocation_date_key"), pickDate, despatchDate, deliveryDate, invoiceDate, cashDate)
    )
    cycleComplete = cashDate.isNotNull() | cancelled
    threshold = regionStalledThresholdDays(F.col("o.region_code"))
    stalled = (~cycleComplete) & (F.datediff(asOf, lastMilestone) > threshold)
    serviceTarget = F.when(F.col("o.region_code") == REGION_NA, 5).when(F.col("o.region_code") == REGION_EU, 7).otherwise(10)
    pickBreach = F.when(pickDate.isNotNull(), F.datediff(pickDate, orderDate) > 2).otherwise(F.lit(False))
    deliveryBreach = F.when(deliveryDate.isNotNull(), F.datediff(deliveryDate, orderDate) > serviceTarget).otherwise(F.lit(False))
    perfectOrder = cycleComplete & ~cancelled & ~pickBreach & ~deliveryBreach
    return joined.select(
        orderDate.alias("order_date_key"),
        F.col("o.allocation_date_key").alias("allocation_date_key"),
        pickDate.alias("pick_date_key"),
        F.lit(None).cast("date").alias("pack_date_key"),
        despatchDate.alias("despatch_date_key"),
        deliveryDate.alias("delivery_date_key"),
        invoiceDate.alias("invoice_date_key"),
        cashDate.alias("cash_applied_date_key"),
        F.col("o.cancellation_date_key").alias("cancellation_date_key"),
        F.col("o.customer_key").alias("customer_key"),
        F.col("o.stock_item_key").alias("stock_item_key"),
        F.col("o.salesperson_key").alias("salesperson_key"),
        F.coalesce(F.col("sh.warehouse_site_key"), F.lit(UNKNOWN_MEMBER_KEY)).cast("int").alias("warehouse_site_key"),
        F.coalesce(F.col("sh.carrier_key"), F.lit(UNKNOWN_MEMBER_KEY)).cast("int").alias("carrier_key"),
        F.col("o.sales_channel_key").alias("sales_channel_key"),
        F.col("o.sales_territory_key").alias("sales_territory_key"),
        F.col("o.region_code").alias("region_code"),
        F.col("o.order_number").alias("order_number"),
        F.lit(0).alias("order_line_number"),
        F.col("sh.despatch_note_number").alias("despatch_note_number"),
        F.col("inv.invoice_number").alias("invoice_number"),
        F.col("c.receipt_number").alias("receipt_number"),
        F.col("o.order_line_count").alias("order_line_count"),
        F.col("o.transaction_currency_code").alias("transaction_currency_code"),
        F.col("o.quantity_ordered").alias("quantity_ordered"),
        F.col("o.quantity_despatched").alias("quantity_despatched"),
        F.col("inv.quantity_invoiced").alias("quantity_invoiced"),
        F.col("o.order_value_reporting").alias("order_value_reporting"),
        F.coalesce(F.col("inv.invoiced_value_reporting"), stgInvoiced).alias("invoiced_value_reporting"),
        F.coalesce(F.col("c.cash_applied_reporting"), stgCashValue).alias("cash_applied_reporting"),
        F.datediff(pickDate, orderDate).alias("order_to_pick_lag_days"),
        F.datediff(despatchDate, pickDate).alias("pick_to_despatch_lag_days"),
        F.datediff(deliveryDate, despatchDate).alias("despatch_to_delivery_lag_days"),
        F.datediff(invoiceDate, deliveryDate).alias("delivery_to_invoice_lag_days"),
        F.datediff(cashDate, invoiceDate).alias("invoice_to_cash_lag_days"),
        F.datediff(cashDate, orderDate).alias("order_to_cash_cycle_days"),
        serviceTarget.cast("int").alias("service_target_days"),
        pickBreach.alias("pick_sla_breach_flag"),
        deliveryBreach.alias("delivery_sla_breach_flag"),
        perfectOrder.alias("perfect_order_flag"),
        cancelled.alias("cancelled_flag"),
        openMilestones.cast("int").alias("open_milestone_count"),
        status.alias("pipeline_status_code"),
        stalled.alias("stalled_flag"),
        F.when(stalled, F.concat(F.lit("NO_MILESTONE_"), threshold.cast("string"), F.lit("D"))).alias("stalled_reason_code"),
        cycleComplete.alias("cycle_complete_flag"),
        lastMilestone.cast("timestamp").alias("last_milestone_update"),
    )


def mergeOrderFulfilmentSnapshot(existing: DataFrame, incoming: DataFrame, reopenClosed: bool = False) -> DataFrame:
    """Existing rows whose cycle is complete are frozen unless ``reopenClosed``; everything else takes the new row."""
    key = "order_number"
    columns = incoming.columns
    frozen = existing.where(F.col("cycle_complete_flag") & ~F.lit(reopenClosed)).select(*columns)
    refreshed = incoming.join(frozen.select(key), key, "left_anti")
    kept = existing.join(incoming.select(key), key, "left_anti").select(*columns)
    return refreshed.select(*columns).unionByName(frozen).unionByName(kept)


def runFactLoadOrderFulfilment(ctx: RunContext, seedFromFactOrder: bool = True, reopenClosed: bool = False) -> FactResult:
    spark = ctx.spark
    packageName = "FACT_Load_OrderFulfilment"
    target = ctx.table(Tables.goldFactOrderFulfilment)

    staging = snakeCaseColumns(readLegacy(spark, ctx.legacyStaging, "stg", "OrderFulfilment"))
    orders = ordersFromLegacyStaging(staging) if staging.limit(1).count() > 0 else None
    if orders is None and seedFromFactOrder:
        orders = ordersFromLegacyFactOrder(readLegacy(spark, ctx.legacyDw, "Fact", "Order"))
    if orders is None:
        orders = ordersFromLegacyFactOrder(spark.createDataFrame([], T.StructType([])))
    rowsRead = orders.count()

    shipments = readTableOrEmpty(
        spark,
        ctx.table(Tables.goldFactShipment),
        T.StructType(
            [
                T.StructField("order_number", T.StringType()),
                T.StructField("despatch_note_number", T.StringType()),
                T.StructField("despatch_date_key", T.DateType()),
                T.StructField("delivery_confirmed_date_key", T.DateType()),
                T.StructField("warehouse_site_key", T.IntegerType()),
                T.StructField("carrier_key", T.IntegerType()),
            ]
        ),
    ).select(
        "order_number",
        "despatch_note_number",
        "despatch_date_key",
        "delivery_confirmed_date_key",
        "warehouse_site_key",
        "carrier_key",
    )
    shipments = shipments.where(F.col("order_number").isNotNull()).dropDuplicates(["order_number"])

    factSale = readLegacy(spark, ctx.legacyDw, "Fact", "Sale")
    oltpInvoices = spark.table(ctx.legacy(ctx.legacyOltp, "Sales", "Invoices"))
    invoices = invoiceMilestones(factSale, oltpInvoices)
    cash = cashMilestones(readLegacy(spark, ctx.legacyDw, "Fact", "Payment"), invoices.select("wwi_order_id", "invoice_number"))

    incoming = _load(ctx, buildOrderFulfilmentRows(orders, shipments, invoices, cash)).withColumn(
        "natural_key_hash", rowHash(F.col("order_number"), F.col("order_line_number"))
    )
    existingCount = 0
    if tableExists(spark, target):
        existing = spark.table(target)
        existingCount = existing.count()
        snapshot = mergeOrderFulfilmentSnapshot(existing, incoming, reopenClosed=reopenClosed)
    else:
        snapshot = incoming
    total = snapshot.count()
    overwriteTable(snapshot, target)
    inserted = max(total - existingCount, 0)
    logRowCount(
        ctx,
        packageName,
        Tables.goldFactOrderFulfilment,
        {"rows_read": rowsRead, "rows_inserted": inserted, "rows_updated": rowsRead - inserted, "rows_total": total},
    )
    return FactResult(rowsRead, inserted, rowsRead - inserted, 0)


# ---------------------------------------------------------------------------------------------
# FACT_Load_Return (incremental; delete-and-reload of the return-date range)
# ---------------------------------------------------------------------------------------------


def statutoryWindowDays(regionCode: F.Column, reasonWindowDays: F.Column) -> F.Column:
    """EU 14 days; APAC the reason's window (default 7); NA 30 days from the original invoice."""
    return (
        F.when(regionCode == REGION_EU, F.lit(EU_RETURN_WINDOW_DAYS))
        .when(regionCode == REGION_APAC, F.coalesce(reasonWindowDays, F.lit(APAC_DEFAULT_RETURN_WINDOW_DAYS)))
        .otherwise(F.lit(NA_RETURN_WINDOW_DAYS))
    )


def statutoryAnchorDate(regionCode: F.Column, deliveryDate: F.Column, invoiceDate: F.Column) -> F.Column:
    """EU counts from delivery (invoice fallback); NA/APAC from the original invoice."""
    return F.when(regionCode == REGION_EU, F.coalesce(deliveryDate, invoiceDate)).otherwise(invoiceDate)


def restockingFee(
    regionCode: F.Column, daysSinceInvoice: F.Column, withinWindow: F.Column, gross: F.Column, sourceFee: F.Column
) -> F.Column:
    """FACT_Load_Return "Derive Return Measures": EU in-window 0; NA > 30 days 15 %; APAC flat 5.00; else source fee."""
    return (
        F.when((regionCode == REGION_EU) & withinWindow, F.lit(0))
        .when((regionCode == REGION_NA) & (daysSinceInvoice > NA_RETURN_WINDOW_DAYS), gross * F.lit(NA_RESTOCKING_PERCENT) / 100)
        .when(regionCode == REGION_APAC, F.lit(APAC_RESTOCKING_FLAT_FEE))
        .otherwise(F.coalesce(sourceFee, F.lit(0)))
    ).cast("decimal(19,4)")


def buildReturnFactRows(staged: DataFrame, originalSales: DataFrame, reasonWindows: DataFrame) -> DataFrame:
    """stg.Return + original Fact.Sale + reason windows -> the Fact.Return contract (negative measures)."""
    s = staged.alias("s")
    joined = s.join(originalSales.alias("os"), F.col("s.original_invoice_id") == F.col("os.original_invoice_id"), "left").join(
        reasonWindows.alias("rw"),
        (F.col("s.return_reason_code") == F.col("rw.reason_code")) & (F.col("s.region_code") == F.col("rw.reason_region_code")),
        "left",
    )
    region = F.col("s.region_code")
    returnDate = F.col("s.returned_date")
    invoiceDate = F.col("os.original_invoice_date_key")
    anchor = statutoryAnchorDate(region, F.col("os.original_delivery_date_key"), invoiceDate)
    windowDays = statutoryWindowDays(region, F.col("rw.reason_window_days"))
    daysSince = F.datediff(returnDate, anchor)
    withinWindow = F.when(daysSince.isNull(), F.lit(None).cast("boolean")).otherwise(daysSince <= windowDays)
    qty = F.col("s.returned_quantity").cast("decimal(18,3)")
    gross = F.col("s.refund_amount").cast("decimal(19,4)")
    fee = restockingFee(
        region, F.coalesce(daysSince, F.lit(0)), F.coalesce(withinWindow, F.lit(False)), gross, F.col("s.restocking_fee_amount")
    )
    taxRate = F.coalesce(F.col("os.original_tax_rate"), F.lit(0)).cast("decimal(9,3)")
    tax = (gross * taxRate / (100 + taxRate)).cast("decimal(19,4)")
    netCredit = (gross - fee).cast("decimal(19,4)")
    unitCost = F.when(
        F.col("os.original_quantity").isNotNull() & (F.col("os.original_quantity") != 0),
        F.col("os.original_cost_amount") / F.col("os.original_quantity"),
    )
    costReturned = (unitCost * qty).cast("decimal(19,4)")
    fx = F.when(gross.isNotNull() & (gross != 0), (F.col("s.refund_amount_usd") / gross).cast("decimal(18,8)"))
    faulty = F.upper(F.col("s.return_reason_code")).isin(*FAULTY_REASON_CODES) | F.coalesce(
        F.col("rw.reason_is_quality_defect"), F.lit(False)
    )
    disposition = (
        F.when(F.upper(F.col("s.inspection_result_code")).isin("DAMAGED", "SCRAP"), F.lit("SCRAP"))
        .when(F.upper(F.col("s.return_reason_code")).isin(*SCRAP_REASON_CODES), F.lit("SCRAP"))
        .otherwise(F.lit("RESTOCK"))
    )
    return joined.select(
        returnDate.alias("return_date_key"),
        invoiceDate.alias("original_invoice_date_key"),
        F.lit(None).cast("date").alias("credit_issued_date_key"),
        F.col("s.customer_id").alias("customer_natural_key"),
        F.col("s.stock_item_id").alias("stock_item_natural_key"),
        F.col("s.return_reason_code").alias("return_reason_natural_key"),
        F.coalesce(F.col("os.original_sales_territory_key"), F.lit(NOT_APPLICABLE_KEY)).cast("int").alias("sales_territory_key"),
        F.coalesce(F.col("os.original_salesperson_key"), F.lit(NOT_APPLICABLE_KEY)).cast("int").alias("salesperson_key"),
        F.lit(UNKNOWN_MEMBER_KEY).alias("warehouse_site_key"),
        F.lit(UNKNOWN_MEMBER_KEY).alias("currency_key"),
        region.alias("region_code"),
        F.col("s.rma_number").alias("rma_number"),
        F.col("s.return_line_id").cast("int").alias("rma_line_number"),
        F.coalesce(F.col("os.original_invoice_number"), F.col("s.original_invoice_id").cast("string")).alias(
            "original_invoice_number"
        ),
        F.lit(None).cast("int").alias("original_invoice_line_number"),
        F.lit(None).cast("string").alias("credit_note_number"),
        F.col("s.source_return_reason_code").alias("return_reason_free_text"),
        (-qty).alias("quantity_returned"),
        (-F.coalesce(F.col("s.restocked_quantity"), F.lit(0))).cast("decimal(18,3)").alias("quantity_restocked"),
        (-F.coalesce(F.col("s.scrapped_quantity"), F.lit(0))).cast("decimal(18,3)").alias("quantity_scrapped"),
        F.lit("EA").alias("source_uom_code"),
        F.col("s.transaction_currency_code").alias("transaction_currency_code"),
        (-gross).alias("gross_return_amount"),
        fee.alias("restocking_fee_amount"),
        (-tax).alias("tax_amount"),
        (-netCredit).alias("net_credit_amount"),
        fx.alias("fx_rate_to_reporting"),
        (-F.col("s.refund_amount_usd")).cast("decimal(19,4)").alias("net_credit_amount_reporting"),
        (-costReturned).alias("cost_of_returned_goods"),
        (-(netCredit - F.coalesce(costReturned, F.lit(0)))).cast("decimal(19,4)").alias("margin_reversed"),
        daysSince.cast("int").alias("days_since_invoice"),
        withinWindow.alias("within_statutory_window_flag"),
        windowDays.cast("int").alias("statutory_window_days"),
        faulty.alias("faulty_goods_flag"),
        disposition.alias("disposition_code"),
        F.col("os.original_invoice_id").isNull().alias("original_sale_missing_flag"),
        F.col("s.return_line_business_key").alias("return_line_business_key"),
        F.col("s.loaded_at_utc").alias("staged_at_utc"),
    )


def _attachReturnKeys(ctx: RunContext, rows: DataFrame) -> DataFrame:
    rows = attachKey(rows, dimensionLookup(ctx, CUSTOMER), "customer_natural_key", "customer_key")
    rows = attachKey(rows, dimensionLookup(ctx, STOCK_ITEM), "stock_item_natural_key", "stock_item_key")
    rows = attachKey(rows, dimensionLookup(ctx, RETURN_REASON), "return_reason_natural_key", "return_reason_key")
    return rows.withColumn(
        "inferred_member_flag",
        inferredMemberFlag("customer_key", "stock_item_key", "return_reason_key") | F.col("original_sale_missing_flag"),
    ).drop("customer_natural_key", "stock_item_natural_key", "return_reason_natural_key")


def saleReversalRows(returnRows: DataFrame) -> DataFrame:
    """ "Write Sale Reversal Rows": the negated sale measures keyed to the return (work.SaleReversal equivalent)."""
    return returnRows.select(
        F.col("return_line_business_key"),
        F.col("original_invoice_number"),
        F.col("return_date_key").alias("reversal_date_key"),
        F.col("customer_key"),
        F.col("stock_item_key"),
        F.col("quantity_returned").alias("quantity_reversed"),
        F.col("net_credit_amount").alias("net_amount_reversed"),
        F.col("tax_amount").alias("tax_amount_reversed"),
        F.col("cost_of_returned_goods").alias("cost_reversed"),
        F.col("margin_reversed"),
        F.col("transaction_currency_code"),
        F.col("region_code"),
        F.col("batch_id"),
        F.col("load_datetime"),
    )


def runFactLoadReturn(ctx: RunContext) -> FactResult:
    spark = ctx.spark
    packageName = "FACT_Load_Return"
    target = ctx.table(Tables.goldFactReturn)
    silver = spark.table(ctx.table(Tables.silverReturn))
    wmFrom = getWatermark(ctx, SOURCE_SYSTEM_OLTP, "Fact.Return", WATERMARK_TYPE)
    if wmFrom is not None:
        silver = silver.where(F.col("loaded_at_utc") > F.lit(wmFrom).cast("timestamp"))
    rowsRead = silver.count()

    rows = _attachReturnKeys(ctx, buildReturnFactRows(silver, legacyFactSale(ctx), returnReasonWindows(ctx)))
    rows = _load(ctx, rows).withColumn(
        "natural_key_hash", rowHash(F.col("rma_number"), F.col("rma_line_number"), F.lit(SOURCE_SYSTEM_OLTP))
    )
    inserted = rows.count()

    if inserted:
        bounds = rows.agg(F.min("return_date_key"), F.max("return_date_key")).collect()[0]
        if tableExists(spark, target):
            spark.sql(f"DELETE FROM {target} WHERE return_date_key BETWEEN '{bounds[0]}' AND '{bounds[1]}'")
        appendTable(rows.drop("staged_at_utc"), target)
        appendTable(saleReversalRows(rows), ctx.table(Tables.goldFactSaleReversal))
    else:
        ensureTable(spark, target, rows.drop("staged_at_utc").schema)
        ensureTable(spark, ctx.table(Tables.goldFactSaleReversal), saleReversalRows(rows).schema)

    orphan = rows.where(F.col("original_sale_missing_flag"))
    orphanCount = orphan.count()
    if orphanCount:
        appendTable(
            rejectRows(
                orphan,
                ctx,
                packageName,
                "RETURN_NO_ORIGINAL_SALE",
                "Original sale not found in Fact.Sale; loaded with inferred cost",
                "return_line_business_key",
            ),
            ctx.table(Tables.errRejectedFact),
        )
    newWatermark = spark.table(ctx.table(Tables.silverReturn)).agg(F.max("loaded_at_utc").cast("string")).collect()[0][0]
    if newWatermark:
        setWatermark(ctx, SOURCE_SYSTEM_OLTP, "Fact.Return", WATERMARK_TYPE, newWatermark)
    logRowCount(
        ctx,
        packageName,
        Tables.goldFactReturn,
        {"rows_read": rowsRead, "rows_inserted": inserted, "rows_orphan_original_sale": orphanCount},
    )
    return FactResult(rowsRead, inserted, 0, orphanCount)


# ---------------------------------------------------------------------------------------------
# FACT_Load_CreditNote (incremental; duplicate / return-linked / approval-hold safeguards)
# ---------------------------------------------------------------------------------------------


def taxRegime(regionCode: F.Column) -> F.Column:
    return F.when(regionCode == REGION_NA, F.lit("SALESTAX")).when(regionCode == REGION_EU, F.lit("VAT")).otherwise(F.lit("GST"))


def buildCreditNoteFactRows(staged: DataFrame, originalSales: DataFrame) -> DataFrame:
    """stg.CreditNote + original Fact.Sale -> the Fact.Credit Note contract (negative measures, regional tax fields)."""
    s = staged.alias("s")
    joined = s.join(originalSales.alias("os"), F.col("s.original_invoice_id") == F.col("os.original_invoice_id"), "left")
    region = F.col("s.region_code")
    net = F.col("s.net_amount").cast("decimal(19,4)")
    tax = F.col("s.tax_amount").cast("decimal(19,4)")
    gross = F.col("s.gross_amount").cast("decimal(19,4)")
    rate = F.when(net != 0, (tax / net * 100)).otherwise(F.lit(0)).cast("decimal(9,3)")
    regime = taxRegime(region)
    originalNet = F.coalesce(F.col("os.original_net_amount"), F.lit(0))
    secondApproval = (gross > CREDIT_SECOND_APPROVAL_THRESHOLD) | (
        F.when(F.col("os.original_net_amount").isNull(), F.lit(0)).otherwise(gross) > originalNet
    )
    reason = F.upper(F.col("s.credit_reason_code"))
    taxAdjustReason = (
        F.when(regime == "VAT", F.lit("VAT_CREDIT_NOTE"))
        .when(regime == "GST", F.lit("GST_ADJUSTMENT"))
        .otherwise(F.lit("SALES_TAX_REFUND"))
    )
    fx = F.when(net != 0, (F.col("s.net_amount_usd") / net).cast("decimal(18,8)"))
    return joined.select(
        F.col("s.credit_note_date").alias("credit_note_date_key"),
        F.col("os.original_invoice_date_key").alias("original_invoice_date_key"),
        F.col("s.customer_id").alias("customer_natural_key"),
        F.coalesce(F.col("os.original_bill_to_customer_key"), F.lit(UNKNOWN_MEMBER_KEY))
        .cast("int")
        .alias("bill_to_customer_key"),
        F.lit(UNKNOWN_MEMBER_KEY).alias("stock_item_key"),
        F.coalesce(F.col("os.original_salesperson_key"), F.lit(NOT_APPLICABLE_KEY)).cast("int").alias("salesperson_key"),
        F.coalesce(F.col("os.original_sales_territory_key"), F.lit(NOT_APPLICABLE_KEY)).cast("int").alias("sales_territory_key"),
        F.lit(UNKNOWN_MEMBER_KEY).alias("currency_key"),
        F.lit(None).cast("long").alias("return_key"),
        region.alias("region_code"),
        F.col("s.credit_note_number").alias("credit_note_number"),
        F.lit(1).alias("credit_note_line_number"),
        F.coalesce(F.col("os.original_invoice_number"), F.col("s.original_invoice_id").cast("string")).alias(
            "original_invoice_number"
        ),
        F.col("s.credit_reason_code").alias("credit_reason_code"),
        taxAdjustReason.alias("tax_adjustment_reason_code"),
        F.lit(None).cast("int").alias("approved_by_employee_key"),
        F.col("s.transaction_currency_code").alias("transaction_currency_code"),
        F.lit(None).cast("decimal(18,3)").alias("quantity_credited"),
        (-net).alias("credit_excluding_tax"),
        (-tax).alias("tax_credit_amount"),
        (-gross).alias("credit_including_tax"),
        fx.alias("fx_rate_to_reporting"),
        (-(F.col("s.net_amount_usd") + F.coalesce(F.col("s.net_amount_usd") * tax / F.when(net != 0, net), F.lit(0))))
        .cast("decimal(19,4)")
        .alias("credit_including_tax_reporting"),
        regime.alias("tax_regime_code"),
        F.when(regime == "VAT", rate).alias("vat_rate"),
        F.when(regime == "GST", rate).alias("gst_rate"),
        F.when(regime == "SALESTAX", rate).alias("sales_tax_rate"),
        reason.isin("GOODWILL", "GW", "NOR").alias("goodwill_flag"),
        reason.isin("REBATE", "RBT").alias("rebate_settlement_flag"),
        F.year("s.credit_note_date").cast("smallint").alias("fiscal_year"),
        F.month("s.credit_note_date").cast("smallint").alias("fiscal_period"),
        F.col("os.original_invoice_id").isNull().alias("original_sale_missing_flag"),
        secondApproval.alias("requires_second_approval_flag"),
        F.col("s.approved_by_name").alias("approved_by_name"),
        gross.alias("credit_amount"),
        F.col("s.rma_number").alias("rma_number"),
        F.col("s.credit_note_business_key").alias("credit_note_business_key"),
        originalNet.cast("decimal(19,4)").alias("original_net_amount"),
        F.coalesce(F.col("os.original_tax_amount"), F.lit(0)).cast("decimal(19,4)").alias("original_tax_amount"),
    )


def splitCreditNotes(
    rows: DataFrame,
    existingNumbers: DataFrame,
    returnLinkedNumbers: DataFrame,
    approvalThreshold: float = CREDIT_APPROVAL_THRESHOLD,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Returns ``(toLoad, duplicates, returnLinked, held)``.

    Duplicates = credit note number already in the fact; return-linked = carries an RMA or is referenced by a return;
    held = above the approval threshold without a named approver (parked in the approval-hold table, not loaded).
    """
    duplicates = rows.join(existingNumbers.select("credit_note_number"), "credit_note_number", "left_semi")
    remaining = rows.join(existingNumbers.select("credit_note_number"), "credit_note_number", "left_anti")
    linkedNumbers = (
        returnLinkedNumbers.select(F.col("credit_note_number")).where(F.col("credit_note_number").isNotNull()).distinct()
    )
    linked = remaining.where(F.col("rma_number").isNotNull()).unionByName(
        remaining.where(F.col("rma_number").isNull()).join(linkedNumbers, "credit_note_number", "left_semi")
    )
    unlinked = remaining.join(linked.select("credit_note_business_key"), "credit_note_business_key", "left_anti")
    heldCondition = (F.col("credit_amount") > approvalThreshold) & F.col("approved_by_name").isNull()
    held = unlinked.where(heldCondition)
    toLoad = unlinked.where(~heldCondition)
    return toLoad, duplicates, linked, held


def saleRestatementRows(creditRows: DataFrame) -> DataFrame:
    """ "Restate Original Sale" + "Record Restatement Audit": the original sale net of the credit, per credit note."""
    return creditRows.select(
        F.col("credit_note_number"),
        F.col("original_invoice_number"),
        F.col("credit_note_date_key").alias("restated_date_key"),
        F.col("customer_key"),
        F.col("original_net_amount"),
        F.col("original_tax_amount"),
        (F.col("original_net_amount") + F.col("credit_excluding_tax")).cast("decimal(19,4)").alias("restated_net_amount"),
        (F.col("original_tax_amount") + F.col("tax_credit_amount")).cast("decimal(19,4)").alias("restated_tax_amount"),
        F.when(F.col("original_sale_missing_flag"), F.lit("ORIGINAL_SALE_NOT_FOUND"))
        .otherwise(F.lit("RESTATED"))
        .alias("restatement_result_code"),
        F.col("region_code"),
        F.col("batch_id"),
        F.col("load_datetime"),
    )


def runFactLoadCreditNote(ctx: RunContext, approvalThreshold: float = CREDIT_APPROVAL_THRESHOLD) -> FactResult:
    spark = ctx.spark
    packageName = "FACT_Load_CreditNote"
    target = ctx.table(Tables.goldFactCreditNote)
    silver = spark.table(ctx.table(Tables.silverCreditNote))
    wmFrom = getWatermark(ctx, SOURCE_SYSTEM_OLTP, "Fact.CreditNote", WATERMARK_TYPE)
    if wmFrom is not None:
        silver = silver.where(F.col("loaded_at_utc") > F.lit(wmFrom).cast("timestamp"))
    rowsRead = silver.count()

    rows = buildCreditNoteFactRows(silver, legacyFactSale(ctx))
    rows = attachKey(rows, dimensionLookup(ctx, CUSTOMER), "customer_natural_key", "customer_key").drop("customer_natural_key")
    rows = rows.withColumn("inferred_member_flag", inferredMemberFlag("customer_key") | F.col("original_sale_missing_flag"))
    rows = _load(ctx, rows).withColumn(
        "natural_key_hash", rowHash(F.col("credit_note_number"), F.col("credit_note_line_number"), F.lit(SOURCE_SYSTEM_OLTP))
    )
    existing = readTableOrEmpty(spark, target, T.StructType([T.StructField("credit_note_number", T.StringType())]))
    returns = readTableOrEmpty(
        spark, ctx.table(Tables.goldFactReturn), T.StructType([T.StructField("credit_note_number", T.StringType())])
    )
    toLoad, duplicates, linked, held = splitCreditNotes(rows, existing, returns, approvalThreshold)
    inserted = toLoad.count()

    helperColumns = {
        "requires_second_approval_flag", "approved_by_name", "credit_amount", "rma_number",
        "credit_note_business_key", "original_net_amount", "original_tax_amount",
    }  # fmt: skip
    factColumns = [c for c in toLoad.columns if c not in helperColumns]
    if inserted:
        appendTable(toLoad.select(*factColumns), target)
        appendTable(saleRestatementRows(toLoad), ctx.table(Tables.goldFactSaleRestatement))
    else:
        ensureTable(spark, target, toLoad.select(*factColumns).schema)
        ensureTable(spark, ctx.table(Tables.goldFactSaleRestatement), saleRestatementRows(toLoad).schema)

    heldCount = held.count()
    holdTable = ctx.table(Tables.goldCreditNoteApprovalHold)
    holdRows = held.select(
        "credit_note_business_key", "credit_note_number", "credit_note_date_key", "customer_key", "region_code", "credit_amount",
        "transaction_currency_code", "requires_second_approval_flag", "batch_id", "load_datetime",
    )  # fmt: skip
    if heldCount:
        appendTable(holdRows, holdTable)
    else:
        ensureTable(spark, holdTable, holdRows.schema)
    rejected = 0
    for frame, code, text in (
        (duplicates, "CREDIT_DUPLICATE", "Credit note number already loaded"),
        (linked, "CREDIT_RETURN_LINKED", "Credit note is linked to a return; credited through Fact.Return"),
        (
            rows.where(F.col("original_sale_missing_flag")),
            "CREDIT_NO_ORIGINAL_SALE",
            "Original sale not found in Fact.Sale; loaded with unknown members",
        ),
    ):
        count = frame.count()
        rejected += count
        if count:
            appendTable(
                rejectRows(frame, ctx, packageName, code, text, "credit_note_business_key"), ctx.table(Tables.errRejectedFact)
            )

    newWatermark = spark.table(ctx.table(Tables.silverCreditNote)).agg(F.max("loaded_at_utc").cast("string")).collect()[0][0]
    if newWatermark:
        setWatermark(ctx, SOURCE_SYSTEM_OLTP, "Fact.CreditNote", WATERMARK_TYPE, newWatermark)
    logRowCount(
        ctx,
        packageName,
        Tables.goldFactCreditNote,
        {
            "rows_read": rowsRead,
            "rows_inserted": inserted,
            "rows_held_for_approval": heldCount,
            "rows_rejected_or_flagged": rejected,
        },
    )
    return FactResult(rowsRead, inserted, 0, heldCount + rejected)
