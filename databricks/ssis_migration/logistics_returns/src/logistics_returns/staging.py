"""STG_Load_Shipment and STG_Load_ReturnAndCredit (incremental_append: raw.* -> stg.*).

Both packages share the control flow Get Watermark (etl.usp_GetWatermark, timestamp watermark on the business
date) -> Data Flow(s) (OLE DB source bounded by the watermark -> Derived Column "Standardize" -> Lookup ->
Conditional Split -> stg.* | err.*) -> stg.usp_AppendIncremental_* -> Set Watermark -> Log Row Counts.

The pure transformations are module-level functions over DataFrames so they run on local Spark in the tests;
``runStgLoadShipment`` / ``runStgLoadReturnAndCredit`` wire them to the landing tables.
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
    loadMetadata,
    logRowCount,
    readTableOrEmpty,
    rowHash,
    setWatermark,
    sourceSystemKey,
    tableExists,
    upperCode,
)
from logistics_returns.config import SOURCE_SYSTEM_OLTP, RunContext, Tables
from logistics_returns.reference import (
    REGION_APAC,
    REGION_EU,
    REGION_NA,
    carrierReference,
    convertToUsd,
    countryRegion,
    currencyRegion,
    fxRatesToUsd,
    regionStatutoryWindowDays,
    returnReasonCrosswalk,
)

WATERMARK_TYPE = "Timestamp"
UNKNOWN_CARRIER = "UNKN"
DEFAULT_SERVICE_LEVEL = "STD"
UNSTATED_REASON = "UNSTATED"
COLD_CHAIN_SERVICE_LEVELS = ("CHILL", "CHILLED", "COOL", "FROZEN")
COLD_CHAIN_MAX_C = 8.0
COLD_CHAIN_MIN_C = -2.0
APPROVAL_AUTO_LIMIT = 500
APPROVAL_MANAGER_LIMIT = 5000

REJECT_COLUMNS = [
    "package_name",
    "reject_reason_code",
    "reject_reason_text",
    "business_key",
    "source_record",
    "batch_id",
    "package_execution_id",
    "loaded_at_utc",
    "source_system_code",
]


@dataclass
class StagingResult:
    rowsRead: int
    rowsLoaded: int
    rowsRejected: int
    watermarkFrom: str | None
    watermarkTo: str | None


def _bool(flag: bool) -> str:
    return "Y" if flag else "N"


def rejectRows(df: DataFrame, ctx: RunContext, packageName: str, reasonCode: str, reasonText: str, keyCol: str) -> DataFrame:
    """Shape any rejected rows onto the shared err.* contract (source record kept as JSON)."""
    return df.select(
        F.lit(packageName).alias("package_name"),
        F.lit(reasonCode).alias("reject_reason_code"),
        F.lit(reasonText).alias("reject_reason_text"),
        F.col(keyCol).cast("string").alias("business_key"),
        F.to_json(F.struct(*[F.col(c) for c in df.columns if not c.startswith("_")])).alias("source_record"),
        *loadMetadata(ctx, SOURCE_SYSTEM_OLTP),
    )


def standardizePostalCode(postal: F.Column, country: F.Column) -> F.Column:
    """The ref.PostalFormatRule-driven standardisation the Derived Column applied per country.

    US/CA/JP use the primary block only (ZIP5, FSA-LDU without the space, 7 digits); GB/IE/NL keep the outward and
    inward codes separated by one space; everything else is upper-cased and trimmed.
    """
    compact = F.upper(F.regexp_replace(F.coalesce(postal.cast("string"), F.lit("")), r"[\s-]", ""))
    country = F.upper(F.trim(country.cast("string")))
    cleaned = F.upper(F.trim(F.regexp_replace(postal.cast("string"), r"\s{2,}", " ")))
    return (
        F.when(compact == "", F.lit(None))
        .when(country == "US", F.substring(compact, 1, 5))
        .when(country == "CA", F.substring(compact, 1, 6))
        .when(country == "JP", F.substring(compact, 1, 7))
        .when(
            country.isin("GB", "IE", "NL"),
            F.when(
                F.length(compact) > 3,
                F.regexp_replace(compact, r"^(.+)(.{3})$", "$1 $2"),
            ).otherwise(compact),
        )
        .otherwise(cleaned)
    ).alias("ship_to_postal_code_standardized")


# ---------------------------------------------------------------------------------------------
# STG_Load_Shipment
# ---------------------------------------------------------------------------------------------


def standardizeShipment(bronze: DataFrame) -> DataFrame:
    """ "Standardize Shipment" derived column + the stg.Shipment column contract (snake_case)."""
    sourceSystem = F.coalesce(F.col("source_system_code"), F.lit(SOURCE_SYSTEM_OLTP))
    shipped = F.col("shipped_when")
    promised = F.col("promised_delivery_when")
    delivered = F.col("delivered_when")
    latencyHours = ((F.unix_timestamp(delivered) - F.unix_timestamp(promised)) / 3600.0).cast("decimal(9,2)")
    shipToCountry = upperCode(F.col("ship_to_country_code"))
    return bronze.select(
        sourceSystemKey(sourceSystem, F.col("shipment_id")).alias("shipment_business_key"),
        sourceSystem.alias("source_system_code"),
        F.col("shipment_id").cast("long").alias("shipment_id"),
        F.coalesce(F.col("shipment_reference"), F.col("shipment_id").cast("string")).alias("shipment_reference"),
        F.when(F.col("invoice_id").isNotNull(), sourceSystemKey(sourceSystem, F.col("invoice_id"))).alias("sale_business_key"),
        F.when(F.col("customer_id").isNotNull(), sourceSystemKey(sourceSystem, F.col("customer_id"))).alias(
            "customer_business_key"
        ),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.col("invoice_id").cast("long").alias("invoice_id"),
        F.coalesce(upperCode(F.col("carrier_code")), F.lit(UNKNOWN_CARRIER)).alias("carrier_code"),
        F.coalesce(upperCode(F.col("service_level_code")), F.lit(DEFAULT_SERVICE_LEVEL)).alias("service_level_code"),
        upperCode(F.col("delivery_route_code")).alias("delivery_route_code"),
        upperCode(F.col("ship_from_warehouse_code")).alias("ship_from_warehouse_code"),
        shipToCountry.alias("ship_to_country_code"),
        standardizePostalCode(F.col("ship_to_postal_code"), shipToCountry),
        F.to_date(shipped).alias("shipped_date"),
        shipped.alias("shipped_date_time_utc"),
        promised.alias("promised_delivery_utc"),
        delivered.alias("delivered_date_time_utc"),
        F.when(delivered.isNotNull() & promised.isNotNull(), latencyHours).alias("delivery_latency_hours"),
        F.when(delivered.isNull(), F.lit(None).cast("string"))
        .when(promised.isNull(), F.lit(None).cast("string"))
        .when(delivered <= promised, F.lit("Y"))
        .otherwise(F.lit("N"))
        .alias("on_time_delivery_flag"),
        F.col("total_weight_kg").cast("decimal(18,3)").alias("total_weight_kg"),
        F.col("total_volume_m3").cast("decimal(18,3)").alias("total_volume_m3"),
        F.col("freight_charge_amount").cast("decimal(19,4)").alias("freight_charge_amount"),
        upperCode(F.col("freight_currency_code")).alias("freight_currency_code"),
        F.col("customs_declaration_ref").alias("customs_declaration_ref"),
        F.when(F.col("customs_declaration_ref").isNotNull() | (F.col("cross_border_flag") == "Y"), F.lit("Y"))
        .otherwise(F.lit("N"))
        .alias("customs_required_flag"),
        F.coalesce(
            upperCode(F.col("shipment_status_code")),
            F.when(delivered.isNotNull(), F.lit("DELIVERED")).otherwise(F.lit("INTRANSIT")),
        ).alias("shipment_status_code"),
        F.coalesce(upperCode(F.col("region_code")), countryRegion(shipToCountry)).alias("region_code"),
        F.col("transit_hours").cast("int").alias("transit_hours"),
        F.col("cross_border_flag").alias("cross_border_flag"),
        F.col("last_edited_when").alias("source_last_edited_when"),
    )


def applyCarrierLookup(shipments: DataFrame, carriers: DataFrame, strict: bool = False) -> tuple[DataFrame, DataFrame]:
    """ "Lookup Carrier (Full Cache)" with NoMatchBehavior = fail -> ERR Shipment Unknown Carrier.

    Returns ``(matched, unmatched)``. When ``strict`` is False the unmatched rows are *also* kept in ``matched``
    with ``carrier_name`` NULL and ``dq_status_code = 'CARRIER_UNMATCHED'`` (README deviation: the reference the
    SSIS lookup cached, ``ref.Carrier``, does not exist on the baseline host, so the strict behaviour would reject
    100 % of shipments).
    """
    ref = carriers.select("carrier_code", "carrier_name", "carrier_region_code", "on_time_target_percent")
    joined = shipments.join(ref, "carrier_code", "left")
    unmatched = joined.where(F.col("carrier_name").isNull())
    matched = joined.withColumn(
        "dq_status_code", F.when(F.col("carrier_name").isNull(), F.lit("CARRIER_UNMATCHED")).otherwise(F.lit("OK"))
    )
    if strict:
        matched = matched.where(F.col("carrier_name").isNotNull())
    return matched, unmatched


def latestScanPerShipment(scans: DataFrame) -> DataFrame:
    """Latest carrier scan per shipment reference / tracking number (ranked by scan time, then file order)."""
    key = F.coalesce(F.col("shipment_reference"), F.col("tracking_number"))
    ranked = scans.withColumn("_scan_key", key).withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy("_scan_key").orderBy(F.col("scan_timestamp_utc").desc(), F.col("source_row_number").desc())
        ),
    )
    return ranked.where(F.col("_rank") == 1).select(
        F.col("_scan_key").alias("scan_match_key"),
        F.col("scan_event_code").alias("last_scan_event_code"),
        F.col("scan_timestamp_utc").alias("last_scan_utc"),
    )


def attachLatestScan(shipments: DataFrame, scans: DataFrame) -> DataFrame:
    latest = latestScanPerShipment(scans)
    joined = shipments.join(
        latest,
        (F.col("shipment_reference") == F.col("scan_match_key"))
        | (F.col("shipment_id").cast("string") == F.col("scan_match_key")),
        "left",
    ).drop("scan_match_key")
    return joined.withColumn(
        "shipment_status_code",
        F.when(
            F.col("last_scan_event_code").isin("DLV", "POD") & (F.col("shipment_status_code") != "DELIVERED"), F.lit("DELIVERED")
        )
        .when(F.col("last_scan_event_code") == "LST", F.lit("LOST"))
        .otherwise(F.col("shipment_status_code")),
    )


SHIPMENT_HASH_COLUMNS = [
    "shipment_business_key", "carrier_code", "service_level_code", "ship_to_country_code", "shipped_date_time_utc",
    "promised_delivery_utc", "delivered_date_time_utc", "total_weight_kg", "freight_charge_amount",
    "freight_currency_code", "shipment_status_code", "last_scan_event_code",
]  # fmt: skip


def finaliseShipment(df: DataFrame, ctx: RunContext) -> DataFrame:
    return (
        df.withColumn("row_hash", rowHash(*[F.col(c) for c in SHIPMENT_HASH_COLUMNS]))
        .withColumn("batch_id", F.lit(ctx.batchId).cast("long"))
        .withColumn("package_execution_id", F.lit(ctx.packageExecutionId).cast("long"))
        .withColumn("loaded_at_utc", F.lit(ctx.startedAtUtc).cast("timestamp"))
    )


def standardizeShipmentLine(lines: DataFrame, shipments: DataFrame) -> DataFrame:
    """ "Rebase Line Weight" + serial-number count + cold-chain rule -> the stg.ShipmentLine contract.

    ``shipments`` is the standardised header set for the same watermark window (service level drives the
    cold-chain rule; the header business key is carried onto the line).
    """
    sourceSystem = F.coalesce(F.col("l.source_system_code"), F.lit(SOURCE_SYSTEM_OLTP))
    header = shipments.select(
        F.col("shipment_id").alias("h_shipment_id"),
        F.col("shipment_business_key").alias("h_shipment_business_key"),
        F.col("service_level_code").alias("h_service_level_code"),
        F.col("region_code").alias("h_region_code"),
    )
    joined = lines.alias("l").join(header, F.col("l.shipment_id") == F.col("h_shipment_id"), "inner")
    serials = F.when(
        F.length(F.trim(F.coalesce(F.col("l.serial_numbers"), F.lit("")))) > 0,
        F.size(F.split(F.trim(F.col("l.serial_numbers")), r"\s*[|,;]\s*")),
    ).otherwise(F.lit(0))
    temp = F.col("l.temperature_at_load_c").cast("decimal(9,2)")
    coldChain = F.upper(F.col("h_service_level_code")).isin(*COLD_CHAIN_SERVICE_LEVELS)
    breach = coldChain & temp.isNotNull() & ((temp > F.lit(COLD_CHAIN_MAX_C)) | (temp < F.lit(COLD_CHAIN_MIN_C)))
    return joined.select(
        sourceSystemKey(sourceSystem, F.col("l.shipment_line_id")).alias("shipment_line_business_key"),
        F.col("h_shipment_business_key").alias("shipment_business_key"),
        sourceSystem.alias("source_system_code"),
        F.col("l.shipment_line_id").cast("long").alias("shipment_line_id"),
        F.col("l.shipment_id").cast("long").alias("shipment_id"),
        F.when(F.col("l.invoice_line_id").isNotNull(), sourceSystemKey(sourceSystem, F.col("l.invoice_line_id"))).alias(
            "sale_line_business_key"
        ),
        F.when(F.col("l.stock_item_id").isNotNull(), sourceSystemKey(sourceSystem, F.col("l.stock_item_id"))).alias(
            "stock_item_business_key"
        ),
        F.col("l.stock_item_id").cast("int").alias("stock_item_id"),
        upperCode(F.col("l.package_type_code")).alias("package_type_code"),
        F.col("l.shipped_quantity").cast("decimal(18,3)").alias("shipped_quantity"),
        F.col("l.weight_kg").cast("decimal(18,3)").alias("weight_kg"),
        (F.col("l.weight_kg").cast("decimal(18,3)") * 1000).cast("decimal(18,0)").alias("line_weight_grams"),
        serials.cast("int").alias("serial_number_count"),
        temp.alias("temperature_at_load_c"),
        F.when(breach, F.lit("Y")).otherwise(F.lit("N")).alias("cold_chain_breach_flag"),
        F.coalesce(upperCode(F.col("l.line_status_code")), F.lit("SHIPPED")).alias("line_status_code"),
        F.col("h_region_code").alias("region_code"),
        F.when(
            (F.col("l.shipped_quantity").cast("decimal(18,3)") <= 0) | F.col("l.shipped_quantity").isNull(),
            F.lit("QTY_NOT_POSITIVE"),
        )
        .when(F.col("l.weight_kg").cast("decimal(18,3)") < 0, F.lit("WEIGHT_NEGATIVE"))
        .otherwise(F.lit("OK"))
        .alias("dq_status_code"),
    )


LINE_HASH_COLUMNS = [
    "shipment_line_business_key", "shipment_business_key", "stock_item_business_key", "package_type_code",
    "shipped_quantity", "weight_kg", "serial_number_count", "temperature_at_load_c", "line_status_code",
]  # fmt: skip


def appendIncremental(ctx: RunContext, df: DataFrame, targetTable: str, keyColumn: str) -> int:
    """stg.usp_AppendIncremental_*: append rows whose (business key, row_hash) is not already staged."""
    target = ctx.table(targetTable)
    if tableExists(ctx.spark, target):
        existing = ctx.spark.table(target).select(keyColumn, "row_hash").dropDuplicates()
        df = df.join(existing, [keyColumn, "row_hash"], "left_anti")
    count = df.count()
    if count:
        appendTable(df, target)
    else:
        ensureTable(ctx.spark, target, df.schema)
    return count


def writeRejects(ctx: RunContext, rejects: DataFrame, targetTable: str) -> int:
    count = rejects.count()
    if count:
        appendTable(rejects, ctx.table(targetTable))
    else:
        ensureTable(ctx.spark, ctx.table(targetTable), rejects.schema)
    return count


def windowBounds(ctx: RunContext, objectName: str, source: DataFrame, dateColumn: str) -> tuple[str | None, str | None]:
    wmFrom = getWatermark(ctx, SOURCE_SYSTEM_OLTP, objectName, WATERMARK_TYPE)
    maxValue = source.agg(F.max(F.col(dateColumn)).cast("string")).collect()[0][0]
    return wmFrom, maxValue


def inWindow(df: DataFrame, dateColumn: str, wmFrom: str | None, wmTo: str | None) -> DataFrame:
    if wmTo is None:
        return df.where(F.lit(False))
    filtered = df.where(F.col(dateColumn) <= F.lit(wmTo).cast("timestamp"))
    if wmFrom is not None:
        filtered = filtered.where(F.col(dateColumn) > F.lit(wmFrom).cast("timestamp"))
    return filtered


def runStgLoadShipment(ctx: RunContext, strictCarrierLookup: bool = False) -> StagingResult:
    spark = ctx.spark
    packageName = "STG_Load_Shipment"
    bronzeShipments = spark.table(ctx.table(Tables.bronzeSqlShipment))
    bronzeLines = spark.table(ctx.table(Tables.bronzeSqlShipmentLine))
    scans = readTableOrEmpty(spark, ctx.table(Tables.bronzeFileCarrierScan), latestScanInputSchema())

    wmFrom, wmTo = windowBounds(ctx, "stg.Shipment", bronzeShipments, "shipped_when")
    windowed = inWindow(bronzeShipments, "shipped_when", wmFrom, wmTo).where(F.col("shipped_when").isNotNull())
    rowsRead = windowed.count()

    standardized = standardizeShipment(windowed)
    matched, unmatched = applyCarrierLookup(standardized, carrierReference(ctx), strict=strictCarrierLookup)
    withScans = attachLatestScan(matched, scans)
    withUsd = convertToUsd(
        withScans,
        fxRatesToUsd(ctx),
        "freight_charge_amount",
        "freight_currency_code",
        "shipped_date",
        "freight_charge_amount_usd",
    )
    withUsd = withUsd.withColumn(
        "dq_status_code",
        F.when(
            (F.col("dq_status_code") == "OK")
            & F.col("freight_charge_amount_usd").isNull()
            & F.col("freight_charge_amount").isNotNull(),
            F.lit("FX_RATE_MISSING"),
        ).otherwise(F.col("dq_status_code")),
    )
    shipments = finaliseShipment(withUsd.drop("carrier_region_code", "on_time_target_percent"), ctx)
    loaded = appendIncremental(ctx, shipments, Tables.silverShipment, "shipment_business_key")

    rejects = rejectRows(
        unmatched, ctx, packageName, "CARRIER_UNMATCHED", "Carrier code not in carrier reference", "shipment_business_key"
    )
    rejected = writeRejects(ctx, rejects, Tables.errRejectedLookupFailure)

    lines = standardizeShipmentLine(bronzeLines, standardized)
    goodLines = lines.where(F.col("dq_status_code") == "OK").withColumn(
        "row_hash", rowHash(*[F.col(c) for c in LINE_HASH_COLUMNS])
    )
    goodLines = (
        goodLines.withColumn("batch_id", F.lit(ctx.batchId).cast("long"))
        .withColumn("package_execution_id", F.lit(ctx.packageExecutionId).cast("long"))
        .withColumn("loaded_at_utc", F.lit(ctx.startedAtUtc).cast("timestamp"))
    )
    linesLoaded = appendIncremental(ctx, goodLines, Tables.silverShipmentLine, "shipment_line_business_key")
    badLines = lines.where(F.col("dq_status_code") != "OK")
    lineRejects = rejectRows(
        badLines,
        ctx,
        packageName,
        "LINE_SOURCE_ERROR",
        "Shipment line failed quantity/weight constraint",
        "shipment_line_business_key",
    )
    lineRejected = writeRejects(ctx, lineRejects, Tables.errRejectedConstraintViolation)

    if wmTo is not None:
        setWatermark(ctx, SOURCE_SYSTEM_OLTP, "stg.Shipment", WATERMARK_TYPE, wmTo)
    logRowCount(
        ctx,
        packageName,
        Tables.silverShipment,
        {
            "shipment_rows_read": rowsRead,
            "shipment_rows_loaded": loaded,
            "shipment_rows_unknown_carrier": rejected,
            "shipment_line_rows_loaded": linesLoaded,
            "shipment_line_rows_rejected": lineRejected,
        },
    )
    return StagingResult(rowsRead, loaded + linesLoaded, rejected + lineRejected, wmFrom, wmTo)


def latestScanInputSchema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("shipment_reference", T.StringType()),
            T.StructField("tracking_number", T.StringType()),
            T.StructField("scan_event_code", T.StringType()),
            T.StructField("scan_timestamp_utc", T.TimestampType()),
            T.StructField("source_row_number", T.LongType()),
        ]
    )


# ---------------------------------------------------------------------------------------------
# STG_Load_ReturnAndCredit
# ---------------------------------------------------------------------------------------------


def standardizeReturn(bronze: DataFrame) -> DataFrame:
    """ "Standardize Return" derived column: region default NA, reason default UNSTATED, regional return window."""
    sourceSystem = F.coalesce(F.col("source_system_code"), F.lit(SOURCE_SYSTEM_OLTP))
    region = F.coalesce(upperCode(F.col("region_code")), currencyRegion(F.col("currency_code")), F.lit(REGION_NA))
    return bronze.select(
        sourceSystemKey(sourceSystem, F.col("return_line_id")).alias("return_line_business_key"),
        sourceSystem.alias("source_system_code"),
        F.col("return_line_id").cast("long").alias("return_line_id"),
        F.coalesce(F.col("rma_number"), F.col("return_authorization_id").cast("string")).alias("rma_number"),
        F.when(F.col("invoice_line_id").isNotNull(), sourceSystemKey(sourceSystem, F.col("invoice_line_id"))).alias(
            "sale_line_business_key"
        ),
        F.col("original_invoice_id").cast("long").alias("original_invoice_id"),
        F.when(F.col("customer_id").isNotNull(), sourceSystemKey(sourceSystem, F.col("customer_id"))).alias(
            "customer_business_key"
        ),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.when(F.col("stock_item_id").isNotNull(), sourceSystemKey(sourceSystem, F.col("stock_item_id"))).alias(
            "stock_item_business_key"
        ),
        F.col("stock_item_id").cast("int").alias("stock_item_id"),
        F.coalesce(upperCode(F.col("return_reason_code")), F.lit(UNSTATED_REASON)).alias("source_return_reason_code"),
        F.col("returned_quantity").cast("decimal(18,3)").alias("returned_quantity"),
        F.col("restocked_quantity").cast("decimal(18,3)").alias("restocked_quantity"),
        F.col("scrapped_quantity").cast("decimal(18,3)").alias("scrapped_quantity"),
        F.coalesce(upperCode(F.col("inspection_result_code")), F.lit("PENDING")).alias("inspection_result_code"),
        F.coalesce(F.col("restocking_fee_amount").cast("decimal(19,4)"), F.lit(0).cast("decimal(19,4)")).alias(
            "restocking_fee_amount"
        ),
        F.col("refund_amount").cast("decimal(19,4)").alias("refund_amount"),
        upperCode(F.col("currency_code")).alias("transaction_currency_code"),
        F.to_date(F.col("returned_when")).alias("returned_date"),
        F.col("returned_when").alias("returned_when"),
        F.to_date(F.col("processed_when")).alias("processed_date"),
        F.lit(None).cast("int").alias("days_since_sale"),
        region.alias("region_code"),
        regionStatutoryWindowDays(region).alias("return_window_days"),
    )


def crosswalkReturnReason(returns: DataFrame, crosswalk: DataFrame) -> tuple[DataFrame, DataFrame]:
    """ "Lookup Return Reason" (ref.CodeCrosswalk, no-match = ERR Return Unknown Reason)."""
    ref = crosswalk.select(
        F.col("source_code_value").alias("source_return_reason_code"),
        F.col("conformed_code_value").alias("return_reason_code"),
        F.col("return_reason_group_code"),
    )
    joined = returns.join(ref, "source_return_reason_code", "left")
    matched = joined.where(F.col("return_reason_code").isNotNull())
    unmatched = joined.where(F.col("return_reason_code").isNull())
    return matched, unmatched


def screenReturns(returns: DataFrame) -> tuple[DataFrame, DataFrame]:
    """ "Screen Returns" conditional split: ReturnedQuantity > 0 -> stg.Return, else constraint violation."""
    positive = F.col("returned_quantity").isNotNull() & (F.col("returned_quantity") > 0)
    return returns.where(positive), returns.where(~positive | F.col("returned_quantity").isNull())


def withinStatutoryWindow(returns: DataFrame) -> DataFrame:
    """Flag is 'Y'/'N' when the days since sale are known; NULL (unknown) otherwise."""
    days = F.col("days_since_sale")
    return returns.withColumn(
        "within_statutory_window_flag",
        F.when(days.isNull(), F.lit(None).cast("string"))
        .when(days <= F.col("return_window_days"), F.lit("Y"))
        .otherwise(F.lit("N")),
    )


RETURN_HASH_COLUMNS = [
    "return_line_business_key", "customer_business_key", "stock_item_business_key", "return_reason_code",
    "returned_quantity", "inspection_result_code", "refund_amount", "transaction_currency_code", "returned_date",
]  # fmt: skip


def standardizeCreditNote(bronze: DataFrame, autoApproveBand: bool = True) -> DataFrame:
    """ "Standardize Credit" derived column: approval band, approved flag, region, VAT credit-note flag.

    ``autoApproveBand``: the SSIS expression only set ApprovedFlag from ApprovedBy; the OLTP extract never carries
    an approver so the legacy package rejects 100 % of credits. With the flag on, the AUTO band (< 500) is treated
    as approved without a named approver (README deviation).
    """
    sourceSystem = F.coalesce(F.col("source_system_code"), F.lit(SOURCE_SYSTEM_OLTP))
    net = F.coalesce(F.col("net_amount").cast("decimal(19,4)"), F.lit(0).cast("decimal(19,4)"))
    tax = F.coalesce(F.col("tax_amount").cast("decimal(19,4)"), F.lit(0).cast("decimal(19,4)"))
    gross = F.coalesce(F.col("gross_amount").cast("decimal(19,4)"), (net + tax).cast("decimal(19,4)"))
    creditAmount = F.coalesce(gross, F.lit(0).cast("decimal(19,4)"))
    band = (
        F.when(creditAmount < APPROVAL_AUTO_LIMIT, F.lit("AUTO"))
        .when(creditAmount < APPROVAL_MANAGER_LIMIT, F.lit("MGR"))
        .otherwise(F.lit("FIN"))
    )
    hasApprover = F.length(F.trim(F.coalesce(F.col("approved_by").cast("string"), F.lit("")))) > 0
    approved = hasApprover | (F.lit(autoApproveBand) & (band == "AUTO"))
    region = F.coalesce(upperCode(F.col("region_code")), currencyRegion(F.col("currency_code")), F.lit(REGION_NA))
    return bronze.select(
        sourceSystemKey(sourceSystem, F.col("credit_note_id")).alias("credit_note_business_key"),
        sourceSystem.alias("source_system_code"),
        F.col("credit_note_id").cast("long").alias("credit_note_id"),
        F.coalesce(F.col("credit_note_number"), F.concat(F.lit("CN-"), F.col("credit_note_id").cast("string"))).alias(
            "credit_note_number"
        ),
        F.when(F.col("customer_id").isNotNull(), sourceSystemKey(sourceSystem, F.col("customer_id"))).alias(
            "customer_business_key"
        ),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.when(F.col("original_invoice_id").isNotNull(), sourceSystemKey(sourceSystem, F.col("original_invoice_id"))).alias(
            "original_sale_business_key"
        ),
        F.col("original_invoice_id").cast("long").alias("original_invoice_id"),
        F.coalesce(
            F.col("rma_number") if "rma_number" in bronze.columns else F.lit(None).cast("string"),
            F.col("return_authorization_id").cast("string"),
        ).alias("rma_number"),
        F.coalesce(upperCode(F.col("credit_reason_code")), F.lit(UNSTATED_REASON)).alias("credit_reason_code"),
        F.col("credit_note_date").cast("date").alias("credit_note_date"),
        net.alias("net_amount"),
        tax.alias("tax_amount"),
        gross.alias("gross_amount"),
        creditAmount.alias("credit_amount"),
        upperCode(F.col("currency_code")).alias("transaction_currency_code"),
        F.when(F.col("applied_to_invoice_id").isNotNull(), sourceSystemKey(sourceSystem, F.col("applied_to_invoice_id"))).alias(
            "applied_to_sale_business_key"
        ),
        F.when(hasApprover, F.trim(F.col("approved_by"))).alias("approved_by_name"),
        band.alias("approval_band"),
        F.when(approved, F.lit("Y")).otherwise(F.lit("N")).alias("approved_flag"),
        F.coalesce(upperCode(F.col("credit_status_code")), F.when(approved, F.lit("APPROVED")).otherwise(F.lit("PENDING"))).alias(
            "credit_status_code"
        ),
        F.when(region == REGION_EU, F.lit("Y")).otherwise(F.lit("N")).alias("vat_credit_note_required_flag"),
        region.alias("region_code"),
    )


def screenCreditNotes(credits: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """ "Screen Credits": approved & amount > 0 -> stg.CreditNote; unapproved -> reject; zero/negative -> reject."""
    approved = F.col("approved_flag") == "Y"
    positive = F.col("credit_amount") > 0
    return credits.where(approved & positive), credits.where(~approved), credits.where(approved & ~positive)


CREDIT_HASH_COLUMNS = [
    "credit_note_business_key", "customer_business_key", "original_sale_business_key", "credit_reason_code",
    "credit_note_date", "net_amount", "tax_amount", "gross_amount", "transaction_currency_code", "credit_status_code",
]  # fmt: skip


def _stamp(ctx: RunContext, df: DataFrame, hashColumns: list[str]) -> DataFrame:
    return (
        df.withColumn("dq_status_code", F.lit("OK"))
        .withColumn("row_hash", rowHash(*[F.col(c) for c in hashColumns]))
        .withColumn("batch_id", F.lit(ctx.batchId).cast("long"))
        .withColumn("package_execution_id", F.lit(ctx.packageExecutionId).cast("long"))
        .withColumn("loaded_at_utc", F.lit(ctx.startedAtUtc).cast("timestamp"))
    )


def runStgLoadReturnAndCredit(ctx: RunContext, autoApproveBand: bool = True) -> StagingResult:
    spark = ctx.spark
    packageName = "STG_Load_ReturnAndCredit"
    bronzeReturns = spark.table(ctx.table(Tables.bronzeSqlReturnLine))
    bronzeCredits = spark.table(ctx.table(Tables.bronzeSqlCreditNote))
    fx = fxRatesToUsd(ctx)

    # Returns ---------------------------------------------------------------------------------
    wmFrom, wmTo = windowBounds(ctx, "stg.Return", bronzeReturns, "returned_when")
    windowed = inWindow(bronzeReturns, "returned_when", wmFrom, wmTo).where(F.col("returned_when").isNotNull())
    returnsRead = windowed.count()
    standardized = standardizeReturn(windowed)
    matched, unknownReason = crosswalkReturnReason(standardized, returnReasonCrosswalk(ctx))
    goodReturns, badQuantity = screenReturns(matched)
    goodReturns = withinStatutoryWindow(goodReturns)
    goodReturns = convertToUsd(
        goodReturns, fx, "refund_amount", "transaction_currency_code", "returned_date", "refund_amount_usd"
    )
    returnsLoaded = appendIncremental(
        ctx, _stamp(ctx, goodReturns, RETURN_HASH_COLUMNS), Tables.silverReturn, "return_line_business_key"
    )
    returnsRejected = writeRejects(
        ctx,
        rejectRows(
            unknownReason,
            ctx,
            packageName,
            "RETURN_REASON_UNMAPPED",
            "Return reason not in RETURN_REASON crosswalk",
            "return_line_business_key",
        ),
        Tables.errRejectedLookupFailure,
    )
    returnsRejected += writeRejects(
        ctx,
        rejectRows(
            badQuantity, ctx, packageName, "RETURN_QTY_NOT_POSITIVE", "ReturnedQuantity must be > 0", "return_line_business_key"
        ),
        Tables.errRejectedConstraintViolation,
    )
    if wmTo is not None:
        setWatermark(ctx, SOURCE_SYSTEM_OLTP, "stg.Return", WATERMARK_TYPE, wmTo)

    # Credit notes -----------------------------------------------------------------------------
    cwmFrom, cwmTo = windowBounds(ctx, "stg.CreditNote", bronzeCredits, "credit_note_date")
    cwindowed = inWindow(
        bronzeCredits.withColumn("_cn_ts", F.col("credit_note_date").cast("timestamp")), "_cn_ts", cwmFrom, cwmTo
    )
    creditsRead = cwindowed.count()
    credits = standardizeCreditNote(cwindowed.drop("_cn_ts"), autoApproveBand=autoApproveBand)
    goodCredits, unapproved, nonPositive = screenCreditNotes(credits)
    goodCredits = convertToUsd(goodCredits, fx, "net_amount", "transaction_currency_code", "credit_note_date", "net_amount_usd")
    creditsLoaded = appendIncremental(
        ctx, _stamp(ctx, goodCredits, CREDIT_HASH_COLUMNS), Tables.silverCreditNote, "credit_note_business_key"
    )
    creditsRejected = writeRejects(
        ctx,
        rejectRows(
            unapproved,
            ctx,
            packageName,
            "CREDIT_UNAPPROVED",
            "Credit note has no approver (band requires approval)",
            "credit_note_business_key",
        ),
        Tables.errRejectedConstraintViolation,
    )
    creditsRejected += writeRejects(
        ctx,
        rejectRows(
            nonPositive, ctx, packageName, "CREDIT_AMOUNT_NOT_POSITIVE", "Credit amount must be > 0", "credit_note_business_key"
        ),
        Tables.errRejectedConstraintViolation,
    )
    if cwmTo is not None:
        setWatermark(ctx, SOURCE_SYSTEM_OLTP, "stg.CreditNote", WATERMARK_TYPE, cwmTo)

    logRowCount(
        ctx,
        packageName,
        Tables.silverReturn,
        {
            "return_rows_read": returnsRead,
            "return_rows_loaded": returnsLoaded,
            "return_rows_rejected": returnsRejected,
            "credit_rows_read": creditsRead,
            "credit_rows_loaded": creditsLoaded,
            "credit_rows_rejected": creditsRejected,
        },
    )
    return StagingResult(
        returnsRead + creditsRead, returnsLoaded + creditsLoaded, returnsRejected + creditsRejected, wmFrom, wmTo
    )


__all__ = [
    "REGION_APAC",
    "REGION_EU",
    "StagingResult",
    "applyCarrierLookup",
    "attachLatestScan",
    "crosswalkReturnReason",
    "runStgLoadReturnAndCredit",
    "runStgLoadShipment",
    "screenCreditNotes",
    "screenReturns",
    "standardizeCreditNote",
    "standardizeReturn",
    "standardizeShipment",
    "standardizeShipmentLine",
]
