"""EXT_SQL_Shipments / EXT_SQL_ShipmentLines / EXT_SQL_Returns / EXT_SQL_CreditNotes.

Each SSIS package is: Get Watermark -> Read Source Max <key> -> OLE DB source (key > ? AND key <= ?) ->
Derived Column -> Row Count -> OLE DB destination raw.* -> Set Watermark -> Log Row Counts.
The four packages share that control flow, so they are data-driven here (one ``ExtractSpec`` each).

Baseline seeding: the legacy ``raw.*`` tables on the host already hold the rows the SSIS estate extracted
(batch 2025010601) while ``wwi_legacy_oltp.Shipping/Returns`` are empty. On the first run the bronze table is
seeded from ``wwi_legacy_staging.raw.<table>`` (the SSIS output, used as the reconciliation baseline) and the
watermark is set to the seeded max key; every later run extracts only keys above the watermark from OLTP.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from logistics_returns.common import (
    appendTable,
    getWatermark,
    loadMetadata,
    logRowCount,
    overwriteTable,
    setWatermark,
    snakeCaseColumns,
    tableExists,
)
from logistics_returns.config import SOURCE_SYSTEM_OLTP, RunContext, Tables

WATERMARK_TYPE = "NumericKey"

# ISO-3166 alpha-3 -> alpha-2 for the countries WWI trades with (Application.Countries only carries alpha-3).
ISO3_TO_ISO2 = {
    "USA": "US", "CAN": "CA", "MEX": "MX", "GBR": "GB", "IRL": "IE", "DEU": "DE", "FRA": "FR", "NLD": "NL",
    "BEL": "BE", "ESP": "ES", "ITA": "IT", "PRT": "PT", "AUT": "AT", "POL": "PL", "SWE": "SE", "DNK": "DK",
    "FIN": "FI", "NOR": "NO", "CHE": "CH", "CZE": "CZ", "AUS": "AU", "NZL": "NZ", "SGP": "SG", "JPN": "JP",
    "KOR": "KR", "HKG": "HK", "MYS": "MY", "THA": "TH", "IND": "IN", "CHN": "CN", "IDN": "ID", "PHL": "PH",
    "VNM": "VN", "TWN": "TW", "ARE": "AE", "ZAF": "ZA", "BRA": "BR", "ARG": "AR", "CHL": "CL",
}  # fmt: skip


def iso3ToIso2Case(columnSql: str) -> str:
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in ISO3_TO_ISO2.items())
    return f"CASE UPPER({columnSql}) {whens} ELSE NULL END"


@dataclass
class ExtractSpec:
    packageName: str
    targetTable: str
    legacyRawTable: str
    watermarkObject: str
    keyColumn: str
    sourceMaxSql: Callable[[RunContext], str]
    sourceQuerySql: Callable[[RunContext, int, int], str]
    derive: Callable[[DataFrame], DataFrame]
    columnTypes: dict[str, str] = field(default_factory=dict)
    businessColumns: list[str] = field(default_factory=list)
    seedEnrich: Callable[[RunContext, DataFrame], DataFrame] | None = None


def ensureColumns(df: DataFrame, columnTypes: dict[str, str]) -> DataFrame:
    """Cast the known raw columns to their declared type; add any that the source did not supply as NULL.

    The legacy ``raw.*`` tables are all NVARCHAR while the live OLTP query is typed, so both paths are
    normalised through this before the Derived Column step.
    """
    for name, dataType in columnTypes.items():
        if name in df.columns:
            df = df.withColumn(name, F.col(name).cast(dataType))
        else:
            df = df.withColumn(name, F.lit(None).cast(dataType))
    return df


SHIPMENT_TYPES = {
    "shipment_id": "bigint", "shipment_reference": "string", "invoice_id": "bigint", "customer_id": "int",
    "carrier_code": "string", "service_level_code": "string", "delivery_route_code": "string",
    "shipped_when": "timestamp", "promised_delivery_when": "timestamp", "delivered_when": "timestamp",
    "ship_from_warehouse_code": "string", "ship_to_country_code": "string", "ship_to_postal_code": "string",
    "total_weight_kg": "decimal(18,3)", "total_volume_m3": "decimal(18,3)", "freight_charge_amount": "decimal(18,2)",
    "freight_currency_code": "string", "customs_declaration_ref": "string", "shipment_status_code": "string",
    "last_edited_when": "timestamp", "region_code": "string", "tracking_number": "string",
    "origin_country_code": "string", "destination_country_code": "string",
}  # fmt: skip
SHIPMENT_LINE_TYPES = {
    "shipment_line_id": "bigint", "shipment_id": "bigint", "invoice_line_id": "bigint", "stock_item_id": "int",
    "package_type_code": "string", "shipped_quantity": "decimal(18,3)", "weight_kg": "decimal(18,3)",
    "serial_numbers": "string", "temperature_at_load_c": "decimal(9,2)", "line_status_code": "string",
    "last_edited_when": "timestamp", "tracking_number": "string", "last_scan_status_code": "string",
    "last_scan_when": "timestamp",
}  # fmt: skip
RETURN_TYPES = {
    "return_line_id": "bigint", "return_authorization_id": "bigint", "rma_number": "string",
    "invoice_line_id": "bigint", "customer_id": "int", "stock_item_id": "int", "return_reason_code": "string",
    "return_reason_description": "string", "returned_quantity": "decimal(18,3)", "restocked_quantity": "decimal(18,3)",
    "scrapped_quantity": "decimal(18,3)", "inspection_result_code": "string", "disposition_code": "string",
    "restocking_fee_amount": "decimal(19,4)", "refund_amount": "decimal(19,4)", "currency_code": "string",
    "returned_when": "timestamp", "processed_when": "timestamp", "last_edited_when": "timestamp",
    "region_code": "string", "original_invoice_id": "bigint",
}  # fmt: skip
CREDIT_NOTE_TYPES = {
    "credit_note_id": "bigint", "credit_note_number": "string", "customer_id": "int", "original_invoice_id": "bigint",
    "return_authorization_id": "bigint", "credit_reason_code": "string", "credit_note_date": "date",
    "net_amount": "decimal(19,4)", "tax_amount": "decimal(19,4)", "gross_amount": "decimal(19,4)",
    "currency_code": "string", "applied_to_invoice_id": "bigint", "approved_by": "string",
    "credit_status_code": "string", "last_edited_when": "timestamp", "region_code": "string",
    "tax_treatment_code": "string", "tax_reversal_basis_code": "string",
}  # fmt: skip


LEGACY_META_TYPES = {
    "batch_id": "bigint", "package_execution_id": "bigint", "loaded_at_utc": "timestamp",
    "source_system_code": "string", "source_row_number": "bigint",
}  # fmt: skip


# ---------------------------------------------------------------------------------------------
# Derived Column transformations (the SSIS expressions, one per package)
# ---------------------------------------------------------------------------------------------


def deriveShipment(df: DataFrame) -> DataFrame:
    """Derive Delivery Performance: transit hours (-1 when undelivered) and cross-border flag.

    The SSIS expression compared the customs declaration's origin/destination ISO3 codes; when there is no
    declaration (the legacy raw baseline carries none) the ship-from site country vs ship-to country is used.
    """
    origin = F.coalesce(F.col("origin_country_code"), warehouseCountry(F.col("ship_from_warehouse_code")))
    destination = F.coalesce(F.col("destination_country_code"), F.col("ship_to_country_code"))
    return df.withColumn(
        "transit_hours",
        F.when(F.col("delivered_when").isNull(), F.lit(-1)).otherwise(
            ((F.unix_timestamp("delivered_when") - F.unix_timestamp("shipped_when")) / 3600).cast("int")
        ),
    ).withColumn(
        "cross_border_flag",
        F.when(origin.isNull() | destination.isNull(), F.lit("N")).when(origin == destination, F.lit("N")).otherwise(F.lit("Y")),
    )


# Ship-from site code prefix -> ISO2 country (Warehouse.Sites is not federated; the codes are stable).
WAREHOUSE_COUNTRY = {
    "TOR": "CA", "NJ": "US", "CHI": "US", "DAL": "US", "MAN": "GB", "LYO": "FR", "ROT": "NL", "HAM": "DE",
    "MEL": "AU", "SYD": "AU", "SIN": "SG", "OSA": "JP",
}  # fmt: skip


def warehouseCountry(siteCode: Column) -> Column:
    prefix = F.regexp_extract(F.upper(F.trim(siteCode)), r"^([A-Z]+)", 1)
    mapping = F.create_map(*[F.lit(x) for kv in WAREHOUSE_COUNTRY.items() for x in kv])
    return mapping[prefix]


def deriveShipmentLine(df: DataFrame) -> DataFrame:
    """Derive Scan State: has_scan_flag = ISNULL(LastScanWhen) ? "Y" : "N" (SSIS labels are inverted; kept)."""
    return df.withColumn("has_scan_flag", F.when(F.col("last_scan_when").isNull(), F.lit("Y")).otherwise(F.lit("N")))


def enrichReturnSeed(ctx: RunContext, df: DataFrame) -> DataFrame:
    """Legacy raw.SqlReturnLine carries only InvoiceLineID; resolve OriginalInvoiceID through Sales.InvoiceLines."""
    invoiceLines = ctx.spark.table(ctx.legacy(ctx.legacyOltp, "Sales", "InvoiceLines")).select(
        F.col("InvoiceLineID").cast("bigint").alias("invoice_line_id"),
        F.col("InvoiceID").cast("bigint").alias("_resolved_invoice_id"),
    )
    return (
        df.join(invoiceLines, "invoice_line_id", "left")
        .withColumn("original_invoice_id", F.coalesce(F.col("original_invoice_id"), F.col("_resolved_invoice_id")))
        .drop("_resolved_invoice_id")
    )


def deriveReturn(df: DataFrame) -> DataFrame:
    """Derive Return Attributes + the Route Pending Inspections split (both outputs land in raw.SqlReturnLine)."""
    return (
        df.withColumn(
            "days_to_inspection",
            F.when(F.col("processed_when").isNull(), F.lit(-1)).otherwise(
                F.datediff(F.to_date("processed_when"), F.to_date("returned_when"))
            ),
        )
        .withColumn("restock_flag", F.when(F.col("disposition_code") == "RESTOCK", F.lit("Y")).otherwise(F.lit("N")))
        .withColumn(
            "inspection_complete_flag",
            F.coalesce(F.col("inspection_result_code"), F.lit("PENDING")) != F.lit("PENDING"),
        )
    )


def deriveCreditNote(df: DataFrame) -> DataFrame:
    """Derive Credit Attributes: negated credit (gross, or net + tax when the source has no gross) and VAT flag."""
    gross = F.coalesce(F.col("gross_amount"), F.col("net_amount") + F.coalesce(F.col("tax_amount"), F.lit(0)))
    return df.withColumn("credited_including_tax_negated", -gross).withColumn(
        "vat_credit_flag", F.when(F.col("tax_treatment_code") == "VAT", F.lit("Y")).otherwise(F.lit("N"))
    )


# ---------------------------------------------------------------------------------------------
# Source queries against the live OLTP (through Lakehouse Federation)
# ---------------------------------------------------------------------------------------------


def shipmentSourceSql(ctx: RunContext, wmFrom: int, wmTo: int) -> str:
    o = ctx.legacyOltp
    return f"""
SELECT  s.ShipmentID                              AS shipment_id,
        s.ShipmentReference                       AS shipment_reference,
        s.InvoiceID                               AS invoice_id,
        s.CustomerID                              AS customer_id,
        s.CarrierCode                             AS carrier_code,
        s.ServiceLevelCode                        AS service_level_code,
        dr.RouteCode                              AS delivery_route_code,
        s.DespatchedWhen                          AS shipped_when,
        s.PromisedDeliveryWhen                    AS promised_delivery_when,
        s.DeliveredWhen                           AS delivered_when,
        s.SiteCode                                AS ship_from_warehouse_code,
        {iso3ToIso2Case("ctry.IsoAlpha3Code")}   AS ship_to_country_code,
        cust.DeliveryPostalCode                   AS ship_to_postal_code,
        s.TotalGrossWeightKg                      AS total_weight_kg,
        h.TotalVolumeM3                           AS total_volume_m3,
        s.FreightChargeAmount                     AS freight_charge_amount,
        s.FreightCurrencyCode                     AS freight_currency_code,
        cd.DeclarationReference                   AS customs_declaration_ref,
        s.ShipmentStatus                          AS shipment_status_code,
        s.ChangedWhen                             AS last_edited_when,
        s.RegionCode                              AS region_code,
        s.TrackingNumber                          AS tracking_number,
        cd.CountryOfOriginISO3                    AS origin_country_code,
        cd.DestinationCountryISO3                 AS destination_country_code
FROM    `{o}`.`Shipping`.`vw_ShipmentExtract` AS s
        JOIN `{o}`.`Shipping`.`ShipmentHeaders` AS h ON h.ShipmentID = s.ShipmentID
        LEFT JOIN `{o}`.`Shipping`.`DeliveryRoutes` AS dr ON dr.DeliveryRouteID = h.DeliveryRouteID
        LEFT JOIN (
            SELECT ShipmentID, DeclarationReference, CountryOfOriginISO3, DestinationCountryISO3,
                   ROW_NUMBER() OVER (PARTITION BY ShipmentID ORDER BY DeclaredWhen DESC, CustomsDeclarationID DESC) AS rn
            FROM `{o}`.`Shipping`.`CustomsDeclarations`
        ) AS cd ON cd.ShipmentID = s.ShipmentID AND cd.rn = 1
        LEFT JOIN `{o}`.`Sales`.`Customers` AS cust ON cust.CustomerID = s.CustomerID
        LEFT JOIN `{o}`.`Application`.`Cities` AS city ON city.CityID = cust.DeliveryCityID
        LEFT JOIN `{o}`.`Application`.`StateProvinces` AS sp ON sp.StateProvinceID = city.StateProvinceID
        LEFT JOIN `{o}`.`Application`.`Countries` AS ctry ON ctry.CountryID = sp.CountryID
WHERE   s.ShipmentID > {wmFrom} AND s.ShipmentID <= {wmTo}
  AND   s.ShipmentStatus <> 'VOID'
"""


def shipmentLineSourceSql(ctx: RunContext, wmFrom: int, wmTo: int) -> str:
    o = ctx.legacyOltp
    return f"""
SELECT  sl.ShipmentLineID                         AS shipment_line_id,
        sl.ShipmentID                             AS shipment_id,
        sl.OrderLineID                            AS invoice_line_id,
        sl.StockItemID                            AS stock_item_id,
        pt.PackagingCode                          AS package_type_code,
        sl.QuantityShipped                        AS shipped_quantity,
        sl.PackageGrossWeightKg                   AS weight_kg,
        sl.SerialNumberList                       AS serial_numbers,
        CAST(NULL AS DECIMAL(9,2))                AS temperature_at_load_c,
        sl.LineStatus                             AS line_status_code,
        sl.LastEditedWhen                         AS last_edited_when,
        sl.TrackingNumber                         AS tracking_number,
        ev.EventTypeCode                          AS last_scan_status_code,
        ev.EventWhenUtc                           AS last_scan_when
FROM    (SELECT l.*, h.TrackingNumber FROM `{o}`.`Shipping`.`ShipmentLines` AS l
         JOIN `{o}`.`Shipping`.`ShipmentHeaders` AS h ON h.ShipmentID = l.ShipmentID) AS sl
        LEFT JOIN `{o}`.`Shipping`.`PackagingTypes` AS pt ON pt.PackagingTypeID = sl.PackagingTypeID
        LEFT JOIN (
            SELECT ShipmentID, EventTypeCode, EventWhenUtc,
                   ROW_NUMBER() OVER (PARTITION BY ShipmentID ORDER BY EventWhenUtc DESC, EventSequence DESC) AS rn
            FROM `{o}`.`Shipping`.`ShipmentEvents`
        ) AS ev ON ev.ShipmentID = sl.ShipmentID AND ev.rn = 1
WHERE   sl.ShipmentLineID > {wmFrom} AND sl.ShipmentLineID <= {wmTo}
"""


def returnSourceSql(ctx: RunContext, wmFrom: int, wmTo: int) -> str:
    o = ctx.legacyOltp
    return f"""
SELECT  rl.ReturnLineID                           AS return_line_id,
        rl.ReturnAuthorizationID                  AS return_authorization_id,
        rl.RmaNumber                              AS rma_number,
        l.OriginalInvoiceLineID                   AS invoice_line_id,
        rl.CustomerID                             AS customer_id,
        rl.StockItemID                            AS stock_item_id,
        rl.ReasonCode                             AS return_reason_code,
        rr.ReasonDescription                      AS return_reason_description,
        COALESCE(rl.QuantityReceived, rl.QuantityAuthorized) AS returned_quantity,
        rl.QuantityAccepted                       AS restocked_quantity,
        rl.QuantityScrapped                       AS scrapped_quantity,
        COALESCE(ri.ConditionGrade, 'PENDING')    AS inspection_result_code,
        COALESCE(rl.LatestInspectionDisposition, 'UNKNOWN') AS disposition_code,
        CAST(rl.GrossCreditAmount * COALESCE(rl.RestockingPercent, 0) / 100 AS DECIMAL(19,4)) AS restocking_fee_amount,
        rl.GrossCreditAmount                      AS refund_amount,
        rl.CreditCurrencyCode                     AS currency_code,
        rl.GoodsReceivedWhen                      AS returned_when,
        ri.InspectedWhen                          AS processed_when,
        rl.ChangedWhen                            AS last_edited_when,
        rl.RegionCode                             AS region_code,
        rl.OriginalInvoiceID                      AS original_invoice_id
FROM    `{o}`.`Returns`.`vw_ReturnExtract` AS rl
        JOIN `{o}`.`Returns`.`ReturnLines` AS l ON l.ReturnLineID = rl.ReturnLineID
        LEFT JOIN `{o}`.`Returns`.`ReturnReasons` AS rr ON rr.ReturnReasonID = l.ReturnReasonID
        LEFT JOIN (
            SELECT ReturnLineID, ConditionGrade, InspectedWhen,
                   ROW_NUMBER() OVER (PARTITION BY ReturnLineID ORDER BY InspectionSequence DESC) AS rn
            FROM `{o}`.`Returns`.`ReturnInspections`
        ) AS ri ON ri.ReturnLineID = rl.ReturnLineID AND ri.rn = 1
WHERE   rl.ReturnLineID > {wmFrom} AND rl.ReturnLineID <= {wmTo}
"""


def creditNoteSourceSql(ctx: RunContext, wmFrom: int, wmTo: int) -> str:
    o = ctx.legacyOltp
    return f"""
SELECT  cn.CreditNoteID                           AS credit_note_id,
        cn.CreditNoteNumber                       AS credit_note_number,
        cn.CustomerID                             AS customer_id,
        cn.OriginalInvoiceID                      AS original_invoice_id,
        cn.ReturnAuthorizationID                  AS return_authorization_id,
        cn.CreditReasonCode                       AS credit_reason_code,
        cn.IssuedDate                             AS credit_note_date,
        cn.NetAmount                              AS net_amount,
        cn.TaxAmount                              AS tax_amount,
        cn.TotalAmount                            AS gross_amount,
        cn.CurrencyCode                           AS currency_code,
        CASE WHEN COALESCE(cn.LedgerAppliedAmount, 0) > 0 THEN cn.OriginalInvoiceID END AS applied_to_invoice_id,
        CAST(NULL AS STRING)                      AS approved_by,
        cn.CreditNoteStatus                       AS credit_status_code,
        cn.ChangedWhen                            AS last_edited_when,
        cn.RegionCode                             AS region_code,
        CASE TRIM(cn.RegionCode) WHEN 'NA' THEN 'SALESTAX' WHEN 'EU' THEN 'VAT' WHEN 'APAC' THEN 'GST'
             ELSE 'NONE' END                      AS tax_treatment_code,
        CASE TRIM(cn.RegionCode) WHEN 'NA' THEN 'ORIGRATE' WHEN 'EU' THEN 'CREDITREF' WHEN 'APAC' THEN 'ISSUEPRD'
             ELSE 'NONE' END                      AS tax_reversal_basis_code
FROM    `{o}`.`Returns`.`vw_CreditNoteExtract` AS cn
WHERE   cn.CreditNoteID > {wmFrom} AND cn.CreditNoteID <= {wmTo}
"""


def maxKeySql(schema: str, table: str, key: str) -> Callable[[RunContext], str]:
    return lambda ctx: f"SELECT COALESCE(MAX(`{key}`), 0) AS max_key FROM `{ctx.legacyOltp}`.`{schema}`.`{table}`"


SPECS: dict[str, ExtractSpec] = {
    "EXT_SQL_Shipments": ExtractSpec(
        packageName="EXT_SQL_Shipments",
        targetTable=Tables.bronzeSqlShipment,
        legacyRawTable="SqlShipment",
        watermarkObject="Shipping.ShipmentHeaders",
        keyColumn="shipment_id",
        sourceMaxSql=maxKeySql("Shipping", "ShipmentHeaders", "ShipmentID"),
        sourceQuerySql=shipmentSourceSql,
        derive=deriveShipment,
        columnTypes=SHIPMENT_TYPES,
        businessColumns=[
            "shipment_id",
            "customer_id",
            "carrier_code",
            "service_level_code",
            "shipped_when",
            "promised_delivery_when",
            "delivered_when",
            "ship_from_warehouse_code",
            "ship_to_country_code",
            "ship_to_postal_code",
            "total_weight_kg",
            "freight_charge_amount",
            "freight_currency_code",
            "shipment_status_code",
        ],
    ),
    "EXT_SQL_ShipmentLines": ExtractSpec(
        packageName="EXT_SQL_ShipmentLines",
        targetTable=Tables.bronzeSqlShipmentLine,
        legacyRawTable="SqlShipmentLine",
        watermarkObject="Shipping.ShipmentLines",
        keyColumn="shipment_line_id",
        sourceMaxSql=maxKeySql("Shipping", "ShipmentLines", "ShipmentLineID"),
        sourceQuerySql=shipmentLineSourceSql,
        derive=deriveShipmentLine,
        columnTypes=SHIPMENT_LINE_TYPES,
        businessColumns=[
            "shipment_line_id",
            "shipment_id",
            "stock_item_id",
            "package_type_code",
            "shipped_quantity",
            "weight_kg",
            "serial_numbers",
            "line_status_code",
        ],
    ),
    "EXT_SQL_Returns": ExtractSpec(
        packageName="EXT_SQL_Returns",
        targetTable=Tables.bronzeSqlReturnLine,
        legacyRawTable="SqlReturnLine",
        watermarkObject="Returns.ReturnLines",
        keyColumn="return_line_id",
        sourceMaxSql=maxKeySql("Returns", "ReturnLines", "ReturnLineID"),
        sourceQuerySql=returnSourceSql,
        derive=deriveReturn,
        seedEnrich=enrichReturnSeed,
        columnTypes=RETURN_TYPES,
        businessColumns=[
            "return_line_id",
            "customer_id",
            "stock_item_id",
            "return_reason_code",
            "returned_quantity",
            "restocked_quantity",
            "scrapped_quantity",
            "inspection_result_code",
            "refund_amount",
            "currency_code",
            "returned_when",
        ],
    ),
    "EXT_SQL_CreditNotes": ExtractSpec(
        packageName="EXT_SQL_CreditNotes",
        targetTable=Tables.bronzeSqlCreditNote,
        legacyRawTable="SqlCreditNote",
        watermarkObject="Returns.CreditNotes",
        keyColumn="credit_note_id",
        sourceMaxSql=maxKeySql("Returns", "CreditNotes", "CreditNoteID"),
        sourceQuerySql=creditNoteSourceSql,
        derive=deriveCreditNote,
        columnTypes=CREDIT_NOTE_TYPES,
        businessColumns=[
            "credit_note_id",
            "original_invoice_id",
            "credit_reason_code",
            "credit_note_date",
            "net_amount",
            "tax_amount",
            "currency_code",
        ],
    ),
}


# ---------------------------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------------------------


def seedFromLegacyRaw(ctx: RunContext, spec: ExtractSpec) -> int:
    """First-run baseline: copy the SSIS-produced raw.* rows (snake_cased) into bronze and set the watermark."""
    legacy = snakeCaseColumns(ctx.spark.table(ctx.legacy(ctx.legacyStaging, "raw", spec.legacyRawTable)))
    legacy = ensureColumns(ensureColumns(legacy, spec.columnTypes), LEGACY_META_TYPES)
    if spec.seedEnrich is not None:
        legacy = spec.seedEnrich(ctx, legacy)
    legacy = spec.derive(legacy)
    legacy = legacy.withColumn("seeded_from_legacy_raw", F.lit(True))
    overwriteTable(legacy, ctx.table(spec.targetTable))
    maxKey = ctx.spark.table(ctx.table(spec.targetTable)).agg(F.max(spec.keyColumn)).collect()[0][0]
    setWatermark(ctx, SOURCE_SYSTEM_OLTP, spec.watermarkObject, WATERMARK_TYPE, str(int(maxKey or 0)))
    return legacy.count()


def withRowNumber(df: DataFrame, keyColumn: str) -> DataFrame:
    return df.withColumn("source_row_number", F.row_number().over(Window.orderBy(keyColumn)))


def runExtract(ctx: RunContext, packageName: str) -> dict[str, int]:
    spec = SPECS[packageName]
    spark = ctx.spark
    target = ctx.table(spec.targetTable)
    metrics: dict[str, int] = {}

    if ctx.seedFromLegacyRaw and (not tableExists(spark, target) or ctx.reloadFullHistory):
        metrics["rows_seeded_from_legacy_raw"] = seedFromLegacyRaw(ctx, spec)

    wmFrom = int(getWatermark(ctx, SOURCE_SYSTEM_OLTP, spec.watermarkObject, WATERMARK_TYPE) or 0)
    wmTo = int(spark.sql(spec.sourceMaxSql(ctx)).collect()[0][0] or 0)
    metrics["watermark_from"] = wmFrom
    metrics["watermark_to"] = wmTo

    rowsRead = 0
    if wmTo > wmFrom:
        extracted = spark.sql(spec.sourceQuerySql(ctx, wmFrom, wmTo))
        extracted = spec.derive(ensureColumns(extracted, spec.columnTypes))
        extracted = extracted.select("*", *loadMetadata(ctx, SOURCE_SYSTEM_OLTP))
        extracted = withRowNumber(extracted, spec.keyColumn).withColumn("seeded_from_legacy_raw", F.lit(False))
        rowsRead = extracted.count()
        if rowsRead:
            appendTable(extracted, target)
        setWatermark(ctx, SOURCE_SYSTEM_OLTP, spec.watermarkObject, WATERMARK_TYPE, str(wmTo))
    elif not tableExists(spark, target):
        spark.createDataFrame([], T.StructType([T.StructField(spec.keyColumn, T.LongType())])).write.format("delta").saveAsTable(
            target
        )

    metrics["rows_read"] = rowsRead
    metrics["rows_inserted"] = rowsRead
    logRowCount(ctx, spec.packageName, spec.targetTable, metrics)
    return metrics
