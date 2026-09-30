"""Reconciliation evidence for the 12 logistics_returns packages -> otterorders_migration.evidence.recon_results.

One evidence run = one ``run_id`` (UUID) and exactly one row per package.  For packages whose legacy SSIS output is
populated on the baseline host (the four ``raw.*`` extract tables) the Delta table is compared with the legacy table
directly (row count + order-independent SUM(xxhash64) checksum over the business columns) and can reach ``PASS``.
For packages whose legacy target is empty on the host (``stg.*``, ``Fact.Shipment``, ``Fact.Order Fulfilment``,
``Fact.Return``, ``Fact.Credit Note``, ``Aggregate.Delivery Performance Summary``, ``raw.FileCarrierScan``) an expected
result is derived from the *inputs* with the package's own (pure, unit-tested) transformation functions and the
verdict is capped at ``PARTIAL`` with ``"baseline": "source_derived"`` in the checks.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from logistics_returns.aggregate import DEFAULT_WEEKS_TO_REFRESH, buildWeeklyDeliveryPerformance, refreshWindowStart
from logistics_returns.carrier_scan import (
    FILE_PATTERN,
    listInboundFiles,
    parseScanEvent,
    readCarrierFile,
    toBronzeColumns,
    validScanRow,
)
from logistics_returns.common import readLegacy, readTableOrEmpty, rowHash, snakeCaseColumns, tableExists
from logistics_returns.config import (
    ACTOR,
    BRANCH,
    EVIDENCE_SCHEMA,
    HARNESS_VERSION,
    LEGACY_DW_DATABASE,
    LEGACY_STAGING_DATABASE,
    RunContext,
    Tables,
)
from logistics_returns.dimensions import legacyFactSale, returnReasonWindows
from logistics_returns.extracts import SPECS, ensureColumns
from logistics_returns.facts import (
    SCAN_EVENTS_SCHEMA,
    buildCreditNoteFactRows,
    buildReturnFactRows,
    buildShipmentFactRows,
    latestStagedShipments,
    ordersFromLegacyFactOrder,
    ordersFromLegacyStaging,
    splitCreditNotes,
    summarizeScanEvents,
)
from logistics_returns.reference import returnReasonCrosswalk
from logistics_returns.staging import (
    crosswalkReturnReason,
    screenCreditNotes,
    screenReturns,
    standardizeCreditNote,
    standardizeReturn,
    standardizeShipment,
    standardizeShipmentLine,
)

UNIT_TYPE = "ssis_package"
EVIDENCE_TABLE = "recon_results"

EVIDENCE_SCHEMA_STRUCT = T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("run_at", T.TimestampType()),
        T.StructField("unit", T.StringType()),
        T.StructField("unit_type", T.StringType()),
        T.StructField("verdict", T.StringType()),
        T.StructField("branch", T.StringType()),
        T.StructField("source_object", T.StringType()),
        T.StructField("target_object", T.StringType()),
        T.StructField("checks", T.StringType()),
        T.StructField("summary", T.StringType()),
        T.StructField("git_sha", T.StringType()),
        T.StructField("actor", T.StringType()),
        T.StructField("harness_version", T.StringType()),
    ]
)


@dataclass
class PackageEvidence:
    unit: str
    verdict: str
    sourceObject: str
    targetObject: str
    checks: list[dict] = field(default_factory=list)
    summary: str = ""


# ---------------------------------------------------------------------------------------------
# Check helpers
# ---------------------------------------------------------------------------------------------


def checksumOf(df: DataFrame, columns: list[str]) -> str | None:
    """Order-independent SUM(xxhash64(business columns cast to string)); summed as decimal(38,0) so ANSI mode cannot overflow."""
    present = [c for c in columns if c in df.columns]
    if not present:
        return None
    perRow = rowHash(*[F.col(c).cast("string") for c in present]).cast("decimal(38,0)")
    row = df.select(F.sum(perRow).alias("cs")).collect()[0]
    return None if row[0] is None else str(row[0])


def rowCountCheck(source: int | None, target: int, baseline: str | None = None) -> dict:
    check: dict = {"check": "row_count", "source": source, "target": target, "pass": source is not None and source == target}
    if baseline:
        check["baseline"] = baseline
    return check


def checksumCheck(sourceDf: DataFrame, targetDf: DataFrame, columns: list[str], baseline: str | None = None) -> dict:
    source, target = checksumOf(sourceDf, columns), checksumOf(targetDf, columns)
    check: dict = {
        "check": "checksum",
        "method": f"sum(xxhash64({', '.join(columns)}))",
        "source": source,
        "target": target,
        "pass": source == target,
    }
    if baseline:
        check["baseline"] = baseline
    return check


def nullRateCheck(sourceDf: DataFrame | None, targetDf: DataFrame, column: str, tolerance: float = 0.0) -> dict:
    def rate(df: DataFrame | None) -> float | None:
        if df is None or column not in df.columns:
            return None
        row = df.agg(F.count(F.lit(1)).alias("n"), F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias("k")).collect()[0]
        return 0.0 if not row["n"] else round(float(row["k"] or 0) / float(row["n"]), 6)

    s, t = rate(sourceDf), rate(targetDf)
    passed = t is not None and (s is None or abs(s - t) <= tolerance)
    return {"check": "column_null_rate", "column": column, "source": s, "target": t, "pass": passed}


def legacyCountOrNone(ctx: RunContext, catalog: str, schema: str, table: str) -> int | None:
    try:
        return readLegacy(ctx.spark, catalog, schema, table).count()
    except Exception:  # noqa: BLE001 - federation may not expose the object at all; recorded in the summary
        return None


def verdictFor(checks: list[dict], sourceDerived: bool) -> str:
    core = [c for c in checks if c["check"] in ("row_count", "checksum")]
    allPass = bool(core) and all(c["pass"] for c in core)
    if sourceDerived:
        return "PARTIAL" if allPass else "FAIL"
    return "PASS" if allPass else "FAIL"


def legacyName(database: str, schema: str, table: str) -> str:
    return f"{database}.{schema}.{table}"


# ---------------------------------------------------------------------------------------------
# Per-package evidence
# ---------------------------------------------------------------------------------------------


def extractEvidence(ctx: RunContext, packageName: str) -> PackageEvidence:
    spec = SPECS[packageName]
    spark = ctx.spark
    targetName = ctx.table(spec.targetTable)
    target = spark.table(targetName)
    legacy = ensureColumns(snakeCaseColumns(readLegacy(spark, ctx.legacyStaging, "raw", spec.legacyRawTable)), spec.columnTypes)
    baseline = target.where(F.col("seeded_from_legacy_raw")) if "seeded_from_legacy_raw" in target.columns else target
    incremental = target.count() - baseline.count()
    legacyCount = legacy.count()
    checks = [
        rowCountCheck(legacyCount, baseline.count()),
        checksumCheck(legacy, baseline, spec.businessColumns),
        nullRateCheck(legacy, baseline, spec.keyColumn),
        {"check": "incremental_rows_beyond_legacy_watermark", "source": None, "target": incremental, "pass": True},
    ]
    ev = PackageEvidence(
        packageName,
        verdictFor(checks, sourceDerived=False),
        legacyName(LEGACY_STAGING_DATABASE, "raw", spec.legacyRawTable),
        targetName.replace("`", ""),
        checks,
    )
    ev.summary = (
        f"Bronze baseline seeded from legacy raw.{spec.legacyRawTable} ({legacyCount} rows) compared on count + checksum over "
        f"{len(spec.businessColumns)} business columns; {incremental} additional rows extracted from OLTP beyond the legacy "
        f"NumericKey watermark (not in the legacy table by construction)."
    )
    return ev


def carrierScanEvidence(ctx: RunContext) -> PackageEvidence:
    spark = ctx.spark
    targetName = ctx.table(Tables.bronzeFileCarrierScan)
    target = readTableOrEmpty(spark, targetName, T.StructType([T.StructField("tracking_number", T.StringType())]))
    inboundDir = ctx.volumePath("inbound", "carrier")
    files = listInboundFiles(inboundDir)
    expectedFrames = [
        parseScanEvent(readCarrierFile(spark, f"{inboundDir}/{name}")) for name in files if FILE_PATTERN.match(name)
    ]
    columns = ["carrier_code", "tracking_number", "scan_event_code", "scan_timestamp_utc", "depot_code"]
    if expectedFrames:
        parsed = expectedFrames[0]
        for frame in expectedFrames[1:]:
            parsed = parsed.unionByName(frame)
        expected = toBronzeColumns(parsed.where(validScanRow()))
        rejectedRows = parsed.where(~validScanRow()).count()
    else:
        expected = spark.createDataFrame([], target.schema)
        rejectedRows = 0
    legacyCount = legacyCountOrNone(ctx, ctx.legacyStaging, "raw", "FileCarrierScan")
    checks = [
        rowCountCheck(expected.count(), target.count(), baseline="source_derived"),
        checksumCheck(expected, target, columns, baseline="source_derived"),
        nullRateCheck(expected, target, "scan_timestamp_utc"),
        {"check": "legacy_target_row_count", "source": legacyCount, "target": None, "pass": True},
        {"check": "files_in_landing_volume", "source": len(files), "target": len(files), "pass": True},
        {"check": "rows_rejected_by_validation", "source": rejectedRows, "target": rejectedRows, "pass": True},
    ]
    ev = PackageEvidence(
        "ING_FILE_CarrierScan",
        verdictFor(checks, sourceDerived=True),
        legacyName(LEGACY_STAGING_DATABASE, "raw", "FileCarrierScan"),
        targetName.replace("`", ""),
        checks,
    )
    ev.summary = (
        f"Legacy raw.FileCarrierScan holds {legacyCount} rows on the baseline host (feed never landed), so the expected result "
        f"is re-derived from the {len(files)} carrier_scan_*.csv file(s) in the landing volume with the package's own "
        f"parse/validate rules ({rejectedRows} rows rejected). PARTIAL by contract (source_derived baseline)."
    )
    return ev


def stgShipmentEvidence(ctx: RunContext) -> PackageEvidence:
    spark = ctx.spark
    bronze = spark.table(ctx.table(Tables.bronzeSqlShipment)).where(F.col("shipped_when").isNotNull())
    bronzeLines = spark.table(ctx.table(Tables.bronzeSqlShipmentLine))
    silver = spark.table(ctx.table(Tables.silverShipment))
    silverLines = spark.table(ctx.table(Tables.silverShipmentLine))
    standardized = standardizeShipment(bronze)
    expectedLines = standardizeShipmentLine(bronzeLines, standardized).where(F.col("dq_status_code") == "OK")
    shipmentColumns = [
        "shipment_business_key",
        "carrier_code",
        "service_level_code",
        "ship_to_country_code",
        "shipped_date_time_utc",
        "total_weight_kg",
        "freight_charge_amount",
        "freight_currency_code",
        "region_code",
    ]
    lineColumns = [
        "shipment_line_business_key",
        "shipment_business_key",
        "package_type_code",
        "shipped_quantity",
        "weight_kg",
        "line_status_code",
    ]
    expectedCount = bronze.select("shipment_id").distinct().count()
    checks = [
        rowCountCheck(expectedCount, silver.select("shipment_business_key").distinct().count(), baseline="source_derived"),
        checksumCheck(standardized, silver, shipmentColumns, baseline="source_derived"),
        {
            **rowCountCheck(expectedLines.count(), silverLines.count(), baseline="source_derived"),
            "check": "row_count_shipment_line",
        },
        {**checksumCheck(expectedLines, silverLines, lineColumns, baseline="source_derived"), "check": "checksum_shipment_line"},
        nullRateCheck(standardized, silver, "carrier_code"),
        {
            "check": "legacy_target_row_count",
            "source": legacyCountOrNone(ctx, ctx.legacyStaging, "stg", "Shipment"),
            "target": None,
            "pass": True,
        },
        {
            "check": "rows_with_unmatched_carrier",
            "source": None,
            "target": silver.where(F.col("dq_status_code") == "CARRIER_UNMATCHED").count(),
            "pass": True,
        },
    ]
    ev = PackageEvidence(
        "STG_Load_Shipment",
        verdictFor(checks, sourceDerived=True),
        f"{legacyName(LEGACY_STAGING_DATABASE, 'stg', 'Shipment')}, {legacyName(LEGACY_STAGING_DATABASE, 'stg', 'ShipmentLine')}",
        f"{ctx.table(Tables.silverShipment)}, {ctx.table(Tables.silverShipmentLine)}".replace("`", ""),
        checks,
    )
    ev.summary = (
        "Legacy stg.Shipment / stg.ShipmentLine are empty on the baseline host; expected rows re-derived from bronze with the "
        "package's Standardize Shipment / Shipment Line rules (distinct shipment keys, line constraint screen). Carrier lookup "
        "is lenient (ref.Carrier absent on host) so unmatched carriers are flagged, not rejected. PARTIAL by contract."
    )
    return ev


def stgReturnAndCreditEvidence(ctx: RunContext) -> PackageEvidence:
    spark = ctx.spark
    bronzeReturns = spark.table(ctx.table(Tables.bronzeSqlReturnLine)).where(F.col("returned_when").isNotNull())
    bronzeCredits = spark.table(ctx.table(Tables.bronzeSqlCreditNote))
    silverReturns = spark.table(ctx.table(Tables.silverReturn))
    silverCredits = spark.table(ctx.table(Tables.silverCreditNote))
    matched, _ = crosswalkReturnReason(standardizeReturn(bronzeReturns), returnReasonCrosswalk(ctx))
    expectedReturns, _ = screenReturns(matched)
    expectedCredits, _, _ = screenCreditNotes(standardizeCreditNote(bronzeCredits))
    returnColumns = [
        "return_line_business_key",
        "customer_business_key",
        "return_reason_code",
        "returned_quantity",
        "refund_amount",
        "transaction_currency_code",
        "returned_date",
        "region_code",
    ]
    creditColumns = [
        "credit_note_business_key",
        "customer_business_key",
        "credit_reason_code",
        "credit_note_date",
        "net_amount",
        "tax_amount",
        "gross_amount",
        "transaction_currency_code",
        "approval_band",
        "region_code",
    ]
    checks = [
        rowCountCheck(expectedReturns.count(), silverReturns.count(), baseline="source_derived"),
        checksumCheck(expectedReturns, silverReturns, returnColumns, baseline="source_derived"),
        {
            **rowCountCheck(expectedCredits.count(), silverCredits.count(), baseline="source_derived"),
            "check": "row_count_credit_note",
        },
        {
            **checksumCheck(expectedCredits, silverCredits, creditColumns, baseline="source_derived"),
            "check": "checksum_credit_note",
        },
        nullRateCheck(expectedReturns, silverReturns, "return_reason_code"),
        {
            "check": "legacy_target_row_count",
            "source": legacyCountOrNone(ctx, ctx.legacyStaging, "stg", "Return"),
            "target": None,
            "pass": True,
        },
        {
            "check": "legacy_target_row_count_credit_note",
            "source": legacyCountOrNone(ctx, ctx.legacyStaging, "stg", "CreditNote"),
            "target": None,
            "pass": True,
        },
    ]
    ev = PackageEvidence(
        "STG_Load_ReturnAndCredit",
        verdictFor(checks, sourceDerived=True),
        f"{legacyName(LEGACY_STAGING_DATABASE, 'stg', 'Return')}, {legacyName(LEGACY_STAGING_DATABASE, 'stg', 'CreditNote')}",
        f"{ctx.table(Tables.silverReturn)}, {ctx.table(Tables.silverCreditNote)}".replace("`", ""),
        checks,
    )
    ev.summary = (
        "Legacy stg.Return / stg.CreditNote are empty on the baseline host; expected rows re-derived from bronze with the "
        "package's reason crosswalk, quantity screen, approval-band and positive-amount screens. PARTIAL by contract."
    )
    return ev


def factShipmentEvidence(ctx: RunContext) -> PackageEvidence:
    spark = ctx.spark
    silver = spark.table(ctx.table(Tables.silverShipment))
    gold = spark.table(ctx.table(Tables.goldFactShipment))
    lines = readTableOrEmpty(
        spark, ctx.table(Tables.silverShipmentLine), T.StructType([T.StructField("shipment_business_key", T.StringType())])
    )
    lineCounts = lines.groupBy("shipment_business_key").agg(F.count(F.lit(1)).alias("package_count"))
    scans = readTableOrEmpty(spark, ctx.table(Tables.bronzeFileCarrierScan), SCAN_EVENTS_SCHEMA)
    expected = buildShipmentFactRows(latestStagedShipments(silver), lineCounts, summarizeScanEvents(scans))
    columns = [
        "despatch_note_number",
        "despatch_date_key",
        "promised_delivery_date_key",
        "service_level_code",
        "region_code",
        "total_weight_kg",
        "chargeable_weight_kg",
        "freight_charge",
        "package_count",
        "on_time_delivery_flag",
    ]
    checks = [
        rowCountCheck(silver.select("shipment_business_key").distinct().count(), gold.count(), baseline="source_derived"),
        checksumCheck(expected, gold, columns, baseline="source_derived"),
        nullRateCheck(None, gold, "carrier_key"),
        {
            "check": "legacy_target_row_count",
            "source": legacyCountOrNone(ctx, ctx.legacyDw, "Fact", "Shipment"),
            "target": None,
            "pass": True,
        },
        {
            "check": "rows_with_inferred_members",
            "source": None,
            "target": gold.where(F.col("inferred_member_flag")).count(),
            "pass": True,
        },
    ]
    ev = PackageEvidence(
        "FACT_Load_Shipment",
        verdictFor(checks, sourceDerived=True),
        legacyName(LEGACY_DW_DATABASE, "Fact", "Shipment"),
        ctx.table(Tables.goldFactShipment).replace("`", ""),
        checks,
    )
    ev.summary = (
        "Legacy Fact.Shipment is empty on the baseline host; accumulating snapshot re-derived from silver_shipment + carrier "
        "scans with the package's milestone / chargeable-weight / on-time rules (one row per despatch note). Dimension keys "
        "come from wwi_legacy_dw dimensions with unknown-member fallback. PARTIAL by contract (source_derived)."
    )
    return ev


def factOrderFulfilmentEvidence(ctx: RunContext) -> PackageEvidence:
    spark = ctx.spark
    gold = spark.table(ctx.table(Tables.goldFactOrderFulfilment))
    staging = snakeCaseColumns(readLegacy(spark, ctx.legacyStaging, "stg", "OrderFulfilment"))
    stagingCount = staging.count()
    if stagingCount:
        orders, sourceName = ordersFromLegacyStaging(staging), legacyName(LEGACY_STAGING_DATABASE, "stg", "OrderFulfilment")
    else:
        orders, sourceName = (
            ordersFromLegacyFactOrder(readLegacy(spark, ctx.legacyDw, "Fact", "Order")),
            legacyName(LEGACY_DW_DATABASE, "Fact", "Order"),
        )
    columns = [
        "order_number",
        "order_date_key",
        "pick_date_key",
        "order_line_count",
        "quantity_ordered",
        "quantity_despatched",
        "order_value_reporting",
    ]
    checks = [
        rowCountCheck(orders.count(), gold.count(), baseline="source_derived"),
        checksumCheck(orders, gold, columns, baseline="source_derived"),
        nullRateCheck(orders, gold, "order_date_key"),
        {
            "check": "legacy_target_row_count",
            "source": legacyCountOrNone(ctx, ctx.legacyDw, "Fact", "Order Fulfilment"),
            "target": None,
            "pass": True,
        },
        {"check": "legacy_stg_order_fulfilment_row_count", "source": stagingCount, "target": None, "pass": True},
        {"check": "rows_stalled", "source": None, "target": gold.where(F.col("stalled_flag")).count(), "pass": True},
        {
            "check": "rows_cycle_complete",
            "source": None,
            "target": gold.where(F.col("cycle_complete_flag")).count(),
            "pass": True,
        },
    ]
    ev = PackageEvidence(
        "FACT_Load_OrderFulfilment",
        verdictFor(checks, sourceDerived=True),
        f"{legacyName(LEGACY_DW_DATABASE, 'Fact', 'Order Fulfilment')} (seeded from {sourceName})",
        ctx.table(Tables.goldFactOrderFulfilment).replace("`", ""),
        checks,
    )
    ev.summary = (
        "Legacy Fact.Order Fulfilment and its stg.OrderFulfilment input are both empty on the baseline host; the order grain is "
        f"seeded from {sourceName} (one row per order, line measures summed) and milestones come from Fact.Sale / Fact.Payment / "
        "gold_fact_shipment. Expected count/checksum re-derived from the same source. PARTIAL by contract (source_derived)."
    )
    return ev


def factReturnEvidence(ctx: RunContext) -> PackageEvidence:
    spark = ctx.spark
    silver = spark.table(ctx.table(Tables.silverReturn))
    gold = spark.table(ctx.table(Tables.goldFactReturn))
    expected = buildReturnFactRows(silver, legacyFactSale(ctx), returnReasonWindows(ctx))
    columns = [
        "return_line_business_key",
        "return_date_key",
        "region_code",
        "quantity_returned",
        "gross_return_amount",
        "restocking_fee_amount",
        "net_credit_amount",
        "statutory_window_days",
        "within_statutory_window_flag",
        "disposition_code",
    ]
    checks = [
        rowCountCheck(silver.count(), gold.count(), baseline="source_derived"),
        checksumCheck(expected, gold, columns, baseline="source_derived"),
        nullRateCheck(None, gold, "customer_key"),
        {
            "check": "negative_measures",
            "source": None,
            "target": gold.where(F.col("quantity_returned") > 0).count(),
            "pass": gold.where(F.col("quantity_returned") > 0).count() == 0,
        },
        {
            "check": "legacy_target_row_count",
            "source": legacyCountOrNone(ctx, ctx.legacyDw, "Fact", "Return"),
            "target": None,
            "pass": True,
        },
        {
            "check": "rows_missing_original_sale",
            "source": None,
            "target": gold.where(F.col("original_sale_missing_flag")).count(),
            "pass": True,
        },
    ]
    ev = PackageEvidence(
        "FACT_Load_Return",
        verdictFor(checks, sourceDerived=True),
        legacyName(LEGACY_DW_DATABASE, "Fact", "Return"),
        ctx.table(Tables.goldFactReturn).replace("`", ""),
        checks,
    )
    ev.summary = (
        "Legacy Fact.Return is empty on the baseline host; expected rows re-derived from silver_return + Fact.Sale with the "
        "package's negative-measure, regional statutory-window and restocking-fee rules. Returns without an original sale are "
        "loaded with inferred cost and logged to err_rejected_fact. PARTIAL by contract (source_derived)."
    )
    return ev


def factCreditNoteEvidence(ctx: RunContext) -> PackageEvidence:
    spark = ctx.spark
    silver = spark.table(ctx.table(Tables.silverCreditNote))
    gold = spark.table(ctx.table(Tables.goldFactCreditNote))
    hold = readTableOrEmpty(
        spark, ctx.table(Tables.goldCreditNoteApprovalHold), T.StructType([T.StructField("credit_note_number", T.StringType())])
    )
    returns = readTableOrEmpty(
        spark, ctx.table(Tables.goldFactReturn), T.StructType([T.StructField("credit_note_number", T.StringType())])
    )
    rows = buildCreditNoteFactRows(silver, legacyFactSale(ctx))
    empty = spark.createDataFrame([], T.StructType([T.StructField("credit_note_number", T.StringType())]))
    expected, _, linked, held = splitCreditNotes(rows, empty, returns)
    columns = [
        "credit_note_number",
        "credit_note_date_key",
        "region_code",
        "credit_reason_code",
        "credit_excluding_tax",
        "credit_including_tax",
        "tax_regime_code",
        "tax_adjustment_reason_code",
    ]
    checks = [
        rowCountCheck(
            expected.select("credit_note_number").distinct().count(),
            gold.select("credit_note_number").distinct().count(),
            baseline="source_derived",
        ),
        checksumCheck(
            expected, gold.dropDuplicates(["credit_note_number", "credit_note_line_number"]), columns, baseline="source_derived"
        ),
        nullRateCheck(None, gold, "customer_key"),
        {
            "check": "negative_measures",
            "source": None,
            "target": gold.where(F.col("credit_including_tax") > 0).count(),
            "pass": gold.where(F.col("credit_including_tax") > 0).count() == 0,
        },
        {
            "check": "legacy_target_row_count",
            "source": legacyCountOrNone(ctx, ctx.legacyDw, "Fact", "Credit Note"),
            "target": None,
            "pass": True,
        },
        {"check": "rows_parked_for_approval", "source": held.count(), "target": hold.count(), "pass": True},
        {"check": "rows_return_linked", "source": linked.count(), "target": None, "pass": True},
    ]
    ev = PackageEvidence(
        "FACT_Load_CreditNote",
        verdictFor(checks, sourceDerived=True),
        legacyName(LEGACY_DW_DATABASE, "Fact", "Credit Note"),
        ctx.table(Tables.goldFactCreditNote).replace("`", ""),
        checks,
    )
    ev.summary = (
        "Legacy Fact.Credit Note is empty on the baseline host; expected rows re-derived from silver_credit_note + Fact.Sale "
        "with the package's negative-measure, regional tax-regime, duplicate / return-linked / approval-hold splits. PARTIAL."
    )
    return ev


def aggDeliveryPerformanceEvidence(ctx: RunContext) -> PackageEvidence:
    spark = ctx.spark
    fact = spark.table(ctx.table(Tables.goldFactShipment))
    gold = spark.table(ctx.table(Tables.goldAggDeliveryPerformanceSummary))
    fromDate = refreshWindowStart(ctx.startedAtUtc.date(), DEFAULT_WEEKS_TO_REFRESH, ctx.reloadFullHistory)
    expected = buildWeeklyDeliveryPerformance(fact, fromDate, ctx.batchId)
    window = gold if fromDate is None else gold.where(F.col("iso_week_start_date") >= F.lit(fromDate).cast("date"))
    columns = [
        "iso_week_start_date",
        "carrier_key",
        "warehouse_site_key",
        "sales_territory_key",
        "region_code",
        "service_level_code",
        "consignment_count",
        "delivered_count",
        "on_time_count",
        "on_time_percent",
        "sla_target_percent",
        "sla_breach_flag",
        "service_credit_reporting",
    ]
    checks = [
        rowCountCheck(expected.count(), window.count(), baseline="source_derived"),
        checksumCheck(expected, window, columns, baseline="source_derived"),
        nullRateCheck(expected, window, "on_time_percent"),
        {
            "check": "legacy_target_row_count",
            "source": legacyCountOrNone(ctx, ctx.legacyDw, "Aggregate", "Delivery Performance Summary"),
            "target": None,
            "pass": True,
        },
        {
            "check": "fact_shipment_rows_in_window",
            "source": fact.count()
            if fromDate is None
            else fact.where(F.col("despatch_date_key") >= F.lit(fromDate).cast("date")).count(),
            "target": None,
            "pass": True,
        },
        {"check": "rows_total_all_weeks", "source": None, "target": gold.count(), "pass": True},
    ]
    ev = PackageEvidence(
        "AGG_Refresh_DeliveryPerformanceSummary",
        verdictFor(checks, sourceDerived=True),
        legacyName(LEGACY_DW_DATABASE, "Aggregate", "Delivery Performance Summary"),
        ctx.table(Tables.goldAggDeliveryPerformanceSummary).replace("`", ""),
        checks,
    )
    ev.summary = (
        "Legacy Aggregate.Delivery Performance Summary is empty on the baseline host (and its Fact.Shipment input too); the "
        "weekly carrier x site x territory x region x service-level summary is re-derived from gold_fact_shipment with the "
        "proc's regional on-time rule, SLA targets and EU service-credit cap, compared for the refresh window. PARTIAL."
    )
    return ev


EVIDENCE_BUILDERS: dict[str, Callable[[RunContext], PackageEvidence]] = {
    "EXT_SQL_Shipments": lambda ctx: extractEvidence(ctx, "EXT_SQL_Shipments"),
    "EXT_SQL_ShipmentLines": lambda ctx: extractEvidence(ctx, "EXT_SQL_ShipmentLines"),
    "EXT_SQL_Returns": lambda ctx: extractEvidence(ctx, "EXT_SQL_Returns"),
    "EXT_SQL_CreditNotes": lambda ctx: extractEvidence(ctx, "EXT_SQL_CreditNotes"),
    "ING_FILE_CarrierScan": carrierScanEvidence,
    "STG_Load_Shipment": stgShipmentEvidence,
    "STG_Load_ReturnAndCredit": stgReturnAndCreditEvidence,
    "FACT_Load_Shipment": factShipmentEvidence,
    "FACT_Load_OrderFulfilment": factOrderFulfilmentEvidence,
    "FACT_Load_Return": factReturnEvidence,
    "FACT_Load_CreditNote": factCreditNoteEvidence,
    "AGG_Refresh_DeliveryPerformanceSummary": aggDeliveryPerformanceEvidence,
}


def failedEvidence(unit: str, error: Exception) -> PackageEvidence:
    checks = [
        {"check": "row_count", "source": None, "target": None, "pass": False},
        {"check": "checksum", "source": None, "target": None, "pass": False},
        {"check": "error", "message": str(error)[:1000], "pass": False},
    ]
    return PackageEvidence(unit, "FAIL", "unknown", "unknown", checks, f"Evidence computation failed: {str(error)[:500]}")


def buildEvidenceRows(ctx: RunContext, runId: str, runAt: datetime) -> list[tuple]:
    rows: list[tuple] = []
    for unit, builder in EVIDENCE_BUILDERS.items():
        try:
            ev = builder(ctx)
        except Exception as exc:  # noqa: BLE001 - one broken package must not hide the other eleven
            ev = failedEvidence(unit, exc)
        rows.append(
            (
                runId,
                runAt,
                ev.unit,
                UNIT_TYPE,
                ev.verdict,
                BRANCH,
                ev.sourceObject,
                ev.targetObject,
                json.dumps(ev.checks, default=str),
                ev.summary,
                ctx.gitSha,
                ACTOR,
                HARNESS_VERSION,
            )
        )
    return rows


def runRecon(ctx: RunContext) -> tuple[str, DataFrame]:
    """Compute and append the evidence rows; returns ``(run_id, evidence_frame)``."""
    runId = str(uuid.uuid4())
    runAt = datetime.utcnow()
    evidence = ctx.spark.createDataFrame(buildEvidenceRows(ctx, runId, runAt), EVIDENCE_SCHEMA_STRUCT)
    evidence = evidence.withColumn("run_at", F.current_timestamp())
    evidenceTable = f"`{ctx.catalog}`.`{EVIDENCE_SCHEMA}`.`{EVIDENCE_TABLE}`"
    evidence.write.format("delta").mode("append").saveAsTable(evidenceTable)
    localCopy = ctx.table(Tables.ctlReconResults)
    if tableExists(ctx.spark, localCopy):
        evidence.write.format("delta").mode("append").saveAsTable(localCopy)
    else:
        evidence.write.format("delta").saveAsTable(localCopy)
    return runId, evidence
