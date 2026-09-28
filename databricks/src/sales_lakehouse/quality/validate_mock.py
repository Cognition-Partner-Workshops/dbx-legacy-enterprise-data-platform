"""Validate the loaded lakehouse against the mock generator's manifest and CSV extracts.

The mock generator (``sales_lakehouse.mock_data``) writes ``manifest.json`` with every
edge case it planted and the natural keys it planted them on. This module derives the
expected downstream footprint of each case from the manifest + the raw CSVs and compares
it with what bronze -> silver -> gold actually produced:

* ``FACT_SALE_NET_USD``   sum(fact_sale.net_amount_reporting) vs the invoice lines with the
                          regional FX rule re-applied independently from ``WWI_REF.FX_RATE_DAILY``
* ``FACT_SALE_ROWS_<R>``  fact_sale rows per region vs distinct mock invoice lines per region
* ``FX_RATE_MATCH``       every fact_sale row carries the rate the regional rule implies
* ``MISSING_FX_*``        planted FX gaps resolve to a prior rate or ``DEFAULT_1`` - never to
                          a rate quoted on the missing day
* one check per planted transactional / MDM edge case (see ``EDGE_CASE_CHECKS``)

Every check returns :class:`ValidationResult`; :func:`run` prints a pass/fail table and the
CLI exits non-zero when any check fails. Nothing here mutates the lakehouse.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from sales_lakehouse.common.config import REGION_CODES, PipelineConfig
from sales_lakehouse.common.quality import REJECTED_ROWS_TABLE
from sales_lakehouse.common.tables import tableExists
from sales_lakehouse.silver.business_keys import DEFAULT_SOURCE_SYSTEM
from sales_lakehouse.silver.rules.fx import APAC_FALLBACK_DAYS, NA_FALLBACK_DAYS, SOURCE_DEFAULT_1
from sales_lakehouse.silver.rules.tax import TREATMENT_REVERSE_CHARGE, TREATMENT_REVERSE_CHARGE_NO_VATREG

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_SKIPPED = "SKIPPED"

MANIFEST_FILE = "manifest.json"
SQLSERVER = "sqlserver"
ORACLE = "oracle"
NET_TOTAL_TOLERANCE = Decimal("0.001")  # 0.1 % - APAC GST truncation residuals are sub-cent per line
PILOT_CHANNEL_REJECT_RULE = "COMM_NON_COMMISSIONABLE_CHANNEL"
DUP_ORDER_LINE_RULE = "DUP_ORDER_LINE"
MONEY = DecimalType(19, 4)


@dataclass(frozen=True)
class ValidationResult:
    code: str
    status: str
    observed: str
    expected: str
    detail: str = ""


@dataclass
class ValidationReport:
    results: list[ValidationResult] = field(default_factory=list)

    def add(self, result: ValidationResult) -> None:
        self.results.append(result)

    @property
    def failed(self) -> list[ValidationResult]:
        return [r for r in self.results if r.status == STATUS_FAIL]

    @property
    def ok(self) -> bool:
        return not self.failed

    def describe(self) -> str:
        counts = {s: sum(r.status == s for r in self.results) for s in (STATUS_PASS, STATUS_FAIL, STATUS_SKIPPED)}
        return ", ".join(f"{n} {s.lower()}" for s, n in counts.items())

    def table(self) -> str:
        header = ("check", "status", "observed", "expected", "detail")
        rows = [header] + [(r.code, r.status, r.observed, r.expected, r.detail) for r in self.results]
        widths = [max(len(str(row[i])) for row in rows) for i in range(len(header))]
        lines = ["  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)) for row in rows]
        lines.insert(1, "  ".join("-" * w for w in widths))
        return "\n".join(lines)


# --------------------------------------------------------------------------- manifest / csv access
def loadManifest(mockRoot: str) -> dict:
    with open(os.path.join(mockRoot, MANIFEST_FILE), encoding="utf-8") as fh:
        return json.load(fh)


def edgeCaseKeys(manifest: dict, code: str) -> list[dict]:
    for case in manifest.get("edgeCases", []):
        if case.get("code") == code:
            return list(case.get("keys", []))
    return []


def readCsv(spark: SparkSession, cfg: PipelineConfig, system: str, schema: str, table: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", False).csv(cfg.sourcePath(system, schema, table))


def _table(spark: SparkSession, cfg: PipelineConfig, layer: str, table: str) -> DataFrame | None:
    fqn = cfg.fqn(layer, table)
    return spark.table(fqn) if tableExists(spark, fqn) else None


def businessKey(naturalKey: object) -> str:
    return f"{DEFAULT_SOURCE_SYSTEM}|{naturalKey}"


def lineKey(headerKey: object, lineId: object) -> str:
    return f"{businessKey(headerKey)}|{lineId}"


def _pass(code: str, observed: object, expected: object, detail: str = "") -> ValidationResult:
    return ValidationResult(code, STATUS_PASS, str(observed), str(expected), detail)


def _fail(code: str, observed: object, expected: object, detail: str = "") -> ValidationResult:
    return ValidationResult(code, STATUS_FAIL, str(observed), str(expected), detail)


def _skip(code: str, detail: str) -> ValidationResult:
    return ValidationResult(code, STATUS_SKIPPED, "-", "-", detail)


def _compare(code: str, observed: object, expected: object, detail: str = "") -> ValidationResult:
    return _pass(code, observed, expected, detail) if observed == expected else _fail(code, observed, expected, detail)


# --------------------------------------------------------------------------- expected FX / invoice lines
def expectedFxRates(fxDaily: DataFrame, keys: DataFrame, reportingCurrency: str) -> DataFrame:
    """Independent re-statement of ``silver.rules.fx``: one rate per (currency, region, txn date).

    NA: latest quote within 7 days before the transaction date; EU: latest quote on or before
    the date (no window); APAC: the quote anchored on the first of the month, 7-day window.
    Missing -> 1.0 with ``expected_fx_default`` = true. ``keys`` carries
    ``ccy, region, txn_date``.
    """
    rates = fxDaily.filter(
        (F.upper(F.col("TO_CURR_CD")) == reportingCurrency) & (F.upper(F.coalesce(F.col("SUPERSEDED_FLG"), F.lit("N"))) != "Y")
    ).select(
        F.upper(F.trim(F.col("FROM_CURR_CD"))).alias("_fx_ccy"),
        F.to_date(F.col("RATE_DT")).alias("_fx_date"),
        F.col("RATE").cast(DecimalType(19, 8)).alias("_fx_rate"),
    )
    anchored = keys.withColumn(
        "_anchor", F.when(F.col("region") == "APAC", F.trunc(F.col("txn_date"), "MM")).otherwise(F.col("txn_date"))
    ).withColumn(
        "_lookback",
        F.when(F.col("region") == "NA", F.lit(NA_FALLBACK_DAYS))
        .when(F.col("region") == "APAC", F.lit(APAC_FALLBACK_DAYS))
        .otherwise(F.lit(None).cast("int")),
    )
    cond = (
        (F.col("ccy") == F.col("_fx_ccy"))
        & (F.col("_fx_date") <= F.col("_anchor"))
        & (F.col("_lookback").isNull() | (F.col("_fx_date") >= F.date_sub(F.col("_anchor"), F.col("_lookback"))))
    )
    picked = (
        anchored.join(rates, cond, "left")
        .groupBy("ccy", "region", "txn_date", "_anchor")
        .agg(F.max_by("_fx_rate", "_fx_date").alias("_rate"), F.max("_fx_date").alias("expected_fx_effective_date"))
    )
    return picked.select(
        "ccy",
        "region",
        "txn_date",
        F.col("_anchor").alias("expected_fx_anchor_date"),
        F.when(F.col("ccy") == reportingCurrency, F.lit(Decimal(1)).cast(DecimalType(19, 8)))
        .otherwise(F.coalesce(F.col("_rate"), F.lit(Decimal(1)).cast(DecimalType(19, 8))))
        .alias("expected_fx_rate"),
        ((F.col("ccy") != reportingCurrency) & F.col("_rate").isNull()).alias("expected_fx_default"),
        "expected_fx_effective_date",
    )


def mockInvoiceLines(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Invoice lines joined to their header, region and currency (``ccy, region, txn_date`` ready)."""
    invoices = readCsv(spark, cfg, SQLSERVER, "Sales", "Invoices")
    lines = readCsv(spark, cfg, SQLSERVER, "Sales", "InvoiceLines")
    territories = readCsv(spark, cfg, SQLSERVER, "Sales", "SalesTerritories")
    hdr = invoices.join(
        territories.select(F.col("SalesTerritoryID").alias("_terr_id"), F.upper(F.col("RegionCode")).alias("region")),
        invoices["SalesTerritoryID"] == F.col("_terr_id"),
        "left",
    ).select(
        F.col("InvoiceID").alias("invoice_id"),
        F.col("OrderID").alias("order_id"),
        F.col("CustomerID").alias("customer_id"),
        F.to_date(F.col("InvoiceDate")).alias("txn_date"),
        F.upper(F.trim(F.col("CurrencyCode"))).alias("ccy"),
        F.col("region"),
        F.coalesce(F.col("IsCreditNote"), F.lit("0")).alias("is_credit_note"),
    )
    return lines.select(
        F.col("InvoiceLineID").alias("invoice_line_id"),
        F.col("InvoiceID").alias("invoice_id"),
        F.col("StockItemID").alias("stock_item_id"),
        F.col("Quantity").cast("decimal(18,3)").alias("quantity"),
        F.col("UnitPrice").cast(MONEY).alias("unit_price"),
        F.col("TaxRate").cast("decimal(18,3)").alias("tax_rate"),
        F.col("TaxAmount").cast(MONEY).alias("tax_amount"),
        F.col("ExtendedPrice").cast(MONEY).alias("extended_price"),
    ).join(hdr, "invoice_id", "inner")


def _factSale(spark: SparkSession, cfg: PipelineConfig) -> DataFrame | None:
    fact = _table(spark, cfg, "gold", "fact_sale")
    if fact is None:
        return None
    if "correction_type_code" in fact.columns:
        fact = fact.filter(F.col("correction_type_code").isNull() | (F.col("correction_type_code") != "REVERSAL"))
    return fact


# --------------------------------------------------------------------------- checks
def checkFactSaleTotals(spark: SparkSession, cfg: PipelineConfig, manifest: dict, report: ValidationReport) -> None:
    fact = _factSale(spark, cfg)
    if fact is None:
        report.add(_skip("FACT_SALE_NET_USD", "gold.fact_sale missing"))
        return
    lines = mockInvoiceLines(spark, cfg).filter(F.col("is_credit_note").isin("0", "false", "False"))
    keys = lines.select("ccy", "region", "txn_date").distinct()
    fx = expectedFxRates(readCsv(spark, cfg, ORACLE, "WWI_REF", "FX_RATE_DAILY"), keys, cfg.reportingCurrency)
    expected = (
        lines.join(fx, ["ccy", "region", "txn_date"], "left")
        .withColumn("_net_local", (F.col("extended_price") - F.coalesce(F.col("tax_amount"), F.lit(0))).cast(MONEY))
        .withColumn("_net_usd", (F.col("_net_local") * F.col("expected_fx_rate")).cast(MONEY))
        .groupBy("region")
        .agg(F.sum("_net_usd").alias("expected_net_usd"), F.countDistinct("invoice_line_id").alias("expected_rows"))
        .collect()
    )
    observed = (
        fact.groupBy("region_code")
        .agg(F.sum(F.col("net_amount_reporting").cast(MONEY)).alias("net_usd"), F.count(F.lit(1)).alias("rows"))
        .collect()
    )
    expectedByRegion = {r["region"]: r for r in expected}
    observedByRegion = {r["region_code"]: r for r in observed}
    expTotal = sum((Decimal(r["expected_net_usd"] or 0) for r in expected), Decimal(0))
    obsTotal = sum((Decimal(r["net_usd"] or 0) for r in observed), Decimal(0))
    diff = abs(obsTotal - expTotal)
    tolerance = abs(expTotal) * NET_TOTAL_TOLERANCE
    detail = f"diff={diff:.4f} tolerance={tolerance:.4f} (mock invoice lines x regional FX rule)"
    result = _pass if diff <= tolerance else _fail
    report.add(result("FACT_SALE_NET_USD", f"{obsTotal:.2f}", f"{expTotal:.2f}", detail))
    for region in REGION_CODES:
        exp = expectedByRegion.get(region)
        obs = observedByRegion.get(region)
        report.add(
            _compare(
                f"FACT_SALE_ROWS_{region}",
                int(obs["rows"]) if obs else 0,
                int(exp["expected_rows"]) if exp else 0,
                "fact_sale rows vs distinct mock invoice lines",
            )
        )


def checkFxRates(spark: SparkSession, cfg: PipelineConfig, manifest: dict, report: ValidationReport) -> None:
    fact = _factSale(spark, cfg)
    if fact is None:
        report.add(_skip("FX_RATE_MATCH", "gold.fact_sale missing"))
        return
    rows = fact.select(
        F.upper(F.col("transaction_currency_code")).alias("ccy"),
        F.col("region_code").alias("region"),
        F.col("invoice_date_key").alias("txn_date"),
        F.col("fx_rate_to_reporting").cast(DecimalType(19, 8)).alias("fx_rate"),
        F.col("fx_rate_source_code").alias("fx_source"),
        F.col("fx_rate_effective_date").alias("fx_effective_date"),
    ).filter(F.col("ccy").isNotNull() & F.col("txn_date").isNotNull())
    fx = expectedFxRates(
        readCsv(spark, cfg, ORACLE, "WWI_REF", "FX_RATE_DAILY"), rows.select("ccy", "region", "txn_date").distinct(), cfg.reportingCurrency
    )
    joined = rows.join(fx, ["ccy", "region", "txn_date"], "left")
    mismatched = joined.filter(F.abs(F.col("fx_rate") - F.col("expected_fx_rate")) > F.lit(Decimal("0.00000001"))).count()
    report.add(_compare("FX_RATE_MATCH", mismatched, 0, "fact_sale rows whose FX rate differs from the regional rule"))
    defaults = joined.filter(F.col("expected_fx_default")).count()
    defaultsFlagged = joined.filter(F.col("expected_fx_default") & (F.col("fx_source") == SOURCE_DEFAULT_1)).count()
    report.add(_compare("FX_DEFAULT_1_FLAGGED", defaultsFlagged, defaults, "rows with no rate in window carry DEFAULT_1"))

    for code in ("MISSING_FX_RATE", "MISSING_FX_MONTH", "MISSING_FX_WEEKEND"):
        keys = edgeCaseKeys(manifest, code)
        if not keys:
            report.add(_skip(code, "not in manifest"))
            continue
        missing = spark.createDataFrame(
            [(k["FROM_CURR_CD"].upper(), dt.date.fromisoformat(k["RATE_DT"])) for k in keys], "ccy string, missing_date date"
        )
        affected = joined.join(
            missing, (joined["ccy"] == missing["ccy"]) & (F.col("expected_fx_anchor_date") == F.col("missing_date")), "inner"
        )
        affectedCount = affected.count()
        if affectedCount == 0:
            report.add(_skip(code, "no fact_sale line anchored on a planted missing FX day"))
            continue
        quotedOnMissingDay = affected.filter(F.col("fx_effective_date") == F.col("missing_date")).count()
        fallback = affected.filter((F.col("fx_source") == SOURCE_DEFAULT_1) | (F.col("fx_effective_date") < F.col("missing_date"))).count()
        detail = (
            f"{affectedCount} lines anchored on planted gaps: {fallback} prior-rate/DEFAULT_1, {quotedOnMissingDay} quoted on the gap day"
        )
        report.add(_compare(code, fallback, affectedCount, detail))


def checkDuplicateOrderLines(spark: SparkSession, cfg: PipelineConfig, manifest: dict, report: ValidationReport) -> None:
    keys = edgeCaseKeys(manifest, "DUPLICATE_ORDER_LINE")
    rejected = _table(spark, cfg, "quality", REJECTED_ROWS_TABLE)
    if not keys or rejected is None:
        report.add(_skip("DUPLICATE_ORDER_LINE", "manifest keys or rejected_rows missing"))
        return
    dupRows = rejected.filter(F.col("rule_code") == DUP_ORDER_LINE_RULE).select("row_json")
    matched = 0
    for k in keys:
        needles = [lineKey(k["OrderID"], lineId.strip()) for lineId in str(k["OrderLineIDs"]).split(",")]
        cond = F.lit(False)
        for needle in needles:
            cond = cond | F.col("row_json").contains(needle)
        if dupRows.filter(cond).limit(1).count() > 0:
            matched += 1
    total = dupRows.count()
    report.add(
        _compare("DUPLICATE_ORDER_LINE", matched, len(keys), f"planted pairs with a {DUP_ORDER_LINE_RULE} loser ({total} rows total)")
    )


def checkPilotChannel(spark: SparkSession, cfg: PipelineConfig, manifest: dict, report: ValidationReport) -> None:
    channelKeys = edgeCaseKeys(manifest, "PILOT_CHANNEL")
    orderKeys = edgeCaseKeys(manifest, "PILOT_CHANNEL_ORDER")
    channel = _table(spark, cfg, "silver", "dim_sales_channel")
    if not channelKeys or channel is None:
        report.add(_skip("PILOT_CHANNEL", "manifest keys or dim_sales_channel missing"))
        return
    codes = [k["ChannelCode"] for k in channelKeys]
    pilot = channel.filter(F.col("sales_channel_code").isin(*codes))
    report.add(
        _compare(
            "PILOT_CHANNEL",
            pilot.filter(~F.coalesce(F.col("is_commissionable"), F.lit(True)) & F.coalesce(F.col("is_orderable"), F.lit(False))).count(),
            len(codes),
            "PILOT channels are orderable but not commissionable",
        )
    )
    fact = _factSale(spark, cfg)
    rejected = _table(spark, cfg, "quality", REJECTED_ROWS_TABLE)
    if fact is None or rejected is None or not orderKeys:
        report.add(_skip("PILOT_CHANNEL_ORDER", "fact_sale, rejected_rows or manifest keys missing"))
        return
    pilotKeys = [r["sales_channel_key"] for r in pilot.select("sales_channel_key").collect()]
    pilotLines = fact.filter(F.col("sales_channel_key").isin(*pilotKeys)).count() if pilotKeys else 0
    commissionRejects = rejected.filter(F.col("rule_code") == PILOT_CHANNEL_REJECT_RULE).count()
    orderNumbers = [businessKey(k["OrderID"]) for k in orderKeys]
    loadedOrders = fact.filter(F.col("order_number").isin(*orderNumbers)).select("order_number").distinct().count()
    detail = f"{pilotLines} fact_sale lines on PILOT channels, {commissionRejects} {PILOT_CHANNEL_REJECT_RULE} rejects"
    ok = pilotLines > 0 and commissionRejects >= min(pilotLines, 1) and loadedOrders > 0
    result = _pass if ok else _fail
    report.add(result("PILOT_CHANNEL_ORDER", f"orders_loaded={loadedOrders} rejects={commissionRejects}", "loaded>0 and rejects>0", detail))


def checkEuConsent(spark: SparkSession, cfg: PipelineConfig, manifest: dict, report: ValidationReport, partnerFeedDir: str | None) -> None:
    keys = edgeCaseKeys(manifest, "EU_CONSENT_N")
    customer = _table(spark, cfg, "silver", "dim_customer")
    if not keys or customer is None:
        report.add(_skip("EU_CONSENT_N", "manifest keys or dim_customer missing"))
        return
    feedDir = partnerFeedDir or os.path.join(cfg.mockDataRoot, "outbound", "partner_feed")
    euFiles = (
        sorted(f for f in os.listdir(feedDir) if f.startswith("partner_feed_EU_") and f.endswith(".csv")) if os.path.isdir(feedDir) else []
    )
    if not euFiles:
        report.add(_skip("EU_CONSENT_N", f"no EU partner feed under {feedDir}"))
        return
    references = {businessKey(k["CustomerID"]) for k in keys}
    references |= {str(k["CustomerID"]) for k in keys}
    feed = spark.read.option("header", True).csv(os.path.join(feedDir, euFiles[-1]))
    leaked = feed.filter(F.col("CustomerReference").isin(*references)).count()
    report.add(_compare("EU_CONSENT_N", leaked, 0, f"unconsented EU customers present in {euFiles[-1]}"))


def checkGstResidual(spark: SparkSession, cfg: PipelineConfig, manifest: dict, report: ValidationReport) -> None:
    keys = edgeCaseKeys(manifest, "GST_INCLUSIVE_RESIDUAL")
    fact = _factSale(spark, cfg)
    if not keys or fact is None:
        report.add(_skip("GST_INCLUSIVE_RESIDUAL", "manifest keys or fact_sale missing"))
        return
    if "tax_residual_local" not in fact.columns:
        report.add(_fail("GST_INCLUSIVE_RESIDUAL", "column missing", "tax_residual_local on fact_sale"))
        return
    planted = spark.createDataFrame(
        [(businessKey(k["OrderID"]), Decimal(str(k["UnitPrice"]))) for k in keys], "order_number string, planted_unit_price decimal(19,4)"
    )
    hits = fact.join(planted, ["order_number"], "inner").filter(F.col("unit_price").cast(MONEY) == F.col("planted_unit_price"))
    loaded = hits.count()
    if loaded == 0:
        report.add(_skip("GST_INCLUSIVE_RESIDUAL", "planted order lines were not invoiced in this extract"))
        return
    nonZero = hits.filter(F.coalesce(F.col("tax_residual_local"), F.lit(0)) != 0).count()
    report.add(_compare("GST_INCLUSIVE_RESIDUAL", nonZero, loaded, "GST-inclusive lines with a non-zero truncation residual"))


def checkReverseChargeNullVat(spark: SparkSession, cfg: PipelineConfig, manifest: dict, report: ValidationReport) -> None:
    keys = edgeCaseKeys(manifest, "EU_REVERSE_CHARGE_NULL_VAT")
    fact = _factSale(spark, cfg)
    if not keys or fact is None:
        report.add(_skip("EU_REVERSE_CHARGE_NULL_VAT", "manifest keys or fact_sale missing"))
        return
    invoiceNumbers = [businessKey(k["InvoiceID"]) for k in keys]
    rows = fact.filter(F.col("invoice_number").isin(*invoiceNumbers))
    loadedInvoices = rows.select("invoice_number").distinct().count()
    report.add(
        _compare("EU_REVERSE_CHARGE_NULL_VAT_LOADED", loadedInvoices, len(keys), "reverse-charge invoices without a VAT number are loaded")
    )
    # The invoice-level CustomerTaxNumber is NULL, but silver backfills the registration from the
    # customer master (legacy FACT_EU_Load_Sale looks it up in stg.Customer), so the line is a plain
    # REVERSE_CHARGE unless the customer has no VAT number either (REVERSE_CHARGE_NO_VATREG, WARN).
    reverseCharged = rows.filter(F.col("tax_treatment_code").isin(TREATMENT_REVERSE_CHARGE, TREATMENT_REVERSE_CHARGE_NO_VATREG))
    zeroTax = reverseCharged.filter(F.coalesce(F.col("tax_amount"), F.lit(0)) == 0).count()
    noVatReg = reverseCharged.filter(F.col("tax_treatment_code") == TREATMENT_REVERSE_CHARGE_NO_VATREG).count()
    report.add(
        _compare(
            "EU_REVERSE_CHARGE_NULL_VAT_TREATMENT",
            zeroTax,
            rows.count(),
            f"lines reverse-charged with zero tax ({noVatReg} {TREATMENT_REVERSE_CHARGE_NO_VATREG}, rest backfilled from customer master)",
        )
    )


def checkXrefResolution(spark: SparkSession, cfg: PipelineConfig, manifest: dict, report: ValidationReport) -> None:
    customer = _table(spark, cfg, "silver", "customer")
    if customer is None:
        report.add(_skip("XREF_RETIRED", "silver.customer missing"))
        return
    for code in ("XREF_RETIRED_SINGLE_HOP", "XREF_RETIRED_TWO_HOP"):
        keys = edgeCaseKeys(manifest, code)
        if not keys:
            report.add(_skip(code, "not in manifest"))
            continue
        expected = spark.createDataFrame(
            [(str(k["CustomerID"]), str(k["SURVIVOR_CUST_ID"])) for k in keys], "wwi_customer_id string, survivor string"
        )
        resolved = (
            customer.select(
                F.col("wwi_customer_id").cast("string").alias("wwi_customer_id"), F.col("erp_party_id").cast("string").alias("erp_party_id")
            )
            .join(expected, "wwi_customer_id", "inner")
            .filter(F.col("erp_party_id") == F.col("survivor"))
            .count()
        )
        report.add(_compare(code, resolved, len(keys), "retired PARTY_XREF resolves to the merge survivor"))
    keys = edgeCaseKeys(manifest, "XREF_RETIRED_NO_MERGE")
    if not keys:
        return
    # silver.party_resolution is the record of the walk; the customer row itself may be quarantined by an
    # unrelated screen (e.g. CUSTOMER_NO_CONSENT), so the status is read there when the table exists.
    resolution = _table(spark, cfg, "silver", "party_resolution")
    if resolution is not None:
        source, statusCol = resolution, "resolution_status_code"
    elif "party_resolution_status_code" in customer.columns:
        source, statusCol = customer, "party_resolution_status_code"
    else:
        report.add(_skip("XREF_RETIRED_NO_MERGE", "no party resolution status available"))
        return
    ids = [str(k["CustomerID"]) for k in keys]
    flagged = (
        source.filter(F.col("wwi_customer_id").cast("string").isin(*ids))
        .filter(F.col(statusCol) == "RETIRED_NO_SURVIVOR")
        .count()
    )
    report.add(_compare("XREF_RETIRED_NO_MERGE", flagged, len(keys), "retired party without merge history is flagged (WARN), not dropped"))


def checkUntranslatedCodes(spark: SparkSession, cfg: PipelineConfig, manifest: dict, report: ValidationReport) -> None:
    keys = edgeCaseKeys(manifest, "UNTRANSLATED_CODE")
    channel = _table(spark, cfg, "silver", "dim_sales_channel")
    payment = _table(spark, cfg, "silver", "payment")
    if not keys or channel is None:
        report.add(_skip("UNTRANSLATED_CODE", "manifest keys or dim_sales_channel missing"))
        return
    channelCodes = [k["SOURCE_VALUE_TXT"] for k in keys if k.get("CODE_SET_CD") == "SALES_CHANNEL"]
    if channelCodes:
        present = channel.filter(F.col("sales_channel_code").isin(*channelCodes)).select("sales_channel_code").distinct().count()
        report.add(
            _compare(
                "UNTRANSLATED_CODE_CHANNEL",
                present,
                len(channelCodes),
                "channel codes without CODE_TRANSLATION pass through to dim_sales_channel",
            )
        )
    paymentKeys = edgeCaseKeys(manifest, "UNTRANSLATED_PAYMENT_METHOD_USED")
    if paymentKeys and payment is not None:
        ids = [businessKey(k["CustomerPaymentID"]) for k in paymentKeys]
        loaded = payment.filter(F.col("payment_business_key").isin(*ids))
        methods = sorted(r[0] for r in loaded.select("payment_method_code").distinct().collect() if r[0] is not None)
        report.add(
            _compare(
                "UNTRANSLATED_CODE_PAYMENT_METHOD",
                loaded.count(),
                len(ids),
                f"payments on an untranslated method are loaded, not rejected (normalised as {', '.join(methods) or 'n/a'})",
            )
        )


EDGE_CASE_CHECKS: tuple[Callable[[SparkSession, PipelineConfig, dict, ValidationReport], None], ...] = (
    checkFactSaleTotals,
    checkFxRates,
    checkDuplicateOrderLines,
    checkPilotChannel,
    checkGstResidual,
    checkReverseChargeNullVat,
    checkXrefResolution,
    checkUntranslatedCodes,
)


def validate(spark: SparkSession, cfg: PipelineConfig, partnerFeedDir: str | None = None) -> ValidationReport:
    manifest = loadManifest(cfg.mockDataRoot)
    report = ValidationReport()
    for check in EDGE_CASE_CHECKS:
        check(spark, cfg, manifest, report)
    checkEuConsent(spark, cfg, manifest, report, partnerFeedDir)
    return report


def run(spark: SparkSession, cfg: PipelineConfig, partnerFeedDir: str | None = None) -> ValidationReport:
    """Validate and print the pass/fail table; raises ``RuntimeError`` when a check fails."""
    report = validate(spark, cfg, partnerFeedDir)
    print(report.table())
    if not report.ok:
        raise RuntimeError(f"{len(report.failed)} validation check(s) failed: {', '.join(r.code for r in report.failed)}")
    return report


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the lakehouse against the mock manifest.")
    parser.add_argument("--mock-root", required=True, help="directory holding manifest.json and the CSV extracts")
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--batch-id", type=int, default=None)
    parser.add_argument("--partner-feed-dir", default=None)
    parser.add_argument("--warehouse-dir", default=None, help="persistent local warehouse (SALES_LAKEHOUSE_WAREHOUSE_DIR)")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.warehouse_dir:
        os.environ["SALES_LAKEHOUSE_WAREHOUSE_DIR"] = args.warehouse_dir
    from sales_lakehouse.common.spark import getSpark

    cfg = PipelineConfig(catalog=args.catalog, mockDataRoot=args.mock_root, **({"batchId": args.batch_id} if args.batch_id else {}))
    report = validate(getSpark(), cfg, args.partner_feed_dir)
    print(report.table())
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
