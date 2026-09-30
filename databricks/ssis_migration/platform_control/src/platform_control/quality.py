"""Generic DQ engine: DQ_Rule_Engine, DQ_Threshold_Gate, DQ_Referential_Screen, DQ_Reject_Reprocess,
DQ_File_Screen and ING_FILE_QuarantineMalformed.

Rules are table driven (etl_data_quality_rule) and evaluated with a fixed, reviewed translation of
the steward predicates (rules.translateRuleExpression) - never by concatenating the raw T-SQL.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from platform_control.control import ControlFramework, sqlLiteral, utcNow
from platform_control.files import FileOps
from platform_control.rules import (
    classifyQuarantineRow,
    controlTotalStatus,
    dqResultStatus,
    fileScreenGates,
    inferOriginFeed,
    qualityScore,
    rejectPercent,
    rejectReprocessDecision,
    ruleEngineGates,
    screenFileRow,
    stockItemIdFromBusinessKey,
    thresholdGateOutcome,
)

FILE_PARTNER_SALES_EXPECTED_COLUMNS = 9


class GateFailure(RuntimeError):
    """Raised by a Failure-severity gate (raise_gate(..., severity="Failure") in the packages)."""


class ObjectResolver:
    """Map legacy ``schema.Table`` references to the table the DQ engine should read.

    err.* / etl.* / work.* objects are ours (snake_case Delta tables); stg/ref/raw objects belong to
    sibling groups and are read from the LEGACY staging database through federation."""

    OWNED_SCHEMAS = {"err", "etl", "work"}

    def __init__(self, cf: ControlFramework, overrides: dict[str, str] | None = None):
        self.cf = cf
        self.overrides = {k.lower(): v for k, v in (overrides or {}).items()}

    @staticmethod
    def snake(name: str) -> str:
        out = []
        for i, ch in enumerate(name):
            if ch.isupper() and i > 0 and (not name[i - 1].isupper() or (i + 1 < len(name) and name[i + 1].islower())):
                out.append("_")
            out.append(ch.lower())
        return "".join(out)

    def resolve(self, schemaName: str, tableName: str) -> str:
        key = f"{schemaName}.{tableName}".lower()
        if key in self.overrides:
            return self.overrides[key]
        if schemaName.lower() in self.OWNED_SCHEMAS:
            return self.cf.t(f"{schemaName.lower()}_{self.snake(tableName)}")
        return self.cf.cfg.legacyStaging(schemaName, tableName)

    def resolveObject(self, objectName: str) -> str:
        schemaName, _, tableName = objectName.replace("[", "").replace("]", "").partition(".")
        return self.resolve(schemaName, tableName)


def _fail(cf: ControlFramework, batchId: int, gateName: str, detail: str) -> None:
    cf.logError(batchId, f"{gateName}: {detail}", severity="Error", sourceComponent=gateName)
    raise GateFailure(f"{gateName}: {detail}")


def _warn(cf: ControlFramework, batchId: int, gateName: str, detail: str) -> None:
    cf.logError(batchId, f"{gateName}: {detail}", severity="Warning", sourceComponent=gateName)


def applyGates(cf: ControlFramework, batchId: int, fired: list[tuple[str, str]], detail: str) -> list[str]:
    for gateName, severity in fired:
        if severity == "Failure":
            _fail(cf, batchId, gateName, detail)
        _warn(cf, batchId, gateName, detail)
    return [g for g, _s in fired]


def recordMeasure(
    cf: ControlFramework, batchId: int, packageExecutionId: int | None, objectName: str, measureCode: str,
    measured: Decimal | int | None, threshold: Decimal | int | None, rowsEvaluated: int | None = None,
    regionCode: str | None = None, detail: str | None = None,
) -> str:
    status = dqResultStatus(None if measured is None else Decimal(str(measured)), Decimal(str(threshold or 0)), "WARN")
    cf.insertRows(
        "etl_data_quality_result",
        [
            {
                "batch_id": batchId,
                "package_execution_id": packageExecutionId,
                "object_name": objectName,
                "rule_code": measureCode,
                "measured_value": None if measured is None else Decimal(str(measured)),
                "threshold_value": None if threshold is None else Decimal(str(threshold)),
                "rows_evaluated": rowsEvaluated,
                "result_status": status,
                "region_code": regionCode,
                "detail_text": detail,
                "evaluated_at_utc": utcNow(),
            }
        ],
    )
    return status


# ------------------------------------------------------------------------------ DQ_Rule_Engine


def runRuleEngine(
    cf: ControlFramework,
    batchId: int,
    packageExecutionId: int | None = None,
    ruleGroupCode: str | None = None,
    objectName: str | None = None,
    businessDate: date | None = None,
    resolver: ObjectResolver | None = None,
    applyGateSeverity: bool = True,
) -> dict:
    """etl.usp_EvaluateDataQualityRules + the DQ_Rule_Engine gates."""
    resolver = resolver or ObjectResolver(cf)
    businessDate = businessDate or utcNow().date()
    where = ["is_active"]
    if ruleGroupCode and ruleGroupCode.upper() != "ALL":
        where.append(f"rule_group_code = {sqlLiteral(ruleGroupCode)}")
    if objectName and objectName.upper() != "ALL":
        where.append(f"object_name = {sqlLiteral(objectName)}")
    rules = cf.spark.sql(f"SELECT * FROM {cf.t('etl_data_quality_rule')} WHERE {' AND '.join(where)} ORDER BY rule_group_code, rule_code").collect()

    # Rule inventory rows (first data flow of the package): one summary row per group/severity.
    inventory = cf.spark.sql(
        f"SELECT rule_group_code, severity_code, COUNT(*) AS rule_count, AVG(threshold_value) AS average_threshold "
        f"FROM {cf.t('etl_data_quality_rule')} WHERE {' AND '.join(where)} GROUP BY rule_group_code, severity_code"
    ).collect()
    now = utcNow()
    cf.insertRows(
        "etl_data_quality_result",
        [
            {
                "batch_id": batchId, "package_execution_id": packageExecutionId, "object_name": "etl.DataQualityRule",
                "rule_code": f"INVENTORY_{i['rule_group_code']}_{i['severity_code']}",
                "measured_value": Decimal(i["rule_count"]), "threshold_value": Decimal(str(i["average_threshold"] or 0)),
                "rows_evaluated": i["rule_count"], "result_status": "Passed", "detail_text": "Rule inventory", "evaluated_at_utc": now,
            }
            for i in inventory
        ],
    )

    exceptions = cf.spark.sql(
        f"SELECT rule_code, object_name, region_code FROM {cf.t('etl_data_quality_rule_exception')} "
        f"WHERE {sqlLiteral(businessDate)} BETWEEN effective_from AND effective_to"
    ).collect()
    rowCountCache: dict[str, int | None] = {}
    results = []
    for rule in rules:
        target = resolver.resolveObject(rule["object_name"])
        expression = rule["spark_expression"]
        measured: Decimal
        detail = None
        rowsEvaluated = None
        try:
            if not expression or expression.startswith("/* untranslatable"):
                raise ValueError("rule expression is not translated")
            if target not in rowCountCache:
                rowCountCache[target] = int(cf.spark.sql(f"SELECT COUNT(*) AS c FROM {target}").first()["c"])
            rowsEvaluated = rowCountCache[target]
            measured = Decimal(int(cf.spark.sql(f"SELECT COUNT(*) AS c FROM {target} AS t WHERE {expression}").first()["c"]))
        except Exception as exc:  # noqa: BLE001 - a bad steward rule is NotEvaluated, not a batch failure
            measured = Decimal(-1)
            detail = f"Rule {rule['rule_code']} could not be evaluated: {str(exc)[:900]}"
            cf.logError(batchId, detail, severity="Warning", packageExecutionId=packageExecutionId, procedureName="etl.usp_EvaluateDataQualityRules")
        hasException = any(
            x["rule_code"] == rule["rule_code"]
            and (x["object_name"] is None or x["object_name"] == rule["object_name"])
            and (x["region_code"] is None or x["region_code"] == rule["region_code"])
            for x in exceptions
        )
        status = dqResultStatus(measured, rule["threshold_value"], rule["severity_code"], hasException)
        results.append(
            {
                "batch_id": batchId, "package_execution_id": packageExecutionId, "object_name": rule["object_name"],
                "rule_code": rule["rule_code"], "measured_value": measured, "threshold_value": rule["threshold_value"],
                "rows_evaluated": rowsEvaluated, "result_status": status, "region_code": rule["region_code"],
                "detail_text": detail or f"{target}: {expression}", "evaluated_at_utc": utcNow(),
            }
        )
    if results:
        cf.insertRows("etl_data_quality_result", results)
    failedRuleCount = sum(1 for r in results if r["measured_value"] is not None and r["measured_value"] > Decimal(str(r["threshold_value"] or 0)))
    statusCounts = {}
    for r in results:
        statusCounts[r["result_status"]] = statusCounts.get(r["result_status"], 0) + 1
    gates = applyGates(cf, batchId, ruleEngineGates(failedRuleCount) if applyGateSeverity else [], f"{failedRuleCount} rule(s) over threshold")
    return {"rulesEvaluated": len(results), "failedRuleCount": failedRuleCount, "statusCounts": statusCounts, "gates": gates}


# ------------------------------------------------------------------------------ DQ_Threshold_Gate


def thresholdGate(cf: ControlFramework, batchId: int, packageExecutionId: int | None = None, businessDate: date | None = None) -> dict:
    from platform_control.errors import reconcileRowCounts

    audit = cf.t("etl_row_count_audit")
    totals = cf.spark.sql(
        f"SELECT COALESCE(SUM(source_row_count), 0) AS source_rows, COALESCE(SUM(reject_row_count), 0) AS reject_rows "
        f"FROM {audit} WHERE batch_id = {batchId}"
    ).first()
    rejectRate = rejectPercent(int(totals["source_rows"]), int(totals["reject_rows"]))
    recordMeasure(cf, batchId, packageExecutionId, "BATCH", "DQ_BATCH_REJECT_RATE", rejectRate, Decimal("2"), int(totals["source_rows"]))

    perObject = cf.spark.sql(
        f"SELECT object_name, COALESCE(SUM(source_row_count),0) AS source_row_count, COALESCE(SUM(target_row_count),0) AS target_row_count, "
        f"COALESCE(SUM(reject_row_count),0) AS reject_row_count, COALESCE(SUM(variance_row_count),0) AS variance_row_count "
        f"FROM {audit} WHERE batch_id = {batchId} GROUP BY object_name"
    ).collect()
    now = utcNow()
    balanced, breaches = [], []
    for o in perObject:
        variancePct, status = controlTotalStatus(int(o["source_row_count"]), int(o["variance_row_count"]))
        if status == "BREACH":
            breaches.append(
                {
                    "batch_id": batchId, "package_execution_id": packageExecutionId, "target_object_name": o["object_name"],
                    "constraint_name": "CONTROL_TOTAL", "constraint_type_code": "RECONCILIATION", "violating_business_key": o["object_name"],
                    "violating_column_name": "variance_row_count", "violating_value": str(o["variance_row_count"]),
                    "reject_reason_code": "DQ_RECON_BREACH", "reject_reason": f"Control total variance {variancePct}% exceeds tolerance",
                    "reject_stage": "Quality", "reprocess_status_code": "Pending", "rejected_at_utc": now,
                }
            )
        else:
            balanced.append(
                {
                    "batch_id": batchId, "reconciliation_name": "CONTROL_TOTAL", "object_name": o["object_name"],
                    "source_amount": Decimal(int(o["source_row_count"])), "target_amount": Decimal(int(o["target_row_count"])),
                    "variance_amount": Decimal(int(o["variance_row_count"])), "variance_status": status, "evaluated_at_utc": now,
                }
            )
    if balanced:
        cf.insertRows("etl_reconciliation_result", balanced)
    if breaches:
        cf.insertRows("err_rejected_constraint_violation", breaches)

    failedObjectCount = reconcileRowCounts(cf, batchId, raiseOnFailure=False)["failedObjectCount"] + len(breaches)
    measures = cf.spark.sql(
        f"SELECT r.measured_value, r.threshold_value FROM {cf.t('etl_data_quality_result')} AS r "
        f"JOIN {cf.t('etl_data_quality_rule')} AS d ON d.rule_code = r.rule_code WHERE r.batch_id = {batchId} AND r.measured_value >= 0"
    ).collect()
    score = qualityScore([(m["measured_value"], m["threshold_value"]) for m in measures])
    recordMeasure(cf, batchId, packageExecutionId, "BATCH", "DQ_SCORECARD", Decimal(100) - score, Decimal("10"), len(measures), detail=f"quality_score={score}")
    gates = applyGates(cf, batchId, thresholdGateOutcome(rejectRate, failedObjectCount, score), f"reject_rate={rejectRate}% failed_objects={failedObjectCount} quality_score={score}")
    return {"rejectRatePercent": rejectRate, "failedObjectCount": failedObjectCount, "qualityScore": score, "controlTotals": len(perObject), "breaches": len(breaches), "gates": gates}


# ------------------------------------------------------------------------------ DQ_Referential_Screen

REFERENTIAL_SOURCES = {
    "stg.OrderLine": ("stg", "OrderLine"), "stg.StockItem": ("stg", "StockItem"), "ref.PackageType": ("ref", "PackageType"),
    "stg.SaleLine": ("stg", "SaleLine"), "stg.Sale": ("stg", "Sale"), "ref.Currency": ("ref", "Currency"),
    "stg.SalesTerritory": ("stg", "SalesTerritory"),
}


def _legacySources(cf: ControlFramework, overrides: dict[str, DataFrame] | None) -> dict[str, DataFrame]:
    sources = dict(overrides or {})
    for key, (schemaName, tableName) in REFERENTIAL_SOURCES.items():
        if key not in sources:
            sources[key] = cf.spark.table(cf.cfg.legacyStaging(schemaName, tableName))
    return sources


def _firstPresent(df: DataFrame, *candidates: list[str]) -> list[str]:
    """First candidate column list fully present in df (legacy business-key names first), else the last one."""
    for cols in candidates:
        if all(c in df.columns for c in cols):
            return cols
    return candidates[-1]


def _orphans(source: DataFrame, lookup: DataFrame, sourceCol: str, lookupCol: str, keyCols: list[str], lookupName: str, sourceObject: str, reasonCode: str) -> DataFrame:
    matched = lookup.select(F.col(lookupCol).alias("__lk")).dropna().distinct()
    joined = source.join(matched, source[sourceCol] == matched["__lk"], "left_anti")
    return joined.select(
        F.lit(sourceObject).alias("source_object_name"),
        F.concat_ws("|", *[F.col(c).cast("string") for c in keyCols]).alias("source_business_key"),
        F.lit(lookupName).alias("lookup_name"),
        F.lit(sourceCol).alias("lookup_column_name"),
        F.col(sourceCol).cast("string").alias("lookup_value"),
        F.lit(reasonCode).alias("reject_reason_code"),
        F.lit(f"{sourceCol} not found in {lookupName}").alias("reject_reason"),
        F.to_json(F.struct(*[F.col(c) for c in source.columns])).alias("record_payload"),
    )


def referentialScreen(
    cf: ControlFramework, batchId: int, packageExecutionId: int | None = None, sources: dict[str, DataFrame] | None = None,
    orphanWarnThreshold: int = 0, orphanFailThreshold: int = 1000,
) -> dict:
    src = _legacySources(cf, sources)
    orderLine, saleLine, sale = src["stg.OrderLine"], src["stg.SaleLine"], src["stg.Sale"]
    # legacy stg.* carries business keys (OrderLineBusinessKey, StockItemBusinessKey, SaleBusinessKey); the
    # OLTP-style *Id names are accepted so the same screen runs over raw extracts and test frames
    olKeys = _firstPresent(orderLine, ["OrderLineBusinessKey"], ["OrderId", "OrderLineId"])
    slKeys = _firstPresent(saleLine, ["SaleLineBusinessKey"], ["InvoiceId", "InvoiceLineId"])
    stockItemCol = _firstPresent(orderLine, ["StockItemBusinessKey"], ["StockItemId"])[0]
    saleKey = _firstPresent(saleLine, ["SaleBusinessKey"], ["InvoiceId"])[0]
    currencyCol = _firstPresent(sale, ["TransactionCurrencyCode"], ["SaleCurrencyCode"])[0]
    territoryCol = _firstPresent(sale, ["SalesTerritoryCode"], [])
    frames = [
        _orphans(orderLine, src["stg.StockItem"], stockItemCol, stockItemCol, olKeys, "StockItem", "stg.OrderLine", "DQ_REF_ORDERLINE"),
        _orphans(orderLine, src["ref.PackageType"], "PackageTypeCode", "PackageTypeCode", olKeys, "PackageType", "stg.OrderLine", "DQ_REF_ORDERLINE"),
    ]
    saleCols = [saleKey, currencyCol] + territoryCol
    saleJoined = saleLine.join(sale.select(*saleCols), saleKey, "left") if saleKey in saleLine.columns else saleLine
    frames.append(_orphans(saleJoined, src["ref.Currency"], currencyCol, "CurrencyCode", slKeys, "Currency", "stg.SaleLine", "DQ_REF_SALELINE"))
    if territoryCol:
        frames.append(_orphans(saleJoined, src["stg.SalesTerritory"], territoryCol[0], "SalesTerritoryCode", slKeys, "SalesTerritory", "stg.SaleLine", "DQ_REF_SALELINE"))
    orphans = frames[0]
    for f in frames[1:]:
        orphans = orphans.unionByName(f)
    # one reject row per (source key, lookup): repeated occurrences are counted, not duplicated
    deduped = orphans.groupBy("source_object_name", "source_business_key", "lookup_name", "lookup_column_name", "lookup_value", "reject_reason_code", "reject_reason").agg(
        F.count("*").cast("int").alias("occurrence_count"), F.first("record_payload").alias("record_payload")
    )
    now = utcNow()
    rejects = deduped.select(
        F.lit(None).cast("bigint").alias("reject_id"), F.lit(batchId).cast("bigint").alias("batch_id"),
        F.lit(packageExecutionId).cast("bigint").alias("package_execution_id"), "source_object_name", "source_business_key", "lookup_name",
        "lookup_column_name", "lookup_value", F.lit("STAGING").alias("source_system_code"), "reject_reason_code", "reject_reason",
        F.lit("Referential").alias("reject_stage"), F.lit(False).alias("routed_to_unknown_member"), F.lit(True).alias("queued_for_late_arrival"),
        "occurrence_count", "record_payload", F.lit("Pending").alias("reprocess_status_code"), F.lit(0).alias("reprocess_attempt_count"),
        F.lit(None).cast("timestamp").alias("reprocessed_at_utc"), F.lit(None).cast("bigint").alias("reprocessed_by_execution_id"),
        F.lit(now).alias("rejected_at_utc"),
    )
    orphanRows = [r.asDict() for r in rejects.collect()]
    cf.insertRows("err_rejected_lookup_failure", orphanRows)
    orphanCount = len(orphanRows)
    for objectName, df in (("stg.OrderLine", orderLine), ("stg.SaleLine", saleLine)):
        evaluated = df.count()
        objectOrphans = sum(1 for r in orphanRows if r["source_object_name"] == objectName)
        recordMeasure(cf, batchId, packageExecutionId, objectName, "DQ_REF_ORPHANS", objectOrphans, 0, evaluated)
    ruleOutcome = runRuleEngine(cf, batchId, packageExecutionId, ruleGroupCode="REFERENTIAL", applyGateSeverity=False)
    fired = []
    if orphanCount > orphanWarnThreshold:
        fired.append(("Warn On Referential Orphans", "Warning"))
    if orphanCount > orphanFailThreshold or ruleOutcome["failedRuleCount"] > 0:
        fired.append(("Fail On Referential Breach", "Failure"))
    gates = applyGates(cf, batchId, fired, f"orphans={orphanCount} failed_rules={ruleOutcome['failedRuleCount']}")
    return {"orphanCount": orphanCount, "failedRuleCount": ruleOutcome["failedRuleCount"], "gates": gates}


# ------------------------------------------------------------------------------ DQ_Reject_Reprocess


def rejectReprocess(cf: ControlFramework, batchId: int, packageExecutionId: int | None = None, stockItems: DataFrame | None = None, now: datetime | None = None) -> dict:
    now = now or utcNow()
    stockItemDf = stockItems if stockItems is not None else cf.spark.table(cf.cfg.legacyStaging("stg", "StockItem"))
    stockItemCol = _firstPresent(stockItemDf, ["StockItemBusinessKey"], ["StockItemId"])[0]
    knownStockItems = {str(r[0]) for r in stockItemDf.select(stockItemCol).dropna().distinct().collect()}
    candidates = cf.spark.sql(
        f"SELECT reject_id, source_business_key, lookup_column_name, lookup_value, reprocess_attempt_count, rejected_at_utc, record_payload, package_execution_id "
        f"FROM {cf.t('err_rejected_lookup_failure')} WHERE lookup_name = 'StockItem' "
        f"AND COALESCE(reprocess_status_code, 'Pending') IN ('Pending', 'Unresolved') AND COALESCE(reprocess_attempt_count, 0) < 5"
    ).collect()
    counts = {"REPLAY": 0, "AGED_OUT": 0, "UNRESOLVED": 0, "EXHAUSTED": 0}
    replayRows = []
    statusFor = {"REPLAY": "Reprocessed", "AGED_OUT": "Abandoned", "UNRESOLVED": "Unresolved", "EXHAUSTED": "Exhausted"}
    for c in candidates:
        stockItemKey = c["lookup_value"]
        if stockItemKey is None:
            legacyId = stockItemIdFromBusinessKey(c["source_business_key"])
            stockItemKey = None if legacyId is None else str(legacyId)
        decision = rejectReprocessDecision(c["reprocess_attempt_count"], c["rejected_at_utc"], now, stockItemKey in knownStockItems)
        # replay rows keep the numeric OLTP id when the business key carries one (TOKEN(key, "|", 2) in the SSIS derived column)
        stockItemId = int(stockItemKey) if stockItemKey and stockItemKey.isdigit() else stockItemIdFromBusinessKey(stockItemKey)
        counts[decision] += 1
        assignments = {"reprocess_status_code": statusFor[decision], "reprocess_attempt_count": int(c["reprocess_attempt_count"] or 0) + 1}
        if decision == "REPLAY":
            assignments.update({"reprocessed_at_utc": now, "reprocessed_by_execution_id": packageExecutionId})
            replayRows.append(
                {"reject_id": c["reject_id"], "batch_id": batchId, "package_execution_id": packageExecutionId, "source_business_key": c["source_business_key"],
                 "stock_item_id": stockItemId, "record_payload": c["record_payload"], "replayed_at_utc": now}
            )
        cf.update("err_rejected_lookup_failure", assignments, f"reject_id = {c['reject_id']}")
    if replayRows:
        cf.insertRows("stg_order_line_replay", replayRows)
    recordMeasure(cf, batchId, packageExecutionId, "err.RejectedLookupFailure", "DQ_REPROCESS_REPLAYED", counts["REPLAY"], 0, len(candidates))
    ruleOutcome = runRuleEngine(cf, batchId, packageExecutionId, ruleGroupCode="REPROCESS", applyGateSeverity=False)
    fired = [("Warn On Persistent Rejects", "Warning")] if (ruleOutcome["failedRuleCount"] > 0 or counts["UNRESOLVED"] > 0) else []
    gates = applyGates(cf, batchId, fired, f"unresolved={counts['UNRESOLVED']} failed_rules={ruleOutcome['failedRuleCount']}")
    return {"candidates": len(candidates), **counts, "gates": gates}


# ------------------------------------------------------------------------------ DQ_File_Screen

_SCREEN_SCHEMA = T.StructType(
    [
        T.StructField("delimiter_count", T.IntegerType()), T.StructField("well_formed", T.BooleanType()),
        T.StructField("reject_reason_code", T.StringType()),
    ]
)


@F.udf(returnType=_SCREEN_SCHEMA)
def _screenUdf(rawLine, saleDateText, amountText):
    screen = screenFileRow(rawLine, saleDateText, amountText)
    return (screen.delimiterCount, screen.wellFormed, screen.rejectReasonCode)


def partnerSalesRawView(df: DataFrame) -> DataFrame:
    """The package expects (FileRowId, FileLineNumber, RawLine, SaleDateText, AmountText) which the
    real raw.FilePartnerSales does not have; reconstruct them from the landed columns."""
    if "RawLine" in df.columns:
        return df
    fields = ["PartnerCode", "PartnerOutletCode", "ReportingPeriod", "TransactionReference", "TransactionDate", "PartnerProductCode", "QuantitySold", "NetAmount", "CurrencyCode"]
    present = [F.coalesce(F.col(c).cast("string"), F.lit("")) if c in df.columns else F.lit("") for c in fields]
    return df.select(
        F.col("SourceRowNumber").cast("bigint").alias("FileRowId") if "SourceRowNumber" in df.columns else F.monotonically_increasing_id().alias("FileRowId"),
        F.col("SourceRowNumber").cast("bigint").alias("FileLineNumber") if "SourceRowNumber" in df.columns else F.monotonically_increasing_id().alias("FileLineNumber"),
        F.concat_ws("|", *present).alias("RawLine"),
        (F.col("TransactionDate").cast("string") if "TransactionDate" in df.columns else F.lit(None).cast("string")).alias("SaleDateText"),
        (F.col("NetAmount").cast("string") if "NetAmount" in df.columns else F.lit(None).cast("string")).alias("AmountText"),
        (F.col("SourceFileName") if "SourceFileName" in df.columns else F.lit("raw.FilePartnerSales")).alias("SourceFileName"),
    )


def fileScreen(cf: ControlFramework, batchId: int, packageExecutionId: int | None = None, source: DataFrame | None = None, sourceSystemCode: str = "PARTNER_NA") -> dict:
    raw = source if source is not None else cf.spark.table(cf.cfg.legacyStaging("raw", "FilePartnerSales"))
    rows = partnerSalesRawView(raw).withColumn("screen", _screenUdf("RawLine", "SaleDateText", "AmountText"))
    rows = rows.select("*", "screen.*").drop("screen")
    total = rows.count()
    malformed = rows.filter(~F.col("well_formed"))
    now = utcNow()
    rejectRows = [
        {
            "batch_id": batchId, "package_execution_id": packageExecutionId, "source_system_code": sourceSystemCode,
            "source_file_name": r["SourceFileName"], "file_format_version": "v1", "source_row_number": r["FileLineNumber"], "raw_row_text": r["RawLine"],
            "expected_column_count": FILE_PARTNER_SALES_EXPECTED_COLUMNS, "actual_column_count": int(r["delimiter_count"]) + 1, "delimiter_used": "|",
            "decimal_separator_used": ".", "date_format_assumed": "yyyy-MM-dd", "reject_reason_code": r["reject_reason_code"],
            "reject_reason": {"DQ_FILE_DELIMITER": "Delimiter count breach", "DQ_FILE_DATE": "Unparsable sale date", "DQ_FILE_AMOUNT": "Unparsable amount / encoding"}.get(r["reject_reason_code"], "Malformed row"),
            "reject_stage": "Extract", "reprocess_status_code": "Pending", "reprocess_attempt_count": 0, "rejected_at_utc": now,
        }
        for r in malformed.collect()
    ]
    cf.insertRows("err_rejected_file_row", rejectRows)
    malformedCount = len(rejectRows)
    malformedRate = rejectPercent(total, malformedCount)
    recordMeasure(cf, batchId, packageExecutionId, "raw.FilePartnerSales", "DQ_FILE_MALFORMED_RATE", malformedRate, Decimal("1"), total)
    ruleOutcome = runRuleEngine(cf, batchId, packageExecutionId, ruleGroupCode="FILEROW", applyGateSeverity=False)
    gates = applyGates(cf, batchId, fileScreenGates(malformedRate, ruleOutcome["failedRuleCount"]), f"malformed_rate={malformedRate}% failed_rules={ruleOutcome['failedRuleCount']}")
    rows.unpersist()
    return {"rowsScreened": total, "malformedCount": malformedCount, "malformedRatePercent": malformedRate, "failedRuleCount": ruleOutcome["failedRuleCount"], "gates": gates}


# ------------------------------------------------------------------------------ ING_FILE_QuarantineMalformed


def quarantineSweep(cf: ControlFramework, batchId: int, packageExecutionId: int | None, fileOps: FileOps, quarantineFolderName: str = "quarantine") -> dict:
    folder = fileOps.join(quarantineFolderName)
    poisonFolder = fileOps.join(quarantineFolderName, "poison")
    archiveFolder = fileOps.join("archive", quarantineFolderName, utcNow().strftime("%Y%m"))
    now = utcNow()
    files = fileOps.listFiles(folder)
    counts = {"filesSeen": len(files), "rowsRecorded": 0, "replayableRows": 0, "archivedFiles": 0, "poisonFiles": 0}
    for info in files:
        try:
            lines = fileOps.readLines(info.path)
        except Exception as exc:  # noqa: BLE001 - unreadable -> poison, never deleted
            fileOps.move(info.path, poisonFolder)
            counts["poisonFiles"] += 1
            cf.logError(batchId, f"Unreadable quarantine file {info.name}: {exc}", severity="Warning", packageExecutionId=packageExecutionId, sourceName="ING_FILE_QuarantineMalformed")
            continue
        feed = inferOriginFeed(info.name)
        rows, replayable = [], 0
        for lineNumber, line in enumerate(lines, start=1):
            reasonCode, replayEligible = classifyQuarantineRow(line)
            replayable += replayEligible
            rows.append(
                {
                    "batch_id": batchId, "package_execution_id": packageExecutionId, "source_system_code": feed, "source_file_name": info.name,
                    "file_format_version": "quarantine", "source_row_number": lineNumber, "raw_row_text": line, "expected_column_count": None,
                    "actual_column_count": line.count("|") + 1 if line.strip() else 0, "delimiter_used": "|", "decimal_separator_used": None,
                    "date_format_assumed": None, "reject_reason_code": reasonCode,
                    "reject_reason": "Blank line in quarantined file" if reasonCode == "EMPTY_LINE" else f"Row quarantined from {feed} feed",
                    "reject_stage": "Quarantine", "reprocess_status_code": "ReplayEligible" if replayEligible else "NotReplayable",
                    "reprocess_attempt_count": 0, "rejected_at_utc": now,
                }
            )
        cf.insertRows("err_rejected_file_row", rows)
        counts["rowsRecorded"] += len(rows)
        counts["replayableRows"] += replayable
        if replayable > 0:
            fileOps.move(info.path, archiveFolder)
            counts["archivedFiles"] += 1
        else:
            fileOps.move(info.path, poisonFolder)
            counts["poisonFiles"] += 1
    cf.logRowCount(packageExecutionId, batchId, "err.RejectedFileRow", "Target", sourceRowCount=counts["rowsRecorded"], targetRowCount=counts["rowsRecorded"], insertRowCount=counts["rowsRecorded"])
    return counts
