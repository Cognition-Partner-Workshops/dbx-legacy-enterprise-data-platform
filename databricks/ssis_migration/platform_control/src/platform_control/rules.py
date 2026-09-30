"""Pure (Spark-free) business rules lifted from the SSIS packages and the etl.usp_* procedures.

Everything here is deterministic and unit-tested locally; the Spark modules call these
functions so the semantics live in exactly one place.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

# --------------------------------------------------------------------------------------
# ERR_Handle_PackageFailure / ERR_Retry_FailedSteps
# --------------------------------------------------------------------------------------

# "The transient list is the one operations built up over the years": deadlocks, lock
# timeouts, connection resets, Oracle listener refusals and the file share dropping out.
RETRYABLE_ERROR_CODES = frozenset({1205, 1222, 10054, 10060, 12154, 12541, 64, 121})

TRANSIENT_MESSAGE_MARKERS = (
    "deadlock",
    "lock request time out",
    "connection reset",
    "connection timed out",
    "tns:",
    "listener",
    "network name is no longer available",
    "semaphore timeout",
    "temporarily unavailable",
    "throttl",
    "rate limit",
    "429",
    "503",
)


def classifyFailure(errorCode: int | None, errorMessage: str | None = None) -> tuple[str, bool]:
    """Return (FailureClass, RetryableFlag) exactly as the Classify Failure task did.

    Databricks exceptions carry no SQL Server error number, so a message-marker fallback
    maps the same operational categories (deadlock, reset, listener, share drop) to TRANSIENT.
    """
    if errorCode is not None and int(errorCode) in RETRYABLE_ERROR_CODES:
        return "TRANSIENT", True
    if errorMessage:
        lowered = errorMessage.lower()
        if any(marker in lowered for marker in TRANSIENT_MESSAGE_MARKERS):
            return "TRANSIENT", True
    return "PERMANENT", False


def computeBackoffSeconds(backoffBaseSeconds: int, attemptNumber: int) -> int:
    """Compute Backoff: base * (attempt + 1)."""
    return int(backoffBaseSeconds) * (int(attemptNumber) + 1)


def retryDecision(attemptNumber: int, maxRetryAttempts: int, retryableStepCount: int) -> str:
    """Expression-and-constraint retry from ERR_Retry_FailedSteps."""
    if attemptNumber <= maxRetryAttempts and retryableStepCount > 0:
        return "sweep"
    return "exhausted"


# --------------------------------------------------------------------------------------
# etl.usp_EndBatch
# --------------------------------------------------------------------------------------


def deriveBatchStatus(
    failedPackages: int, runningPackages: int, warningCount: int, forceStatus: str | None = None
) -> str:
    if forceStatus:
        return forceStatus
    if (failedPackages or 0) > 0:
        return "Failed"
    if (runningPackages or 0) > 0:
        return "Failed"
    if (warningCount or 0) > 0:
        return "SucceededWithWarnings"
    return "Succeeded"


# --------------------------------------------------------------------------------------
# ERR_Reconcile_RowCounts / etl.usp_AssertRowCountReconciliation
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RowCountReconciliation:
    expectedTargetRowCount: int
    differenceRowCount: int
    differencePercent: Decimal
    reconciliationStatus: str


def reconcileRowCounts(
    stagingRowCount: int,
    targetRowCount: int,
    rejectedRowCount: int,
    tolerancePercent: Decimal | float | int,
    explanationCode: str | None,
) -> RowCountReconciliation:
    expected = int(stagingRowCount or 0) - int(rejectedRowCount or 0)
    difference = int(targetRowCount or 0) - expected
    if not stagingRowCount:
        pct = Decimal("0.00")
    else:
        pct = (Decimal(abs(difference)) * 100 / Decimal(int(stagingRowCount))).quantize(Decimal("0.01"))
    if difference == 0:
        status = "MATCHED"
    elif explanationCode:
        status = "EXPLAINED"
    elif pct <= Decimal(str(tolerancePercent or 0)):
        status = "TOLERATED"
    else:
        status = "FAILED"
    return RowCountReconciliation(expected, difference, pct, status)


# --------------------------------------------------------------------------------------
# DQ_Threshold_Gate
# --------------------------------------------------------------------------------------

THRESHOLD_GATE_WARN_REJECT_PCT = Decimal("2")
THRESHOLD_GATE_FAIL_REJECT_PCT = Decimal("5")
THRESHOLD_GATE_MIN_QUALITY_SCORE = Decimal("90")
THRESHOLD_GATE_VARIANCE_TOLERANCE_PCT = Decimal("0.5")


def controlTotalStatus(sourceRowCount: int, varianceRowCount: int) -> tuple[Decimal, str]:
    """Compute Variance Percent / ReconciliationStatusCode from the threshold gate flow."""
    if not sourceRowCount:
        return Decimal("0"), "EMPTY"
    variancePct = (Decimal(varianceRowCount) * 100 / Decimal(sourceRowCount)).quantize(Decimal("0.0001"))
    if varianceRowCount == 0:
        return variancePct, "BALANCED"
    if abs(variancePct) <= THRESHOLD_GATE_VARIANCE_TOLERANCE_PCT:
        return variancePct, "WITHIN_TOL"
    return variancePct, "BREACH"


def rejectPercent(sourceRowCount: int, rejectRowCount: int) -> Decimal:
    if not sourceRowCount:
        return Decimal("0")
    return (Decimal(rejectRowCount or 0) * 100 / Decimal(sourceRowCount)).quantize(Decimal("0.0001"))


def qualityScore(measuredVsThreshold: list[tuple[Decimal | None, Decimal | None]]) -> Decimal:
    """Publish Quality Scorecard: 100 - AVG(100 if measured > threshold else 0)."""
    if not measuredVsThreshold:
        return Decimal("100.0000")
    failures = sum(
        1 for measured, threshold in measuredVsThreshold
        if measured is not None and threshold is not None and Decimal(measured) > Decimal(threshold)
    )
    avg = Decimal(failures) * 100 / Decimal(len(measuredVsThreshold))
    return (Decimal(100) - avg).quantize(Decimal("0.0001"))


def thresholdGateOutcome(rejectRatePct: Decimal, failedObjectCount: int, score: Decimal) -> list[tuple[str, str]]:
    """Return the (gate name, severity) list of gates that fire, in package order."""
    fired = []
    if rejectRatePct > THRESHOLD_GATE_WARN_REJECT_PCT:
        fired.append(("Warn On Reject Rate", "Warning"))
    if rejectRatePct > THRESHOLD_GATE_FAIL_REJECT_PCT:
        fired.append(("Fail On Reject Rate", "Failure"))
    if failedObjectCount > 0:
        fired.append(("Fail On Reconciliation Breach", "Failure"))
    if score < THRESHOLD_GATE_MIN_QUALITY_SCORE:
        fired.append(("Fail On Low Quality Score", "Failure"))
    return fired


# --------------------------------------------------------------------------------------
# DQ_Rule_Engine / etl.usp_EvaluateDataQualityRules
# --------------------------------------------------------------------------------------

NOT_EVALUATED_SENTINEL = Decimal("-1")


def dqResultStatus(
    measured: Decimal | None, threshold: Decimal | None, severityCode: str | None, hasException: bool = False
) -> str:
    if measured is None or Decimal(measured) < 0:
        return "NotEvaluated"
    if Decimal(measured) <= Decimal(threshold or 0):
        return "Passed"
    if hasException:
        return "Warned"
    if (severityCode or "WARN").strip().upper() == "FAIL":
        return "Failed"
    return "Warned"


RULE_ENGINE_WARN_FAILED_RULES = 0
RULE_ENGINE_BLOCKING_FAILED_RULES = 10


def ruleEngineGates(failedRuleCount: int) -> list[tuple[str, str]]:
    fired = []
    if failedRuleCount > RULE_ENGINE_WARN_FAILED_RULES:
        fired.append(("Warn On Failing Rules", "Warning"))
    if failedRuleCount > RULE_ENGINE_BLOCKING_FAILED_RULES:
        fired.append(("Fail On Blocking Rule Set", "Failure"))
    return fired


_OBJECT_REF = re.compile(r"\b(stg|ref|err|raw|work|etl)\.\[?(\w+)\]?(\.)?", re.IGNORECASE)


_UNSAFE_RULE_SQL = re.compile(
    r";|--|/\*|\b(DROP|DELETE|INSERT|UPDATE|MERGE|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|EXEC|EXECUTE|xp_\w+|sp_\w+)\b",
    re.IGNORECASE,
)


def translateRuleExpression(tsqlExpression: str, mainObject: str, resolveObject, mainAlias: str = "t") -> str:
    """Translate a T-SQL predicate fragment from etl.DataQualityRule into Spark SQL.

    The stewards' rules are simple predicates; the translation is a fixed, reviewed set of
    rewrites (no arbitrary SQL is generated). ``resolveObject(schema, table)`` returns the
    fully qualified table that a ``schema.table`` reference should read from.
    """
    expr = tsqlExpression
    if _UNSAFE_RULE_SQL.search(expr):
        raise ValueError(f"DataQualityRule expression contains statement syntax and is refused: {tsqlExpression!r}")

    def replaceObject(match: re.Match) -> str:
        schemaName, tableName, trailingDot = match.group(1), match.group(2), match.group(3)
        legacyName = f"{schemaName}.{tableName}"
        if trailingDot:
            # correlated reference such as stg.OrderLine.OrderBusinessKey -> t.OrderBusinessKey
            if legacyName.lower() == mainObject.lower():
                return f"{mainAlias}."
            return f"{resolveObject(schemaName, tableName)}."
        return resolveObject(schemaName, tableName)

    expr = _OBJECT_REF.sub(replaceObject, expr)
    expr = re.sub(r"\bN'", "'", expr)
    expr = re.sub(r"\bCOUNT_BIG\s*\(", "COUNT(", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bSYSUTCDATETIME\s*\(\s*\)", "current_timestamp()", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bGETUTCDATE\s*\(\s*\)", "current_timestamp()", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bGETDATE\s*\(\s*\)", "current_timestamp()", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bLTRIM\s*\(\s*RTRIM\s*\(([^()]*)\)\s*\)", r"trim(\1)", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bLEN\s*\(", "length(", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bISNULL\s*\(", "coalesce(", expr, flags=re.IGNORECASE)
    expr = re.sub(
        r"\bDATEADD\s*\(\s*DAY\s*,\s*([^,]+),\s*([^)]+)\)", r"date_add(\2, \1)", expr, flags=re.IGNORECASE
    )
    expr = re.sub(r"\bCAST\s*\(\s*([^()]+?)\s+AS\s+DATE\s*\)", r"CAST(\1 AS DATE)", expr, flags=re.IGNORECASE)
    return expr


def adaptBooleanComparisons(sparkExpression: str, booleanColumns: set[str]) -> str:
    """BIT columns arrive as BOOLEAN through federation; ``Flag = 0`` must compare as INT."""
    expr = sparkExpression
    for column in booleanColumns:
        expr = re.sub(rf"\b{re.escape(column)}\s*(=|<>|!=)\s*([01])\b", rf"CAST({column} AS INT) \1 \2", expr)
    return expr


# --------------------------------------------------------------------------------------
# DQ_File_Screen (raw.FilePartnerSales)
# --------------------------------------------------------------------------------------

FILE_SCREEN_EXPECTED_DELIMITERS = 8
FILE_SCREEN_WARN_MALFORMED_PCT = Decimal("1")
FILE_SCREEN_FAIL_MALFORMED_PCT = Decimal("25")
REPLACEMENT_CHARACTER = "\ufffd"


@dataclass(frozen=True)
class FileRowScreen:
    delimiterCount: int
    delimiterBreach: bool
    unparsableDate: bool
    unparsableAmount: bool
    highBit: bool
    rejectReasonCode: str | None

    @property
    def wellFormed(self) -> bool:
        return not (self.delimiterBreach or self.unparsableDate or self.unparsableAmount or self.highBit)


_FEED_DATE_FORMATS = ("%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S")


def parseFeedDate(text: str | None) -> date | None:
    """(DT_DBDATE) cast of the SSIS derived column: None when the text is not a real date."""
    candidate = (text or "").strip()
    for fmt in _FEED_DATE_FORMATS:
        try:
            return datetime.strptime(candidate, fmt).date()
        except ValueError:
            continue
    return None


def parseFeedAmount(text: str | None) -> Decimal | None:
    """(DT_NUMERIC) cast: thousands separators and currency signs tolerated, anything else is malformed."""
    candidate = (text or "").strip().replace(",", "").replace("$", "")
    if not candidate:
        return None
    try:
        return Decimal(candidate)
    except InvalidOperation:
        return None


def screenFileRow(rawLine: str | None, saleDateText: str | None, amountText: str | None) -> FileRowScreen:
    raw = rawLine or ""
    delimiterCount = raw.count("|")
    delimiterBreach = delimiterCount != FILE_SCREEN_EXPECTED_DELIMITERS
    unparsableDate = parseFeedDate(saleDateText) is None
    unparsableAmount = parseFeedAmount(amountText) is None
    highBit = REPLACEMENT_CHARACTER in raw
    if delimiterBreach:
        reason = "DQ_FILE_DELIMITER"
    elif unparsableDate:
        reason = "DQ_FILE_DATE"
    elif unparsableAmount or highBit:
        reason = "DQ_FILE_AMOUNT"
    else:
        reason = None
    return FileRowScreen(delimiterCount, delimiterBreach, unparsableDate, unparsableAmount, highBit, reason)


def fileScreenGates(malformedRatePct: Decimal, failedRuleCount: int) -> list[tuple[str, str]]:
    fired = []
    if malformedRatePct > FILE_SCREEN_WARN_MALFORMED_PCT:
        fired.append(("Warn On Malformed File Rows", "Warning"))
    if malformedRatePct > FILE_SCREEN_FAIL_MALFORMED_PCT or failedRuleCount > 0:
        fired.append(("Fail On File Structure Breach", "Failure"))
    return fired


# --------------------------------------------------------------------------------------
# ING_FILE_QuarantineMalformed
# --------------------------------------------------------------------------------------

_FEED_MARKERS = (
    ("partner_sales_na", "PARTNER_NA"),
    ("partner_sales_eu", "PARTNER_EU"),
    ("partner_sales_apac", "PARTNER_APAC"),
    ("carrier_scan", "CARRIER"),
    ("supplier_catalog", "SUPPLIER"),
    ("fx_override", "FX"),
)


def inferOriginFeed(fileName: str) -> str:
    lowered = (fileName or "").lower()
    for marker, code in _FEED_MARKERS:
        if marker in lowered:
            return code
    return "UNKNOWN"


def classifyQuarantineRow(rawLine: str | None) -> tuple[str, bool]:
    """Return (RejectReasonCode, ReplayEligibleFlag)."""
    text = rawLine or ""
    if len(text.strip()) == 0:
        return "EMPTY_LINE", False
    return "QUARANTINED", "\x00" not in text


# --------------------------------------------------------------------------------------
# ERR_Quarantine_BadFiles
# --------------------------------------------------------------------------------------


def classifyBadFile(fileSizeBytes: int | None, structuralCheckStatus: str | None, feedCode: str | None) -> str:
    if (fileSizeBytes or 0) == 0:
        return "ZERO_LENGTH"
    if structuralCheckStatus == "Failed":
        return "STRUCTURE"
    if feedCode is None:
        return "UNKNOWN_FEED"
    return "OTHER"


def quarantineFolder(quarantineRoot: str, asOf: datetime) -> str:
    """Build Quarantine Path: <root>/yyyyMM."""
    return f"{quarantineRoot.rstrip('/')}/{asOf.strftime('%Y%m')}"


# --------------------------------------------------------------------------------------
# ERR_Notify_Operations / ERR_Route_RejectedRows
# --------------------------------------------------------------------------------------


def notificationSeverity(failedStepCount: int, highCriticalityFailedCount: int) -> str:
    if highCriticalityFailedCount > 0:
        return "CRITICAL"
    if failedStepCount > 0:
        return "WARNING"
    return "INFO"


def isEscalated(ageDays: int, rejectEscalationDays: int) -> bool:
    return ageDays > rejectEscalationDays


def rejectFileName(batchId: int, asOf: date) -> str:
    return f"rejects_{batchId}_{asOf.strftime('%Y%m%d')}.csv"


# --------------------------------------------------------------------------------------
# DQ_Reject_Reprocess
# --------------------------------------------------------------------------------------

REPROCESS_MAX_RETRIES = 5
REPROCESS_ABANDON_AFTER_DAYS = 30


def rejectReprocessDecision(retryCount: int | None, firstRejectedAtUtc: datetime, now: datetime, resolves: bool) -> str:
    """REPLAY (resolved, replay into staging), AGED_OUT (>30 days), UNRESOLVED (still orphan)."""
    if (retryCount or 0) >= REPROCESS_MAX_RETRIES:
        return "EXHAUSTED"
    ageDays = (now - firstRejectedAtUtc).days
    if ageDays > REPROCESS_ABANDON_AFTER_DAYS:
        return "AGED_OUT"
    return "REPLAY" if resolves else "UNRESOLVED"


def stockItemIdFromBusinessKey(businessKey: str | None) -> int | None:
    """(DT_I4)TOKEN(BusinessKey, "|", 2) - the second pipe-delimited token."""
    if not businessKey:
        return None
    parts = businessKey.split("|")
    if len(parts) < 2:
        return None
    try:
        return int(parts[1])
    except ValueError:
        return None


# --------------------------------------------------------------------------------------
# Watermarks (etl.usp_GetWatermark / etl.usp_SetWatermark)
# --------------------------------------------------------------------------------------


def computeWatermarkWindow(lastValue: datetime | None, lookbackMinutes: int, now: datetime, floor: datetime) -> tuple[datetime, datetime]:
    """Incremental window: [lastValue - lookback, now]; first load starts at the floor."""
    start = floor if lastValue is None else lastValue - timedelta(minutes=lookbackMinutes or 0)
    return max(start, floor), now


# --------------------------------------------------------------------------------------
# MNT_* rules
# --------------------------------------------------------------------------------------


def diskSpaceStatus(freePercent: Decimal | float, availableGb: Decimal | float, projectedGb: Decimal | float, minimumFreePercent: int) -> str:
    freePercent = Decimal(str(freePercent))
    if freePercent < Decimal(minimumFreePercent) / 2:
        return "CRITICAL"
    if freePercent < Decimal(minimumFreePercent) or Decimal(str(availableGb)) < Decimal(str(projectedGb)):
        return "WARNING"
    return "OK"


def configurationCheckStatus(value: str | None) -> str:
    if value is None:
        return "MISSING"
    if value.strip() == "":
        return "EMPTY"
    return "OK"


def indexMaintenanceAction(fragmentationPercent: Decimal | float, reorganiseThresholdPercent: int, rebuildThresholdPercent: int) -> str:
    """REBUILD / REORGANISE / NONE - both map to OPTIMIZE on Delta, NONE is skipped."""
    pct = Decimal(str(fragmentationPercent))
    if pct >= Decimal(rebuildThresholdPercent):
        return "REBUILD"
    if pct >= Decimal(reorganiseThresholdPercent):
        return "REORGANISE"
    return "NONE"


def statisticsRefreshNeeded(modifiedRows: int, modificationThresholdRows: int) -> bool:
    return int(modifiedRows or 0) >= int(modificationThresholdRows)


def archiveEligible(fileModifiedAtUtc: datetime, now: datetime, minimumFileAgeHours: int) -> bool:
    return now - fileModifiedAtUtc >= timedelta(hours=minimumFileAgeHours)


def archiveExpired(archivedAtUtc: datetime, now: datetime, archiveRetentionDays: int) -> bool:
    return now - archivedAtUtc > timedelta(days=archiveRetentionDays)


# --------------------------------------------------------------------------------------
# SSIS precedence-constraint expressions (orchestration plan)
# --------------------------------------------------------------------------------------

_SSIS_VAR = re.compile(r"@\[(User|\$Package|\$Project)::(\w+)\]")


def _coerce(value):
    """Job parameters arrive as strings; give them the SSIS parameter's natural type."""
    if isinstance(value, str):
        text = value.strip()
        if text in ("True", "true"):
            return True
        if text in ("False", "false"):
            return False
        if re.fullmatch(r"-?\d+", text):
            return int(text)
        if re.fullmatch(r"-?\d+\.\d+", text):
            return float(text)
    return value


def evaluateSsisExpression(expression: str, variables: dict, parameters: dict) -> bool:
    """Evaluate a precedence-constraint expression such as
    ``@[User::ExtractAttempt] <= @[$Package::MaxExtractAttempts] && !@[$Package::Skip]``.

    Only boolean/comparison/arithmetic syntax is accepted (ast whitelist)."""
    names: dict[str, object] = {}

    def substitute(match: re.Match) -> str:
        scope, name = match.group(1), match.group(2)
        key = f"{'v' if scope == 'User' else 'p'}_{name}"
        source = variables if scope == "User" else parameters
        if name not in source:
            raise KeyError(f"{scope}::{name} is not defined")
        names[key] = _coerce(source[name])
        return key

    pyExpr = _SSIS_VAR.sub(substitute, expression)
    pyExpr = pyExpr.replace("&&", " and ").replace("||", " or ")
    pyExpr = re.sub(r"!(?!=)", " not ", pyExpr)
    tree = ast.parse(pyExpr.strip(), mode="eval")
    allowed = (
        ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.UnaryOp, ast.Not, ast.USub, ast.Compare,
        ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.Name, ast.Load, ast.Constant,
        ast.BinOp, ast.Add, ast.Sub, ast.Mult, ast.Div,
    )
    for node in ast.walk(tree):
        if not isinstance(node, allowed):
            raise ValueError(f"Unsupported syntax in SSIS expression: {expression!r}")
    return bool(eval(compile(tree, "<ssis-expression>", "eval"), {"__builtins__": {}}, names))  # noqa: S307


def evaluateSsisAssignment(assignment: str, variables: dict, parameters: dict) -> tuple[str, object]:
    """``@[User::ExtractAttempt] = @[User::ExtractAttempt] + 1`` -> ("ExtractAttempt", value)."""
    target, _, rhs = assignment.partition("=")
    match = _SSIS_VAR.fullmatch(target.strip())
    if not match or match.group(1) != "User":
        raise ValueError(f"Unsupported assignment target: {assignment!r}")
    names: dict[str, object] = {}

    def substitute(m: re.Match) -> str:
        scope, name = m.group(1), m.group(2)
        key = f"{'v' if scope == 'User' else 'p'}_{name}"
        source = variables if scope == "User" else parameters
        names[key] = _coerce(source.get(name, 0))
        return key

    pyExpr = _SSIS_VAR.sub(substitute, rhs.strip())
    tree = ast.parse(pyExpr.strip(), mode="eval")
    allowed = (ast.Expression, ast.BinOp, ast.Add, ast.Sub, ast.Mult, ast.Name, ast.Load, ast.Constant, ast.UnaryOp, ast.USub)
    for node in ast.walk(tree):
        if not isinstance(node, allowed):
            raise ValueError(f"Unsupported syntax in SSIS assignment: {assignment!r}")
    return match.group(2), eval(compile(tree, "<ssis-assignment>", "eval"), {"__builtins__": {}}, names)  # noqa: S307
