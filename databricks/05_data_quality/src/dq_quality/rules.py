"""Rule-engine helpers: translate legacy T-SQL predicates to Spark SQL and evaluate them.

The legacy ``DQ_Rule_Engine`` package runs, for every active ``etl.DataQualityRule``::

    SELECT @Out = CAST(COUNT_BIG(*) AS DECIMAL(18,4)) FROM <ObjectName> WHERE <RuleExpression>;

through sp_executesql, records ``-1`` when the fragment fails to execute, and counts a
rule as failing when ``MeasuredValue > ThresholdValue``. The same contract is kept here:
the offender count is measured with one Spark SQL statement per rule, a rule that cannot
be executed records ``-1`` instead of aborting the sweep, and the legacy rule text is never
modified in the control table - only the executable copy is rewritten.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterable, Optional

from dq_quality.naming_map import deltaTable

NOT_EVALUATED_MEASURE = -1.0

_LEGACY_TABLE_REF = re.compile(
    r"\b(raw|stg|work|err|ref|etl|Dimension|Fact|Aggregate|Report|Integration)\.(\[[A-Za-z_ ]+\]|[A-Za-z_]+)"
)
_LTRIM_RTRIM = re.compile(r"LTRIM\s*\(\s*RTRIM\s*\(([^()]*)\)\s*\)", re.IGNORECASE)
_FUNCTION_MAP = [
    (re.compile(r"\bCOUNT_BIG\s*\(", re.IGNORECASE), "COUNT("),
    (re.compile(r"\bLEN\s*\(", re.IGNORECASE), "LENGTH("),
    (re.compile(r"\bISNULL\s*\(", re.IGNORECASE), "COALESCE("),
    (re.compile(r"\bSYSUTCDATETIME\s*\(\s*\)", re.IGNORECASE), "CURRENT_TIMESTAMP()"),
    (re.compile(r"\bGETUTCDATE\s*\(\s*\)", re.IGNORECASE), "CURRENT_TIMESTAMP()"),
    (re.compile(r"\bGETDATE\s*\(\s*\)", re.IGNORECASE), "CURRENT_TIMESTAMP()"),
    (re.compile(r"\bCHARINDEX\s*\(", re.IGNORECASE), "INSTR("),
]
_NATIONAL_LITERAL = re.compile(r"\bN'")


def translateRuleExpression(expression: str, objectName: str, catalog: str, alias: str = "t") -> str:
    """Rewrite a legacy T-SQL predicate into a Spark SQL predicate.

    * ``<ObjectName>.<Column>`` qualifiers become ``<alias>.<Column>`` (the FROM clause
      aliases the target object as ``alias``);
    * every ``schema.Object`` reference (including ``stg.[Order]``) becomes the mapped
      Unity Catalog name;
    * ``N'x'`` -> ``'x'``, ``LTRIM(RTRIM(x))`` -> ``TRIM(x)``, ``LEN`` -> ``LENGTH``,
      ``COUNT_BIG`` -> ``COUNT``, ``ISNULL`` -> ``COALESCE``, ``SYSUTCDATETIME()`` /
      ``GETUTCDATE()`` / ``GETDATE()`` -> ``CURRENT_TIMESTAMP()``.
    Anything else is left untouched; if the result is not valid Spark SQL the engine
    records ``-1`` for that rule exactly as the legacy CATCH block did.
    """
    text = expression
    qualifier = re.compile(r"\b" + re.escape(objectName.strip()) + r"\.(?=[A-Za-z_\[])")
    text = qualifier.sub(alias + ".", text)
    text = _LEGACY_TABLE_REF.sub(lambda m: deltaTable(catalog, "%s.%s" % (m.group(1), m.group(2))), text)
    text = _NATIONAL_LITERAL.sub("'", text)
    text = _LTRIM_RTRIM.sub(lambda m: "TRIM(%s)" % m.group(1), text)
    for pattern, replacement in _FUNCTION_MAP:
        text = pattern.sub(replacement, text)
    return text


def ruleCountSql(catalog: str, objectName: str, expression: str, alias: str = "t") -> str:
    predicate = translateRuleExpression(expression, objectName, catalog, alias)
    return "SELECT CAST(COUNT(*) AS DECIMAL(18,4)) AS OffenderCount FROM %s AS %s WHERE %s" % (
        deltaTable(catalog, objectName), alias, predicate)


def classifySeverity(severityCode: Optional[str]) -> tuple[str, str]:
    """Legacy ``Classify Rule Severity`` derived column: ``(SeverityCode, BlockingFlag)``."""
    code = (severityCode if severityCode is not None else "WARN").strip().upper()
    return code, ("Y" if code == "FAIL" else "N")


def resultStatus(measuredValue: float, thresholdValue: Optional[float], severityCode: Optional[str]) -> str:
    """Mirror etl.usp_EvaluateDataQualityRules status derivation for one measured rule."""
    if measuredValue is None or measuredValue < 0:
        return "NotEvaluated"
    threshold = float(thresholdValue) if thresholdValue is not None else 0.0
    if measuredValue > threshold:
        return "Failed" if classifySeverity(severityCode)[0] == "FAIL" else "Warned"
    return "Passed"


@dataclass(frozen=True)
class RuleDefinition:
    ruleId: Optional[int]
    ruleCode: str
    ruleGroupCode: str
    objectName: str
    ruleExpression: str
    severityCode: Optional[str]
    thresholdValue: Optional[float]


@dataclass(frozen=True)
class RuleOutcome:
    rule: RuleDefinition
    measuredValue: float
    sql: str
    error: Optional[str]

    @property
    def status(self) -> str:
        return resultStatus(self.measuredValue, self.rule.thresholdValue, self.rule.severityCode)

    @property
    def breached(self) -> bool:
        """Legacy ``Count Failing Rules``: ``MeasuredValue > ThresholdValue`` regardless of severity."""
        threshold = float(self.rule.thresholdValue) if self.rule.thresholdValue is not None else 0.0
        return self.measuredValue > threshold


def evaluateRule(rule: RuleDefinition, catalog: str, runScalar: Callable[[str], float]) -> RuleOutcome:
    sql = ruleCountSql(catalog, rule.objectName, rule.ruleExpression)
    try:
        measured = float(runScalar(sql))
        return RuleOutcome(rule, measured, sql, None)
    except Exception as exc:  # noqa: BLE001 - legacy CATCH: a broken rule records -1, never aborts
        return RuleOutcome(rule, NOT_EVALUATED_MEASURE, sql, "%s: %s" % (type(exc).__name__, str(exc)[:400]))


def evaluateRules(rules: Iterable[RuleDefinition], catalog: str,
                  runScalar: Callable[[str], float]) -> list[RuleOutcome]:
    ordered = sorted(rules, key=lambda r: (r.ruleGroupCode or "", r.ruleCode))
    return [evaluateRule(rule, catalog, runScalar) for rule in ordered]


def countFailingRules(outcomes: Iterable[RuleOutcome]) -> int:
    return sum(1 for o in outcomes if o.breached)


def outcomeRows(outcomes: Iterable[RuleOutcome], batchId: int, packageExecutionId: Optional[int],
                evaluatedAtUtc: Optional[datetime] = None) -> list[dict]:
    """Rows for ``etl.data_quality_result`` (legacy PascalCase columns)."""
    ts = evaluatedAtUtc or datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for o in outcomes:
        rows.append({
            "BatchId": batchId,
            "PackageExecutionId": packageExecutionId,
            "ObjectName": o.rule.objectName,
            "RuleCode": o.rule.ruleCode,
            "MeasuredValue": o.measuredValue,
            "ThresholdValue": float(o.rule.thresholdValue) if o.rule.thresholdValue is not None else None,
            "RowsEvaluated": None,
            "ResultStatus": o.status,
            "RegionCode": None,
            "DetailText": o.error if o.error else o.sql,
            "EvaluatedAtUtc": ts,
        })
    return rows


def sparkScalarRunner(spark) -> Callable[[str], float]:
    def run(sql: str) -> float:
        row = spark.sql(sql).collect()[0]
        return float(row[0]) if row[0] is not None else 0.0
    return run
