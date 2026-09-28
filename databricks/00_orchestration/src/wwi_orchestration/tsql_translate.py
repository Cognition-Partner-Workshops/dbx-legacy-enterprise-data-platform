"""Translate the plan's T-SQL control statements to Spark SQL / ``dbx_etl_common`` calls.

The orchestration plan carries ten distinct ``control`` statements/queries (see
``ssis/orchestration-plan.json``). They use a small T-SQL dialect subset - ``N''`` literals,
``TOP (n)``, ``ISNULL``, ``DATEADD(unit, n, SYSUTCDATETIME())``, bit comparisons and ``?``
placeholders - which this module rewrites deterministically at generation time. Legacy
``etl.Object`` references are translated at *runtime* with
``dbx_etl_common.naming.translateLegacyReferences`` because they depend on the catalog.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

_BIT_COLUMNS = ("IsReprocessed", "IsActive", "IsLocked", "IsEnabled", "IsCurrent", "IsDeleted")
_DATEADD = re.compile(r"DATEADD\(\s*(\w+)\s*,\s*(-?\d+)\s*,\s*([^()]*\(\)|[^,()]+)\s*\)", re.IGNORECASE)
_TOP = re.compile(r"\bTOP\s*\(\s*(\d+)\s*\)\s*", re.IGNORECASE)
_EXEC_LOG_ERROR = re.compile(r"^\s*EXEC\s+etl\.usp_LogError\s+(?P<args>.+?);?\s*$", re.IGNORECASE | re.DOTALL)
_NAMED_ARG = re.compile(r"@(?P<name>\w+)\s*=\s*(?P<value>N?'(?:[^']|'')*'|NULL|-?\d+(?:\.\d+)?)", re.IGNORECASE)


def translateSql(sql: str) -> str:
    """T-SQL -> Spark SQL for the constructs the plan uses. ``?`` become ``:p0``, ``:p1``..."""
    out = sql.strip().rstrip(";")
    out = re.sub(r"\bN'", "'", out)
    out = re.sub(r"\bISNULL\s*\(", "coalesce(", out, flags=re.IGNORECASE)
    out = re.sub(r"\bSYSUTCDATETIME\(\)", "current_timestamp()", out, flags=re.IGNORECASE)
    out = re.sub(r"\bGETUTCDATE\(\)", "current_timestamp()", out, flags=re.IGNORECASE)
    out = re.sub(r"\bCOUNT_BIG\s*\(", "count(", out, flags=re.IGNORECASE)

    def _dateadd(m: "re.Match[str]") -> str:
        unit, n, expr = m.group(1).upper(), int(m.group(2)), m.group(3).strip()
        return f"({expr} + INTERVAL {n} {unit})"
    out = _DATEADD.sub(_dateadd, out)

    top = _TOP.search(out)
    if top:
        out = _TOP.sub("", out, count=1).rstrip() + f" LIMIT {int(top.group(1))}"
    for col in _BIT_COLUMNS:
        out = re.sub(rf"\b{col}\s*=\s*0\b", f"{col} = false", out)
        out = re.sub(rf"\b{col}\s*=\s*1\b", f"{col} = true", out)
    counter = [0]

    def _param(_: "re.Match[str]") -> str:
        name = f":p{counter[0]}"
        counter[0] += 1
        return name
    out = re.sub(r"\?", _param, out)
    return re.sub(r"\s+", " ", out).strip()


def _literal(text: str) -> Any:
    if text.upper() == "NULL":
        return None
    if text[:1] in "N'":
        return text[text.index("'") + 1:-1].replace("''", "'")
    return float(text) if "." in text else int(text)


def translateStatement(sql: str) -> Dict[str, Any]:
    """A plan ``statement`` -> ``{"sql": ...}`` or ``{"call": "logError", "args": {...}}``."""
    m = _EXEC_LOG_ERROR.match(sql)
    if m:
        args = {a.group("name"): _literal(a.group("value")) for a in _NAMED_ARG.finditer(m.group("args"))}
        mapped = {
            "packageExecutionId": args.get("PackageExecutionId"), "batchId": args.get("BatchId"),
            "errorSeverity": args.get("ErrorSeverity", "Error"), "errorCode": args.get("ErrorCode"),
            "sourceName": args.get("SourceName"), "sourceComponent": args.get("SourceComponent"),
            "procedureName": args.get("ProcedureName"), "errorDescription": args.get("ErrorDescription"),
        }
        return {"call": "logError", "args": mapped}
    if re.match(r"^\s*EXEC\b", sql, re.IGNORECASE):
        raise ValueError(f"unsupported stored-procedure call in plan statement: {sql}")
    return {"sql": translateSql(sql)}


def bindParameters(sql: str, values: List[Any]) -> Tuple[str, Dict[str, Any]]:
    """Pair a translated statement with its positional values -> ``(sql, {"p0": v0, ...})``."""
    names = re.findall(r":p(\d+)", sql)
    if len(names) != len(values):
        raise ValueError(f"statement expects {len(names)} parameter(s), {len(values)} supplied: {sql}")
    return sql, {f"p{i}": values[i] for i in range(len(values))}
