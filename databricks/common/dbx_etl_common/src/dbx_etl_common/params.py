"""Job parameter parsing (Databricks job parameters -> typed Python values).

Job parameters are all strings. ``getJobParams(dbutils)`` reads them through ``dbutils.widgets``
(job parameters are exposed to notebook tasks as widgets) and returns the typed dict every
migrated notebook expects::

    p = params.getJobParams(dbutils)
    p["batchId"]           -> int   (0 when not supplied)
    p["businessDate"]      -> datetime.date (today UTC when empty)
    p["reloadFullHistory"] -> bool
    p["environmentCode"]   -> str   ("DEV" when empty)
    p["restartFromStep"]   -> str   ("" when empty)
    p["catalog"]           -> str
    p["maxParallelStreams"]-> int   (4 when empty)
    p["maxExtractAttempts"]-> int   (3 when empty)
    p["raw"]               -> dict of every raw widget value that was present
"""
from __future__ import annotations

import datetime as _dt
from typing import Any, Dict, Mapping, Optional

TRUE_VALUES = {"1", "true", "t", "yes", "y", "on"}
FALSE_VALUES = {"0", "false", "f", "no", "n", "off", ""}

STANDARD_PARAMETERS = (
    "BatchId", "BusinessDate", "ReloadFullHistory", "EnvironmentCode", "RestartFromStep", "catalog",
    "MaxParallelStreams", "MaxExtractAttempts",
)


def parseBool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    v = str(value).strip().lower()
    if v in TRUE_VALUES:
        return True
    if v in FALSE_VALUES:
        return default if v == "" else False
    raise ValueError(f"Cannot interpret '{value}' as a boolean.")


def parseInt(value: Optional[str], default: int = 0) -> int:
    if value is None or str(value).strip() == "":
        return default
    return int(str(value).strip())


def parseDate(value: Optional[str], default: Optional[_dt.date] = None) -> _dt.date:
    if value is None or str(value).strip() == "":
        return default if default is not None else _dt.datetime.now(_dt.timezone.utc).date()
    v = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return _dt.datetime.strptime(v[:len(fmt) + 4] if "%S" in fmt else v, fmt).date()
        except ValueError:
            continue
    return _dt.date.fromisoformat(v[:10])


def getWidget(dbutils: Any, name: str, default: Optional[str] = None) -> Optional[str]:
    """Read one widget / job parameter; returns ``default`` when the widget does not exist."""
    try:
        value = dbutils.widgets.get(name)
    except Exception:  # widget not defined for this task
        return default
    if value is None:
        return default
    return str(value)


def fromMapping(values: Mapping[str, Any]) -> Dict[str, Any]:
    """Typed parameters from a plain mapping (used by tests and by the orchestration tools)."""
    get = lambda k: None if values.get(k) is None else str(values.get(k))  # noqa: E731
    return {
        "batchId": parseInt(get("BatchId"), 0),
        "businessDate": parseDate(get("BusinessDate")),
        "reloadFullHistory": parseBool(get("ReloadFullHistory"), False),
        "environmentCode": (get("EnvironmentCode") or "DEV").strip() or "DEV",
        "restartFromStep": (get("RestartFromStep") or "").strip(),
        "catalog": (get("catalog") or "").strip(),
        "maxParallelStreams": parseInt(get("MaxParallelStreams"), 4),
        "maxExtractAttempts": parseInt(get("MaxExtractAttempts"), 3),
        "raw": {k: v for k, v in values.items() if v is not None},
    }


def getJobParams(dbutils: Any) -> Dict[str, Any]:
    """Read the standard job parameters through ``dbutils.widgets`` and return typed values."""
    raw: Dict[str, str] = {}
    for name in STANDARD_PARAMETERS:
        v = getWidget(dbutils, name)
        if v is not None:
            raw[name] = v
    p = fromMapping(raw)
    if not p["catalog"]:
        raise ValueError("Job parameter 'catalog' is required (bundle variable ${var.catalog}).")
    return p
