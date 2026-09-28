"""Watermark window handling shared by every incremental package.

``control.getWatermark`` (dbx_etl_common, mirror of etl.usp_GetWatermark) returns
the raw (from, to) pair as the legacy procedure did: ISO timestamps for
Timestamp watermarks, ISO dates for DateWindow watermarks and numeric strings
for NumericKey watermarks (whose upper bound the package must resolve from the
source, exactly like the 'Read Source Max Key' Execute SQL Task).
"""
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Union

from oracle_extract.model import WATERMARK_DATE_WINDOW, WATERMARK_NUMERIC_KEY, WATERMARK_TIMESTAMP

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"


@dataclass(frozen=True)
class WatermarkWindow:
    watermarkType: str
    fromValue: Union[datetime, date, int]
    toValue: Optional[Union[datetime, date, int]]

    @property
    def fromText(self) -> str:
        return formatBound(self.watermarkType, self.fromValue)

    @property
    def toText(self) -> Optional[str]:
        return None if self.toValue is None else formatBound(self.watermarkType, self.toValue)

    def sqlLiteral(self, bound: str) -> str:
        """Literal usable inside a pushed-down predicate for the given bound ('from'/'to')."""
        text = self.fromText if bound == "from" else self.toText
        if text is None:
            raise ValueError("upper bound is not resolved")
        if self.watermarkType == WATERMARK_NUMERIC_KEY:
            return text
        return "'" + text + "'"


def formatBound(watermarkType: str, value) -> str:
    if watermarkType == WATERMARK_NUMERIC_KEY:
        return str(int(value))
    if watermarkType == WATERMARK_DATE_WINDOW:
        if isinstance(value, datetime):
            value = value.date()
        return value.strftime(DATE_FORMAT)
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime(value.year, value.month, value.day)
    return value.strftime(TIMESTAMP_FORMAT)


def parseBound(watermarkType: str, value):
    """Normalise whatever the control layer handed back (str / datetime / date / int / Decimal)."""
    if value is None:
        return None
    if watermarkType == WATERMARK_NUMERIC_KEY:
        return int(float(value)) if isinstance(value, str) else int(value)
    if isinstance(value, datetime):
        return value.date() if watermarkType == WATERMARK_DATE_WINDOW else value
    if isinstance(value, date):
        return value if watermarkType == WATERMARK_DATE_WINDOW else datetime(value.year, value.month, value.day)
    text = str(value).strip().replace("T", " ")
    if watermarkType == WATERMARK_DATE_WINDOW:
        return datetime.strptime(text[:10], DATE_FORMAT).date()
    for fmt in (TIMESTAMP_FORMAT, "%Y-%m-%d %H:%M:%S.%f", DATE_FORMAT):
        try:
            return datetime.strptime(text[:26], fmt)
        except ValueError:
            continue
    raise ValueError(f"unparseable {watermarkType} watermark: {value!r}")


def buildWindow(watermarkType: str, watermarkFrom, watermarkTo) -> WatermarkWindow:
    if watermarkType not in (WATERMARK_TIMESTAMP, WATERMARK_NUMERIC_KEY, WATERMARK_DATE_WINDOW):
        raise ValueError(f"unknown watermark type {watermarkType}")
    return WatermarkWindow(watermarkType, parseBound(watermarkType, watermarkFrom), parseBound(watermarkType, watermarkTo))


def windowPredicate(window: WatermarkWindow, column: str, lowerInclusive: bool, upperOpen: bool,
                    upperUnbounded: bool = False, oracle: bool = True) -> str:
    """The legacy WHERE fragment for one source column.

    Oracle flavour reproduces the ``TO_DATE(?, ...)`` binding; Spark flavour is
    the same predicate for the generated-extract-file reader."""
    lowerOp = ">=" if lowerInclusive else ">"
    upperOp = "<" if upperOpen else "<="
    parts = [f"{column} {lowerOp} {_bound(window, 'from', oracle)}"]
    if not upperUnbounded and window.toValue is not None:
        parts.append(f"{column} {upperOp} {_bound(window, 'to', oracle)}")
    return " AND ".join(parts)


def _bound(window: WatermarkWindow, bound: str, oracle: bool) -> str:
    literal = window.sqlLiteral(bound)
    if window.watermarkType == WATERMARK_NUMERIC_KEY:
        return literal
    if oracle:
        fmt = "YYYY-MM-DD" if window.watermarkType == WATERMARK_DATE_WINDOW else "YYYY-MM-DD HH24:MI:SS"
        return f"TO_DATE({literal}, '{fmt}')"
    return f"DATE{literal}" if window.watermarkType == WATERMARK_DATE_WINDOW else f"TIMESTAMP{literal}"


def bindOracleSql(sql: str, bindOrder, window: WatermarkWindow) -> str:
    """Replace the OLE DB '?' placeholders with literals, in legacy bind order.

    The legacy source SQL is kept verbatim; only the parameter markers are
    substituted so the whole WHERE clause is pushed down to Oracle."""
    if sql.count("?") != len(bindOrder):
        raise ValueError(f"expected {len(bindOrder)} bind markers, found {sql.count('?')}")
    out = sql
    for bound in bindOrder:
        out = out.replace("?", window.sqlLiteral(bound), 1)
    return out
