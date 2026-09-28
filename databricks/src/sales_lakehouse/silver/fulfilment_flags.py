"""Pipe-delimited ``Sales.Orders.FulfilmentFlags`` parsing.

Legacy: sqlserver/oltp/02_extensions/2000_Sales.Orders.Extensions.sql lines
19-21 document the flag letters: 'H' hold, 'B' backorder, 'S' split, 'X' export.
The screen wrote the list free-hand, so ``P||B`` and trailing pipes exist in the
data; they are parsed leniently and only warned about, never rejected.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

FULFILMENT_FLAG_LETTERS: dict[str, str] = {
    "H": "is_hold_flagged",
    "B": "is_backorder_flagged",
    "S": "is_split_flagged",
    "X": "is_export_flagged",
}

FLAG_COLUMNS = ("fulfilment_flags_raw", *FULFILMENT_FLAG_LETTERS.values(), "fulfilment_flags_unknown", "fulfilment_flags_malformed")


def _tokens(raw: Column) -> Column:
    """Split on '|', trim/uppercase each token, drop empties (lenient parse)."""
    parts = F.split(F.coalesce(raw, F.lit("")), r"\|")
    cleaned = F.transform(parts, lambda t: F.upper(F.trim(t)))
    return F.filter(cleaned, lambda t: t != "")


def isMalformed(raw: Column) -> Column:
    """True for empty segments (``P||B``, leading/trailing pipe) or multi-letter tokens."""
    trimmed = F.trim(raw)
    hasEmptySegment = trimmed.rlike(r"(^\|)|(\|\|)|(\|$)")
    hasWideToken = F.exists(_tokens(raw), lambda t: F.length(t) > 1)
    return F.when(raw.isNull() | (trimmed == ""), F.lit(False)).otherwise(hasEmptySegment | hasWideToken)


def parseFulfilmentFlags(df: DataFrame, rawCol: str = "FulfilmentFlags") -> DataFrame:
    """Add ``fulfilment_flags_raw``, one boolean per documented letter,
    ``fulfilment_flags_unknown`` (array of undocumented letters) and
    ``fulfilment_flags_malformed`` (drives ``dq_status_code = 'WARN'``)."""
    raw = F.col(rawCol)
    tokens = _tokens(raw)
    out = df.withColumn("fulfilment_flags_raw", raw)
    for letter, colName in FULFILMENT_FLAG_LETTERS.items():
        out = out.withColumn(colName, F.array_contains(tokens, letter))
    known = F.array(*[F.lit(k) for k in FULFILMENT_FLAG_LETTERS])
    out = out.withColumn(
        "fulfilment_flags_unknown",
        F.array_distinct(F.filter(tokens, lambda t: ~F.array_contains(known, t))),
    )
    return out.withColumn("fulfilment_flags_malformed", isMalformed(raw))
