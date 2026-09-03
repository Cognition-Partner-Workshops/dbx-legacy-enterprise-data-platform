"""Deliberate defects in the file feeds.

The file landing zone is where the estate's worst data comes from: partner
extracts produced by spreadsheets, carrier scanners with a flat battery, and a
supplier catalogue that has been through three character-set conversions. The
ingestion packages route these to ``err.RejectedFileRow`` rather than failing,
so the generator has to actually produce them - at a configurable, reproducible
rate.

Every defect kind here corresponds to something the ingestion and staging
layer is expected to detect:

============================  ==================================================
kind                          what the loader should do with it
============================  ==================================================
``short_row``                 field count below the contract - reject the row
``long_row``                  extra trailing field - reject the row
``bad_date``                  unparseable or impossible date - safe-date to NULL
``bad_decimal``               thousands separators or a stray currency symbol
``unknown_code``              a code absent from the reference tables
``overflow``                  a value wider than the target column
``bad_encoding``              a Latin-1 byte in a UTF-8 feed
``embedded_delimiter``        an unescaped delimiter inside a text field
``null_key``                  a mandatory business key left empty
``whitespace_key``            a key that only differs by padding
``negative_quantity``         a quantity that should never be below zero
============================  ==================================================

Note that defects are injected by *rewriting an already-valid row*, so the
clean and broken versions of a feed differ only in the intended way.
"""

from __future__ import annotations

from . import rng

DEFECT_KINDS = (
    "short_row", "long_row", "bad_date", "bad_decimal", "unknown_code", "overflow",
    "bad_encoding", "embedded_delimiter", "null_key", "whitespace_key", "negative_quantity",
)

_DEFECT_WEIGHTS = (12, 9, 16, 14, 13, 7, 6, 6, 8, 5, 4)

_BAD_DATES = ("2019-02-30", "31/13/2020", "0000-00-00", "2021-13-01", "n/a", "20220731",
              "2023-06-31", "TBC")

_BAD_DECIMALS = ("1,234.56", "$980.00", "12.34.56", "(45.00)", "1 200,50", "NULL", "--")

_UNKNOWN_CODES = ("ZZZ", "XX99", "LEGACY7", "?", "TMP-CODE", "999")


def choose_defect(seed: int, *parts) -> str:
    return rng.weighted_pick(seed, DEFECT_KINDS, _DEFECT_WEIGHTS, "defect-kind", *parts)


def apply_defect(seed: int, kind: str, fields, spec, ordinal: int):
    """Return ``(payload, reject_reason)`` for a defective row.

    ``payload`` is either a list of string fields (re-joined by the caller) or
    raw ``bytes`` when the defect is an encoding problem.
    """
    values = list(fields)
    columns = spec.columns
    delimiter = spec.delimiter

    if kind == "short_row":
        cut = 1 + rng.stable_hash(seed, "short", ordinal) % max(len(values) // 3, 1)
        return values[:-cut], "FIELD_COUNT_LOW"

    if kind == "long_row":
        return values + ["EXTRA_%d" % (ordinal % 997)], "FIELD_COUNT_HIGH"

    if kind == "bad_date":
        index = _index_of_date(columns, seed, ordinal)
        values[index] = rng.pick(seed, _BAD_DATES, "bad-date", ordinal)
        return values, "UNPARSEABLE_DATE"

    if kind == "bad_decimal":
        index = _index_of_type(columns, ("decimal",), seed, ordinal)
        values[index] = rng.pick(seed, _BAD_DECIMALS, "bad-dec", ordinal)
        return values, "UNPARSEABLE_DECIMAL"

    if kind == "unknown_code":
        index = _index_of_code(columns, seed, ordinal)
        values[index] = rng.pick(seed, _UNKNOWN_CODES, "unk-code", ordinal)
        return values, "UNKNOWN_REFERENCE_CODE"

    if kind == "overflow":
        index = _index_of_type(columns, ("string",), seed, ordinal)
        width = max(columns[index].length, 10) + 40
        values[index] = "%s-%s" % (values[index] or "X", "P" * width)
        return values, "VALUE_TOO_LONG"

    if kind == "bad_encoding":
        index = _index_of_type(columns, ("string",), seed, ordinal)
        values[index] = str(values[index] or "").replace("a", "\u00e0", 1) or "caf\u00e9"
        line = delimiter.join("" if value is None else str(value) for value in values)
        return line.encode("latin-1", "replace"), "INVALID_ENCODING"

    if kind == "embedded_delimiter":
        index = _index_of_type(columns, ("string",), seed, ordinal)
        values[index] = "%s%sSUFFIX" % (values[index], delimiter)
        return values, "EMBEDDED_DELIMITER"

    if kind == "null_key":
        values[0] = ""
        return values, "MISSING_BUSINESS_KEY"

    if kind == "whitespace_key":
        values[0] = "  %s " % values[0]
        return values, "UNTRIMMED_BUSINESS_KEY"

    index = _index_of_type(columns, ("integer",), seed, ordinal)
    values[index] = "-%s" % (values[index] if values[index] not in (None, "") else "1")
    return values, "NEGATIVE_QUANTITY"


REJECT_CODE = {
    "short_row": "FIELD_COUNT_LOW",
    "long_row": "FIELD_COUNT_HIGH",
    "bad_date": "UNPARSEABLE_DATE",
    "bad_decimal": "UNPARSEABLE_DECIMAL",
    "unknown_code": "UNKNOWN_REFERENCE_CODE",
    "overflow": "VALUE_TOO_LONG",
    "bad_encoding": "INVALID_ENCODING",
    "embedded_delimiter": "EMBEDDED_DELIMITER",
    "null_key": "MISSING_BUSINESS_KEY",
    "whitespace_key": "UNTRIMMED_BUSINESS_KEY",
    "negative_quantity": "NEGATIVE_QUANTITY",
}


def reject_code(kind: str) -> str:
    """The classification the reject framework records for a defect kind."""
    return REJECT_CODE[kind]


def _index_of_type(columns, wanted, seed: int, ordinal: int):
    """A column of one of ``wanted`` types, falling back to the first column."""
    candidates = [index for index, column in enumerate(columns) if column.type in wanted]
    if not candidates:
        return 0
    return candidates[rng.stable_hash(seed, "defect-col", ordinal) % len(candidates)]


def _index_of_date(columns, seed: int, ordinal: int):
    """Date-ish columns, including the feeds that carry their dates as text."""
    candidates = [index for index, column in enumerate(columns)
                  if column.type in ("date", "timestamp")
                  or "DATE" in column.name.upper()
                  or column.name.upper().endswith(("_DT", "_TS"))]
    if not candidates:
        return _index_of_type(columns, ("string",), seed, ordinal)
    return candidates[rng.stable_hash(seed, "defect-date-col", ordinal) % len(candidates)]


def _index_of_code(columns, seed: int, ordinal: int):
    candidates = [index for index, column in enumerate(columns)
                  if column.name.lower().endswith(("code", "_cd", "status", "reason", "currency"))]
    if not candidates:
        return _index_of_type(columns, ("string",), seed, ordinal)
    return candidates[rng.stable_hash(seed, "defect-code-col", ordinal) % len(candidates)]
