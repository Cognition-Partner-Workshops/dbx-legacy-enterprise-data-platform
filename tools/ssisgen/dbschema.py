"""The SQL Server landing schema as the OLE DB destination sees it.

An OLE DB destination persists the *table's* columns as its external metadata
and validates that metadata against the real table when the package starts. A
destination whose external metadata was copied from the pipeline instead of the
table therefore fails validation with VS_NEEDSNEWMETADATA before a row moves,
even though the package generated, built and deployed cleanly.

This module reads the deployed DDL under ``sqlserver/`` - the same scripts that
create the tables - so a generated destination describes the table it opens.
Nothing here contacts a database; the DDL is the contract.
"""

from __future__ import annotations

import os
import re

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DDL_ROOT = os.path.join(REPO_ROOT, "sqlserver")

CREATE_TABLE = re.compile(r"CREATE\s+TABLE\s+(\[?\w+\]?\.\[?\w+\]?)\s*\(", re.IGNORECASE)

# Definitions inside a table body that describe something other than a column.
NON_COLUMN = ("CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "INDEX", "PERIOD")

# SQL Server type -> (SSIS pipeline data type, carries length, carries precision/scale)
TYPE_MAP = {
    "bigint": ("i8", False, False),
    "int": ("i4", False, False),
    "smallint": ("i2", False, False),
    "tinyint": ("ui1", False, False),
    "bit": ("bool", False, False),
    "decimal": ("numeric", False, True),
    "numeric": ("numeric", False, True),
    "money": ("cy", False, False),
    "smallmoney": ("cy", False, False),
    "float": ("r8", False, False),
    "real": ("r4", False, False),
    "date": ("dbDate", False, False),
    "datetime": ("dbTimeStamp", False, False),
    "smalldatetime": ("dbTimeStamp", False, False),
    "datetime2": ("dbTimeStamp2", False, True),
    "time": ("dbTime2", False, True),
    "datetimeoffset": ("dbTimeStampOffset", False, True),
    "uniqueidentifier": ("guid", False, False),
    "nvarchar": ("wstr", True, False),
    "nchar": ("wstr", True, False),
    "varchar": ("str", True, False),
    "char": ("str", True, False),
    "varbinary": ("bytes", True, False),
    "binary": ("bytes", True, False),
    "xml": ("nText", False, False),
}

# The max-length forms are their own SSIS types and carry no length.
MAX_TYPE_MAP = {
    "nvarchar": "nText",
    "nchar": "nText",
    "varchar": "text",
    "char": "text",
    "varbinary": "image",
    "binary": "image",
}


class SchemaError(ValueError):
    """The DDL does not describe a table a destination claims to write."""


class TableColumn:
    """One column of a deployed table, described the way SSIS describes it."""

    def __init__(self, name, dtype, length=None, precision=None, scale=None,
                 codepage=None, nullable=True, has_default=False, identity=False):
        self.name = name
        self.dtype = dtype
        self.length = length
        self.precision = precision
        self.scale = scale
        self.codepage = codepage
        self.nullable = nullable
        self.has_default = has_default
        self.identity = identity

    @property
    def supplied_by_the_server(self):
        """True when an insert that omits the column still succeeds."""
        return self.identity or self.has_default or self.nullable

    def metadata_attrs(self):
        parts = ['dataType="%s"' % self.dtype]
        if self.length is not None:
            parts.append('length="%d"' % self.length)
        if self.precision is not None:
            parts.append('precision="%d"' % self.precision)
        if self.scale is not None:
            parts.append('scale="%d"' % self.scale)
        if self.codepage is not None:
            parts.append('codePage="%d"' % self.codepage)
        return " ".join(parts)


# The OLE DB destination inserts a buffer column into a table column by
# converting between them, and it refuses the conversions across these families:
# Unicode text into a non-Unicode column, a number into a text column, and so
# on. A mapping across two families is a component that fails validation, so the
# generator refuses it instead.
TYPE_FAMILIES = {
    "wstr": "unicode", "nText": "unicode",
    "str": "ansi", "text": "ansi",
    "i1": "number", "i2": "number", "i4": "number", "i8": "number",
    "ui1": "number", "ui2": "number", "ui4": "number", "ui8": "number",
    "numeric": "number", "decimal": "number", "cy": "number",
    "r4": "number", "r8": "number",
    "date": "datetime", "dbDate": "datetime", "dbTime": "datetime",
    "dbTime2": "datetime", "dbTimeStamp": "datetime", "dbTimeStamp2": "datetime",
    "dbTimeStampOffset": "datetime", "filetime": "datetime",
    "bool": "bool",
    "guid": "guid",
    "bytes": "binary", "image": "binary",
}


def type_family(dtype):
    return TYPE_FAMILIES.get(dtype, dtype)


def compatible(buffer_dtype, table_dtype):
    """True when the destination can insert *buffer_dtype* into *table_dtype*."""
    return type_family(buffer_dtype) == type_family(table_dtype)


def normalise_table(name):
    """'[raw].[FilePartnerSales]' and 'raw.FilePartnerSales' name one table."""
    return name.replace("[", "").replace("]", "").strip().lower()


def _split_definitions(body):
    """The comma-separated definitions of a table body, nesting respected."""
    parts = []
    depth = 0
    current = []
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    if "".join(current).strip():
        parts.append("".join(current))
    return parts


def _table_body(text, start):
    """The text between the parentheses of a CREATE TABLE beginning at *start*."""
    depth = 1
    for index in range(start, len(text)):
        if text[index] == "(":
            depth += 1
        elif text[index] == ")":
            depth -= 1
            if depth == 0:
                return text[start:index]
    return text[start:]


def _parse_column(definition):
    tokens = definition.replace("\n", " ").strip()
    match = re.match(r"^\[?(\w+)\]?\s+\[?(\w+)\]?\s*(\(([^)]*)\))?(.*)$", tokens, re.DOTALL)
    if not match:
        return None
    name, sql_type, _, args, rest = match.groups()
    sql_type = sql_type.lower()
    if sql_type not in TYPE_MAP:
        return None
    rest_upper = (rest or "").upper()
    args = (args or "").strip().lower()
    dtype, has_length, has_scale = TYPE_MAP[sql_type]
    length = precision = scale = codepage = None
    if args == "max":
        dtype = MAX_TYPE_MAP[sql_type]
        if dtype == "text":
            codepage = 1252
    elif has_length and args:
        length = int(args.split(",")[0])
        if dtype == "str":
            codepage = 1252
    elif has_scale and args:
        pieces = [int(piece) for piece in args.split(",")]
        if sql_type in ("decimal", "numeric"):
            precision, scale = pieces[0], (pieces[1] if len(pieces) > 1 else 0)
        else:
            scale = pieces[0]
    elif sql_type == "datetime2":
        scale = 7
    return TableColumn(
        name=name,
        dtype=dtype,
        length=length,
        precision=precision,
        scale=scale,
        codepage=codepage,
        nullable="NOT NULL" not in rest_upper,
        has_default="DEFAULT" in rest_upper,
        identity="IDENTITY" in rest_upper,
    )


def strip_comments(text):
    """Drop SQL comments; a comma inside one is not a column separator."""
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", "", text)


def _parse_file(path, tables):
    with open(path, encoding="utf-8", errors="replace") as handle:
        text = strip_comments(handle.read())
    for match in CREATE_TABLE.finditer(text):
        table = normalise_table(match.group(1))
        columns = []
        for definition in _split_definitions(_table_body(text, match.end())):
            stripped = definition.strip().lstrip("[")
            if not stripped or stripped.split(" ")[0].upper() in NON_COLUMN:
                continue
            column = _parse_column(definition)
            if column is not None:
                columns.append(column)
        if columns:
            tables[table] = columns


_CACHE = None


def catalog():
    """Every table the deployed DDL creates, keyed by 'schema.table'."""
    global _CACHE
    if _CACHE is None:
        tables = {}
        for directory, _dirs, names in os.walk(DDL_ROOT):
            for name in sorted(names):
                if name.endswith(".sql"):
                    _parse_file(os.path.join(directory, name), tables)
        _CACHE = tables
    return _CACHE


def has_table(table):
    """True when the deployed DDL creates *table*."""
    return normalise_table(table) in catalog()


def table_columns(table):
    """The columns of *table*, in the order the DDL declares them.

    Raises :class:`SchemaError` when the DDL creates no such table: a
    destination cannot describe a table nobody deploys.
    """
    key = normalise_table(table)
    known = catalog()
    if key not in known:
        raise SchemaError(
            "no CREATE TABLE under sqlserver/ deploys %s; an OLE DB destination "
            "cannot describe a table that does not exist" % table)
    return known[key]
