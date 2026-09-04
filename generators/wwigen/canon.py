"""The canonical database contracts the generator has to produce rows for.

The estate DDL is the source of truth: ``oracle/`` for the ERP schemas and
``sqlserver/staging/tables/`` for the landing tables. Nothing here invents a
column - a generated extract is only ever a projection of a table that the
deployment actually creates, which is what makes the emitted SQL*Loader and
bcp artefacts loadable.

The DDL parsing itself is shared with the offline checks under
``validation/checks``; this module adds the parts the generator needs and the
checks do not: primary keys, foreign keys, and the value sets that CHECK
constraints restrict a code column to.
"""

from __future__ import annotations

import os
import re
import sys

REPO_ROOT = os.environ.get("WWI_ESTATE_ROOT") or os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_CHECKS_DIR = os.path.join(REPO_ROOT, "validation", "checks")
if _CHECKS_DIR not in sys.path:
    sys.path.insert(0, _CHECKS_DIR)

import oraclelib  # noqa: E402
import tsqllib  # noqa: E402

PK_RE = re.compile(
    r"ADD\s+CONSTRAINT\s+\w+\s+PRIMARY\s+KEY\s*\(([^)]*)\)", re.I)
INLINE_PK_RE = re.compile(r"CONSTRAINT\s+\w+\s+PRIMARY\s+KEY\s*\(([^)]*)\)", re.I)
FK_RE = re.compile(
    r"ADD\s+CONSTRAINT\s+\w+\s+FOREIGN\s+KEY\s*\(([^)]*)\)\s*"
    r"REFERENCES\s+([A-Z0-9_]+)\.([A-Z0-9_]+)\s*\(([^)]*)\)", re.I)
CHECK_IN_RE = re.compile(
    r"CHECK\s*\(\s*([A-Z][A-Z0-9_]*)\s+IN\s*\(([^)]*)\)\s*\)", re.I)
TABLE_HEADER_RE = re.compile(r"CREATE\s+TABLE\s+([A-Z0-9_]+)\.([A-Z0-9_]+)", re.I)


class OracleColumn:
    """One canonical Oracle column and everything a writer needs about it."""

    def __init__(self, name, type_name, precision_text, nullable, has_default):
        self.name = name
        self.type_name = type_name.upper()
        self.precision_text = precision_text or ""
        self.nullable = nullable
        self.has_default = has_default

    @property
    def is_character(self):
        return self.type_name in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR")

    @property
    def is_numeric(self):
        return self.type_name in ("NUMBER", "INTEGER", "FLOAT",
                                  "BINARY_DOUBLE", "BINARY_FLOAT")

    @property
    def is_date(self):
        return self.type_name in ("DATE", "TIMESTAMP")

    @property
    def width(self):
        """Declared character width, or None."""
        if not self.is_character:
            return None
        digits = re.search(r"\d+", self.precision_text)
        return int(digits.group(0)) if digits else None

    @property
    def precision(self):
        digits = re.findall(r"\d+", self.precision_text)
        return int(digits[0]) if digits else 0

    @property
    def scale(self):
        digits = re.findall(r"\d+", self.precision_text)
        return int(digits[1]) if len(digits) > 1 else 0

    @property
    def required(self):
        """NOT NULL and no DEFAULT: the extract must supply a value."""
        return (not self.nullable) and (not self.has_default)


class OracleTable:
    def __init__(self, key, path):
        self.key = key
        self.path = path
        self.columns = []                # [OracleColumn] in declaration order
        self.by_name = {}
        self.primary_key = ()
        self.foreign_keys = {}           # column -> (parent key, parent column)
        self.allowed_values = {}         # column -> (value, ...)

    @property
    def schema(self):
        return self.key.split(".")[0]

    @property
    def name(self):
        return self.key.split(".")[1]

    def column(self, name):
        return self.by_name.get(name.upper())

    def has(self, name):
        return name.upper() in self.by_name

    @property
    def required_columns(self):
        return tuple(column.name for column in self.columns if column.required)


def _oracle_constraints(table, text):
    """PK, FK and CHECK-IN value sets for one table, from its own DDL file."""
    statements = re.split(r"\n\s*/\s*\n", oraclelib.strip_comments(text))
    for statement in statements:
        header = TABLE_HEADER_RE.search(statement)
        if header and "%s.%s" % (header.group(1).upper(),
                                 header.group(2).upper()) != table.key:
            continue
        if not header and table.name.upper() not in statement.upper():
            continue
        for match in list(PK_RE.finditer(statement)) + list(INLINE_PK_RE.finditer(statement)):
            columns = tuple(c.strip().upper() for c in match.group(1).split(","))
            if all(table.has(column) for column in columns):
                table.primary_key = columns
        for columns, parent_schema, parent_name, parent_columns in FK_RE.findall(statement):
            child = [c.strip().upper() for c in columns.split(",")]
            parent = [c.strip().upper() for c in parent_columns.split(",")]
            if len(child) != len(parent):
                continue
            for child_column, parent_column in zip(child, parent):
                table.foreign_keys[child_column] = (
                    "%s.%s" % (parent_schema.upper(), parent_name.upper()), parent_column)
        for column, values in CHECK_IN_RE.findall(statement):
            literals = tuple(
                literal for literal in
                (oraclelib.string_literal(part) for part in values.split(","))
                if literal is not None)
            if literals and table.has(column):
                table.allowed_values.setdefault(column.upper(), literals)


def _oracle_files():
    root = os.path.join(REPO_ROOT, "oracle", "tables")
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in sorted(filenames):
            if filename.lower().endswith(".sql"):
                yield os.path.join(dirpath, filename)


_ORACLE_CACHE = {}


def oracle_tables():
    """{SCHEMA.TABLE: OracleTable} for every table under oracle/tables."""
    if _ORACLE_CACHE.get("root") == REPO_ROOT:
        return _ORACLE_CACHE["tables"]
    tables = {}
    for path in _oracle_files():
        rel = os.path.relpath(path, REPO_ROOT).replace(os.sep, "/")
        with open(path, errors="replace") as handle:
            text = handle.read()
        for parsed in oraclelib.parse_tables(text, rel):
            table = OracleTable(parsed.key, rel)
            for name in parsed.columns:
                type_name, precision = parsed.types[name]
                column = OracleColumn(name, type_name, precision,
                                      name not in parsed.not_null,
                                      name in parsed.has_default)
                table.columns.append(column)
                table.by_name[name] = column
            _oracle_constraints(table, text)
            tables[table.key] = table
    _ORACLE_CACHE.update(root=REPO_ROOT, tables=tables)
    return tables


class SqlColumn:
    def __init__(self, name, type_text, nullable, computed):
        self.name = name
        self.type_text = type_text
        self.nullable = nullable
        self.computed = computed

    @property
    def type_name(self):
        return re.split(r"[(]", self.type_text)[0]

    @property
    def is_character(self):
        return self.type_name in ("char", "varchar", "nchar", "nvarchar")

    @property
    def is_numeric(self):
        return self.type_name in ("bigint", "int", "smallint", "tinyint",
                                  "decimal", "numeric", "money", "float", "real")

    @property
    def is_date(self):
        return self.type_name in ("date", "datetime", "datetime2",
                                  "smalldatetime", "datetimeoffset", "time")

    @property
    def width(self):
        if not self.is_character:
            return None
        digits = re.search(r"\((\d+|max)\)", self.type_text)
        if not digits:
            return None
        return None if digits.group(1) == "max" else int(digits.group(1))


class SqlTable:
    def __init__(self, key, path):
        self.key = key
        self.path = path
        self.columns = []
        self.by_name = {}
        self.defaults = set()            # columns with a DEFAULT constraint

    def column(self, name):
        return self.by_name.get(name.upper())

    def has(self, name):
        return name.upper() in self.by_name

    @property
    def required_columns(self):
        return tuple(column.name for column in self.columns
                     if not column.nullable and not column.computed
                     and column.name not in self.defaults)


STAGING_TABLE_FILES = ("10_raw_tables_oracle.sql", "11_raw_tables_sqlserver.sql",
                       "12_raw_tables_file.sql", "40_err_tables.sql")

SQL_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?\s*\(", re.I)
SQL_COLUMN_NAME_RE = re.compile(r"^\s*(?:\[([^\]]+)\]|(\w+))\b")
NOT_A_COLUMN = ("CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN", "INDEX")


def _sql_defaults(text):
    """{schema.table: {COLUMN, ...}} for every column carrying a DEFAULT.

    A DEFAULT is routinely declared on a continuation line of its column, so
    the table body is split on its own top-level commas rather than by line.
    """
    defaults = {}
    for header in SQL_TABLE_RE.finditer(text):
        key = "%s.%s" % ((header.group(1) or "dbo").lower(),
                         header.group(2).lower())
        depth, position = 1, header.end()
        segment_start, segments = position, []
        while position < len(text) and depth:
            character = text[position]
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if not depth:
                    segments.append(text[segment_start:position])
                    break
            elif character == "," and depth == 1:
                segments.append(text[segment_start:position])
                segment_start = position + 1
            position += 1
        for segment in segments:
            if not re.search(r"\bDEFAULT\b", segment, re.I):
                continue
            name = SQL_COLUMN_NAME_RE.match(segment)
            if not name:
                continue
            token = (name.group(1) or name.group(2)).upper()
            if token not in NOT_A_COLUMN:
                defaults.setdefault(key, set()).add(token)
    return defaults

_SQL_CACHE = {}


def sqlserver_landing_tables():
    """{schema.table: SqlTable} for the raw and err landing tables."""
    if _SQL_CACHE.get("root") == REPO_ROOT:
        return _SQL_CACHE["tables"]
    files = []
    root = os.path.join(REPO_ROOT, "sqlserver", "staging", "tables")
    for filename in STAGING_TABLE_FILES:
        path = os.path.join(root, filename)
        if not os.path.isfile(path):
            continue
        rel = os.path.relpath(path, REPO_ROOT).replace(os.sep, "/")
        with open(path, errors="replace") as handle:
            files.append((rel, handle.read()))
    parsed = tsqllib.load_tables(files)
    defaults = {}
    for _rel, text in files:
        defaults.update(_sql_defaults(tsqllib.strip_comments(text)))
    tables = {}
    for key, table in parsed.items():
        built = SqlTable(key, table.path)
        defaulted = defaults.get(key.lower(), set())
        for name in table.columns:
            column = SqlColumn(name, table.types.get(name, ""),
                               name not in table.not_null, name in table.computed)
            built.columns.append(column)
            built.by_name[name.upper()] = column
            if name.upper() in defaulted:
                built.defaults.add(name)
        tables[key] = built
    _SQL_CACHE.update(root=REPO_ROOT, tables=tables)
    return tables


def reset_cache():
    """Drop the parsed contracts; the fixtures repoint REPO_ROOT at a copy."""
    _ORACLE_CACHE.clear()
    _SQL_CACHE.clear()
