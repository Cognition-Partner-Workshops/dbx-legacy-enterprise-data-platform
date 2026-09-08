"""The canonical database contracts the generator has to produce rows for.

The estate DDL is the source of truth: ``oracle/`` for the ERP schemas and
``sqlserver/staging/tables/`` for the landing tables. Nothing here invents a
column - a generated extract is only ever a projection of a table that the
deployment actually creates, which is what makes the emitted SQL*Loader and
bcp artefacts loadable.

The DDL parsing itself is shared with the offline checks under
``validation/checks``; this module adds the parts the generator needs and the
checks do not: primary and unique keys, foreign keys, partition bounds, the
rows the deployment's own reference layer already seeds, and the CHECK
constraints - not merely the value sets of the simple ones, but every
constraint the estate declares, parsed by :mod:`wwigen.oracheck` so a row can
be held against it before SQL*Loader ever sees the file.
"""

from __future__ import annotations

import datetime
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

from . import oracheck  # noqa: E402

PK_RE = re.compile(
    r"ADD\s+CONSTRAINT\s+\w+\s+PRIMARY\s+KEY\s*\(([^)]*)\)", re.I)
INLINE_PK_RE = re.compile(r"CONSTRAINT\s+\w+\s+PRIMARY\s+KEY\s*\(([^)]*)\)", re.I)
FK_RE = re.compile(
    r"ADD\s+CONSTRAINT\s+\w+\s+FOREIGN\s+KEY\s*\(([^)]*)\)\s*"
    r"REFERENCES\s+([A-Z0-9_]+)\.([A-Z0-9_]+)\s*\(([^)]*)\)", re.I)
TABLE_HEADER_RE = re.compile(r"CREATE\s+TABLE\s+([A-Z0-9_]+)\.([A-Z0-9_]+)", re.I)
ALTER_HEADER_RE = re.compile(r"ALTER\s+TABLE\s+([A-Z0-9_]+)\.([A-Z0-9_]+)", re.I)
UNIQUE_RE = re.compile(r"CONSTRAINT\s+\w+\s+UNIQUE\s*\(([^)]*)\)", re.I)
CHECK_START_RE = re.compile(
    r"(?:CONSTRAINT\s+(\w+)\s+)?CHECK\s*\(", re.I)
PARTITION_BY_RE = re.compile(r"PARTITION\s+BY\s+RANGE\s*\(\s*([A-Z0-9_]+)\s*\)", re.I)
INTERVAL_RE = re.compile(r"\bINTERVAL\s*\(", re.I)
PARTITION_BOUND_RE = re.compile(
    r"PARTITION\s+([A-Z0-9_]+)\s+VALUES\s+LESS\s+THAN\s*\((.*?)\)\s*(?:,|\)|TABLESPACE)",
    re.I | re.S)
TO_DATE_RE = re.compile(r"TO_DATE\s*\(\s*'([^']+)'", re.I)


class OracleColumn:
    """One canonical Oracle column and everything a writer needs about it."""

    def __init__(self, name, type_name, precision_text, nullable, has_default,
                 default_text=""):
        self.name = name
        self.type_name = type_name.upper()
        self.precision_text = precision_text or ""
        self.nullable = nullable
        self.has_default = has_default
        self.default_text = default_text or ""

    @property
    def default_value(self):
        """The value a row that omits this column will hold, if it is knowable.

        ``DEFAULT 0`` and ``DEFAULT 'STD'`` are as much part of the row the
        engine will hold as anything the extract writes, so a constraint over
        such a column has a verdict before the load. ``SYSDATE`` and ``USER``
        do not: they are :data:`oracheck.UNKNOWN_VALUE`.
        """
        if not self.has_default:
            return oracheck.UNKNOWN_VALUE
        value = seed_value(self.default_text)
        return oracheck.UNKNOWN_VALUE if value is None else value

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


class OracleCheck:
    """One CHECK constraint, with its text parsed into an expression tree."""

    def __init__(self, name, text):
        self.name = name or ""
        self.text = " ".join(text.split())
        self.node = oracheck.parse(self.text)

    @property
    def readable(self):
        """Whether the constraint parsed into something evaluable."""
        return not isinstance(self.node, oracheck.Unknown)

    def evaluate(self, row):
        return self.node.evaluate(row)

    def columns(self):
        return tuple(sorted(set(self.node.columns())))

    def __repr__(self):
        return "OracleCheck(%s)" % (self.name or self.text)


class OraclePartition:
    """The range partitioning of one table, as the DDL declares it."""

    def __init__(self, column, interval, bounds):
        self.column = column
        self.interval = interval         # Oracle cuts further partitions itself
        self.bounds = tuple(bounds)      # [(partition name, high value or None)]

    @property
    def open_ended(self):
        """Whether a value beyond the last declared bound still has a home."""
        return self.interval or any(bound is None for _name, bound in self.bounds)

    @property
    def highest(self):
        values = [bound for _name, bound in self.bounds if bound is not None]
        return max(values) if values else None

    def accepts(self, value):
        """Whether a partition exists for ``value`` (ORA-14400 otherwise)."""
        if value is None:
            return False
        if self.open_ended:
            return True
        highest = self.highest
        return highest is not None and _as_date(value) is not None \
            and _as_date(value) < highest


def _as_date(value):
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return None


class OracleTable:
    def __init__(self, key, path):
        self.key = key
        self.path = path
        self.columns = []                # [OracleColumn] in declaration order
        self.by_name = {}
        self.primary_key = ()
        self.foreign_keys = {}           # column -> (parent key, parent column)
        self.allowed_values = {}         # column -> (value, ...)
        self.checks = []                 # [OracleCheck]
        self.unique_keys = []            # [(column, ...)] including the PK
        self.partition = None            # OraclePartition or None

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

    def violations(self, row):
        """The constraints ``row`` breaks: those the engine evaluates FALSE."""
        return tuple(check for check in self.checks if check.evaluate(row) is False)


def _balanced(text, start):
    """The text inside a parenthesis opened at ``start`` - 1."""
    depth, index = 1, start
    while index < len(text):
        character = text[index]
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if not depth:
                return text[start:index], index + 1
        index += 1
    return text[start:], len(text)


def _checks_in(statement):
    """Every CHECK constraint declared in one statement, name and body."""
    found, position = [], 0
    while True:
        match = CHECK_START_RE.search(statement, position)
        if match is None:
            return found
        body, position = _balanced(statement, match.end())
        found.append((match.group(1), body))


def _partition_of(statement):
    """The range partitioning declared by a CREATE TABLE statement."""
    header = PARTITION_BY_RE.search(statement)
    if header is None:
        return None
    bounds = []
    for name, expression in PARTITION_BOUND_RE.findall(statement):
        literal = TO_DATE_RE.search(expression)
        if literal:
            try:
                bounds.append((name.upper(), datetime.datetime.strptime(
                    literal.group(1)[:10], "%Y-%m-%d").date()))
                continue
            except ValueError:
                pass
        bounds.append((name.upper(), None))      # MAXVALUE, or unparsed
    return OraclePartition(header.group(1).upper(),
                           bool(INTERVAL_RE.search(statement)), bounds)


def _oracle_constraints(table, text):
    """PK, FK and CHECK-IN value sets for one table, from its own DDL file."""
    statements = re.split(r"\n\s*/\s*\n", oraclelib.strip_comments(text))
    for statement in statements:
        # A constraint belongs to the table its own statement names. Matching
        # on the name appearing anywhere would give a parent table the foreign
        # keys its children declare onto it.
        named = [match for match in (TABLE_HEADER_RE.search(statement),
                                     ALTER_HEADER_RE.search(statement)) if match]
        if named:
            if any("%s.%s" % (match.group(1).upper(), match.group(2).upper()) != table.key
                   for match in named):
                continue
        elif table.name.upper() not in statement.upper():
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
        for match in UNIQUE_RE.finditer(statement):
            columns = tuple(c.strip().upper() for c in match.group(1).split(","))
            if all(table.has(column) for column in columns) \
                    and columns not in table.unique_keys:
                table.unique_keys.append(columns)
        for name, body in _checks_in(statement):
            check = OracleCheck(name, body)
            if any(table.has(column) for column in check.node.columns()) \
                    or not check.readable:
                table.checks.append(check)
        if TABLE_HEADER_RE.search(statement):
            partition = _partition_of(statement)
            if partition and table.has(partition.column):
                table.partition = partition

    if table.primary_key and table.primary_key not in table.unique_keys:
        table.unique_keys.insert(0, table.primary_key)
    for check in table.checks:
        for column in check.node.columns():
            if not table.has(column):
                continue
            domain = oracheck.domain_of(check.node, column)
            if domain:
                existing = table.allowed_values.get(column)
                table.allowed_values[column] = tuple(
                    value for value in domain
                    if existing is None or value in existing) or domain


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
                                      name in parsed.has_default,
                                      parsed.defaults.get(name, ""))
                table.columns.append(column)
                table.by_name[name] = column
            _oracle_constraints(table, text)
            tables[table.key] = table
    _ORACLE_CACHE.update(root=REPO_ROOT, tables=tables)
    return tables


def seed_value(text):
    """One seed literal as the value the row would hold, or None."""
    literal = oraclelib.string_literal(text)
    if literal is not None:
        return literal
    stripped = text.strip()
    if oraclelib.is_null_literal(stripped):
        return None
    date = _date_literal(stripped)
    if date is not None:
        return date
    try:
        return int(stripped)
    except ValueError:
        pass
    try:
        return float(stripped)
    except ValueError:
        return None


DATE_LITERAL_RE = re.compile(r"^DATE\s*'(\d{4}-\d{2}-\d{2})'", re.I)


def _date_literal(text):
    """``DATE '2018-01-02'`` or ``TO_DATE('2018-01-02', ...)`` as a date.

    An effective date is part of the key the estate seeds - leaving it
    unparsed hid a seeded row from the uniqueness contract and turned it into
    ORA-00001 at load time.
    """
    for pattern in (DATE_LITERAL_RE, TO_DATE_RE):
        match = pattern.search(text)
        if match:
            try:
                return datetime.datetime.strptime(match.group(1)[:10], "%Y-%m-%d").date()
            except ValueError:
                return None
    return None


class OracleSeed:
    """The rows the deployment's reference layer already created.

    The generator does not own an empty database: ``oracle/reference`` and
    ``oracle/seed`` run before any extract is loaded, so a generated key that
    happens to equal a seeded one is ORA-00001 at load time. Holding the seed
    keys here is what lets generation avoid them deterministically, and what
    lets a child row legitimately reference a parent the generator never
    produced.
    """

    def __init__(self):
        self.rows = {}                   # table -> [{column: value}]

    def add(self, key, values):
        self.rows.setdefault(key, []).append(values)

    def values_of(self, key, column):
        """Every seeded value of one column, as a set."""
        return frozenset(
            row[column] for row in self.rows.get(key, ())
            if row.get(column) is not None)

    def keys_of(self, key, columns):
        """Every seeded value of one key, as a set of tuples."""
        found = set()
        for row in self.rows.get(key, ()):
            if all(row.get(column) is not None for column in columns):
                found.add(tuple(row[column] for column in columns))
        return found

    def count(self, key):
        return len(self.rows.get(key, ()))


_SEED_CACHE = {}


def oracle_seed():
    """The :class:`OracleSeed` for oracle/reference and oracle/seed."""
    if _SEED_CACHE.get("root") == REPO_ROOT:
        return _SEED_CACHE["seed"]
    previous = oraclelib.REPO_ROOT
    oraclelib.REPO_ROOT = REPO_ROOT
    try:
        rows = oraclelib.load_oracle_seed_rows()
    finally:
        oraclelib.REPO_ROOT = previous
    seed = OracleSeed()
    for row in rows:
        seed.add(row.key, {column: seed_value(text)
                           for column, text in row.values.items()})
    _SEED_CACHE.update(root=REPO_ROOT, seed=seed)
    return seed


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
    _SEED_CACHE.clear()
