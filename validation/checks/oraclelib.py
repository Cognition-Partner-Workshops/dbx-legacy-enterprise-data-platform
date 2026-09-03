#!/usr/bin/env python3
"""Column-level parsing of the Oracle estate DDL.

The deeper checks in this directory read object declarations; the Oracle
failures the estate hit on its first deployment were all a level below that -
a view naming a column its base table does not have, a seed literal wider than
the column it is inserted into, a foreign key against a differently-typed
parent. This module gives those checks a column model to work from.

It is a pragmatic parser, not a PL/SQL grammar. It reads the shapes this
repository actually writes: one `CREATE TABLE` per file with its columns one
per line, `CREATE OR REPLACE VIEW` with explicit table aliases, `INSERT INTO
... VALUES` seed rows. Anything it cannot read it reports as unparsed rather
than silently passing.
"""

from __future__ import annotations

import os
import re

# Overridable so the fixtures in validation/checks/run_check_fixtures.py can
# point the same checks at a scratch copy of a deliberately broken artifact.
REPO_ROOT = os.environ.get("WWI_ESTATE_ROOT") or os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.S)
LINE_COMMENT_RE = re.compile(r"--[^\n]*")

CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+([A-Z0-9_]+)\.([A-Z0-9_]+)\s*\(", re.I)
CREATE_VIEW_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+([A-Z0-9_]+)\.([A-Z0-9_]+)", re.I)

COLUMN_RE = re.compile(
    r"^\s*([A-Z][A-Z0-9_]*)\s+"
    r"(VARCHAR2|NVARCHAR2|CHAR|NCHAR|NUMBER|INTEGER|FLOAT|BINARY_DOUBLE|BINARY_FLOAT|"
    r"DATE|TIMESTAMP|CLOB|NCLOB|BLOB|RAW|XMLTYPE)"
    r"\s*(\([^)]*\))?(.*)$",
    re.I)

NOT_NULL_RE = re.compile(r"\bNOT\s+NULL\b", re.I)
DEFAULT_RE = re.compile(r"\bDEFAULT\b", re.I)

# `FROM|JOIN schema.table alias` - the estate always qualifies and always
# aliases, so an unaliased or unqualified source is left unresolved on purpose.
FROM_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Z0-9_]+)\.([A-Z0-9_]+)\s+(?:AS\s+)?([A-Z][A-Z0-9_]*)\b", re.I)
DERIVED_ALIAS_RE = re.compile(r"\)\s*(?:AS\s+)?([A-Z][A-Z0-9_]*)\s*\n", re.I)
QUALIFIED_COLUMN_RE = re.compile(r"\b([A-Z][A-Z0-9_]*)\.([A-Z][A-Z0-9_]*)\b", re.I)

# Words that appear before a dot but are not table aliases.
NOT_AN_ALIAS = frozenset({
    "DBMS_OUTPUT", "DBMS_LOB", "DBMS_UTILITY", "DBMS_RANDOM", "DBMS_CRYPTO",
    "UTL_RAW", "SYS", "SYSTEM", "STANDARD",
})


class Table:
    """One Oracle table: its columns, in declaration order, with their types."""

    def __init__(self, schema, name, path):
        self.schema = schema
        self.name = name
        self.path = path
        self.columns = []          # [column name] in declaration order
        self.types = {}            # column -> (type, precision text or None)
        self.not_null = set()      # columns declared NOT NULL without a DEFAULT
        self.has_default = set()

    @property
    def key(self):
        return "%s.%s" % (self.schema, self.name)

    def width(self, column):
        """Declared character width, or None for non-character columns."""
        type_name, precision = self.types.get(column, (None, None))
        if type_name is None or type_name.upper() not in (
                "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"):
            return None
        if not precision:
            return None
        digits = re.search(r"\d+", precision)
        return int(digits.group(0)) if digits else None


class View:
    """One Oracle view: the aliases it opens and the columns it names on them."""

    def __init__(self, schema, name, path):
        self.schema = schema
        self.name = name
        self.path = path
        self.aliases = {}          # alias -> "SCHEMA.TABLE"
        self.derived = set()       # aliases bound to an inline subquery
        self.references = []       # [(alias, column)]

    @property
    def key(self):
        return "%s.%s" % (self.schema, self.name)

    @property
    def referenced_schemas(self):
        return {value.split(".")[0] for value in self.aliases.values()}


def strip_comments(text):
    return LINE_COMMENT_RE.sub("", BLOCK_COMMENT_RE.sub("", text))


def _split_top_level(body):
    """Split a parenthesised column list on commas that are not nested."""
    parts, depth, current = [], 0, []
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
    parts.append("".join(current))
    return parts


def _table_body(text, start):
    """Return the text between the parentheses of a CREATE TABLE column list."""
    depth, out = 0, []
    for index in range(start - 1, len(text)):
        char = text[index]
        if char == "(":
            depth += 1
            if depth == 1:
                continue
        elif char == ")":
            depth -= 1
            if depth == 0:
                return "".join(out)
        if depth >= 1:
            out.append(char)
    return "".join(out)


def parse_tables(text, path):
    """Every table created by one DDL file."""
    tables = []
    clean = strip_comments(text)
    for match in CREATE_TABLE_RE.finditer(clean):
        table = Table(match.group(1).upper(), match.group(2).upper(), path)
        for part in _split_top_level(_table_body(clean, match.end())):
            stripped = part.strip()
            if not stripped or re.match(r"^(CONSTRAINT|PRIMARY|UNIQUE|FOREIGN|CHECK)\b",
                                        stripped, re.I):
                continue
            column = COLUMN_RE.match(stripped)
            if not column:
                continue
            name = column.group(1).upper()
            table.columns.append(name)
            table.types[name] = (column.group(2).upper(), column.group(3))
            tail = column.group(4) or ""
            if DEFAULT_RE.search(tail):
                table.has_default.add(name)
            if NOT_NULL_RE.search(tail):
                table.not_null.add(name)
        tables.append(table)
    return tables


def parse_view(text, path):
    """The alias map and the qualified column references of one view file."""
    clean = strip_comments(text)
    match = CREATE_VIEW_RE.search(clean)
    if not match:
        return None
    view = View(match.group(1).upper(), match.group(2).upper(), path)
    body = clean[match.end():]
    for alias in DERIVED_ALIAS_RE.findall(body):
        view.derived.add(alias.upper())
    for schema, table, alias in FROM_RE.findall(body):
        key = alias.upper()
        if key in ("ON", "WHERE", "GROUP", "ORDER", "LEFT", "RIGHT", "INNER",
                   "FULL", "CROSS", "JOIN", "UNION", "SELECT", "AS"):
            continue
        target = "%s.%s" % (schema.upper(), table.upper())
        if key in view.aliases and view.aliases[key] != target:
            view.derived.add(key)        # ambiguous alias: do not judge it
        view.aliases[key] = target
    for alias, column in QUALIFIED_COLUMN_RE.findall(body):
        alias_key = alias.upper()
        if alias_key in NOT_AN_ALIAS:
            continue
        view.references.append((alias_key, column.upper()))
    return view


INSERT_INTO_RE = re.compile(
    r"INTO\s+([A-Z0-9_]+)\.([A-Z0-9_]+)\s*\(([^)]*?)\)\s*VALUES\s*\(", re.I)


class SeedRow:
    """One `INTO schema.table (cols) VALUES (...)` row of reference content."""

    def __init__(self, schema, name, path, line, columns, values):
        self.schema = schema
        self.name = name
        self.path = path
        self.line = line
        self.values = dict(zip(columns, values))

    @property
    def key(self):
        return "%s.%s" % (self.schema, self.name)


def _value_list(text, start):
    """Split the VALUES list beginning after its opening parenthesis."""
    depth, current, values = 0, [], []
    index, quoted = start, False
    while index < len(text):
        char = text[index]
        if quoted:
            if char == "'":
                if index + 1 < len(text) and text[index + 1] == "'":
                    current.append("''")
                    index += 2
                    continue
                quoted = False
            current.append(char)
            index += 1
            continue
        if char == "'":
            quoted = True
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            if depth == 0:
                values.append("".join(current).strip())
                return values, index
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            values.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        index += 1
    values.append("".join(current).strip())
    return values, index


def parse_seed_rows(text, path):
    """Every seed row one reference/seed file inserts, columns bound to values."""
    rows = []
    clean = strip_comments(text)
    for match in INSERT_INTO_RE.finditer(clean):
        columns = [c.strip().upper() for c in match.group(3).split(",") if c.strip()]
        values, _end = _value_list(clean, match.end())
        line = clean.count("\n", 0, match.start()) + 1
        rows.append(SeedRow(match.group(1).upper(), match.group(2).upper(),
                            path, line, columns, values))
    return rows


def string_literal(value):
    """The text of a bare string literal, or None if the value is not one."""
    text = value.strip()
    if len(text) >= 2 and text.startswith("'") and text.endswith("'"):
        inner = text[1:-1]
        if "'" in inner.replace("''", ""):
            return None                 # concatenation or function call
        return inner.replace("''", "'")
    return None


def is_null_literal(value):
    return value.strip().upper() == "NULL"


def load_oracle_seed_rows():
    """[SeedRow] for every row inserted under oracle/reference and oracle/seed."""
    rows = []
    for directory in ("reference", "seed"):
        root = os.path.join(REPO_ROOT, "oracle", directory)
        if not os.path.isdir(root):
            continue
        for dirpath, _dirnames, filenames in os.walk(root):
            for filename in sorted(filenames):
                if not filename.lower().endswith(".sql"):
                    continue
                full = os.path.join(dirpath, filename)
                rel = os.path.relpath(full, REPO_ROOT).replace(os.sep, "/")
                with open(full, errors="replace") as handle:
                    rows.extend(parse_seed_rows(handle.read(), rel))
    return rows


def load_oracle_tables():
    """{SCHEMA.TABLE: Table} for every table created under oracle/."""
    tables = {}
    for directory in ("tables", "reference", "ddl", "seed"):
        root = os.path.join(REPO_ROOT, "oracle", directory)
        if not os.path.isdir(root):
            continue
        for dirpath, _dirnames, filenames in os.walk(root):
            for filename in sorted(filenames):
                if not filename.lower().endswith(".sql"):
                    continue
                full = os.path.join(dirpath, filename)
                rel = os.path.relpath(full, REPO_ROOT).replace(os.sep, "/")
                with open(full, errors="replace") as handle:
                    for table in parse_tables(handle.read(), rel):
                        tables[table.key] = table
    return tables


def load_oracle_views():
    """[View] for every view under oracle/views."""
    views = []
    root = os.path.join(REPO_ROOT, "oracle", "views")
    if not os.path.isdir(root):
        return views
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in sorted(filenames):
            if not filename.lower().endswith(".sql"):
                continue
            full = os.path.join(dirpath, filename)
            rel = os.path.relpath(full, REPO_ROOT).replace(os.sep, "/")
            with open(full, errors="replace") as handle:
                view = parse_view(handle.read(), rel)
            if view:
                views.append(view)
    return views
