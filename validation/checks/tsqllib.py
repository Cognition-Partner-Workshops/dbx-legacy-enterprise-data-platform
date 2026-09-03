#!/usr/bin/env python3
"""Column-level parsing of the SQL Server estate DDL.

The Oracle side has oraclelib; this is its T-SQL counterpart, and it exists for
the same reason: the failures the estate hit on its first deployment were all a
level below object names - an INSERT naming a column the table does not have, a
foreign key against a differently-typed parent, a procedure writing a computed
column, a call passing the wrong number of arguments to a scalar function.

It is a pragmatic parser, not a T-SQL grammar. It reads the shapes this
repository writes: one `CREATE TABLE` per object with its columns one per line,
`ALTER TABLE ... ADD` extension blocks guarded by COL_LENGTH, bracketed
identifiers, and `CREATE FUNCTION`/`CREATE PROCEDURE` parameter lists.
"""

from __future__ import annotations

import os
import re

REPO_ROOT = os.environ.get("WWI_ESTATE_ROOT") or os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.S)
LINE_COMMENT_RE = re.compile(r"--[^\n]*")

NAME = r"(?:\[[^\]]+\]|\w+)"
CREATE_TABLE_RE = re.compile(r"CREATE\s+TABLE\s+(%s)\s*\.\s*(%s)\s*\(" % (NAME, NAME), re.I)
ALTER_ADD_RE = re.compile(
    r"ALTER\s+TABLE\s+(%s)\s*\.\s*(%s)\s+ADD\s+(.*?);" % (NAME, NAME), re.I | re.S)
CREATE_ROUTINE_RE = re.compile(
    r"CREATE\s+(?:OR\s+ALTER\s+)?(FUNCTION|PROCEDURE|PROC)\s+(%s)\s*\.\s*(%s)" % (NAME, NAME), re.I)
INSERT_RE = re.compile(
    r"INSERT\s+INTO\s+(%s)\s*\.\s*(%s)\s*\(([^;]*?)\)\s*(?:VALUES|SELECT|EXEC)" % (NAME, NAME),
    re.I | re.S)
CREATE_INDEX_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?(?:CLUSTERED\s+|NONCLUSTERED\s+)?(?:COLUMNSTORE\s+)?INDEX\s+"
    r"%s\s+ON\s+(%s)\s*\.\s*(%s)\s*\(([^)]*)\)" % (NAME, NAME, NAME), re.I | re.S)
FOREIGN_KEY_RE = re.compile(
    r"FOREIGN\s+KEY\s*\(([^)]*)\)\s*REFERENCES\s+(%s)\s*\.\s*(%s)\s*\(([^)]*)\)" % (NAME, NAME),
    re.I | re.S)

COMPUTED_RE = re.compile(r"^\s*(%s)\s+AS\s" % NAME, re.I)
COLUMN_RE = re.compile(
    r"^\s*(%s)\s+"
    r"(\[?\w+\]?)"                      # type, possibly a schema-less UDT
    r"\s*(\([^)]*\))?(.*)$" % NAME,
    re.I | re.S)

TYPE_WORDS = frozenset("""
    bit tinyint smallint int bigint decimal numeric money smallmoney float real
    date datetime datetime2 datetimeoffset smalldatetime time char varchar text
    nchar nvarchar ntext binary varbinary image uniqueidentifier xml sql_variant
    geography geometry hierarchyid rowversion timestamp
""".split())

NOT_NULL_RE = re.compile(r"\bNOT\s+NULL\b", re.I)


def strip_comments(text):
    return LINE_COMMENT_RE.sub("", BLOCK_COMMENT_RE.sub("", text))


def unbracket(name):
    return name.strip().strip("[]").strip()


def split_top_level(body, separator=","):
    parts, depth, current = [], 0, []
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == separator and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return parts


def balanced_body(text, start):
    """Text between the parentheses whose opener precedes index `start`."""
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


class Table:
    """One SQL Server table: columns with their types, and which are computed."""

    def __init__(self, schema, name, path):
        self.schema = schema
        self.name = name
        self.path = path
        self.columns = []
        # SQL Server's default collation is case insensitive, so every lookup
        # here folds case: [Vat Rate] and [VAT Rate] are the same column.
        self.folded = {}               # upper-cased column -> column as written
        self.types = {}                # column -> normalised type text
        self.computed = set()
        self.not_null = set()
        self.paths = [path]
        # False when only ALTER TABLE ... ADD extensions were seen: the base
        # table belongs to the Microsoft sample, so the column list here is a
        # fragment and nothing may be concluded from a column's absence.
        self.created = False

    @property
    def key(self):
        return "%s.%s" % (self.schema, self.name)

    def has(self, column):
        return column.upper() in self.folded

    def type_of(self, column):
        return self.types.get(self.folded.get(column.upper(), column))

    def is_computed(self, column):
        return self.folded.get(column.upper(), column) in self.computed

    def add_column(self, definition):
        computed = COMPUTED_RE.match(definition)
        if computed:
            name = unbracket(computed.group(1))
            self.columns.append(name)
            self.folded[name.upper()] = name
            self.computed.add(name)
            self.types[name] = "computed"
            return
        match = COLUMN_RE.match(definition)
        if not match:
            return
        type_name = unbracket(match.group(2)).lower()
        if type_name not in TYPE_WORDS:
            return                      # a constraint or something we do not model
        name = unbracket(match.group(1))
        precision = (match.group(3) or "").replace(" ", "").lower()
        self.columns.append(name)
        self.folded[name.upper()] = name
        self.types[name] = type_name + precision
        if NOT_NULL_RE.search(match.group(4) or ""):
            self.not_null.add(name)


class Routine:
    """One function or procedure: its parameters and whether they have defaults."""

    def __init__(self, kind, schema, name, path, parameters, defaulted):
        self.kind = kind
        self.schema = schema
        self.name = name
        self.path = path
        self.parameters = parameters
        self.defaulted = defaulted

    @property
    def key(self):
        return "%s.%s" % (self.schema, self.name)

    @property
    def minimum_arguments(self):
        return len(self.parameters) - self.defaulted


def parse_tables(text, path, tables):
    """Add every table created or extended by one file to `tables`."""
    clean = strip_comments(text)
    for match in CREATE_TABLE_RE.finditer(clean):
        table = tables.get("%s.%s" % (unbracket(match.group(1)), unbracket(match.group(2))))
        if table is None:
            table = Table(unbracket(match.group(1)), unbracket(match.group(2)), path)
        table.created = True
        for part in split_top_level(balanced_body(clean, match.end())):
            stripped = part.strip()
            if not stripped or re.match(r"^(CONSTRAINT|PRIMARY|UNIQUE|FOREIGN|CHECK|INDEX)\b",
                                        stripped, re.I):
                continue
            table.add_column(stripped)
        tables[table.key] = table

    for schema, name, body in ALTER_ADD_RE.findall(clean):
        key = "%s.%s" % (unbracket(schema), unbracket(name))
        table = tables.get(key)
        if table is None:
            table = Table(unbracket(schema), unbracket(name), path)
            tables[key] = table
        if re.match(r"^\s*(CONSTRAINT|PRIMARY|UNIQUE|FOREIGN|CHECK)\b", body, re.I):
            continue
        for part in split_top_level(body):
            table.add_column(part.strip())
        if path not in table.paths:
            table.paths.append(path)


def parse_routines(text, path):
    """[Routine] for every function or procedure created in one file."""
    clean = strip_comments(text)
    routines = []
    for match in CREATE_ROUTINE_RE.finditer(clean):
        kind = match.group(1).upper()
        tail = clean[match.end():]
        opener = tail.find("(")
        as_at = re.search(r"\bAS\b", tail, re.I)
        body = ""
        if opener != -1 and (as_at is None or opener < as_at.start()):
            body = balanced_body(tail, opener + 1)
        else:                            # procedure with an unparenthesised list
            end = as_at.start() if as_at else 0
            body = tail[:end]
        parameters, defaulted = [], 0
        for part in split_top_level(body):
            stripped = part.strip()
            if not stripped.startswith("@"):
                continue
            parameters.append(unbracket(stripped.split()[0]))
            if "=" in stripped:
                defaulted += 1
        routines.append(Routine(kind, unbracket(match.group(2)), unbracket(match.group(3)),
                                path, parameters, defaulted))
    return routines


def sql_files(subdirectories=("sqlserver",)):
    """[(repo-relative path, text)] for every .sql file under the given roots."""
    files = []
    for subdirectory in subdirectories:
        root = os.path.join(REPO_ROOT, *subdirectory.split("/"))
        if not os.path.isdir(root):
            continue
        for dirpath, _dirnames, filenames in os.walk(root):
            for filename in sorted(filenames):
                if not filename.lower().endswith(".sql"):
                    continue
                full = os.path.join(dirpath, filename)
                rel = os.path.relpath(full, REPO_ROOT).replace(os.sep, "/")
                with open(full, errors="replace") as handle:
                    files.append((rel, handle.read()))
    return sorted(files)


def load_tables(files=None):
    """{schema.table: Table} across the SQL Server estate, extensions applied."""
    tables = {}
    for path, text in files if files is not None else sql_files():
        parse_tables(text, path, tables)
    return tables


def load_routines(files=None):
    """{schema.routine: Routine} across the SQL Server estate."""
    routines = {}
    for path, text in files if files is not None else sql_files():
        for routine in parse_routines(text, path):
            routines[routine.key] = routine
    return routines
