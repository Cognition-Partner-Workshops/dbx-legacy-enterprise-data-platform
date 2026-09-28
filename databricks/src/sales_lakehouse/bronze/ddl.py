"""Parse the checked-in legacy DDL (T-SQL and Oracle) into Spark column types.

Used by ``generate_registry`` to derive the bronze source registry and by the
tests to assert the checked-in registry still matches the DDL.
"""
from __future__ import annotations

import glob
import os
import re
from dataclasses import dataclass, field

SQLSERVER_DDL_DIRS = (
    "wwi-ssdt/wwi-ssdt/{schema}/Tables/{table}.sql",
    "sqlserver/oltp/01_tables/*_{schema}.{table}.sql",
    "sqlserver/oltp/02_extensions/*_{schema}.{table}.sql",
    "sqlserver/oltp/02_extensions/*_{schema}.{table}.Extensions.sql",
)
ORACLE_DDL_DIRS = ("oracle/tables/{schema}.{table}.sql",)


@dataclass
class ParsedTable:
    schema: str
    table: str
    columns: list[tuple[str, str]] = field(default_factory=list)  # (name, spark type string)
    primaryKey: list[str] = field(default_factory=list)
    computedColumns: list[str] = field(default_factory=list)
    ddlFiles: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Type mapping
# --------------------------------------------------------------------------- #
_SQLSERVER_SIMPLE = {
    "INT": "int",
    "INTEGER": "int",
    "BIGINT": "bigint",
    "SMALLINT": "smallint",
    "TINYINT": "tinyint",
    "BIT": "boolean",
    "MONEY": "decimal(19,4)",
    "SMALLMONEY": "decimal(10,4)",
    "FLOAT": "double",
    "REAL": "float",
    "DATE": "date",
    "DATETIME": "timestamp",
    "DATETIME2": "timestamp",
    "SMALLDATETIME": "timestamp",
    "DATETIMEOFFSET": "timestamp",
    "TIME": "string",
    "UNIQUEIDENTIFIER": "string",
    "XML": "string",
    "ROWVERSION": "string",
    "TIMESTAMP": "string",
    "SQL_VARIANT": "string",
}


def sqlServerTypeToSpark(ddlType: str) -> str:
    text = ddlType.strip()
    if text.lower().startswith("[sys]."):
        return "string"  # geography / hierarchyid land as WKT / text in the CSV extract
    match = re.match(r"([A-Za-z0-9_]+)\s*(?:\(\s*([^)]*)\))?", text)
    if match is None:
        raise ValueError(f"unparseable SQL Server type: {ddlType!r}")
    base = match.group(1).upper()
    args = match.group(2)
    if base in ("DECIMAL", "NUMERIC"):
        precision, scale = 18, 0
        if args:
            parts = [p.strip() for p in args.split(",")]
            precision = int(parts[0])
            scale = int(parts[1]) if len(parts) > 1 else 0
        return f"decimal({precision},{scale})"
    if base in ("NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "NTEXT", "TEXT", "VARBINARY", "BINARY", "IMAGE"):
        return "string"
    if base in _SQLSERVER_SIMPLE:
        return _SQLSERVER_SIMPLE[base]
    raise ValueError(f"unmapped SQL Server type: {ddlType!r}")


def oracleTypeToSpark(ddlType: str) -> str:
    text = ddlType.strip()
    match = re.match(r"([A-Za-z0-9_]+)\s*(?:\(\s*([^)]*)\))?", text)
    if match is None:
        raise ValueError(f"unparseable Oracle type: {ddlType!r}")
    base = match.group(1).upper()
    args = match.group(2)
    if base in ("NUMBER", "DECIMAL", "NUMERIC"):
        if not args:
            return "decimal(38,10)"
        parts = [p.strip() for p in args.split(",")]
        precision = int(parts[0])
        scale = int(parts[1]) if len(parts) > 1 else 0
        return f"decimal({precision},{scale})"
    if base in ("INTEGER", "INT", "SMALLINT"):
        return "decimal(38,0)"
    if base in ("FLOAT", "BINARY_DOUBLE"):
        return "double"
    if base == "BINARY_FLOAT":
        return "float"
    if base in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG", "RAW", "BLOB", "ROWID"):
        # LEGACY QUIRK: Oracle Y/N flags are CHAR/VARCHAR2(1) and stay strings in bronze.
        return "string"
    if base in ("DATE", "TIMESTAMP"):
        # Oracle DATE carries a time component; land as timestamp.
        return "timestamp"
    raise ValueError(f"unmapped Oracle type: {ddlType!r}")


# --------------------------------------------------------------------------- #
# T-SQL parsing
# --------------------------------------------------------------------------- #
_TSQL_CREATE = re.compile(r"CREATE\s+TABLE\s+\[(\w+)\]\.\[(\w+)\]\s*\(", re.IGNORECASE)
_TSQL_COLUMN = re.compile(
    r"^\s*\[(\w+)\]\s+(\[sys\]\.\[\w+\]|[A-Za-z0-9_]+(?:\s*\(\s*(?:MAX|\d+(?:\s*,\s*\d+)?)\s*\))?)",
    re.IGNORECASE,
)
_TSQL_COMPUTED = re.compile(r"^\s*\[(\w+)\]\s+AS\b", re.IGNORECASE)
_TSQL_PK = re.compile(r"PRIMARY\s+KEY\s+(?:CLUSTERED|NONCLUSTERED)?\s*\(([^)]*)\)", re.IGNORECASE)
_TSQL_ALTER_ADD = re.compile(
    r"ALTER\s+TABLE\s+\[(\w+)\]\.\[(\w+)\]\s+ADD\s+\[(\w+)\]\s+"
    r"(\[sys\]\.\[\w+\]|[A-Za-z0-9_]+(?:\s*\(\s*(?:MAX|\d+(?:\s*,\s*\d+)?)\s*\))?)",
    re.IGNORECASE,
)
_SKIP_LINE = re.compile(
    r"^\s*(CONSTRAINT\b|PERIOD\s+FOR\b|INDEX\b|PRIMARY\s+KEY\b|UNIQUE\s*\(|FOREIGN\s+KEY\b|CHECK\s*\(|--)",
    re.IGNORECASE,
)


def _stripBlockComments(text: str) -> str:
    return re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)


def parseSqlServerDdl(text: str, schema: str, table: str, parsed: ParsedTable | None = None) -> ParsedTable:
    """Merge CREATE TABLE columns and ``ALTER TABLE ... ADD [col] type`` columns."""
    result = parsed or ParsedTable(schema=schema, table=table)
    text = _stripBlockComments(text.lstrip("\ufeff"))
    for match in _TSQL_CREATE.finditer(text):
        if match.group(1).lower() != schema.lower() or match.group(2).lower() != table.lower():
            continue
        body = text[match.end():]
        depth = 1
        end = 0
        for idx, ch in enumerate(body):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    end = idx
                    break
        body = body[:end]
        for line in body.splitlines():
            stripped = line.strip()
            if not stripped or _SKIP_LINE.match(stripped):
                continue
            if _TSQL_COMPUTED.match(line):
                result.computedColumns.append(_TSQL_COMPUTED.match(line).group(1))
                continue
            col = _TSQL_COLUMN.match(line)
            if col:
                result.columns.append((col.group(1), sqlServerTypeToSpark(col.group(2))))
        pk = _TSQL_PK.search(body)
        if pk:
            result.primaryKey = [c.strip().strip("[]").split("]")[0] for c in re.findall(r"\[(\w+)\]", pk.group(1))]
    for match in _TSQL_ALTER_ADD.finditer(text):
        if match.group(1).lower() != schema.lower() or match.group(2).lower() != table.lower():
            continue
        name = match.group(3)
        if name not in [c for c, _ in result.columns]:
            result.columns.append((name, sqlServerTypeToSpark(match.group(4))))
    return result


# --------------------------------------------------------------------------- #
# Oracle parsing
# --------------------------------------------------------------------------- #
_ORA_CREATE = re.compile(r"CREATE\s+TABLE\s+(\w+)\.(\w+)\s*\(", re.IGNORECASE)
_ORA_COLUMN = re.compile(
    r"^\s*([A-Za-z][A-Za-z0-9_#$]*)\s+"
    r"((?:NUMBER|VARCHAR2|NVARCHAR2|CHAR|NCHAR|DATE|TIMESTAMP|CLOB|NCLOB|BLOB|RAW|INTEGER|INT|SMALLINT|FLOAT|"
    r"BINARY_DOUBLE|BINARY_FLOAT|DECIMAL|NUMERIC|LONG)(?:\s*\([^)]*\))?)",
    re.IGNORECASE,
)
_ORA_PK = re.compile(r"PRIMARY\s+KEY\s*\(([^)]*)\)", re.IGNORECASE)


def parseOracleDdl(text: str, schema: str, table: str, parsed: ParsedTable | None = None) -> ParsedTable:
    result = parsed or ParsedTable(schema=schema, table=table)
    text = _stripBlockComments(text)
    for match in _ORA_CREATE.finditer(text):
        if match.group(1).upper() != schema.upper() or match.group(2).upper() != table.upper():
            continue
        body = text[match.end():]
        depth = 1
        end = 0
        for idx, ch in enumerate(body):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    end = idx
                    break
        body = body[:end]
        for line in body.splitlines():
            stripped = line.strip()
            if not stripped or _SKIP_LINE.match(stripped):
                continue
            col = _ORA_COLUMN.match(line)
            if col:
                result.columns.append((col.group(1).upper(), oracleTypeToSpark(col.group(2))))
        pk = _ORA_PK.search(body)
        if pk:
            result.primaryKey = [c.strip().upper() for c in pk.group(1).split(",")]
    return result


# --------------------------------------------------------------------------- #
# File discovery
# --------------------------------------------------------------------------- #
def ddlFilesFor(repoRoot: str, system: str, schema: str, table: str) -> list[str]:
    patterns = SQLSERVER_DDL_DIRS if system == "sqlserver" else ORACLE_DDL_DIRS
    files: list[str] = []
    for pattern in patterns:
        files.extend(sorted(glob.glob(os.path.join(repoRoot, pattern.format(schema=schema, table=table)))))
    return files


def parseTableFromRepo(repoRoot: str, system: str, schema: str, table: str) -> ParsedTable:
    parsed = ParsedTable(schema=schema, table=table)
    for path in ddlFilesFor(repoRoot, system, schema, table):
        with open(path, encoding="utf-8-sig") as handle:
            text = handle.read()
        if system == "sqlserver":
            parseSqlServerDdl(text, schema, table, parsed)
        else:
            parseOracleDdl(text, schema, table, parsed)
        parsed.ddlFiles.append(os.path.relpath(path, repoRoot))
    if not parsed.columns:
        raise FileNotFoundError(f"no DDL columns found for {system} {schema}.{table} under {repoRoot}")
    return parsed


def findRepoRoot(start: str | None = None) -> str:
    path = os.path.abspath(start or os.path.dirname(__file__))
    while path != os.path.dirname(path):
        if os.path.isdir(os.path.join(path, "oracle", "tables")) and os.path.isdir(os.path.join(path, "sqlserver")):
            return path
        path = os.path.dirname(path)
    raise FileNotFoundError("could not locate the legacy repository root (needs oracle/tables and sqlserver/)")
