"""Column and table descriptors shared by the writers and the loader emitters.

A :class:`TableSpec` is the single declaration of a generated object: its
target system, its columns and their physical types, the delimiters and
encoding of its extract file, and the callable that streams its rows. The
SQL*Loader control files, the bcp format files and the BULK INSERT scripts are
all derived from this one declaration, so a column added in one place cannot
drift out of the loader artefacts.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Physical types. Deliberately few: this is a flat-file interchange contract,
# not a database DDL.
STRING = "string"
INTEGER = "integer"
DECIMAL = "decimal"
DATE = "date"
TIMESTAMP = "timestamp"
FLAG = "flag"

ORACLE = "oracle"
SQLSERVER = "sqlserver"
FILE_FEED = "file"


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    length: int = 0
    precision: int = 0
    scale: int = 0
    nullable: bool = True
    note: str = ""

    @property
    def sqlloader_type(self) -> str:
        if self.type == DATE:
            return 'DATE "YYYY-MM-DD"'
        if self.type == TIMESTAMP:
            return 'TIMESTAMP "YYYY-MM-DD HH24:MI:SS"'
        if self.type in (INTEGER, DECIMAL):
            return "CHAR(%d)" % max(self.length, 40)
        return "CHAR(%d)" % max(self.length, 1)

    @property
    def tsql_type(self) -> str:
        if self.type == DATE:
            return "date"
        if self.type == TIMESTAMP:
            return "datetime2(0)"
        if self.type == INTEGER:
            return "bigint"
        if self.type == DECIMAL:
            return "decimal(%d,%d)" % (self.precision or 19, self.scale or 2)
        if self.type == FLAG:
            return "char(1)"
        return "nvarchar(%d)" % max(self.length, 1)


def str_col(name: str, length: int, nullable: bool = True, note: str = "") -> Column:
    return Column(name, STRING, length=length, nullable=nullable, note=note)


def int_col(name: str, nullable: bool = True, note: str = "") -> Column:
    return Column(name, INTEGER, length=20, nullable=nullable, note=note)


def dec_col(name: str, precision: int = 19, scale: int = 2, nullable: bool = True,
            note: str = "") -> Column:
    return Column(name, DECIMAL, length=precision + 3, precision=precision, scale=scale,
                  nullable=nullable, note=note)


def date_col(name: str, nullable: bool = True, note: str = "") -> Column:
    return Column(name, DATE, length=10, nullable=nullable, note=note)


def ts_col(name: str, nullable: bool = True, note: str = "") -> Column:
    return Column(name, TIMESTAMP, length=19, nullable=nullable, note=note)


def flag_col(name: str, nullable: bool = True, note: str = "") -> Column:
    return Column(name, FLAG, length=1, nullable=nullable, note=note)


@dataclass(frozen=True)
class TableSpec:
    """One generated object and everything needed to load it."""

    key: str                      # unique generator key, e.g. "oracle.WWI_MDM.CUST_MASTER"
    system: str                   # ORACLE | SQLSERVER | FILE_FEED
    schema: str                   # Oracle schema, SQL Server schema, or feed folder
    name: str                     # object or feed name
    columns: tuple
    produce: object               # callable(cfg, ctx) -> iterator of row tuples
    row_count_key: str = ""       # scale key that drives the row count, if any
    delimiter: str = "|"
    encoding: str = "utf-8"
    line_terminator: str = "\n"
    header: bool = False
    extension: str = "dat"
    target_object: str = ""       # the staging/raw object the loader targets
    group: str = ""               # logical group used by --only / --group
    description: str = ""
    allows_defects: bool = False
    tags: tuple = field(default_factory=tuple)

    @property
    def qualified_name(self) -> str:
        return "%s.%s" % (self.schema, self.name)

    @property
    def file_stem(self) -> str:
        return "%s.%s" % (self.schema, self.name)

    def relative_path(self) -> str:
        return "%s/%s/%s.%s" % (self.system, self.schema, self.name, self.extension)
