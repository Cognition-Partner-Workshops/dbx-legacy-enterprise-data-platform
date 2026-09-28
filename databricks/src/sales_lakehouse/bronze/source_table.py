"""Declarative description of one legacy source table landed in bronze."""
from __future__ import annotations

import re
from dataclasses import dataclass

from pyspark.sql import types as T

SOURCE_SYSTEM_LABELS = {"sqlserver": "SQLSERVER_WWI_OLTP", "oracle": "ORACLE_WWIGERP"}

_SIMPLE_TYPES: dict[str, T.DataType] = {
    "string": T.StringType(),
    "boolean": T.BooleanType(),
    "tinyint": T.ByteType(),
    "smallint": T.ShortType(),
    "int": T.IntegerType(),
    "bigint": T.LongType(),
    "float": T.FloatType(),
    "double": T.DoubleType(),
    "date": T.DateType(),
    "timestamp": T.TimestampType(),
}


def parseSparkType(kind: str) -> T.DataType:
    """Parse the registry's type strings without needing a live SparkContext."""
    text = kind.strip().lower()
    if text in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[text]
    match = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", text)
    if match:
        return T.DecimalType(int(match.group(1)), int(match.group(2)))
    raise ValueError(f"unsupported registry type {kind!r}")


def toSnakeCase(name: str) -> str:
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    text = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", text)
    return text.lower()


@dataclass(frozen=True)
class SourceTable:
    system: str  # "sqlserver" | "oracle"
    schema: str  # source schema, verbatim (Sales, WWI_MDM, ...)
    table: str  # source table, verbatim (OrderLines, CUST_MASTER, ...)
    columns: tuple[tuple[str, str], ...]  # (source column name, spark type string) in DDL order
    naturalKey: tuple[str, ...]
    loadMode: str = "full"  # "full" | "incremental"
    watermarkColumn: str | None = None
    overlapMinutes: int = 0  # timestamp-watermark lookback (Integration.ChangeTrackingWatermark.OverlapMinutes)
    legacyPackage: str | None = None
    ddlFiles: tuple[str, ...] = ()

    @property
    def sourceObject(self) -> str:
        return f"{self.schema}.{self.table}"

    @property
    def sourceSystemLabel(self) -> str:
        return SOURCE_SYSTEM_LABELS[self.system]

    @property
    def bronzeTable(self) -> str:
        return f"{self.system}_{toSnakeCase(self.schema)}_{toSnakeCase(self.table)}"

    @property
    def relativePath(self) -> str:
        return f"{self.system}/{self.schema}/{self.table}.csv"

    @property
    def columnNames(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)

    def sparkType(self, column: str) -> T.DataType:
        return parseSparkType(dict(self.columns)[column])

    def sparkSchema(self) -> T.StructType:
        return T.StructType([T.StructField(name, parseSparkType(kind), True) for name, kind in self.columns])
