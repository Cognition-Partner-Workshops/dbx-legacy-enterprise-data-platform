"""Small helpers shared by the procurement transforms."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

MONEY = T.DecimalType(18, 2)
PERCENT = T.DecimalType(9, 4)

FAR_FUTURE = "9999-12-31"


def money(col: Column) -> Column:
    return col.cast(MONEY)


def zeroMoney() -> Column:
    return F.lit(Decimal("0.00")).cast(MONEY)


def parseBool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "yes", "y", "t")


def parseDate(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value).strip()[:10], "%Y-%m-%d").date()


def parseInt(value, default: int = 0) -> int:
    if value is None or str(value).strip() == "":
        return default
    return int(str(value).strip())


def withBatchColumns(df: DataFrame, batchId: int, loadedAtUtc: datetime | None = None) -> DataFrame:
    ts = F.lit(loadedAtUtc) if loadedAtUtc is not None else F.current_timestamp()
    return df.withColumn("BatchId", F.lit(int(batchId)).cast("bigint")).withColumn("LoadedAtUtc", ts)


def nullSafeDiv(numerator: Column, denominator: Column) -> Column:
    """T-SQL style `x / (d == 0 ? 1 : d)` guard used throughout the package expressions."""
    return numerator / F.when(denominator == 0, F.lit(1)).otherwise(denominator)
