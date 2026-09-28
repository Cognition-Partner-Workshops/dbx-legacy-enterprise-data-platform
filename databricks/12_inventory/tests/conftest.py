"""Local PySpark harness for the WWI_Inventory transforms.

`tests/fakes` provides a minimal stand-in for `dbx_etl_common` so `inv_common.runtime`
imports; the real package (session 00) is what runs on Databricks.
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "fakes"))
sys.path.insert(0, str(HERE.parent / "src"))


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("wwi_12_inventory_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


SNAPSHOT_DATE = date(2024, 3, 15)
NOW_UTC = datetime(2024, 3, 15, 12, 0, 0)


@pytest.fixture(scope="session")
def position(spark):
    cols = "StockItemId int, WarehouseSiteCode string, BinLocationCode string, SnapshotDate date, QuantityOnHand int, " \
           "QuantityAllocated int, QuantityOnOrder int, QuantityInTransit int, LastMovementDate date, ReceiptDate date"
    rows = [
        # fresh, chiller, not expired
        (1, "LDN", "A1", SNAPSHOT_DATE, 100, 20, 10, 0, date(2024, 3, 1), date(2024, 3, 1)),
        # D365P, negative available
        (2, "LDN", "A2", SNAPSHOT_DATE, 10, 15, 0, 0, date(2022, 1, 1), date(2022, 1, 1)),
        # D180
        (3, "SYD", "B1", SNAPSHOT_DATE, 40, 0, 0, 5, date(2023, 8, 1), date(2023, 8, 1)),
        # D090, expired chiller (receipt 40 days ago, shelf life 30)
        (4, "SYD", "B2", SNAPSHOT_DATE, 8, 0, 0, 0, date(2023, 12, 1), date(2024, 2, 4)),
        # NEVER moved, unknown to the dimension -> reject
        (5, "LDN", "A3", SNAPSHOT_DATE, 3, 0, 0, 0, None, None),
        # other date -> filtered out
        (1, "LDN", "A1", date(2024, 3, 14), 999, 0, 0, 0, date(2024, 3, 1), date(2024, 3, 1)),
    ]
    return spark.createDataFrame(rows, cols)


@pytest.fixture(scope="session")
def currentPosition(position):
    """Operational 'current' position (one row per item/site/bin) as the intraday packages read it."""
    return position.where(position.SnapshotDate == SNAPSHOT_DATE)


@pytest.fixture(scope="session")
def stockItem(spark):
    cols = "StockItemId int, StockItemName string, SupplierId int, LeadTimeDays int, ReorderLevel int, TargetStockLevel int, " \
           "QuantityPerOuter int, IsChillerStock boolean, RegionCode string, IsDiscontinued boolean, UnitCost decimal(18,4), ShelfLifeDays int"
    rows = [
        (1, "Chiller item", 10, 7, 50, 200, 12, True, "EU", False, Decimal("2.5000"), 30),
        (2, "Old stock", 10, 14, 10, 50, 1, False, "NA", False, Decimal("10.0000"), None),
        (3, "APAC item", 11, 10, 20, 100, 6, False, "APAC", False, Decimal("4.0000"), None),
        (4, "Expired chiller", 11, 5, 5, 20, 1, True, "APAC", False, Decimal("1.0000"), 30),
        (5, "Unknown to DW", 12, 5, 5, 20, 1, False, "NA", False, Decimal("1.0000"), None),
        (6, "Discontinued", 12, 5, 5, 20, 1, False, "NA", True, Decimal("1.0000"), None),
    ]
    return spark.createDataFrame(rows, cols)


@pytest.fixture(scope="session")
def dimStockItem(spark):
    cols = "StockItemKey int, WWIStockItemID int, ValidTo timestamp"
    rows = [
        (101, 1, datetime(9999, 12, 31)),
        (102, 2, datetime(9999, 12, 31)),
        (103, 3, datetime(9999, 12, 31)),
        (104, 4, datetime(9999, 12, 31)),
        (99, 4, datetime(2023, 1, 1)),      # expired version must not be picked
        (106, 6, datetime(9999, 12, 31)),
    ]
    return spark.createDataFrame(rows, cols)


@pytest.fixture(scope="session")
def dimWarehouseSite(spark):
    cols = "WarehouseSiteKey int, WarehouseSiteCode string, RegionCode string"
    return spark.createDataFrame([(1, "LDN", "EU"), (2, "SYD", "APAC"), (3, "NYC", "NA")], cols)


@pytest.fixture(scope="session")
def warehouseSite(spark):
    cols = "WarehouseSiteCode string, RegionCode string"
    return spark.createDataFrame([("LDN", "EU"), ("SYD", "APAC"), ("NYC", "NA"), ("MAN", "EU")], cols)
