"""The checked-in bronze registry must match the legacy DDL it was generated from."""
from __future__ import annotations

import csv
import os
import re

import pytest

from sales_lakehouse.bronze import ddl, generate_registry
from sales_lakehouse.bronze.registry import BY_SOURCE_OBJECT, REGISTRY, SOURCE_TABLES
from sales_lakehouse.bronze.source_table import parseSparkType

REPO_ROOT = ddl.findRepoRoot()

REQUIRED_SOURCE_OBJECTS = [
    "Application.People", "Sales.Customers", "Sales.CustomerCategories", "Sales.BuyingGroups", "Sales.SalesChannels",
    "Sales.SalesTerritories", "Sales.CommissionPlans", "Sales.PriceLists", "Sales.PriceListLines", "Sales.QuoteHeaders",
    "Sales.QuoteLines", "Sales.Orders", "Sales.OrderLines", "Sales.OrderAmendments", "Sales.OrderHolds", "Sales.Backorders",
    "Sales.Invoices", "Sales.InvoiceLines", "Sales.CustomerTransactions", "Sales.PaymentAllocations", "Sales.CustomerDisputes",
    "Sales.CustomerWriteOffs", "Sales.OrderDeletionLog", "Returns.ReturnReasons", "Returns.ReturnAuthorizations",
    "Returns.ReturnLines", "Returns.ReturnInspections", "Returns.CreditNotes", "Returns.CreditNoteLines", "Warehouse.StockItems",
    "Shipping.ShipmentHeaders", "Shipping.ShipmentLines", "Integration.ChangeTrackingWatermark",
    "WWI_MDM.CUST_MASTER", "WWI_MDM.PARTY_XREF", "WWI_MDM.MDM_MERGE_HISTORY", "WWI_REF.FX_RATE_DAILY", "WWI_REF.CALENDAR_FISCAL",
    "WWI_FIN.GL_PERIOD_STATUS", "WWI_FIN.TAX_RATE", "WWI_FIN.TAX_JURISDICTION", "WWI_REF.CODE_TRANSLATION", "WWI_FIN.COST_CENTER",
    "WWI_FIN.COST_ALLOCATION_RULE",
]


def test_required_legacy_objects_are_registered():
    missing = [o for o in REQUIRED_SOURCE_OBJECTS if o not in BY_SOURCE_OBJECT]
    assert missing == []


def test_registry_keys_follow_naming_contract():
    for name, source in REGISTRY.items():
        assert re.fullmatch(r"(sqlserver|oracle)_[a-z0-9_]+", name), name
        assert name == source.bronzeTable
        assert source.relativePath == f"{source.system}/{source.schema}/{source.table}.csv"
    assert REGISTRY["sqlserver_sales_orders"].sourceObject == "Sales.Orders"
    assert REGISTRY["sqlserver_sales_order_lines"].sourceObject == "Sales.OrderLines"
    assert REGISTRY["oracle_wwi_mdm_cust_master"].sourceObject == "WWI_MDM.CUST_MASTER"
    assert REGISTRY["oracle_wwi_ref_fx_rate_daily"].sourceObject == "WWI_REF.FX_RATE_DAILY"


def test_inventory_names_are_exact():
    """Objects from sqlserver/oltp and oracle/tables appear verbatim in docs/inventories/sql-objects.csv.

    The wwi-ssdt base tables (Sales.Orders, Application.People, ...) are not
    inventoried there, so for those we only require the checked-in DDL file.
    """
    inventory = os.path.join(REPO_ROOT, "docs", "inventories", "sql-objects.csv")
    with open(inventory, encoding="utf-8") as handle:
        rows = {(r["schema"], r["object_name"]) for r in csv.DictReader(handle) if r["object_type"] == "TABLE"}
    for source in SOURCE_TABLES:
        assert source.ddlFiles, source.sourceObject
        for rel in source.ddlFiles:
            assert os.path.exists(os.path.join(REPO_ROOT, rel)), rel
        if all(not rel.startswith("wwi-ssdt/") for rel in source.ddlFiles):
            assert (source.schema, source.table) in rows, source.sourceObject


@pytest.mark.parametrize("source", SOURCE_TABLES, ids=lambda s: s.sourceObject)
def test_registry_columns_match_ddl(source):
    parsed = ddl.parseTableFromRepo(REPO_ROOT, source.system, source.schema, source.table)
    assert list(source.columnNames) == [name for name, _ in parsed.columns]
    assert list(source.columns) == parsed.columns
    assert list(source.naturalKey) == parsed.primaryKey
    assert tuple(source.ddlFiles) == tuple(parsed.ddlFiles)
    for _name, kind in source.columns:
        parseSparkType(kind)
    if source.loadMode == "incremental":
        assert source.watermarkColumn in source.columnNames
    else:
        assert source.watermarkColumn is None and source.overlapMinutes == 0


# Independent (regex, not ddl.py) column-name extraction so the parser itself is checked too.
_TSQL_COL = re.compile(r"^\s*\[(\w+)\]\s+(?!AS\b)(?:\[sys\]\.\[\w+\]|[A-Za-z]\w*)", re.IGNORECASE)
_TSQL_ADD = re.compile(r"ALTER\s+TABLE\s+\[(\w+)\]\.\[(\w+)\]\s+ADD\s+\[(\w+)\]\s+[A-Za-z]", re.IGNORECASE)
_ORA_COL = re.compile(r"^\s{4}([A-Z][A-Z0-9_]*)\s+(?:NUMBER|VARCHAR2|CHAR|DATE|TIMESTAMP|CLOB|NVARCHAR2|RAW|BLOB|INTEGER|FLOAT)\b")


def naiveColumnNames(source) -> list[str]:
    names: list[str] = []
    for rel in source.ddlFiles:
        with open(os.path.join(REPO_ROOT, rel), encoding="utf-8-sig") as handle:
            text = re.sub(r"/\*.*?\*/", "", handle.read(), flags=re.DOTALL)
        if source.system == "sqlserver":
            marker = f"CREATE TABLE [{source.schema}].[{source.table}] ("
            if marker in text:
                body = text.split(marker, 1)[1].split("\n)", 1)[0]
                for line in body.splitlines():
                    if line.strip().upper().startswith(("CONSTRAINT", "PERIOD", "INDEX")):
                        continue
                    match = _TSQL_COL.match(line)
                    if match:
                        names.append(match.group(1))
            for match in _TSQL_ADD.finditer(text):
                if match.group(1) == source.schema and match.group(2) == source.table and match.group(3) not in names:
                    names.append(match.group(3))
        else:
            marker = f"CREATE TABLE {source.schema}.{source.table}"
            body = text.split(marker, 1)[1].split("\n)", 1)[0]
            for line in body.splitlines():
                match = _ORA_COL.match(line)
                if match and not line.strip().upper().startswith("CONSTRAINT"):
                    names.append(match.group(1))
    return names


@pytest.mark.parametrize("source", SOURCE_TABLES, ids=lambda s: s.sourceObject)
def test_registry_columns_match_ddl_independent_regex(source):
    assert list(source.columnNames) == naiveColumnNames(source)


def test_generator_output_is_checked_in():
    rendered = generate_registry.render(REPO_ROOT)
    registryPath = os.path.join(os.path.dirname(generate_registry.__file__), "registry.py")
    with open(registryPath, encoding="utf-8") as handle:
        assert handle.read() == rendered, "run: PYTHONPATH=src python -m sales_lakehouse.bronze.generate_registry"


def test_type_mapping_rules():
    assert ddl.sqlServerTypeToSpark("BIT") == "boolean"
    assert ddl.sqlServerTypeToSpark("DATETIME2 (7)") == "timestamp"
    assert ddl.sqlServerTypeToSpark("DECIMAL (18, 2)") == "decimal(18,2)"
    assert ddl.sqlServerTypeToSpark("NVARCHAR (MAX)") == "string"
    assert ddl.sqlServerTypeToSpark("[sys].[geography]") == "string"
    assert ddl.oracleTypeToSpark("NUMBER(19,4)") == "decimal(19,4)"
    assert ddl.oracleTypeToSpark("NUMBER(12)") == "decimal(12,0)"
    assert ddl.oracleTypeToSpark("VARCHAR2(1)") == "string"
    assert ddl.oracleTypeToSpark("DATE") == "timestamp"
    assert ddl.oracleTypeToSpark("TIMESTAMP(6)") == "timestamp"
