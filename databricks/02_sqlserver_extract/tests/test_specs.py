"""The package catalogue must mirror the 22 packages emitted by generate_sqlserver_extracts.py."""
import os
import re

from wwi_sqlserver_extract import specs, transforms

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SSIS_DIR = os.path.join(REPO_ROOT, "ssis", "02_sqlserver_extract")

EXPECTED_PACKAGES = [
    "EXT_SQL_Orders", "EXT_SQL_OrderLines", "EXT_SQL_Invoices", "EXT_SQL_InvoiceLines", "EXT_SQL_Promotions",
    "EXT_SQL_SalesTerritories", "EXT_SQL_CustomerSegments", "EXT_SQL_StockItems", "EXT_SQL_StockMovements",
    "EXT_SQL_StockTransfers", "EXT_SQL_Shipments", "EXT_SQL_ShipmentLines", "EXT_SQL_Returns", "EXT_SQL_CreditNotes",
    "EXT_SQL_WebSessions", "EXT_SQL_LoyaltyLedger", "EXT_SQL_CustomerTransactions", "EXT_SQL_SupplierTransactions",
    "EXT_SQL_People", "EXT_SQL_Cities", "EXT_SQL_PaymentMethods", "EXT_SQL_TransactionTypes",
]


def test_every_dtsx_has_a_spec():
    assert list(specs.PACKAGES) == EXPECTED_PACKAGES
    if os.path.isdir(SSIS_DIR):
        dtsx = sorted(f[:-5] for f in os.listdir(SSIS_DIR) if f.endswith(".dtsx"))
        assert dtsx == sorted(EXPECTED_PACKAGES)


def test_placeholders_match_parameters_and_transforms_exist():
    for spec in specs.PACKAGES.values():
        assert spec.sourceSql.count("?") == len(spec.sqlParams), spec.name
        assert spec.transform in transforms.TRANSFORMS, spec.name
        assert spec.targetTable.startswith("raw_sql_") or spec.targetTable == "raw_oracle_geography", spec.name
        assert re.match(r"^[a-z0-9_]+$", spec.targetTable), spec.name
        if spec.isIncremental:
            assert spec.watermarkObject, spec.name
        if spec.loadPattern == specs.NUMERIC_KEY_BOUNDED:
            assert spec.maxKeySql and spec.keyColumn, spec.name
        if spec.deleteDetection:
            assert spec.deleteDetection.sql.count("?") == 1, spec.name


def test_shared_raw_tables_carry_record_kind():
    shared = [s for s in specs.PACKAGES.values() if s.recordKind]
    assert {s.name for s in shared} == {
        "EXT_SQL_Promotions", "EXT_SQL_SalesTerritories", "EXT_SQL_CustomerSegments", "EXT_SQL_People",
        "EXT_SQL_Cities", "EXT_SQL_PaymentMethods", "EXT_SQL_TransactionTypes", "EXT_SQL_CustomerTransactions",
        "EXT_SQL_SupplierTransactions",
    }
    for spec in shared:
        if spec.loadPattern == specs.FULL_RELOAD:
            assert spec.clearSql == "DELETE FROM %s WHERE RecordKind = N'%s';" % (spec.legacyTarget, spec.recordKind), spec.name
