"""The committed SQL must match the Python spec, and the spec must cover every legacy control table."""
import os
import re

from dbx_etl_common import bootstrap, schema

COMMON = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPO = os.path.abspath(os.path.join(COMMON, "..", ".."))

LEGACY_FILES = [
    "sqlserver/control/02_tables_control_framework.sql",
    "sqlserver/control/04_tables_data_quality.sql",
    "sqlserver/control/06_tables_reconciliation.sql",
    "sqlserver/control/07_tables_operations.sql",
    "sqlserver/control/procedures/etl.usp_LogRejectedRecordSet.sql",
    "sqlserver/control/procedures/etl.usp_PurgeControlHistory.sql",
]


def _legacyTables():
    found = set()
    for rel in LEGACY_FILES:
        text = open(os.path.join(REPO, rel), encoding="utf-8").read()
        found |= set(re.findall(r"CREATE TABLE\s+etl\.(\w+)", text))
    return found


def _legacyColumns(tableName):
    for rel in LEGACY_FILES:
        text = open(os.path.join(REPO, rel), encoding="utf-8").read()
        m = re.search(r"CREATE TABLE\s+etl\." + tableName + r"\s*\((.*?)\n\s*\);", text, re.S)
        if m:
            cols = []
            for line in m.group(1).splitlines():
                mm = re.match(r"\s+(\w+)\s+(?:N?VARCHAR|BIGINT|INT|DATE|DATETIME2|BIT|DECIMAL|TINYINT|SMALLINT|NUMERIC|FLOAT|TIME|UNIQUEIDENTIFIER|VARBINARY|CHAR|NCHAR|SYSNAME|DATETIMEOFFSET|MONEY|XML|REAL|AS\b)", line)
                if mm and mm.group(1).upper() not in ("CONSTRAINT", "INDEX", "PRIMARY"):
                    cols.append(mm.group(1))
            return cols
    raise AssertionError(tableName)


def test_every_legacy_table_has_a_spec():
    specs = {t.legacyName.split(".")[1] for t in schema.TABLES}
    missing = _legacyTables() - specs
    assert not missing, f"legacy control tables without a Delta spec: {sorted(missing)}"


def test_every_legacy_column_is_preserved():
    for t in schema.TABLES:
        legacyCols = _legacyColumns(t.legacyName.split(".")[1])
        specCols = [c.name for c in t.columns]
        assert legacyCols == specCols, f"{t.legacyName}: legacy {legacyCols} != spec {specCols}"


def test_rendered_sql_is_current():
    assert open(os.path.join(COMMON, "sql", "00_etl_control_tables.sql"), encoding="utf-8").read() == bootstrap.renderDdl()
    assert open(os.path.join(COMMON, "sql", "01_etl_seed_control_data.sql"), encoding="utf-8").read() == bootstrap.renderSeedSql()
    assert open(os.path.join(COMMON, "views", "00_etl_operational_views.sql"), encoding="utf-8").read() == bootstrap.renderViewSql()


def test_ddl_uses_delta_identity_and_checks():
    ddl = bootstrap.renderDdl("wwi_dev")
    assert "BatchId BIGINT GENERATED ALWAYS AS IDENTITY" in ddl
    assert "CONSTRAINT CK_Batch_Status CHECK" in ddl
    assert "wwi_dev.etl.package_execution" in ddl
    assert "GENERATED ALWAYS AS (unix_timestamp(CompletedAtUtc) - unix_timestamp(StartedAtUtc))" in ddl
