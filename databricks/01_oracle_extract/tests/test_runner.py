"""End-to-end package runs in ``files`` source mode against local Delta tables + the fake control layer."""
import os
from datetime import datetime

import pytest

from dbx_etl_common import control

from oracle_extract.naming import deltaTable
from oracle_extract.runner import PackageRunner, RunSettings, ensureBatch
from oracle_extract.source_reader import GeneratedFileReader
from oracle_extract.specs import PACKAGES

CATALOG = "spark_catalog"


def writeFile(volume, source, rows):
    path = os.path.join(volume, source.fileName + ".dat")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        for row in rows:
            fh.write("|".join("" if v is None else str(v) for v in row) + "\n")


def blankRow(source, **values):
    return [values.get(c.name) for c in source.columns]


@pytest.fixture
def volume(tmp_path):
    return str(tmp_path / "oracle_extracts")


@pytest.fixture(autouse=True)
def schemas(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    yield
    for t in spark.catalog.listTables("bronze"):
        spark.sql(f"DROP TABLE IF EXISTS bronze.{t.name}")


def runner(spark, volume, reloadFullHistory=False, batchId=7):
    settings = RunSettings(catalog=CATALOG, batchId=batchId, reloadFullHistory=reloadFullHistory, environmentCode="DEV")
    return PackageRunner(spark, GeneratedFileReader(volume), settings)


def test_full_load_geography_truncates_and_logs(spark, volume):
    spec = PACKAGES["EXT_ORA_Geography"]
    src = spec.sources[0]
    writeFile(volume, src, [blankRow(src, GeographyKey=1, COUNTRY_CD="US", CITY_NAME="Denver"),
                            blankRow(src, GeographyKey=2, COUNTRY_CD="GB", CITY_NAME="London")])
    summary = runner(spark, volume).run(spec)
    target = deltaTable(CATALOG, spec.legacyTargetTable)
    assert summary.rowsRead == 2 and summary.rowsInserted == 2
    assert spark.table(target).count() == 2
    # second run overwrites (legacy TRUNCATE TABLE raw.OracleGeography)
    runner(spark, volume).run(spec)
    assert spark.table(target).count() == 2
    assert control.state["rowCounts"][-1]["objectName"] == "raw.OracleGeography"
    assert control.state["packages"][-1]["status"] == "Succeeded"
    assert {"BatchId", "PackageExecutionId", "ExtractedAtUtc", "SourceSystemCode", "WatermarkFrom", "WatermarkTo"} <= set(spark.table(target).columns)


def test_incremental_customer_master_window_and_watermark(spark, volume):
    spec = PACKAGES["EXT_ORA_CustomerMaster"]
    control.state["now"] = datetime(2024, 6, 2, 0, 0, 0)
    control.state["watermarks"][(spec.sourceSystemCode, spec.watermarkObject)] = "2024-06-01 00:00:00"
    main, deletes = spec.sources
    writeFile(volume, main, [
        blankRow(main, CUST_ID=1, CUST_NBR="C1", CUST_NAME="Old", LAST_UPDATE_DT="2024-05-31 23:00:00"),
        blankRow(main, CUST_ID=2, CUST_NBR="C2", CUST_NAME="New", LAST_UPDATE_DT="2024-06-01 12:00:00"),
        blankRow(main, CUST_ID=3, CUST_NBR="C3", CUST_NAME="Future", LAST_UPDATE_DT="2024-06-02 00:00:00"),
    ])
    writeFile(volume, deletes, [blankRow(deletes, CUST_ID=9, CUST_NBR="C9", LAST_UPDATE_DT="2024-06-01 08:00:00")])
    summary = runner(spark, volume).run(spec)
    target = spark.table(deltaTable(CATALOG, spec.legacyTargetTable))
    rows = {r["CUST_ID"]: r for r in target.collect()}
    assert set(rows) == {2, 9}
    assert rows[2]["DeleteFlag"] == "N" and rows[9]["DeleteFlag"] == "Y"
    assert rows[2]["WatermarkFrom"] == "2024-06-01 00:00:00" and rows[2]["WatermarkTo"] == "2024-06-02 00:00:00"
    assert summary.rowsDeleted == 1
    assert control.state["watermarks"][(spec.sourceSystemCode, spec.watermarkObject)] == "2024-06-02 00:00:00"
    control.state.pop("now")


def test_reload_full_history_resets_to_epoch_and_overwrites(spark, volume):
    spec = PACKAGES["EXT_ORA_SupplierMaster"]
    src = spec.sources[0]
    key = (spec.sourceSystemCode, spec.watermarkObject)
    control.state["now"] = datetime(2024, 6, 2, 0, 0, 0)
    control.state["watermarks"][key] = "2024-06-01 00:00:00"
    writeFile(volume, src, [blankRow(src, SUPP_ID=1, SUPP_NBR="S1", LAST_UPDATE_DT="2020-01-01 00:00:00")])
    target = deltaTable(CATALOG, spec.legacyTargetTable)
    first = runner(spark, volume).run(spec)          # 2020 row is before the window -> nothing extracted
    assert first.rowsRead == 0
    second = runner(spark, volume, reloadFullHistory=True).run(spec)
    assert second.watermarkFrom == "1900-01-01 00:00:00" and second.rowsInserted == 1
    assert spark.table(target).count() == 1
    runner(spark, volume, reloadFullHistory=True).run(spec)
    assert spark.table(target).count() == 1          # overwrite, not duplicate
    assert control.state["watermarks"][key] == "2024-06-02 00:00:00"
    control.state.pop("now")


def test_numeric_key_receipt_line_keeps_watermark_when_upper_is_null(spark, volume):
    spec = PACKAGES["EXT_ORA_ReceiptLine"]
    src = spec.sources[0]
    key = (spec.sourceSystemCode, spec.watermarkObject)
    control.state["watermarkTypes"][key] = "NumericKey"
    control.state["watermarks"][key] = "10"
    writeFile(volume, src, [blankRow(src, RECEIPT_LINE_ID=10, VARIANCE_PCT=0.0, INSPECTION_STATUS_CD="PASS"),
                            blankRow(src, RECEIPT_LINE_ID=11, VARIANCE_PCT=0.2, INSPECTION_STATUS_CD="FAIL")])
    summary = runner(spark, volume).run(spec)
    assert summary.rowsRead == 1 and summary.rowsInserted == 1   # failed inspections stay in the raw table
    assert spark.table(deltaTable(CATALOG, spec.legacyTargetTable)).first()["VarianceBand"] == "EXCP"
    assert control.state["watermarks"][key] == "10"   # legacy usp_SetWatermark ignores NULL WatermarkTo


def test_lookup_reject_customer_address(spark, volume):
    geo = PACKAGES["EXT_ORA_Geography"]
    geoSrc = geo.sources[0]
    writeFile(volume, geoSrc, [blankRow(geoSrc, GeographyKey=1, COUNTRY_CD="US", POSTAL_CD="80202")])
    runner(spark, volume).run(geo)
    spec = PACKAGES["EXT_ORA_CustomerAddress"]
    src = spec.sources[0]
    writeFile(volume, src, [
        blankRow(src, ADDRESS_ID=1, CUST_ID=1, COUNTRY_CD="US", POSTAL_CD="80202", REGION_CD="NA", ADDRESS_LINE_1="a", CITY_NAME="b", LAST_UPDATE_DT="2024-01-01 00:00:00"),
        blankRow(src, ADDRESS_ID=2, CUST_ID=1, COUNTRY_CD="US", POSTAL_CD="00000", REGION_CD="NA", ADDRESS_LINE_1="a", CITY_NAME="b", LAST_UPDATE_DT="2024-01-01 00:00:00"),
    ])
    summary = runner(spark, volume).run(spec)
    assert summary.rowsRead == 2 and summary.rowsInserted == 1 and summary.rowsRejected == 1
    assert control.state["rejects"][-1]["rejectReasonCode"] == "GEO_NOMATCH"


def test_record_kind_scope_delete_code_translation(spark, volume):
    cust = PACKAGES["EXT_ORA_CustomerMaster"]
    main, deletes = cust.sources
    writeFile(volume, main, [blankRow(main, CUST_ID=1, CUST_NBR="C1", CUST_NAME="Acme", LAST_UPDATE_DT="2024-01-01 00:00:00")])
    writeFile(volume, deletes, [])
    runner(spark, volume).run(cust)
    spec = PACKAGES["EXT_ORA_CodeTranslation"]
    src = spec.sources[0]
    writeFile(volume, src, [blankRow(src, **{src.columns[0].name: "A", src.columns[1].name: "B"})])
    runner(spark, volume).run(spec)
    runner(spark, volume).run(spec)
    target = spark.table(deltaTable(CATALOG, spec.legacyTargetTable))
    assert target.where("RecordKind = 'CODEXREF'").count() == 1
    assert target.where("RecordKind IS NULL OR RecordKind <> 'CODEXREF'").count() == 1


def test_ensure_batch_adopts_running_batch(spark):
    settings = RunSettings(catalog=CATALOG, batchId=0, reloadFullHistory=False, environmentCode="DEV")
    a = ensureBatch(spark, settings)
    b = ensureBatch(spark, settings)
    assert a == b > 0


def test_reload_of_shared_table_keeps_sibling_package_rows(spark, volume):
    cust = PACKAGES["EXT_ORA_CustomerMaster"]
    xref = PACKAGES["EXT_ORA_CodeTranslation"]
    main, deletes = cust.sources
    control.state["now"] = datetime(2024, 6, 2, 0, 0, 0)
    writeFile(volume, main, [blankRow(main, CUST_ID=1, CUST_NBR="C1", CUST_NAME="Acme", LAST_UPDATE_DT="2024-01-01 00:00:00")])
    writeFile(volume, deletes, [])
    src = xref.sources[0]
    writeFile(volume, src, [blankRow(src, **{src.columns[0].name: "A", src.columns[1].name: "B"})])
    runner(spark, volume).run(cust)
    runner(spark, volume).run(xref)
    runner(spark, volume, reloadFullHistory=True).run(cust)
    target = spark.table(deltaTable(CATALOG, cust.legacyTargetTable))
    assert target.where("RecordKind = 'CODEXREF'").count() == 1
    assert target.where("RecordKind IS NULL").count() == 1
    runner(spark, volume, reloadFullHistory=True).run(xref)
    target = spark.table(deltaTable(CATALOG, cust.legacyTargetTable))
    assert target.where("RecordKind = 'CODEXREF'").count() == 1 and target.where("RecordKind IS NULL").count() == 1
    control.state.pop("now")
