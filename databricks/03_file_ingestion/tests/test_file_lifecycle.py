"""Archive / quarantine / duplicate / re-send behaviour through the whole runner (batch file discovery)."""

import os

from wwi_file_ingestion import control_totals as ct
from wwi_file_ingestion import feeds, runner

from conftest import CATALOG
from test_smoke_runner import NA_FILE, dropFile

SUPPLIER_OK = "|".join(feeds.SUPPLIER_CATALOG.columns) + "\n" + (
    "HDR|SUP1|||||||||||||||\n"
    "DTL|SUP1|ITEM1|MPN1|Widget|EA|1|12.50|10.00|USD|10|14|20240101||UN1234||\n"
    "TRL|||||||||||||||1|1000\n"
)
SUPPLIER_BAD_CHECKSUM = SUPPLIER_OK.replace("|1|1000", "|1|999")


def run(spark, dbutils, control, spec, **kwargs):
    return runner.runPackage(spark, dbutils, control, CATALOG, batchId=kwargs.pop("batchId", 1),
                             packageExecutionId=kwargs.pop("packageExecutionId", 10), spec=spec,
                             useAutoLoader=False, **kwargs)


def test_duplicate_file_is_routed_not_reloaded(spark, volumeRoot, dbutils, control, cleanTables):
    spec = feeds.PARTNER_SALES_NA
    dropFile(CATALOG, spec, "partner_sales_na_20240517_001.csv", NA_FILE)
    first = run(spark, dbutils, control, spec, packageExecutionId=11)
    assert first.filesProcessed == 1

    dropFile(CATALOG, spec, "partner_sales_na_20240517_001.csv", NA_FILE)  # same name, same bytes
    second = run(spark, dbutils, control, spec, packageExecutionId=12)
    assert second.filesDuplicate == 1 and second.rowsInserted == 0
    moved = dbutils.fs.moves[-1][1]
    assert "/quarantine/partner/na/duplicate/partner_sales_na_20240517_001.csv" in moved
    assert spark.table("%s.bronze.raw_file_partner_sales" % CATALOG).count() == 2
    statuses = sorted(r["Status"] for r in spark.table("%s.etl.%s" % (CATALOG, feeds.FILE_INGESTION_LOG)).collect())
    assert statuses == [ct.STATUS_DUPLICATE, ct.STATUS_PROCESSED]
    assert control.callsNamed("logError")[-1]["errorSeverity"] == "Warning"


def test_resend_with_different_content_is_loaded_again(spark, volumeRoot, dbutils, control, cleanTables):
    spec = feeds.PARTNER_SALES_NA
    dropFile(CATALOG, spec, "partner_sales_na_20240517_002.csv", NA_FILE)
    run(spark, dbutils, control, spec, packageExecutionId=21)
    dropFile(CATALOG, spec, "partner_sales_na_20240517_002.csv", NA_FILE.replace("T-2", "T-22"))
    second = run(spark, dbutils, control, spec, packageExecutionId=22)
    assert second.filesProcessed == 1 and second.rowsInserted == 2
    numbers = sorted(r["TransactionNumber"] for r in spark.table("%s.bronze.raw_file_partner_sales" % CATALOG).collect())
    assert numbers == ["T-1", "T-1", "T-2", "T-22"]


def test_rerun_of_same_package_execution_is_idempotent(spark, volumeRoot, dbutils, control, cleanTables):
    spec = feeds.PARTNER_SALES_NA
    dropFile(CATALOG, spec, "partner_sales_na_20240517_003.csv", NA_FILE)
    runner.ensureTables(spark, CATALOG)
    runner.discoverFiles(spark, CATALOG, spec, useAutoLoader=False)
    # first attempt processes the file but "dies" before finalize: run ingestPending twice
    runner.ingestPending(spark, CATALOG, 1, 31, spec)
    summary = runner.ingestPending(spark, CATALOG, 1, 31, spec)
    assert spark.table("%s.bronze.raw_file_partner_sales" % CATALOG).count() == 2
    assert spark.table("%s.silver.%s" % (CATALOG, feeds.ERR_REJECTED_FILE_ROW)).count() == 4
    runner.finalizeFiles(spark, dbutils, control, CATALOG, 1, 31, spec, summary)
    assert runner.pendingFiles(spark, CATALOG, spec).count() == 0


def test_supplier_checksum_mismatch_goes_to_poison(spark, volumeRoot, dbutils, control, cleanTables):
    spec = feeds.SUPPLIER_CATALOG
    dropFile(CATALOG, spec, "supplier_catalog_20240517_001.psv", SUPPLIER_BAD_CHECKSUM)
    summary = run(spark, dbutils, control, spec, packageExecutionId=41)
    assert summary.filesQuarantined == 1 and summary.rowsInserted == 1
    assert "/quarantine/poison/supplier_catalog_20240517_001.psv" in dbutils.fs.moves[-1][1]
    assert any("control totals do not reconcile" in w for w in summary.warnings)

    dropFile(CATALOG, spec, "supplier_catalog_20240517_002.psv", SUPPLIER_OK)
    ok = run(spark, dbutils, control, spec, packageExecutionId=42)
    assert ok.filesProcessed == 1
    assert "/archive/supplier/2" in dbutils.fs.moves[-1][1]


def test_strict_mode_quarantines_na_footer_mismatch(spark, volumeRoot, dbutils, control, cleanTables):
    spec = feeds.PARTNER_SALES_NA
    dropFile(CATALOG, spec, "partner_sales_na_20240517_004.csv", NA_FILE.replace(",,,,,,,,,,,,,,2\n", ",,,,,,,,,,,,,,5\n"))
    summary = run(spark, dbutils, control, spec, packageExecutionId=51, controlTotalMode=ct.MODE_STRICT)
    assert summary.filesQuarantined == 1
    assert "/quarantine/poison/" in dbutils.fs.moves[-1][1]


def test_carrier_sidecar_total_from_control_table(spark, volumeRoot, dbutils, control, cleanTables):
    spec = feeds.CARRIER_SCAN
    text = ",".join(spec.columns) + "\n" + "DHL,TRK1,SHP1,DLV,Delivered,2024-05-17T10:15:30+02:00,BER,DE,,Jane\n"
    dropFile(CATALOG, spec, "carrier_scan_20240517_001.csv", text)
    spark.sql(
        "INSERT INTO %s.etl.%s (FileName, FeedCode, ExpectedRowCount, ReceivedAtUtc) VALUES ('carrier_scan_20240517_001.csv', 'carrier_scan', 2, current_timestamp())"
        % (CATALOG, feeds.FILE_CONTROL_TOTAL)
    )
    strict = run(spark, dbutils, control, spec, packageExecutionId=61, controlTotalMode=ct.MODE_STRICT)
    assert strict.filesQuarantined == 1 and strict.fileTotals[0].sidecarRowCount == 2


def test_quarantine_sweep_archives_replayable_and_poisons_unreadable(spark, volumeRoot, dbutils, control, cleanTables):
    spec = feeds.QUARANTINE_MALFORMED
    dropFile(CATALOG, spec, "quarantine_rejects_carrier_scan_20240517_001.dat", "header\nDHL,broken,line\n\n")
    dropFile(CATALOG, spec, "quarantine_rejects_fx_override_20240517_001.dat", "header\n\n\x00\x00\n")
    summary = run(spark, dbutils, control, spec, packageExecutionId=71)
    assert summary.filesProcessed == 1 and summary.filesQuarantined == 1
    err = {r["SourceFileName"]: r for r in spark.table("%s.silver.%s" % (CATALOG, feeds.ERR_REJECTED_FILE_ROW)).collect()}
    assert err["quarantine_rejects_carrier_scan_20240517_001.dat"]["OriginFeedCode"] == "CARRIER"
    targets = sorted(dst for _, dst in dbutils.fs.moves)
    assert any("/archive/quarantine/" in t and "carrier_scan" in t for t in targets)
    assert any("/quarantine/poison/quarantine_rejects_fx_override" in t for t in targets)
    codes = sorted(c["rejectReasonCode"] for c in control.callsNamed("logRejectedRecordSet"))
    assert codes == ["EMPTY_LINE", "QUARANTINED"]
    assert not os.path.exists(os.path.join(runner.inboundPath(CATALOG, spec), "quarantine_rejects_carrier_scan_20240517_001.dat"))
