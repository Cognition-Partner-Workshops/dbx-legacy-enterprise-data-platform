import json

from wwi_file_ingestion import feeds, notebook_support, reconciliation, runner

from conftest import CATALOG
from test_smoke_runner import NA_FILE, dropFile


def test_skip_for_restart_uses_master_phase_order():
    assert not notebook_support.skipForRestart(feeds.PARTNER_SALES_NA, "")
    assert notebook_support.skipForRestart(feeds.PARTNER_SALES_NA, "Carrier And Catalog")
    assert not notebook_support.skipForRestart(feeds.CARRIER_SCAN, "Carrier And Catalog")
    assert notebook_support.skipForRestart(feeds.SUPPLIER_CATALOG, "Quarantine")
    assert not notebook_support.skipForRestart(feeds.QUARANTINE_MALFORMED, "Quarantine")
    assert not notebook_support.skipForRestart(feeds.PARTNER_SALES_NA, "File Screen")  # later master phases: not ours


def test_resolve_batch_adopts_running_batch_when_parameter_is_zero(spark, control):
    batchId, startedHere = notebook_support.resolveBatchId(spark, control, CATALOG, {"batchId": 0, "environmentCode": "DEV"})
    assert startedHere and batchId > 0
    call = control.callsNamed("startBatch")[0]
    assert call["batchName"] == "Master_File_Ingestion" and call["batchType"] == "FileIngestion" and call["allowAdoptRunning"]
    assert notebook_support.resolveBatchId(spark, control, CATALOG, {"batchId": 42}) == (42, False)


def test_package_notebook_lifecycle(spark, volumeRoot, dbutils, control, cleanTables):
    spec = feeds.PARTNER_SALES_NA
    dropFile(CATALOG, spec, "partner_sales_na_20240517_009.csv", NA_FILE)
    params = {"batchId": 5, "environmentCode": "DEV", "restartFromStep": "", "businessDate": None}
    summary = notebook_support.runPackageNotebook(spark, dbutils, control, params, spec, CATALOG, useAutoLoader=False)
    start = control.callsNamed("logPackageStart")[0]
    assert start["packageName"] == "ING_FILE_PartnerSales_NA" and start["projectName"] == "WWI_Ingest_Files"
    assert start["stepName"] == "Partner Drops" and start["batchId"] == 5
    end = control.callsNamed("logPackageEnd")[0]
    assert end["status"] == "Succeeded" and end["rowsInserted"] == 2 and end["rowsRejected"] == 3
    assert not control.callsNamed("endBatch")
    payload = json.loads(notebook_support.summaryJson(summary))
    assert payload["filesProcessed"] == 1 and payload["fileTotals"][0]["status"] == "Processed"


def test_package_notebook_logs_failure(spark, volumeRoot, dbutils, control, cleanTables, monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("volume unreachable")

    monkeypatch.setattr(runner, "runPackage", boom)
    params = {"batchId": 5, "environmentCode": "DEV", "restartFromStep": ""}
    try:
        notebook_support.runPackageNotebook(spark, dbutils, control, params, feeds.CARRIER_SCAN, CATALOG)
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected the failure to be re-raised")
    assert control.callsNamed("logPackageEnd")[0]["status"] == "Failed"
    assert control.callsNamed("logError")[0]["errorCode"] == "RuntimeError"


def test_reconciliation_compare_and_log(spark, volumeRoot, dbutils, control, cleanTables):
    spec = feeds.PARTNER_SALES_NA
    dropFile(CATALOG, spec, "partner_sales_na_20240517_010.csv", NA_FILE)
    runner.runPackage(spark, dbutils, control, CATALOG, 9, 90, spec, useAutoLoader=False)
    actual = reconciliation.actualCounts(spark, CATALOG, 9)
    counts = {r["ObjectName"]: r["RowCount"] for r in actual.collect()}
    assert counts["bronze.raw_file_partner_sales"] == 2 and counts["silver.err_rejected_file_row"] == 4
    baseline = reconciliation.loadBaseline(spark, "", json.dumps([
        {"ObjectName": "bronze.raw_file_partner_sales", "BatchId": 9, "RowCount": 2, "RowHash": None},
        {"ObjectName": "silver.err_rejected_file_row", "BatchId": 9, "RowCount": 3, "RowHash": None},
        {"ObjectName": "bronze.raw_file_fx_override", "BatchId": 9, "RowCount": 1, "RowHash": None},
    ]))
    comparison = reconciliation.compare(actual, baseline)
    status = {r["ObjectName"]: r["Status"] for r in comparison.collect()}
    assert status["bronze.raw_file_partner_sales"] == "Match"
    assert status["silver.err_rejected_file_row"] == "CountMismatch"
    assert status["bronze.raw_file_fx_override"] == "MissingActual"
    failed = reconciliation.logResults(spark, control, CATALOG, 90, comparison)
    assert failed == 2
    assert len(control.callsNamed("logRowCount")) >= 3
    # hash is deterministic across two computations
    again = reconciliation.actualCounts(spark, CATALOG, 9)
    assert {r["ObjectName"]: r["RowHash"] for r in again.collect()} == {r["ObjectName"]: r["RowHash"] for r in actual.collect()}
