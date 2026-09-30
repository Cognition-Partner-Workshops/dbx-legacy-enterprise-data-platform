import json
import os
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import Row

from platform_control import errors, quality
from platform_control.control import ControlFramework
from platform_control.files import FileOps


@pytest.fixture
def cf(spark, cfg):
    return ControlFramework(spark, cfg)


def _batch(cf, name):
    batchId = cf.startBatch(name, "Adhoc", jobRunId=name)
    execId = cf.startPackageExecution(batchId, None, name)
    return batchId, execId


def testReferentialScreenFindsOrphansAndDedupes(spark, cf):
    orderLine = spark.createDataFrame(
        [Row(OrderId=1, OrderLineId=1, StockItemId=10, PackageTypeCode="EA"), Row(OrderId=1, OrderLineId=2, StockItemId=99, PackageTypeCode="EA"),
         Row(OrderId=1, OrderLineId=2, StockItemId=99, PackageTypeCode="EA"), Row(OrderId=2, OrderLineId=1, StockItemId=10, PackageTypeCode="ZZ")]
    )
    stockItem = spark.createDataFrame([Row(StockItemId=10)])
    packageType = spark.createDataFrame([Row(PackageTypeCode="EA")])
    saleLine = spark.createDataFrame([Row(InvoiceId=5, InvoiceLineId=1)])
    sale = spark.createDataFrame([Row(InvoiceId=5, SalesTerritoryCode="NA-E", SaleCurrencyCode="XXX")])
    currency = spark.createDataFrame([Row(CurrencyCode="USD")])
    territory = spark.createDataFrame([Row(SalesTerritoryCode="NA-E")])
    sources = {"stg.OrderLine": orderLine, "stg.StockItem": stockItem, "ref.PackageType": packageType, "stg.SaleLine": saleLine,
               "stg.Sale": sale, "ref.Currency": currency, "stg.SalesTerritory": territory}
    batchId, execId = _batch(cf, "ref-screen")
    result = quality.referentialScreen(cf, batchId, execId, sources=sources, orphanWarnThreshold=0, orphanFailThreshold=1000)
    assert result["orphanCount"] == 3  # StockItem 99 (deduped, occurrence 2), PackageType ZZ, Currency XXX
    rows = spark.sql(f"SELECT lookup_name, occurrence_count, queued_for_late_arrival FROM {cf.t('err_rejected_lookup_failure')} WHERE batch_id = {batchId}").collect()
    byLookup = {r["lookup_name"]: r for r in rows}
    assert byLookup["StockItem"]["occurrence_count"] == 2 and byLookup["StockItem"]["queued_for_late_arrival"]
    assert "Warn On Referential Orphans" in result["gates"]


def testRejectReprocessReplaysLateArrivingStockItems(spark, cf):
    batchId, execId = _batch(cf, "reject-reprocess")
    now = datetime.now(timezone.utc)
    cf.insertRows(
        "err_rejected_lookup_failure",
        [
            {"batch_id": batchId, "package_execution_id": execId, "source_object_name": "stg.OrderLine", "source_business_key": "7|1", "lookup_name": "StockItem",
             "lookup_column_name": "StockItemId", "lookup_value": "42", "source_system_code": "STAGING", "reject_reason_code": "DQ_REF_ORDERLINE", "reject_reason": "x",
             "reject_stage": "Referential", "routed_to_unknown_member": False, "queued_for_late_arrival": True, "occurrence_count": 1,
             "record_payload": json.dumps({"OrderId": 7, "OrderLineId": 1, "StockItemId": 42}), "reprocess_status_code": "Pending", "reprocess_attempt_count": 0,
             "rejected_at_utc": now - timedelta(days=1)},
            {"batch_id": batchId, "package_execution_id": execId, "source_object_name": "stg.OrderLine", "source_business_key": "8|1", "lookup_name": "StockItem",
             "lookup_column_name": "StockItemId", "lookup_value": "43", "source_system_code": "STAGING", "reject_reason_code": "DQ_REF_ORDERLINE", "reject_reason": "x",
             "reject_stage": "Referential", "routed_to_unknown_member": False, "queued_for_late_arrival": True, "occurrence_count": 1,
             "record_payload": json.dumps({"OrderId": 8, "OrderLineId": 1, "StockItemId": 43}), "reprocess_status_code": "Pending", "reprocess_attempt_count": 0,
             "rejected_at_utc": now - timedelta(days=1)},
        ],
    )
    stockItems = spark.createDataFrame([Row(StockItemId=42)])
    batch2, exec2 = _batch(cf, "reject-reprocess-2")
    result = quality.rejectReprocess(cf, batch2, exec2, stockItems=stockItems)
    statuses = {r["lookup_value"]: r["reprocess_status_code"] for r in spark.sql(f"SELECT lookup_value, reprocess_status_code FROM {cf.t('err_rejected_lookup_failure')} WHERE batch_id = {batchId}").collect()}
    assert statuses["42"] == "Reprocessed" and statuses["43"] == "Unresolved"
    assert spark.sql(f"SELECT COUNT(*) AS n FROM {cf.t('stg_order_line_replay')} WHERE package_execution_id = {exec2}").first()["n"] == 1
    assert result


def testFileScreenRejectsMalformedRows(spark, cf):
    raw = spark.createDataFrame(
        [Row(FileRowId=1, FileLineNumber=1, RawLine="a|b|c|d|e|f|g|h|i", SaleDateText="2026-09-28", AmountText="1.5", SourceFileName="f.csv"),
         Row(FileRowId=2, FileLineNumber=2, RawLine="a|b|c", SaleDateText="2026-09-28", AmountText="1.5", SourceFileName="f.csv"),
         Row(FileRowId=3, FileLineNumber=3, RawLine="a|b|c|d|e|f|g|h|i", SaleDateText="bad", AmountText="1.5", SourceFileName="f.csv")]
    )
    batchId, execId = _batch(cf, "file-screen")
    # 2 of 3 rows malformed breaches the legacy "Fail On File Structure Breach" gate, but rejects are persisted first
    with pytest.raises(quality.GateFailure):
        quality.fileScreen(cf, batchId, execId, source=raw)
    reasons = {r["source_row_number"]: r["reject_reason_code"] for r in spark.sql(f"SELECT source_row_number, reject_reason_code FROM {cf.t('err_rejected_file_row')} WHERE batch_id = {batchId}").collect()}
    assert reasons == {2: "DQ_FILE_DELIMITER", 3: "DQ_FILE_DATE"}
    # one malformed row in a large file only warns
    good = [Row(FileRowId=i, FileLineNumber=i, RawLine="a|b|c|d|e|f|g|h|i", SaleDateText="2026-09-28", AmountText="1.5", SourceFileName="g.csv") for i in range(1, 200)]
    bad = [Row(FileRowId=200, FileLineNumber=200, RawLine="a|b|c|d|e|f|g|h|i", SaleDateText="2026-09-28", AmountText="1,5x", SourceFileName="g.csv")]
    batchId2, execId2 = _batch(cf, "file-screen-ok")
    result = quality.fileScreen(cf, batchId2, execId2, source=spark.createDataFrame(good + bad))
    assert result["rowsScreened"] == 200 and result["malformedCount"] == 1
    assert "Fail On File Structure Breach" not in result["gates"]


def testQuarantineSweepRecordsSampleFiles(spark, cf, cfg):
    fileOps = FileOps(cfg.volumeRoot)
    samples = os.path.join(os.path.dirname(__file__), "..", "samples", "landing", "quarantine")
    fileOps.ensureDir(fileOps.join("quarantine"))
    for name in os.listdir(samples):
        with open(os.path.join(samples, name), "rb") as src:
            data = src.read()
        with open(os.path.join(fileOps.join("quarantine"), name), "wb") as dst:
            dst.write(data)
    batchId, execId = _batch(cf, "quarantine-sweep")
    result = quality.quarantineSweep(cf, batchId, execId, fileOps)
    assert result["filesSeen"] == 3 and result["rowsRecorded"] >= 8
    assert not fileOps.listFiles(fileOps.join("quarantine"))
    rows = spark.sql(f"SELECT reject_reason_code, reprocess_status_code FROM {cf.t('err_rejected_file_row')} WHERE batch_id = {batchId} AND reject_stage = 'Quarantine'").collect()
    codes = {r["reject_reason_code"] for r in rows}
    assert "EMPTY_LINE" in codes and "QUARANTINED" in codes
    assert any(r["reprocess_status_code"] == "NotReplayable" for r in rows)


def testRowCountReconciliationStatuses(spark, cf):
    batchId, execId = _batch(cf, "recon-rows")
    cf.logRowCount(execId, batchId, "Fact.Match", countStage="Staging", sourceRowCount=100, targetRowCount=100)
    cf.logRowCount(execId, batchId, "Fact.Match", countStage="Target", sourceRowCount=100, targetRowCount=100)
    cf.logRowCount(execId, batchId, "Fact.Broken", countStage="Staging", sourceRowCount=100, targetRowCount=100)
    cf.logRowCount(execId, batchId, "Fact.Broken", countStage="Target", sourceRowCount=100, targetRowCount=50)
    result = errors.reconcileRowCounts(cf, batchId, raiseOnFailure=False)
    statuses = {r["object_name"]: r["reconciliation_status"] for r in spark.sql(f"SELECT object_name, reconciliation_status FROM {cf.t('work_row_count_reconciliation')} WHERE batch_id = {batchId}").collect()}
    assert statuses.get("Fact.Match") == "MATCHED" and statuses.get("Fact.Broken") == "FAILED"
    assert result
    with pytest.raises(Exception):  # noqa: B017
        errors.reconcileRowCounts(cf, batchId, raiseOnFailure=True)


def testHandlePackageFailureTransientVsPermanent(spark, cf):
    batchId, execId = _batch(cf, "pkg-failure")
    stepId = cf.startBatchStep(batchId, "Extract", 1, "Extract", packageName="EXT_X")
    cf.endBatchStep(stepId, "Failed", errorMessage="deadlock")
    transient = errors.handlePackageFailure(cf, batchId, "EXT_X", 1205, "Transaction was deadlocked")
    assert transient["retryableFlag"] is True
    assert cf.scalar(f"SELECT is_retryable FROM {cf.t('etl_batch_step')} WHERE batch_step_id = {stepId}") is True
    permanent = errors.handlePackageFailure(cf, batchId, "EXT_X", 547, "FK violation")
    assert permanent["retryableFlag"] is False
    assert cf.scalar(f"SELECT status FROM {cf.t('etl_batch')} WHERE batch_id = {batchId}") == "Failed"


def testRetryFailedStepsBoundedByMaxAttempts(spark, cf):
    batchId, execId = _batch(cf, "retry-steps")
    stepId = cf.startBatchStep(batchId, "Extract Oracle", 1, "Extract", packageName="EXT_ORA")
    cf.endBatchStep(stepId, "Failed", errorMessage="timeout", isRetryable=True)
    calls = []

    def rerun(step):
        calls.append(step)
        return len(calls) >= 2

    result = errors.retryFailedSteps(cf, batchId, maxRetryAttempts=3, backoffBaseSeconds=1, rerunStep=rerun, sleep=lambda s: None)
    assert len(calls) == 2
    assert result


def testRouteRejectedRowsWritesFileAndEscalates(spark, cf, cfg):
    batchId, execId = _batch(cf, "route-rejects")
    now = datetime.now(timezone.utc)
    cf.logRejectedRecords(
        [
            {"packageExecutionId": execId, "batchId": batchId, "sourceSystemCode": "STG", "objectName": "Fact.Sale", "businessKey": "k-new",
             "rejectReasonCode": "CODE_UNMAPPED", "rejectReason": "x", "rejectStage": "Fact", "recordPayload": "{}", "loggedAtUtc": now},
            {"packageExecutionId": execId, "batchId": batchId, "sourceSystemCode": "STG", "objectName": "Fact.Sale", "businessKey": "k-old",
             "rejectReasonCode": "CODE_UNMAPPED", "rejectReason": "x", "rejectStage": "Fact", "recordPayload": "{}", "loggedAtUtc": now - timedelta(days=10)},
        ]
    )
    fileOps = FileOps(cfg.volumeRoot)
    result = errors.routeRejectedRows(cf, batchId, fileOps, rejectEscalationDays=5)
    assert result["routedRowCount"] >= 2 and result["escalatedCount"] >= 1
    assert os.path.exists(result["rejectFile"])
    again = errors.routeRejectedRows(cf, batchId, fileOps, rejectEscalationDays=5)
    assert again["routedRowCount"] == 0


def testNotifyOperationsRaisesNotification(spark, cf):
    batchId, execId = _batch(cf, "notify-ops")
    stepId = cf.startBatchStep(batchId, "Facts", 5, "Load", criticality="high")
    cf.endBatchStep(stepId, "Failed", errorMessage="boom")
    result = errors.notifyOperations(cf, batchId)
    assert result
    assert spark.sql(f"SELECT COUNT(*) AS n FROM {cf.t('etl_operator_notification')} WHERE batch_id = {batchId}").first()["n"] >= 1
