from datetime import datetime, timezone

import pytest

from platform_control.control import ControlFramework


@pytest.fixture
def cf(spark, cfg):
    return ControlFramework(spark, cfg)


def testBatchLifecycle(cf):
    batchId = cf.startBatch("UnitBatch", "Adhoc", jobRunId="ctl-1")
    with pytest.raises(RuntimeError):
        cf.startBatch("UnitBatch", "Adhoc", jobRunId="ctl-1b")
    assert cf.startBatch("UnitBatch", "Adhoc", jobRunId="ctl-1c", allowAdoptRunning=True, restartFromStep="Stage Load") == batchId
    stepId = cf.startBatchStep(batchId, "Stage Load", 3, "Stage")
    execId = cf.startPackageExecution(batchId, stepId, "PKG_X", jobRunId="ctl-1")
    cf.endPackageExecution(execId, "Succeeded", rowsRead=10, rowsInserted=9, rowsRejected=1)
    cf.endBatchStep(stepId, "Succeeded")
    secondAttempt = cf.startBatchStep(batchId, "Stage Load", 3, "Stage")
    assert cf.scalar(f"SELECT attempt_number FROM {cf.t('etl_batch_step')} WHERE batch_step_id = {secondAttempt}") == 2
    cf.endBatchStep(secondAttempt, "Succeeded")
    result = cf.endBatch(batchId)
    assert result["batchStatus"] == "Succeeded"


def testEndBatchClosesRunningExecutionsAsFailed(cf):
    batchId = cf.startBatch("UnitBatch2", "Adhoc", jobRunId="ctl-2")
    execId = cf.startPackageExecution(batchId, None, "PKG_Y")
    result = cf.endBatch(batchId)
    assert result["batchStatus"] == "Failed"
    assert cf.scalar(f"SELECT status FROM {cf.t('etl_package_execution')} WHERE package_execution_id = {execId}") == "Failed"


def testWarningsGiveSucceededWithWarnings(cf):
    batchId = cf.startBatch("UnitBatch3", "Adhoc", jobRunId="ctl-3")
    cf.logError(batchId, "soft problem", severity="Warning", sourceName="unit")
    assert cf.endBatch(batchId)["batchStatus"] == "SucceededWithWarnings"


def testWatermarkPreviousValueRollsForward(cf):
    assert cf.getWatermark("ORA", "orders") is None
    cf.setWatermark("ORA", "orders", "2026-09-01T00:00:00")
    cf.setWatermark("ORA", "orders", "2026-09-02T00:00:00")
    wm = cf.getWatermark("ORA", "orders")
    assert wm["last_value"] == "2026-09-02T00:00:00" and wm["previous_value"] == "2026-09-01T00:00:00"


def testRowCountAuditAndRejects(cf):
    batchId = cf.startBatch("UnitBatch4", "Adhoc", jobRunId="ctl-4")
    execId = cf.startPackageExecution(batchId, None, "PKG_Z")
    cf.logRowCount(execId, batchId, "Fact.Test", sourceRowCount=100, targetRowCount=98, rejectRowCount=2)
    row = cf.spark.sql(f"SELECT variance_row_count FROM {cf.t('etl_row_count_audit')} WHERE package_execution_id = {execId}").first()
    assert row["variance_row_count"] == 0
    n = cf.logRejectedRecords(
        [
            {"packageExecutionId": execId, "batchId": batchId, "sourceSystemCode": "STG", "objectName": "Fact.Test", "businessKey": "k1",
             "rejectReasonCode": "CODE_UNMAPPED", "rejectReason": "x", "rejectStage": "Fact", "recordPayload": "{}",
             "is_reprocessed": False, "loggedAtUtc": datetime.now(timezone.utc)}
        ]
    )
    assert n == 1
    assert cf.raiseNotification(batchId, "TEST", "INFO", "s", "b", oncePerBatchAndType=True)
    assert not cf.raiseNotification(batchId, "TEST", "INFO", "s", "b", oncePerBatchAndType=True)
    cf.endBatch(batchId)
