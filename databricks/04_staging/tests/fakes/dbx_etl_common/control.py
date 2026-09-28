import contextlib
import itertools

STATE = {
    "batches": [],
    "steps": [],
    "packages": [],
    "errors": [],
    "rowCounts": [],
    "rejects": [],
    "rejectSets": [],
    "watermarks": {},
    "configuration": {},
}
_ids = itertools.count(1)


def reset():
    for key, value in STATE.items():
        value.clear()


def startBatch(spark, catalog, batchName, batchType="Daily", businessDate=None, environmentCode=None, allowAdoptRunning=False, notes=None):
    for b in STATE["batches"]:
        if allowAdoptRunning and b["batchName"] == batchName and b["businessDate"] == businessDate and b["status"] == "Running":
            return b["batchId"]
    batchId = next(_ids)
    STATE["batches"].append({"batchId": batchId, "batchName": batchName, "batchType": batchType, "businessDate": businessDate, "environmentCode": environmentCode, "status": "Running", "notes": notes})
    return batchId


def endBatch(spark, catalog, batchId, forceStatus=None):
    for b in STATE["batches"]:
        if b["batchId"] == batchId:
            b["status"] = forceStatus or "Succeeded"


def startBatchStep(spark, catalog, batchId, stepName, stepSequence, stepGroup=None):
    stepId = next(_ids)
    STATE["steps"].append({"batchStepId": stepId, "batchId": batchId, "stepName": stepName, "stepSequence": stepSequence, "stepGroup": stepGroup, "status": "Running"})
    return stepId


def endBatchStep(spark, catalog, batchStepId, status="Succeeded"):
    for s in STATE["steps"]:
        if s["batchStepId"] == batchStepId:
            s["status"] = status


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    pid = next(_ids)
    STATE["packages"].append({"packageExecutionId": pid, "batchId": batchId, "packageName": packageName, "projectName": projectName, "stepName": stepName, "status": "Running"})
    return pid


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=None, rowsInserted=None, rowsUpdated=None, rowsDeleted=None, rowsRejected=None, watermarkFrom=None, watermarkTo=None):
    for p in STATE["packages"]:
        if p["packageExecutionId"] == packageExecutionId:
            p.update({"status": status, "rowsRead": rowsRead, "rowsInserted": rowsInserted, "rowsUpdated": rowsUpdated, "rowsDeleted": rowsDeleted, "rowsRejected": rowsRejected, "watermarkFrom": watermarkFrom, "watermarkTo": watermarkTo})


class _PackageRun:
    def __init__(self, packageExecutionId):
        self.packageExecutionId = packageExecutionId
        self.rowsRead = None
        self.rowsInserted = None
        self.rowsUpdated = None
        self.rowsDeleted = None
        self.rowsRejected = None
        self.watermarkFrom = None
        self.watermarkTo = None


@contextlib.contextmanager
def packageRun(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    pid = logPackageStart(spark, catalog, batchId, packageName, projectName=projectName, stepName=stepName)
    run = _PackageRun(pid)
    try:
        yield run
    except Exception as exc:
        logError(spark, catalog, packageExecutionId=pid, batchId=batchId, sourceName=packageName, errorDescription=str(exc))
        logPackageEnd(spark, catalog, pid, status="Failed")
        raise
    logPackageEnd(spark, catalog, pid, status="Succeeded", rowsRead=run.rowsRead, rowsInserted=run.rowsInserted, rowsUpdated=run.rowsUpdated, rowsDeleted=run.rowsDeleted, rowsRejected=run.rowsRejected, watermarkFrom=run.watermarkFrom, watermarkTo=run.watermarkTo)


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None, sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None):
    STATE["errors"].append({"packageExecutionId": packageExecutionId, "batchId": batchId, "errorSeverity": errorSeverity, "errorCode": errorCode, "sourceName": sourceName, "sourceComponent": sourceComponent, "procedureName": procedureName, "errorDescription": errorDescription})


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None, insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    STATE["rowCounts"].append({"packageExecutionId": packageExecutionId, "objectName": objectName, "sourceRowCount": sourceRowCount, "targetRowCount": targetRowCount, "insertRowCount": insertRowCount, "updateRowCount": updateRowCount, "deleteRowCount": deleteRowCount, "rejectRowCount": rejectRowCount})


def logRejectedRecord(spark, catalog, objectName, rejectReasonCode, packageExecutionId=None, batchId=None, sourceSystemCode=None, businessKey=None, rejectReason=None, rejectStage="Stage", recordPayload=None):
    STATE["rejects"].append({"objectName": objectName, "rejectReasonCode": rejectReasonCode, "packageExecutionId": packageExecutionId, "batchId": batchId, "sourceSystemCode": sourceSystemCode, "businessKey": businessKey, "rejectReason": rejectReason, "rejectStage": rejectStage, "recordPayload": recordPayload})


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None, sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    rows = rejectedDf.collect()
    for row in rows:
        logRejectedRecord(spark, catalog, objectName, rejectReasonCode or row["RejectReasonCode"], packageExecutionId=packageExecutionId, batchId=batchId, sourceSystemCode=sourceSystemCode, businessKey=row[businessKeyColumn] if businessKeyColumn else None, rejectStage=rejectStage)
    STATE["rejectSets"].append({"objectName": objectName, "rejectReasonCode": rejectReasonCode, "count": len(rows)})
    return len(rows)


def getWatermark(spark, catalog, sourceSystemCode, objectName, reloadFullHistory=False):
    current = STATE["watermarks"].get((sourceSystemCode, objectName))
    watermarkFrom = "1900-01-01T00:00:00.000" if reloadFullHistory or current is None else current
    return watermarkFrom, "9999-12-31T00:00:00.000"


def setWatermark(spark, catalog, sourceSystemCode, objectName, watermarkTo, packageExecutionId=None, allowRewind=False):
    STATE["watermarks"][(sourceSystemCode, objectName)] = watermarkTo


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    return STATE["configuration"].get(configurationKey)


def assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True):
    return 0


def assertRowCountTolerance(spark, catalog, batchId, scope="ALL", objectName=None, absoluteTolerance=None, percentTolerance=None, raiseOnFailure=True):
    return 0


def evaluateDataQualityRules(spark, catalog, batchId=None, packageExecutionId=None, ruleGroupCode=None, objectName=None, regionCode=None, businessDate=None):
    return 0


def purgeControlHistory(spark, catalog, retentionDays=None, executionHistoryMonths=13, errorHistoryMonths=24, rejectHistoryMonths=12, qualityHistoryMonths=13, whatIf=False):
    return None
