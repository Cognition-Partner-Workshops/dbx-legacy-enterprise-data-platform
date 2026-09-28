"""In-memory stand-in for the etl.* control procedures (contract signatures only)."""
from datetime import datetime, timedelta, timezone

EPOCH = "1900-01-01 00:00:00"
LOOKBACK_MINUTES = 0

state = {
    "batches": [], "packages": [], "errors": [], "rowCounts": [], "rejects": [],
    "watermarks": {}, "watermarkTypes": {}, "configuration": {}, "nextId": 1,
}


def reset():
    state.update({"batches": [], "packages": [], "errors": [], "rowCounts": [], "rejects": [],
                  "watermarks": {}, "watermarkTypes": {}, "configuration": {}, "nextId": 1})


def _nextId():
    state["nextId"] += 1
    return state["nextId"] - 1


def startBatch(spark, catalog, batchName, batchType="Daily", businessDate=None, environmentCode=None,
               allowAdoptRunning=False, notes=None):
    for b in state["batches"]:
        if b["batchName"] == batchName and b["status"] == "Running" and allowAdoptRunning:
            return b["batchId"]
    batchId = _nextId()
    state["batches"].append({"batchId": batchId, "batchName": batchName, "batchType": batchType, "status": "Running"})
    return batchId


def endBatch(spark, catalog, batchId, forceStatus=None):
    for b in state["batches"]:
        if b["batchId"] == batchId:
            b["status"] = forceStatus or "Succeeded"


def startBatchStep(spark, catalog, batchId, stepName, stepSequence, stepGroup=None):
    return _nextId()


def endBatchStep(spark, catalog, batchStepId, status="Succeeded"):
    return None


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    packageExecutionId = _nextId()
    state["packages"].append({"packageExecutionId": packageExecutionId, "batchId": batchId, "packageName": packageName,
                              "projectName": projectName, "stepName": stepName, "status": "Running"})
    return packageExecutionId


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=None, rowsInserted=None,
                  rowsUpdated=None, rowsDeleted=None, rowsRejected=None, watermarkFrom=None, watermarkTo=None):
    for p in state["packages"]:
        if p["packageExecutionId"] == packageExecutionId:
            p.update({"status": status, "rowsRead": rowsRead, "rowsInserted": rowsInserted, "rowsUpdated": rowsUpdated,
                      "rowsDeleted": rowsDeleted, "rowsRejected": rowsRejected,
                      "watermarkFrom": watermarkFrom, "watermarkTo": watermarkTo})


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None,
             sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None):
    state["errors"].append({"packageExecutionId": packageExecutionId, "batchId": batchId, "errorSeverity": errorSeverity,
                            "sourceName": sourceName, "sourceComponent": sourceComponent, "errorDescription": errorDescription})


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None,
                insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    state["rowCounts"].append({"packageExecutionId": packageExecutionId, "objectName": objectName,
                               "sourceRowCount": sourceRowCount, "targetRowCount": targetRowCount,
                               "insertRowCount": insertRowCount, "updateRowCount": updateRowCount,
                               "deleteRowCount": deleteRowCount, "rejectRowCount": rejectRowCount})


def logRejectedRecord(spark, catalog, objectName, rejectReasonCode, packageExecutionId=None, batchId=None,
                      sourceSystemCode=None, businessKey=None, rejectReason=None, rejectStage="Stage", recordPayload=None):
    state["rejects"].append({"objectName": objectName, "rejectReasonCode": rejectReasonCode, "businessKey": businessKey,
                             "rejectReason": rejectReason, "rejectStage": rejectStage, "recordPayload": recordPayload})


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None,
                         sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    rows = rejectedDf.collect()
    for r in rows:
        logRejectedRecord(spark, catalog, objectName, rejectReasonCode, packageExecutionId, batchId, sourceSystemCode,
                          businessKey=r[businessKeyColumn] if businessKeyColumn else None,
                          rejectReason=r["RejectReason"] if "RejectReason" in r.__fields__ else None,
                          rejectStage=rejectStage,
                          recordPayload=r["RecordPayload"] if "RecordPayload" in r.__fields__ else None)
    return len(rows)


def getWatermark(spark, catalog, sourceSystemCode, objectName, reloadFullHistory=False):
    key = (sourceSystemCode, objectName)
    watermarkType = state["watermarkTypes"].get(key, "Timestamp")
    lastValue = state["watermarks"].get(key)
    if lastValue is None:
        lastValue = "0" if watermarkType == "NumericKey" else EPOCH
        state["watermarks"][key] = lastValue
    if reloadFullHistory:
        lastValue = "0" if watermarkType == "NumericKey" else EPOCH
    if watermarkType == "NumericKey":
        return lastValue, None
    now = state.get("now") or datetime.now(timezone.utc).replace(tzinfo=None)
    if watermarkType == "DateWindow":
        return lastValue[:10], now.date().isoformat()
    fromValue = datetime.fromisoformat(lastValue.replace("T", " ")) - timedelta(minutes=LOOKBACK_MINUTES)
    return fromValue.strftime("%Y-%m-%d %H:%M:%S"), now.strftime("%Y-%m-%d %H:%M:%S")


def setWatermark(spark, catalog, sourceSystemCode, objectName, watermarkTo, packageExecutionId=None, allowRewind=False):
    if watermarkTo is None or str(watermarkTo) == "":
        return
    key = (sourceSystemCode, objectName)
    last = state["watermarks"].get(key)
    if last is not None and not allowRewind and str(watermarkTo) < str(last) and len(str(watermarkTo)) == len(str(last)):
        raise ValueError(f"watermark rewind refused for {objectName}: {last} -> {watermarkTo}")
    state["watermarks"][key] = str(watermarkTo)


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    return state["configuration"].get((configurationKey, environmentCode), state["configuration"].get((configurationKey, None)))


def assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True):
    return 0
