from contextlib import contextmanager
from datetime import datetime

calls = []
rowCounts = []
rejects = []
errors = []
watermarks = {}
configuration = {}
_nextId = {"batch": 100, "step": 200, "package": 300}


def reset():
    calls.clear()
    rowCounts.clear()
    rejects.clear()
    errors.clear()
    watermarks.clear()
    configuration.clear()


def _next(kind):
    _nextId[kind] += 1
    return _nextId[kind]


def startBatch(spark, catalog, batchName, batchType="Daily", businessDate=None, environmentCode=None, allowAdoptRunning=False, notes=None):
    calls.append(("startBatch", batchName))
    return _next("batch")


def endBatch(spark, catalog, batchId, forceStatus=None):
    calls.append(("endBatch", batchId, forceStatus))


def startBatchStep(spark, catalog, batchId, stepName, stepSequence, stepGroup=None):
    calls.append(("startBatchStep", stepName))
    return _next("step")


def endBatchStep(spark, catalog, batchStepId, status="Succeeded"):
    calls.append(("endBatchStep", batchStepId, status))


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    calls.append(("logPackageStart", packageName))
    return _next("package")


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=None, rowsInserted=None, rowsUpdated=None, rowsDeleted=None, rowsRejected=None, watermarkFrom=None, watermarkTo=None):
    calls.append(("logPackageEnd", packageExecutionId, status))


class _Run:
    def __init__(self, packageExecutionId):
        self.packageExecutionId = packageExecutionId
        self.rowsRead = 0
        self.rowsInserted = 0
        self.rowsUpdated = 0
        self.rowsDeleted = 0
        self.rowsRejected = 0


@contextmanager
def packageRun(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    run = _Run(logPackageStart(spark, catalog, batchId, packageName, projectName, stepName))
    try:
        yield run
    except Exception as exc:
        logError(spark, catalog, packageExecutionId=run.packageExecutionId, batchId=batchId, errorDescription=str(exc))
        logPackageEnd(spark, catalog, run.packageExecutionId, status="Failed")
        raise
    logPackageEnd(spark, catalog, run.packageExecutionId, status="Succeeded", rowsRead=run.rowsRead)


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None, sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None):
    errors.append({"packageExecutionId": packageExecutionId, "errorCode": errorCode, "errorDescription": errorDescription})


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None, insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    rowCounts.append({
        "packageExecutionId": packageExecutionId, "objectName": objectName, "sourceRowCount": sourceRowCount,
        "targetRowCount": targetRowCount, "insertRowCount": insertRowCount, "updateRowCount": updateRowCount,
        "deleteRowCount": deleteRowCount, "rejectRowCount": rejectRowCount,
    })


def logRejectedRecord(spark, catalog, objectName, rejectReasonCode, packageExecutionId=None, batchId=None, sourceSystemCode=None, businessKey=None, rejectReason=None, rejectStage="Stage", recordPayload=None):
    rejects.append({"objectName": objectName, "rejectReasonCode": rejectReasonCode, "businessKey": businessKey, "count": 1})


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None, sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    n = rejectedDf.count()
    rejects.append({"objectName": objectName, "rejectReasonCode": rejectReasonCode, "businessKey": businessKeyColumn, "count": n})
    return n


EPOCH_WINDOW = ("1900-01-01T00:00:00.000", "9999-12-31T00:00:00.000")


def getWatermark(spark, catalog, sourceSystemCode, objectName, reloadFullHistory=False):
    """Like the real usp_GetWatermark port: (watermarkFrom, watermarkTo) as yyyy-MM-ddTHH:mm:ss.fff strings."""
    if reloadFullHistory:
        return EPOCH_WINDOW
    return watermarks.get((sourceSystemCode, objectName), EPOCH_WINDOW)


def setWatermark(spark, catalog, sourceSystemCode, objectName, watermarkTo, packageExecutionId=None, allowRewind=False):
    assert isinstance(watermarkTo, str), "setWatermark takes the ISO string the contract specifies"
    calls.append(("setWatermark", sourceSystemCode, objectName, watermarkTo))
    watermarks[(sourceSystemCode, objectName)] = (watermarkTo, watermarkTo)


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    return configuration.get(configurationKey)


def assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True):
    calls.append(("assertRowCountReconciliation", batchId))
    return 0


def assertRowCountTolerance(spark, catalog, batchId, scope="ALL", objectName=None, absoluteTolerance=None, percentTolerance=None, raiseOnFailure=True):
    calls.append(("assertRowCountTolerance", batchId, objectName))
    return 0


def evaluateDataQualityRules(spark, catalog, batchId=None, packageExecutionId=None, ruleGroupCode=None, objectName=None, regionCode=None, businessDate=None):
    calls.append(("evaluateDataQualityRules", objectName))
    return 0


def purgeControlHistory(spark, catalog, retentionDays=None, executionHistoryMonths=13, errorHistoryMonths=24, rejectHistoryMonths=12, qualityHistoryMonths=13, whatIf=False):
    calls.append(("purgeControlHistory",))
