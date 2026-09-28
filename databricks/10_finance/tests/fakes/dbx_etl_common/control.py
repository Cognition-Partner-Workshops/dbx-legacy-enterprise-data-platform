calls: list[tuple] = []
_ids = {"batch": 100, "step": 200, "pkg": 300}


def _next(kind):
    _ids[kind] += 1
    return _ids[kind]


def startBatch(spark, catalog, batchName, batchType="Daily", businessDate=None, environmentCode=None, allowAdoptRunning=False, notes=None):
    calls.append(("startBatch", batchName, batchType, allowAdoptRunning))
    return _next("batch")


def endBatch(spark, catalog, batchId, forceStatus=None):
    calls.append(("endBatch", batchId, forceStatus))


def startBatchStep(spark, catalog, batchId, stepName, stepSequence, stepGroup=None):
    calls.append(("startBatchStep", stepName, stepSequence))
    return _next("step")


def endBatchStep(spark, catalog, batchStepId, status="Succeeded"):
    calls.append(("endBatchStep", batchStepId, status))


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    calls.append(("logPackageStart", packageName, projectName, stepName))
    return _next("pkg")


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=None, rowsInserted=None, rowsUpdated=None,
                  rowsDeleted=None, rowsRejected=None, watermarkFrom=None, watermarkTo=None):
    calls.append(("logPackageEnd", packageExecutionId, status, rowsRead, rowsInserted, rowsRejected))


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None, sourceName=None,
             sourceComponent=None, procedureName=None, errorDescription=None):
    calls.append(("logError", errorSeverity, errorCode, sourceName, errorDescription))


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None, insertRowCount=None,
                updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    calls.append(("logRowCount", objectName, sourceRowCount, targetRowCount, rejectRowCount))


def logRejectedRecord(spark, catalog, objectName, rejectReasonCode, packageExecutionId=None, batchId=None, sourceSystemCode=None,
                      businessKey=None, rejectReason=None, rejectStage="Stage", recordPayload=None):
    calls.append(("logRejectedRecord", objectName, rejectReasonCode, businessKey))


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None, sourceSystemCode=None,
                         rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    n = rejectedDf.count()
    calls.append(("logRejectedRecordSet", objectName, rejectReasonCode, n))
    return n


def getWatermark(spark, catalog, sourceSystemCode, objectName, reloadFullHistory=False):
    return None, None


def setWatermark(spark, catalog, sourceSystemCode, objectName, watermarkTo, packageExecutionId=None, allowRewind=False):
    calls.append(("setWatermark", objectName, watermarkTo))


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    return None


def assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True):
    return 0


def assertRowCountTolerance(spark, catalog, batchId, scope="ALL", objectName=None, absoluteTolerance=None, percentTolerance=None, raiseOnFailure=True):
    calls.append(("assertRowCountTolerance", scope, absoluteTolerance, raiseOnFailure))
    return 0
