"""Records every control call so tests can assert on lifecycle / audit behaviour."""

calls: list[tuple] = []
_ids = {"batch": 100, "package": 1000}


def reset():
    calls.clear()


def startBatch(spark, catalog, batchName, batchType="Daily", businessDate=None, environmentCode=None,
               allowAdoptRunning=False, notes=None):
    _ids["batch"] += 1
    calls.append(("startBatch", batchName, batchType))
    return _ids["batch"]


def endBatch(spark, catalog, batchId, forceStatus=None):
    calls.append(("endBatch", batchId, forceStatus))


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    _ids["package"] += 1
    calls.append(("logPackageStart", batchId, packageName, projectName))
    return _ids["package"]


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=None, rowsInserted=None,
                  rowsUpdated=None, rowsDeleted=None, rowsRejected=None, watermarkFrom=None, watermarkTo=None):
    calls.append(("logPackageEnd", packageExecutionId, status, rowsRead, rowsInserted, rowsRejected))


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None,
             sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None):
    calls.append(("logError", packageExecutionId, errorSeverity, sourceName, errorDescription))


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None,
                insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    calls.append(("logRowCount", objectName, sourceRowCount, targetRowCount, rejectRowCount))


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None,
                         sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    n = rejectedDf.count()
    calls.append(("logRejectedRecordSet", objectName, rejectReasonCode, rejectStage, n))
    return n


def assertRowCountTolerance(spark, catalog, batchId, scope="ALL", objectName=None, absoluteTolerance=None,
                            percentTolerance=None, raiseOnFailure=True):
    calls.append(("assertRowCountTolerance", batchId, scope, objectName, absoluteTolerance))
    return 0
