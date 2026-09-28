"""Records every lifecycle call in memory so tests can assert on them."""
calls = []
_ids = {"batch": 100, "package": 1000}


def _record(name, **kwargs):
    calls.append((name, kwargs))


def reset():
    calls.clear()


def startBatch(spark, catalog, batchName, batchType="Daily", businessDate=None, environmentCode=None,
               allowAdoptRunning=False, notes=None):
    _ids["batch"] += 1
    _record("startBatch", batchName=batchName, businessDate=businessDate, environmentCode=environmentCode, notes=notes)
    return _ids["batch"]


def endBatch(spark, catalog, batchId, forceStatus=None):
    _record("endBatch", batchId=batchId, forceStatus=forceStatus)


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    _ids["package"] += 1
    _record("logPackageStart", batchId=batchId, packageName=packageName, projectName=projectName, stepName=stepName)
    return _ids["package"]


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=None, rowsInserted=None,
                  rowsUpdated=None, rowsDeleted=None, rowsRejected=None, watermarkFrom=None, watermarkTo=None):
    _record("logPackageEnd", packageExecutionId=packageExecutionId, status=status, rowsRead=rowsRead,
            rowsInserted=rowsInserted, rowsUpdated=rowsUpdated, rowsDeleted=rowsDeleted, rowsRejected=rowsRejected)


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None,
             sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None):
    _record("logError", packageExecutionId=packageExecutionId, errorSeverity=errorSeverity, errorCode=errorCode,
            sourceName=sourceName, sourceComponent=sourceComponent, errorDescription=errorDescription)


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None,
                insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    _record("logRowCount", packageExecutionId=packageExecutionId, objectName=objectName, sourceRowCount=sourceRowCount,
            targetRowCount=targetRowCount, insertRowCount=insertRowCount, updateRowCount=updateRowCount,
            deleteRowCount=deleteRowCount, rejectRowCount=rejectRowCount)


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None,
                         sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    count = rejectedDf.count()
    _record("logRejectedRecordSet", objectName=objectName, count=count, rejectReasonCode=rejectReasonCode)
    return count


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    return "1.0"
