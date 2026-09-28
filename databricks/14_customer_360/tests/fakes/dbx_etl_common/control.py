calls = []
_nextId = [0]


def _record(name, **kwargs):
    calls.append((name, kwargs))


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    _nextId[0] += 1
    _record("logPackageStart", batchId=batchId, packageName=packageName, projectName=projectName, stepName=stepName)
    return _nextId[0]


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
    _record("logRowCount", packageExecutionId=packageExecutionId, objectName=objectName,
            sourceRowCount=sourceRowCount, targetRowCount=targetRowCount, rejectRowCount=rejectRowCount)


def assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True):
    _record("assertRowCountReconciliation", batchId=batchId)
    return 0


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    return None
