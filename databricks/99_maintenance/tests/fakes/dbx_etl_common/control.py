"""Records calls so tests can assert on the lifecycle; performs no I/O."""
calls = []
_nextId = [1]


def _record(name, **kwargs):
    calls.append((name, kwargs))
    _nextId[0] += 1
    return _nextId[0]


def reset():
    del calls[:]


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    return _record("logPackageStart", batchId=batchId, packageName=packageName, projectName=projectName, stepName=stepName)


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
    _record("logRowCount", objectName=objectName, sourceRowCount=sourceRowCount, targetRowCount=targetRowCount,
            insertRowCount=insertRowCount, updateRowCount=updateRowCount, deleteRowCount=deleteRowCount,
            rejectRowCount=rejectRowCount)


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    raise KeyError(configurationKey)


def purgeControlHistory(spark, catalog, retentionDays=None, executionHistoryMonths=13, errorHistoryMonths=24,
                        rejectHistoryMonths=12, qualityHistoryMonths=13, whatIf=False):
    _record("purgeControlHistory", retentionDays=retentionDays, executionHistoryMonths=executionHistoryMonths,
            errorHistoryMonths=errorHistoryMonths, rejectHistoryMonths=rejectHistoryMonths,
            qualityHistoryMonths=qualityHistoryMonths, whatIf=whatIf)
