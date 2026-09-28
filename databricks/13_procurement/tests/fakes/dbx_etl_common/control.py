from contextlib import contextmanager

calls = []
_nextId = [100]


def _record(name, **kwargs):
    calls.append((name, kwargs))


def reset():
    del calls[:]


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    _nextId[0] += 1
    _record("logPackageStart", batchId=batchId, packageName=packageName, projectName=projectName, stepName=stepName)
    return _nextId[0]


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=None, rowsInserted=None,
                  rowsUpdated=None, rowsDeleted=None, rowsRejected=None, watermarkFrom=None, watermarkTo=None):
    _record("logPackageEnd", packageExecutionId=packageExecutionId, status=status, rowsRead=rowsRead,
            rowsInserted=rowsInserted, rowsUpdated=rowsUpdated, rowsDeleted=rowsDeleted, rowsRejected=rowsRejected)


class _Run:
    def __init__(self, packageExecutionId):
        self.packageExecutionId = packageExecutionId
        self.rowsRead = None
        self.rowsInserted = None
        self.rowsUpdated = None
        self.rowsDeleted = None
        self.rowsRejected = None
        self.watermarkFrom = None
        self.watermarkTo = None


@contextmanager
def packageRun(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    run = _Run(logPackageStart(spark, catalog, batchId, packageName, projectName, stepName))
    try:
        yield run
    except Exception as exc:
        logError(spark, catalog, packageExecutionId=run.packageExecutionId, batchId=batchId,
                 sourceName=packageName, errorDescription=str(exc))
        logPackageEnd(spark, catalog, run.packageExecutionId, status="Failed")
        raise
    logPackageEnd(spark, catalog, run.packageExecutionId, status="Succeeded", rowsRead=run.rowsRead,
                  rowsInserted=run.rowsInserted, rowsUpdated=run.rowsUpdated, rowsDeleted=run.rowsDeleted,
                  rowsRejected=run.rowsRejected, watermarkFrom=run.watermarkFrom, watermarkTo=run.watermarkTo)


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None,
             sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None):
    _record("logError", packageExecutionId=packageExecutionId, batchId=batchId, sourceName=sourceName,
            errorDescription=errorDescription)


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None,
                insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    _record("logRowCount", packageExecutionId=packageExecutionId, objectName=objectName,
            sourceRowCount=sourceRowCount, targetRowCount=targetRowCount, insertRowCount=insertRowCount,
            updateRowCount=updateRowCount, deleteRowCount=deleteRowCount, rejectRowCount=rejectRowCount)


def logRejectedRecord(spark, catalog, objectName, rejectReasonCode, packageExecutionId=None, batchId=None,
                      sourceSystemCode=None, businessKey=None, rejectReason=None, rejectStage="Stage",
                      recordPayload=None):
    _record("logRejectedRecord", objectName=objectName, rejectReasonCode=rejectReasonCode, businessKey=businessKey,
            rejectStage=rejectStage)


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None,
                         sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    n = rejectedDf.count()
    _record("logRejectedRecordSet", objectName=objectName, rejectStage=rejectStage, rejectReasonCode=rejectReasonCode,
            businessKeyColumn=businessKeyColumn, rejectedRowCount=n)
    return n


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    return None
