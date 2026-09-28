"""Minimal in-memory recorder of the control calls the dimension notebooks make."""
from contextlib import contextmanager

calls = []


def _record(name, **kwargs):
    calls.append((name, kwargs))


def startBatch(spark, catalog, batchName, batchType="Daily", businessDate=None, environmentCode=None, allowAdoptRunning=False, notes=None):
    _record("startBatch", batchName=batchName)
    return 1


def endBatch(spark, catalog, batchId, forceStatus=None):
    _record("endBatch", batchId=batchId, forceStatus=forceStatus)


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    _record("logPackageStart", packageName=packageName)
    return 100


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", **kwargs):
    _record("logPackageEnd", packageExecutionId=packageExecutionId, status=status, **kwargs)


class _Run:
    def __init__(self, packageExecutionId):
        self.packageExecutionId = packageExecutionId
        self.rowsRead = self.rowsInserted = self.rowsUpdated = self.rowsDeleted = self.rowsRejected = 0


@contextmanager
def packageRun(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    run = _Run(logPackageStart(spark, catalog, batchId, packageName, projectName, stepName))
    try:
        yield run
    except Exception as exc:
        logError(spark, catalog, packageExecutionId=run.packageExecutionId, errorDescription=str(exc))
        logPackageEnd(spark, catalog, run.packageExecutionId, status="Failed")
        raise
    logPackageEnd(spark, catalog, run.packageExecutionId, status="Succeeded", rowsRead=run.rowsRead,
                  rowsInserted=run.rowsInserted, rowsUpdated=run.rowsUpdated, rowsRejected=run.rowsRejected)


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None,
             sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None):
    _record("logError", errorCode=errorCode, errorDescription=errorDescription)


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None,
                insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    _record("logRowCount", objectName=objectName, sourceRowCount=sourceRowCount, targetRowCount=targetRowCount,
            insertRowCount=insertRowCount, updateRowCount=updateRowCount, rejectRowCount=rejectRowCount)


def logRejectedRecord(spark, catalog, objectName, rejectReasonCode, **kwargs):
    _record("logRejectedRecord", objectName=objectName, rejectReasonCode=rejectReasonCode)


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None,
                         sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    n = rejectedDf.count()
    _record("logRejectedRecordSet", objectName=objectName, rejectedRowCount=n)
    return n


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    return {"Dimension.City.PopulationChangeThreshold": "5.0", "Dimension.Rekey.MaxQueueAgeDays": "30"}.get(configurationKey)


def getWatermark(spark, catalog, sourceSystemCode, objectName, reloadFullHistory=False):
    return None, None


def setWatermark(spark, catalog, sourceSystemCode, objectName, watermarkTo, packageExecutionId=None, allowRewind=False):
    _record("setWatermark", objectName=objectName)
