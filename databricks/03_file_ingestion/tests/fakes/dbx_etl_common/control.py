"""In-memory recorder for the control API surface used by 03_file_ingestion."""

import itertools

calls = []
_ids = itertools.count(1000)


def reset():
    del calls[:]


def _record(name, **kwargs):
    calls.append((name, kwargs))


def callsNamed(name):
    return [kwargs for called, kwargs in calls if called == name]


def startBatch(spark, catalog, batchName, batchType="Daily", businessDate=None, environmentCode=None,
               allowAdoptRunning=False, notes=None):
    batchId = next(_ids)
    _record("startBatch", batchName=batchName, batchType=batchType, allowAdoptRunning=allowAdoptRunning, batchId=batchId)
    return batchId


def endBatch(spark, catalog, batchId, forceStatus=None):
    _record("endBatch", batchId=batchId, forceStatus=forceStatus)


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    packageExecutionId = next(_ids)
    _record("logPackageStart", batchId=batchId, packageName=packageName, projectName=projectName,
            stepName=stepName, packageExecutionId=packageExecutionId)
    return packageExecutionId


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=None, rowsInserted=None,
                  rowsUpdated=None, rowsDeleted=None, rowsRejected=None, watermarkFrom=None, watermarkTo=None):
    _record("logPackageEnd", packageExecutionId=packageExecutionId, status=status, rowsRead=rowsRead,
            rowsInserted=rowsInserted, rowsRejected=rowsRejected)


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None,
             sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None):
    _record("logError", packageExecutionId=packageExecutionId, errorSeverity=errorSeverity, errorCode=errorCode,
            sourceName=sourceName, errorDescription=errorDescription)


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None,
                insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    _record("logRowCount", packageExecutionId=packageExecutionId, objectName=objectName,
            sourceRowCount=sourceRowCount, targetRowCount=targetRowCount, rejectRowCount=rejectRowCount)


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None,
                         sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    count = rejectedDf.count()
    _record("logRejectedRecordSet", objectName=objectName, rejectedRowCount=count, batchId=batchId,
            packageExecutionId=packageExecutionId, sourceSystemCode=sourceSystemCode, rejectStage=rejectStage,
            rejectReasonCode=rejectReasonCode, businessKeyColumn=businessKeyColumn)
    return count
