"""Records calls so tests can assert on the lifecycle; performs no etl.* I/O."""
import datetime as dt
from contextlib import contextmanager

calls = []
watermarks = {}
configuration = {}
_nextId = [100]


def _record(name, **kwargs):
    calls.append((name, kwargs))
    _nextId[0] += 1
    return _nextId[0]


def reset():
    del calls[:]
    watermarks.clear()
    configuration.clear()


def callsNamed(name):
    return [kw for n, kw in calls if n == name]


class _Run:
    def __init__(self, packageExecutionId):
        self.packageExecutionId = packageExecutionId
        self.rowsRead = None
        self.rowsInserted = None
        self.rowsUpdated = None
        self.rowsDeleted = None
        self.rowsRejected = None


@contextmanager
def packageRun(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    packageExecutionId = logPackageStart(spark, catalog, batchId, packageName, projectName, stepName)
    run = _Run(packageExecutionId)
    try:
        yield run
    except Exception as exc:
        logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                 errorDescription=str(exc), sourceName=packageName)
        logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
        raise
    logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=run.rowsRead,
                  rowsInserted=run.rowsInserted, rowsUpdated=run.rowsUpdated, rowsDeleted=run.rowsDeleted,
                  rowsRejected=run.rowsRejected)


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    return _record("logPackageStart", batchId=batchId, packageName=packageName, projectName=projectName,
                   stepName=stepName)


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


def logRejectedRecord(spark, catalog, objectName, rejectReasonCode, packageExecutionId=None, batchId=None,
                      sourceSystemCode=None, businessKey=None, rejectReason=None, rejectStage="Stage",
                      recordPayload=None):
    _record("logRejectedRecord", objectName=objectName, rejectReasonCode=rejectReasonCode, businessKey=businessKey,
            rejectReason=rejectReason, rejectStage=rejectStage)


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None,
                         sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    n = rejectedDf.count()
    keys = [r[businessKeyColumn] for r in rejectedDf.select(businessKeyColumn).collect()] if businessKeyColumn else []
    _record("logRejectedRecordSet", objectName=objectName, rejectedRowCount=n, rejectStage=rejectStage,
            rejectReasonCode=rejectReasonCode, businessKeys=keys)
    return n


def getWatermark(spark, catalog, sourceSystemCode, objectName, reloadFullHistory=False):
    _record("getWatermark", objectName=objectName)
    wm = watermarks.get((sourceSystemCode, objectName))
    return dt.datetime(1900, 1, 1), wm


def setWatermark(spark, catalog, sourceSystemCode, objectName, watermarkTo, packageExecutionId=None,
                 allowRewind=False):
    watermarks[(sourceSystemCode, objectName)] = watermarkTo
    _record("setWatermark", objectName=objectName, watermarkTo=watermarkTo, allowRewind=allowRewind)


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    _record("getConfiguration", configurationKey=configurationKey)
    if configurationKey not in configuration:
        raise KeyError(configurationKey)
    return configuration[configurationKey]


def assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True):
    _record("assertRowCountReconciliation", batchId=batchId, raiseOnFailure=raiseOnFailure)
    return 0


def assertRowCountTolerance(spark, catalog, batchId, scope="ALL", objectName=None, absoluteTolerance=None,
                            percentTolerance=None, raiseOnFailure=True):
    _record("assertRowCountTolerance", scope=scope, objectName=objectName, absoluteTolerance=absoluteTolerance)
    return 0


def evaluateDataQualityRules(spark, catalog, batchId=None, packageExecutionId=None, ruleGroupCode=None,
                             objectName=None, regionCode=None, businessDate=None):
    _record("evaluateDataQualityRules", ruleGroupCode=ruleGroupCode, objectName=objectName)
    return 0
