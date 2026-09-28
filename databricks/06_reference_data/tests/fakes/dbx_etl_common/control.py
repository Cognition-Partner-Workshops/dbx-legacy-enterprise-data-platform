import itertools
from contextlib import contextmanager

from pyspark.sql import functions as F

CALLS = []
ROW_COUNTS = []
ERRORS = []
REJECT_SETS = []
PACKAGE_RUNS = {}
CONFIGURATION = {}
_ids = itertools.count(1)


def reset():
    del CALLS[:], ROW_COUNTS[:], ERRORS[:], REJECT_SETS[:]
    PACKAGE_RUNS.clear()
    CONFIGURATION.clear()


def _record(name, **kwargs):
    CALLS.append((name, kwargs))


def startBatch(spark, catalog, batchName, batchType="Daily", businessDate=None, environmentCode=None,
               allowAdoptRunning=False, notes=None):
    _record("startBatch", batchName=batchName, batchType=batchType, businessDate=businessDate)
    return 900 + next(_ids)


def endBatch(spark, catalog, batchId, forceStatus=None):
    _record("endBatch", batchId=batchId, forceStatus=forceStatus)


def startBatchStep(spark, catalog, batchId, stepName, stepSequence, stepGroup=None):
    _record("startBatchStep", stepName=stepName)
    return next(_ids)


def endBatchStep(spark, catalog, batchStepId, status="Succeeded"):
    _record("endBatchStep", batchStepId=batchStepId, status=status)


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    packageExecutionId = next(_ids)
    PACKAGE_RUNS[packageExecutionId] = {"packageName": packageName, "projectName": projectName, "stepName": stepName,
                                        "batchId": batchId, "status": "Running"}
    _record("logPackageStart", packageName=packageName, projectName=projectName)
    return packageExecutionId


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=None, rowsInserted=None,
                  rowsUpdated=None, rowsDeleted=None, rowsRejected=None, watermarkFrom=None, watermarkTo=None):
    PACKAGE_RUNS[packageExecutionId].update(status=status, rowsRead=rowsRead, rowsInserted=rowsInserted,
                                            rowsUpdated=rowsUpdated, rowsDeleted=rowsDeleted, rowsRejected=rowsRejected)
    _record("logPackageEnd", packageExecutionId=packageExecutionId, status=status)


@contextmanager
def packageRun(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    raise NotImplementedError("the notebooks use wwi_ref.runtime.packageRun")


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None,
             sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None):
    ERRORS.append({"packageExecutionId": packageExecutionId, "batchId": batchId, "errorSeverity": errorSeverity,
                   "errorCode": errorCode, "sourceName": sourceName, "sourceComponent": sourceComponent,
                   "errorDescription": errorDescription})


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None,
                insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    ROW_COUNTS.append({"packageExecutionId": packageExecutionId, "objectName": objectName, "sourceRowCount": sourceRowCount,
                       "targetRowCount": targetRowCount, "insertRowCount": insertRowCount, "updateRowCount": updateRowCount,
                       "deleteRowCount": deleteRowCount, "rejectRowCount": rejectRowCount})


def logRejectedRecord(spark, catalog, objectName, rejectReasonCode, packageExecutionId=None, batchId=None,
                      sourceSystemCode=None, businessKey=None, rejectReason=None, rejectStage="Stage", recordPayload=None):
    REJECT_SETS.append({"objectName": objectName, "rejectReasonCode": rejectReasonCode, "count": 1})


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None,
                         sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    if businessKeyColumn is not None and businessKeyColumn not in rejectedDf.columns:
        raise ValueError("businessKeyColumn %s not in rejected frame" % businessKeyColumn)
    count = rejectedDf.count()
    REJECT_SETS.append({"objectName": objectName, "rejectReasonCode": rejectReasonCode, "count": count,
                        "rejectStage": rejectStage, "batchId": batchId})
    return count


def getWatermark(spark, catalog, sourceSystemCode, objectName, reloadFullHistory=False):
    return None, None


def setWatermark(spark, catalog, sourceSystemCode, objectName, watermarkTo, packageExecutionId=None, allowRewind=False):
    _record("setWatermark", objectName=objectName)


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    if configurationKey not in CONFIGURATION:
        raise KeyError("Configuration key %s is not defined" % configurationKey)
    return CONFIGURATION[configurationKey]


def assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True):
    return 0


def assertRowCountTolerance(spark, catalog, batchId, scope="ALL", objectName=None, absoluteTolerance=None,
                            percentTolerance=None, raiseOnFailure=True):
    return 0


def evaluateDataQualityRules(spark, catalog, batchId=None, packageExecutionId=None, ruleGroupCode=None, objectName=None,
                             regionCode=None, businessDate=None):
    return 0


def purgeControlHistory(spark, catalog, retentionDays=None, executionHistoryMonths=13, errorHistoryMonths=24,
                        rejectHistoryMonths=12, qualityHistoryMonths=13, whatIf=False):
    return None
