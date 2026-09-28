"""No-op control API with the shared signatures; records calls for assertions."""
from contextlib import contextmanager

calls = []


class _Run:
    packageExecutionId = 1
    rowsRead = rowsInserted = rowsUpdated = rowsDeleted = rowsRejected = None


@contextmanager
def packageRun(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    calls.append(("packageRun", packageName))
    yield _Run()


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None,
             sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None):
    calls.append(("logError", errorSeverity, errorDescription))


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None,
                insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    calls.append(("logRowCount", objectName, sourceRowCount, targetRowCount, rejectRowCount))


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None,
                         sourceSystemCode=None, rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    n = rejectedDf.count()
    calls.append(("logRejectedRecordSet", objectName, rejectReasonCode, rejectStage, n))
    return n


def evaluateDataQualityRules(spark, catalog, batchId=None, packageExecutionId=None, ruleGroupCode=None,
                             objectName=None, regionCode=None, businessDate=None):
    calls.append(("evaluateDataQualityRules", ruleGroupCode, objectName))
    return 0


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    raise KeyError(configurationKey)


def assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True):
    return 0
