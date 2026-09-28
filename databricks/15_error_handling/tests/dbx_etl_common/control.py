"""Records every control-framework call so tests can assert on the sequence."""

calls = []


def _record(name, **kwargs):
    calls.append((name, kwargs))


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    _record("logPackageStart", batchId=batchId, packageName=packageName, projectName=projectName, stepName=stepName)
    return 1000 + len(calls)


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", **kwargs):
    _record("logPackageEnd", packageExecutionId=packageExecutionId, status=status, **kwargs)


def endBatchStep(spark, catalog, batchStepId, status="Succeeded"):
    _record("endBatchStep", batchStepId=batchStepId, status=status)


def endBatch(spark, catalog, batchId, forceStatus=None):
    _record("endBatch", batchId=batchId, forceStatus=forceStatus)


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None,
             sourceName=None, sourceComponent=None, procedureName=None, errorDescription=None):
    _record("logError", packageExecutionId=packageExecutionId, batchId=batchId, errorSeverity=errorSeverity,
            errorCode=errorCode, sourceName=sourceName, sourceComponent=sourceComponent, errorDescription=errorDescription)


def logRowCount(spark, catalog, packageExecutionId, objectName, **kwargs):
    _record("logRowCount", packageExecutionId=packageExecutionId, objectName=objectName, **kwargs)


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, **kwargs):
    count = rejectedDf.count()
    _record("logRejectedRecordSet", objectName=objectName, rejectedRowCount=count, **kwargs)
    return count


def assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True):
    _record("assertRowCountReconciliation", batchId=batchId, raiseOnFailure=raiseOnFailure)
    return 0
