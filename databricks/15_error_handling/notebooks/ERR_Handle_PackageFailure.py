# Databricks notebook source
# MAGIC %md
# MAGIC # ERR_Handle_PackageFailure
# MAGIC Central failure handler (legacy `ssis/15_error_handling/ERR_Handle_PackageFailure.dtsx`).
# MAGIC
# MAGIC Wire this notebook as a task with `run_if: AT_LEAST_ONE_FAILED` (or `ALL_FAILED`) behind the
# MAGIC tasks it protects. It reads the failed task context from `dbutils.jobs.taskValues` and the
# MAGIC Jobs run API (`WorkspaceClient.jobs.get_run` / `get_run_output`), writes the error once through
# MAGIC `control.logError`, closes the open package execution(s) and batch step(s) as `Failed`, classifies
# MAGIC the failure as TRANSIENT or PERMANENT from the native error code, and either fails the batch
# MAGIC (`control.endBatch(forceStatus="Failed")`) or marks it `RetryPending` for `ERR_Retry_FailedSteps`.
# MAGIC
# MAGIC Package parameters (all strings, optional): `FailedPackage`, `FailureErrorCode`, `FailureMessage`,
# MAGIC `FailBatchOnPermanent` (default `True`), `jobRunId` (pass `{{job.run_id}}`).

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
from err_handling import failure, tables, widgets  # noqa: E402

PACKAGE_NAME = "ERR_Handle_PackageFailure"
PROJECT_NAME = "WWI_ErrorHandling"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = int(p["batchId"])

failedPackageParam = widgets.getText(dbutils, "FailedPackage", "")
failureErrorCodeParam = widgets.getInt(dbutils, "FailureErrorCode", 0)
failureMessageParam = widgets.getText(dbutils, "FailureMessage", "")
failBatchOnPermanent = widgets.getBool(dbutils, "FailBatchOnPermanent", True)
jobRunId = widgets.getText(dbutils, "jobRunId", "")

batchTable = naming.table(catalog, "etl", "batch")
batchStepTable = naming.table(catalog, "etl", "batch_step")
packageExecutionTable = naming.table(catalog, "etl", "package_execution")
stepFailureTable = tables.ensureWorkTable(spark, naming, catalog, "work_step_failure")

# COMMAND ----------

# MAGIC %md ## Discover the failed task context
# MAGIC Precedence: explicit `FailedPackage`/`FailureErrorCode`/`FailureMessage` parameters (legacy child-package
# MAGIC call) > task values published by the failed task (`packageExecutionId`, `errorCode`, `errorMessage`)
# MAGIC > Jobs run API (`get_run` for the failed task keys, `get_run_output` for the error text).


def discoverFailures():
    """Return a list of dicts: packageName, errorCode, errorMessage, packageExecutionId."""
    if failedPackageParam:
        return [{
            "packageName": failedPackageParam,
            "errorCode": failureErrorCodeParam,
            "errorMessage": failureMessageParam,
            "packageExecutionId": None,
        }]
    failures = []
    if jobRunId:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
        run = client.jobs.get_run(run_id=int(jobRunId))
        for task in failure.failedTasksFromRun(run):
            errorMessage = ""
            try:
                output = client.jobs.get_run_output(run_id=task["runId"])
                errorMessage = output.error or ""
                if output.error_trace:
                    errorMessage = "%s\n%s" % (errorMessage, output.error_trace[:4000])
            except Exception as exc:  # the run output is best effort; the failure is still handled
                errorMessage = "run output unavailable: %s" % exc
            taskKey = task["taskKey"]
            publishedCode = dbutils.jobs.taskValues.get(taskKey=taskKey, key="errorCode", default=None, debugValue=None)
            publishedMessage = dbutils.jobs.taskValues.get(taskKey=taskKey, key="errorMessage", default=None, debugValue=None)
            publishedExecutionId = dbutils.jobs.taskValues.get(taskKey=taskKey, key="packageExecutionId", default=None, debugValue=None)
            failures.append({
                "packageName": taskKey,
                "errorCode": int(publishedCode) if publishedCode not in (None, "") else failure.extractErrorCode(errorMessage),
                "errorMessage": publishedMessage or errorMessage or task["resultState"],
                "packageExecutionId": int(publishedExecutionId) if publishedExecutionId not in (None, "") else None,
            })
    return failures


failures = discoverFailures()
if not failures:
    raise ValueError(
        "ERR_Handle_PackageFailure could not determine the failed package: pass FailedPackage or jobRunId ({{job.run_id}})."
    )
print("Handling %d failure(s): %s" % (len(failures), [f["packageName"] for f in failures]))

# COMMAND ----------

# MAGIC %md ## Log, classify and close


def closeFailedPackage(packageName, packageExecutionId, retryableFlag):
    """Legacy Close Failed Batch Step + Close Package Execution, through the control framework."""
    running = spark.sql(
        "SELECT PackageExecutionId, BatchStepId FROM %s WHERE BatchId = %d AND PackageName = %s AND Status = 'Running'"
        % (packageExecutionTable, batchId, tables.sqlLiteral(packageName))
    ).collect()
    closedExecutions, closedSteps = [], []
    for row in running:
        control.logPackageEnd(spark, catalog, int(row["PackageExecutionId"]), status="Failed")
        closedExecutions.append(int(row["PackageExecutionId"]))
        if row["BatchStepId"] is not None:
            closedSteps.append(int(row["BatchStepId"]))
    if packageExecutionId is not None and packageExecutionId not in closedExecutions:
        stepRow = spark.sql(
            "SELECT BatchStepId, Status FROM %s WHERE PackageExecutionId = %d" % (packageExecutionTable, packageExecutionId)
        ).first()
        if stepRow is not None and stepRow["Status"] == "Running":
            control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
            closedExecutions.append(packageExecutionId)
        if stepRow is not None and stepRow["BatchStepId"] is not None:
            closedSteps.append(int(stepRow["BatchStepId"]))
    for batchStepId in sorted(set(closedSteps)):
        status = tables.scalar(spark, "SELECT Status FROM %s WHERE BatchStepId = %d" % (batchStepTable, batchStepId))
        if status == "Running":
            control.endBatchStep(spark, catalog, batchStepId, status="Failed")
    return closedExecutions, sorted(set(closedSteps))


def recordClassification(packageName, packageExecutionId, batchStepIds, failureClass, retryableFlag, errorCode, errorMessage):
    """IsRetryable / ErrorMessage / FailureClass that legacy wrote onto etl.BatchStep."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    for batchStepId in (batchStepIds or [None]):
        spark.sql(
            "INSERT INTO %s (BatchId, BatchStepId, PackageExecutionId, PackageName, FailureClass, IsRetryable, ErrorCode, ErrorMessage, ClassifiedAtUtc) "
            "VALUES (%d, %s, %s, %s, %s, %d, %d, %s, TIMESTAMP'%s')"
            % (
                stepFailureTable, batchId,
                "NULL" if batchStepId is None else str(batchStepId),
                "NULL" if packageExecutionId is None else str(packageExecutionId),
                tables.sqlLiteral(packageName), tables.sqlLiteral(failureClass), int(retryableFlag), int(errorCode or 0),
                tables.sqlLiteral((errorMessage or "")[:4000]), now,
            )
        )


handlerExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName="Failure Handling")
errorsLogged = 0
overallClass = failure.TRANSIENT
try:
    for item in failures:
        failureClass, retryableFlag = failure.classifyFailure(item["errorCode"])
        control.logError(
            spark, catalog,
            packageExecutionId=item["packageExecutionId"], batchId=batchId, errorSeverity="Error",
            errorCode=int(item["errorCode"] or 0), sourceName=item["packageName"],
            sourceComponent=PACKAGE_NAME, errorDescription=item["errorMessage"],
        )
        errorsLogged += 1
        closedExecutions, closedSteps = closeFailedPackage(item["packageName"], item["packageExecutionId"], retryableFlag)
        recordClassification(
            item["packageName"], item["packageExecutionId"] or (closedExecutions[0] if closedExecutions else None),
            closedSteps, failureClass, retryableFlag, item["errorCode"], item["errorMessage"],
        )
        if failureClass == failure.PERMANENT:
            overallClass = failure.PERMANENT
        print("%s -> %s (retryable=%d), closed executions %s, steps %s" % (item["packageName"], failureClass, retryableFlag, closedExecutions, closedSteps))

    # Legacy: Fail Batch when PERMANENT && FailBatchOnPermanent; Mark Batch Retryable when TRANSIENT.
    batchOutcome = "Unchanged"
    if overallClass == failure.PERMANENT and failBatchOnPermanent:
        control.endBatch(spark, catalog, batchId, forceStatus="Failed")
        batchOutcome = "Failed"
    elif overallClass == failure.TRANSIENT:
        spark.sql("UPDATE %s SET Status = 'RetryPending' WHERE BatchId = %d AND Status = 'Running'" % (batchTable, batchId))
        batchOutcome = "RetryPending"

    control.logRowCount(spark, catalog, handlerExecutionId, "etl.ErrorLog", sourceRowCount=len(failures), targetRowCount=errorsLogged, insertRowCount=errorsLogged)
    control.logPackageEnd(spark, catalog, handlerExecutionId, status="Succeeded", rowsRead=len(failures), rowsInserted=errorsLogged)
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=handlerExecutionId, batchId=batchId, sourceName=PACKAGE_NAME, sourceComponent="OnError", errorDescription=str(exc))
    control.logPackageEnd(spark, catalog, handlerExecutionId, status="Failed", rowsRead=len(failures), rowsInserted=errorsLogged)
    raise

# COMMAND ----------

dbutils.jobs.taskValues.set(key="failureClass", value=overallClass)
dbutils.jobs.taskValues.set(key="retryableFlag", value=1 if overallClass == failure.TRANSIENT else 0)
dbutils.jobs.taskValues.set(key="failedPackages", value=",".join(f["packageName"] for f in failures))
dbutils.jobs.taskValues.set(key="batchOutcome", value=batchOutcome)
dbutils.notebook.exit(
    '{"package": "%s", "batchId": %d, "failureClass": "%s", "batchOutcome": "%s", "failedPackages": "%s"}'
    % (PACKAGE_NAME, batchId, overallClass, batchOutcome, ",".join(f["packageName"] for f in failures))
)
