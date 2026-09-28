# Databricks notebook source
# MAGIC %md
# MAGIC # ERR_Retry_FailedSteps
# MAGIC Bounded retry driver (legacy `ssis/15_error_handling/ERR_Retry_FailedSteps.dtsx`).
# MAGIC
# MAGIC Walks the retryable failed steps of a batch (steps closed `Failed` by `ERR_Handle_PackageFailure`
# MAGIC with a TRANSIENT classification, or whose failed package execution carries a transient error code),
# MAGIC re-opens them as `Pending` with `AttemptNumber + 1` after a backoff, writes an `etl.batch_step_rerun_request`
# MAGIC row per step, and declares retries exhausted once `MaxRetryAttempts` is reached. It does **not** execute the
# MAGIC packages: it publishes the list of packages to re-run (`retryPackages`) for the caller's ExtractAttempt loop.
# MAGIC
# MAGIC Callable interface (see `docs/migration/15_error_handling-package-mapping.md`):
# MAGIC * inputs: `BatchId`, `MaxRetryAttempts` (default `3`; bind session 00's `MaxExtractAttempts`),
# MAGIC   `BackoffBaseSeconds` (default `30`), `StepGroupScope` (default `""` = all; e.g. `Extract`), `RerunMode`
# MAGIC   (`request` default; `plan` skips backoff/writes and only reports), `catalog`.
# MAGIC * task values: `retryableStepCount`, `attemptNumber`, `stillFailingCount`, `retriesExhausted`, `retryPackages`.
# MAGIC * notebook exit: JSON with the same fields.

# COMMAND ----------

import json
import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
from err_handling import failure, retry, tables, widgets  # noqa: E402

PACKAGE_NAME = "ERR_Retry_FailedSteps"
PROJECT_NAME = "WWI_ErrorHandling"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = int(p["batchId"])

maxRetryAttempts = widgets.getInt(dbutils, "MaxRetryAttempts", 3)
backoffBaseSeconds = widgets.getInt(dbutils, "BackoffBaseSeconds", 30)
stepGroupScope = widgets.getText(dbutils, "StepGroupScope", "")
rerunMode = widgets.getText(dbutils, "RerunMode", "request").lower()

batchStepTable = naming.table(catalog, "etl", "batch_step")
packageExecutionTable = naming.table(catalog, "etl", "package_execution")
errorLogTable = naming.table(catalog, "etl", "error_log")
rerunRequestTable = naming.table(catalog, "etl", "batch_step_rerun_request")
stepFailureTable = tables.ensureWorkTable(spark, naming, catalog, "work_step_failure")

transientCodes = ",".join(str(c) for c in sorted(failure.TRANSIENT_ERROR_CODES))
scopeFilter = "" if not stepGroupScope else " AND s.StepGroup = %s" % tables.sqlLiteral(stepGroupScope)

# COMMAND ----------

# MAGIC %md ## Retryable steps
# MAGIC Legacy predicate: `StepStatus = 'Failed' AND IsRetryable = 1 AND AttemptNumber < MaxRetryAttempts`.
# MAGIC `IsRetryable` lives in `silver.work_step_failure` (written by the failure handler); a failed step is also
# MAGIC retryable when the latest error logged for its failed package execution carries a transient error code.

RETRYABLE_STEPS_SQL = """
WITH retryable AS (
    SELECT DISTINCT f.BatchStepId
    FROM {stepFailure} f
    WHERE f.BatchId = {batchId} AND f.IsRetryable = 1 AND f.BatchStepId IS NOT NULL
    UNION
    SELECT DISTINCT pe.BatchStepId
    FROM {packageExecution} pe
    JOIN {errorLog} e ON e.PackageExecutionId = pe.PackageExecutionId
    WHERE pe.BatchId = {batchId} AND pe.Status = 'Failed' AND pe.BatchStepId IS NOT NULL
      AND e.ErrorCode IN ({transientCodes})
)
SELECT s.BatchStepId, s.StepName, s.StepGroup, s.Status, COALESCE(s.AttemptNumber, 1) AS AttemptNumber
FROM {batchStep} s
JOIN retryable r ON r.BatchStepId = s.BatchStepId
WHERE s.BatchId = {batchId} AND s.Status IN ({statuses})
  AND COALESCE(s.AttemptNumber, 1) < {maxAttempts}{scopeFilter}
"""


def retryableSteps(statuses):
    sql = RETRYABLE_STEPS_SQL.format(
        stepFailure=stepFailureTable, packageExecution=packageExecutionTable, errorLog=errorLogTable,
        batchStep=batchStepTable, batchId=batchId, transientCodes=transientCodes,
        statuses=",".join("'%s'" % s for s in statuses), maxAttempts=maxRetryAttempts, scopeFilter=scopeFilter,
    )
    return spark.sql(sql).collect()


def failedPackagesForSteps(stepIds):
    """Packages to re-run: latest execution per package in the step is Failed and AttemptNumber < MaxRetryAttempts."""
    if not stepIds:
        return []
    rows = spark.sql(
        """
        WITH latest AS (
            SELECT PackageName, BatchStepId, Status, AttemptNumber,
                   ROW_NUMBER() OVER (PARTITION BY PackageName ORDER BY AttemptNumber DESC, PackageExecutionId DESC) AS rn
            FROM %s WHERE BatchId = %d
        )
        SELECT DISTINCT PackageName FROM latest
        WHERE rn = 1 AND Status = 'Failed' AND BatchStepId IN (%s) AND COALESCE(AttemptNumber, 1) < %d
        ORDER BY PackageName
        """
        % (packageExecutionTable, batchId, ",".join(str(int(s)) for s in stepIds), maxRetryAttempts)
    ).collect()
    return [r["PackageName"] for r in rows]


def sweep(attemptNumber):
    """One legacy Retry Sweep container: back off, reset the steps, request the reruns, recheck."""
    stepIds = [int(r["BatchStepId"]) for r in retryableSteps(["Failed"])]
    if not stepIds:
        return 0
    wait = retry.backoffSeconds(backoffBaseSeconds, attemptNumber)
    print("Sweep %d: %d retryable step(s), backing off %ds" % (attemptNumber, len(stepIds), wait))
    if rerunMode != "plan":
        time.sleep(wait)
        idList = ",".join(str(s) for s in stepIds)
        spark.sql(
            "UPDATE %s SET Status = 'Pending', AttemptNumber = COALESCE(AttemptNumber, 1) + 1, CompletedAtUtc = NULL "
            "WHERE BatchId = %d AND BatchStepId IN (%s)" % (batchStepTable, batchId, idList)
        )
        spark.sql(
            "INSERT INTO %s (BatchId, BatchStepId, PackageName, AttemptNumber, RequestedAtUtc, RequestStatus) "
            "SELECT s.BatchId, s.BatchStepId, pe.PackageName, s.AttemptNumber, current_timestamp(), 'Requested' "
            "FROM %s s JOIN (SELECT DISTINCT BatchStepId, PackageName FROM %s WHERE BatchId = %d AND Status = 'Failed') pe "
            "  ON pe.BatchStepId = s.BatchStepId "
            "WHERE s.BatchId = %d AND s.Status = 'Pending' AND s.BatchStepId IN (%s)"
            % (rerunRequestTable, batchStepTable, packageExecutionTable, batchId, batchId, idList)
        )
    return len(retryableSteps(["Failed", "Pending"]))


# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName="Extract Retry Driver")
result = {"package": PACKAGE_NAME, "batchId": batchId, "maxRetryAttempts": maxRetryAttempts}
try:
    attemptNumber = 1
    candidateSteps = retryableSteps(["Failed"])
    retryableStepCount = len(candidateSteps)
    retryPackages = failedPackagesForSteps([int(r["BatchStepId"]) for r in candidateSteps])
    print("Retryable steps: %d, packages to re-run: %s" % (retryableStepCount, retryPackages))

    sweeps = 0
    if retryableStepCount > 0:
        retryableStepCount = sweep(attemptNumber)
        sweeps += 1
        attemptNumber += 1
        if rerunMode != "plan":
            spark.sql(
                "UPDATE %s s SET AttemptNumber = %d WHERE s.BatchId = %d AND s.Status IN ('Failed', 'Pending') AND s.BatchStepId IN "
                "(SELECT BatchStepId FROM %s WHERE BatchId = %d AND IsRetryable = 1 AND BatchStepId IS NOT NULL)"
                % (batchStepTable, attemptNumber, batchId, stepFailureTable, batchId)
            )
        while (sweeps < retry.MAX_SWEEPS_PER_INVOCATION
               and retry.shouldSweepAgain(attemptNumber, maxRetryAttempts, retryableStepCount)):
            retryableStepCount = sweep(attemptNumber)
            sweeps += 1

    exhausted = retry.retriesExhausted(attemptNumber, maxRetryAttempts, retryableStepCount) or sweeps >= retry.MAX_SWEEPS_PER_INVOCATION
    if exhausted and rerunMode != "plan":
        # Legacy Mark Retries Exhausted: steps at the attempt limit become a permanent Failed.
        spark.sql(
            "UPDATE %s SET Status = 'Failed' WHERE BatchId = %d AND COALESCE(AttemptNumber, 1) >= %d AND Status IN ('Failed', 'Pending')"
            % (batchStepTable, batchId, maxRetryAttempts)
        )
        spark.sql(
            "UPDATE %s SET IsRetryable = 0, ErrorMessage = CONCAT(COALESCE(ErrorMessage, ''), ' | retry limit reached') "
            "WHERE BatchId = %d AND IsRetryable = 1 AND BatchStepId IN (SELECT BatchStepId FROM %s WHERE BatchId = %d AND COALESCE(AttemptNumber, 1) >= %d)"
            % (stepFailureTable, batchId, batchStepTable, batchId, maxRetryAttempts)
        )

    stillFailingCount = int(tables.scalar(spark, "SELECT COUNT(*) FROM %s WHERE BatchId = %d AND Status = 'Failed'" % (batchStepTable, batchId)) or 0)
    pendingCount = int(tables.scalar(spark, "SELECT COUNT(*) FROM %s WHERE BatchId = %d AND Status = 'Pending'" % (batchStepTable, batchId)) or 0)

    if rerunMode == "plan":
        rerunPackages = retryPackages
    else:
        pendingStepIds = [int(r["BatchStepId"]) for r in spark.sql(
            "SELECT BatchStepId FROM %s WHERE BatchId = %d AND Status = 'Pending'%s" % (batchStepTable, batchId, scopeFilter.replace("s.", ""))
        ).collect()]
        rerunPackages = failedPackagesForSteps(pendingStepIds)
    # The caller goes to Failure Handling when nothing was re-opened and attempts are used up.
    retriesExhausted = bool(exhausted) and not rerunPackages

    result.update({
        "retryableStepCount": retryableStepCount, "attemptNumber": attemptNumber, "stillFailingCount": stillFailingCount,
        "pendingStepCount": pendingCount, "retriesExhausted": retriesExhausted, "retryPackages": rerunPackages,
    })
    control.logRowCount(spark, catalog, packageExecutionId, "etl.BatchStep", sourceRowCount=len(candidateSteps), targetRowCount=pendingCount, updateRowCount=pendingCount)
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=len(candidateSteps), rowsUpdated=pendingCount)
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, sourceName=PACKAGE_NAME, sourceComponent="OnError", errorDescription=str(exc))
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise

# COMMAND ----------

dbutils.jobs.taskValues.set(key="retryableStepCount", value=result["retryableStepCount"])
dbutils.jobs.taskValues.set(key="attemptNumber", value=result["attemptNumber"])
dbutils.jobs.taskValues.set(key="stillFailingCount", value=result["stillFailingCount"])
dbutils.jobs.taskValues.set(key="retriesExhausted", value="true" if result["retriesExhausted"] else "false")
dbutils.jobs.taskValues.set(key="retryPackages", value=",".join(result["retryPackages"]))
dbutils.notebook.exit(json.dumps(result))
