# Databricks notebook source
# MAGIC %md
# MAGIC # ERR_Notify_Operations
# MAGIC Operator notification (legacy `ssis/15_error_handling/ERR_Notify_Operations.dtsx`).
# MAGIC
# MAGIC Assesses the batch outcome from `etl.batch_step` (severity `CRITICAL` when a high-criticality package's step
# MAGIC failed, `WARNING` when anything failed, else `INFO`), raises one deduplicated `BATCH_FAILURE` notification per
# MAGIC batch with the legacy subject/body (`[ENV] Batch N (Type) failed` / `Failed steps: ... Last error: ...`), or a
# MAGIC `BATCH_WARNING` for unresolved rejects when `NotifyOnWarnings` is true, then posts every notification raised in
# MAGIC this run to the operations webhook whose URL is read from the secret scope
# MAGIC (`dbutils.secrets.get(webhookSecretScope, webhookSecretKey)`). Job-level `email_notifications` /
# MAGIC `webhook_notifications` live in `resources/wwi_15_error_handling.job.yml`.
# MAGIC
# MAGIC Package parameters: `NotifyOnWarnings` (default `False`), `webhookSecretScope` (default `wwi`),
# MAGIC `webhookSecretKey` (default `ops-webhook-url`), `PostToWebhook` (default `True`).

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

import requests  # noqa: E402

from dbx_etl_common import control, naming, params  # noqa: E402
from err_handling import notify, tables, widgets  # noqa: E402

PACKAGE_NAME = "ERR_Notify_Operations"
PROJECT_NAME = "WWI_ErrorHandling"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = int(p["batchId"])
environmentCode = p["environmentCode"]
notifyOnWarnings = widgets.getBool(dbutils, "NotifyOnWarnings", False)
webhookSecretScope = widgets.getText(dbutils, "webhookSecretScope", "wwi")
webhookSecretKey = widgets.getText(dbutils, "webhookSecretKey", "ops-webhook-url")
postToWebhook = widgets.getBool(dbutils, "PostToWebhook", True)

batchTable = naming.table(catalog, "etl", "batch")
batchStepTable = naming.table(catalog, "etl", "batch_step")
packageExecutionTable = naming.table(catalog, "etl", "package_execution")
errorLogTable = naming.table(catalog, "etl", "error_log")
rejectedRecordTable = naming.table(catalog, "etl", "rejected_record")
notificationTable = naming.table(catalog, "etl", "operator_notification")

# COMMAND ----------


def assessBatchOutcome():
    """Legacy Assess Batch Outcome. Criticality comes from docs/inventories/ssis-packages.csv (package level)."""
    criticality = notify.loadPackageCriticality()
    failedSteps = spark.sql(
        "SELECT s.BatchStepId, s.StepName FROM %s s WHERE s.BatchId = %d AND s.Status = 'Failed' ORDER BY s.StepSequence, s.BatchStepId"
        % (batchStepTable, batchId)
    ).collect()
    failedPackages = spark.sql(
        "SELECT DISTINCT PackageName FROM %s WHERE BatchId = %d AND Status = 'Failed'" % (packageExecutionTable, batchId)
    ).collect()
    highCriticalityFailures = sum(1 for r in failedPackages if criticality.get(r["PackageName"], "").lower() == "high")
    severity = notify.deriveSeverity(len(failedSteps), highCriticalityFailures)
    return [r["StepName"] for r in failedSteps], severity


def raiseBatchFailure(failedStepNames, severity):
    batch = spark.sql("SELECT BatchId, BatchType FROM %s WHERE BatchId = %d" % (batchTable, batchId)).first()
    if batch is None:
        return 0
    existing = tables.scalar(spark, "SELECT COUNT(*) FROM %s WHERE BatchId = %d AND NotificationTypeCode = 'BATCH_FAILURE'" % (notificationTable, batchId))
    if int(existing or 0) > 0:
        return 0
    lastError = tables.scalar(spark, "SELECT ErrorDescription FROM %s WHERE BatchId = %d ORDER BY LoggedAtUtc DESC LIMIT 1" % (errorLogTable, batchId))
    subject = notify.batchFailureSubject(environmentCode, batchId, batch["BatchType"])
    body = notify.batchFailureBody(failedStepNames, lastError)
    spark.sql(
        "INSERT INTO %s (BatchId, NotificationTypeCode, Severity, Subject, Body, ObjectName, RaisedAtUtc, IsAcknowledged) "
        "VALUES (%d, 'BATCH_FAILURE', %s, %s, %s, NULL, current_timestamp(), false)"
        % (notificationTable, batchId, tables.sqlLiteral(severity), tables.sqlLiteral(subject), tables.sqlLiteral(body))
    )
    return 1


def raiseBatchWarning():
    rejectedRows = int(tables.scalar(spark, "SELECT COUNT(*) FROM %s WHERE BatchId = %d AND IsReprocessed = false" % (rejectedRecordTable, batchId)) or 0)
    if rejectedRows == 0:
        return 0
    spark.sql(
        "INSERT INTO %s (BatchId, NotificationTypeCode, Severity, Subject, Body, ObjectName, RaisedAtUtc, IsAcknowledged) "
        "VALUES (%d, 'BATCH_WARNING', 'WARNING', %s, %s, NULL, current_timestamp(), false)"
        % (notificationTable, batchId, tables.sqlLiteral(notify.BATCH_WARNING_SUBJECT), tables.sqlLiteral(notify.batchWarningBody(rejectedRows)))
    )
    return 1


def postWebhook(notifications):
    """Post each notification raised in this run to the operations webhook (URL from the secret scope)."""
    if not postToWebhook or not notifications:
        return 0
    webhookUrl = dbutils.secrets.get(scope=webhookSecretScope, key=webhookSecretKey)
    posted = 0
    for row in notifications:
        response = requests.post(webhookUrl, json=notify.webhookPayload(row.asDict(), environmentCode), timeout=30)
        if response.status_code >= 300:
            control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, errorSeverity="Warning",
                             sourceName="ops-webhook", sourceComponent="Post Webhook",
                             errorDescription="webhook returned HTTP %d for notification %s" % (response.status_code, row["OperatorNotificationId"]))
        else:
            posted += 1
    return posted


# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName="Failure Handling")
result = {"package": PACKAGE_NAME, "batchId": batchId, "environmentCode": environmentCode}
try:
    startedAt = spark.sql("SELECT current_timestamp() AS ts").first()["ts"]
    failedStepNames, severity = assessBatchOutcome()
    raised = 0
    if failedStepNames:
        raised += raiseBatchFailure(failedStepNames, severity)
    elif notifyOnWarnings:
        raised += raiseBatchWarning()
    newNotifications = spark.sql(
        "SELECT * FROM %s WHERE BatchId = %d AND RaisedAtUtc >= TIMESTAMP'%s' AND IsAcknowledged = false ORDER BY OperatorNotificationId"
        % (notificationTable, batchId, startedAt.strftime("%Y-%m-%d %H:%M:%S.%f"))
    ).collect()
    posted = postWebhook(newNotifications)
    notificationCount = int(tables.scalar(spark, "SELECT COUNT(*) FROM %s WHERE BatchId = %d AND IsAcknowledged = false" % (notificationTable, batchId)) or 0)
    result.update({"severity": severity, "failedStepCount": len(failedStepNames), "raisedThisRun": raised, "postedToWebhook": posted,
                   "unacknowledgedNotifications": notificationCount})
    control.logRowCount(spark, catalog, packageExecutionId, "etl.OperatorNotification", sourceRowCount=len(failedStepNames), targetRowCount=notificationCount, insertRowCount=raised)
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=len(failedStepNames), rowsInserted=raised)
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, sourceName=PACKAGE_NAME, sourceComponent="OnError", errorDescription=str(exc))
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise

# COMMAND ----------

dbutils.jobs.taskValues.set(key="severity", value=result["severity"])
dbutils.jobs.taskValues.set(key="notificationCount", value=result["unacknowledgedNotifications"])
dbutils.notebook.exit(json.dumps(result))
