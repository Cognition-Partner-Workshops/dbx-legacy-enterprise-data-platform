# Databricks notebook source
# MAGIC %md
# MAGIC `batch_start` plan node -> `control.startBatch` (legacy `etl.usp_StartBatch`). A non-zero `BatchId`
# MAGIC job parameter, or a non-empty `RestartFromStep`, adopts the still-running batch of the same name
# MAGIC (`allowAdoptRunning=True`) exactly like `Invoke-EstateOrchestration.ps1` does when resuming.
# MAGIC Publishes task value `BatchId` for every downstream task.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
import datetime

batchIdParam = widgetInt("BatchId", 0)
restartFromStep = widget("RestartFromStep", "")
businessDateText = widget("BusinessDate", "")
businessDate = None
if not isUnresolved(businessDateText) and businessDateText != "1900-01-01":
    businessDate = datetime.date.fromisoformat(businessDateText)
environmentCode = widget("EnvironmentCode", "") or None

if batchIdParam > 0:
    row = spark.sql(
        f"SELECT BatchName, BatchStatus FROM {naming.controlTable(ctx['catalog'], 'batch')} WHERE BatchId = :b",
        args={"b": batchIdParam},
    ).first()
    if row is None:
        raise ValueError(f"BatchId {batchIdParam} does not exist in {naming.controlTable(ctx['catalog'], 'batch')}")
    if row.BatchStatus != "Running":
        raise ValueError(f"BatchId {batchIdParam} is {row.BatchStatus}; only a Running batch can be resumed")
    batchId = batchIdParam
else:
    batchId = control.startBatch(
        spark, ctx["catalog"], ctx["masterName"], batchType=widget("batchType", "Daily"),
        businessDate=businessDate, environmentCode=environmentCode,
        allowAdoptRunning=bool(restartFromStep and not isUnresolved(restartFromStep)),
        notes=f"Databricks job run {ctx['jobRunId']}",
    )

setTaskValue("BatchId", batchId)
variables = readVariables()
variables["User::BatchId"] = batchId
print({"batchId": batchId, "edges": evaluateEdges(variables)})
