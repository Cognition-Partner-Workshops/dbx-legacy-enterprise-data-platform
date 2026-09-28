# Databricks notebook source
# MAGIC %md
# MAGIC `batch_end` plan node -> `control.endBatch` (legacy `etl.usp_EndBatch`). `forceStatus` carries the
# MAGIC plan's `force_status` (the failure-branch "End Batch (Failed)" nodes); otherwise the status is
# MAGIC derived from the batch's package executions and error log like the procedure does.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
batchId = widgetInt("BatchId", 0)
if batchId <= 0:
    raise ValueError("BatchId could not be resolved from the Start Batch task")
forceStatus = widget("forceStatus", "") or None
status = control.endBatch(spark, ctx["catalog"], batchId, forceStatus=forceStatus)
setTaskValue("BatchStatus", status)
variables = readVariables()
variables["User::BatchId"] = batchId
print({"batchId": batchId, "status": status, "edges": evaluateEdges(variables)})
