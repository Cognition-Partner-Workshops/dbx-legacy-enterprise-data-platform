# Databricks notebook source
# MAGIC %md
# MAGIC `reconcile` plan node -> `control.assertRowCountReconciliation` (legacy
# MAGIC `etl.usp_AssertRowCountReconciliation`); `raiseOnFailure` carries the plan's `raise_on_failure`.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
batchId = widgetInt("BatchId", 0)
if batchId <= 0:
    raise ValueError("BatchId could not be resolved from the Start Batch task")
raiseOnFailure = widget("raiseOnFailure", "true") == "true"
failedObjectCount = control.assertRowCountReconciliation(spark, ctx["catalog"], batchId, raiseOnFailure=raiseOnFailure)
setTaskValue("FailedObjectCount", failedObjectCount)
variables = readVariables()
variables["User::BatchId"] = batchId
print({"batchId": batchId, "failedObjectCount": failedObjectCount, "edges": evaluateEdges(variables)})
