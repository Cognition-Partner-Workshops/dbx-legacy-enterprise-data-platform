# Databricks notebook source
# MAGIC %md
# MAGIC Helper task (not a plan node): the `finally` block of `Invoke-EstateOrchestration.ps1`. Runs after
# MAGIC every sink task with `run_if: ALL_DONE`; if the batch is still `Running` because the graph failed
# MAGIC before reaching an `End Batch` node it is closed with `control.endBatch(forceStatus="Failed")`.

# COMMAND ----------
# MAGIC %run ./_bootstrap

# COMMAND ----------
batchId = widgetInt("BatchId", 0)
if batchId <= 0:
    print("no batch was started by this run; nothing to close")
    dbutils.notebook.exit("NO_BATCH")  # noqa: F821
row = spark.sql(
    f"SELECT BatchStatus FROM {naming.controlTable(ctx['catalog'], 'batch')} WHERE BatchId = :b", args={"b": batchId}
).first()
if row is not None and row.BatchStatus == "Running":
    control.logError(spark, ctx["catalog"], batchId=batchId, errorSeverity="Error", sourceName=ctx["masterName"],
                     sourceComponent="close_batch_if_open", procedureName="close_batch_if_open",
                     errorDescription=f"batch left Running by job run {ctx['jobRunId']}; closing as Failed")
    print({"batchId": batchId, "closedAs": control.endBatch(spark, ctx["catalog"], batchId, forceStatus="Failed")})
else:
    print({"batchId": batchId, "status": None if row is None else row.BatchStatus, "action": "none"})
