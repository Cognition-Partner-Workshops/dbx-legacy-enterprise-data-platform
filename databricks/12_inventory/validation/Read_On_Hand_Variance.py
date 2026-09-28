# Databricks notebook source
# MAGIC %md
# MAGIC # Read On Hand Variance (Master_Intraday_Inventory control node)
# MAGIC Port of the orchestration-plan query
# MAGIC `SELECT ISNULL(MAX(ABS(ra.SourceRowCount - ra.TargetRowCount)), 0) ... WHERE pe.BatchId = ? AND ra.ObjectName = N'Fact.Stock Holding'`
# MAGIC and of the two conditional edges that follow it. Publishes `replenishmentAllowed`
# MAGIC as a task value for the `Replenishment_Gate` condition task.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, params, naming  # noqa: E402,F401
from inv_common import contracts, runtime  # noqa: E402

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = int(p["batchId"] or 0)
pickingWindowOpen = runtime.asBool(runtime.widget(dbutils, "PickingWindowOpen", "True"))
onHandVarianceTolerance = int(runtime.widget(dbutils, "OnHandVarianceTolerance", "25"))

# COMMAND ----------

rowCountLog = contracts.table(catalog, "etl.RowCountAudit")
packageExecution = contracts.table(catalog, "etl.PackageExecution")
row = spark.sql(
    f"""
    SELECT COALESCE(MAX(ABS(ra.SourceRowCount - ra.TargetRowCount)), 0) AS OnHandVariance
    FROM {rowCountLog} AS ra
    INNER JOIN {packageExecution} AS pe ON pe.PackageExecutionId = ra.PackageExecutionId
    WHERE pe.BatchId = {batchId} AND ra.ObjectName = 'Fact.Stock Holding'
    """
).first()
onHandVariance = int(row["OnHandVariance"] or 0)
replenishmentAllowed = pickingWindowOpen and onHandVariance <= onHandVarianceTolerance
escalate = onHandVariance > onHandVarianceTolerance
print(f"OnHandVariance={onHandVariance} tolerance={onHandVarianceTolerance} pickingWindowOpen={pickingWindowOpen} "
      f"replenishmentAllowed={replenishmentAllowed} escalate={escalate}")

dbutils.jobs.taskValues.set(key="onHandVariance", value=onHandVariance)
dbutils.jobs.taskValues.set(key="replenishmentAllowed", value="true" if replenishmentAllowed else "false")
dbutils.jobs.taskValues.set(key="escalateOnHandVariance", value="true" if escalate else "false")

if escalate:
    # "On Hand Variance Escalation" phase (ERR_Reconcile_RowCounts / ERR_Notify_Operations) belongs to
    # session 15; here we only raise the failed-object count into the control log for it to pick up.
    control.logError(
        spark, catalog, batchId=batchId, errorSeverity="Warning", errorCode="ONHAND_VARIANCE",
        sourceName="Master_Intraday_Inventory", sourceComponent="Read On Hand Variance",
        errorDescription=f"On-hand variance {onHandVariance} exceeds tolerance {onHandVarianceTolerance}; replenishment stood down.",
    )
