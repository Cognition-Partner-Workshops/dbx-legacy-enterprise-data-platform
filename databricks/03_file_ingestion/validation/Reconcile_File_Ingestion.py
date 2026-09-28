# Databricks notebook source
# MAGIC %md
# MAGIC # Reconcile_File_Ingestion
# MAGIC
# MAGIC Row counts and deterministic hashes for every table `wwi_03_file_ingestion` loads, compared with the SQL Server
# MAGIC baseline (port of `validation/runtime/02_row_count_reconciliation.sql` section 1 + the file-feed hop counts).
# MAGIC
# MAGIC * Actuals: per `BatchId` (and per `SourceFileName` for the file feeds) row counts and `xxhash64` over the sorted business
# MAGIC   columns of `bronze.raw_file_partner_sales`, `bronze.raw_file_carrier_scan`, `bronze.raw_file_supplier_catalog`,
# MAGIC   `bronze.raw_file_fx_override` and `silver.err_rejected_file_row`.
# MAGIC * Baseline: a Delta table (`BaselineTable`, columns `ObjectName`, `BatchId`, `RowCount`, `RowHash`) **or** a JSON
# MAGIC   parameter (`BaselineJson`, list of the same objects) captured from SQL Server with the query printed below.
# MAGIC * Results go to `etl.row_count_log` through `control.logRowCount` (source = baseline, target = Delta) and the notebook
# MAGIC   fails when any object differs, mirroring `etl.usp_AssertRowCountReconciliation`.

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, params
from pyspark.sql import functions as F

from wwi_file_ingestion import feeds, notebook_support, reconciliation

# COMMAND ----------

notebook_support.defineWidgets(dbutils)
dbutils.widgets.text("BaselineTable", "")
dbutils.widgets.text("BaselineJson", "")
p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = int(p["batchId"] or 0)

# COMMAND ----------

actual = reconciliation.actualCounts(spark, catalog, batchId if batchId > 0 else None)
display(actual.orderBy("ObjectName", "BatchId"))

# COMMAND ----------

baseline = reconciliation.loadBaseline(spark, dbutils.widgets.get("BaselineTable"), dbutils.widgets.get("BaselineJson"))
comparison = reconciliation.compare(actual, baseline)
display(comparison.orderBy("ObjectName", "BatchId"))

# COMMAND ----------

print(reconciliation.BASELINE_SQL)

# COMMAND ----------

runBatchId, _ = notebook_support.resolveBatchId(spark, control, catalog, p, p.get("businessDate"))
packageExecutionId = control.logPackageStart(
    spark, catalog, runBatchId, "Reconcile_File_Ingestion", projectName=feeds.PROJECT_NAME, stepName="File Screen"
)
failed = reconciliation.logResults(spark, control, catalog, packageExecutionId, comparison)
control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded" if failed == 0 else "Failed")
if failed:
    raise RuntimeError("%d object/batch combinations differ from the SQL Server baseline" % failed)
dbutils.notebook.exit(json.dumps({"objectsCompared": comparison.count(), "failed": failed}))
