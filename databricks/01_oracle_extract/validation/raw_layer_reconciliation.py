# Databricks notebook source
# MAGIC %md
# MAGIC # Raw layer reconciliation - WWI_Extract_Oracle (session 01)
# MAGIC
# MAGIC Port of `validation/runtime/02_row_count_reconciliation.sql` for the 15 bronze `raw_oracle_*` tables
# MAGIC loaded by the 22 `EXT_ORA_*` packages:
# MAGIC
# MAGIC 1. per table: row count (whole table and for the current `BatchId`) and a deterministic hash
# MAGIC    (`sum(xxhash64(concat_ws('|', <business columns>)))`, metadata columns excluded);
# MAGIC 2. comparison with the SQL Server baseline supplied either as a Delta table (`baseline_table`, columns
# MAGIC    `ObjectName`, `RowCount`, `HashValue`, legacy names such as `raw.OracleCustomerMaster`) or as JSON
# MAGIC    (`baseline_json`, `[{"ObjectName": ..., "RowCount": ..., "HashValue": ...}]`);
# MAGIC 3. results written to `etl.row_count_log` through `control.logRowCount` (one row per raw table, SourceRowCount =
# MAGIC    baseline, TargetRowCount = bronze) under a package execution named `VAL_01_RawLayerReconciliation`;
# MAGIC 4. queries 1, 2, 3 and 5 of the legacy script executed against the `etl.*` Delta tables and displayed.
# MAGIC
# MAGIC Job parameters `BatchId`, `catalog` (and the other standard ones) are read via `dbx_etl_common.params`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, params

from oracle_extract import reconciliation
from oracle_extract.runner import RunSettings, ensureBatch
from oracle_extract.specs import PROJECT_NAME

dbutils.widgets.text("baseline_table", "", "baseline_table")
dbutils.widgets.text("baseline_json", "", "baseline_json")
dbutils.widgets.text("log_results", "true", "log_results")

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
settings = RunSettings(catalog=catalog, batchId=int(p["batchId"]), reloadFullHistory=bool(p["reloadFullHistory"]),
                       environmentCode=p["environmentCode"], businessDate=p.get("businessDate"))
batchId = ensureBatch(spark, settings)
logResults = dbutils.widgets.get("log_results").strip().lower() in ("true", "1", "yes")

baseline = reconciliation.loadBaseline(spark, dbutils.widgets.get("baseline_table"), dbutils.widgets.get("baseline_json"))
packageExecutionId = control.logPackageStart(spark, catalog, batchId, "VAL_01_RawLayerReconciliation",
                                             projectName=PROJECT_NAME, stepName="Extract Oracle")
try:
    results = reconciliation.reconcileRawLayer(spark, catalog, None, baseline, packageExecutionId, logResults)
    batchResults = reconciliation.reconcileRawLayer(spark, catalog, batchId, {}, None, False)
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded",
                          rowsRead=sum(r["TargetRowCount"] for r in results))
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                     sourceName="VAL_01_RawLayerReconciliation", errorDescription=str(exc)[:4000])
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise

# COMMAND ----------

display(reconciliation.resultsFrame(spark, results))
display(reconciliation.resultsFrame(spark, batchResults).withColumnRenamed("TargetRowCount", "BatchRowCount"))

# COMMAND ----------

for name, sql in reconciliation.hopVarianceSql(catalog).items():
    print(f"-- {name}")
    try:
        display(spark.sql(sql))
    except Exception as exc:  # etl.* tables are provisioned by session 00; report instead of failing the job
        print(f"skipped {name}: {exc}")

# COMMAND ----------

mismatches = [r["ObjectName"] for r in results if r["Status"] == "Mismatch"]
if mismatches:
    raise AssertionError(f"raw layer reconciliation failed for: {', '.join(mismatches)}")
dbutils.notebook.exit(f"reconciled {len(results)} raw tables, {sum(1 for r in results if r['Status'] == 'Match')} matched, "
                      f"{sum(1 for r in results if r['Status'] == 'NoBaseline')} without baseline")
