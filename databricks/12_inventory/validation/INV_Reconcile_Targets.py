# Databricks notebook source
# MAGIC %md
# MAGIC # INV reconciliation: row counts + deterministic hash vs SQL Server baseline
# MAGIC For every target table written by the wwi_12_inventory job, computes the row count
# MAGIC (per BusinessDate / BatchId where the table carries one) and an order-independent
# MAGIC `xxhash64` fingerprint (sum of per-row hashes over the sorted, concatenated columns),
# MAGIC compares them with the SQL Server baseline and writes the outcome to
# MAGIC `etl.row_count_log` through `control.logRowCount`.
# MAGIC
# MAGIC Baseline input (either):
# MAGIC * `BaselineTable` – a Delta table `<catalog>.etl.inv_baseline` with columns
# MAGIC   `ObjectName STRING, BusinessDate DATE, BatchId BIGINT, RowCount BIGINT, RowHashSum BIGINT`
# MAGIC   captured by running `validation/runtime/02_row_count_reconciliation.sql` (section 1) plus the
# MAGIC   hash queries in the mapping doc against SQL Server; or
# MAGIC * `BaselineJson` – the same rows as a JSON array string passed as a task parameter.
# MAGIC Without a baseline the notebook still logs the Databricks-side counts/hashes (target side only).

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import LongType, StringType, StructField, StructType, DateType  # noqa: E402

from dbx_etl_common import control, params, naming  # noqa: E402,F401
from inv_common import contracts, runtime, transforms  # noqa: E402

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = int(p["batchId"] or 0)
businessDate = p["businessDate"]
baselineTable = runtime.widget(dbutils, "BaselineTable", "")
baselineJson = runtime.widget(dbutils, "BaselineJson", "")

packageExecutionId = control.logPackageStart(
    spark, catalog, batchId, "INV_Reconcile_Targets", projectName=contracts.PROJECT_NAME, stepName="Validate"
)

# COMMAND ----------

# (legacy object, date column, batch column) - None when the table has no such column
TARGETS = [
    ("Fact.Daily Inventory Snapshot", "SnapshotDateKey", "BatchId"),
    ("Fact.Movement", None, "BatchId"),
    ("Aggregate.Daily Inventory Health", "SnapshotDate", "RefreshBatchId"),
    ("work.ReplenishmentSuggestion", None, "BatchId"),
    ("work.CycleCountVariance", None, "BatchId"),
    ("work.StockTransferMovement", None, "BatchId"),
    ("err.InventorySnapshotReject", "SnapshotDate", "BatchId"),
    ("etl.ReconciliationResult", None, "BatchId"),
]

baselineSchema = StructType([
    StructField("ObjectName", StringType()), StructField("BusinessDate", DateType()),
    StructField("BatchId", LongType()), StructField("RowCount", LongType()), StructField("RowHashSum", LongType()),
])
if baselineTable:
    baseline = spark.table(baselineTable)
elif baselineJson:
    rows = json.loads(baselineJson)
    baseline = spark.createDataFrame(
        [(r["ObjectName"], r.get("BusinessDate"), r.get("BatchId"), r.get("RowCount"), r.get("RowHashSum")) for r in rows],
        schema=StructType([
            StructField("ObjectName", StringType()), StructField("BusinessDate", StringType()),
            StructField("BatchId", LongType()), StructField("RowCount", LongType()), StructField("RowHashSum", LongType()),
        ]),
    ).withColumn("BusinessDate", F.to_date("BusinessDate"))
else:
    baseline = spark.createDataFrame([], baselineSchema)
baselineRows = {r["ObjectName"]: r for r in baseline.collect()}

# COMMAND ----------

results = []
for legacyName, dateCol, batchCol in TARGETS:
    fullName = contracts.table(catalog, legacyName)
    if not spark.catalog.tableExists(fullName):
        print(f"skip {legacyName}: {fullName} does not exist")
        continue
    df = spark.table(fullName)
    if batchCol and batchId:
        df = df.where(F.col(batchCol) == F.lit(batchId))
    elif dateCol and businessDate:
        df = df.where(F.col(dateCol) == F.lit(businessDate).cast("date"))
    fingerprint = transforms.deterministicHash(df).first()
    targetCount = int(fingerprint["RowCount"] or 0)
    targetHash = int(fingerprint["RowHashSum"] or 0)
    base = baselineRows.get(legacyName)
    sourceCount = int(base["RowCount"]) if base and base["RowCount"] is not None else None
    sourceHash = int(base["RowHashSum"]) if base and base["RowHashSum"] is not None else None
    hashMatch = None if sourceHash is None else (sourceHash == targetHash)
    control.logRowCount(
        spark, catalog, packageExecutionId, legacyName,
        sourceRowCount=sourceCount, targetRowCount=targetCount,
        rejectRowCount=0 if hashMatch in (None, True) else 1,   # hash mismatch surfaces as 1 "rejected" object
    )
    results.append((legacyName, contracts.deltaName(legacyName), sourceCount, targetCount, sourceHash, targetHash, hashMatch))

display(spark.createDataFrame(
    results, "ObjectName string, DeltaTable string, BaselineRowCount long, TargetRowCount long, "
             "BaselineRowHashSum long, TargetRowHashSum long, HashMatch boolean",
))

# COMMAND ----------

failed = control.assertRowCountTolerance(spark, catalog, batchId, scope="ALL", absoluteTolerance=0, raiseOnFailure=False) if batchId else 0
control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded" if not failed else "SucceededWithWarnings",
                      rowsRead=len(results), rowsRejected=int(failed or 0))
print(f"objects checked={len(results)} outsideTolerance={failed}")
