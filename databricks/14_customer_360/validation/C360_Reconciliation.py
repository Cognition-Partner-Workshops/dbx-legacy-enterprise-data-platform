# Databricks notebook source
# MAGIC %md
# MAGIC # Customer 360 reconciliation (session 14)
# MAGIC For every mart table loaded by the `wwi_14_customer_360` job this notebook computes
# MAGIC * row counts per `BatchId` / `BusinessDate`,
# MAGIC * a deterministic content hash (`sum(xxhash64(*))` over the business columns, sorted by key),
# MAGIC
# MAGIC and compares them with the SQL Server baseline captured from the legacy `Customer360.*`
# MAGIC tables. Baseline figures come from either a Delta table (`BaselineTable`) or a JSON
# MAGIC parameter (`BaselineJson`) with rows
# MAGIC `{"objectName": "Customer360.CustomerProfile", "rowCount": 123, "hashValue": "..."}`.
# MAGIC Results are written to `etl.row_count_log` through `control.logRowCount` (source = baseline,
# MAGIC target = Delta) so `validation/runtime/02_row_count_reconciliation.sql` ports 1:1.
# MAGIC
# MAGIC Ported baseline queries (SQL Server -> Spark SQL) are in the last cells; run them against
# MAGIC SQL Server to capture the baseline and here to compare.

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath("../src"))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control  # noqa: E402

from c360_lib import runtime as R  # noqa: E402
from c360_lib import tables as T  # noqa: E402

PACKAGE_NAME = "C360_Reconciliation"

# COMMAND ----------

ctx = R.getJobContext(dbutils)
catalog = ctx.catalog
baselineTable = R.widget(dbutils, "BaselineTable", "")
baselineJson = R.widget(dbutils, "BaselineJson", "[]")
failOnMismatch = R.asBool(R.widget(dbutils, "FailOnMismatch", "False"))

# legacy object -> (key columns, columns excluded from the hash because they are run-specific)
RECONCILED_OBJECTS = {
    "Customer360.CustomerProfile": (["CustomerKey"], {"BatchId", "BusinessDate"}),
    "Customer360.CustomerRollingMetric": (["CustomerKey"], {"BatchId", "BusinessDate"}),
    "Customer360.LoyaltyOverlay": (["CustomerId"], {"BatchId", "BusinessDate"}),
    "Customer360.CustomerChurnFlag": (["CustomerKey"], {"BatchId", "BusinessDate"}),
    "Customer360.CustomerSegment": (["CustomerId"], {"BatchId", "BusinessDate", "AssignedAtUtc"}),
}

# COMMAND ----------


def loadBaseline():
    if baselineTable:
        return {r["objectName"]: r.asDict() for r in spark.table(baselineTable).collect()}
    return {r["objectName"]: r for r in json.loads(baselineJson or "[]")}


def deterministicHash(df, keyColumns, excluded):
    """sum(xxhash64(concat_ws('|', sorted business columns))) — order independent, deterministic."""
    cols = sorted(c for c in df.columns if c not in excluded)
    hashed = df.select(F.xxhash64(F.concat_ws("|", *[F.col(c).cast("string") for c in cols])).alias("h"))
    row = hashed.agg(F.sum("h").alias("s"), F.count("*").alias("n")).collect()[0]
    return str(row["s"]), int(row["n"])


def countsByBatch(df):
    grp = [c for c in ("BatchId", "BusinessDate") if c in df.columns]
    if not grp:
        return [{"BatchId": None, "BusinessDate": None, "rowCount": df.count()}]
    return [r.asDict() for r in df.groupBy(*grp).count().withColumnRenamed("count", "rowCount").collect()]


# COMMAND ----------

baseline = loadBaseline()
results = []
with R.packageLifecycle(spark, ctx, PACKAGE_NAME, "Reconcile Row Counts") as run:
    for legacyName, (keyColumns, excluded) in RECONCILED_OBJECTS.items():
        fullName = T.table(catalog, legacyName)
        df = R.readTable(spark, fullName, required=False)
        if df is None:
            results.append({"objectName": legacyName, "status": "MISSING_TARGET"})
            continue
        hashValue, targetRows = deterministicHash(df, keyColumns, excluded)
        base = baseline.get(legacyName, {})
        baseRows = base.get("rowCount")
        baseHash = base.get("hashValue")
        status = "NO_BASELINE" if not base else (
            "MATCH" if (baseRows == targetRows and (baseHash is None or str(baseHash) == hashValue)) else "MISMATCH")
        results.append({
            "objectName": legacyName, "deltaTable": fullName, "targetRows": targetRows, "baselineRows": baseRows,
            "targetHash": hashValue, "baselineHash": baseHash, "status": status, "byBatch": countsByBatch(df),
        })
        control.logRowCount(
            spark, catalog, run.packageExecutionId, legacyName,
            sourceRowCount=baseRows, targetRowCount=targetRows,
            rejectRowCount=0 if status in ("MATCH", "NO_BASELINE") else abs((baseRows or 0) - targetRows),
        )
        run.rowsRead += baseRows or 0
        run.rowsInserted += targetRows

    # Row-count audit consistency for this batch (port of 02_row_count_reconciliation.sql §1):
    # the shared implementation of usp_AssertRowCountReconciliation.
    failedObjects = control.assertRowCountReconciliation(spark, catalog, ctx.batchId, raiseOnFailure=False)
    results.append({"objectName": "etl.row_count_log", "status": "OK" if failedObjects == 0 else "VARIANCE", "failedObjects": failedObjects})

display(spark.createDataFrame([json.dumps(r) for r in results], "string").withColumnRenamed("value", "result"))
mismatches = [r for r in results if r.get("status") in ("MISMATCH", "MISSING_TARGET")]
if failOnMismatch and mismatches:
    raise RuntimeError(f"Reconciliation mismatches: {[m['objectName'] for m in mismatches]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline capture queries (run on SQL Server, paste into `BaselineJson`)
# MAGIC ```sql
# MAGIC -- one row per mart table; hash uses the same column set as the Delta side (business columns,
# MAGIC -- sorted by name, pipe-separated, cast to string). Keep the JSON shape below.
# MAGIC SELECT N'Customer360.CustomerProfile' AS objectName, COUNT(*) AS rowCount FROM Customer360.CustomerProfile
# MAGIC UNION ALL SELECT N'Customer360.CustomerRollingMetric', COUNT(*) FROM Customer360.CustomerRollingMetric
# MAGIC UNION ALL SELECT N'Customer360.LoyaltyOverlay',        COUNT(*) FROM Customer360.LoyaltyOverlay
# MAGIC UNION ALL SELECT N'Customer360.CustomerChurnFlag',     COUNT(*) FROM Customer360.CustomerChurnFlag
# MAGIC UNION ALL SELECT N'Customer360.CustomerSegment',       COUNT(*) FROM Customer360.CustomerSegment;
# MAGIC ```
# MAGIC `xxhash64` has no T-SQL equivalent, so the cross-platform comparison is row-count based unless
# MAGIC the baseline hash is produced by reading the SQL Server tables into Spark (JDBC) and running
# MAGIC `deterministicHash` above on both sides.
# MAGIC
# MAGIC ## Ported runtime checks (Spark SQL over the etl.* Delta tables)
# MAGIC ```sql
# MAGIC -- 02_row_count_reconciliation.sql §1: variance between source and target counts for this batch
# MAGIC SELECT r.ObjectName, r.SourceRowCount, r.TargetRowCount, r.RejectRowCount,
# MAGIC        r.SourceRowCount - r.TargetRowCount - COALESCE(r.RejectRowCount, 0) AS Variance
# MAGIC FROM ${catalog}.etl.row_count_log AS r
# MAGIC JOIN ${catalog}.etl.package_execution AS p ON p.PackageExecutionId = r.PackageExecutionId
# MAGIC WHERE p.BatchId = ${BatchId} AND r.ObjectName LIKE 'Customer360.%'
# MAGIC   AND r.SourceRowCount - r.TargetRowCount - COALESCE(r.RejectRowCount, 0) <> 0;
# MAGIC
# MAGIC -- 02 §3: C360 packages that succeeded without logging a row count
# MAGIC SELECT p.PackageName, p.PackageExecutionId
# MAGIC FROM ${catalog}.etl.package_execution AS p
# MAGIC LEFT JOIN ${catalog}.etl.row_count_log AS r ON r.PackageExecutionId = p.PackageExecutionId
# MAGIC WHERE p.BatchId = ${BatchId} AND p.PackageName LIKE 'C360_%' AND p.Status = 'Succeeded' AND r.RowCountLogId IS NULL;
# MAGIC ```
