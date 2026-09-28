# Databricks notebook source
# MAGIC %md
# MAGIC # WWI_DataQuality reconciliation
# MAGIC For every Delta table the `DQ_*` packages write, compute the row count for the batch and a deterministic
# MAGIC hash (`sum(xxhash64(concat_ws(...)))` over the ordered business columns), compare with the SQL Server
# MAGIC baseline (ported from `validation/runtime/02_row_count_reconciliation.sql` / `05_data_quality_rules.sql`)
# MAGIC and log the outcome through `control.logRowCount`.
# MAGIC
# MAGIC Baseline input: either the Delta table named by `BaselineTable` (columns `ObjectName`, `BatchId`,
# MAGIC `RowCount`, `RowHash`) or the `BaselineJson` widget (`[{"ObjectName": ..., "RowCount": ..., "RowHash": ...}]`).

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, params, naming  # noqa: F401

from dq_quality import delta_io
from dq_quality.naming_map import controlTable, deltaTable
from dq_quality.notebook_support import PROJECT_NAME, ensureWidgets

PACKAGE_NAME = "DQ_Validation_Reconciliation"

# legacy object -> (business columns hashed; None = every non-audit column)
TARGET_OBJECTS = {
    "err.RejectedCustomer": ["CustomerBusinessKey", "RejectReasonCode", "FailedColumnName", "FailedValue"],
    "err.RejectedSupplier": ["SupplierBusinessKey", "RejectReasonCode", "FailedColumnName", "FailedValue"],
    "err.RejectedOrderLine": ["OrderLineBusinessKey", "RejectReasonCode"],
    "err.RejectedInvoiceLine": ["InvoiceLineBusinessKey", "RejectReasonCode", "VarianceAmount"],
    "err.RejectedPayment": ["PaymentNumber", "RejectReasonCode"],
    "err.RejectedFileRow": ["SourceFileName", "SourceRowNumber", "RejectReasonCode"],
    "err.RejectedLookupFailure": ["SourceObjectName", "SourceBusinessKey", "LookupName", "RejectReasonCode",
                                  "ReprocessStatusCode"],
    "err.RejectedConstraintViolation": ["TargetObjectName", "ConstraintName", "ViolatingBusinessKey", "RejectReasonCode"],
    "etl.DataQualityResult": ["ObjectName", "RuleCode", "MeasuredValue", "ResultStatus"],
    "etl.ReconciliationResult": ["ReconciliationName", "ObjectName", "SourceAmount", "TargetAmount", "VarianceAmount"],
    "stg.OrderLine": ["OrderLineBusinessKey", "OrderedQuantity", "NetLineAmount", "DqStatusCode"],
}

ensureWidgets(dbutils)
dbutils.widgets.text("BaselineTable", "")
dbutils.widgets.text("BaselineJson", "[]")
p = params.getJobParams(dbutils)
catalog, batchId = p["catalog"], p["batchId"]

# COMMAND ----------

baselineTable = dbutils.widgets.get("BaselineTable").strip()
if baselineTable:
    baseline = {r["ObjectName"]: r.asDict() for r in
                spark.table(baselineTable).filter(F.col("BatchId") == batchId).collect()}
else:
    baseline = {b["ObjectName"]: b for b in json.loads(dbutils.widgets.get("BaselineJson") or "[]")}


def hashAndCount(tableName: str, columns):
    df = spark.table(tableName).filter(F.col("BatchId") == batchId)
    cols = [c for c in columns if c in df.columns]
    hashed = df.select(F.count(F.lit(1)).alias("RowCount"),
                       F.sum(F.xxhash64(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols])))
                       .alias("RowHash"))
    row = hashed.collect()[0]
    return int(row["RowCount"]), int(row["RowHash"] or 0), cols


with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME,
                        stepName="Validation") as run:
    packageExecutionId = run.packageExecutionId
    results = []
    for legacyName, columns in TARGET_OBJECTS.items():
        tableName = deltaTable(catalog, legacyName)
        if not delta_io.tableExists(spark, tableName):
            results.append((legacyName, tableName, None, None, "MISSING_TABLE"))
            continue
        rowCount, rowHash, cols = hashAndCount(tableName, columns)
        expected = baseline.get(legacyName)
        if expected is None:
            status = "NO_BASELINE"
        elif int(expected.get("RowCount") or 0) != rowCount:
            status = "COUNT_MISMATCH"
        elif expected.get("RowHash") is not None and int(expected["RowHash"]) != rowHash:
            status = "HASH_MISMATCH"
        else:
            status = "MATCH"
        control.logRowCount(spark, catalog, packageExecutionId, legacyName,
                            sourceRowCount=int(expected["RowCount"]) if expected else None,
                            targetRowCount=rowCount)
        results.append((legacyName, tableName, rowCount, rowHash, status))

    resultDf = spark.createDataFrame(results, ["ObjectName", "DeltaTable", "RowCount", "RowHash", "Status"])
    display(resultDf)  # noqa: F821 - Databricks display

    mismatches = [r for r in results if r[4] in ("COUNT_MISMATCH", "HASH_MISMATCH")]
    run.rowsRead = len(results)
    run.rowsRejected = len(mismatches)
    if mismatches:
        control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                         errorSeverity="Warning", errorCode=50000, sourceName=PACKAGE_NAME,
                         errorDescription="reconciliation mismatches: %s" % ", ".join(m[0] for m in mismatches))

dbutils.notebook.exit(json.dumps({"batchId": batchId, "objects": len(results), "mismatches": len(mismatches)}))
