# Databricks notebook source
# MAGIC %md
# MAGIC # PRC_Reconcile_Procurement
# MAGIC Reconciliation for every table loaded by the 13_procurement job.
# MAGIC
# MAGIC For each target: row count for the current BatchId (and the BusinessDate slice where the table has
# MAGIC one), total row count, and an order-independent `xxhash64` digest (port of the row-count
# MAGIC reconciliation idea in `validation/runtime/02_row_count_reconciliation.sql`; the SQL Server
# MAGIC side is captured separately and supplied as `BaselineJson` or a `BaselineTable`). Every result is
# MAGIC written to `etl.row_count_log` through `control.logRowCount` so
# MAGIC `control.assertRowCountReconciliation` / `assertRowCountTolerance` can gate the batch.

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, naming, params  # noqa: E402

from procurement_lib import PROJECT_NAME  # noqa: E402
from procurement_lib import reconciliation as rc  # noqa: E402
from procurement_lib.common import parseBool  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "PRC_Reconcile_Procurement"

for name, default in (("BatchId", "0"), ("BusinessDate", ""), ("ReloadFullHistory", "False"), ("EnvironmentCode", "DEV"),
                      ("RestartFromStep", ""), ("catalog", ""), ("BaselineJson", ""), ("BaselineTable", ""),
                      ("FailOnMismatch", "False")):
    dbutils.widgets.text(name, default)

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
failOnMismatch = parseBool(dbutils.widgets.get("FailOnMismatch"))

# (schema, table, legacy object, batch column, business-date column or None)
TARGETS = [
    ("gold", "fact_purchase", "Fact.Purchase", "batch_id", "date_key"),
    ("gold", "fact_purchase_receipt", "Fact.Purchase Receipt", "batch_id", "receipt_date_key"),
    ("gold", "agg_supplier_performance", "Aggregate.Supplier Performance", "refresh_batch_id", None),
    ("silver", "work_purchase_spend_line", "work.PurchaseSpendLine", None, None),
    ("silver", "work_receipt_match", "work.ReceiptMatch", None, None),
    ("silver", "work_supplier_scorecard", "work.SupplierScorecard", None, None),
    ("silver", "work_contract_compliance", "work.ContractCompliance", None, None),
    ("silver", "work_supplier_statement_archive", "work.SupplierStatementArchive", "BatchId", None),
]

# COMMAND ----------


def loadBaseline():
    """Baseline figures keyed by '<schema>.<table>' from the JSON parameter and/or a Delta table."""
    baseline = {}
    rawJson = dbutils.widgets.get("BaselineJson").strip()
    if rawJson:
        doc = json.loads(rawJson)
        baseline.update(doc.get("tables", doc))
    baselineTable = dbutils.widgets.get("BaselineTable").strip()
    if baselineTable and spark.catalog.tableExists(baselineTable):
        for row in spark.table(baselineTable).collect():
            d = row.asDict()
            key = d.get("table_name") or d.get("TableName")
            baseline[key] = {"row_count": d.get("row_count", d.get("RowCount")), "digest": d.get("digest", d.get("Digest"))}
    return baseline


baseline = loadBaseline()

# COMMAND ----------

results = []
with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName="Procurement Mart") as run:
    for schema, table, legacyObject, batchColumn, dateColumn in TARGETS:
        fullName = naming.table(catalog, schema, table)
        key = "%s.%s" % (schema, table)
        if not spark.catalog.tableExists(fullName):
            results.append({"table": key, "legacyObject": legacyObject, "status": "Missing"})
            control.logRowCount(spark, catalog, run.packageExecutionId, legacyObject, sourceRowCount=None, targetRowCount=0)
            continue
        df = spark.table(fullName)
        totalCount, digest = rc.fingerprint(df)
        batchCount = df.where(F.col(batchColumn) == F.lit(int(batchId))).count() if batchColumn else None
        dateCount = df.where(F.col(dateColumn) == F.lit(businessDate)).count() if dateColumn else None
        comparison = rc.compareWithBaseline(totalCount, digest, baseline.get(key))
        results.append({"table": key, "legacyObject": legacyObject, "rowCount": totalCount, "batchRowCount": batchCount,
                        "businessDateRowCount": dateCount, "digest": digest, **comparison})
        control.logRowCount(
            spark, catalog, run.packageExecutionId, legacyObject,
            sourceRowCount=comparison["baselineRowCount"], targetRowCount=totalCount,
            insertRowCount=batchCount,
        )
    mismatches = [r for r in results if r.get("status") in ("CountMismatch", "HashMismatch", "Missing")]
    run.rowsRead = len(results)
    run.rowsInserted = len(results)
    run.rowsRejected = len(mismatches)

display(spark.createDataFrame([json.dumps(r, default=str) for r in results], "string").select(F.from_json("value", "map<string,string>").alias("r")).select("r.*"))
if mismatches and failOnMismatch:
    raise RuntimeError("Reconciliation mismatches: %s" % [m["table"] for m in mismatches])
dbutils.notebook.exit(json.dumps({"tables": len(results), "mismatches": len(mismatches)}))
