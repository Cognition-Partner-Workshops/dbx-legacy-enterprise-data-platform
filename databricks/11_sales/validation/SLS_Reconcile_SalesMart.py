# Databricks notebook source
# MAGIC %md
# MAGIC # SLS_Reconcile_SalesMart - reconciliation of the WWI_Sales targets
# MAGIC For every table loaded by the wwi_11_sales job this notebook computes
# MAGIC * row counts (per BatchId / RefreshBatchId partition where the table carries one), and
# MAGIC * a deterministic hash: `sum(xxhash64(concat_ws('|', <all columns sorted by name>)))` with the
# MAGIC   volatile refresh timestamps excluded,
# MAGIC and compares them with the SQL Server baseline captured from the legacy warehouse. The baseline
# MAGIC is supplied either as a Delta table (`BaselineTable`, default `<catalog>.etl.sales_reconciliation_baseline`,
# MAGIC columns ObjectName, PartitionKey, BaselineRowCount, BaselineHash) or inline as JSON (`BaselineJson`,
# MAGIC a list of objects with the same keys). Results are written to `etl.row_count_log` through
# MAGIC `control.logRowCount` (source = SQL Server baseline, target = Delta) and the variance checks of
# MAGIC `validation/runtime/02_row_count_reconciliation.sql` are re-run in Spark SQL over `etl.*`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, naming, params  # noqa: E402

import sales_common as sc  # noqa: E402
import sales_schemas as schemas  # noqa: E402
import json  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "SLS_Reconcile_SalesMart"
ctx = sc.resolveContext(spark, dbutils, params, control, PACKAGE_NAME,
                        (("BaselineTable", ""), ("BaselineJson", ""), ("VarianceTolerancePercent", "")))


def t(schema, table):
    return naming.table(ctx.catalog, schema, table)


baselineTable = sc.parseOptional(sc.getWidget(dbutils, "BaselineTable")) or t("etl", "sales_reconciliation_baseline")
baselineJson = sc.parseOptional(sc.getWidget(dbutils, "BaselineJson"))
toleranceText = sc.parseOptional(sc.getWidget(dbutils, "VarianceTolerancePercent"))
if toleranceText is None:
    try:
        toleranceText = control.getConfiguration(spark, ctx.catalog, "RowCountVarianceTolerancePercent",
                                                 environmentCode=ctx.environmentCode)
    except Exception:
        toleranceText = None
tolerancePercent = float(toleranceText) if toleranceText else 0.0

# Legacy object name -> (Delta table, partition column, volatile columns excluded from the hash)
TARGETS = {
    "work.CommissionNa": (t("silver", "work_commission_na"), "BatchId", ()),
    "work.CommissionEu": (t("silver", "work_commission_eu"), "BatchId", ()),
    "work.CommissionEuHeld": (t("silver", "work_commission_eu_held"), "BatchId", ()),
    "work.CommissionApac": (t("silver", "work_commission_apac"), "BatchId", ()),
    "err.CommissionApacReject": (t("silver", "err_commission_apac_reject"), "BatchId", ()),
    "Fact.Sale": (t("gold", "fact_sale_commission"), "BatchId", ("PostedAtUtc", "PackageExecutionId")),
    "work.QuotaAttainment": (t("silver", "work_quota_attainment"), "BatchId", ()),
    "Aggregate.Regional Sales Performance": (t("gold", "agg_regional_sales_performance"), "RefreshBatchId", ("RefreshedDatetime",)),
    "work.PromotionSpill": (t("silver", "work_promotion_spill"), "BatchId", ()),
    "Aggregate.Promotion Effectiveness": (t("gold", "agg_promotion_effectiveness"), "RefreshBatchId", ("RefreshedDatetime",)),
    "file:partner_feed.csv": (t("silver", "work_partner_feed_archive"), "BatchId", ("ExportedAtUtc", "PackageExecutionId")),
}

# COMMAND ----------


def deterministicHash(df, excluded=()):
    cols = sorted(c for c in df.columns if c not in excluded)
    return F.sum(F.xxhash64(F.concat_ws("|", *[F.coalesce(F.col("`%s`" % c).cast("string"), F.lit("<null>")) for c in cols])))


def measure(objectName, tableName, partitionColumn, excluded):
    if not spark.catalog.tableExists(tableName):
        return [{"ObjectName": objectName, "PartitionKey": None, "DeltaRowCount": None, "DeltaHash": None,
                 "Note": "table missing: " + tableName}]
    df = spark.table(tableName)
    if partitionColumn in df.columns:
        scope = df if ctx.reloadFullHistory or ctx.batchId == 0 else df.where(F.col(partitionColumn) == ctx.batchId)
        grouped = (scope.groupBy(F.col(partitionColumn).cast("string").alias("PartitionKey"))
                        .agg(F.count(F.lit(1)).alias("DeltaRowCount"), deterministicHash(scope, excluded + (partitionColumn,)).alias("DeltaHash")))
    else:
        grouped = df.agg(F.count(F.lit(1)).alias("DeltaRowCount"), deterministicHash(df, excluded).alias("DeltaHash")) \
                    .withColumn("PartitionKey", F.lit(None).cast("string"))
    return [{"ObjectName": objectName, "PartitionKey": r["PartitionKey"], "DeltaRowCount": int(r["DeltaRowCount"]),
             "DeltaHash": int(r["DeltaHash"]) if r["DeltaHash"] is not None else None, "Note": None}
            for r in grouped.collect()]


measured = []
for objectName, (tableName, partitionColumn, excluded) in TARGETS.items():
    measured.extend(measure(objectName, tableName, partitionColumn, excluded))
measuredDf = spark.createDataFrame(measured, "ObjectName string, PartitionKey string, DeltaRowCount long, DeltaHash long, Note string")
display(measuredDf)

# COMMAND ----------

if baselineJson:
    baselineDf = spark.createDataFrame(json.loads(baselineJson), schemas.RECONCILIATION_BASELINE)
elif spark.catalog.tableExists(baselineTable):
    baselineDf = spark.table(baselineTable)
else:
    baselineDf = spark.createDataFrame([], schemas.RECONCILIATION_BASELINE)
    print("No baseline supplied (%s missing, BaselineJson empty): Delta figures logged without comparison." % baselineTable)

joined = (measuredDf.alias("d").join(baselineDf.alias("b"),
          (F.col("d.ObjectName") == F.col("b.ObjectName")) & (F.col("d.PartitionKey").eqNullSafe(F.col("b.PartitionKey"))), "left")
          .select("d.*", "b.BaselineRowCount", "b.BaselineHash")
          .withColumn("VarianceRowCount", F.col("DeltaRowCount") - F.col("BaselineRowCount"))
          .withColumn("VariancePercent", F.when(F.col("BaselineRowCount") > 0,
                                                F.abs(F.col("VarianceRowCount")) * 100.0 / F.col("BaselineRowCount")))
          .withColumn("HashMatches", F.col("DeltaHash").eqNullSafe(F.col("BaselineHash")))
          .withColumn("Outcome", F.when(F.col("BaselineRowCount").isNull(), "NO_BASELINE")
                                  .when(F.col("HashMatches") & (F.col("VarianceRowCount") == 0), "MATCH")
                                  .when(F.coalesce(F.col("VariancePercent"), F.lit(0.0)) <= tolerancePercent, "WITHIN_TOLERANCE")
                                  .otherwise("MISMATCH")))
display(joined)

# COMMAND ----------

with sc.legacyPackageRun(spark, control, ctx, PACKAGE_NAME) as run:
    pid = run.packageExecutionId
    mismatches = 0
    for r in joined.collect():
        objectName = r["ObjectName"] if r["PartitionKey"] is None else "%s[%s]" % (r["ObjectName"], r["PartitionKey"])
        control.logRowCount(spark, ctx.catalog, pid, objectName,
                            sourceRowCount=r["BaselineRowCount"], targetRowCount=r["DeltaRowCount"],
                            rejectRowCount=None if r["Outcome"] in ("MATCH", "NO_BASELINE", "WITHIN_TOLERANCE") else r["VarianceRowCount"])
        run.rowsRead += 1
        if r["Outcome"] == "MISMATCH":
            mismatches += 1
            control.logError(spark, ctx.catalog, packageExecutionId=pid, batchId=ctx.batchId,
                             errorSeverity="Warning", errorCode="RECONCILIATION_MISMATCH", sourceName=PACKAGE_NAME,
                             sourceComponent=objectName,
                             errorDescription="baseline rows=%s hash=%s; delta rows=%s hash=%s" %
                                              (r["BaselineRowCount"], r["BaselineHash"], r["DeltaRowCount"], r["DeltaHash"]))
    run.rowsRejected = mismatches
print("objects compared: %d, mismatches: %d" % (run.rowsRead, mismatches))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control-framework checks ported from validation/runtime/02_row_count_reconciliation.sql
# MAGIC (variance per package in this batch, successful packages that logged no row count).

# COMMAND ----------

rowCountLog = t("etl", "row_count_log")
packageExecution = t("etl", "package_execution")
if spark.catalog.tableExists(rowCountLog) and spark.catalog.tableExists(packageExecution):
    variance = spark.sql(f"""
        SELECT pe.PackageName, rc.ObjectName, rc.SourceRowCount, rc.TargetRowCount, rc.RejectRowCount,
               rc.SourceRowCount - rc.TargetRowCount - COALESCE(rc.RejectRowCount, 0) AS UnexplainedVariance,
               CASE WHEN rc.SourceRowCount > 0
                    THEN ABS(rc.SourceRowCount - rc.TargetRowCount - COALESCE(rc.RejectRowCount, 0)) * 100.0 / rc.SourceRowCount END AS VariancePercent
        FROM {rowCountLog} rc
        JOIN {packageExecution} pe ON pe.PackageExecutionId = rc.PackageExecutionId
        WHERE pe.BatchId = {ctx.batchId} AND pe.ProjectName = '{sc.PROJECT_NAME}'
        ORDER BY VariancePercent DESC NULLS LAST
    """)
    display(variance)
    silent = spark.sql(f"""
        SELECT pe.PackageName, pe.Status, pe.RowsRead, pe.RowsInserted
        FROM {packageExecution} pe
        LEFT JOIN {rowCountLog} rc ON rc.PackageExecutionId = pe.PackageExecutionId
        WHERE pe.BatchId = {ctx.batchId} AND pe.ProjectName = '{sc.PROJECT_NAME}'
          AND pe.Status = 'Succeeded' AND rc.PackageExecutionId IS NULL
    """)
    display(silent)
else:
    print("etl.row_count_log / etl.package_execution not found in %s; skipping the control checks." % ctx.catalog)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"objectsCompared": run.rowsRead, "mismatches": mismatches, "tolerancePercent": tolerancePercent}))
