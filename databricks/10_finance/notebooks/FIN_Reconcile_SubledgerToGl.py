# Databricks notebook source
# MAGIC %md
# MAGIC # FIN_Reconcile_SubledgerToGl
# MAGIC Ties the AP subledger rows in `gold.fact_payment` back to the GL control accounts in `gold.fact_gl_posting` by ledger/period/account, writes `etl.reconciliation_result`, applies `Finance.KnownVariance.<Account>` explanations, publishes every account's source/target amount to `etl.row_count_log` via `control.logRowCount` and fails on out-of-tolerance via `control.assertRowCountTolerance`.
# MAGIC
# MAGIC Legacy: `ssis/10_finance/FIN_Reconcile_SubledgerToGl.dtsx` (WWI_Finance). Control framework calls go through
# MAGIC `dbx_etl_common` (session 00); finance logic lives in `../src/finance_rules.py`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import delta_io
import finance_common as fc
import finance_rules as rules
import finance_sources as sources
import finance_tables as tables
import period_lock

PACKAGE_NAME = "FIN_Reconcile_SubledgerToGl"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
fin = fc.getFinanceParams(dbutils, p["businessDate"])
if fc.shouldSkipForRestart(PACKAGE_NAME, p.get("restartFromStep")):
    dbutils.notebook.exit(f"{PACKAGE_NAME} skipped: RestartFromStep={p.get('restartFromStep')}")

batchId, ownsBatch = fc.resolveBatchId(spark, catalog, p)
stepName, stepSequence, stepGroup = fc.stepFor(PACKAGE_NAME)
reloadFullHistory = bool(p.get("reloadFullHistory"))

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
tables.ensureFinanceTables(spark, catalog)
tables.ensureGoldTargets(spark, catalog)
print(f"{PACKAGE_NAME} batch={batchId} period={fin.accountingPeriod} businessDate={p['businessDate']} catalog={catalog}")

# COMMAND ----------


reconTable = naming.table(catalog, "etl", "reconciliation_result")
SCOPE = "Finance.SubledgerToGl"

with fc.PackageExecution(spark, catalog, batchId, PACKAGE_NAME, stepName) as run:
    period_lock.assertPeriodOpen(spark, catalog, fin.accountingPeriod, PACKAGE_NAME)

    payments = spark.table(naming.table(catalog, "gold", "fact_payment"))
    glPostings = spark.table(naming.table(catalog, "gold", "fact_gl_posting"))

    # "Build Reconciliation Set" + "Apply Known Explanations"
    results = rules.buildReconciliationSet(payments, glPostings, fin.accountingPeriod, fin.varianceTolerance, batchId)
    results = rules.applyKnownExplanations(results, period_lock.knownVariancesFromConfiguration(spark, catalog)) \
        .withColumn("PackageExecutionId", F.lit(run.packageExecutionId).cast("bigint")).cache()

    # idempotent per batch: replace this batch's rows for the reconciliation
    spark.sql(f"DELETE FROM {reconTable} WHERE BatchId = {batchId} AND ReconciliationName = '{rules.RECONCILIATION_NAME}'")
    reconCols = [c for c in spark.table(reconTable).columns if c in results.columns]
    run.rowsInserted = delta_io.appendRows(results.select(*reconCols), reconTable)
    run.rowsRead = run.rowsInserted

    # "Measure Variances"
    variances = rules.unexplainedVariances(results).cache()
    varianceRowCount = variances.count()
    largestVariance = variances.agg(F.max(F.abs(F.col("VarianceAmount")))).collect()[0][0] or 0

    # Publish every account to etl.row_count_log: SourceRowCount = subledger amount,
    # TargetRowCount = ledger amount (whole functional-currency units), so the shared
    # tolerance gate can evaluate the variance. Explained rows are published as balanced.
    for row in results.select("LedgerCode", "AccountCode", "SourceAmount", "TargetAmount", "VarianceStatus").collect():
        source = int(round(float(row["SourceAmount"] or 0)))
        target = source if row["VarianceStatus"] == "Explained" else int(round(float(row["TargetAmount"] or 0)))
        run.logRowCount(f"{SCOPE}|{row['LedgerCode']}|{row['AccountCode']}", sourceRowCount=source, targetRowCount=target)

    # "Raise Unexplained Variances" -> etl.rejected_record (SUBLEDGER_VARIANCE)
    if varianceRowCount > 0:
        run.rowsRejected = control.logRejectedRecordSet(
            spark, catalog, "etl.ReconciliationResult", variances, batchId=batchId,
            packageExecutionId=run.packageExecutionId, sourceSystemCode="ORAERP", rejectStage="Fact",
            rejectReasonCode="SUBLEDGER_VARIANCE", businessKeyColumn="SourceKey",
        ) or varianceRowCount

    run.logRowCount("etl.ReconciliationResult", sourceRowCount=run.rowsRead, targetRowCount=run.rowsInserted,
                    insertRowCount=run.rowsInserted, rejectRowCount=varianceRowCount)

    # Tolerance gate (Master_Finance_Close AllowCloseWithVariance decides whether it is fatal)
    failedObjectCount = control.assertRowCountTolerance(
        spark, catalog, batchId, scope=SCOPE, objectName=None,
        absoluteTolerance=fin.varianceTolerance, percentTolerance=0,
        raiseOnFailure=not fin.allowCloseWithVariance,
    )
    print(f"variances={varianceRowCount} largest={largestVariance} failedObjects={failedObjectCount}")
    results.unpersist(); variances.unpersist()

dbutils.notebook.exit(f"{PACKAGE_NAME} ok: rows={run.rowsInserted} unexplainedVariances={run.rowsRejected}")
