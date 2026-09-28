# Databricks notebook source
# MAGIC %md
# MAGIC # FIN_Load_GlPostings
# MAGIC Loads POSTED GL journal lines whose period is the open period for their ledger into `gold.fact_gl_posting`. Lines belonging to another period are **held** (`silver.work_gl_held_line`) and logged as `PERIOD_NOT_OPEN` rejects rather than dropped; unbalanced journals stop the load unless `AllowUnbalancedJournals`.
# MAGIC
# MAGIC Legacy: `ssis/10_finance/FIN_Load_GlPostings.dtsx` (WWI_Finance). Control framework calls go through
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

PACKAGE_NAME = "FIN_Load_GlPostings"

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


factGl = naming.table(catalog, "gold", "fact_gl_posting")
workHeld = naming.table(catalog, "silver", "work_gl_held_line")

with fc.PackageExecution(spark, catalog, batchId, PACKAGE_NAME, stepName) as run:
    period_lock.assertPeriodOpen(spark, catalog, fin.accountingPeriod, PACKAGE_NAME)

    journalLines = spark.sql(sources.glJournalLineSql(catalog))

    # "Check Journal Balance"
    unbalanced = rules.unbalancedJournalCount(journalLines, batchId, reloadFullHistory)
    if unbalanced > 0 and not fin.allowUnbalancedJournals:
        raise RuntimeError(f"{unbalanced} unbalanced journal(s) in batch {batchId}; set AllowUnbalancedJournals=True to override.")

    # Data flow "Load GL Postings"
    openPeriods = period_lock.openPeriodsFromConfiguration(spark, catalog, p.get("environmentCode"))
    posted = rules.deriveGlPostingAttributes(rules.glPostedLines(journalLines, openPeriods, batchId, reloadFullHistory))
    postable, held = rules.splitHeldLines(posted, fin.accountingPeriod)
    postable = postable.cache()
    held = held.withColumn("BatchId", F.lit(batchId).cast("bigint")).withColumn("HeldAtUtc", F.current_timestamp()).cache()

    run.rowsRead = posted.count()
    heldCount = held.count()

    factRows = postable.withColumn("PeriodStatusCode", F.lit("Open")) \
        .withColumn("BatchId", F.lit(batchId).cast("bigint")).withColumn("LoadDatetime", F.current_timestamp())
    run.rowsInserted = delta_io.mergeInto(spark, factGl, factRows, ["GlJournalLineId"])

    # Held branch -> work.GlHeldLine + usp_LogRejectedRecord(PERIOD_NOT_OPEN, BusinessKey='batch')
    delta_io.replaceWhere(held, workHeld, f"BatchId = {batchId}")
    if heldCount > 0:
        control.logRejectedRecord(
            spark, catalog, "stg.GlJournalLine", "PERIOD_NOT_OPEN",
            packageExecutionId=run.packageExecutionId, batchId=batchId, sourceSystemCode="ORAERP",
            businessKey="batch", rejectStage="Fact",
            rejectReason=f"Journal line belongs to a period other than the one being posted ({heldCount} line(s) held).",
            recordPayload=None,
        )
    run.rowsRejected = heldCount

    run.logRowCount("Fact.GL Posting", sourceRowCount=run.rowsRead, targetRowCount=run.rowsInserted,
                    insertRowCount=run.rowsInserted, rejectRowCount=heldCount)
    postable.unpersist(); held.unpersist()

dbutils.notebook.exit(f"{PACKAGE_NAME} ok: read={run.rowsRead} posted={run.rowsInserted} held={run.rowsRejected}")
