# Databricks notebook source
# MAGIC %md
# MAGIC # FIN_Close_PeriodLock
# MAGIC Closes the accounting period once the close sequence has run: refuses when unexplained subledger variances remain, then writes one `Locked` row per ledger to `etl.period_lock` (NA/EU on the calendar month, APAC only when its 4-4-5 period has ended), flips `Finance.PeriodStatus.<Ledger>` and stamps the closing batch. Every other FIN_* notebook checks `etl.period_lock` before writing, so a locked period cannot be reloaded.
# MAGIC
# MAGIC Legacy: `ssis/10_finance/FIN_Close_PeriodLock.dtsx` (WWI_Finance). Control framework calls go through
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

PACKAGE_NAME = "FIN_Close_PeriodLock"

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
batchTable = naming.table(catalog, "etl", "batch")
factGl = naming.table(catalog, "gold", "fact_gl_posting")

with fc.PackageExecution(spark, catalog, batchId, PACKAGE_NAME, stepName) as run:
    # "Count Open Variances" - evaluated on the latest reconciliation batch for the period
    recon = spark.table(reconTable).where(F.col("AccountingPeriod") == F.lit(fin.accountingPeriod))
    latestBatch = recon.agg(F.max("BatchId")).collect()[0][0]
    openVariances = 0
    if latestBatch is not None:
        openVariances = recon.where((F.col("BatchId") == F.lit(latestBatch)) & (F.col("VarianceStatus") == "Variance")).count()
    run.rowsRead = openVariances

    if openVariances > 0 and not fin.allowCloseWithVariance:
        # "Refuse Close" (legacy error 51001) - surfaced as a failed task so the master escalates
        control.logError(spark, catalog, packageExecutionId=run.packageExecutionId, batchId=batchId,
                         errorSeverity="Error", errorCode="51001", sourceName=PACKAGE_NAME,
                         sourceComponent="Refuse Close", procedureName=None,
                         errorDescription="Period lock refused: unexplained subledger variances remain.")
        raise RuntimeError(f"Period lock refused for {fin.accountingPeriod}: {openVariances} unexplained variance(s).")

    # "Lock Ledgers": ledgers that posted in the period, filtered by LedgerScope
    ledgers = [r[0] for r in spark.table(factGl).where(F.col("AccountingPeriod") == F.lit(fin.accountingPeriod))
               .select("LedgerCode").distinct().collect() if r[0]]
    if fin.ledgerScope != "ALL":
        ledgers = [l for l in ledgers if l.upper() == fin.ledgerScope]

    # "Lock APAC 445 Period": APAC ledgers only lock once the 4-4-5 period end has passed
    apacEnded = period_lock.apac445PeriodEnded(spark, catalog, fin.accountingPeriod)
    toLock = [l for l in ledgers if not l.upper().startswith("APAC") or apacEnded]
    lockedCount = period_lock.lockLedgers(spark, catalog, toLock, fin.accountingPeriod, batchId, PACKAGE_NAME,
                                         notes=f"ledgerScope={fin.ledgerScope}; apac445Ended={apacEnded}")
    run.rowsInserted = lockedCount

    # "Stamp Closing Batch"
    if spark.catalog.tableExists(batchTable):
        spark.sql(f"UPDATE {batchTable} SET Notes = CONCAT(COALESCE(Notes, ''), ' | period {fin.accountingPeriod} locked') WHERE BatchId = {batchId}")

    run.logRowCount("etl.Batch", sourceRowCount=len(ledgers), targetRowCount=lockedCount, updateRowCount=lockedCount)
    print(f"locked {lockedCount}/{len(ledgers)} ledger(s) for {fin.accountingPeriod}: {toLock}")

if ownsBatch:
    control.endBatch(spark, catalog, batchId, forceStatus=None)

dbutils.notebook.exit(f"{PACKAGE_NAME} ok: lockedLedgers={run.rowsInserted}")
