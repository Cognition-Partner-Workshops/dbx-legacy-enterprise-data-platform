# Databricks notebook source
# MAGIC %md
# MAGIC # FIN_Currency_Revaluation
# MAGIC Month-end revaluation of open foreign-currency items in `gold.fact_payment`: closing rate for balance-sheet items, period average for P&L, quote currency per ledger (EU->EUR, APAC->entity currency, NA->USD), inverse rate derived, and a missing closing rate stops the close unless `FailOnMissingRate=False`.
# MAGIC
# MAGIC Legacy: `ssis/10_finance/FIN_Currency_Revaluation.dtsx` (WWI_Finance). Control framework calls go through
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

PACKAGE_NAME = "FIN_Currency_Revaluation"

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


workRates = naming.table(catalog, "silver", "work_fx_revaluation_rate")
factPayment = naming.table(catalog, "gold", "fact_payment")

with fc.PackageExecution(spark, catalog, batchId, PACKAGE_NAME, stepName) as run:
    period_lock.assertPeriodOpen(spark, catalog, fin.accountingPeriod, PACKAGE_NAME)

    # Data flow "Load Closing Rates" -> work.FxRevaluationRate
    fxRates = spark.sql(sources.fxRateSql(catalog))
    rates = rules.closingRates(fxRates, fin.revaluationDate).withColumn("BatchId", F.lit(batchId).cast("bigint")).cache()
    run.rowsRead = rates.count()
    delta_io.replaceWhere(rates, workRates, f"BatchId = {batchId}")

    # "Find Missing Rates"
    openInvoices = spark.sql(sources.apInvoiceSql(catalog))
    missing = rules.missingClosingRateCurrencies(openInvoices, rates)
    missingCount = missing.count()
    if missingCount > 0 and fin.failOnMissingRate:
        control.logError(spark, catalog, packageExecutionId=run.packageExecutionId, batchId=batchId,
                         errorSeverity="Error", errorCode="51010", sourceName=PACKAGE_NAME,
                         sourceComponent="Find Missing Rates", procedureName=None,
                         errorDescription="Closing FX rate missing for one or more open-item currencies: "
                                          + ", ".join(sorted(r[0] for r in missing.collect() if r[0])))
        run.logRowCount("Fact.Payment", sourceRowCount=run.rowsRead, targetRowCount=0, rejectRowCount=missingCount)
        raise RuntimeError(f"Closing FX rate missing for {missingCount} currency(ies); FailOnMissingRate=True.")

    # "Revalue Open Items" -> MERGE update on gold.fact_payment
    payments = spark.table(factPayment)
    revalued = rules.revalueOpenItems(payments, rates).cache()
    run.rowsUpdated = delta_io.mergeInto(
        spark, factPayment, revalued, ["PaymentBusinessKey"],
        updateColumns=["RevaluedFunctionalAmount", "UnrealisedGainLossAmount", "RevaluationRateDate",
                       "RevaluationRateType", "FxRateToReporting", "FxRateSourceCode"],
    )

    # "Count Revalued Items"
    revaluedItems = spark.table(factPayment).where(F.col("RevaluationRateDate") == F.lit(fin.revaluationDate)).count()
    run.logRowCount("Fact.Payment", sourceRowCount=payments.where(F.coalesce(F.col("OpenAmount"), F.lit(0)) != 0).count(),
                    targetRowCount=run.rowsUpdated, updateRowCount=run.rowsUpdated, rejectRowCount=missingCount)
    print(f"rates={run.rowsRead} missing={missingCount} revalued={run.rowsUpdated} onRevaluationDate={revaluedItems}")
    rates.unpersist(); revalued.unpersist()

dbutils.notebook.exit(f"{PACKAGE_NAME} ok: rates={run.rowsRead} revalued={run.rowsUpdated}")
