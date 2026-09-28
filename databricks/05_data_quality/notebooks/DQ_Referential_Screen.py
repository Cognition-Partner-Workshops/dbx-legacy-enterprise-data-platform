# Databricks notebook source
# MAGIC %md
# MAGIC # DQ_Referential_Screen
# MAGIC Migrated from `ssis/05_data_quality/DQ_Referential_Screen.dtsx` (WWI_DataQuality).
# MAGIC Referential integrity screen: staged order lines must resolve to a staged stock item and a package
# MAGIC type, staged sale lines to a reference currency and a sales territory. Lookup misses are quarantined
# MAGIC into `silver.err_rejected_lookup_failure` instead of failing the flow; rule group `REFERENTIAL` is
# MAGIC evaluated and the legacy orphan-key gates are raised.
# MAGIC
# MAGIC The orchestration plan runs this package twice ("Referential Screen" and, after
# MAGIC `DQ_Reject_Reprocess`, "Referential Rescreen"); the `ScreenScope` widget selects the phase.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dbx_etl_common import control, params, naming  # noqa: F401

from dq_quality import delta_io, screens
from dq_quality.gates import Gate, applyGates
from dq_quality.naming_map import deltaTable
from dq_quality.notebook_support import PACKAGE_STEP, PROJECT_NAME, ensureWidgets, setTaskValue, shouldSkipForRestart

PACKAGE_NAME = "DQ_Referential_Screen"
SOURCE_SYSTEM_CODE = "WWI_OLTP"
OBJECT_NAME = "stg.OrderLine"
ERR_TABLE = "err.RejectedLookupFailure"

ensureWidgets(dbutils)
dbutils.widgets.text("ScreenScope", "Referential Screen")
p = params.getJobParams(dbutils)
catalog, batchId = p["catalog"], p["batchId"]
screenScope = dbutils.widgets.get("ScreenScope").strip() or PACKAGE_STEP[PACKAGE_NAME]
rejectStage = "Referential" if screenScope == "Referential Screen" else "ReferentialRescreen"

if shouldSkipForRestart(p["restartFromStep"], screenScope):
    dbutils.notebook.exit("skipped: RestartFromStep=%s" % p["restartFromStep"])

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME,
                        stepName=screenScope) as run:
    packageExecutionId = run.packageExecutionId
    stgOrderLine = spark.table(deltaTable(catalog, "stg.OrderLine"))
    stgSaleLine = spark.table(deltaTable(catalog, "stg.SaleLine"))
    stgSale = spark.table(deltaTable(catalog, "stg.Sale"))
    stockItemDf = (spark.table(deltaTable(catalog, "stg.StockItem"))
                   .select(F.col("StockItemBusinessKey").alias("StockItemId"), "StockItemName"))

    # Measure Orphan Key Count (whole table: order lines without a staged stock item)
    orphanCount = (stgOrderLine.alias("l")
                   .join(stockItemDf.alias("s"), F.col("l.StockItemBusinessKey") == F.col("s.StockItemId"), "left")
                   .filter(F.col("s.StockItemId").isNull()).count())
    measuredValue = float(orphanCount)

    # ref.PackageType has no legacy DDL; when the Delta table is absent the lookup is skipped and logged.
    packageTypeTable = deltaTable(catalog, "ref.PackageType")
    if delta_io.tableExists(spark, packageTypeTable):
        packageTypeDf = spark.table(packageTypeTable).select("PackageTypeCode", "PackageTypeName")
    else:
        control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                         errorSeverity="Warning", errorCode=50000, sourceName=PACKAGE_NAME,
                         sourceComponent="Lookup Package Type (Full Cache)",
                         errorDescription="%s does not exist; package type lookup skipped" % packageTypeTable)
        packageTypeDf = (stgOrderLine.select("PackageTypeCode").distinct()
                         .withColumn("PackageTypeName", F.col("PackageTypeCode")))

    orderKeysDf = (stgOrderLine.filter(F.col("BatchId") == batchId)
                   .select(F.col("OrderLineBusinessKey").alias("OrderLineId"),
                           F.col("StockItemBusinessKey").alias("StockItemId"),
                           "PackageTypeCode",
                           F.lit("stg.OrderLine").alias("SourceObjectName"),
                           "SourceSystemCode", "OrderLineBusinessKey", "BatchId"))
    orderResult = screens.screenReferentialOrder(orderKeysDf, stockItemDf, packageTypeDf)

    # stg.Sale carries no SalesTerritoryCode; the territory lookup resolves the sale RegionCode
    # against stg.SalesTerritory.RegionCode (see mapping doc "needs decision").
    currencyDf = (spark.table(deltaTable(catalog, "ref.Currency"))
                  .select(F.col("CurrencyCode").alias("SaleCurrencyCode"), "CurrencyName"))
    territoryDf = (spark.table(deltaTable(catalog, "stg.SalesTerritory"))
                   .select(F.col("RegionCode").alias("SalesTerritoryCode"),
                           F.min("SalesTerritoryName").over(Window.partitionBy("RegionCode")).alias("SalesTerritoryName"))
                   .dropDuplicates(["SalesTerritoryCode"]))
    saleKeysDf = (stgSaleLine.alias("l").filter(F.col("l.BatchId") == batchId)
                  .join(stgSale.alias("s"), F.col("l.SaleBusinessKey") == F.col("s.SaleBusinessKey"), "inner")
                  .select(F.col("l.SaleLineBusinessKey").alias("InvoiceLineId"),
                          F.col("l.StockItemBusinessKey").alias("StockItemId"),
                          F.col("s.TransactionCurrencyCode").alias("SaleCurrencyCode"),
                          F.col("s.RegionCode").alias("SalesTerritoryCode"),
                          F.lit("stg.SaleLine").alias("SourceObjectName"),
                          F.col("l.SourceSystemCode"), F.col("l.SaleLineBusinessKey"), F.col("l.BatchId")))
    saleResult = screens.screenReferentialSale(saleKeysDf, currencyDf, territoryDf)

    rowsRead = orderKeysDf.count() + saleKeysDf.count()
    rowsPassed = orderResult.passed.count() + saleResult.passed.count()

    def toLookupFailure(df, keyColumn):
        return (delta_io.withPayload(df, excludeColumns=["RejectBranch", "LookupName", "LookupColumnName", "LookupValue"])
                .withColumn("BatchId", F.lit(batchId).cast("long"))
                .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("long"))
                .withColumn("SourceSystemCode", F.coalesce(F.col("SourceSystemCode"), F.lit(SOURCE_SYSTEM_CODE)))
                .withColumn("SourceBusinessKey", F.col(keyColumn))
                .withColumn("RejectReason", F.col("RejectBranch"))
                .withColumn("RejectStage", F.lit(rejectStage))
                .withColumn("RoutedToUnknownMember", F.lit(False))
                .withColumn("QueuedForLateArrival", F.col("LookupName").startswith("Lookup Stock Item"))
                .withColumn("OccurrenceCount", F.lit(1))
                .withColumn("ReprocessStatusCode", F.lit("NEW"))
                .withColumn("RejectedAtUtc", F.current_timestamp()))

    rejects = screens.unionAll([toLookupFailure(orderResult.rejected, "OrderLineBusinessKey"),
                                toLookupFailure(saleResult.rejected, "SaleLineBusinessKey")])
    rowsRejected = delta_io.writeReplaceWhere(spark, rejects, deltaTable(catalog, ERR_TABLE),
                                              "BatchId = %d AND RejectStage = '%s'" % (batchId, rejectStage))
    delta_io.registerRejects(spark, catalog, rejects.filter(F.col("SourceObjectName") == "stg.OrderLine"),
                             "stg.OrderLine", batchId, packageExecutionId, SOURCE_SYSTEM_CODE,
                             "SourceBusinessKey", control.logRejectedRecordSet, rejectStage=rejectStage)
    delta_io.registerRejects(spark, catalog, rejects.filter(F.col("SourceObjectName") == "stg.SaleLine"),
                             "stg.SaleLine", batchId, packageExecutionId, SOURCE_SYSTEM_CODE,
                             "SourceBusinessKey", control.logRejectedRecordSet, rejectStage=rejectStage)

    failedRuleCount = control.evaluateDataQualityRules(spark, catalog, batchId=batchId,
                                                       packageExecutionId=packageExecutionId,
                                                       ruleGroupCode="REFERENTIAL", objectName=OBJECT_NAME)

    delta_io.writeResults(spark, catalog, [
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_REF_ORPHANS", measuredValue,
                            thresholdValue=0, rowsEvaluated=rowsRead),
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_REF_SCREEN_PASS", rowsPassed,
                            rowsEvaluated=rowsRead, resultStatus="Passed", detailText=screenScope),
    ], batchId)

    control.logRowCount(spark, catalog, packageExecutionId, OBJECT_NAME, sourceRowCount=orderKeysDf.count(),
                        targetRowCount=orderResult.passed.count(), rejectRowCount=orderResult.rejected.count())
    control.logRowCount(spark, catalog, packageExecutionId, "stg.SaleLine", sourceRowCount=saleKeysDf.count(),
                        targetRowCount=saleResult.passed.count(), rejectRowCount=saleResult.rejected.count())
    run.rowsRead, run.rowsInserted, run.rowsRejected = rowsRead, rowsPassed, rowsRejected
    setTaskValue(dbutils, "rowsRejected", rowsRejected)
    setTaskValue(dbutils, "failedRuleCount", int(failedRuleCount or 0))
    setTaskValue(dbutils, "measuredValue", measuredValue)

    applyGates(spark, catalog, [
        Gate("Warn On Orphan Keys", "Orphan foreign keys were detected in the staged transaction feeds.",
             "Warning", lambda m: m["MeasuredValue"] > 0, "@[User::MeasuredValue] > 0"),
        Gate("Fail On Excessive Orphans", "Orphan foreign key count exceeded the blocking threshold.",
             "Failure", lambda m: m["MeasuredValue"] > 1000, "@[User::MeasuredValue] > 1000"),
    ], {"MeasuredValue": measuredValue, "FailedRuleCount": int(failedRuleCount or 0)},
        packageExecutionId, batchId, PACKAGE_NAME, control.logError)
