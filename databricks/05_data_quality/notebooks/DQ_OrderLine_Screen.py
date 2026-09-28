# Databricks notebook source
# MAGIC %md
# MAGIC # DQ_OrderLine_Screen
# MAGIC Migrated from `ssis/05_data_quality/DQ_OrderLine_Screen.dtsx` (WWI_DataQuality).
# MAGIC Joins staged order lines to their order header, resolves the order customer (uncached lookup;
# MAGIC misses go to `silver.err_rejected_lookup_failure`), checks quantity range, unit-price outliers and
# MAGIC extension mismatches, quarantines offenders into `silver.err_rejected_order_line`, evaluates rule
# MAGIC group `ORDERLINE` and raises the legacy gates.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, params, naming  # noqa: F401

from dq_quality import delta_io, screens
from dq_quality.gates import Gate, applyGates
from dq_quality.naming_map import deltaTable
from dq_quality.notebook_support import PACKAGE_STEP, PROJECT_NAME, ensureWidgets, setTaskValue, shouldSkipForRestart

PACKAGE_NAME = "DQ_OrderLine_Screen"
SOURCE_SYSTEM_CODE = "WWI_OLTP"
OBJECT_NAME = "stg.OrderLine"
ERR_TABLE = "err.RejectedOrderLine"
LOOKUP_ERR_TABLE = "err.RejectedLookupFailure"

ensureWidgets(dbutils)
p = params.getJobParams(dbutils)
catalog, batchId = p["catalog"], p["batchId"]

if shouldSkipForRestart(p["restartFromStep"], PACKAGE_STEP[PACKAGE_NAME]):
    dbutils.notebook.exit("skipped: RestartFromStep=%s" % p["restartFromStep"])

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME,
                        stepName=PACKAGE_STEP[PACKAGE_NAME]) as run:
    packageExecutionId = run.packageExecutionId
    stgOrderLine = spark.table(deltaTable(catalog, OBJECT_NAME))
    stgOrder = spark.table(deltaTable(catalog, "stg.Order"))

    # Measure Order Line Reject Rate
    rate = stgOrderLine.select(
        (F.lit(100.0) * F.sum(F.when((F.col("OrderedQuantity") <= 0) | (F.col("OrderedQuantity") > 10000), 1).otherwise(0))
         / F.nullif(F.count(F.lit(1)), F.lit(0))).cast("decimal(9,4)").alias("m")).collect()[0]["m"]
    measuredValue = float(rate or 0)

    # STG OrderLine Joined To Order: legacy Ids map onto the staged business keys
    orderLineDf = (stgOrderLine.alias("l").filter(F.col("l.BatchId") == batchId)
                   .join(stgOrder.alias("o"), F.col("l.OrderBusinessKey") == F.col("o.OrderBusinessKey"), "inner")
                   .select(F.col("l.OrderLineBusinessKey").alias("OrderLineId"),
                           F.col("l.OrderBusinessKey").alias("OrderId"),
                           F.col("o.CustomerBusinessKey").alias("CustomerId"),
                           F.col("l.StockItemBusinessKey").alias("StockItemId"),
                           F.col("l.OrderedQuantity").alias("Quantity"),
                           F.col("l.UnitPriceAmount"),
                           F.col("l.NetLineAmount").alias("ExtendedAmount"),
                           F.col("o.RegionCode"),
                           F.col("l.SourceSystemCode"), F.col("l.OrderBusinessKey"), F.col("l.OrderLineBusinessKey"),
                           F.col("l.LineNumber"), F.col("l.BatchId")))
    customerDf = (spark.table(deltaTable(catalog, "stg.Customer"))
                  .select(F.col("CustomerBusinessKey").alias("CustomerId"), F.col("CustomerBusinessKey").alias("CustomerCode")))

    result = screens.screenOrderLine(orderLineDf, customerDf)
    rowsRead = orderLineDf.count()
    rowsPassed = result.passed.count()

    common = lambda df: (df.withColumn("BatchId", F.lit(batchId).cast("long"))  # noqa: E731
                         .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("long"))
                         .withColumn("SourceSystemCode", F.coalesce(F.col("SourceSystemCode"), F.lit(SOURCE_SYSTEM_CODE)))
                         .withColumn("RejectReason", F.col("RejectBranch"))
                         .withColumn("ReprocessStatusCode", F.lit("NEW"))
                         .withColumn("RejectedAtUtc", F.current_timestamp()))

    rejects = common(delta_io.withPayload(result.rejected, excludeColumns=["RejectBranch"])
                     .withColumn("StockItemReference", F.col("StockItemId").cast("string"))
                     .withColumn("OrderedQuantityText", F.col("Quantity").cast("string"))
                     .withColumn("UnitPriceText", F.col("UnitPriceAmount").cast("string"))
                     .withColumn("RejectStage", F.lit("Quality"))
                     .withColumn("ReprocessAttemptCount", F.lit(0)))
    rowsRejected = delta_io.writeRejects(spark, rejects, deltaTable(catalog, ERR_TABLE), batchId,
                                         screens.ORDER_LINE_REASON_CODES)

    # Lookup Order Customer (No Cache) -> ERR Customer Lookup Failure
    lookupFailures = common(delta_io.withPayload(result.branches["Customer Lookup Failure"],
                                                 excludeColumns=["RejectBranch", "LookupName", "LookupColumnName", "LookupValue"])
                            .withColumn("SourceObjectName", F.lit(OBJECT_NAME))
                            .withColumn("SourceBusinessKey", F.col("OrderLineBusinessKey"))
                            .withColumn("RejectStage", F.lit("Quality"))
                            .withColumn("RoutedToUnknownMember", F.lit(False))
                            .withColumn("QueuedForLateArrival", F.lit(True))
                            .withColumn("OccurrenceCount", F.lit(1)))
    lookupRejected = delta_io.writeReplaceWhere(
        spark, lookupFailures, deltaTable(catalog, LOOKUP_ERR_TABLE),
        "BatchId = %d AND SourceObjectName = '%s' AND LookupName = 'Lookup Order Customer (No Cache)'" % (batchId, OBJECT_NAME))
    rowsRejected += lookupRejected

    delta_io.registerRejects(spark, catalog, rejects, OBJECT_NAME, batchId, packageExecutionId,
                             SOURCE_SYSTEM_CODE, "OrderLineBusinessKey", control.logRejectedRecordSet)
    delta_io.registerRejects(spark, catalog, lookupFailures, OBJECT_NAME, batchId, packageExecutionId,
                             SOURCE_SYSTEM_CODE, "OrderLineBusinessKey", control.logRejectedRecordSet)

    failedRuleCount = control.evaluateDataQualityRules(spark, catalog, batchId=batchId,
                                                       packageExecutionId=packageExecutionId,
                                                       ruleGroupCode="ORDERLINE", objectName=OBJECT_NAME)

    delta_io.writeResults(spark, catalog, [
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_OL_REJECT_RATE", measuredValue,
                            thresholdValue=3, rowsEvaluated=rowsRead),
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_OL_SCREEN_PASS", rowsPassed,
                            rowsEvaluated=rowsRead, resultStatus="Passed",
                            detailText="Passes All Rules output of DFT Screen Order Line"),
    ], batchId)

    control.logRowCount(spark, catalog, packageExecutionId, OBJECT_NAME, sourceRowCount=rowsRead,
                        targetRowCount=rowsPassed, rejectRowCount=rowsRejected)
    run.rowsRead, run.rowsInserted, run.rowsRejected = rowsRead, rowsPassed, rowsRejected
    setTaskValue(dbutils, "rowsRejected", rowsRejected)
    setTaskValue(dbutils, "failedRuleCount", int(failedRuleCount or 0))
    setTaskValue(dbutils, "measuredValue", measuredValue)

    applyGates(spark, catalog, [
        Gate("Warn On Order Line Reject Rate", "Order line reject rate exceeded the configured tolerance.",
             "Warning", lambda m: m["MeasuredValue"] > 3, "@[User::MeasuredValue] > 3"),
        Gate("Fail On Order Line Rule Breach", "One or more blocking order line quality rules failed.",
             "Failure", lambda m: m["FailedRuleCount"] > 0, "@[User::FailedRuleCount] > 0"),
    ], {"MeasuredValue": measuredValue, "FailedRuleCount": int(failedRuleCount or 0)},
        packageExecutionId, batchId, PACKAGE_NAME, control.logError)
