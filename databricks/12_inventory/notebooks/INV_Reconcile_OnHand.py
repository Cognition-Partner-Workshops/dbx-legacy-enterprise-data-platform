# Databricks notebook source
# MAGIC %md
# MAGIC # INV_Reconcile_OnHand
# MAGIC Port of `ssis/12_inventory/INV_Reconcile_OnHand.dtsx` (WWI_Inventory).
# MAGIC Ties `gold.fact_stock_holding` back to the operational position per site and item,
# MAGIC classifies differences (Matched / Timing / Negative on hand / Variance) into
# MAGIC `etl.reconciliation_result`, escalates genuine variances (`ONHAND_VARIANCE`) and
# MAGIC records the on-hand totals through `control.logRowCount` so the master job's
# MAGIC `OnHandVarianceTolerance` gate and `control.assertRowCountTolerance` can read them.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, params, naming  # noqa: E402,F401
from inv_common import contracts, runtime, transforms  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "INV_Reconcile_OnHand"
ctx = runtime.PackageContext(spark, dbutils, PACKAGE_NAME)  # Log Package Start

siteScope = runtime.widget(dbutils, "SiteScope", "ALL")
timingWindowMinutes = int(runtime.widget(dbutils, "TimingWindowMinutes", "30"))
onHandVarianceTolerance = runtime.widget(dbutils, "OnHandVarianceTolerance", "")  # optional absolute tolerance

# COMMAND ----------


def body(ctx: runtime.PackageContext) -> None:
    runtime.ensureProjectTables(spark, ctx.catalog)

    position = ctx.read("work.InventoryPositionDaily")
    stockHolding = ctx.read("Fact.Stock Holding")
    stockMovement = ctx.read("stg.StockMovement")

    # Build On Hand Comparison -> etl.ReconciliationResult (rerun-safe: replace this batch's rows)
    comparison = transforms.buildOnHandComparison(
        position, stockHolding, stockMovement, ctx.batchId, timingWindowMinutes, siteScope, runtime.utcNow(),
        reconciliationName=contracts.RECONCILIATION_NAME, objectName="Fact.Stock Holding",
    )
    resultTable = ctx.table("etl.ReconciliationResult")
    predicate = f"BatchId = {ctx.batchId} AND ObjectName = 'Fact.Stock Holding'"
    ctx.rowsInserted = runtime.replaceWhere(spark, comparison, resultTable, predicate)
    results = spark.table(resultTable).where(predicate)
    ctx.rowsRead = ctx.rowsInserted

    # Classify Differences
    genuineDifferenceCount, timingDifferenceCount = transforms.classifyDifferences(results)
    print(f"GenuineDifferenceCount={genuineDifferenceCount} TimingDifferenceCount={timingDifferenceCount}")

    # Escalate Genuine Differences (precedence: GenuineDifferenceCount > 0)
    if genuineDifferenceCount > 0:
        ctx.logRejectedSet("Fact.Stock Holding", transforms.genuineDifferences(results), contracts.REASON_ONHAND_VARIANCE)

    # Log Row Counts: the package's own audit row ...
    ctx.logPackageRowCounts("etl.ReconciliationResult")
    # ... plus the on-hand totals the intraday master reads as OnHandVariance
    # (ABS(SourceRowCount - TargetRowCount) for ObjectName = 'Fact.Stock Holding')
    totals = results.agg(
        F.sum("SourceAmount").alias("src"), F.sum("TargetAmount").alias("tgt"),
        F.sum(F.when(F.col("VarianceStatus") == "Variance", 1).otherwise(0)).alias("rej"),
    ).first()
    ctx.logRowCount(
        "Fact.Stock Holding",
        sourceRowCount=int(totals["src"] or 0), targetRowCount=int(totals["tgt"] or 0),
        rejectRowCount=int(totals["rej"] or 0),
    )
    if onHandVarianceTolerance not in ("", None):
        control.assertRowCountTolerance(
            spark, ctx.catalog, ctx.batchId, scope="OBJECT", objectName="Fact.Stock Holding",
            absoluteTolerance=int(onHandVarianceTolerance), raiseOnFailure=False,
        )


runtime.runPackage(ctx, body)
