# Databricks notebook source
# MAGIC %md
# MAGIC # C360_Build_ChurnFlags
# MAGIC Migrated from `ssis/14_customer_360/C360_Build_ChurnFlags.dtsx` (project `WWI_Customer360`).
# MAGIC Joins the churn feature set with the loyalty overlay, applies the five churn rules (rule set 2019.3), bands the risk, writes `Customer360.CustomerChurnFlag` -> `gold.c360_customer_churn_flag`, copies HIGH rows to the high-risk work table and queues them for outreach.
# MAGIC
# MAGIC Control framework calls go through the shared `dbx_etl_common` package (session 00); the
# MAGIC business rules live in `../src/c360_lib` so they can be unit tested with local PySpark.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from pyspark.sql import functions as F  # noqa: E402

from c360_lib import runtime as R  # noqa: E402
from c360_lib import tables as T  # noqa: E402

from c360_lib import churn as C  # noqa: E402

PACKAGE_NAME = "C360_Build_ChurnFlags"
STEP_NAME = "Customer 360 Build"

# COMMAND ----------

ctx = R.getJobContext(dbutils)
highRiskScoreThreshold = int(R.widget(dbutils, "HighRiskScoreThreshold", str(C.DEFAULT_HIGH_RISK_THRESHOLD)))
orderGapMultiplier = float(R.widget(dbutils, "OrderGapMultiplier", str(C.DEFAULT_ORDER_GAP_MULTIPLIER)))
catalog = ctx.catalog
asOf = ctx.businessDate

if R.shouldSkipForRestart(ctx, PACKAGE_NAME):
    dbutils.notebook.exit(f"SKIPPED (RestartFromStep={ctx.restartFromStep})")

# COMMAND ----------

with R.packageLifecycle(spark, ctx, PACKAGE_NAME, STEP_NAME) as run:
    rollingMetric = R.readTable(spark, T.table(catalog, "Customer360.CustomerRollingMetric"))
    dimCustomer = R.readTable(spark, T.table(catalog, "Dimension.Customer"))
    salesSummary = R.readTable(spark, T.table(catalog, "work.CustomerSalesSummary"))
    aggRolling12 = R.readTable(spark, T.table(catalog, "Aggregate.Customer Rolling 12 Month"))
    loyaltyOverlay = R.readTable(spark, T.table(catalog, "work.LoyaltyOverlay"))

    # work.ChurnFeatureSet (feeder rebuilt from the C360 / aggregate inputs)
    features = C.buildChurnFeatureSet(rollingMetric, dimCustomer, salesSummary, aggRolling12, asOf)
    featureTable = T.table(catalog, "work.ChurnFeatureSet")
    R.overwriteTable(R.stamp(features, ctx), featureTable)

    # Truncate work_CustomerChurnFlag + Score Churn Risk (data flow)
    source = C.buildChurnSource(R.readTable(spark, featureTable), loyaltyOverlay)
    run.rowsRead = source.count()
    scored = R.stamp(C.scoreChurnRisk(source, highRiskScoreThreshold, orderGapMultiplier), ctx)
    run.rowsInserted = R.overwriteTable(scored, T.table(catalog, "Customer360.CustomerChurnFlag"))

    highRiskTable = T.table(catalog, "work.CustomerChurnHighRisk")
    highRiskCount = R.overwriteTable(C.splitHighRisk(spark.table(T.table(catalog, "Customer360.CustomerChurnFlag"))), highRiskTable)
    print(f"HighRiskCount={highRiskCount}")

    # Queue High Risk For Outreach (only when HighRiskCount > 0)
    if highRiskCount > 0:
        queueTable = T.table(catalog, "work.CustomerOutreachQueue")
        highRisk = R.readTable(spark, highRiskTable)
        emptyQueue = C.newOutreachQueueRows(highRisk.limit(0), highRisk.limit(0).withColumn("QueueReasonCode", F.lit("")))
        queue = R.readTableOrEmpty(spark, queueTable, emptyQueue)
        newRows = C.newOutreachQueueRows(highRisk, queue)
        if not spark.catalog.tableExists(queueTable):
            R.overwriteTable(newRows, queueTable)
        else:
            R.appendTable(newRows, queueTable)

    R.logRowCounts(spark, ctx, run, "Customer360.CustomerChurnFlag")
