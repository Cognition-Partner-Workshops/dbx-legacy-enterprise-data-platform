# Databricks notebook source
# MAGIC %md
# MAGIC # C360_Build_RollingMetrics
# MAGIC Migrated from `ssis/14_customer_360/C360_Build_RollingMetrics.dtsx` (project `WWI_Customer360`).
# MAGIC Aggregates the rolling sales window per customer, realigns APAC to the latest completed 4-4-5 period, derives ratios / activity status / orders-per-month and assigns regional RFM deciles into `Customer360.CustomerRollingMetric` -> `gold.c360_customer_rolling_metric`.
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
from c360_lib import rolling as RM  # noqa: E402

PACKAGE_NAME = "C360_Build_RollingMetrics"
STEP_NAME = "Customer 360 Build"

# COMMAND ----------

ctx = R.getJobContext(dbutils)
windowMonths = int(R.widget(dbutils, "WindowMonths", str(RM.DEFAULT_WINDOW_MONTHS)))
inactiveThresholdDays = int(R.widget(dbutils, "InactiveThresholdDays", str(RM.DEFAULT_INACTIVE_THRESHOLD_DAYS)))
catalog = ctx.catalog
asOf = ctx.businessDate

if R.shouldSkipForRestart(ctx, PACKAGE_NAME):
    dbutils.notebook.exit(f"SKIPPED (RestartFromStep={ctx.restartFromStep})")

# COMMAND ----------

with R.packageLifecycle(spark, ctx, PACKAGE_NAME, STEP_NAME) as run:
    factSale = R.readTable(spark, T.table(catalog, "Fact.Sale"))
    dimCustomer = R.readTable(spark, T.table(catalog, "Dimension.Customer"))

    # Truncate work_CustomerRollingMetric + Aggregate Rolling Window
    window = RM.aggregateRollingWindow(factSale, dimCustomer, asOf, windowMonths)

    # Realign APAC Window To 445
    fiscal = R.readTable(spark, T.table(catalog, "stg.FiscalCalendar445Period"), required=False)
    period = RM.latestCompleted445Period(fiscal, asOf) if fiscal is not None else None
    if period is None:
        R.logWarning(spark, ctx, run, PACKAGE_NAME, "Realign APAC Window To 445",
                     "stg.FiscalCalendar445Period missing or has no completed period; APAC window kept on calendar months")
    window = RM.realignApacWindow(window, period)
    workTable = T.table(catalog, "work.CustomerRollingMetric")
    run.rowsRead = R.overwriteTable(R.stamp(window, ctx), workTable)

    # Derive Rolling Metrics (data flow) + Assign RFM Deciles (post-load UPDATE)
    metrics = RM.deriveRollingMetrics(R.readTable(spark, workTable), windowMonths, inactiveThresholdDays)
    metrics = RM.assignRfmDeciles(metrics)
    target = T.table(catalog, "Customer360.CustomerRollingMetric")
    run.rowsInserted = R.overwriteTable(metrics, target)

    inactiveCustomerCount = spark.table(target).where(F.col("ActivityStatusCode") == "INACTIVE").count()
    print(f"MetricRowCount={run.rowsInserted} InactiveCustomerCount={inactiveCustomerCount}")

    R.logRowCounts(spark, ctx, run, "Customer360.CustomerRollingMetric")
