# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_DailySalesSnapshot
# MAGIC Port of `ssis/08_facts/FACT_Load_DailySalesSnapshot.dtsx` (`build_fact_packages.py::build_fact_load_daily_sales_snapshot`).
# MAGIC
# MAGIC * Periodic snapshot: `silver.stg_daily_sales_snapshot` rows for `SnapshotDate = BusinessDate` replace that date in `gold.fact_daily_sales_snapshot` (`replaceWhere` on the `snapshot_date_key` partition; legacy deleted by `Invoice Date Key` = snapshot date, which is the same day in the staging feed).
# MAGIC * The staging feed already carries warehouse surrogate keys (`CustomerKey`, `StockItemKey`, `InvoiceDateKey` yyyymmdd) so no dimension lookups are re-done; missing keys fall back to -1.
# MAGIC * Derived: margin %, average order value per line, commission accrual (regional rate), fiscal year / period on the calendar year (sales snapshot is corporate-calendar based), sales territory / channel keys are not carried by the feed (-2).
# MAGIC * Partitioned by `snapshot_date_key` so each nightly run swaps exactly one partition.

# COMMAND ----------

import os
import sys
from datetime import timedelta

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import fact_common as fc
import fact_rules as rules

# COMMAND ----------

PACKAGE_NAME = "FACT_Load_DailySalesSnapshot"
STEP_NAME = "Load Daily Sales Snapshot"
OBJECT_NAME = "Fact.Daily Sales Snapshot"
TARGET_TABLE = "fact_daily_sales_snapshot"
SNAPSHOT_DATE_COL = "snapshot_date_key"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
targetName = naming.table(catalog, "gold", TARGET_TABLE)
snapshotDate = p["businessDate"]
print("catalog=%s batchId=%s snapshotDate=%s target=%s" % (catalog, batchId, snapshotDate, targetName))

# COMMAND ----------

def readSnapshotSource(spark, catalog):
    return fc.readTable(spark, catalog, "silver", "stg_daily_sales_snapshot").where(F.col("SnapshotDate") == F.lit(snapshotDate))


def buildSnapshotRows(spark, catalog, df):
    net, gross, cost, margin, lines = F.col("NetAmount"), F.col("GrossAmount"), F.col("TotalCostAmount"), F.col("MarginAmount"), F.coalesce(F.col("InvoiceLineCount"), F.lit(1))
    invoiceDate = F.to_date(F.col("InvoiceDateKey").cast("string"), "yyyyMMdd")
    return df.select(
        F.col("SnapshotDate").alias("snapshot_date_key"),
        F.coalesce(invoiceDate, F.col("SnapshotDate")).alias("invoice_date_key"),
        F.coalesce(F.col("CustomerKey"), F.lit(fc.UNKNOWN_KEY)).cast("int").alias("customer_key"),
        F.coalesce(F.col("StockItemKey"), F.lit(fc.UNKNOWN_KEY)).cast("int").alias("stock_item_key"),
        F.lit(fc.NOT_APPLICABLE_KEY).alias("salesperson_key"), F.lit(fc.NOT_APPLICABLE_KEY).alias("sales_territory_key"), F.lit(fc.NOT_APPLICABLE_KEY).alias("sales_channel_key"),
        F.col("RegionCode").alias("region_code"), F.col("SourceSystemCode").alias("source_system_code"),
        F.col("Quantity").cast("decimal(18,4)").alias("quantity_sold"),
        lines.cast("int").alias("line_count"),
        rules.money(gross).alias("gross_sales_amount"), rules.money(F.coalesce(F.col("DiscountAmount"), F.lit(0))).alias("discount_amount"),
        rules.money(net).alias("net_sales_amount"), rules.money(F.coalesce(F.col("TaxAmount"), F.lit(0))).alias("tax_amount"),
        rules.money(cost).alias("cost_of_sales_amount"),
        rules.money(F.coalesce(margin, net - cost)).alias("gross_margin_amount"),
        F.coalesce(F.col("MarginPercent"), rules.marginPercent(F.coalesce(margin, net - cost), net)).cast(rules.PCT).alias("margin_percent"),
        rules.money(rules.safeDivide(net, lines)).alias("average_order_value"),
        rules.commissionAccrual(F.col("RegionCode"), net, F.coalesce(margin, net - cost)).alias("commission_accrued_amount"),
        rules.fiscalYear(F.col("SnapshotDate"), 1).alias("fiscal_year"), rules.fiscalPeriod(F.col("SnapshotDate"), 1).alias("fiscal_period"),
        F.lit(False).alias("restated_flag"),
        F.col("DqStatusCode").alias("dq_status_code"),
        fc.naturalKeyHash(F.col("SnapshotDate"), F.col("CustomerKey"), F.col("StockItemKey"), F.col("InvoiceDateKey")).alias("natural_key_hash"),
    )

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    stepId = control.startBatchStep(spark, catalog, batchId, STEP_NAME, 30, stepGroup="Facts")
    try:
        source = readSnapshotSource(spark, catalog)
        sourceCount = source.count()
        rows = buildSnapshotRows(spark, catalog, source)
        rows = fc.loadAuditColumns(rows, batchId, run.packageExecutionId)
        replaced = fc.replaceDateRange(spark, targetName, rows, SNAPSHOT_DATE_COL, snapshotDate, snapshotDate)
        pass
        run.rowsRead = sourceCount
        run.rowsInserted = replaced["inserted"]
        run.rowsDeleted = replaced["deleted"]
        fc.logFactRowCounts(spark, catalog, run.packageExecutionId, OBJECT_NAME, sourceCount, targetName, replaced)
        control.endBatchStep(spark, catalog, stepId, status="Succeeded")
    except Exception:
        control.endBatchStep(spark, catalog, stepId, status="Failed")
        raise
    print(replaced)
