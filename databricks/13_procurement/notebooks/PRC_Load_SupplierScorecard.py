# Databricks notebook source
# MAGIC %md
# MAGIC # PRC_Load_SupplierScorecard
# MAGIC Migrated from `ssis/13_procurement/PRC_Load_SupplierScorecard.dtsx` (WWI_Procurement).
# MAGIC
# MAGIC Builds supplier x region scorecard measures over `ScoringWindowDays` ending at BusinessDate,
# MAGIC applies regional weights from `etl.supplier_scoring_weight` (defaults 0.4/0.2/0.2/0.2), bands
# MAGIC suppliers A/B/C/D/NODATA, writes `silver.work_supplier_scorecard` and MERGEs
# MAGIC `gold.agg_supplier_performance` at (calendar_month, supplier_key, region_code) grain.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402

from procurement_lib import PROJECT_NAME  # noqa: E402
from procurement_lib import delta_io  # noqa: E402
from procurement_lib import purchase_spend as ps  # noqa: E402
from procurement_lib import supplier_scorecard as sc  # noqa: E402
from procurement_lib.common import parseInt  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "PRC_Load_SupplierScorecard"
STEP_NAME = "Procurement Mart"

for name, default in (("BatchId", "0"), ("BusinessDate", ""), ("ReloadFullHistory", "False"), ("EnvironmentCode", "DEV"),
                      ("RestartFromStep", ""), ("catalog", ""), ("ScoringWindowDays", "90"), ("MinimumOrdersForScore", "5")):
    dbutils.widgets.text(name, default)

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
scoringWindowDays = parseInt(dbutils.widgets.get("ScoringWindowDays"), 90)
minimumOrdersForScore = parseInt(dbutils.widgets.get("MinimumOrdersForScore"), 5)

stgSupplier = naming.table(catalog, "silver", "stg_supplier")
stgPurchaseOrder = naming.table(catalog, "silver", "stg_purchase_order")
stgPurchaseOrderLine = naming.table(catalog, "silver", "stg_purchase_order_line")
stgReceipt = naming.table(catalog, "silver", "stg_receipt")
stgApInvoiceLine = naming.table(catalog, "silver", "stg_ap_invoice_line")
etlSupplierScoringWeight = naming.table(catalog, "etl", "supplier_scoring_weight")
dimSupplier = naming.table(catalog, "gold", "dim_supplier")
workSupplierScorecard = naming.table(catalog, "silver", "work_supplier_scorecard")
aggSupplierPerformance = naming.table(catalog, "gold", "agg_supplier_performance")

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName=STEP_NAME) as run:
    # "Build Scorecard Measures" (Execute SQL into work.SupplierScorecard)
    measures = sc.buildScorecardMeasures(
        spark.table(stgSupplier), spark.table(stgPurchaseOrder), spark.table(stgPurchaseOrderLine),
        spark.table(stgReceipt), spark.table(stgApInvoiceLine), businessDate, scoringWindowDays,
    ).cache()
    delta_io.overwriteTable(measures, workSupplierScorecard)
    rowsRead = delta_io.countWhere(spark, workSupplierScorecard)

    # Data flow "Score Suppliers": Lookup Scoring Weights -> Derive Percentages -> Derive Score Band -> aggregate
    scored = sc.scoreSuppliers(measures, spark.table(etlSupplierScoringWeight), minimumOrdersForScore)
    supplierKeys = ps.currentSupplierKeys(spark.table(dimSupplier))
    aggRows = sc.toAggSupplierPerformance(scored, supplierKeys, businessDate, batchId).cache()
    rowsInserted = aggRows.count()
    delta_io.mergeInto(spark, aggRows, aggSupplierPerformance, keyColumns=["calendar_month", "supplier_key", "region_code"])

    # "Count Unscored Suppliers"
    unscoredSupplierCount = sc.countUnscoredSuppliers(measures, minimumOrdersForScore)
    print("ScoredSupplierCount=%s UnscoredSupplierCount=%s" % (rowsInserted, unscoredSupplierCount))

    # "Log Row Counts"
    control.logRowCount(spark, catalog, run.packageExecutionId, "Aggregate.Supplier Performance",
                        sourceRowCount=rowsRead, targetRowCount=rowsInserted, insertRowCount=rowsInserted, rejectRowCount=0)
    run.rowsRead = rowsRead
    run.rowsInserted = rowsInserted
    run.rowsUpdated = 0
    run.rowsRejected = 0
    aggRows.unpersist()
    measures.unpersist()

dbutils.notebook.exit("%s: rowsRead=%s rowsInserted=%s unscored=%s" % (PACKAGE_NAME, rowsRead, rowsInserted, unscoredSupplierCount))
