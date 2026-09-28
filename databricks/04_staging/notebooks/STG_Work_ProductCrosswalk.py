# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Work_ProductCrosswalk
# MAGIC Legacy: `ssis/04_staging/STG_Work_ProductCrosswalk.dtsx` (WWI_Staging). Rebuild work.ProductCrosswalk for the batch: union of the Oracle product master and OLTP stock items (data flow GTIN / NAME survivorship) resolved through the `work.usp_BuildProductCrosswalk` passes MANUAL_XREF (100) > BARCODE (95, ambiguous on multi-hit) > NAME token overlap >= 80% within brand > UNMATCHED; unmatchable candidates go to err.RejectedLookupFailure.
# MAGIC
# MAGIC Control framework: `dbx_etl_common` (session 00) via `stg_common.loader.StagingRun`
# MAGIC (`control.packageRun` / `getWatermark` / `setWatermark` / `logRejectedRecordSet` / `logRowCount`).

# COMMAND ----------

import os
import sys

from pyspark.sql import Window  # noqa: F401
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402,F401
from stg_common import expressions as X  # noqa: E402,F401
from stg_common import refs  # noqa: E402,F401
from stg_common import transforms as T  # noqa: E402,F401
from stg_common import work as W  # noqa: E402,F401
from stg_common.loader import PHASE_STAGE_WORK, StagingRun, lookupIgnore, lookupLeft, splitByCondition  # noqa: E402,F401

p = params.getJobParams(dbutils)  # noqa: F821  (batchId, businessDate, reloadFullHistory, environmentCode, restartFromStep, catalog)

# COMMAND ----------

run = StagingRun(spark, dbutils, "STG_Work_ProductCrosswalk", sourceSystemCode="ORA_ERP", objectName="work.ProductCrosswalk", phase=PHASE_STAGE_WORK, jobParams=p)

CROSSWALK_COLUMNS = [
    "ErpProductCode", "ErpProductBusinessKey", "SourceSystemCode", "SourceItemCode", "OltpStockItemId", "StockItemId", "ProductKey", "StockItemBusinessKey", "PartnerProductCode",
    "Barcode", "MatchKey", "MatchRuleCode", "MatchMethodCode", "MatchConfidence", "NormalizedName", "NameTokenOverlapPercent", "IsAmbiguous", "CandidateCount", "ResolvedFlag", "ReviewedByName",
]


def load(run):
    products = run.silver("stg_product")
    run.countRead(products)
    stockItems = run.silver("stg_stock_item")
    erpSide = W.shapeErpCrosswalkSide(products)
    oltpSide = W.shapeOltpCrosswalkSide(stockItems)
    feed = erpSide.unionByName(oltpSide, allowMissingColumns=True)
    run.rebuildForBatch(feed.select("SourceSystemCode", "SourceItemCode", "MatchKey", "MatchRuleCode", "StockItemId"), "work_product_crosswalk_feed")
    preferred, nameMatch, unmatchable = W.splitCrosswalkCandidates(feed)
    run.rejectLookupFailures(unmatchable, "Product Crosswalk", "MatchKey", "SourceItemCode", "Neither a GTIN nor a name of at least 8 characters", rejectStage="Transform")

    manualXref = run.silverIfExists("ref_source_key_crosswalk")
    catalog = run.bronze("raw_file_supplier_catalog") if spark.catalog.tableExists(run.bronzeTable("raw_file_supplier_catalog")) else None
    resolved = W.resolveProductCrosswalk(erpSide, oltpSide, manualXref=manualXref, supplierCatalog=catalog)
    unresolved = resolved.where(F.col("MatchMethodCode") == "UNMATCHED")
    run.rejectLookupFailures(unresolved, "Stock Item", "NormalizedName", "ErpProductCode", "No OLTP stock item matched by xref, barcode or name", rejectStage="Transform")
    run.rebuildForBatch(resolved.select(*CROSSWALK_COLUMNS), "work_product_crosswalk")
    run.counters.rowsUpdated += oltpSide.count()


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
