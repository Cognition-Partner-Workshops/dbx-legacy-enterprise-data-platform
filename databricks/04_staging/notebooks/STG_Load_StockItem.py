# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_StockItem
# MAGIC Legacy: `ssis/04_staging/STG_Load_StockItem.dtsx` (WWI_Staging). Conform raw.SqlStockItem into stg.StockItem: brand/size defaults, weight to grams, price banding, chiller flag. `stg.usp_CleanStringBatch` trimming is folded into the derived columns.
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

run = StagingRun(spark, dbutils, "STG_Load_StockItem", sourceSystemCode="SQL_WWI", objectName="stg.StockItem", jobParams=p)

STOCK_ITEM_COLUMNS = [
    "StockItemId", "StockItemName", "BrandName", "SizeText", "Barcode", "SupplierId", "UnitPriceAmount", "TypicalWeightGrams",
    "PriceBandCode", "ChillerFlag", "MarketingText", "ChangeHash",
]


def load(run):
    src = run.bronze("raw_sql_stock_item", currentBatchOnly=False)
    run.countRead(src)
    cleansed = T.cleanseStockItem(src)
    valid, missingName, negativePrice = T.splitStockItem(cleansed)
    run.rejectConstraint(missingName, "stg.StockItem", "CK_stgStockItem_Name", "StockItemId", "StockItemName", "MISSING_NAME", "Stock item name shorter than 3 characters")
    run.rejectConstraint(negativePrice, "stg.StockItem", "CK_stgStockItem_UnitPrice", "StockItemId", "UnitPriceAmount", "NEGATIVE_PRICE", "Unit price is negative")
    run.truncateReload(valid.select(*STOCK_ITEM_COLUMNS), "stg_stock_item")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
