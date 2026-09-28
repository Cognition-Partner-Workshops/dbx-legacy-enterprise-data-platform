# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Work_InventoryPosition
# MAGIC Legacy: `ssis/04_staging/STG_Work_InventoryPosition.dtsx` (WWI_Staging). Rebuild work.InventoryPositionDaily over a rolling 90-day window: movements summed per item / warehouse / day (data flow) then rolled forward per `work.usp_BuildInventoryPositionDaily` (opening = prior closing, receipts / issues / adjustments / transfers, weighted receipt cost carry-forward, 28-day days-of-cover, negative-balance and roll-forward-break flags); outliers to err.RejectedConstraintViolation.
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

run = StagingRun(spark, dbutils, "STG_Work_InventoryPosition", sourceSystemCode="SQL_WWI", objectName="work.InventoryPositionDaily", phase=PHASE_STAGE_WORK, jobParams=p)

POSITION_COLUMNS = [
    "PositionDate", "StockItemId", "StockItemBusinessKey", "StockItemName", "BrandName", "PriceBandCode", "WarehouseCode", "OpeningQuantity", "ReceiptQuantity", "IssueQuantity",
    "AdjustmentQuantity", "TransferInQuantity", "TransferOutQuantity", "ClosingQuantity", "NetQuantity", "MovementCount", "MaxSingleMovement", "MinSingleMovement",
    "StockPositionCode", "HighChurnFlag", "ClosingValueUsd", "AverageUnitCostUsd", "DaysOfCoverEstimate", "NegativeBalanceFlag", "RollForwardBrokenFlag",
]
WINDOW_DAYS = 90
DAYS_OF_COVER_WINDOW = 28


def load(run):
    movements = run.silver("stg_stock_movement").where(F.col("MovementDate").cast("date") >= F.date_sub(F.lit(p["businessDate"]).cast("date"), WINDOW_DAYS))
    run.countRead(movements)
    daily = W.aggregateDailyPosition(movements)
    items = run.silver("stg_stock_item").select("StockItemId", "StockItemName", "BrandName", "PriceBandCode")
    withItem, unknownItem = lookupLeft(daily, items, ["StockItemId"], ["StockItemName", "BrandName", "PriceBandCode"])
    run.rejectLookupFailures(unknownItem, "Stock Item", "StockItemId", "StockItemId", "Movement stock item not in stg.StockItem", rejectStage="Transform")
    classified = W.classifyPosition(withItem)
    plausible, outlier = splitByCondition(classified, (F.col("NetQuantity") > -1000000) & (F.col("NetQuantity") < 1000000))
    keyed = outlier.withColumn("PositionKey", F.concat_ws("|", "StockItemId", "WarehouseCode", F.col("PositionDate").cast("string")))
    run.rejectConstraint(keyed, "work.InventoryPositionDaily", "CK_workInventoryPosition_Plausible", "PositionKey", "NetQuantity", "POSITION_OUTLIER", "Net daily quantity outside +/- 1,000,000")
    rolled = W.rollForwardInventory(plausible, sourceSystemCode=run.sourceSystemCode, daysOfCoverWindow=DAYS_OF_COVER_WINDOW)
    inserted = run.rebuildForBatch(rolled.select(*POSITION_COLUMNS), "work_inventory_position_daily")
    broken = rolled.where((F.col("RollForwardBrokenFlag") == F.lit(True)) | (F.col("NegativeBalanceFlag") == F.lit(True))).count()
    run.logRowCount(objectName="work.InventoryPositionDaily", sourceRowCount=run.counters.rowsRead, targetRowCount=inserted, insertRowCount=inserted, rejectRowCount=broken)


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
