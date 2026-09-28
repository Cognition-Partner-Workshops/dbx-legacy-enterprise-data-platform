# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_StockMovement
# MAGIC Legacy: `ssis/04_staging/STG_Load_StockMovement.dtsx` (WWI_Staging). Watermarked (TransactionOccurredWhen) stock movement conformance: ref.TransactionType sign, UoM to eaches (ignore miss = factor 1), counterparty typing, zero-movement screening; `stg.usp_AppendIncremental_StockMovement` per-batch append is the keyed merge.
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

run = StagingRun(spark, dbutils, "STG_Load_StockMovement", sourceSystemCode="SQL_WWI", objectName="stg.StockMovement", watermark=True, jobParams=p)

MOVEMENT_COLUMNS = ["StockMovementId", "StockItemId", "TransactionTypeCode", "TransactionTypeName", "MovementSign", "CounterpartyTypeCode", "UomCode", "MovementConversionFactor", "SignedQuantity", "WarehouseCode", "UnitCostAmount", "MovementDate", "TransactionOccurredWhen"]


def load(run):
    src = run.bronzeWatermarked("raw_sql_stock_movement", "TransactionOccurredWhen")
    run.countRead(src)
    types = run.silver("ref_transaction_type").select("TransactionTypeCode", "TransactionTypeName", "MovementSign")
    withType, unknownType = lookupLeft(src, types, ["TransactionTypeCode"], ["TransactionTypeName", "MovementSign"])
    run.rejectLookupFailures(unknownType, "Transaction Type", "TransactionTypeCode", "StockItemTransactionID", "Transaction type not in ref.TransactionType")
    uom = refs.uomConversion(run).where(F.col("ToUomCode") == "EA").select(F.col("FromUomCode").alias("UomCode"), F.col("ConversionFactor").alias("MovementConversionFactor"))
    withUom = lookupIgnore(withType, uom, ["UomCode"], ["MovementConversionFactor"])
    signed = T.applyMovementSign(withUom).withColumn("WarehouseCode", F.coalesce(F.upper(F.trim(F.col("WarehouseCode"))), F.lit("UNKNOWN"))).withColumn("UnitCostAmount", F.col("UnitCost").cast("decimal(18,4)"))
    valid, zero = splitByCondition(signed, F.col("SignedQuantity") != 0)
    run.rejectConstraint(zero, "stg.StockMovement", "CK_stgStockMovement_Quantity", "StockMovementId", "SignedQuantity", "ZERO_MOVEMENT", "Signed quantity is zero")
    run.mergeByKey(valid.select(*MOVEMENT_COLUMNS), "stg_stock_movement", ["StockMovementId"])


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
