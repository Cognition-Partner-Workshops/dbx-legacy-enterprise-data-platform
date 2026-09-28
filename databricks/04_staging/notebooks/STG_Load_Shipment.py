# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_Shipment
# MAGIC Legacy: `ssis/04_staging/STG_Load_Shipment.dtsx` (WWI_Staging). Watermarked (DespatchedWhen) shipment header conformance (carrier / tracking / postal standardisation, weight to grams, transit days, ref.Carrier lookup) and line weight rebasing; `stg.usp_AppendIncremental_Shipment` per-batch append is the keyed merge.
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

run = StagingRun(spark, dbutils, "STG_Load_Shipment", sourceSystemCode="SQL_WWI", objectName="stg.Shipment", watermark=True, jobParams=p)

SHIPMENT_COLUMNS = ["ShipmentId", "InvoiceId", "CarrierCode", "CarrierName", "ServiceLevelCode", "TrackingNumber", "DestinationCountryCode", "DestinationPostalCode", "GrossWeightGrams", "DeliveredFlag", "TransitDays", "DespatchedWhen", "DeliveredWhen"]
LINE_COLUMNS = ["ShipmentLineId", "ShipmentId", "StockItemId", "QuantityShipped", "LineWeightGrams"]


def load(run):
    shipments = run.bronzeWatermarked("raw_sql_shipment", "DespatchedWhen")
    run.countRead(shipments)
    standardized = T.standardizeShipment(shipments)
    carriers = run.silver("ref_carrier").where(F.col("IsActive") == F.lit(True)).select("CarrierCode", "CarrierName", "ServiceLevelCode")
    matched, unknownCarrier = lookupLeft(standardized, carriers, ["CarrierCode"], ["CarrierName", "ServiceLevelCode"])
    run.rejectLookupFailures(unknownCarrier, "Carrier", "CarrierCode", "ShipmentId", "Carrier not in ref.Carrier")
    run.mergeByKey(matched.select(*SHIPMENT_COLUMNS), "stg_shipment", ["ShipmentId"])

    lines = run.bronze("raw_sql_shipment_line", currentBatchOnly=False).join(shipments.select("ShipmentID").distinct(), "ShipmentID")
    rebased = T.rebaseShipmentLine(lines)
    valid, invalid = splitByCondition(rebased, F.col("ShipmentLineId").isNotNull() & F.col("QuantityShipped").isNotNull())
    run.rejectConstraint(invalid, "stg.ShipmentLine", "CK_stgShipmentLine_Source", "ShipmentLineId", "QuantityShipped", "SOURCE_ERROR", "Shipment line failed type conversion")
    lineCount = run.mergeByKey(valid.select(*LINE_COLUMNS), "stg_shipment_line", ["ShipmentLineId"])
    run.counters.rowsUpdated += lineCount
    run.logRowCount(objectName="stg.ShipmentLine", insertRowCount=lineCount)


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
