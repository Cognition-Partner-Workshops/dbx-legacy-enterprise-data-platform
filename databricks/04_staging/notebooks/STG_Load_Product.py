# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_Product
# MAGIC Legacy: `ssis/04_staging/STG_Load_Product.dtsx` (WWI_Staging). Conform raw.OracleProductMaster into stg.Product: normalise UoM (eaches per pack), convert weights to KG, classify sellable / discontinued / priceless.
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

run = StagingRun(spark, dbutils, "STG_Load_Product", sourceSystemCode="ORA_ERP", objectName="stg.Product", jobParams=p)

PRODUCT_COLUMNS = [
    "ProductCode", "ProductDescription", "ProductFamilyCode", "BaseUomCode", "PackQuantity", "EachesPerPack", "NetWeightKg",
    "WeightUomCode", "ListPriceAmount", "ListPriceCurrencyCode", "HazardousFlag", "DiscontinuedFlag", "ChangeHash",
]


def rejectProduct(df, code, reason, failedColumn):
    return run.reject(
        df, "err_rejected_product", code, reason, businessKeyColumn="ProductCode",
        keyColumns={"ProductCode": "ProductCode", "SourceProductId": F.col("PROD_CODE"), "ProductName": "ProductDescription", "FailedColumnName": F.lit(failedColumn), "FailedValue": failedColumn},
    )


def load(run):
    src = run.bronze("raw_oracle_product_master")
    run.countRead(src)
    cleansed = T.cleanseProduct(src)
    uom = refs.uomConversion(run)
    eaches = uom.where(F.col("ToUomCode") == "EA").select(F.col("FromUomCode").alias("BaseUomCode"), "ConversionFactor")
    withEaches = lookupIgnore(cleansed, eaches, ["BaseUomCode"], ["ConversionFactor"])
    kg = uom.where(F.col("ToUomCode") == "KG").select(F.col("FromUomCode").alias("WeightUomCode"), F.col("ConversionFactor").alias("WeightFactorKg"))
    matched, unknownWeightUom = lookupLeft(withEaches, kg, ["WeightUomCode"], ["WeightFactorKg"])
    rejectProduct(unknownWeightUom, "UNKNOWN_WEIGHT_UOM", "Weight UoM has no conversion to KG", "WeightUomCode")
    converted = T.convertProductUnits(matched)
    sellable, discontinued, priceless = T.splitProduct(converted)
    rejectProduct(priceless, "NO_LIST_PRICE", "Active product without a positive list price", "ListPriceAmount")
    run.truncateReload(sellable.unionByName(discontinued).select(*PRODUCT_COLUMNS), "stg_product")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
