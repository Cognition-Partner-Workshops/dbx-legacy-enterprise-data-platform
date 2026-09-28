# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_Geography
# MAGIC Legacy: `ssis/04_staging/STG_Load_Geography.dtsx` (WWI_Staging). Conform raw.OracleGeography into stg.Geography: coordinate validation, sales-territory lookup, change hash; `stg.usp_TruncateAndReload_Geography` hierarchy rebuild is the truncate/reload write with a surrogate GeographyKey.
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

run = StagingRun(spark, dbutils, "STG_Load_Geography", sourceSystemCode="ORA_ERP", objectName="stg.Geography", jobParams=p)

GEOGRAPHY_COLUMNS = [
    "GeographyKey", "GeographyCode", "CityName", "StateProvinceCode", "StateProvinceName", "CountryCode", "RegionCode",
    "Latitude", "Longitude", "PopulationCount", "SalesTerritoryCode", "SalesTerritoryName", "TerritoryRegionCode", "CityStateKey", "ChangeHash",
]


def load(run):
    src = run.bronze("raw_oracle_geography")
    run.countRead(src)
    cleansed = T.cleanseGeography(src)
    territory = run.silver("stg_sales_territory").select("SalesTerritoryCode", "SalesTerritoryName", F.col("RegionCode").alias("TerritoryRegionCode"))
    matched, unknownTerritory = lookupLeft(cleansed, territory, ["SalesTerritoryCode"], ["SalesTerritoryName", "TerritoryRegionCode"])
    run.rejectLookupFailures(unknownTerritory, "Sales Territory", "SalesTerritoryCode", "GeographyCode", "Sales territory not found in stg.SalesTerritory")
    plausible, outOfRange = splitByCondition(matched, T.coordinatesPlausible())
    run.rejectConstraint(
        outOfRange.withColumn("Coordinates", F.concat_ws(",", "Latitude", "Longitude")),
        "stg.Geography", "CK_stgGeography_Coordinates", "GeographyCode", "Coordinates", "COORDS_OUT_OF_RANGE", "Latitude/longitude outside the plausible range",
    )
    keyed = plausible.withColumn("GeographyKey", F.row_number().over(Window.orderBy("RegionCode", "CountryCode", "StateProvinceCode", "CityName", "GeographyCode")))
    run.truncateReload(keyed.select(*GEOGRAPHY_COLUMNS), "stg_geography")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
