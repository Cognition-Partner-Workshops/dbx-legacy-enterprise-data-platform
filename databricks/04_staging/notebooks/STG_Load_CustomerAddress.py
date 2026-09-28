# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_CustomerAddress
# MAGIC Legacy: `ssis/04_staging/STG_Load_CustomerAddress.dtsx` (WWI_Staging). Standardise raw.OracleCustomerAddress per region (NA USPS / EU country-prefixed postal / APAC digits-only), resolve stg.Geography and validate the postal code; three regional Data Flows become one loop. `stg.usp_NormalizeAddress` casing is folded into the derived columns.
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

run = StagingRun(spark, dbutils, "STG_Load_CustomerAddress", sourceSystemCode="ORA_ERP", objectName="stg.CustomerAddress", jobParams=p)

ADDRESS_COLUMNS = [
    "CustomerCode", "AddressTypeCode", "AddressLine1", "AddressLine2", "CityName", "StateProvinceCode", "PostalCode",
    "PostalStandard", "CountryCode", "RegionCode", "GeographyKey", "EffectiveFromDate",
]


def load(run):
    src = run.bronze("raw_oracle_customer_address")
    run.countRead(src)
    geography = run.silver("stg_geography").select("GeographyKey", "CityName", "StateProvinceCode", "CountryCode", "RegionCode")
    outputs = []
    for regionCode in ("NA", "EU", "APAC"):
        regional = src.where(F.upper(F.trim(F.col("REGION_CD"))) == regionCode)
        standardized = T.standardizeAddress(regional, regionCode)
        geo = geography.where(F.col("RegionCode") == regionCode).drop("RegionCode", "CountryCode")
        matched, unknownGeo = lookupLeft(standardized, geo, ["CityName", "StateProvinceCode"], ["GeographyKey"])
        run.rejectLookupFailures(
            unknownGeo.withColumn("CityStateKey", F.concat_ws("|", "CityName", "StateProvinceCode")),
            "%s Geography" % regionCode, "CityStateKey", "CustomerCode", "City/state not found in stg.Geography (%s)" % regionCode,
        )
        validPostal, invalidPostal = splitByCondition(matched, T.postalCodeValid(regionCode))
        run.reject(
            invalidPostal, "err_rejected_customer", "INVALID_POSTAL", "Postal code fails the %s format rule" % regionCode,
            businessKeyColumn="CustomerCode",
            keyColumns={"CustomerCode": "CustomerCode", "SourceCustomerId": F.col("CUST_CODE"), "RegionCode": "RegionCode", "FailedColumnName": F.lit("PostalCode"), "FailedValue": "PostalCode"},
        )
        outputs.append(validPostal.select(*ADDRESS_COLUMNS))
    allRegions = outputs[0]
    for other in outputs[1:]:
        allRegions = allRegions.unionByName(other)
    run.truncateReload(allRegions, "stg_customer_address")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
