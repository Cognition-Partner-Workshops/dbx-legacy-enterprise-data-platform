# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_PromotionAndTerritory
# MAGIC Legacy: `ssis/04_staging/STG_Load_PromotionAndTerritory.dtsx` (WWI_Staging). Two Data Flows over raw.SqlOrder: promotions (typing, overlapping-window survivorship, discount screening) into stg.Promotion and sales territories (hierarchy conformance, country lookup) into stg.SalesTerritory. `stg.usp_TranslateSourceCodes stg.Promotion` = `refs.translateSourceCodes`.
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

run = StagingRun(spark, dbutils, "STG_Load_PromotionAndTerritory", sourceSystemCode="SQL_WWI", objectName="stg.Promotion", jobParams=p)

PROMOTION_COLUMNS = ["PromotionCode", "PromotionName", "PromotionTypeCode", "ConformedPromotionTypeCode", "RegionCode", "DiscountPercent", "DiscountAmount", "ValidFromDate", "ValidToDate"]
TERRITORY_COLUMNS = ["SalesTerritoryCode", "SalesTerritoryName", "RegionCode", "CountryCode", "CountryName", "ParentTerritoryCode"]


def load(run):
    orders = run.bronze("raw_sql_order", currentBatchOnly=False)
    # raw.SqlOrder INNER JOIN raw.SqlPromotion / raw.SqlSalesTerritory (unscripted OLTP code extracts)
    promotionRows = run.bronze("raw_sql_promotion", currentBatchOnly=False).select("PromotionCode", "PromotionName", "PromotionTypeCode", "DiscountPercent", "DiscountAmount", "RegionCode", "ValidFrom", "ValidTo")
    promotions = orders.select("PromotionCode").where(F.col("PromotionCode").isNotNull()).dropDuplicates().join(promotionRows, "PromotionCode", "inner").dropDuplicates()
    run.countRead(promotions)
    typed = T.typePromotions(promotions).withColumn("ValidFromDate", F.col("ValidFrom").cast("timestamp"))
    # Sort Promotions By Validity (PromotionCode, ValidToDate) with duplicate removal: the longest-lived window survives
    survivors = T.survivorshipDedupe(typed, ["PromotionCode"], [F.col("ValidToDate").desc()])
    valid, outlier = splitByCondition(survivors, F.col("DiscountPercent").between(0, 90))
    run.rejectConstraint(outlier, "stg.Promotion", "CK_stgPromotion_DiscountPercent", "PromotionCode", "DiscountPercent", "DISCOUNT_OUTLIER", "Discount percent outside 0..90")
    translated = refs.translateSourceCodes(run, valid, "PROMOTION_TYPE", "PromotionTypeCode", "ConformedPromotionTypeCode", "PromotionCode", unmatchedAction="LEAVE")
    run.truncateReload(translated.select(*PROMOTION_COLUMNS), "stg_promotion")

    territoryRows = run.bronze("raw_sql_sales_territory", currentBatchOnly=False).select("SalesTerritoryCode", "SalesTerritoryName", "RegionCode", "CountryCode", "ParentTerritoryCode")
    territories = orders.select("SalesTerritoryCode").where(F.col("SalesTerritoryCode").isNotNull()).dropDuplicates().join(territoryRows, "SalesTerritoryCode", "inner").dropDuplicates()
    run.countRead(territories)
    standardized = T.standardizeTerritory(territories)
    country = refs.country(run, activeOnly=False).select("CountryCode", "CountryName")
    matched, unknownCountry = lookupLeft(standardized, country, ["CountryCode"], ["CountryName"])
    run.rejectLookupFailures(unknownCountry, "Territory Country", "CountryCode", "SalesTerritoryCode", "Territory country not found in ref.Country", objectName="stg.SalesTerritory")
    run.truncateReload(matched.select(*TERRITORY_COLUMNS), "stg_sales_territory")
    run.logRowCount(objectName="stg.SalesTerritory")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
