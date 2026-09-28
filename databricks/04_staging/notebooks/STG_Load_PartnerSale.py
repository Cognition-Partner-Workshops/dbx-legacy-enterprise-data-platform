# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_PartnerSale
# MAGIC Legacy: `ssis/04_staging/STG_Load_PartnerSale.dtsx` (WWI_Staging). Parse and type the partner sales flat-file feed (raw.FilePartnerSales, current batch): ISO/EU/US date and decimal-comma handling, country-by-name and PARTNER_CUSTOMER crosswalk lookups; unparsable rows to err.RejectedFileRow. `stg.usp_NormalizeCustomer` is applied by STG_Load_Customer.
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

run = StagingRun(spark, dbutils, "STG_Load_PartnerSale", sourceSystemCode="FILE_PARTNER", objectName="stg.PartnerSale", watermark=True, jobParams=p)

PARTNER_COLUMNS = ["PartnerCode", "PartnerOrderRef", "CustomerRef", "CustomerCode", "ItemRef", "SaleDate", "Quantity", "GrossAmount", "PartnerCurrencyCode", "CountryName", "CountryCode", "RegionCode", "SourceFileName", "SourceRowNumber"]


def rejectRow(df, code, reason):
    return run.reject(
        df, "err_rejected_file_row", code, reason, businessKeyColumn="PartnerOrderRef",
        keyColumns={"SourceFileName": "SourceFileName", "SourceRowNumber": "SourceRowNumber", "RawRowText": F.concat_ws(",", "PartnerCode", "PartnerOrderRef", "SaleDateText", "CustomerRef", "ItemRef", "QuantityText", "AmountText", "CurrencyText", "CountryText"), "DecimalSeparatorUsed": F.when(F.col("AmountText").contains(","), F.lit(",")).otherwise(F.lit("."))},
    )


def load(run):
    src = run.bronze("raw_file_partner_sales")
    run.countRead(src)
    run.recordWatermarkTo(src, "LoadedAtUtc")
    derived = T.derivePartnerSale(src)
    country = refs.country(run, activeOnly=False).select(F.upper(F.col("CountryName")).alias("CountryName"), "CountryCode", "RegionCode")
    withCountry, unknownCountry = lookupLeft(derived, country, ["CountryName"], ["CountryCode", "RegionCode"])
    rejectRow(unknownCountry, "UNKNOWN_COUNTRY", "Country name not in ref.Country")
    crosswalk = refs.codeCrosswalk(run, "PARTNER_CUSTOMER").select(F.col("SourceCodeValue").alias("CustomerRef"), F.col("ConformedCodeValue").alias("CustomerCode")).dropDuplicates(["CustomerRef"])
    withCustomer, unknownCustomer = lookupLeft(withCountry, crosswalk, ["CustomerRef"], ["CustomerCode"])
    rejectRow(unknownCustomer, "UNKNOWN_CUSTOMER", "Partner customer reference has no PARTNER_CUSTOMER crosswalk")
    valid, badAmount, missingRef = T.splitPartnerSale(withCustomer)
    rejectRow(badAmount, "UNPARSABLE_AMOUNT", "Gross amount did not parse to a positive decimal")
    rejectRow(missingRef, "MISSING_REFERENCE", "Partner order reference is blank")
    run.rebuildForBatch(valid.select(*PARTNER_COLUMNS), "stg_partner_sale")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
