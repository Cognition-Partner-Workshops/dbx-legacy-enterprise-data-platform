# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_Customer
# MAGIC Legacy: `ssis/04_staging/STG_Load_Customer.dtsx` (WWI_Staging). Conform raw.OracleCustomerMaster into stg.Customer: trim/case-fold codes, regional consent and retention rules, country reference, change hash; failures to err.RejectedCustomer. Post-flow `stg.usp_NormalizeCustomer` is folded into `T.deriveCustomerHash`.
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

run = StagingRun(spark, dbutils, "STG_Load_Customer", sourceSystemCode="ORA_ERP", objectName="stg.Customer", jobParams=p)

CUSTOMER_COLUMNS = [
    "CustomerCode", "CustomerName", "TradingName", "CustomerNameStandardized", "CustomerClassCode", "CreditStatusCode",
    "CountryCode", "CountryName", "IsoNumeric", "RegionCode", "TaxRegistrationNumber", "MarketingConsentFlag",
    "RetentionMonths", "RetentionExpiryDate", "CreditLimitAmount", "CreditCurrencyCode", "SourceCreatedDate",
    "SourceModifiedDate", "ChangeHash", "RowHash",
]


def rejectCustomer(df, code, reason):
    return run.reject(
        df, "err_rejected_customer", code, reason, businessKeyColumn="CustomerCode",
        keyColumns={"CustomerCode": "CustomerCode", "SourceCustomerId": F.col("CUST_CODE"), "CustomerName": "CustomerName", "RegionCode": "RegionCode"},
    )


def load(run):
    src = run.bronze("raw_oracle_customer_master").where(X.nullIfBlank("CUST_CODE").isNotNull())
    run.countRead(src)
    cleansed = T.cleanseCustomer(src)
    # Lookup Country (Full Cache) -> no match: ERR Customer Unknown Country
    # ref.Country carries CountryCodeIso3 / LocalCurrencyCode where the package expected IsoNumeric / DefaultCurrencyCode
    country = refs.country(run).select("CountryCode", "CountryName", F.col("CountryCodeIso3").alias("IsoNumeric"), F.col("LocalCurrencyCode").alias("DefaultCurrencyCode"))
    matched, unknownCountry = lookupLeft(cleansed, country, ["CountryCode"], ["CountryName", "IsoNumeric", "DefaultCurrencyCode"])
    rejectCustomer(unknownCountry, "UNKNOWN_COUNTRY", "Country code not found in ref.Country")
    hashed = T.deriveCustomerHash(matched)
    valid, missingName, malformed = T.splitCustomer(hashed)
    rejectCustomer(missingName, "MISSING_NAME", "Customer name is blank")
    rejectCustomer(malformed, "MALFORMED_CODE", "Customer code shorter than 4 characters")
    run.truncateReload(valid.select(*CUSTOMER_COLUMNS), "stg_customer")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
