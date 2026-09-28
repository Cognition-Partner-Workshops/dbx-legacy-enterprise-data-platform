# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Work_CustomerDedup
# MAGIC Legacy: `ssis/04_staging/STG_Work_CustomerDedup.dtsx` (WWI_Staging). Rebuild work.CustomerDedup / work.CustomerAddressStandardized for the batch: name|country|postal blocking (data flow) plus the `stg.usp_DeduplicateCustomer` survivorship rule set (EXACT_TAXNUM > NAME_POSTAL > NAME_FUZZY grouping; SourceRank + completeness + recency + EU consent scoring; ref.SourceKeyCrosswalk retirement).
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

run = StagingRun(spark, dbutils, "STG_Work_CustomerDedup", sourceSystemCode="ORA_ERP", objectName="work.CustomerDedup", phase=PHASE_STAGE_WORK, jobParams=p)

DEDUP_COLUMNS = [
    "DuplicateGroupId", "CandidateCustomerBusinessKey", "SourceSystemCode", "SourceCustomerId", "MatchKeyName", "MatchKeyPostal", "MatchKeyTaxNumber",
    "BlockingKey", "BlockMemberCount", "MatchRuleCode", "MatchScore", "SurvivorshipScore", "SourceRank", "AttributeCompleteness", "SourceModifiedDate",
    "IsSelectedSurvivor", "LosesToBusinessKey", "DecisionNote",
]
ADDRESS_COLUMNS = [
    "AddressBusinessKey", "CustomerBusinessKey", "RuleSetCode", "InputAddressLine1", "InputCityName", "InputPostalCode", "InputCountryCode",
    "OutputAddressLine1", "OutputCityName", "OutputStateProvinceCode", "OutputPostalCode", "OutputCountryCode", "StandardizationStatusCode", "GeographyBusinessKey",
]


def rejectCustomer(df, code, reason):
    return run.reject(df, "err_rejected_customer", code, reason, businessKeyColumn="CustomerCode", rejectStage="Transform",
                      keyColumns={"CustomerBusinessKey": X.sourceSystemKey(F.lit(run.sourceSystemCode), "CustomerCode"), "CustomerCode": "CustomerCode",
                                  "CustomerName": F.col("CustomerName") if "CustomerName" in df.columns else F.lit(None).cast("string"), "CountryCode": "CountryCode"})


def dedupe(run):
    customers = run.silver("stg_customer")
    billing = run.silver("stg_customer_address").where(F.col("AddressTypeCode") == "BILL").select("CustomerCode", "PostalCode", "AddressLine1", "CityName").dropDuplicates(["CustomerCode"])
    src = customers.join(billing, "CustomerCode", "left")
    run.countRead(src)
    keyed = W.buildCustomerBlockingKey(src)
    blocks = keyed.groupBy("BlockingKey").agg(F.count("CustomerCode").alias("BlockMemberCount"), F.min("CustomerCode").alias("SurvivingCustomerCode"))
    withBlocks = keyed.join(blocks, "BlockingKey")
    resolved, unresolved = splitByCondition(withBlocks, F.col("BlockMemberCount") >= 1)
    rejectCustomer(unresolved, "UNRESOLVED_BLOCK", "Blocking key could not be evaluated")
    scored = W.scoreCustomerSurvivorship(resolved, sourceSystemCode=run.sourceSystemCode)
    inserted = run.rebuildForBatch(scored.select(*DEDUP_COLUMNS), "work_customer_dedup")
    retired = scored.where(F.col("IsSelectedSurvivor") == F.lit(False)).count()
    crosswalk = scored.select(
        F.lit("Customer").alias("EntityName"), "SourceSystemCode", F.col("SourceCustomerId").alias("SourceKeyValue"), F.col("CandidateCustomerBusinessKey").alias("ConformedBusinessKey"),
        F.when(F.col("LosesToBusinessKey").isNull(), F.lit("LOADED")).otherwise(F.lit("DEDUP_SURVIVOR")).alias("MatchMethodCode"),
        F.col("LosesToBusinessKey").alias("SupersededByBusinessKey"), F.col("LosesToBusinessKey").isNull().alias("IsActive"),
    ).where(F.col("SourceKeyValue").isNotNull())
    run.mergeByKey(crosswalk, "ref_source_key_crosswalk", ["EntityName", "SourceSystemCode", "SourceKeyValue"])
    run.logRowCount(objectName="stg.Customer", sourceRowCount=inserted, targetRowCount=inserted - retired, updateRowCount=retired, deleteRowCount=0)


def standardizeAddresses(run):
    addresses = run.silver("stg_customer_address")
    standardized = W.standardizeCustomerAddressByRegion(addresses, sourceSystemCode=run.sourceSystemCode)
    usable, missingPostal, missingStreet = W.splitAddressQuality(standardized)
    rejectCustomer(missingPostal, "ADDR_NO_POSTAL", "Address has no postal code")
    rejectCustomer(missingStreet, "ADDR_NO_STREET", "Address has no street line")
    count = run.rebuildForBatch(usable.select(*ADDRESS_COLUMNS), "work_customer_address_standardized")
    run.counters.rowsUpdated += count
    run.logRowCount(objectName="work.CustomerAddressStandardized", insertRowCount=count)


with run.execute():
    if not run.skipped:
        dedupe(run)
        standardizeAddresses(run)
        run.logRowCount()
