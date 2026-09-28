# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_VendorContract
# MAGIC Legacy: `ssis/04_staging/STG_Load_VendorContract.dtsx` (WWI_Staging). Conform raw.OracleVendorContract into stg.VendorContract: supplier lookup, CONTRACT-type FX to USD, commitment banding, date-window validation.
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

run = StagingRun(spark, dbutils, "STG_Load_VendorContract", sourceSystemCode="ORA_ERP", objectName="stg.VendorContract", jobParams=p)

CONTRACT_COLUMNS = [
    "ContractNumber", "SupplierCode", "SupplierName", "SupplierCurrencyCode", "ContractTypeCode", "StartDate", "EndDate", "CommitAmount",
    "CommitCurrencyCode", "ContractFxRate", "CommitAmountUsd", "ContractBandCode", "DiscountPercent", "RegionCode", "StatusCode",
]


def load(run):
    src = run.bronze("raw_oracle_vendor_contract")
    run.countRead(src)
    cleansed = T.cleanseVendorContract(src)
    suppliers = run.silver("stg_supplier").select("SupplierCode", "SupplierName", F.col("DefaultCurrencyCode").alias("SupplierCurrencyCode"))
    matched, unknownSupplier = lookupLeft(cleansed, suppliers, ["SupplierCode"], ["SupplierName", "SupplierCurrencyCode"])
    run.reject(
        unknownSupplier, "err_rejected_supplier", "UNKNOWN_SUPPLIER", "Contract supplier not found in stg.Supplier", businessKeyColumn="SupplierCode",
        keyColumns={"SupplierCode": "SupplierCode", "SourceSupplierId": F.col("SUPP_CODE"), "RegionCode": "RegionCode", "FailedColumnName": F.lit("SupplierCode"), "FailedValue": "SupplierCode", "ContractNumber": "ContractNumber"},
    )
    fx = run.silver("stg_fx_rate").where((F.col("ToCurrencyCode") == "USD") & (F.col("RateTypeCode") == "CONTRACT")).select(F.col("FromCurrencyCode").alias("CommitCurrencyCode"), F.col("ExchangeRate").alias("ContractFxRate"))
    fx = fx.unionByName(spark.createDataFrame([("USD", 1.0)], ["CommitCurrencyCode", "ContractFxRate"]).withColumn("ContractFxRate", F.col("ContractFxRate").cast(fx.schema["ContractFxRate"].dataType)))
    withFx, missingRate = lookupLeft(matched, fx, ["CommitCurrencyCode"], ["ContractFxRate"])
    run.rejectLookupFailures(missingRate, "Contract FX Rate", "CommitCurrencyCode", "ContractNumber", "No CONTRACT rate to USD for the commitment currency")
    banded = T.bandVendorContract(withFx)
    valid, inverted = splitByCondition(banded, F.col("EndDate") >= F.col("StartDate"))
    run.rejectConstraint(inverted, "stg.VendorContract", "CK_stgVendorContract_Window", "ContractNumber", "EndDate", "INVERTED_WINDOW", "Contract end date precedes start date")
    run.truncateReload(valid.select(*CONTRACT_COLUMNS), "stg_vendor_contract")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
