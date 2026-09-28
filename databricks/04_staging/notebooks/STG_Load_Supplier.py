# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_Supplier
# MAGIC Legacy: `ssis/04_staging/STG_Load_Supplier.dtsx` (WWI_Staging). Conform raw.OracleSupplierMaster into stg.Supplier: tax-id cleansing, payment-terms resolution, survivorship on tax identifier (LATEST_UPDATE). `stg.usp_NormalizeSupplier` tax-id typing / withholding flags are folded into `T.deriveSupplierHash`.
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

run = StagingRun(spark, dbutils, "STG_Load_Supplier", sourceSystemCode="ORA_ERP", objectName="stg.Supplier", jobParams=p)

SUPPLIER_COLUMNS = [
    "SupplierCode", "SupplierName", "TaxIdentifier", "TaxIdentifierTypeCode", "SupplierStatusCode", "CountryCode", "RegionCode",
    "DefaultCurrencyCode", "LeadTimeDays", "PaymentTermsCode", "NetDays", "DiscountPercent", "DiscountDays",
    "WithholdingApplicableFlag", "GstRegisteredFlag", "ChangeHash", "LAST_UPD_DT",
]


def rejectSupplier(df, code, reason, failedColumn):
    return run.reject(
        df, "err_rejected_supplier", code, reason, businessKeyColumn="SupplierCode",
        keyColumns={"SupplierCode": "SupplierCode", "SourceSupplierId": F.col("SUPP_CODE"), "SupplierName": "SupplierName", "TaxIdentifier": "TaxIdentifier", "RegionCode": "RegionCode", "FailedColumnName": F.lit(failedColumn), "FailedValue": failedColumn},
    )


def load(run):
    src = run.bronze("raw_oracle_supplier_master")
    run.countRead(src)
    cleansed = T.cleanseSupplier(src)
    terms = run.silver("stg_payment_terms").where(F.col("IsCurrent") == F.lit(True)).select("PaymentTermsCode", "NetDays", "DiscountPercent", "DiscountDays")
    matched, unknownTerms = lookupLeft(cleansed, terms, ["PaymentTermsCode"], ["NetDays", "DiscountPercent", "DiscountDays"])
    rejectSupplier(unknownTerms, "UNKNOWN_TERMS", "Payment terms code not found in stg.PaymentTerms", "PaymentTermsCode")
    hashed = T.deriveSupplierHash(matched)
    valid, missingTaxId, suspect = T.splitSupplier(hashed)
    rejectSupplier(missingTaxId, "MISSING_TAX_ID", "Supplier has no tax identifier and is not pending", "TaxIdentifier")
    rejectSupplier(suspect, "SUSPECT_SUPPLIER", "Supplier failed the business rules", "SupplierStatusCode")
    # Sort By Tax Identifier + usp_NormalizeSupplier @SurvivorshipRule = LATEST_UPDATE
    survivors = T.survivorshipDedupe(
        valid.where(F.length("TaxIdentifier") > 0), ["TaxIdentifier"], [F.col("LAST_UPD_DT").desc_nulls_last(), F.col("SupplierCode")]
    ).unionByName(valid.where(F.length("TaxIdentifier") == 0))
    out = survivors.select(*SUPPLIER_COLUMNS).withColumnRenamed("LAST_UPD_DT", "SourceModifiedDate")
    run.truncateReload(out, "stg_supplier")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
