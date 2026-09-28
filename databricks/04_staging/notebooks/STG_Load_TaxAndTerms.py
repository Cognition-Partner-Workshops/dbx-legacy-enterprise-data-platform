# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_TaxAndTerms
# MAGIC Legacy: `ssis/04_staging/STG_Load_TaxAndTerms.dtsx` (WWI_Staging). Two Data Flows: raw.OracleTaxRate into stg.TaxRate (sales tax / VAT / GST regional shapes and plausibility bands) and raw.OraclePaymentTerms into stg.PaymentTerms (terms crosswalk; `stg.usp_TranslateSourceCodes PAYMENT_TERMS`).
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

run = StagingRun(spark, dbutils, "STG_Load_TaxAndTerms", sourceSystemCode="ORA_ERP", objectName="stg.TaxRate", jobParams=p)

TAX_COLUMNS = ["TaxCode", "RegionCode", "TaxTypeCode", "TaxRegimeCode", "JurisdictionCode", "RatePercent", "TaxRatePercent", "IsRecoverableFlag", "EffectiveFromDate", "EffectiveToDate", "ValidToDate"]
TERMS_COLUMNS = ["PaymentTermsCode", "ConformedTermsCode", "PaymentTermsDescription", "NetDays", "DiscountPercent", "DiscountDays", "RegionCode", "IsCurrent"]


def load(run):
    tax = run.bronze("raw_oracle_tax_rate")
    run.countRead(tax)
    derived = T.deriveTaxRate(tax).withColumns(
        {
            "TaxRegimeCode": F.when(F.col("TaxTypeCode") == "SALESTAX", F.lit("SUT")).otherwise(F.col("TaxTypeCode")),
            "TaxRatePercent": F.col("RatePercent"),
            "ValidToDate": F.when(F.col("EffectiveToDate") == F.lit(T.FAR_FUTURE).cast("date"), F.lit(None).cast("date")).otherwise(F.col("EffectiveToDate")),
        }
    )
    plausible, implausible = splitByCondition(derived, T.taxRatePlausible())
    run.rejectConstraint(implausible, "stg.TaxRate", "CK_stgTaxRate_RatePercent", "TaxCode", "RatePercent", "IMPLAUSIBLE_RATE", "Tax rate outside the regional plausibility band")
    run.truncateReload(plausible.select(*TAX_COLUMNS), "stg_tax_rate")

    terms = run.bronze("raw_oracle_payment_terms")
    run.countRead(terms)
    cleansed = T.cleansePaymentTerms(terms)
    crosswalk = refs.codeCrosswalk(run, "PAYMENT_TERMS").select(F.col("SourceCodeValue").alias("PaymentTermsCode"), F.col("ConformedCodeValue").alias("ConformedTermsCode"))
    matched, unmapped = lookupLeft(cleansed, crosswalk, ["PaymentTermsCode"], ["ConformedTermsCode"])
    run.rejectLookupFailures(unmapped, "Terms Crosswalk", "PaymentTermsCode", "PaymentTermsCode", "Legacy terms code has no PAYMENT_TERMS crosswalk entry", objectName="stg.PaymentTerms")
    run.truncateReload(matched.select(*TERMS_COLUMNS), "stg_payment_terms")
    run.logRowCount(objectName="stg.PaymentTerms")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
