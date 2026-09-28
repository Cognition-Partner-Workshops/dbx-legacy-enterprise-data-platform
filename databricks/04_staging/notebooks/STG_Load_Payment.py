# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_Payment
# MAGIC Legacy: `ssis/04_staging/STG_Load_Payment.dtsx` (WWI_Staging). Watermarked (PAY_DT) AP payment conformance: PAYMENT_METHOD crosswalk with regional EU SEPA / APAC conformance, value-date rules and future-dated / non-positive screening; `stg.usp_AppendIncremental_Payment` per-batch append is the keyed merge.
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

run = StagingRun(spark, dbutils, "STG_Load_Payment", sourceSystemCode="ORA_ERP", objectName="stg.Payment", watermark=True, jobParams=p)

PAYMENT_COLUMNS = ["PaymentNumber", "SupplierCode", "SourcePaymentMethodCode", "PaymentMethodCode", "PaymentMethodDescription", "RegionalPaymentMethodCode", "BankAccountCode", "PaymentCurrencyCode", "PaymentAmount", "PaymentDate", "ValueDate", "PaymentStatusCode", "RegionCode"]


def rejectPayment(df, code, reason):
    return run.reject(
        df, "err_rejected_payment", code, reason, businessKeyColumn="PaymentNumber",
        keyColumns={"PaymentBusinessKey": X.sourceSystemKey(F.lit(run.sourceSystemCode), "PaymentNumber"), "PaymentNumber": "PaymentNumber", "SupplierReference": "SupplierCode", "PaymentAmountText": F.col("PaymentAmount").cast("string"), "CurrencyCode": "PaymentCurrencyCode"},
    )


def load(run):
    src = run.bronzeWatermarked("raw_oracle_ap_payment", "PAY_DT")
    run.countRead(src)
    derived = T.derivePayment(src)
    crosswalk = run.silver("ref_code_crosswalk").where((F.col("CodeDomainCode") == "PAYMENT_METHOD") & (F.col("SourceSystemCode") == "ORA_ERP") & F.col("EffectiveToDate").isNull()).select(
        F.col("SourceCodeValue").alias("SourcePaymentMethodCode"), F.col("ConformedCodeValue").alias("PaymentMethodCode"), F.col("SourceCodeDescription").alias("PaymentMethodDescription")
    ).dropDuplicates(["SourcePaymentMethodCode"])
    matched, unknownMethod = lookupLeft(derived, crosswalk, ["SourcePaymentMethodCode"], ["PaymentMethodCode", "PaymentMethodDescription"])
    rejectPayment(unknownMethod, "UNKNOWN_METHOD", "Payment method has no PAYMENT_METHOD crosswalk entry")
    regional = matched.withColumn("RegionalPaymentMethodCode", T.regionalPaymentMethod())
    valid, future, nonPositive = T.splitPayment(regional)
    rejectPayment(future, "FUTURE_DATED", "Payment date is in the future")
    rejectPayment(nonPositive, "NON_POSITIVE_AMOUNT", "Payment amount is not positive")
    run.mergeByKey(valid.select(*PAYMENT_COLUMNS), "stg_payment", ["PaymentNumber"])


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
