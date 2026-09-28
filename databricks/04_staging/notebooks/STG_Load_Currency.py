# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_Currency
# MAGIC Legacy: `ssis/04_staging/STG_Load_Currency.dtsx` (WWI_Staging). Two Data Flows: raw.OracleCurrency into stg.Currency (minor-unit defaults, 3-char code check) and raw.OracleFxRate UNION raw.FileFxOverride into stg.FxRate (effective dating, dedupe per pair/date with the file override winning, USD triangulation). `stg.usp_ConvertCurrencyAmounts @RateTypeCode=SPOT` re-values outstanding amounts in the consuming packages (`refs.convertCurrencyAmounts`).
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

run = StagingRun(spark, dbutils, "STG_Load_Currency", sourceSystemCode="ORA_ERP", objectName="stg.Currency", jobParams=p)

CURRENCY_COLUMNS = ["CurrencyCode", "CurrencyName", "MinorUnits", "RegionCode", "IsActiveFlag"]
FX_COLUMNS = ["FromCurrencyCode", "ToCurrencyCode", "RateTypeCode", "EffectiveFromDate", "EffectiveToDate", "ExchangeRate", "InverseRate", "UsdEquivalentRate", "RateSourceCode"]
FX_SOURCE_COLUMNS = ["FROM_CCY", "TO_CCY", "EFF_FROM_DT", "EFF_TO_DT", "RATE", "RATE_TYPE_CD", "SRC_SYSTEM_CD", "BatchId"]


def load(run):
    currency = run.bronze("raw_oracle_currency")
    run.countRead(currency)
    cleansed = T.cleanseCurrency(currency).withColumn("RegionCode", F.upper(F.trim(F.coalesce(F.col("REGION_CD"), F.lit("GLOBAL")))))
    valid, invalid = splitByCondition(cleansed, F.length("CurrencyCode") == 3)
    run.rejectConstraint(invalid, "stg.Currency", "CK_stgCurrency_Code", "CurrencyCode", "CurrencyCode", "INVALID_CURRENCY_CODE", "Currency code is not three characters")
    run.truncateReload(valid.select(*CURRENCY_COLUMNS), "stg_currency")

    oracleFx = run.bronze("raw_oracle_fx_rate").select(*FX_SOURCE_COLUMNS)
    fileFx = run.bronze("raw_file_fx_override").select(*FX_SOURCE_COLUMNS)
    run.countRead(oracleFx)
    run.countRead(fileFx)
    rates = T.normaliseFxRate(oracleFx.unionByName(fileFx))
    deduped = T.dedupeFxByPairAndDate(rates)
    existing = run.silverIfExists("stg_fx_rate")
    if existing is not None and "UsdEquivalentRate" in existing.columns:
        usdCross = existing.where((F.col("RateTypeCode") == "SPOT") & (F.col("EffectiveToDate") == F.lit(T.FAR_FUTURE).cast("date")) & (F.col("ToCurrencyCode") == "USD")).select(F.col("FromCurrencyCode").alias("ToCurrencyCode"), F.col("ExchangeRate").alias("UsdCrossRate"))
    else:
        usdCross = deduped.where((F.col("RateTypeCode") == "SPOT") & (F.col("ToCurrencyCode") == "USD")).select(F.col("FromCurrencyCode").alias("ToCurrencyCode"), F.col("ExchangeRate").alias("UsdCrossRate"))
    triangulated = T.triangulateThroughUsd(deduped, usdCross)
    run.truncateReload(triangulated.select(*FX_COLUMNS), "stg_fx_rate")
    run.logRowCount(objectName="stg.FxRate")


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
