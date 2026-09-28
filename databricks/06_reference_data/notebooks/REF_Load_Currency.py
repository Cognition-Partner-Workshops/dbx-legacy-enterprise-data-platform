# Databricks notebook source
# MAGIC %md
# MAGIC # REF_Load_Currency
# MAGIC Migrated from `ssis/06_reference_data/REF_Load_Currency.dtsx` (generator `build_reference_packages.py`, project WWI_ReferenceData).
# MAGIC
# MAGIC `raw.OracleCurrency` / `raw.OracleFxRate` -> `silver.ref_currency`, `silver.ref_fx_rate_daily` (`ref.usp_LoadCurrency`, `ref.usp_LoadFxRateDaily`) -> `gold.dim_currency` (SCD1) with the latest CORPORATE rate to USD and RATED / STALE / UNRATED status.
# MAGIC
# MAGIC Control flow (legacy task -> here): `CTL Log Package Start` -> `control.logPackageStart`; `Truncate Dimension.Currency` -> idempotent
# MAGIC Delta MERGE (surrogate keys and reserved members are preserved); pre-tasks (`ref.usp_Load*`) -> `wwi_ref.ref_loads`;
# MAGIC data flow -> `wwi_ref.transforms` + `wwi_ref.delta_io`; error outputs -> `silver.err_*` + `control.logRejectedRecordSet`;
# MAGIC `CTL Log Row Count` -> `control.logRowCount`; `CTL Log Package Success` / `OnError` -> `control.logPackageEnd` / `control.logError`.
# MAGIC
# MAGIC Job parameters: `BatchId`, `BusinessDate`, `ReloadFullHistory`, `EnvironmentCode`, `RestartFromStep`, `catalog`.

# COMMAND ----------

import datetime
import os
import sys


def bundleSourcePath():
    """../src of this bundle (workspace files), so `wwi_ref` imports without a wheel build."""
    try:
        notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return os.path.join("/Workspace", os.path.dirname(os.path.dirname(notebookPath)).lstrip("/"), "src")
    except Exception:
        return os.path.abspath(os.path.join(os.getcwd(), "..", "src"))


if bundleSourcePath() not in sys.path:
    sys.path.insert(0, bundleSourcePath())

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, params, naming  # noqa: E402,F401  (dbx_etl_common wheel: task environment)

from wwi_ref import runtime, ref_loads, transforms, delta_io, schemas  # noqa: E402,F401

# COMMAND ----------

PACKAGE_NAME = "REF_Load_Currency"
p = params.getJobParams(dbutils)
print("catalog=%s batchId=%s businessDate=%s environment=%s restartFromStep=%s"
      % (p["catalog"], p["batchId"], p["businessDate"], p["environmentCode"], p["restartFromStep"]))

if runtime.isSkippedByRestart(p, PACKAGE_NAME):
    dbutils.notebook.exit("SKIPPED: RestartFromStep=%s resumes after %s" % (p["restartFromStep"], PACKAGE_NAME))

# COMMAND ----------

with runtime.packageRun(spark, p, PACKAGE_NAME) as ctx:
    # -- pre-tasks: ref.usp_LoadCurrency, ref.usp_LoadFxRateDaily
    ref_loads.loadCurrency(ctx)
    ref_loads.loadFxRateDaily(ctx)

    # -- DFT Load Currency: REF Currency Conformed -> Shape Currency Dimension -> Screen Currency
    ctx.step("DFT Load Currency")
    shaped = transforms.currencyDimension(spark.table(ctx.table("ref.Currency")),
                                          spark.table(ctx.table("ref.FxRateDaily"))).cache()
    rowsRead = shaped.count()
    # legacy: STALE / UNRATED currencies are published AND reported on the error output
    stale = shaped.where(F.col("RateStatusCode") == "STALE")
    unrated = shaped.where(F.col("RateStatusCode") == "UNRATED")
    rejected = ctx.rejectLookupFailures(
        stale, "ref.Currency", "ref.FxRateDaily", "RateDate", "LatestRateDate", "CurrencyCode",
        "REF_FX_RATE_STALE", "latest corporate USD rate is older than 30 days",
        payloadColumns=["CurrencyCode", "LatestRateToUsd", "LatestRateDate", "RateStalenessDays"])
    rejected += ctx.rejectLookupFailures(
        unrated, "ref.Currency", "ref.FxRateDaily", "FromCurrencyCode", "CurrencyCode", "CurrencyCode",
        "REF_FX_RATE_MISSING", "no corporate USD rate exists for the currency",
        payloadColumns=["CurrencyCode", "CurrencyName", "CurrencyStatusCode"])
    runtime.publishScd1(ctx, "Dimension.Currency", shaped, rowsRead, rejectRowCount=rejected)
    shaped.unpersist()

    # -- post-task: ref.usp_ReportUnmappedCodes 'CURRENCY'
    ref_loads.reportUnmappedCodes(ctx, "CURRENCY")

    print(ctx.summary())
