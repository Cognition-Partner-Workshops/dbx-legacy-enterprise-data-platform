# Databricks notebook source
# MAGIC %md
# MAGIC # REF_Load_Geography
# MAGIC Migrated from `ssis/06_reference_data/REF_Load_Geography.dtsx` (generator `build_reference_packages.py`, project WWI_ReferenceData).
# MAGIC
# MAGIC `raw.OracleGeography` / `raw.OracleTaxRate` -> `silver.ref_region`, `silver.ref_country`, `silver.ref_tax_jurisdiction`, `silver.ref_postal_format_rule` -> `gold.dim_geography` (SCD1) with tax structure, EU status and local currency lookup.
# MAGIC
# MAGIC Control flow (legacy task -> here): `CTL Log Package Start` -> `control.logPackageStart`; `Truncate Dimension.Geography` -> idempotent
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

PACKAGE_NAME = "REF_Load_Geography"
p = params.getJobParams(dbutils)
print("catalog=%s batchId=%s businessDate=%s environment=%s restartFromStep=%s"
      % (p["catalog"], p["batchId"], p["businessDate"], p["environmentCode"], p["restartFromStep"]))

if runtime.isSkippedByRestart(p, PACKAGE_NAME):
    dbutils.notebook.exit("SKIPPED: RestartFromStep=%s resumes after %s" % (p["restartFromStep"], PACKAGE_NAME))

# COMMAND ----------

with runtime.packageRun(spark, p, PACKAGE_NAME) as ctx:
    # -- pre-tasks: ref.usp_LoadRegion, ref.usp_LoadCountry, ref.usp_LoadTaxJurisdiction, ref.usp_LoadPostalFormatRule
    # (REGION crosswalk and ref.Currency are lookups of this package; they are refreshed here so a first run in an
    #  empty catalog does not reject every country - see mapping doc "decisions")
    ref_loads.loadRegion(ctx)
    ref_loads.loadCodeCrosswalk(ctx, "REGION")
    ref_loads.loadCurrency(ctx)
    ref_loads.loadCountry(ctx)
    ref_loads.loadTaxJurisdiction(ctx)
    ref_loads.loadPostalFormatRule(ctx)

    # -- DFT Load Geography: REF Geography Conformed -> Derive Tax Structure -> Lookup Local Currency -> Screen Geography
    ctx.step("DFT Load Geography")
    matched, noCurrency = transforms.geographyDimension(
        spark.table(ctx.table("ref.Country")), spark.table(ctx.table("ref.Region")),
        spark.table(ctx.table("ref.TaxJurisdiction")), spark.table(ctx.table("ref.Currency")))
    matched = matched.cache()
    rowsRead = matched.count() + noCurrency.count()
    rejected = ctx.rejectLookupFailures(
        noCurrency, "ref.Country", "ref.Currency", "CurrencyCode", "LocalCurrencyCode", "CountryCode",
        "REF_LOOKUP_MISS", "country local currency is not in ref.Currency", routedToUnknownMember=True,
        payloadColumns=["CountryCode", "CountryName", "RegionCode", "LocalCurrencyCode"])
    published, noTax = transforms.screenGeography(matched)
    rejected += ctx.rejectLookupFailures(
        noTax, "ref.Country", "ref.TaxJurisdiction", "TaxJurisdictionCode", "CountryCode", "CountryCode",
        "REF_TAX_JURISDICTION_MISSING", "country has no effective country-level tax jurisdiction", routedToUnknownMember=True,
        payloadColumns=["CountryCode", "CountryName", "RegionCode", "TaxRegimeCode"])
    runtime.publishScd1(ctx, "Dimension.Geography", published, rowsRead, rejectRowCount=rejected)
    matched.unpersist()

    # -- post-task: ref.usp_ReportUnmappedCodes 'COUNTRY'
    ref_loads.reportUnmappedCodes(ctx, "COUNTRY")

    print(ctx.summary())
