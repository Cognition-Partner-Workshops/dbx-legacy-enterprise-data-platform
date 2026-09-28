# Databricks notebook source
# MAGIC %md
# MAGIC # REF_Load_WarehouseSite
# MAGIC Migrated from `ssis/06_reference_data/REF_Load_WarehouseSite.dtsx` (generator `build_reference_packages.py`, project WWI_ReferenceData).
# MAGIC
# MAGIC `silver.stg_stock_movement` aggregated per warehouse site -> `gold.dim_warehouse_site` (SCD1) with postal-code standardisation (`ref.PostalFormatRule`), region lookup and DC / REGIONAL / SATELLITE site type by movement volume.
# MAGIC
# MAGIC Control flow (legacy task -> here): `CTL Log Package Start` -> `control.logPackageStart`; `Truncate Dimension.Warehouse Site` -> idempotent
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

PACKAGE_NAME = "REF_Load_WarehouseSite"
p = params.getJobParams(dbutils)
print("catalog=%s batchId=%s businessDate=%s environment=%s restartFromStep=%s"
      % (p["catalog"], p["batchId"], p["businessDate"], p["environmentCode"], p["restartFromStep"]))

if runtime.isSkippedByRestart(p, PACKAGE_NAME):
    dbutils.notebook.exit("SKIPPED: RestartFromStep=%s resumes after %s" % (p["restartFromStep"], PACKAGE_NAME))

# COMMAND ----------

with runtime.packageRun(spark, p, PACKAGE_NAME) as ctx:
    # -- pre-tasks: ref.usp_LoadRegion, ref.usp_LoadPostalFormatRule
    ref_loads.loadRegion(ctx)
    ref_loads.loadPostalFormatRule(ctx)

    # -- DFT Load Warehouse Site: REF Warehouse Site Conformed -> Standardize Site -> Lookup Site Region -> Screen Site
    ctx.step("DFT Load Warehouse Site")
    movements = ref_loads.warehouseSiteMovements(ctx)
    matched, noRegion = transforms.warehouseSiteDimension(
        movements, spark.table(ctx.table("ref.Country")), spark.table(ctx.table("ref.PostalFormatRule")),
        spark.table(ctx.table("ref.Region")))
    matched = matched.cache()
    rowsRead = matched.count() + noRegion.count()
    rejected = ctx.rejectLookupFailures(
        noRegion, "stg.StockMovement", "ref.Region", "RegionCode", "CountryCode", "WarehouseSiteId",
        "REF_LOOKUP_MISS", "warehouse site country has no conformed region", routedToUnknownMember=True,
        payloadColumns=["WarehouseSiteId", "WarehouseSiteCode", "CountryCode", "PostalCode", "MovementCount"])
    published, badPostal = transforms.screenWarehouseSite(matched)
    rejected += ctx.rejectConstraintViolations(
        badPostal, "Dimension.Warehouse Site", "WarehouseSiteId", "PostalCodeStandardized", "PostalCode",
        "REF_POSTAL_CODE_INVALID", "standardised postal code is shorter than three characters",
        constraintName="CK_WarehouseSite_PostalCode",
        payloadColumns=["WarehouseSiteId", "WarehouseSiteCode", "CountryCode", "PostalCode", "PostalCodeStandardized"])
    runtime.publishScd1(ctx, "Dimension.Warehouse Site", published, rowsRead, rejectRowCount=rejected)
    matched.unpersist()

    print(ctx.summary())
