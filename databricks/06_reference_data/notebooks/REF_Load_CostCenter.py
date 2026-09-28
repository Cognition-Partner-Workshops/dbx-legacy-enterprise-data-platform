# Databricks notebook source
# MAGIC %md
# MAGIC # REF_Load_CostCenter
# MAGIC Migrated from `ssis/06_reference_data/REF_Load_CostCenter.dtsx` (generator `build_reference_packages.py`, project WWI_ReferenceData).
# MAGIC
# MAGIC COST_CENTER crosswalk -> `gold.dim_cost_center` (SCD2: new version per changed RowHashType2, previous version closed at BusinessDate) with parent hierarchy, company / function codes and suspense flag.
# MAGIC
# MAGIC Control flow (legacy task -> here): `CTL Log Package Start` -> `control.logPackageStart`; `Truncate Dimension.Cost Center` -> idempotent
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

PACKAGE_NAME = "REF_Load_CostCenter"
p = params.getJobParams(dbutils)
print("catalog=%s batchId=%s businessDate=%s environment=%s restartFromStep=%s"
      % (p["catalog"], p["batchId"], p["businessDate"], p["environmentCode"], p["restartFromStep"]))

if runtime.isSkippedByRestart(p, PACKAGE_NAME):
    dbutils.notebook.exit("SKIPPED: RestartFromStep=%s resumes after %s" % (p["restartFromStep"], PACKAGE_NAME))

# COMMAND ----------

with runtime.packageRun(spark, p, PACKAGE_NAME) as ctx:
    # -- pre-task: ref.usp_LoadCodeCrosswalk 'COST_CENTER'
    ref_loads.loadCodeCrosswalk(ctx, "COST_CENTER")

    # -- DFT Load Cost Center: REF Cost Center Conformed -> Derive Hierarchy -> Row Hash Type 2 -> Screen -> SCD2 destination
    ctx.step("DFT Load Cost Center")
    shaped = transforms.costCenterDimension(spark.table(ctx.table("ref.CodeCrosswalk"))).cache()
    rowsRead = shaped.count()
    published, rejects = transforms.screenCostCenter(shaped)
    rejected = ctx.rejectConstraintViolations(
        rejects, "Dimension.Cost Center", "CostCenterCode", "CostCenterCode", "CostCenterCode",
        "REF_COST_CENTER_CODE_INVALID", "cost center code must be at least three characters",
        constraintName="CK_CostCenter_Code")
    runtime.publishScd2(ctx, "Dimension.Cost Center", published, rowsRead, rejectRowCount=rejected)
    shaped.unpersist()

    # -- post-task: ref.usp_ReportUnmappedCodes 'COST_CENTER'
    ref_loads.reportUnmappedCodes(ctx, "COST_CENTER")

    print(ctx.summary())
