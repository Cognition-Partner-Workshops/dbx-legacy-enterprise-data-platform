# Databricks notebook source
# MAGIC %md
# MAGIC # REF_Load_SalesChannel
# MAGIC Migrated from `ssis/06_reference_data/REF_Load_SalesChannel.dtsx` (generator `build_reference_packages.py`, project WWI_ReferenceData).
# MAGIC
# MAGIC SALES_CHANNEL crosswalk -> `gold.dim_sales_channel` (SCD1): DIGITAL / PARTNER / DIRECT grouping and digital flag.
# MAGIC
# MAGIC Control flow (legacy task -> here): `CTL Log Package Start` -> `control.logPackageStart`; `Truncate Dimension.Sales Channel` -> idempotent
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

PACKAGE_NAME = "REF_Load_SalesChannel"
p = params.getJobParams(dbutils)
print("catalog=%s batchId=%s businessDate=%s environment=%s restartFromStep=%s"
      % (p["catalog"], p["batchId"], p["businessDate"], p["environmentCode"], p["restartFromStep"]))

if runtime.isSkippedByRestart(p, PACKAGE_NAME):
    dbutils.notebook.exit("SKIPPED: RestartFromStep=%s resumes after %s" % (p["restartFromStep"], PACKAGE_NAME))

# COMMAND ----------

with runtime.packageRun(spark, p, PACKAGE_NAME) as ctx:
    # -- pre-tasks (Execute SQL Task -> ref.usp_LoadCodeCrosswalk @CodeDomainCode = 'SALES_CHANNEL')
    ref_loads.loadCodeCrosswalk(ctx, "SALES_CHANNEL")

    # -- DFT Load Sales Channel: OLE DB Source (REF ... Conformed) -> Derived Column -> Conditional Split -> destination
    ctx.step("DFT Load Sales Channel")
    crosswalk = spark.table(ctx.table("ref.CodeCrosswalk"))
    shaped = transforms.salesChannelDimension(crosswalk).cache()
    rowsRead = shaped.count()
    published, rejects = transforms.screenSourced(shaped)
    rejected = ctx.rejectLookupFailures(
        rejects, "ref.CodeCrosswalk", "Dimension.Sales Channel", "ChannelCode", "ChannelCode", "ChannelCode",
        "REF_UNMAPPED_CODE", "sales channel has no source code mapped", routedToUnknownMember=True)
    runtime.publishScd1(ctx, "Dimension.Sales Channel", published, rowsRead, rejectRowCount=rejected)
    shaped.unpersist()

    # -- post-task: ref.usp_ReportUnmappedCodes @CodeDomainCode = 'SALES_CHANNEL'
    ref_loads.reportUnmappedCodes(ctx, "SALES_CHANNEL")

    print(ctx.summary())
