# Databricks notebook source
# MAGIC %md
# MAGIC # REF_Load_TransactionType
# MAGIC Migrated from `ssis/06_reference_data/REF_Load_TransactionType.dtsx` (generator `build_reference_packages.py`, project WWI_ReferenceData).
# MAGIC
# MAGIC TRANSACTION_TYPE crosswalk -> `gold.dim_transaction_type` (SCD1) with movement direction/sign, ledger and reversal flags; also refreshes `silver.ref_unit_of_measure` / `silver.ref_uom_conversion` and `gold.dim_unit_of_measure` like the legacy pre-tasks.
# MAGIC
# MAGIC Control flow (legacy task -> here): `CTL Log Package Start` -> `control.logPackageStart`; `Truncate Dimension.Transaction Type` -> idempotent
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

PACKAGE_NAME = "REF_Load_TransactionType"
p = params.getJobParams(dbutils)
print("catalog=%s batchId=%s businessDate=%s environment=%s restartFromStep=%s"
      % (p["catalog"], p["batchId"], p["businessDate"], p["environmentCode"], p["restartFromStep"]))

if runtime.isSkippedByRestart(p, PACKAGE_NAME):
    dbutils.notebook.exit("SKIPPED: RestartFromStep=%s resumes after %s" % (p["restartFromStep"], PACKAGE_NAME))

# COMMAND ----------

with runtime.packageRun(spark, p, PACKAGE_NAME) as ctx:
    # -- pre-tasks (Execute SQL Task -> ref.usp_LoadCodeCrosswalk @CodeDomainCode = 'TRANSACTION_TYPE')
    ref_loads.loadUnitOfMeasure(ctx)
    ref_loads.loadUomConversion(ctx, maintainedByName=PACKAGE_NAME)
    ctx.step("DFT Load Unit Of Measure")
    uomDim = transforms.unitOfMeasureDimension(spark.table(ctx.table("ref.UnitOfMeasure")),
                                               spark.table(ctx.table("ref.UomConversion"))).cache()
    runtime.publishScd1(ctx, "Dimension.Unit Of Measure", uomDim, uomDim.count())
    uomDim.unpersist()
    ref_loads.loadCodeCrosswalk(ctx, "TRANSACTION_TYPE")

    # -- DFT Load Transaction Type: OLE DB Source (REF ... Conformed) -> Derived Column -> Conditional Split -> destination
    ctx.step("DFT Load Transaction Type")
    crosswalk = spark.table(ctx.table("ref.CodeCrosswalk"))
    shaped = transforms.transactionTypeDimension(crosswalk).cache()
    rowsRead = shaped.count()
    published, rejects = transforms.screenTransactionType(shaped)
    rejected = ctx.rejectLookupFailures(
        rejects, "ref.CodeCrosswalk", "Dimension.Transaction Type", "TransactionTypeCode", "TransactionTypeCode", "TransactionTypeCode",
        "REF_UNMAPPED_CODE", "transaction type code is blank after conforming", routedToUnknownMember=True)
    runtime.publishScd1(ctx, "Dimension.Transaction Type", published, rowsRead, rejectRowCount=rejected)
    shaped.unpersist()

    # -- post-task: ref.usp_ReportUnmappedCodes @CodeDomainCode = 'TRANSACTION_TYPE'
    ref_loads.reportUnmappedCodes(ctx, "TRANSACTION_TYPE")

    print(ctx.summary())
