# Databricks notebook source
# MAGIC %md
# MAGIC # REF_Load_UnknownMembers
# MAGIC Migrated from `ssis/06_reference_data/REF_Load_UnknownMembers.dtsx` (generator `build_reference_packages.py`, project WWI_ReferenceData).
# MAGIC
# MAGIC Status / reason code seeds (`ref.usp_LoadStatusCode`, `ref.usp_LoadReasonCode`), `ref.usp_LoadSourceKeyCrosswalk`, the `gold.dim_unknown_member` registry, and the reserved members -1 Unknown / -2 Not Applicable / -3 Invalid / -9 Error (`90_unknown_members.sql`) for every `gold.dim_*` table, idempotently.
# MAGIC
# MAGIC Control flow (legacy task -> here): `CTL Log Package Start` -> `control.logPackageStart`; `Truncate Dimension.Unknown Member` -> idempotent
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
from wwi_ref import reserved  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "REF_Load_UnknownMembers"
p = params.getJobParams(dbutils)
print("catalog=%s batchId=%s businessDate=%s environment=%s restartFromStep=%s"
      % (p["catalog"], p["batchId"], p["businessDate"], p["environmentCode"], p["restartFromStep"]))

if runtime.isSkippedByRestart(p, PACKAGE_NAME):
    dbutils.notebook.exit("SKIPPED: RestartFromStep=%s resumes after %s" % (p["restartFromStep"], PACKAGE_NAME))

# COMMAND ----------

with runtime.packageRun(spark, p, PACKAGE_NAME) as ctx:
    # -- pre-tasks: ref.usp_LoadStatusCode, ref.usp_LoadReasonCode (all domains), ref.usp_LoadSourceKeyCrosswalk
    ref_loads.loadStatusCode(ctx)
    ref_loads.loadReasonCode(ctx)
    ref_loads.loadSourceKeyCrosswalk(ctx, maintainedByName=PACKAGE_NAME)

    # -- DFT Load Unknown Members: REF Unknown Members Conformed (status UNION reason) -> Shape -> Screen -> destination
    ctx.step("DFT Load Unknown Members")
    shaped = transforms.unknownMemberRows(spark.table(ctx.table("ref.StatusCode")),
                                          spark.table(ctx.table("ref.ReasonCode"))).cache()
    rowsRead = shaped.count()
    published, rejects = transforms.screenUnknownMember(shaped)
    rejected = ctx.rejectConstraintViolations(
        rejects, "Dimension.Unknown Member", "ReferenceTableName", "DomainCode", "DomainCode",
        "REF_DOMAIN_MISSING", "unknown member row has no domain code", constraintName="CK_UnknownMember_Domain")
    runtime.publishScd1(ctx, "Dimension.Unknown Member", published, rowsRead, rejectRowCount=rejected)
    shaped.unpersist()

    # -- 90_unknown_members.sql: reserved rows for every dimension (dynamic seeding failures are warnings)
    ctx.step("Seed Reserved Members")
    seeded = reserved.seedAllReservedMembers(ctx)
    for tableName, insertedRows in seeded.items():
        ctx.addCounts("reserved:%s" % tableName, insertRowCount=insertedRows, targetRowCount=len(reserved.RESERVED_MEMBERS))

    print(ctx.summary())
