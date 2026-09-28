# Databricks notebook source
# MAGIC %md
# MAGIC # MNT_Rebuild_Indexes
# MAGIC Legacy: `ssis/99_maintenance/MNT_Rebuild_Indexes.dtsx` + `Integration.usp_RebuildColumnstoreIndexes`.
# MAGIC Surveys every Delta table in bronze/silver/gold from `information_schema`, plans
# MAGIC `REORGANIZE` (-> `OPTIMIZE` bin-packing) or `REBUILD` (-> `OPTIMIZE ... ZORDER BY`, or plain
# MAGIC `OPTIMIZE` on liquid-clustered tables) from the small-file ratio, works largest table first until the
# MAGIC maintenance window is exhausted, and logs each statement to `etl.maintenance_log`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
import maintenance_lib as mnt  # noqa: E402

PACKAGE_NAME = "MNT_Rebuild_Indexes"
PROJECT_NAME = "WWI_Maintenance"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"] or None
reorganiseThreshold = mnt.toInt(mnt.widgetOr(dbutils, "ReorganiseThresholdPercent", "10"), 10)
rebuildThreshold = mnt.toInt(mnt.widgetOr(dbutils, "RebuildThresholdPercent", "30"), 30)
maxDurationMinutes = mnt.toInt(mnt.widgetOr(dbutils, "MaxDurationMinutes", "180"), 180)
onlineRebuild = mnt.toBool(mnt.widgetOr(dbutils, "OnlineRebuild", "False"))  # OPTIMIZE is always online
maintenanceWindowMinutes = mnt.toInt(mnt.widgetOr(dbutils, "MaintenanceWindowMinutes", "240"), 240)
skipIndexRebuild = mnt.toBool(mnt.widgetOr(dbutils, "SkipIndexRebuild", "False"))
schemas = mnt.splitCsv(mnt.widgetOr(dbutils, "Schemas", ",".join(mnt.MAINTENANCE_SCHEMAS)))

# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME,
                                             projectName=PROJECT_NAME, stepName="Index Maintenance")
try:
    # Master_Weekly_Maintenance edge: !SkipIndexRebuild && MaintenanceWindowMinutes >= 120
    if skipIndexRebuild or maintenanceWindowMinutes < 120:
        mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME,
                                 f"SKIPPED: SkipIndexRebuild={skipIndexRebuild}, "
                                 f"MaintenanceWindowMinutes={maintenanceWindowMinutes}")
        control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=0)
        dbutils.notebook.exit("Skipped")

    deadlineMinutes = min(maxDurationMinutes, maintenanceWindowMinutes)
    startedAt = mnt.utcNow()
    planTable = mnt.ensureWorkTable(spark, catalog, "silver.work_index_maintenance_plan")

    plan = []
    for t in mnt.listTables(spark, catalog, schemas):
        if not mnt.isDeltaTable(t):
            continue
        fqn = naming.table(catalog, t["schema"], t["table"])
        detail = mnt.deltaTableDetail(spark, fqn)
        columns = mnt.listColumns(spark, catalog, t["schema"], t["table"])
        action = mnt.planOptimizeAction(t["schema"], t["table"], columns, detail,
                                        reorganiseThreshold, rebuildThreshold)
        plan.append({
            "SchemaName": t["schema"], "TableName": t["table"],
            "IndexTypeCode": action["IndexTypeCode"],
            "FragmentationPercent": action["FragmentationPercent"],
            "NumFiles": int(detail["numFiles"]), "SizeInBytes": int(detail["sizeInBytes"]),
            "PlannedAction": action["PlannedAction"],
            "ZorderColumns": ",".join(action["ZorderColumns"]), "PlannedAtUtc": startedAt,
        })
    mnt.insertRows(spark, planTable, plan, mnt.WORK_TABLE_DDL["silver.work_index_maintenance_plan"])

    executed = failed = skipped = 0
    for row in sorted((r for r in plan if r["PlannedAction"] != "NONE"),
                      key=lambda r: -r["SizeInBytes"]):
        if mnt.deadlineReached(startedAt, deadlineMinutes):
            skipped += 1
            continue
        fqn = naming.table(catalog, row["SchemaName"], row["TableName"])
        stmt = mnt.optimizeStatement(fqn, row["PlannedAction"], mnt.splitCsv(row["ZorderColumns"]))
        try:
            spark.sql(stmt)
            mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME, stmt)
            executed += 1
        except Exception as exc:  # legacy TRY/CATCH: one failed index never abandons the rest
            mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME, f"FAILED: {stmt} -> {str(exc)[:1500]}")
            failed += 1

    rebuilt = sum(1 for r in plan if r["PlannedAction"] == "REBUILD")
    reorganised = sum(1 for r in plan if r["PlannedAction"] == "REORGANIZE")
    mnt.insertMaintenanceLog(
        spark, catalog, PACKAGE_NAME,
        f"Planned actions: {rebuilt + reorganised} (rebuild={rebuilt}, reorganise={reorganised}); "
        f"executed={executed}, failed={failed}, deferred past window={skipped}")
    control.logRowCount(spark, catalog, packageExecutionId, "silver.work_index_maintenance_plan",
                        sourceRowCount=len(plan), targetRowCount=len(plan), updateRowCount=executed)
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded",
                          rowsRead=len(plan), rowsUpdated=executed)
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                     errorSeverity="Error", sourceName=PACKAGE_NAME,
                     sourceComponent="Maintain Rowstore Indexes", errorDescription=str(exc)[:4000])
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise
