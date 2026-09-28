# Databricks notebook source
# MAGIC %md
# MAGIC # MNT_Update_Statistics
# MAGIC Legacy: `ssis/99_maintenance/MNT_Update_Statistics.dtsx`.
# MAGIC Plans an `ANALYZE TABLE ... COMPUTE STATISTICS` for every bronze/silver/gold Delta table whose
# MAGIC Delta history shows at least `ModificationThresholdRows` row modifications since the last refresh
# MAGIC recorded in `etl.maintenance_log`. `gold.dim_*` get `FOR ALL COLUMNS` (legacy FULLSCAN); everything
# MAGIC else gets table-level statistics (legacy SAMPLE n PERCENT has no Delta equivalent).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
import maintenance_lib as mnt  # noqa: E402

PACKAGE_NAME = "MNT_Update_Statistics"
PROJECT_NAME = "WWI_Maintenance"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"] or None
modificationThresholdRows = mnt.toInt(mnt.widgetOr(dbutils, "ModificationThresholdRows", "5000"), 5000)
samplePercent = mnt.toInt(mnt.widgetOr(dbutils, "SamplePercent", "20"), 20)  # not applicable to ANALYZE TABLE
schemas = mnt.splitCsv(mnt.widgetOr(dbutils, "Schemas", ",".join(mnt.MAINTENANCE_SCHEMAS)))

# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME,
                                             projectName=PROJECT_NAME, stepName="Statistics")
try:
    planTable = mnt.ensureWorkTable(spark, catalog, "silver.work_statistics_refresh_plan")
    logTable = mnt.ensureOpsTable(spark, catalog, "etl.maintenance_log")
    plannedAt = mnt.utcNow()

    lastAnalyzed = {
        r["DetailText"]: r["LastRun"]
        for r in spark.sql(
            f"SELECT DetailText, MAX(RecordedAtUtc) AS LastRun FROM {mnt.quoted(logTable)} "
            f"WHERE TaskName = {mnt.sqlString(PACKAGE_NAME)} AND DetailText LIKE 'ANALYZE TABLE %' "
            f"GROUP BY DetailText").collect()
    }

    plan = []
    for t in mnt.listTables(spark, catalog, schemas):
        if not mnt.isDeltaTable(t):
            continue
        fqn = naming.table(catalog, t["schema"], t["table"])
        mode = mnt.statisticsRefreshMode(t["schema"], t["table"])
        since = lastAnalyzed.get(mnt.analyzeStatement(fqn, mode))
        modified = mnt.modifiedRowsSince(mnt.tableHistory(spark, fqn), since)
        if modified >= modificationThresholdRows:
            plan.append({"SchemaName": t["schema"], "TableName": t["table"],
                         "ModifiedRowCount": modified, "RefreshMode": mode, "PlannedAtUtc": plannedAt})
    mnt.insertRows(spark, planTable, plan, mnt.WORK_TABLE_DDL["silver.work_statistics_refresh_plan"])

    refreshed = 0
    for row in sorted(plan, key=lambda r: -r["ModifiedRowCount"]):
        stmt = mnt.analyzeStatement(naming.table(catalog, row["SchemaName"], row["TableName"]), row["RefreshMode"])
        spark.sql(stmt)
        mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME, stmt)
        refreshed += 1

    fullScans = sum(1 for r in plan if r["RefreshMode"] == "FULLSCAN")
    mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME,
                             f"Statistics refreshed: {refreshed} (full scans: {fullScans})")
    control.logRowCount(spark, catalog, packageExecutionId, "silver.work_statistics_refresh_plan",
                        sourceRowCount=len(plan), targetRowCount=len(plan), updateRowCount=refreshed)
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded",
                          rowsRead=len(plan), rowsUpdated=refreshed)
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                     errorSeverity="Error", sourceName=PACKAGE_NAME,
                     sourceComponent="Refresh Statistics", errorDescription=str(exc)[:4000])
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise
