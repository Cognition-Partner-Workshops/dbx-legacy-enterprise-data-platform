# Databricks notebook source
# MAGIC %md
# MAGIC # MNT_Check_DiskSpace
# MAGIC Legacy: `ssis/99_maintenance/MNT_Check_DiskSpace.dtsx` (pre-flight before the batch).
# MAGIC Unity Catalog storage has no volume to run out of, so each schema (and the landing UC Volume) is
# MAGIC measured against a storage budget (`etl.configuration` key `MAINT_STORAGE_BUDGET_GB_<NAME>`, falling
# MAGIC back to `StorageBudgetGb`). Free percent / projected growth are evaluated with the legacy thresholds,
# MAGIC results go to `etl.preflight_result`, shortfalls to `silver.work_volume_shortfall`, and a shortfall
# MAGIC raises an `etl.batch_hold` (`DISK_SPACE`) when `HoldBatchOnShortfall` is set.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
import maintenance_lib as mnt  # noqa: E402

PACKAGE_NAME = "MNT_Check_DiskSpace"
PROJECT_NAME = "WWI_Maintenance"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"] or None
environmentCode = p["environmentCode"]
minimumFreePercent = mnt.toInt(mnt.widgetOr(dbutils, "MinimumFreePercent", "15"), 15)
minimumFreeGbScratch = mnt.toInt(mnt.widgetOr(dbutils, "MinimumFreeGbTempdb", "50"), 50)
holdBatchOnShortfall = mnt.toBool(mnt.widgetOr(dbutils, "HoldBatchOnShortfall", "True"))
storageBudgetGb = mnt.toInt(mnt.widgetOr(dbutils, "StorageBudgetGb", "1024"), 1024)
landingVolumePath = mnt.widgetOr(dbutils, "landingVolumePath", f"/Volumes/{catalog}/bronze/landing")


def budgetGbFor(volumeName: str) -> int:
    key = "MAINT_STORAGE_BUDGET_GB_" + volumeName.upper().replace(".", "_").replace("/", "_").strip("_")
    try:
        return mnt.toInt(control.getConfiguration(spark, catalog, key, environmentCode=environmentCode), storageBudgetGb)
    except Exception:
        return storageBudgetGb


def volumeSizeBytes(path: str, depth: int = 6) -> int:
    total = 0
    try:
        entries = dbutils.fs.ls(path)
    except Exception:
        return 0
    for e in entries:
        if e.isDir():
            total += volumeSizeBytes(e.path, depth - 1) if depth > 0 else 0
        else:
            total += e.size
    return total


# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME,
                                             projectName=PROJECT_NAME, stepName="Pre Flight")
try:
    checkTable = mnt.ensureWorkTable(spark, catalog, "silver.work_volume_space_check")
    growthTable = mnt.ensureWorkTable(spark, catalog, "silver.work_volume_growth_projection")
    shortfallTable = mnt.ensureWorkTable(spark, catalog, "silver.work_volume_shortfall")
    preflightTable = mnt.ensureOpsTable(spark, catalog, "etl.preflight_result")
    historyTable = mnt.ensureOpsTable(spark, catalog, "etl.load_volume_history")
    checkedAt = mnt.utcNow()

    # Survey: bytes per schema from DESCRIBE DETAIL, plus the landing UC Volume (legacy inbound/archive shares)
    usedBytes = {}
    for t in mnt.listTables(spark, catalog, mnt.MAINTENANCE_SCHEMAS):
        if mnt.isDeltaTable(t):
            detail = mnt.deltaTableDetail(spark, naming.table(catalog, t["schema"], t["table"]))
            usedBytes[f"{catalog}.{t['schema']}"] = usedBytes.get(f"{catalog}.{t['schema']}", 0) + int(detail["sizeInBytes"])
    for schema in mnt.MAINTENANCE_SCHEMAS:
        usedBytes.setdefault(f"{catalog}.{schema}", 0)
    usedBytes[landingVolumePath] = volumeSizeBytes(landingVolumePath)

    budgets = {name: budgetGbFor(name) for name in usedBytes}
    surveyRows = []
    for name, used in usedBytes.items():
        totalBytes = budgets[name] * (1024 ** 3)
        available = max(0, totalBytes - used)
        surveyRows.append({
            "VolumeMountPoint": name, "TotalBytes": totalBytes, "AvailableBytes": available,
            "FreePercent": round(available * 100.0 / totalBytes, 2) if totalBytes else 0.0,
            "DatabaseName": catalog, "FileTypeCode": "FILES" if name == landingVolumePath else "DATA",
            "CheckedAtUtc": checkedAt})
    mnt.insertRows(spark, checkTable, surveyRows, mnt.WORK_TABLE_DDL["silver.work_volume_space_check"])

    # Growth projection: average of the recent nightly loads plus a third for index maintenance
    growth = {}
    for r in spark.sql(
            f"SELECT VolumeMountPoint, collect_list(BytesWritten) AS History FROM {mnt.quoted(historyTable)} "
            f"WHERE LoadDate >= current_date() - INTERVAL 14 DAYS GROUP BY VolumeMountPoint").collect():
        growth[r["VolumeMountPoint"]] = mnt.projectedGrowthBytes(r["History"])
    mnt.insertRows(spark, growthTable,
                   [{"VolumeMountPoint": k, "ProjectedBytes": v,
                     "BasisDescription": "Average of the last ten nightly loads plus index maintenance headroom",
                     "CheckedAtUtc": checkedAt} for k, v in growth.items()],
                   mnt.WORK_TABLE_DDL["silver.work_volume_growth_projection"])

    # Evaluate thresholds (legacy derived column + conditional split + multicast)
    preflightRows, shortfallRows = [], []
    for row in surveyRows:
        ev = mnt.evaluateVolume(row["TotalBytes"] - row["AvailableBytes"], budgets[row["VolumeMountPoint"]],
                                growth.get(row["VolumeMountPoint"], 0), minimumFreePercent)
        preflightRows.append({
            "BatchId": batchId, "CheckName": f"VOLUME_{row['VolumeMountPoint']}",
            "CheckStatus": mnt.preflightCheckStatus(ev["SeverityCode"]),
            "DetailText": (f"free {ev['FreePercent']}% ({ev['AvailableGb']} GB of {budgets[row['VolumeMountPoint']]} GB budget), "
                           f"projected growth {ev['ProjectedGb']} GB, headroom {ev['HeadroomGb']} GB"),
            "CheckedAtUtc": checkedAt})
        if ev["HasShortfall"]:
            shortfallRows.append({
                "VolumeMountPoint": row["VolumeMountPoint"], "FreePercent": ev["FreePercent"],
                "AvailableGb": ev["AvailableGb"], "DatabaseName": catalog, "FileTypeCode": row["FileTypeCode"],
                "ProjectedGb": ev["ProjectedGb"], "HasShortfall": True, "SeverityCode": ev["SeverityCode"],
                "HeadroomGb": ev["HeadroomGb"], "CheckedAtUtc": checkedAt})

    # Scratch headroom (legacy tempdb check): silver budget left for the work_* tables
    silverName = f"{catalog}.silver"
    scratchHeadroomGb = mnt.bytesToGb(budgets[silverName] * (1024 ** 3) - usedBytes[silverName])
    preflightRows.append({
        "BatchId": batchId, "CheckName": "TEMPDB_HEADROOM",
        "CheckStatus": "FAILED" if scratchHeadroomGb < minimumFreeGbScratch else "OK",
        "DetailText": f"Scratch (silver.work_*) headroom GB: {scratchHeadroomGb}; minimum {minimumFreeGbScratch}",
        "CheckedAtUtc": checkedAt})

    mnt.insertRows(spark, preflightTable, preflightRows,
                   "BatchId BIGINT, CheckName STRING, CheckStatus STRING, DetailText STRING, CheckedAtUtc TIMESTAMP")
    mnt.insertRows(spark, shortfallTable, shortfallRows, mnt.WORK_TABLE_DDL["silver.work_volume_shortfall"])

    lowVolumeCount = len(shortfallRows)
    smallestFreePercent = min([r["FreePercent"] for r in shortfallRows], default=100)
    preflightStatus = mnt.preflightStatus(lowVolumeCount, smallestFreePercent)
    if lowVolumeCount > 0 and holdBatchOnShortfall:
        holdTable = mnt.ensureOpsTable(spark, catalog, "etl.batch_hold")
        spark.sql(f"INSERT INTO {mnt.quoted(holdTable)} (HoldReasonCode, HoldDetail, RaisedAtUtc, IsCleared) VALUES "
                  f"('DISK_SPACE', 'Volumes below the free-space threshold: {lowVolumeCount}; "
                  f"smallest free percent: {smallestFreePercent}', current_timestamp(), false)")

    mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME,
                             f"Preflight {preflightStatus}: {len(surveyRows)} volumes surveyed, {lowVolumeCount} short")
    control.logRowCount(spark, catalog, packageExecutionId, "silver.work_volume_space_check",
                        sourceRowCount=len(surveyRows), targetRowCount=len(surveyRows), rejectRowCount=lowVolumeCount)
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded",
                          rowsRead=len(surveyRows), rowsInserted=len(preflightRows))
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                     errorSeverity="Error", sourceName=PACKAGE_NAME,
                     sourceComponent="Evaluate Preflight", errorDescription=str(exc)[:4000])
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise
