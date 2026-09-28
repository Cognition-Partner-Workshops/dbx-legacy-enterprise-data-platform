# Databricks notebook source
# MAGIC %md
# MAGIC # MNT_Reconcile_Maintenance
# MAGIC Reconciliation for the WWI_Maintenance project. The maintenance packages never load warehouse data;
# MAGIC their "targets" are the `etl.*` operations tables and the `silver.work_*` plan tables they write, plus the
# MAGIC staging tables the purge trims. For every one of those tables this notebook computes the row count
# MAGIC (per BatchId where the table carries one) and a deterministic hash
# MAGIC (`sum(xxhash64(concat_ws(...)))` over all columns except identity/timestamp columns, ordered by the
# MAGIC concatenation), compares them with the SQL Server baseline supplied as a Delta table or JSON job
# MAGIC parameter, and writes the outcome through `control.logRowCount`.
# MAGIC
# MAGIC Baseline shape (ported from `validation/runtime/01_control_framework_health.sql` /
# MAGIC `02_row_count_reconciliation.sql`): `{"etl.purge_audit": {"rowCount": 123, "hash": "-42", "batchId": 17}, ...}`.

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
import maintenance_lib as mnt  # noqa: E402

PACKAGE_NAME = "MNT_Reconcile_Maintenance"
PROJECT_NAME = "WWI_Maintenance"
EXCLUDED_COLUMN_SUFFIXES = ("Id", "AtUtc", "Utc")
RECONCILED_TABLES = [
    ("etl", "maintenance_log"), ("etl", "purge_audit"), ("etl", "batch_archive"),
    ("etl", "preflight_result"), ("etl", "batch_hold"), ("etl", "operator_notification"),
    ("etl", "archive_expiry_list"), ("etl", "inbound_file_register"),
    ("silver", "work_staging_purge_plan"), ("silver", "work_index_maintenance_plan"),
    ("silver", "work_statistics_refresh_plan"), ("silver", "work_file_archive_queue"),
    ("silver", "work_configuration_validation"), ("silver", "work_volume_space_check"),
    ("silver", "work_volume_shortfall"),
]

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"] or None
baselineTable = mnt.widgetOr(dbutils, "BaselineTable", "")
baselineJson = mnt.widgetOr(dbutils, "BaselineJson", "{}")
includeStaging = mnt.toBool(mnt.widgetOr(dbutils, "IncludeStagingTables", "True"))


def hashColumns(columns):
    return [c for c in columns if not c.endswith(EXCLUDED_COLUMN_SUFFIXES) or c == "BatchId"]


def loadBaseline():
    baseline = json.loads(baselineJson or "{}")
    if baselineTable:
        for r in spark.table(baselineTable).collect():
            baseline[r["ObjectName"]] = {"rowCount": r["RowCount"], "hash": r["RowHash"],
                                         "batchId": r["BatchId"] if "BatchId" in r.asDict() else None}
    return baseline


def reconcileTable(schema, table, baseline):
    fqn = naming.table(catalog, schema, table)
    columns = mnt.listColumns(spark, catalog, schema, table)
    hashed = hashColumns(columns) or columns
    concat = "concat_ws('|', " + ", ".join(f"coalesce(cast(`{c}` as string), '')" for c in hashed) + ")"
    where = f"WHERE BatchId = {batchId}" if batchId is not None and "BatchId" in columns else ""
    row = spark.sql(f"SELECT COUNT(*) AS RowCount, COALESCE(SUM(xxhash64({concat})), 0) AS RowHash "
                    f"FROM {mnt.quoted(fqn)} {where}").collect()[0]
    expected = baseline.get(f"{schema}.{table}") or {}
    return {"ObjectName": f"{schema}.{table}", "TargetRowCount": int(row["RowCount"]),
            "TargetHash": str(row["RowHash"]), "SourceRowCount": expected.get("rowCount"),
            "SourceHash": str(expected["hash"]) if expected.get("hash") is not None else None,
            "HashColumns": ",".join(hashed)}


# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME,
                                             projectName=PROJECT_NAME, stepName="Reconcile")
try:
    baseline = loadBaseline()
    targets = list(RECONCILED_TABLES)
    if includeStaging:
        targets += [(t["schema"], t["table"]) for t in mnt.listTables(spark, catalog, ("bronze", "silver"))
                    if mnt.isStagingPurgeCandidate(t["schema"], t["table"]) and (t["schema"], t["table"]) not in targets]

    results, mismatches = [], []
    for schema, table in targets:
        if not mnt.tableExists(spark, catalog, schema, table):
            continue
        r = reconcileTable(schema, table, baseline)
        results.append(r)
        control.logRowCount(spark, catalog, packageExecutionId, r["ObjectName"],
                            sourceRowCount=r["SourceRowCount"], targetRowCount=r["TargetRowCount"])
        if r["SourceRowCount"] is not None and (r["SourceRowCount"] != r["TargetRowCount"]
                                                or (r["SourceHash"] is not None and r["SourceHash"] != r["TargetHash"])):
            mismatches.append(r)

    display(spark.createDataFrame(results, "ObjectName STRING, TargetRowCount BIGINT, TargetHash STRING, "
                                           "SourceRowCount BIGINT, SourceHash STRING, HashColumns STRING"))
    mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME,
                             f"Reconciled {len(results)} tables; baseline supplied for "
                             f"{sum(1 for r in results if r['SourceRowCount'] is not None)}; mismatches: {len(mismatches)}")
    control.logPackageEnd(spark, catalog, packageExecutionId,
                          status="SucceededWithWarnings" if mismatches else "Succeeded", rowsRead=len(results))
    if mismatches:
        raise RuntimeError("Reconciliation mismatches: " + ", ".join(m["ObjectName"] for m in mismatches))
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                     errorSeverity="Warning" if str(exc).startswith("Reconciliation mismatches") else "Error",
                     sourceName=PACKAGE_NAME, sourceComponent="Reconcile", errorDescription=str(exc)[:4000])
    raise
