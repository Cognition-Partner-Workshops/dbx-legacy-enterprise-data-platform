# Databricks notebook source
# MAGIC %md
# MAGIC # REF_Reconcile_ReferenceData - reconciliation of every 06_reference_data target
# MAGIC
# MAGIC For each Delta table loaded by the 14 `REF_Load_*` notebooks this notebook computes
# MAGIC * the row count (total, excluding reserved members, and for the current `BatchId` via `LastLoadBatchId` where present),
# MAGIC * a deterministic content hash: `sum(xxhash64(concat_ws('|', <all business columns sorted by name>)))` over the rows
# MAGIC   (audit columns `ValidFrom`, `ValidTo`, `LastLoadBatchId`, `LineageKey`, `RejectedAtUtc`, `RejectId` are excluded so a
# MAGIC   re-run of the same BusinessDate yields the same hash),
# MAGIC * the ports of `validation/runtime/02_row_count_reconciliation.sql` (variance = source - target - rejects per hop) and
# MAGIC   `03_dimension_fact_integrity.sql` #7 (Date dimension coverage) and the reserved-member checks of `90_unknown_members.sql`,
# MAGIC
# MAGIC and compares them with the SQL Server baseline. The baseline is supplied either as a Delta table
# MAGIC (`BaselineTable` parameter, default `${catalog}.etl.reference_baseline`) with columns
# MAGIC `LegacyObjectName, BaselineRowCount, BaselineHash` captured from SQL Server with the queries printed at the bottom,
# MAGIC or inline as the `BaselineJson` parameter (`[{"LegacyObjectName": "ref.Currency", "BaselineRowCount": 42, "BaselineHash": null}]`).
# MAGIC Results go to `etl.row_count_log` through `control.logRowCount` (sourceRowCount = baseline, targetRowCount = Delta,
# MAGIC rejectRowCount = |difference|), and the notebook raises when `FailOnVariance` is `True` and any object differs.
# MAGIC
# MAGIC Parameters: `BatchId`, `BusinessDate`, `EnvironmentCode`, `catalog`, `BaselineTable`, `BaselineJson`, `FailOnVariance`.

# COMMAND ----------

import json
import os
import sys


def bundleSourcePath():
    try:
        notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return os.path.join("/Workspace", os.path.dirname(os.path.dirname(notebookPath)).lstrip("/"), "src")
    except Exception:
        return os.path.abspath(os.path.join(os.getcwd(), "..", "src"))


if bundleSourcePath() not in sys.path:
    sys.path.insert(0, bundleSourcePath())

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, params, naming  # noqa: E402,F401

from wwi_ref import runtime, schemas  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "REF_Reconcile_ReferenceData"
p = params.getJobParams(dbutils)
baselineTable = runtime.widgetOrDefault(dbutils, "BaselineTable", "") or naming.table(p["catalog"], "etl", "reference_baseline")
baselineJson = runtime.widgetOrDefault(dbutils, "BaselineJson", "")
failOnVariance = (runtime.widgetOrDefault(dbutils, "FailOnVariance", "False") or "False").lower() in ("true", "1", "yes", "y")

AUDIT_COLUMNS = {"ValidFrom", "ValidTo", "LastLoadBatchId", "LineageKey", "RejectedAtUtc", "RejectId", "ModifiedAtUtc"}
TARGETS = (list(schemas.REF_TABLES) + list(schemas.ERR_TABLES) + list(schemas.ownedDimensions()))

# COMMAND ----------


def contentHash(df):
    """Deterministic hash: xxhash64 of the pipe-joined string form of every business column (sorted by name), summed."""
    cols = sorted(c for c in df.columns if c not in AUDIT_COLUMNS)
    hashed = df.select(F.xxhash64(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols])).alias("h"))
    return hashed.agg(F.sum("h")).first()[0]


def profile(spark, catalog, legacyName, batchId):
    fqnName = schemas.fqn(catalog, legacyName)
    df = spark.table(fqnName)
    out = {"LegacyObjectName": legacyName, "DeltaTable": fqnName, "RowCount": df.count(), "ContentHash": contentHash(df)}
    if "IsReservedMember" in df.columns:
        out["BusinessRowCount"] = df.where(~F.col("IsReservedMember")).count()
    elif legacyName in schemas.DIMENSIONS:
        keyCol = schemas.dimensionKeyColumn(legacyName)
        out["BusinessRowCount"] = df.where(F.col(keyCol) > 0).count()
        out["ReservedMemberCount"] = df.where(F.col(keyCol) < 0).count()
        out["ReservedMemberKeys"] = sorted(r[keyCol] for r in df.where(F.col(keyCol) < 0).select(keyCol).collect())
    if "LastLoadBatchId" in df.columns:
        out["BatchRowCount"] = df.where(F.col("LastLoadBatchId") == batchId).count()
    elif "BatchId" in df.columns:
        out["BatchRowCount"] = df.where(F.col("BatchId") == batchId).count()
    if "IsCurrentRow" in df.columns:
        out["CurrentRowCount"] = df.where(F.col("IsCurrentRow")).count()
    return out


def loadBaseline(spark, baselineTable, baselineJson):
    if baselineJson:
        rows = json.loads(baselineJson)
        return {r["LegacyObjectName"]: r for r in rows}
    if spark.catalog.tableExists(baselineTable):
        return {r["LegacyObjectName"]: r.asDict() for r in spark.table(baselineTable).collect()}
    return {}


def compare(profiles, baseline):
    results = []
    for prof in profiles:
        base = baseline.get(prof["LegacyObjectName"])
        baselineCount = None if base is None else base.get("BaselineRowCount")
        baselineHash = None if base is None else base.get("BaselineHash")
        rowVariance = None if baselineCount is None else prof["RowCount"] - int(baselineCount)
        hashMatch = None if baselineHash in (None, "") else int(baselineHash) == prof["ContentHash"]
        results.append(dict(prof, BaselineRowCount=baselineCount, RowVariance=rowVariance, HashMatch=hashMatch,
                            Status="NO_BASELINE" if base is None else ("MATCH" if (rowVariance == 0 and hashMatch is not False) else "VARIANCE")))
    return results


def dateCoverage(spark, catalog):
    """03_dimension_fact_integrity.sql #7 - Date dimension coverage."""
    d = spark.table(schemas.fqn(catalog, "Dimension.Date")).where(~F.col("IsReservedMember"))
    row = d.agg(F.min("Date").alias("FirstDate"), F.max("Date").alias("LastDate"), F.count(F.lit(1)).alias("DateRows")).first()
    gapFree = (row["DateRows"] == (row["LastDate"] - row["FirstDate"]).days + 1) if row["DateRows"] else False
    return {"FirstDate": str(row["FirstDate"]), "LastDate": str(row["LastDate"]), "DateRows": row["DateRows"], "GapFree": gapFree}


def hopVariances(spark, catalog, batchId):
    """02_row_count_reconciliation.sql #1 - variance per hop logged by this batch (source - target - rejects)."""
    rowCountLog = naming.table(catalog, "etl", "row_count_log")
    packageExecution = naming.table(catalog, "etl", "package_execution")
    if not (spark.catalog.tableExists(rowCountLog) and spark.catalog.tableExists(packageExecution)):
        return []
    rows = spark.sql("""
        SELECT pe.PackageName, r.ObjectName, r.SourceRowCount, r.TargetRowCount, r.RejectRowCount,
               COALESCE(r.SourceRowCount, 0) - COALESCE(r.TargetRowCount, 0) - COALESCE(r.RejectRowCount, 0) AS VarianceRowCount
        FROM %s r JOIN %s pe ON pe.PackageExecutionId = r.PackageExecutionId
        WHERE pe.BatchId = %d AND pe.PackageName LIKE 'REF_Load_%%'
        ORDER BY ABS(COALESCE(r.SourceRowCount, 0) - COALESCE(r.TargetRowCount, 0) - COALESCE(r.RejectRowCount, 0)) DESC
    """ % (rowCountLog, packageExecution, batchId)).collect()
    return [r.asDict() for r in rows]


# COMMAND ----------

with runtime.packageRun(spark, p, PACKAGE_NAME) as ctx:
    ctx.step("Profile targets")
    profiles = [profile(spark, p["catalog"], name, ctx.batchId) for name in TARGETS]
    baseline = loadBaseline(spark, baselineTable, baselineJson)
    results = compare(profiles, baseline)

    ctx.step("Log to etl.row_count_log")
    for r in results:
        control.logRowCount(spark, p["catalog"], ctx.packageExecutionId, "reconcile:%s" % r["LegacyObjectName"],
                            sourceRowCount=r["BaselineRowCount"], targetRowCount=r["RowCount"],
                            rejectRowCount=None if r["RowVariance"] is None else abs(r["RowVariance"]))

    ctx.step("Reserved members and calendar coverage")
    reservedProblems = [r["LegacyObjectName"] for r in results
                        if "ReservedMemberKeys" in r and r["ReservedMemberKeys"] != [-9, -3, -2, -1]]
    coverage = dateCoverage(spark, p["catalog"])
    hops = hopVariances(spark, p["catalog"], ctx.batchId)
    for legacyName in reservedProblems:
        ctx.logWarning("reserved members missing on %s" % legacyName, sourceComponent="Reserved members", errorCode="REF_RESERVED_MISSING")
    if not coverage["GapFree"]:
        ctx.logWarning("Dimension.Date has gaps: %s" % coverage, sourceComponent="Date coverage", errorCode="REF_DATE_GAP")

    report = {"batchId": ctx.batchId, "businessDate": str(p["businessDate"]), "baselineSource": "json" if baselineJson else baselineTable,
              "objects": results, "dateCoverage": coverage, "reservedMemberProblems": reservedProblems, "hopVariances": hops}
    print(json.dumps(report, indent=2, default=str))
    variances = [r["LegacyObjectName"] for r in results if r["Status"] == "VARIANCE"]
    ctx.rowsRead = len(results)
    ctx.rowsRejected = len(variances)
    if variances and failOnVariance:
        raise AssertionError("row count / hash variance against baseline: %s" % ", ".join(variances))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capturing the SQL Server baseline
# MAGIC Run per legacy object on `WideWorldImporters_Staging` (ref/err) or `WideWorldImportersDW` (Dimension) and load the rows into
# MAGIC `${catalog}.etl.reference_baseline (LegacyObjectName STRING, BaselineRowCount BIGINT, BaselineHash BIGINT)`:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT N'ref.Currency' AS LegacyObjectName, COUNT(*) AS BaselineRowCount, NULL AS BaselineHash FROM ref.Currency;
# MAGIC SELECT N'Dimension.Currency', COUNT(*), NULL FROM Dimension.Currency;
# MAGIC SELECT N'Dimension.Date', COUNT(*), NULL FROM Dimension.Date;     -- 03_dimension_fact_integrity.sql #7
# MAGIC ```
# MAGIC `BaselineHash` is optional: T-SQL has no xxhash64, so the content hash is used Delta-vs-Delta (e.g. before/after a re-run,
# MAGIC or against a one-off Delta copy of the SQL Server table loaded via JDBC into the same column layout).
