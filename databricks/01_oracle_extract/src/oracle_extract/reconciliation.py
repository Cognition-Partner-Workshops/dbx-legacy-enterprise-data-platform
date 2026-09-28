"""Raw-layer reconciliation: validation/runtime/02_row_count_reconciliation.sql ported to
Spark SQL for the bronze raw_oracle_* tables, plus row-count + deterministic-hash comparison
with the SQL Server baseline (Delta table or JSON parameter)."""
import json
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dbx_etl_common import control, naming

from oracle_extract import specs
from oracle_extract.naming import deltaTable
from oracle_extract.transforms import METADATA_COLUMNS

BASELINE_COLUMNS = ("ObjectName", "RowCount", "HashValue")


def rawTargets() -> List[str]:
    return sorted({spec.legacyTargetTable for spec in specs.PACKAGES.values()})


def tableHashAndCount(spark: SparkSession, fullName: str, batchId: Optional[int] = None) -> Dict[str, object]:
    """Row count and sum(xxhash64(concat_ws of every business column, in column order)) -
    deterministic, order independent, and portable to SQL Server as
    SUM(CAST(HASHBYTES(...) ...)) once the baseline capture query is agreed."""
    df = spark.table(fullName)
    if batchId is not None:
        df = df.where(F.col("BatchId") == F.lit(int(batchId)))
    business = [c for c in df.columns if c not in METADATA_COLUMNS]
    hashed = df.select(
        F.count(F.lit(1)).alias("RowCount"),
        F.sum(F.xxhash64(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business]))).alias("HashValue"),
    ).first()
    return {"RowCount": int(hashed["RowCount"]), "HashValue": None if hashed["HashValue"] is None else int(hashed["HashValue"]),
            "Columns": business}


def loadBaseline(spark: SparkSession, baselineTable: str = "", baselineJson: str = "") -> Dict[str, Dict[str, object]]:
    """Baseline = {ObjectName: {RowCount, HashValue}} captured on SQL Server (legacy names, e.g. 'raw.OracleCustomerMaster')."""
    rows: Dict[str, Dict[str, object]] = {}
    if baselineTable:
        for r in spark.table(baselineTable).select(*BASELINE_COLUMNS).collect():
            rows[r["ObjectName"]] = {"RowCount": r["RowCount"], "HashValue": r["HashValue"]}
    if baselineJson:
        payload = json.loads(baselineJson)
        items = payload if isinstance(payload, list) else [dict(ObjectName=k, **v) for k, v in payload.items()]
        for item in items:
            rows[item["ObjectName"]] = {"RowCount": item.get("RowCount"), "HashValue": item.get("HashValue")}
    return rows


def reconcileRawLayer(spark: SparkSession, catalog: str, batchId: Optional[int], baseline: Dict[str, Dict[str, object]],
                      packageExecutionId: Optional[int] = None, logResults: bool = True) -> List[Dict[str, object]]:
    results = []
    for legacyName in rawTargets():
        fullName = deltaTable(catalog, legacyName)
        exists = spark.catalog.tableExists(fullName)
        measured = tableHashAndCount(spark, fullName, batchId) if exists else {"RowCount": 0, "HashValue": None, "Columns": []}
        expected = baseline.get(legacyName, {})
        expectedCount = expected.get("RowCount")
        expectedHash = expected.get("HashValue")
        status = "NoBaseline" if expectedCount is None else (
            "Match" if int(expectedCount) == measured["RowCount"] and (expectedHash is None or int(expectedHash) == (measured["HashValue"] or 0))
            else "Mismatch")
        results.append({"ObjectName": legacyName, "DeltaTable": fullName, "TableExists": exists,
                        "TargetRowCount": measured["RowCount"], "TargetHash": measured["HashValue"],
                        "BaselineRowCount": expectedCount, "BaselineHash": expectedHash, "Status": status})
        if logResults and packageExecutionId is not None:
            control.logRowCount(spark, catalog, packageExecutionId, legacyName,
                                sourceRowCount=None if expectedCount is None else int(expectedCount),
                                targetRowCount=measured["RowCount"])
    return results


def hopVarianceSql(catalog: str, tolerancePercentExpr: str = "0.5") -> Dict[str, str]:
    """Queries 1-3 and 5 of validation/runtime/02_row_count_reconciliation.sql in Spark SQL
    against the etl.* Delta tables (PascalCase columns kept). Query 4 (raw vs stg counts) is
    replaced by reconcileRawLayer because stg.* is owned by the staging sessions."""
    rcl = naming.table(catalog, "etl", "row_count_log")
    pe = naming.table(catalog, "etl", "package_execution")
    cfg = naming.table(catalog, "etl", "configuration")
    rej = naming.table(catalog, "etl", "rejected_record")
    variance = "(coalesce(rca.SourceRowCount,0) - coalesce(rca.TargetRowCount,0) - coalesce(rca.RejectRowCount,0))"
    return {
        "01_hops_last_day": f"""
SELECT pe.PackageName, rca.ObjectName, rca.SourceRowCount, rca.TargetRowCount, rca.InsertRowCount,
       rca.UpdateRowCount, rca.DeleteRowCount, rca.RejectRowCount, {variance} AS VarianceRowCount, rca.RecordedAtUtc
FROM {rcl} rca JOIN {pe} pe ON pe.PackageExecutionId = rca.PackageExecutionId
WHERE rca.RecordedAtUtc >= current_timestamp() - INTERVAL 1 DAY
ORDER BY abs({variance}) DESC""",
        "02_hops_outside_tolerance": f"""
WITH tol AS (
  SELECT try_cast(max(ConfigurationValue) AS DECIMAL(9,4)) AS TolerancePercent FROM {cfg}
  WHERE ConfigurationKey = 'RowCountVarianceTolerancePercent' AND EnvironmentCode IN ('ALL','DEV','TEST','PROD'))
SELECT pe.PackageName, rca.ObjectName, rca.SourceRowCount, rca.TargetRowCount, rca.RejectRowCount,
       {variance} AS VarianceRowCount,
       CASE WHEN coalesce(rca.SourceRowCount,0) = 0 THEN NULL
            ELSE CAST(100.0 * abs({variance}) / rca.SourceRowCount AS DECIMAL(9,4)) END AS VariancePercent,
       tol.TolerancePercent
FROM {rcl} rca JOIN {pe} pe ON pe.PackageExecutionId = rca.PackageExecutionId CROSS JOIN tol
WHERE rca.RecordedAtUtc >= current_timestamp() - INTERVAL 1 DAY
  AND coalesce(rca.SourceRowCount,0) > 0
  AND 100.0 * abs({variance}) / rca.SourceRowCount > coalesce(tol.TolerancePercent, {tolerancePercentExpr})
ORDER BY VariancePercent DESC""",
        "03_succeeded_without_row_count": f"""
SELECT pe.PackageName, pe.BatchId, pe.StartedAtUtc, pe.Status, pe.RowsRead, pe.RowsInserted
FROM {pe} pe
WHERE pe.StartedAtUtc >= current_timestamp() - INTERVAL 1 DAY AND pe.Status = 'Succeeded'
  AND NOT EXISTS (SELECT 1 FROM {rcl} rca WHERE rca.PackageExecutionId = pe.PackageExecutionId)
ORDER BY pe.PackageName""",
        "05_outstanding_rejects": f"""
SELECT r.ObjectName, r.RejectStage, r.RejectReasonCode, count(*) AS OutstandingRejects, min(r.LoggedAtUtc) AS OldestRejectAtUtc
FROM {rej} r
WHERE coalesce(r.IsReprocessed, false) = false AND r.LoggedAtUtc < current_timestamp() - INTERVAL 3 DAY
GROUP BY r.ObjectName, r.RejectStage, r.RejectReasonCode
ORDER BY OutstandingRejects DESC""",
    }


def resultsFrame(spark: SparkSession, results: List[Dict[str, object]]) -> DataFrame:
    return spark.createDataFrame([{k: v for k, v in r.items()} for r in results])
