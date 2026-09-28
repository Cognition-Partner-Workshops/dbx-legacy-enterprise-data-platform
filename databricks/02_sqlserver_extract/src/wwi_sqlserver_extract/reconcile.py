"""Row-count / hash reconciliation of the bronze targets loaded by this project.

Ports validation/runtime/02_row_count_reconciliation.sql to Spark SQL:

    1. every hop logged for the batch, worst variance first
    2. hops whose variance is outside RowCountVarianceTolerancePercent
    3. packages that succeeded but logged no row count
    5. rejected rows that were never reprocessed

plus a landed-versus-baseline comparison (query 4) driven by the SQL Server
figures supplied as a Delta table or a JSON parameter.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from wwi_sqlserver_extract import specs
from wwi_sqlserver_extract.extract import AUDIT_COLUMNS, BRONZE_SCHEMA

DEFAULT_TOLERANCE_PERCENT = Decimal("0.5")
TOLERANCE_CONFIGURATION_KEY = "RowCountVarianceTolerancePercent"
NULL_TOKEN = "<null>"

BASELINE_SCHEMA = T.StructType(
    [
        T.StructField("ObjectName", T.StringType(), False),
        T.StructField("SourceRowCount", T.LongType(), True),
        T.StructField("SourceRowHash", T.LongType(), True),
    ]
)

RESULT_SCHEMA = T.StructType(
    [
        T.StructField("PackageName", T.StringType(), False),
        T.StructField("ObjectName", T.StringType(), False),
        T.StructField("TableName", T.StringType(), False),
        T.StructField("BatchId", T.LongType(), True),
        T.StructField("TargetRowCount", T.LongType(), True),
        T.StructField("BatchRowCount", T.LongType(), True),
        T.StructField("TargetRowHash", T.LongType(), True),
        T.StructField("SourceRowCount", T.LongType(), True),
        T.StructField("SourceRowHash", T.LongType(), True),
        T.StructField("VarianceRowCount", T.LongType(), True),
        T.StructField("VariancePercent", T.DecimalType(9, 4), True),
        T.StructField("TolerancePercent", T.DecimalType(9, 4), True),
        T.StructField("HashMatches", T.BooleanType(), True),
        T.StructField("Status", T.StringType(), False),
    ]
)


@dataclass(frozen=True)
class TargetMetrics:
    objectName: str
    tableName: str
    rowCount: Optional[int]
    batchRowCount: Optional[int]
    rowHash: Optional[int]


def hashColumns(df: DataFrame) -> List[str]:
    return sorted(c for c in df.columns if c not in AUDIT_COLUMNS)


def rowHashExpression(columns: Iterable[str]):
    """Order-independent deterministic hash: bit_xor(xxhash64(concat_ws('|', cols...)))."""
    parts = [F.coalesce(F.col("`%s`" % c).cast("string"), F.lit(NULL_TOKEN)) for c in columns]
    return F.bit_xor(F.xxhash64(F.concat_ws("|", *parts)))


def computeTargetMetrics(
    spark: SparkSession, tableName: str, objectName: str, batchId: Optional[int] = None
) -> TargetMetrics:
    if not spark.catalog.tableExists(tableName):
        return TargetMetrics(objectName, tableName, None, None, None)
    df = spark.table(tableName)
    aggregations = [F.count(F.lit(1)).alias("RowCount"), rowHashExpression(hashColumns(df)).alias("RowHash")]
    if batchId is not None and "BatchId" in df.columns:
        aggregations.append(F.sum(F.when(F.col("BatchId") == F.lit(batchId), 1).otherwise(0)).alias("BatchRowCount"))
    row = df.agg(*aggregations).first()
    batchRowCount = int(row["BatchRowCount"]) if "BatchRowCount" in row.asDict() and row["BatchRowCount"] is not None else None
    return TargetMetrics(objectName, tableName, int(row["RowCount"]), batchRowCount, row["RowHash"])


def parseBaselineJson(text: Optional[str]) -> List[Dict[str, Any]]:
    """Accepts {"bronze.raw_sql_order": 123, ...}, {"bronze.raw_sql_order": {"rowCount": 123, "rowHash": 9}}
    or a list of {"ObjectName": ..., "SourceRowCount": ..., "SourceRowHash": ...} entries."""
    if text is None or text.strip() == "":
        return []
    payload = json.loads(text)
    entries: List[Dict[str, Any]] = []
    if isinstance(payload, dict):
        for objectName, value in payload.items():
            if isinstance(value, dict):
                count = value.get("rowCount", value.get("SourceRowCount"))
                rowHash = value.get("rowHash", value.get("SourceRowHash"))
            else:
                count, rowHash = value, None
            entries.append({"ObjectName": objectName, "SourceRowCount": count, "SourceRowHash": rowHash})
    elif isinstance(payload, list):
        for item in payload:
            entries.append(
                {
                    "ObjectName": item.get("ObjectName", item.get("objectName")),
                    "SourceRowCount": item.get("SourceRowCount", item.get("rowCount")),
                    "SourceRowHash": item.get("SourceRowHash", item.get("rowHash")),
                }
            )
    else:
        raise ValueError("Baseline JSON must be an object or a list")
    for entry in entries:
        entry["SourceRowCount"] = None if entry["SourceRowCount"] is None else int(entry["SourceRowCount"])
        entry["SourceRowHash"] = None if entry["SourceRowHash"] is None else int(entry["SourceRowHash"])
    return entries


def loadBaseline(spark: SparkSession, baselineTable: Optional[str], baselineJson: Optional[str]) -> DataFrame:
    frames = []
    if baselineTable:
        table = spark.table(baselineTable)
        columns = {c.lower(): c for c in table.columns}
        selected = [F.col(columns["objectname"]).cast("string").alias("ObjectName")]
        selected.append(F.col(columns["sourcerowcount"]).cast("bigint").alias("SourceRowCount"))
        if "sourcerowhash" in columns:
            selected.append(F.col(columns["sourcerowhash"]).cast("bigint").alias("SourceRowHash"))
        else:
            selected.append(F.lit(None).cast("bigint").alias("SourceRowHash"))
        frames.append(table.select(*selected))
    entries = parseBaselineJson(baselineJson)
    if entries:
        frames.append(spark.createDataFrame([Row(**e) for e in entries], BASELINE_SCHEMA))
    if not frames:
        return spark.createDataFrame([], BASELINE_SCHEMA)
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def resolveTolerancePercent(spark: SparkSession, catalog: str, control: Any, environmentCode: Optional[str]) -> Decimal:
    try:
        value = control.getConfiguration(spark, catalog, TOLERANCE_CONFIGURATION_KEY, environmentCode=environmentCode)
    except Exception:
        return DEFAULT_TOLERANCE_PERCENT
    if value is None or str(value).strip() == "":
        return DEFAULT_TOLERANCE_PERCENT
    return Decimal(str(value))


def targetObjects() -> List[Dict[str, str]]:
    seen = set()
    result = []
    for spec in specs.PACKAGES.values():
        objectName = "%s.%s" % (BRONZE_SCHEMA, spec.targetTable)
        if objectName in seen:
            continue
        seen.add(objectName)
        result.append({"packageName": spec.name, "objectName": objectName, "table": spec.targetTable})
    return result


def reconcileTargets(
    spark: SparkSession,
    catalog: str,
    naming: Any,
    control: Any,
    packageExecutionId: int,
    baseline: DataFrame,
    batchId: Optional[int],
    tolerancePercent: Decimal,
) -> DataFrame:
    baselineRows = {r["ObjectName"]: r for r in baseline.collect()}
    rows = []
    for target in targetObjects():
        tableName = naming.table(catalog, BRONZE_SCHEMA, target["table"])
        metrics = computeTargetMetrics(spark, tableName, target["objectName"], batchId)
        base = baselineRows.get(target["objectName"]) or baselineRows.get(
            specs.getPackage(target["packageName"]).legacyTarget
        )
        sourceCount = base["SourceRowCount"] if base is not None else None
        sourceHash = base["SourceRowHash"] if base is not None else None
        variance = None
        variancePercent = None
        hashMatches = None
        if sourceCount is not None and metrics.rowCount is not None:
            variance = sourceCount - metrics.rowCount
            if sourceCount != 0:
                variancePercent = (Decimal(100) * Decimal(abs(variance)) / Decimal(sourceCount)).quantize(Decimal("0.0001"))
        if sourceHash is not None and metrics.rowHash is not None:
            hashMatches = sourceHash == metrics.rowHash
        if metrics.rowCount is None:
            status = "MissingTarget"
        elif sourceCount is None:
            status = "NoBaseline"
        elif hashMatches is False:
            status = "HashMismatch"
        elif variance == 0:
            status = "Matched"
        elif variancePercent is not None and variancePercent > tolerancePercent:
            status = "OutsideTolerance"
        else:
            status = "WithinTolerance"
        control.logRowCount(
            spark,
            catalog,
            packageExecutionId,
            target["objectName"],
            sourceRowCount=sourceCount,
            targetRowCount=metrics.rowCount,
            rejectRowCount=0,
        )
        rows.append(
            Row(
                PackageName=target["packageName"],
                ObjectName=target["objectName"],
                TableName=tableName,
                BatchId=batchId,
                TargetRowCount=metrics.rowCount,
                BatchRowCount=metrics.batchRowCount,
                TargetRowHash=metrics.rowHash,
                SourceRowCount=sourceCount,
                SourceRowHash=sourceHash,
                VarianceRowCount=variance,
                VariancePercent=variancePercent,
                TolerancePercent=tolerancePercent.quantize(Decimal("0.0001")),
                HashMatches=hashMatches,
                Status=status,
            )
        )
    return spark.createDataFrame(rows, RESULT_SCHEMA)


def packageNamesSql() -> str:
    return ", ".join("'%s'" % name for name in specs.PACKAGES)


def loggedHopsSql(catalog: str, batchId: int) -> str:
    """Query 1 + 2 of the legacy script against etl.row_count_log / etl.package_execution."""
    return """
SELECT pe.PackageName,
       rcl.ObjectName,
       rcl.SourceRowCount,
       rcl.TargetRowCount,
       rcl.InsertRowCount,
       rcl.UpdateRowCount,
       rcl.DeleteRowCount,
       rcl.RejectRowCount,
       COALESCE(rcl.SourceRowCount, 0) - COALESCE(rcl.TargetRowCount, 0) - COALESCE(rcl.RejectRowCount, 0) AS VarianceRowCount,
       CASE WHEN COALESCE(rcl.SourceRowCount, 0) = 0 THEN NULL
            ELSE CAST(100.0 * ABS(COALESCE(rcl.SourceRowCount, 0) - COALESCE(rcl.TargetRowCount, 0) - COALESCE(rcl.RejectRowCount, 0))
                      / rcl.SourceRowCount AS DECIMAL(9, 4)) END AS VariancePercent,
       rcl.RecordedAtUtc
FROM   {catalog}.etl.row_count_log AS rcl
       INNER JOIN {catalog}.etl.package_execution AS pe
               ON pe.PackageExecutionId = rcl.PackageExecutionId
WHERE  pe.BatchId = {batchId}
  AND  pe.PackageName IN ({packages})
ORDER BY ABS(COALESCE(rcl.SourceRowCount, 0) - COALESCE(rcl.TargetRowCount, 0) - COALESCE(rcl.RejectRowCount, 0)) DESC
""".format(catalog=catalog, batchId=int(batchId), packages=packageNamesSql())


def missingRowCountSql(catalog: str, batchId: int) -> str:
    """Query 3: packages that succeeded but never wrote a row count."""
    return """
SELECT pe.PackageName, pe.BatchId, pe.StartedAtUtc, pe.Status, pe.RowsRead, pe.RowsInserted
FROM   {catalog}.etl.package_execution AS pe
WHERE  pe.BatchId = {batchId}
  AND  pe.PackageName IN ({packages})
  AND  pe.Status = 'Succeeded'
  AND  NOT EXISTS (SELECT 1 FROM {catalog}.etl.row_count_log AS rcl
                   WHERE rcl.PackageExecutionId = pe.PackageExecutionId)
ORDER BY pe.PackageName
""".format(catalog=catalog, batchId=int(batchId), packages=packageNamesSql())


def outstandingRejectsSql(catalog: str, slaDays: int = 3) -> str:
    """Query 5: rejects that were never reprocessed and are older than the runbook SLA."""
    legacyTargets = ", ".join(sorted({"'%s'" % s.legacyTarget for s in specs.PACKAGES.values()}))
    return """
SELECT r.ObjectName, r.RejectStage, r.RejectReasonCode,
       COUNT(*) AS OutstandingRejects, MIN(r.LoggedAtUtc) AS OldestRejectAtUtc
FROM   {catalog}.etl.rejected_record AS r
WHERE  r.IsReprocessed = false
  AND  r.LoggedAtUtc < current_timestamp() - INTERVAL {slaDays} DAYS
  AND  r.ObjectName IN ({targets})
GROUP BY r.ObjectName, r.RejectStage, r.RejectReasonCode
ORDER BY OutstandingRejects DESC
""".format(catalog=catalog, slaDays=int(slaDays), targets=legacyTargets)
