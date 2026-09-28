"""Row-count + hash reconciliation for the 03_file_ingestion targets (port of validation/runtime/02_row_count_reconciliation.sql)."""

import json
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from wwi_file_ingestion import feeds, schemas

AUDIT_COLUMNS = {
    "BatchId", "PackageExecutionId", "LoadedAtUtc", "ExtractedAtUtc", "ArrivedAtUtc", "FileModifiedAtUtc",
    "FilePath", "FileSizeBytes", "RejectId", "RejectedAtUtc",
}

BASELINE_SCHEMA = T.StructType([
    T.StructField("ObjectName", T.StringType()),
    T.StructField("BatchId", T.LongType()),
    T.StructField("RowCount", T.LongType()),
    T.StructField("RowHash", T.LongType()),
])

BASELINE_SQL = """
-- SQL Server (WideWorldImporters_Staging): capture per-batch counts + hash for the file feeds.
-- HASHBYTES differs from xxhash64, so the RowHash column is compared only when both sides use the
-- same algorithm; capture RowCount always and RowHash as NULL to compare counts only.
SELECT 'bronze.raw_file_partner_sales' AS ObjectName, BatchId, COUNT(*) AS [RowCount], NULL AS RowHash FROM raw.FilePartnerSales GROUP BY BatchId
UNION ALL SELECT 'bronze.raw_file_carrier_scan', BatchId, COUNT(*), NULL FROM raw.FileCarrierScan GROUP BY BatchId
UNION ALL SELECT 'bronze.raw_file_supplier_catalog', BatchId, COUNT(*), NULL FROM raw.FileSupplierCatalog GROUP BY BatchId
UNION ALL SELECT 'bronze.raw_file_fx_override', BatchId, COUNT(*), NULL FROM raw.FileFxOverride GROUP BY BatchId
UNION ALL SELECT 'silver.err_rejected_file_row', BatchId, COUNT(*), NULL FROM err.RejectedFileRow GROUP BY BatchId;
"""


def targetTables(catalog: str) -> Dict[str, str]:
    names = {"bronze.%s" % t: "%s.bronze.%s" % (catalog, t) for t in schemas.BRONZE_SCHEMAS}
    names["silver.%s" % feeds.ERR_REJECTED_FILE_ROW] = "%s.silver.%s" % (catalog, feeds.ERR_REJECTED_FILE_ROW)
    return names


def businessColumns(df: DataFrame) -> List[str]:
    return sorted(c for c in df.columns if c not in AUDIT_COLUMNS)


def rowHash(df: DataFrame, columns: Sequence[str]):
    """Deterministic, order-independent: sum of xxhash64 over the concatenated (null-safe) business columns."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("\x01")) for c in columns]
    return F.sum(F.xxhash64(F.concat_ws("\x1f", *parts)))


def actualCounts(spark: SparkSession, catalog: str, batchId: Optional[int] = None) -> DataFrame:
    frames = []
    for objectName, fullName in targetTables(catalog).items():
        if not spark.catalog.tableExists(fullName):
            continue
        df = spark.table(fullName)
        if batchId is not None:
            df = df.where(F.col("BatchId") == batchId)
        frames.append(
            df.groupBy("BatchId").agg(
                F.count(F.lit(1)).alias("RowCount"), rowHash(df, businessColumns(df)).alias("RowHash")
            ).select(F.lit(objectName).alias("ObjectName"), F.col("BatchId").cast("long"), "RowCount", "RowHash")
        )
    if not frames:
        return spark.createDataFrame([], BASELINE_SCHEMA)
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out


def loadBaseline(spark: SparkSession, baselineTable: str = "", baselineJson: str = "") -> DataFrame:
    """Baseline from a Delta table or a JSON list [{ObjectName, BatchId, RowCount, RowHash}]; empty when neither is given."""
    if baselineTable and baselineTable.strip():
        return spark.table(baselineTable.strip()).select(
            "ObjectName", F.col("BatchId").cast("long"), F.col("RowCount").cast("long"), F.col("RowHash").cast("long")
        )
    if baselineJson and baselineJson.strip():
        rows = json.loads(baselineJson)
        return spark.createDataFrame(
            [(r["ObjectName"], int(r["BatchId"]), int(r["RowCount"]), r.get("RowHash")) for r in rows], BASELINE_SCHEMA
        )
    return spark.createDataFrame([], BASELINE_SCHEMA)


def compare(actual: DataFrame, baseline: DataFrame) -> DataFrame:
    """Full outer join on ObjectName/BatchId; Status = Match / CountMismatch / HashMismatch / MissingBaseline / MissingActual."""
    a = actual.select("ObjectName", "BatchId", F.col("RowCount").alias("TargetRowCount"), F.col("RowHash").alias("TargetRowHash"))
    b = baseline.select("ObjectName", "BatchId", F.col("RowCount").alias("SourceRowCount"), F.col("RowHash").alias("SourceRowHash"))
    joined = a.join(b, ["ObjectName", "BatchId"], "full_outer")
    status = (
        F.when(F.col("SourceRowCount").isNull(), F.lit("MissingBaseline"))
        .when(F.col("TargetRowCount").isNull(), F.lit("MissingActual"))
        .when(F.col("SourceRowCount") != F.col("TargetRowCount"), F.lit("CountMismatch"))
        .when(F.col("SourceRowHash").isNotNull() & (F.col("SourceRowHash") != F.col("TargetRowHash")), F.lit("HashMismatch"))
        .otherwise(F.lit("Match"))
    )
    return joined.withColumn("Status", status).withColumn(
        "VarianceRowCount", F.coalesce(F.col("SourceRowCount"), F.lit(0)) - F.coalesce(F.col("TargetRowCount"), F.lit(0))
    )


def logResults(spark, control, catalog: str, packageExecutionId: int, comparison: DataFrame) -> int:
    """control.logRowCount per object/batch; returns the number of non-matching rows (MissingBaseline is informational)."""
    failed = 0
    for row in comparison.orderBy("ObjectName", "BatchId").collect():
        control.logRowCount(
            spark, catalog, packageExecutionId, "%s@batch=%s" % (row["ObjectName"], row["BatchId"]),
            sourceRowCount=row["SourceRowCount"], targetRowCount=row["TargetRowCount"],
        )
        if row["Status"] in ("CountMismatch", "HashMismatch", "MissingActual"):
            failed += 1
    return failed
