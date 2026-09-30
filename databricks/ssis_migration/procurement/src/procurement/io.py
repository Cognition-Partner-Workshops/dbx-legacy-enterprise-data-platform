"""Delta / control-table helpers. Everything that touches the workspace lives here so the
transformation modules stay pure and testable on local Spark."""

import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from procurement.config import (
    LEGACY_DW_DATABASE,
    LEGACY_SQLSERVER_CONNECTION,
    LOW_TS,
    qualified,
)

WATERMARK_TABLE = "ctl_watermark"
BATCH_TABLE = "ctl_batch"
RUN_LOG_TABLE = "ctl_package_run"
REJECT_TABLE = "err_rejected_row"


def tableExists(spark, tableName):
    return spark.catalog.tableExists(tableName)


def readTable(spark, tableName):
    return spark.table(tableName)


def readTableOrEmpty(spark, tableName, schema: T.StructType):
    if tableExists(spark, tableName):
        return spark.table(tableName)
    return spark.createDataFrame([], schema)


def writeDelta(df: DataFrame, tableName, mode="overwrite", partitionBy=None, mergeSchema=False):
    writer = df.write.format("delta").mode(mode).option("overwriteSchema", mode == "overwrite")
    if mergeSchema:
        writer = writer.option("mergeSchema", "true")
    if partitionBy:
        writer = writer.partitionBy(*partitionBy)
    writer.saveAsTable(tableName)


def writeThroughWork(spark, df: DataFrame, tableName):
    """SCD rewrite that reads and replaces the same table: materialise into a work table first
    (the SSIS `work.*` pattern) so the target swap is a single deterministic overwrite."""
    workTable = f"{tableName}__work"
    writeDelta(df, workTable)
    writeDelta(spark.table(workTable), tableName)
    spark.sql(f"DROP TABLE IF EXISTS {workTable}")


def replaceWhere(df: DataFrame, tableName, predicate):
    """Idempotent window rewrite (the SSIS 'DELETE window then INSERT' pattern)."""
    if not tableExists(df.sparkSession, tableName):
        writeDelta(df, tableName)
        return
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(tableName)
    )


def appendDelta(df: DataFrame, tableName):
    writeDelta(df, tableName, mode="append", mergeSchema=True)


def readLegacySql(spark, sql, database=LEGACY_DW_DATABASE):
    """Federated pass-through query for legacy objects whose names contain spaces
    (`Fact.[Purchase Receipt]`, `Dimension.[Stock Item]` ...) which the three-level
    foreign-catalog path cannot resolve. Still read-only over the federation connection."""
    escaped = sql.replace("'", "''")
    return spark.sql(
        f"SELECT * FROM remote_query('{LEGACY_SQLSERVER_CONNECTION}', "
        f"database => '{database}', query => '{escaped}')"
    )


# --------------------------------------------------------------------------------------
# control tables (etl.Batch / etl.Watermark / etl.PackageExecution equivalents)
# --------------------------------------------------------------------------------------
WATERMARK_SCHEMA = T.StructType(
    [
        T.StructField("source_system_code", T.StringType()),
        T.StructField("object_name", T.StringType()),
        T.StructField("watermark_value", T.StringType()),
        T.StructField("watermark_kind", T.StringType()),
        T.StructField("updated_at", T.TimestampType()),
        T.StructField("batch_id", T.LongType()),
    ]
)

RUN_LOG_SCHEMA = T.StructType(
    [
        T.StructField("batch_id", T.LongType()),
        T.StructField("package_name", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("rows_read", T.LongType()),
        T.StructField("rows_inserted", T.LongType()),
        T.StructField("rows_updated", T.LongType()),
        T.StructField("rows_rejected", T.LongType()),
        T.StructField("watermark_from", T.StringType()),
        T.StructField("watermark_to", T.StringType()),
        T.StructField("message", T.StringType()),
        T.StructField("logged_at", T.TimestampType()),
    ]
)

REJECT_SCHEMA = T.StructType(
    [
        T.StructField("batch_id", T.LongType()),
        T.StructField("package_name", T.StringType()),
        T.StructField("reject_target", T.StringType()),
        T.StructField("reject_reason_code", T.StringType()),
        T.StructField("business_key", T.StringType()),
        T.StructField("payload_json", T.StringType()),
        T.StructField("rejected_at", T.TimestampType()),
    ]
)


def nowUtc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def getWatermark(spark, sourceSystemCode, objectName, default=LOW_TS):
    table = qualified(WATERMARK_TABLE)
    if not tableExists(spark, table):
        return default
    rows = (
        spark.table(table)
        .where((F.col("source_system_code") == sourceSystemCode) & (F.col("object_name") == objectName))
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .collect()
    )
    return rows[0]["watermark_value"] if rows else default


def setWatermark(spark, sourceSystemCode, objectName, value, kind, batchId):
    if value is None:
        return
    row = [(sourceSystemCode, objectName, str(value), kind, nowUtc(), int(batchId))]
    df = spark.createDataFrame(row, WATERMARK_SCHEMA)
    table = qualified(WATERMARK_TABLE)
    if not tableExists(spark, table):
        writeDelta(df, table)
        return
    df.createOrReplaceTempView("_wm_new")
    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING _wm_new AS s
          ON t.source_system_code = s.source_system_code AND t.object_name = s.object_name
        WHEN MATCHED THEN UPDATE SET watermark_value = s.watermark_value, watermark_kind = s.watermark_kind,
                                     updated_at = s.updated_at, batch_id = s.batch_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def newBatchId():
    """SSIS used etl.Batch identity values; here the batch id is a UTC timestamp number
    (yyyyMMddHHmmss) which is monotonic and human readable."""
    return int(nowUtc().strftime("%Y%m%d%H%M%S"))


def logPackageRun(spark, batchId, packageName, status, rowsRead=0, rowsInserted=0, rowsUpdated=0,
                  rowsRejected=0, watermarkFrom=None, watermarkTo=None, message=None):
    row = [
        (
            int(batchId), packageName, status, int(rowsRead), int(rowsInserted), int(rowsUpdated),
            int(rowsRejected), None if watermarkFrom is None else str(watermarkFrom),
            None if watermarkTo is None else str(watermarkTo), message, nowUtc(),
        )
    ]
    appendDelta(spark.createDataFrame(row, RUN_LOG_SCHEMA), qualified(RUN_LOG_TABLE))


def appendRejects(spark, df: DataFrame, batchId, packageName, rejectTarget, reasonCol, keyCol):
    """Land rejected rows in the shared err_rejected_row table (err.Rejected* equivalents).
    Returns the number of rejected rows."""
    if df is None:
        return 0
    payloadCols = [c for c in df.columns if c not in (reasonCol,)]
    rejects = df.select(
        F.lit(int(batchId)).cast("long").alias("batch_id"),
        F.lit(packageName).alias("package_name"),
        F.lit(rejectTarget).alias("reject_target"),
        F.col(reasonCol).cast("string").alias("reject_reason_code"),
        F.col(keyCol).cast("string").alias("business_key"),
        F.to_json(F.struct(*[F.col(c).cast("string").alias(c) for c in payloadCols])).alias("payload_json"),
        F.current_timestamp().alias("rejected_at"),
    )
    count = rejects.count()
    if count:
        appendDelta(rejects, qualified(REJECT_TABLE))
    return count


def jsonDumps(obj):
    return json.dumps(obj, default=str, separators=(",", ":"))


def newRunId():
    return str(uuid.uuid4())


def ensureControlTables(spark):
    """Create the control/error tables up front so downstream readers never miss them."""
    for name, schema in ((WATERMARK_TABLE, WATERMARK_SCHEMA), (RUN_LOG_TABLE, RUN_LOG_SCHEMA), (REJECT_TABLE, REJECT_SCHEMA)):
        table = qualified(name)
        if not tableExists(spark, table):
            writeDelta(spark.createDataFrame([], schema), table)
