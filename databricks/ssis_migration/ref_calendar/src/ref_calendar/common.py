"""Spark helpers shared by the extract, staging, reference and dimension modules."""
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ref_calendar import config


def newBatchId() -> int:
    """Epoch-millisecond batch id, the Databricks stand-in for etl.Batch's identity column."""
    return int(time.time() * 1000)


def addAuditColumns(df: DataFrame, batchId: int, sourceSystemCode: str) -> DataFrame:
    """Append the landing-table control columns every raw.* table carries in the legacy estate."""
    rowNumber = F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
    return (
        df.withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("source_system_code", F.lit(sourceSystemCode))
        .withColumn("source_row_number", rowNumber)
        .withColumn("loaded_at_utc", F.current_timestamp())
    )


def snakeCaseColumns(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.lower() for c in df.columns])


def writeTable(df: DataFrame, tableName: str, mode: str = "overwrite", replaceWhere: str | None = None) -> None:
    """Delta write with schema evolution; `replaceWhere` implements the legacy DELETE-then-INSERT idiom."""
    writer = df.write.format("delta").mode(mode)
    if replaceWhere:
        writer = writer.option("replaceWhere", replaceWhere)
    else:
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(tableName)


def tableExists(spark: SparkSession, tableName: str) -> bool:
    return spark.catalog.tableExists(tableName)


def readTableOrEmpty(spark: SparkSession, tableName: str, schema: T.StructType) -> DataFrame:
    if tableExists(spark, tableName):
        return spark.table(tableName)
    return spark.createDataFrame([], schema)


def readLegacyDw(spark: SparkSession, schemaName: str, tableName: str, database: str = config.LEGACY_DW_DATABASE) -> DataFrame:
    """Read a legacy DW table; names with spaces are pushed through remote_query because the foreign
    catalog cannot resolve them as three-level identifiers."""
    if " " not in tableName:
        return spark.table(f"{config.LEGACY_DW}.{schemaName}.{tableName}")
    query = f"SELECT * FROM [{schemaName}].[{tableName}]"
    return spark.sql(
        f"SELECT * FROM remote_query('{config.LEGACY_SQLSERVER_CONNECTION}', "
        f"database => '{database}', query => '{query}')"
    )


def cleanString(col):
    """stg.ufn_CleanString(x, 0): trim, collapse double spaces, NULL when empty."""
    cleaned = F.trim(F.regexp_replace(col, r"\s{2,}", " "))
    return F.when(F.length(cleaned) == 0, F.lit(None)).otherwise(cleaned)


def trimUpper(col):
    return F.upper(F.trim(col))


def nullIfBlank(col):
    trimmed = F.trim(col)
    return F.when((trimmed.isNull()) | (F.length(trimmed) == 0), F.lit(None)).otherwise(trimmed)


def safeDate(col):
    """stg.ufn_SafeDate: tolerant parse of the landed NVARCHAR dates (ISO date, ISO timestamp, Oracle DD-MON-YY)."""
    s = F.trim(col.cast("string"))
    return F.coalesce(
        F.to_date(s, "yyyy-MM-dd"),
        F.to_date(F.substring(s, 1, 10), "yyyy-MM-dd"),
        F.to_date(s, "dd-MMM-yy"),
        F.to_date(s, "MM/dd/yyyy"),
    )


def yesNo(cond):
    return F.when(cond, F.lit("Y")).otherwise(F.lit("N"))


def rowHash(*cols) -> F.Column:
    """SHA-256 over pipe-separated, NULL-as-empty string values (mirrors HASHBYTES(SHA2_256, CONCAT_WS('|', ISNULL(...)))."""
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) if isinstance(c, str) else c for c in cols]), 256)


def emptyLike(spark: SparkSession, schema: T.StructType) -> DataFrame:
    return spark.createDataFrame([], schema)


def logRowCount(spark: SparkSession, batchId: int, packageName: str, objectName: str, **counts) -> None:
    """etl.usp_LogRowCount equivalent: one row per (batch, package, object) in etl_row_count_log."""
    row = {
        "batch_id": batchId,
        "package_name": packageName,
        "object_name": objectName,
        "source_row_count": int(counts.get("source", 0)),
        "target_row_count": int(counts.get("target", 0)),
        "reject_row_count": int(counts.get("reject", 0)),
        "logged_at_utc": None,
    }
    schema = T.StructType([
        T.StructField("batch_id", T.LongType()), T.StructField("package_name", T.StringType()),
        T.StructField("object_name", T.StringType()), T.StructField("source_row_count", T.LongType()),
        T.StructField("target_row_count", T.LongType()), T.StructField("reject_row_count", T.LongType()),
        T.StructField("logged_at_utc", T.TimestampType()),
    ])
    df = spark.createDataFrame([row], schema).withColumn("logged_at_utc", F.current_timestamp())
    writeTable(df, config.tbl("etl_row_count_log"), mode="append")
