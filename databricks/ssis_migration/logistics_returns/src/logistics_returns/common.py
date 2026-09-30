"""Shared Spark helpers: table lifecycle, watermarks, audit rows and legacy source access.

These are the Databricks equivalents of the etl.* control procedures the SSIS packages call
(etl.usp_GetWatermark / usp_SetWatermark / usp_LogRowCount) and of the stg.ufn_* scalar functions.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from logistics_returns.config import (
    LEGACY_DW_DATABASE,
    LEGACY_SQLSERVER_CONNECTION,
    LEGACY_STAGING_DATABASE,
    RunContext,
    Tables,
)

WATERMARK_MIN_TIMESTAMP = "1900-01-01 00:00:00"

# ---------------------------------------------------------------------------------------------
# Table lifecycle
# ---------------------------------------------------------------------------------------------


def tableExists(spark: SparkSession, qualifiedName: str) -> bool:
    return spark.catalog.tableExists(qualifiedName.replace("`", ""))


def appendTable(df: DataFrame, qualifiedName: str) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(qualifiedName)


def overwriteTable(df: DataFrame, qualifiedName: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(qualifiedName)


def ensureTable(spark: SparkSession, qualifiedName: str, schema: T.StructType) -> None:
    """Create an empty Delta table if missing; tolerant of parallel tasks racing to create the same control table."""
    if tableExists(spark, qualifiedName):
        return
    try:
        spark.createDataFrame([], schema).write.format("delta").saveAsTable(qualifiedName)
    except AnalysisException as error:
        if "ALREADY_EXISTS" not in str(error):
            raise


def readTableOrEmpty(spark: SparkSession, qualifiedName: str, schema: T.StructType) -> DataFrame:
    if tableExists(spark, qualifiedName):
        return spark.table(qualifiedName)
    return spark.createDataFrame([], schema)


def countRows(spark: SparkSession, qualifiedName: str) -> int:
    if not tableExists(spark, qualifiedName):
        return 0
    return spark.table(qualifiedName).count()


def mergeInto(
    spark: SparkSession,
    targetName: str,
    source: DataFrame,
    keyColumns: list[str],
    updateSet: dict[str, str] | None,
    insertColumns: list[str] | None = None,
    updateCondition: str | None = None,
) -> None:
    """Delta MERGE with an explicit UPDATE SET map (values are SQL expressions over ``s``/``t``)."""
    viewName = f"merge_src_{abs(hash(targetName)) % 10_000_000}"
    source.createOrReplaceTempView(viewName)
    onClause = " AND ".join(f"t.`{c}` = s.`{c}`" for c in keyColumns)
    insertCols = insertColumns or source.columns
    insertList = ", ".join(f"`{c}`" for c in insertCols)
    insertValues = ", ".join(f"s.`{c}`" for c in insertCols)
    sql = f"MERGE INTO {targetName} AS t USING {viewName} AS s ON {onClause}\n"
    if updateSet:
        setClause = ", ".join(f"t.`{k}` = {v}" for k, v in updateSet.items())
        condition = f" AND ({updateCondition})" if updateCondition else ""
        sql += f"WHEN MATCHED{condition} THEN UPDATE SET {setClause}\n"
    sql += f"WHEN NOT MATCHED THEN INSERT ({insertList}) VALUES ({insertValues})"
    spark.sql(sql)


# ---------------------------------------------------------------------------------------------
# Control framework: watermarks + row-count audit (etl.Watermark / etl.RowCountAudit)
# ---------------------------------------------------------------------------------------------

WATERMARK_SCHEMA = T.StructType(
    [
        T.StructField("source_system_code", T.StringType()),
        T.StructField("object_name", T.StringType()),
        T.StructField("watermark_type", T.StringType()),
        T.StructField("last_value", T.StringType()),
        T.StructField("previous_value", T.StringType()),
        T.StructField("last_loaded_at_utc", T.TimestampType()),
        T.StructField("last_package_execution_id", T.LongType()),
    ]
)

ROW_COUNT_AUDIT_SCHEMA = T.StructType(
    [
        T.StructField("package_execution_id", T.LongType()),
        T.StructField("batch_id", T.LongType()),
        T.StructField("package_name", T.StringType()),
        T.StructField("object_name", T.StringType()),
        T.StructField("metric_name", T.StringType()),
        T.StructField("metric_value", T.LongType()),
        T.StructField("logged_at_utc", T.TimestampType()),
    ]
)


def getWatermark(ctx: RunContext, sourceSystemCode: str, objectName: str, watermarkType: str) -> str | None:
    """Return the stored ``last_value`` or ``None`` when absent / ``reloadFullHistory`` is set."""
    if ctx.reloadFullHistory:
        return None
    name = ctx.table(Tables.ctlWatermark)
    ensureTable(ctx.spark, name, WATERMARK_SCHEMA)
    rows = (
        ctx.spark.table(name)
        .where((F.col("source_system_code") == sourceSystemCode) & (F.col("object_name") == objectName))
        .select("last_value")
        .limit(1)
        .collect()
    )
    if not rows or rows[0][0] is None:
        return None
    return str(rows[0][0])


def setWatermark(ctx: RunContext, sourceSystemCode: str, objectName: str, watermarkType: str, newValue: str | None) -> None:
    if newValue is None:
        return
    name = ctx.table(Tables.ctlWatermark)
    ensureTable(ctx.spark, name, WATERMARK_SCHEMA)
    row = ctx.spark.createDataFrame(
        [(sourceSystemCode, objectName, watermarkType, str(newValue), None, ctx.startedAtUtc, ctx.packageExecutionId)],
        WATERMARK_SCHEMA,
    )
    mergeInto(
        ctx.spark,
        name,
        row,
        ["source_system_code", "object_name"],
        {
            "previous_value": "t.last_value",
            "last_value": "s.last_value",
            "watermark_type": "s.watermark_type",
            "last_loaded_at_utc": "s.last_loaded_at_utc",
            "last_package_execution_id": "s.last_package_execution_id",
        },
    )


def logRowCount(ctx: RunContext, packageName: str, objectName: str, metrics: dict[str, int]) -> None:
    name = ctx.table(Tables.ctlRowCountAudit)
    ensureTable(ctx.spark, name, ROW_COUNT_AUDIT_SCHEMA)
    rows = [
        (ctx.packageExecutionId, ctx.batchId, packageName, objectName, metric, int(value), ctx.startedAtUtc)
        for metric, value in metrics.items()
    ]
    appendTable(ctx.spark.createDataFrame(rows, ROW_COUNT_AUDIT_SCHEMA), name)


# ---------------------------------------------------------------------------------------------
# Legacy source access (Lakehouse Federation, with remote_query for names federation cannot bind)
# ---------------------------------------------------------------------------------------------


def readLegacy(spark: SparkSession, catalog: str, schema: str, table: str) -> DataFrame:
    """Read a legacy table through the foreign catalog.

    Some objects whose names contain spaces (``Fact.Order Fulfilment``, ``Dimension.Stock Item`` ...) do not
    bind through the three-level name on this workspace, so those fall back to ``remote_query`` against the
    same read-only SQL Server connection.
    """
    try:
        return spark.table(f"`{catalog}`.`{schema}`.`{table}`")
    except Exception as exc:  # noqa: BLE001 - AnalysisException class differs between Spark versions
        if "TABLE_OR_VIEW_NOT_FOUND" not in str(exc):
            raise
    database = LEGACY_DW_DATABASE if catalog.endswith("_dw") else LEGACY_STAGING_DATABASE
    query = f"SELECT * FROM [{schema}].[{table}]".replace("'", "''")
    return spark.sql(f"SELECT * FROM remote_query('{LEGACY_SQLSERVER_CONNECTION}', database => '{database}', query => '{query}')")


def legacyCount(spark: SparkSession, catalog: str, schema: str, table: str) -> int:
    return readLegacy(spark, catalog, schema, table).count()


# ---------------------------------------------------------------------------------------------
# stg.ufn_* scalar functions as Spark column expressions
# ---------------------------------------------------------------------------------------------

_CLEAN_NULL_TOKENS = ["NULL", "N/A", "NA", "?", "-", "UNKNOWN", "."]


def cleanString(col: Column, upper: bool = False) -> Column:
    """stg.ufn_CleanString: trim, collapse whitespace, treat NULL-ish tokens as NULL."""
    cleaned = F.regexp_replace(col.cast("string"), r"[\t\n\r\u0000\u00a0]", " ")
    cleaned = F.trim(F.regexp_replace(cleaned, r" {2,}", " "))
    cleaned = F.when(cleaned == "", F.lit(None)).when(F.upper(cleaned).isin(_CLEAN_NULL_TOKENS), F.lit(None)).otherwise(cleaned)
    return F.upper(cleaned) if upper else cleaned


def upperCode(col: Column) -> Column:
    """NULLIF(UPPER(LTRIM(RTRIM(x))), '')."""
    v = F.upper(F.trim(col.cast("string")))
    return F.when(v == "", F.lit(None)).otherwise(v)


def sourceSystemKey(sourceSystemCode: Column, naturalKey: Column) -> Column:
    """stg.ufn_SourceSystemKey with CollapseRegionalInstances = 1."""
    system = F.upper(F.trim(F.coalesce(sourceSystemCode.cast("string"), F.lit(""))))
    system = (
        F.when(system.isin("ORA_ERP_NA", "ORA_ERP_EU", "ORA_ERP_AP"), F.lit("ORA_ERP"))
        .when(system == "WWI_WEB", F.lit("WWI_OLTP"))
        .otherwise(system)
    )
    key = F.upper(F.trim(F.coalesce(naturalKey.cast("string"), F.lit(""))))
    key = F.when((system == "ORA_ERP") & key.rlike(r"^[0-9]+$"), F.lpad(key, 10, "0")).otherwise(key)
    key = F.regexp_replace(key, r"\|", "/")
    return F.when((key == "") | (system == ""), F.lit(None)).otherwise(F.concat(system, F.lit("|"), key))


def safeDecimal(col: Column, precision: int = 19, scale: int = 6) -> Column:
    """stg.ufn_SafeDecimal for the '.'-separator shape (the only one the SQL Server extracts use)."""
    text = F.upper(F.trim(col.cast("string")))
    negative = text.startswith("(") & text.endswith(")") | text.endswith("CR") | text.endswith("-") | text.startswith("-")
    digits = F.regexp_replace(text, r"[()\sCRDR$%+,-]", "")
    value = F.when(digits.rlike(r"^[0-9]*\.?[0-9]+$"), digits.cast(T.DecimalType(precision, scale)))
    return F.when(col.isNull() | (text == "") | text.isin("NULL", "N/A", "-", "."), F.lit(None)).otherwise(
        F.when(negative, -value).otherwise(value)
    )


def safeTimestamp(col: Column) -> Column:
    """stg.ufn_SafeDate for typed / ISO inputs: sentinel dates and out-of-range values become NULL."""
    ts = F.coalesce(col.cast("timestamp"), F.to_timestamp(col.cast("string")))
    return F.when(
        ts.isNull()
        | (ts < F.lit("1980-01-01").cast("timestamp"))
        | (ts > F.add_months(F.current_timestamp(), 60))
        | F.date_format(ts, "yyyy-MM-dd").isin("1900-01-01", "9999-12-31", "4712-12-31"),
        F.lit(None).cast("timestamp"),
    ).otherwise(ts)


def rowHash(*cols: Column) -> Column:
    """Order-independent-friendly per-row hash over business columns (all cast to string)."""
    return F.xxhash64(F.concat_ws("|", *[F.coalesce(c.cast("string"), F.lit("")) for c in cols]))


def checksum(df: DataFrame, columns: list[str]) -> str | None:
    """SUM of per-row xxhash64 over the given business columns, as a string (matches the evidence contract)."""
    if not columns:
        return None
    row = df.select(F.sum(rowHash(*[F.col(c) for c in columns])).alias("checksum")).collect()[0]
    return None if row[0] is None else str(row[0])


def loadMetadata(ctx: RunContext, sourceSystemCode: str) -> list[Column]:
    """The BatchId / PackageExecutionId / LoadedAtUtc / SourceSystemCode columns every landing table carries."""
    return [
        F.lit(ctx.batchId).cast("long").alias("batch_id"),
        F.lit(ctx.packageExecutionId).cast("long").alias("package_execution_id"),
        F.lit(ctx.startedAtUtc).cast("timestamp").alias("loaded_at_utc"),
        F.lit(sourceSystemCode).alias("source_system_code"),
    ]


def utcNow() -> datetime:
    return datetime.utcnow()


def snakeCase(name: str) -> str:
    """``ShipmentID`` -> ``shipment_id``, ``TotalVolumeM3`` -> ``total_volume_m3``, ``Region Code`` -> ``region_code``."""
    import re

    text = name.strip().replace(" ", "_")
    text = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    return re.sub(r"_+", "_", text).lower()


def snakeCaseColumns(df: DataFrame) -> DataFrame:
    return df.toDF(*[snakeCase(c) for c in df.columns])
