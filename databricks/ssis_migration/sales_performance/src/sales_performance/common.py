"""Shared Spark helpers: legacy reads, Delta writes, merges, audit columns."""

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_performance import config


def newBatchId():
    return int(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))


def newRunId():
    return str(uuid.uuid4())


def nowUtc():
    return datetime.now(timezone.utc)


def readLegacy(spark: SparkSession, catalog: str, schema: str, table: str, database: str = None) -> DataFrame:
    """Read a legacy object through Lakehouse Federation.

    Objects whose names contain spaces (``Fact.Stock Holding``) do not resolve through the
    foreign catalog, so those fall back to ``remote_query`` against the same connection.
    """
    if " " in table or " " in schema:
        database = database or config.LEGACY_DW_DATABASE
        query = f"SELECT * FROM [{schema}].[{table}]"
        return spark.sql(
            "SELECT * FROM remote_query('{conn}', database => '{db}', query => '{q}')".format(
                conn=config.LEGACY_SQLSERVER_CONNECTION, db=database, q=query.replace("'", "''")
            )
        )
    return spark.table(f"{catalog}.{schema}.`{table}`")


def readDw(spark, schema, table):
    return readLegacy(spark, config.LEGACY_DW, schema, table, config.LEGACY_DW_DATABASE)


def readOltp(spark, schema, table):
    return readLegacy(spark, config.LEGACY_OLTP, schema, table)


def readStaging(spark, schema, table):
    return readLegacy(spark, config.LEGACY_STAGING, schema, table, config.LEGACY_STAGING_DATABASE)


def snakeCase(name: str) -> str:
    out = []
    prev = ""
    for ch in name:
        if ch in " -/":
            out.append("_")
        elif ch.isupper() and prev and (prev.islower() or prev.isdigit()):
            out.append("_" + ch.lower())
        else:
            out.append(ch.lower())
        prev = ch
    text = "".join(out)
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_")


def snakeCaseColumns(df: DataFrame) -> DataFrame:
    return df.select([F.col(f"`{c}`").alias(snakeCase(c)) for c in df.columns])


def withAudit(df: DataFrame, packageName: str, batchId: int) -> DataFrame:
    return (
        df.withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("package_name", F.lit(packageName))
        .withColumn("loaded_at_utc", F.current_timestamp())
    )


def tableExists(spark: SparkSession, fullName: str) -> bool:
    return spark.catalog.tableExists(fullName)


def saveTable(df: DataFrame, name: str, mode: str = "overwrite", partitionBy=None):
    fullName = config.tableName(name)
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    else:
        writer = writer.option("mergeSchema", "true")
    if partitionBy:
        writer = writer.partitionBy(*partitionBy)
    writer.saveAsTable(fullName)
    return fullName


def replaceWindow(df: DataFrame, name: str, predicate: str):
    """Delete/rebuild semantics: replace only the rows matching ``predicate``."""
    fullName = config.tableName(name)
    spark = df.sparkSession
    if not tableExists(spark, fullName):
        return saveTable(df, name)
    (df.write.format("delta").mode("overwrite").option("replaceWhere", predicate).option("mergeSchema", "true").saveAsTable(fullName))
    return fullName


def mergeInto(spark: SparkSession, df: DataFrame, name: str, keys, updateColumns=None, insertAll=True):
    """Upsert ``df`` into the Delta table ``name`` on ``keys`` (DeltaTable.forName(...).merge)."""
    from delta.tables import DeltaTable

    fullName = config.tableName(name)
    if not tableExists(spark, fullName):
        return saveTable(df, name)
    target = DeltaTable.forName(spark, fullName)
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    builder = target.alias("t").merge(df.alias("s"), condition)
    if updateColumns is None:
        builder = builder.whenMatchedUpdateAll()
    else:
        builder = builder.whenMatchedUpdate(set={c: F.col(f"s.`{c}`") for c in updateColumns})
    if insertAll:
        builder = builder.whenNotMatchedInsertAll()
    builder.execute()
    return fullName


def readTable(spark: SparkSession, name: str) -> DataFrame:
    return spark.table(config.tableName(name))


def readTableOrEmpty(spark: SparkSession, name: str, schemaDdl: str) -> DataFrame:
    fullName = config.tableName(name)
    if tableExists(spark, fullName):
        return spark.table(fullName)
    return spark.createDataFrame([], schemaDdl)


def orderIndependentChecksum(df: DataFrame, columns) -> str:
    """SUM of per-row xxhash64 over the given business columns cast to string."""
    if not columns:
        return "0"
    hashed = df.select(F.xxhash64(*[F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("")) for c in columns]).alias("h"))
    row = hashed.agg(F.sum(F.col("h").cast("decimal(38,0)")).alias("s")).collect()[0]
    return str(row["s"]) if row["s"] is not None else "0"
