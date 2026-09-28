"""Idempotent Delta write helpers used by the procurement notebooks.

work.* tables were TRUNCATEd by the packages; here they are overwritten per run. Facts and
aggregates are MERGEd on their natural key so a re-run for the same BatchId / BusinessDate
converges instead of duplicating rows.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _quoted(name: str) -> str:
    return "`%s`" % name.replace("`", "``")


def tableExists(spark: SparkSession, tableName: str) -> bool:
    return spark.catalog.tableExists(tableName)


def overwriteTable(df: DataFrame, tableName: str) -> None:
    """TRUNCATE + reload semantics for work / err tables (schema may evolve between releases)."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tableName)


def replaceWhere(df: DataFrame, tableName: str, predicate: str) -> None:
    """Replace one slice of a table (e.g. one StatementPeriod or one BatchId)."""
    (df.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
       .option("mergeSchema", "true").saveAsTable(tableName))


def mergeInto(spark: SparkSession, df: DataFrame, tableName: str, keyColumns: list[str],
              updateColumns: list[str] | None = None, tempView: str = "_prc_merge_source") -> None:
    """MERGE df into tableName on keyColumns; creates the table on first run.

    updateColumns=None updates every non-key column. Rows present in the source and target are
    updated, new rows inserted; nothing is deleted (the legacy loads were append/update only).
    """
    if not tableExists(spark, tableName):
        df.write.format("delta").mode("overwrite").saveAsTable(tableName)
        return
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    df.createOrReplaceTempView(tempView)
    nonKey = [c for c in df.columns if c not in keyColumns]
    updateCols = updateColumns if updateColumns is not None else nonKey
    onClause = " AND ".join("t.%s <=> s.%s" % (_quoted(c), _quoted(c)) for c in keyColumns)
    setClause = ", ".join("t.%s = s.%s" % (_quoted(c), _quoted(c)) for c in updateCols)
    insertCols = ", ".join(_quoted(c) for c in df.columns)
    insertVals = ", ".join("s.%s" % _quoted(c) for c in df.columns)
    sql = "MERGE INTO %s AS t USING %s AS s ON %s" % (tableName, tempView, onClause)
    if setClause:
        sql += " WHEN MATCHED THEN UPDATE SET %s" % setClause
    sql += " WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)" % (insertCols, insertVals)
    spark.sql(sql)


def updateMatched(spark: SparkSession, df: DataFrame, tableName: str, keyColumns: list[str],
                  updateColumns: list[str], tempView: str = "_prc_update_source") -> None:
    """UPDATE ... FROM semantics: only rows already in the target are touched."""
    if not tableExists(spark, tableName):
        return
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    df.createOrReplaceTempView(tempView)
    onClause = " AND ".join("t.%s <=> s.%s" % (_quoted(c), _quoted(c)) for c in keyColumns)
    setClause = ", ".join("t.%s = s.%s" % (_quoted(c), _quoted(c)) for c in updateColumns)
    spark.sql("MERGE INTO %s AS t USING %s AS s ON %s WHEN MATCHED THEN UPDATE SET %s"
              % (tableName, tempView, onClause, setClause))


def countWhere(spark: SparkSession, tableName: str, predicate: str | None = None) -> int:
    df = spark.table(tableName)
    if predicate:
        df = df.where(predicate)
    return df.count()
