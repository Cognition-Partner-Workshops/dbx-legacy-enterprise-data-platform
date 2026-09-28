"""Idempotent Delta writes used by the FIN_* notebooks (MERGE / replaceWhere)."""
from __future__ import annotations

import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dbx_etl_common import naming


def mergeInto(spark, targetTable: str, df: DataFrame, keys: list[str], updateColumns: list[str] | None = None) -> int:
    """MERGE df into targetTable on keys. Schema evolution is enabled by the
    notebooks so finance-only columns get added to shared gold facts."""
    view = f"_fin_merge_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    if updateColumns:
        setClause = ", ".join(f"t.`{c}` = s.`{c}`" for c in updateColumns)
        matched = f"WHEN MATCHED THEN UPDATE SET {setClause}"
        notMatched = ""
    else:
        matched = "WHEN MATCHED THEN UPDATE SET *"
        notMatched = "WHEN NOT MATCHED THEN INSERT *"
    spark.sql(f"MERGE INTO {targetTable} AS t USING {view} AS s ON {on} {matched} {notMatched}")
    spark.catalog.dropTempView(view)
    return df.count()


def replaceWhere(df: DataFrame, targetTable: str, predicate: str) -> int:
    """Overwrite only the rows matching predicate (legacy TRUNCATE + INSERT of a
    work table, made re-runnable per batch/period)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .option("mergeSchema", "true")
        .saveAsTable(targetTable)
    )
    return df.count()


def appendRows(df: DataFrame, targetTable: str) -> int:
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(targetTable)
    return df.count()


def controlAccounts(spark, catalog: str, subledgerCode: str) -> DataFrame:
    """Finance.ControlAccount.<subledgerCode>.<LedgerCode> configuration rows ->
    (LedgerCode, ControlAccount). The legacy Fact.Payment [Control Account] was
    stamped by Integration.usp_PostApAging / usp_PostWithholdingTax, whose
    source is not in the repo, so the mapping is configuration-driven."""
    prefix = f"Finance.ControlAccount.{subledgerCode}."
    cfgTable = naming.table(catalog, "etl", "configuration")
    if not spark.catalog.tableExists(cfgTable):
        return spark.createDataFrame([], "LedgerCode STRING, ControlAccount STRING")
    return (
        spark.table(cfgTable)
        .where(F.col("ConfigurationKey").like(prefix.replace(".", "\\.") + "%"))
        .select(
            F.regexp_replace(F.col("ConfigurationKey"), "^" + prefix.replace(".", "\\."), "").alias("LedgerCode"),
            F.col("ConfigurationValue").alias("ControlAccount"),
        )
        .dropDuplicates(["LedgerCode"])
    )
