"""Bronze (raw_*) Delta writes reproducing the legacy OLE DB destination + the
Execute SQL Task that prepared the target (TRUNCATE / DELETE scope / DELETE window)."""
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from oracle_extract.model import (
    LOAD_APPEND, LOAD_DELETE_SCOPE, LOAD_DELETE_SNAPSHOT, LOAD_DELETE_WINDOW, LOAD_TRUNCATE, ExtractSpec,
)
from oracle_extract.watermark import WatermarkWindow


def tableExists(spark: SparkSession, fullName: str) -> bool:
    return spark.catalog.tableExists(fullName)


def hasColumns(spark: SparkSession, fullName: str, columns) -> bool:
    present = {c.lower() for c in spark.table(fullName).columns}
    return all(c.lower() in present for c in columns)


def deleteOwnRows(spark: SparkSession, spec: ExtractSpec, fullName: str) -> str:
    """Full-history reload of a package: remove only the rows this package wrote.

    Returns the write mode to use afterwards. A package that is the sole writer of its
    bronze table (or the default owner of a shared table nobody else has written to yet)
    overwrites; otherwise its rows are deleted and the re-extract appended."""
    ownership = spec.ownership
    if ownership is None:
        return "overwrite"
    if not hasColumns(spark, fullName, ownership.columns):
        return "overwrite" if ownership.defaultOwner else "append"
    spark.sql(f"DELETE FROM {fullName} WHERE {ownership.predicate}")
    return "append"


def deleteScope(spark: SparkSession, spec: ExtractSpec, fullName: str, extraPredicate: str = "") -> None:
    if not spec.ownership or not hasColumns(spark, fullName, spec.ownership.columns):
        return  # shared table exists but this package never wrote to it: nothing to clear
    spark.sql(f"DELETE FROM {fullName} WHERE {spec.scopePredicate}{extraPredicate}")


def prepareTarget(spark: SparkSession, spec: ExtractSpec, fullName: str, window: Optional[WatermarkWindow],
                  reloadFullHistory: bool, packageExecutionId: Optional[int] = None) -> str:
    """Run the legacy pre-load step and return the Spark write mode to use.

    * ReloadFullHistory=True on an incremental package: the legacy package reset
      the watermark to the epoch and re-extracted everything into the same raw
      table, so the bronze table is overwritten (truncate + append) to avoid
      duplicating history.
    * truncate:        TRUNCATE TABLE raw.X                      -> overwrite
    * delete_scope:    DELETE FROM raw.X WHERE RecordKind = ...  -> delete + append
    * delete_snapshot: DELETE today's RecordKind rows            -> delete + append
    * delete_window:   DELETE FROM raw.X WHERE col in [from, to) -> delete + append
    * append:          plain append of the extracted window
    """
    exists = tableExists(spark, fullName)
    if spec.loadMode == LOAD_TRUNCATE:
        return "overwrite"
    if not exists:
        return "append"
    if spec.isIncremental and reloadFullHistory:
        # legacy: TRUNCATE was not issued, but the epoch re-extract re-inserted every row;
        # replacing this package's rows is the idempotent equivalent for Delta.
        return deleteOwnRows(spark, spec, fullName)
    if spec.loadMode == LOAD_DELETE_SCOPE:
        deleteScope(spark, spec, fullName)
    elif spec.loadMode == LOAD_DELETE_SNAPSHOT:
        deleteScope(spark, spec, fullName, " AND CAST(ExtractedAtUtc AS DATE) = CAST(current_timestamp() AS DATE)")
    elif spec.loadMode == LOAD_DELETE_WINDOW:
        if window is None or window.toValue is None:
            raise ValueError(f"{spec.packageName}: delete_window needs a resolved watermark window")
        spark.sql(
            f"DELETE FROM {fullName} WHERE {spec.windowDeleteColumn} >= DATE'{window.fromText}' "
            f"AND {spec.windowDeleteColumn} < DATE'{window.toText}'"
        )
    elif spec.loadMode == LOAD_APPEND and packageExecutionId is not None:
        # re-run safety for the same package execution (never happens for a fresh id)
        spark.sql(f"DELETE FROM {fullName} WHERE PackageExecutionId = {int(packageExecutionId)}")
    return "append"


def writeBronze(df: DataFrame, fullName: str, mode: str) -> None:
    writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(fullName)


def countRows(spark: SparkSession, fullName: str, predicate: Optional[str] = None) -> int:
    sql = f"SELECT COUNT(*) AS c FROM {fullName}"
    if predicate:
        sql += f" WHERE {predicate}"
    return int(spark.sql(sql).first()[0])
