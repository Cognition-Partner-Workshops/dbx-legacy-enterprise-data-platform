"""Delta write helpers shared by the DQ notebooks (idempotent per BatchId).

Rejects are written with ``replaceWhere`` scoped to the batch and to the reason codes the
package owns, so a rerun for the same BatchId replaces exactly its own rows in the shared
``silver.err_*`` table and nothing written by another package. Measures go to
``etl.data_quality_result`` the same way, keyed by BatchId + RuleCode.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dq_quality.naming_map import controlTable

RESULT_SCHEMA = T.StructType([
    T.StructField("BatchId", T.LongType()),
    T.StructField("PackageExecutionId", T.LongType()),
    T.StructField("ObjectName", T.StringType()),
    T.StructField("RuleCode", T.StringType()),
    T.StructField("MeasuredValue", T.DecimalType(18, 4)),
    T.StructField("ThresholdValue", T.DecimalType(18, 4)),
    T.StructField("RowsEvaluated", T.LongType()),
    T.StructField("ResultStatus", T.StringType()),
    T.StructField("RegionCode", T.StringType()),
    T.StructField("DetailText", T.StringType()),
    T.StructField("EvaluatedAtUtc", T.TimestampType()),
])


def sqlLiteral(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def inList(values: Sequence) -> str:
    return "(" + ", ".join(sqlLiteral(v) for v in values) + ")"


def tableExists(spark, tableName: str) -> bool:
    return spark.catalog.tableExists(tableName)


def alignToTable(spark, df: DataFrame, tableName: str) -> DataFrame:
    """Keep only the columns the target table has (in table order), casting to its types."""
    target = spark.table(tableName).schema
    names = set(df.columns)
    selected = [F.col(f.name).cast(f.dataType).alias(f.name) for f in target.fields if f.name in names]
    return df.select(*selected)


def writeReplaceWhere(spark, df: DataFrame, tableName: str, predicate: str) -> int:
    """Overwrite exactly the rows matching ``predicate`` with ``df`` (Delta ``replaceWhere``)."""
    aligned = alignToTable(spark, df, tableName) if tableExists(spark, tableName) else df
    count = aligned.count()
    (aligned.write.format("delta").mode("overwrite")
     .option("replaceWhere", predicate).saveAsTable(tableName))
    return count


def writeRejects(spark, rejectedDf: DataFrame, tableName: str, batchId: int,
                 reasonCodes: Sequence[str], rejectStage: str = "Quality") -> int:
    """Write this package's reject rows for the batch, replacing its previous attempt."""
    predicate = "BatchId = %d AND RejectReasonCode IN %s" % (int(batchId), inList(list(reasonCodes)))
    scoped = rejectedDf.filter(F.col("RejectReasonCode").isin(list(reasonCodes)))
    if "RejectStage" not in scoped.columns:
        scoped = scoped.withColumn("RejectStage", F.lit(rejectStage))
    if "RejectedAtUtc" not in scoped.columns:
        scoped = scoped.withColumn("RejectedAtUtc", F.current_timestamp())
    return writeReplaceWhere(spark, scoped, tableName, predicate)


def writeResults(spark, catalog: str, rows: Sequence[dict], batchId: int) -> int:
    """Upsert ``etl.data_quality_result`` rows for the batch (replace the same RuleCodes)."""
    if not rows:
        return 0
    table = controlTable(catalog, "data_quality_result")
    df = spark.createDataFrame([tuple(r.get(f.name) for f in RESULT_SCHEMA.fields) for r in rows], RESULT_SCHEMA)
    codes = sorted({r["RuleCode"] for r in rows})
    predicate = "BatchId = %d AND RuleCode IN %s" % (int(batchId), inList(codes))
    return writeReplaceWhere(spark, df, table, predicate)


def measureRow(batchId: int, packageExecutionId: Optional[int], objectName: str, ruleCode: str,
               measuredValue, thresholdValue=None, rowsEvaluated: Optional[int] = None,
               regionCode: Optional[str] = None, detailText: Optional[str] = None,
               resultStatus: Optional[str] = None) -> dict:
    """``record_measure`` from the generator: one custom measure row for etl.data_quality_result."""
    if resultStatus is None:
        if measuredValue is None or float(measuredValue) < 0:
            resultStatus = "NotEvaluated"
        elif thresholdValue is not None and float(measuredValue) > float(thresholdValue):
            resultStatus = "Warned"
        else:
            resultStatus = "Passed"
    return {
        "BatchId": int(batchId),
        "PackageExecutionId": packageExecutionId,
        "ObjectName": objectName,
        "RuleCode": ruleCode,
        "MeasuredValue": measuredValue,
        "ThresholdValue": thresholdValue,
        "RowsEvaluated": rowsEvaluated,
        "ResultStatus": resultStatus,
        "RegionCode": regionCode,
        "DetailText": detailText,
        "EvaluatedAtUtc": datetime.now(timezone.utc).replace(tzinfo=None),
    }


def registerRejects(spark, catalog: str, rejectedDf: DataFrame, objectName: str, batchId: int,
                    packageExecutionId: Optional[int], sourceSystemCode: Optional[str],
                    businessKeyColumn: str, logRejectedRecordSet, rejectStage: str = "Quality") -> int:
    """``register_rejects`` cursor -> one ``control.logRejectedRecordSet`` call per reason code."""
    total = 0
    codes = [r[0] for r in rejectedDf.select("RejectReasonCode").distinct().collect()]
    for code in sorted(c for c in codes if c is not None):
        subset = rejectedDf.filter(F.col("RejectReasonCode") == code)
        total += int(logRejectedRecordSet(spark, catalog, objectName, subset, batchId=batchId,
                                          packageExecutionId=packageExecutionId,
                                          sourceSystemCode=sourceSystemCode, rejectStage=rejectStage,
                                          rejectReasonCode=code, businessKeyColumn=businessKeyColumn) or 0)
    return total


def withPayload(df: DataFrame, excludeColumns: Sequence[str] = ()) -> DataFrame:
    """Serialise the screened row as JSON into ``RecordPayload`` (legacy RecordPayload/PayloadJson)."""
    cols = [c for c in df.columns if c not in excludeColumns and not c.startswith("__")]
    return df.withColumn("RecordPayload", F.to_json(F.struct(*[F.col(c) for c in cols])))


def ensureColumns(spark, tableName: str, columns: dict) -> list:
    """Add missing nullable columns (``{name: sqlType}``) to a Delta table; returns those added."""
    existing = {c.lower() for c in spark.table(tableName).columns}
    added = []
    for name, sqlType in columns.items():
        if name.lower() not in existing:
            spark.sql("ALTER TABLE %s ADD COLUMNS (%s %s)" % (tableName, name, sqlType))
            added.append(name)
    return added


def configurationDecimal(getConfiguration, spark, catalog: str, key: str, environmentCode: Optional[str], default):
    """``control.getConfiguration`` with a fallback when the key is absent or not numeric."""
    try:
        raw = getConfiguration(spark, catalog, key, environmentCode=environmentCode)
        return Decimal(str(raw).strip()) if raw is not None and str(raw).strip() != "" else Decimal(str(default))
    except (InvalidOperation, ValueError, TypeError, Exception):  # noqa: BLE001 - missing key -> legacy constant
        return Decimal(str(default))
