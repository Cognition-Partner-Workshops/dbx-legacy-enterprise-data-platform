"""Runs one legacy EXT_SQL_* package as a Spark JDBC -> bronze Delta load.

Control flow mirrors the SSIS package:

    Log Package Start        -> control.logPackageStart
    Get Watermark            -> control.getWatermark (honours ReloadFullHistory)
    Read Source Max <Key>    -> readScalar(spec.maxKeySql)
    Extract <Object>         -> JDBC read, conform columns, derived columns, rejects
    Detect Deleted <Object>  -> CHANGETABLE read, DeleteFlag = 'Y'
    Delete/Clear <Rows>      -> Delta overwrite / replaceWhere / MERGE (idempotent)
    Set Watermark            -> control.setWatermark (only after a successful write)
    Log Row Counts           -> control.logRowCount
    Log Package Success      -> control.logPackageEnd
    OnError handler          -> control.logError + logPackageEnd(status="Failed")
"""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wwi_sqlserver_extract import specs, transforms
from wwi_sqlserver_extract.jdbc import SqlServerConnection, renderSql

STEP_NAME = "Extract SQL Server"
JOB_PARAMETER_DEFAULTS = {
    "BatchId": "0",
    "BusinessDate": "",
    "ReloadFullHistory": "False",
    "EnvironmentCode": "DEV",
    "RestartFromStep": "",
    "catalog": "wwi_dev",
}
BRONZE_SCHEMA = "bronze"
AUDIT_COLUMNS: Tuple[str, ...] = (
    "BatchId",
    "PackageExecutionId",
    "ExtractedAtUtc",
    "SourceSystemCode",
    "WatermarkFrom",
    "WatermarkTo",
)
REJECT_REASON_CODE = "CONSTRAINT_VIOLATION"
REJECT_STAGE = "Extract"
DEFAULT_EXPIRY_LOOKBACK_DAYS = 7
TEMPORAL_LITERAL = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2}(\.\d{1,7})?)?)?Z?$")

SourceReader = Callable[[str], DataFrame]


@dataclass
class RunContext:
    spark: SparkSession
    catalog: str
    batchId: int
    packageExecutionId: int
    reloadFullHistory: bool
    nowUtc: datetime
    environmentCode: str = "DEV"


@dataclass
class ExtractResult:
    packageName: str
    targetTable: str
    rowsRead: int = 0
    rowsInserted: int = 0
    rowsUpdated: int = 0
    rowsDeleted: int = 0
    rowsRejected: int = 0
    watermarkFrom: Optional[str] = None
    watermarkTo: Optional[str] = None
    renderedSql: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)

    def toJson(self) -> str:
        return json.dumps(asdict(self), default=str)


def parseNumericWatermark(value: Any) -> int:
    """Lower bound for NumericKey watermarks.

    etl.usp_GetWatermark returns the stored value as text; objects registered on
    first run default to the Timestamp type and hand back the epoch timestamp, which
    the legacy `OrderID > ?` predicate cannot use.  Anything non-numeric is treated
    as "never extracted" (0), exactly what a full reload would use.
    """
    if value is None:
        return 0
    text = str(value).strip()
    if text == "":
        return 0
    try:
        return int(text)
    except ValueError:
        return 0


def validateTemporalLiteral(value: Any, name: str) -> str:
    text = str(value).strip()
    if not TEMPORAL_LITERAL.match(text):
        raise ValueError("%s is not an ISO-8601 date/time literal: %r" % (name, value))
    return text


def getExtraVariable(spec: specs.PackageSpec, name: str, default: Any) -> Any:
    for variableName, value in spec.extraVariables:
        if variableName == name:
            return value
    return default


def readScalar(reader: SourceReader, sql: str) -> int:
    row = reader(sql).first()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def resolveWatermarks(
    spec: specs.PackageSpec, ctx: RunContext, reader: SourceReader, control: Any
) -> Tuple[Dict[str, Any], str, Optional[str]]:
    """Returns (sql bindings, watermarkFrom, watermarkTo); watermarkTo is None when it
    has to be derived from the extracted rows (open-ended numeric-key packages)."""
    watermarkFrom, watermarkTo = control.getWatermark(
        ctx.spark,
        ctx.catalog,
        spec.sourceSystemCode,
        spec.watermarkObject,
        reloadFullHistory=ctx.reloadFullHistory,
    )
    if spec.loadPattern in (specs.NUMERIC_KEY_BOUNDED, specs.NUMERIC_KEY_OPEN):
        lower = 0 if ctx.reloadFullHistory else parseNumericWatermark(watermarkFrom)
        if spec.loadPattern == specs.NUMERIC_KEY_BOUNDED:
            upper = readScalar(reader, spec.maxKeySql)
            return {"watermarkFrom": lower, "watermarkTo": upper}, str(lower), str(upper)
        bindings: Dict[str, Any] = {"watermarkFrom": lower}
        if "expiryLookbackDays" in spec.sqlParams:
            bindings["expiryLookbackDays"] = int(
                getExtraVariable(spec, "ExpiryLookbackDays", DEFAULT_EXPIRY_LOOKBACK_DAYS)
            )
        return bindings, str(lower), None
    lowerText = validateTemporalLiteral(watermarkFrom, "WatermarkFrom")
    upperText = validateTemporalLiteral(watermarkTo, "WatermarkTo")
    return {"watermarkFrom": lowerText, "watermarkTo": upperText}, lowerText, upperText


def conformColumns(df: DataFrame, columns: Sequence[Tuple[str, str]]) -> DataFrame:
    """Select the package's column contract in order, casting to the SSIS buffer types."""
    available = {c.lower(): c for c in df.columns}
    missing = [name for name, _ in columns if name.lower() not in available]
    if missing:
        raise ValueError("Source result set is missing columns: %s" % ", ".join(missing))
    return df.select([F.col("`%s`" % available[name.lower()]).cast(dataType).alias(name) for name, dataType in columns])


def castDerivedColumns(df: DataFrame, derivations: Sequence[Tuple[str, str, str]]) -> DataFrame:
    for name, _, dataType in derivations:
        if name in AUDIT_COLUMNS or name not in df.columns:
            continue
        df = df.withColumn(name, F.col(name).cast(dataType))
    return df


def addAuditColumns(
    df: DataFrame, ctx: RunContext, spec: specs.PackageSpec, watermarkFrom: Optional[str], watermarkTo: Optional[str]
) -> DataFrame:
    return (
        df.withColumn("BatchId", F.lit(ctx.batchId).cast("bigint"))
        .withColumn("PackageExecutionId", F.lit(ctx.packageExecutionId).cast("bigint"))
        .withColumn("ExtractedAtUtc", F.lit(ctx.nowUtc).cast("timestamp"))
        .withColumn("SourceSystemCode", F.lit(spec.sourceSystemCode))
        .withColumn("WatermarkFrom", F.lit(watermarkFrom).cast("string"))
        .withColumn("WatermarkTo", F.lit(watermarkTo).cast("string"))
    )


def rejectPredicate(spec: specs.PackageSpec, df: DataFrame):
    keys = [k for k in spec.keyColumns if k in df.columns and k != "DeleteFlag"]
    predicate = F.lit(False)
    for key in keys:
        predicate = predicate | F.col(key).isNull()
    return predicate


def splitRejects(df: DataFrame, spec: specs.PackageSpec) -> Tuple[DataFrame, DataFrame]:
    """OLE DB destination error output: rows that would violate the raw table's NOT NULL
    key constraints.  RedirectRow -> rejected set, FailComponent -> raise, IgnoreFailure -> keep."""
    predicate = rejectPredicate(spec, df)
    return df.filter(~predicate), df.filter(predicate)


def tableName(naming: Any, catalog: str, table: str) -> str:
    return naming.table(catalog, BRONZE_SCHEMA, table)


def ensureSchema(spark: SparkSession, fullTableName: str) -> None:
    schemaName = fullTableName.rsplit(".", 1)[0]
    spark.sql("CREATE SCHEMA IF NOT EXISTS %s" % schemaName)


def mergeMetrics(spark: SparkSession, fullTableName: str, fallbackCount: int) -> Tuple[int, int]:
    try:
        history = DeltaTable.forName(spark, fullTableName).history(1).select("operationMetrics").first()
        metrics = history[0] if history is not None else {}
        return int(metrics.get("numTargetRowsInserted", fallbackCount)), int(metrics.get("numTargetRowsUpdated", 0))
    except Exception:
        return fallbackCount, 0


def writeTarget(
    ctx: RunContext, spec: specs.PackageSpec, df: DataFrame, fullTableName: str, bindings: Dict[str, Any]
) -> Tuple[int, int]:
    """Idempotent landing that matches the legacy package's clear-then-load semantics."""
    spark = ctx.spark
    ensureSchema(spark, fullTableName)
    if spec.loadPattern == specs.FULL_RELOAD:
        count = df.count()
        writer = df.write.format("delta").mode("overwrite")
        if spec.recordKind and spark.catalog.tableExists(fullTableName):
            # Shared raw table: the legacy clear step only deleted this package's RecordKind.
            writer = writer.option("replaceWhere", "`RecordKind` = '%s'" % spec.recordKind).option("mergeSchema", "true")
        else:
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(fullTableName)
        return count, 0
    if spec.loadPattern == specs.DATE_WINDOW:
        count = df.count()
        predicate = "`%s` >= '%s' AND `%s` < '%s'" % (
            spec.windowColumn,
            bindings["watermarkFrom"],
            spec.windowColumn,
            bindings["watermarkTo"],
        )
        writer = df.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
        if spark.catalog.tableExists(fullTableName):
            writer = writer.option("mergeSchema", "true")
        writer.saveAsTable(fullTableName)
        return count, 0
    deduped = df.dropDuplicates(list(spec.keyColumns))
    if not spark.catalog.tableExists(fullTableName):
        count = deduped.count()
        deduped.write.format("delta").saveAsTable(fullTableName)
        return count, 0
    target = DeltaTable.forName(spark, fullTableName)
    conditions = ["t.`%s` = s.`%s`" % (k, k) for k in spec.keyColumns]
    if spec.recordKind:
        conditions.append("t.`RecordKind` = '%s'" % spec.recordKind)
    condition = " AND ".join(conditions)
    builder = target.alias("t").merge(deduped.alias("s"), condition)
    if hasattr(builder, "withSchemaEvolution"):
        builder = builder.withSchemaEvolution()
    else:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    return mergeMetrics(spark, fullTableName, deduped.count())


def extractDeleteMarkers(
    spec: specs.PackageSpec, ctx: RunContext, reader: SourceReader, control: Any
) -> Tuple[DataFrame, str]:
    detection = spec.deleteDetection
    version = 0
    if detection.versionSql:
        try:
            version = readScalar(reader, detection.versionSql)
        except Exception as exc:
            control.logError(
                ctx.spark,
                ctx.catalog,
                packageExecutionId=ctx.packageExecutionId,
                batchId=ctx.batchId,
                errorSeverity="Warning",
                sourceName=spec.name,
                sourceComponent="Read Change Tracking Version",
                errorDescription="Falling back to CHANGE_TRACKING_MIN_VALID_VERSION: %s" % exc,
            )
            version = readScalar(
                reader,
                "SELECT ISNULL(CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(N'%s')), 0) AS MinValidVersion"
                % detection.changeTrackingObject,
            )
    sql = renderSql(detection.sql, ("changeTrackingVersion",), {"changeTrackingVersion": version})
    df = conformColumns(reader(sql), detection.columns)
    df = transforms.deriveDeleteMarkers(df, ctx.nowUtc)
    return castDerivedColumns(df, detection.derivations), sql


def extractPackage(
    ctx: RunContext, spec: specs.PackageSpec, reader: SourceReader, control: Any, naming: Any
) -> ExtractResult:
    result = ExtractResult(packageName=spec.name, targetTable=spec.targetTable)
    bindings: Dict[str, Any] = {}
    watermarkFrom: Optional[str] = None
    watermarkTo: Optional[str] = None
    if spec.isIncremental:
        bindings, watermarkFrom, watermarkTo = resolveWatermarks(spec, ctx, reader, control)
        if spec.loadPattern == specs.DATE_WINDOW and watermarkFrom >= watermarkTo:
            # The legacy window [LastValue, today) is empty until the next business day.
            result.renderedSql = renderSql(spec.sourceSql, spec.sqlParams, bindings)
            result.watermarkFrom, result.watermarkTo = watermarkFrom, watermarkTo
            result.metrics["skippedEmptyWindow"] = True
            return result

    sql = renderSql(spec.sourceSql, spec.sqlParams, bindings)
    result.renderedSql = sql
    source = conformColumns(reader(sql), spec.columns)
    derived = transforms.applyTransform(spec.transform, source, ctx.nowUtc)
    derived = castDerivedColumns(derived, spec.derivations)

    landed, rejected = splitRejects(derived, spec)
    if spec.errorDisposition == "IgnoreFailure":
        landed, rejected = derived, derived.limit(0)
    landed = landed.cache()
    result.rowsRead = derived.count()
    rejectedCount = rejected.count()
    if rejectedCount:
        if spec.errorDisposition == "FailComponent":
            raise RuntimeError(
                "%d row(s) violate the %s key constraint (%s)" % (rejectedCount, spec.legacyTarget, ", ".join(spec.keyColumns))
            )
        result.rowsRejected = int(
            control.logRejectedRecordSet(
                ctx.spark,
                ctx.catalog,
                spec.legacyTarget,
                addAuditColumns(rejected, ctx, spec, watermarkFrom, watermarkTo),
                batchId=ctx.batchId,
                packageExecutionId=ctx.packageExecutionId,
                sourceSystemCode=spec.sourceSystemCode,
                rejectStage=REJECT_STAGE,
                rejectReasonCode=REJECT_REASON_CODE,
                businessKeyColumn=spec.keyColumns[0],
            )
            or rejectedCount
        )

    splitCondition = transforms.SPLIT_CONDITIONS.get(spec.name)
    if splitCondition is not None and spec.splits:
        result.metrics[spec.splits[0][2].replace(" ", "") + "Count"] = landed.filter(splitCondition()).count()

    if spec.loadPattern == specs.NUMERIC_KEY_OPEN and spec.keyColumns:
        maxKey = landed.agg(F.max(F.col(spec.keyColumns[0]).cast("bigint"))).first()[0]
        watermarkTo = str(maxKey) if maxKey is not None else None

    output = addAuditColumns(landed, ctx, spec, watermarkFrom, watermarkTo)

    if spec.deleteDetection is not None:
        deletes, deleteSql = extractDeleteMarkers(spec, ctx, reader, control)
        deletes = addAuditColumns(deletes, ctx, spec, watermarkFrom, watermarkTo)
        result.rowsDeleted = deletes.count()
        result.metrics["deleteSql"] = deleteSql
        output = output.unionByName(deletes, allowMissingColumns=True)

    fullTableName = tableName(naming, ctx.catalog, spec.targetTable)
    result.rowsInserted, result.rowsUpdated = writeTarget(ctx, spec, output, fullTableName, bindings)
    result.watermarkFrom, result.watermarkTo = watermarkFrom, watermarkTo

    if spec.isIncremental and watermarkTo is not None:
        control.setWatermark(
            ctx.spark,
            ctx.catalog,
            spec.sourceSystemCode,
            spec.watermarkObject,
            watermarkTo,
            packageExecutionId=ctx.packageExecutionId,
        )
    landed.unpersist()
    return result


def rowCountObjectName(spec: specs.PackageSpec) -> str:
    return "%s.%s" % (BRONZE_SCHEMA, spec.targetTable)


def runPackage(
    spark: SparkSession,
    dbutils: Any,
    packageName: str,
    control: Any,
    params: Any,
    naming: Any,
    sourceReader: Optional[SourceReader] = None,
    nowUtc: Optional[datetime] = None,
) -> ExtractResult:
    spec = specs.getPackage(packageName)
    jobParams = params.getJobParams(dbutils)
    catalog = jobParams["catalog"]
    batchId = int(jobParams["batchId"])
    reloadFullHistory = bool(jobParams["reloadFullHistory"])
    runNowUtc = nowUtc or datetime.now(timezone.utc).replace(tzinfo=None)
    if sourceReader is None:
        connection = SqlServerConnection.fromWidgets(dbutils)
        timeout = spec.sourceTimeoutSeconds or None
        sourceReader = lambda sql: connection.readQuery(spark, sql, timeout)  # noqa: E731

    packageExecutionId = control.logPackageStart(
        spark, catalog, batchId, spec.name, projectName=specs.PROJECT_NAME, stepName=STEP_NAME
    )
    ctx = RunContext(
        spark=spark,
        catalog=catalog,
        batchId=batchId,
        packageExecutionId=int(packageExecutionId),
        reloadFullHistory=reloadFullHistory,
        nowUtc=runNowUtc,
        environmentCode=str(jobParams.get("environmentCode", "DEV")),
    )
    try:
        result = extractPackage(ctx, spec, sourceReader, control, naming)
    except Exception as exc:
        control.logError(
            spark,
            catalog,
            packageExecutionId=ctx.packageExecutionId,
            batchId=batchId,
            errorSeverity="Error",
            sourceName=spec.name,
            sourceComponent="Extract %s" % spec.watermarkObject if spec.watermarkObject else spec.name,
            errorDescription=("%s: %s" % (type(exc).__name__, exc))[:4000],
        )
        control.logPackageEnd(spark, catalog, ctx.packageExecutionId, status="Failed")
        raise

    control.logRowCount(
        spark,
        catalog,
        ctx.packageExecutionId,
        rowCountObjectName(spec),
        sourceRowCount=result.rowsRead + result.rowsDeleted,
        targetRowCount=result.rowsInserted + result.rowsUpdated,
        insertRowCount=result.rowsInserted,
        updateRowCount=result.rowsUpdated,
        deleteRowCount=result.rowsDeleted,
        rejectRowCount=result.rowsRejected,
    )
    for objectName in spec.extraRowCountObjects:
        control.logRowCount(
            spark,
            catalog,
            ctx.packageExecutionId,
            objectName,
            sourceRowCount=result.rowsRead,
            targetRowCount=result.rowsInserted,
            rejectRowCount=0,
        )
    control.logPackageEnd(
        spark,
        catalog,
        ctx.packageExecutionId,
        status="Succeeded",
        rowsRead=result.rowsRead,
        rowsInserted=result.rowsInserted,
        rowsUpdated=result.rowsUpdated,
        rowsDeleted=result.rowsDeleted,
        rowsRejected=result.rowsRejected,
        watermarkFrom=result.watermarkFrom,
        watermarkTo=result.watermarkTo,
    )
    return result
