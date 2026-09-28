"""Per-package orchestration: the Foreach File loop of the ING_FILE_* packages.

Flow for one package run (one notebook / job task):

1. ``discoverFiles``  - Auto Loader (``cloudFiles`` / ``binaryFile``) streams every
   new file of the feed from its inbound UC Volume into the manifest table
   ``bronze.raw_file_inbound_manifest`` (``trigger(availableNow=True)``), so file
   discovery is exactly-once and restartable.
2. ``ingestPending``   - every manifest row not yet finalised in
   ``etl.file_ingestion_log`` is processed like one Foreach iteration:
   register -> duplicate check -> decode / split / parse -> load bronze ->
   record rejects in ``silver.err_rejected_file_row`` -> control totals ->
   decide Processed / Quarantined.
3. ``finalizeFiles``   - writes the ``.rej`` companion, moves the file to the
   archive or poison volume, stamps ``CompletedAtUtc`` and hands the rejects to
   ``control.logRejectedRecordSet``.

Only step 3 needs ``dbutils`` (file moves) and the ``dbx_etl_common`` control
module; both are injected so the logic runs under local PySpark in the tests.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import control_totals as ct
from . import feeds, lines, schemas, transforms

MANIFEST_TABLE = "raw_file_inbound_manifest"
FX_PUBLISHED_RATE_TABLE = "raw_oracle_fx_rate"

MANIFEST_SCHEMA = T.StructType(
    [
        T.StructField("FeedCode", T.StringType()),
        T.StructField("PackageName", T.StringType()),
        T.StructField("FilePath", T.StringType()),
        T.StructField("FileName", T.StringType()),
        T.StructField("FileModifiedAtUtc", T.TimestampType()),
        T.StructField("FileSizeBytes", T.LongType()),
        T.StructField("Content", T.BinaryType()),
        T.StructField("DiscoveredAtUtc", T.TimestampType()),
    ]
)

FINAL_STATUSES = (ct.STATUS_PROCESSED, ct.STATUS_QUARANTINED, ct.STATUS_DUPLICATE, ct.STATUS_SWEPT, ct.STATUS_UNREADABLE)


def tableName(catalog: str, schema: str, table: str) -> str:
    return "%s.%s.%s" % (catalog, schema, table)


@dataclass
class RunSummary:
    packageName: str
    rowsRead: int = 0
    rowsInserted: int = 0
    rowsRejected: int = 0
    filesDiscovered: int = 0
    filesProcessed: int = 0
    filesQuarantined: int = 0
    filesDuplicate: int = 0
    fileTotals: List[ct.FileTotals] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def statusCount(self, status: str) -> int:
        return sum(1 for t in self.fileTotals if getattr(t, "status", None) == status)


# ---------------------------------------------------------------------------
# table setup
# ---------------------------------------------------------------------------


def ensureTables(spark: SparkSession, catalog: str) -> None:
    """CREATE TABLE IF NOT EXISTS for everything this bundle writes (no-op when session 00 / 04 created them)."""
    for table, schema in schemas.BRONZE_SCHEMAS.items():
        schemas.createTable(spark, tableName(catalog, "bronze", table), schema,
                            comment="raw.%s landed by WWI_Ingest_Files" % table)
    schemas.createTable(spark, tableName(catalog, "bronze", MANIFEST_TABLE), MANIFEST_SCHEMA,
                        comment="Auto Loader manifest of feed files discovered on the inbound volumes")
    schemas.createTable(spark, tableName(catalog, "silver", feeds.ERR_REJECTED_FILE_ROW),
                        schemas.ERR_REJECTED_FILE_ROW, identityColumn="RejectId", comment="err.RejectedFileRow")
    schemas.createTable(spark, tableName(catalog, "etl", feeds.FILE_INGESTION_LOG),
                        schemas.FILE_INGESTION_LOG, identityColumn="FileIngestionLogId", comment="etl.FileIngestionLog")
    schemas.createTable(spark, tableName(catalog, "etl", feeds.FILE_CONTROL_TOTAL),
                        schemas.FILE_CONTROL_TOTAL, identityColumn="FileControlTotalId", comment="etl.FileControlTotal")


# ---------------------------------------------------------------------------
# step 1 - file discovery
# ---------------------------------------------------------------------------


def inboundPath(catalog: str, spec: feeds.FeedSpec) -> str:
    return feeds.volumePath(catalog, spec.sourceVolume, spec.volumeFolder)


def checkpointPath(catalog: str, spec: feeds.FeedSpec) -> str:
    return feeds.volumePath(catalog, feeds.VOLUME_CHECKPOINTS, "03_file_ingestion", spec.packageName)


def _manifestRows(filesDf: DataFrame, spec: feeds.FeedSpec) -> DataFrame:
    return filesDf.select(
        F.lit(spec.feedCode).alias("FeedCode"),
        F.lit(spec.packageName).alias("PackageName"),
        F.col("path").alias("FilePath"),
        F.element_at(F.split(F.col("path"), "/"), -1).alias("FileName"),
        F.col("modificationTime").alias("FileModifiedAtUtc"),
        F.col("length").cast("long").alias("FileSizeBytes"),
        F.col("content").alias("Content"),
        F.current_timestamp().alias("DiscoveredAtUtc"),
    )


def _topLevelOnly(filesDf: DataFrame, source: str) -> DataFrame:
    """Foreach File enumerators ran with Recurse=0: only files directly under the feed folder are input.

    Keeps the sub-folders this bundle writes under the quarantine volume (``<feed>/<yyyyMMdd>``,
    ``duplicate``, ``poison``) out of the quarantine sweep. ``path`` may carry a scheme prefix.
    """
    return filesDf.where(F.col("path").rlike("^([A-Za-z][A-Za-z0-9+.-]*:)?\\Q%s\\E/[^/]+$" % source.rstrip("/")))


def discoverFiles(spark: SparkSession, catalog: str, spec: feeds.FeedSpec, useAutoLoader: bool = True) -> None:
    """Auto Loader the feed's inbound volume into bronze.raw_file_inbound_manifest.

    ``useAutoLoader=False`` is the batch fallback (plain ``binaryFile`` read with
    an anti-join on the manifest) used by the local tests and available when a
    workspace cannot run ``cloudFiles``.
    """
    source = inboundPath(catalog, spec)
    manifest = tableName(catalog, "bronze", MANIFEST_TABLE)
    if useAutoLoader:
        stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.includeExistingFiles", "true")
            .option("cloudFiles.allowOverwrites", "true")
            .option("pathGlobFilter", spec.fileGlob)
            .load(source)
        )
        query = (
            _manifestRows(_topLevelOnly(stream, source), spec)
            .writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpointPath(catalog, spec))
            .trigger(availableNow=True)
            .toTable(manifest)
        )
        query.awaitTermination()
        return
    filesDf = spark.read.format("binaryFile").option("pathGlobFilter", spec.fileGlob).load(source)
    rows = _manifestRows(_topLevelOnly(filesDf, source), spec)
    known = spark.table(manifest).where(F.col("PackageName") == spec.packageName).select(
        "FilePath", "FileModifiedAtUtc", "FileSizeBytes"
    )
    rows.join(known, ["FilePath", "FileModifiedAtUtc", "FileSizeBytes"], "left_anti").write.format("delta").mode(
        "append"
    ).saveAsTable(manifest)


# ---------------------------------------------------------------------------
# step 2 - per-file processing
# ---------------------------------------------------------------------------


def pendingFiles(spark: SparkSession, catalog: str, spec: feeds.FeedSpec) -> DataFrame:
    """Manifest rows of this package without a finalised etl.file_ingestion_log entry."""
    manifest = spark.table(tableName(catalog, "bronze", MANIFEST_TABLE)).where(F.col("PackageName") == spec.packageName)
    log = spark.table(tableName(catalog, "etl", feeds.FILE_INGESTION_LOG)).where(
        (F.col("ObjectName") == spec.bronzeObjectName()) & F.col("CompletedAtUtc").isNotNull()
    )
    done = log.select("FilePath", "FileModifiedAtUtc", "FileSizeBytes")
    return manifest.join(done, ["FilePath", "FileModifiedAtUtc", "FileSizeBytes"], "left_anti").orderBy(
        "FileModifiedAtUtc", "FileName"
    )


def _registerFile(spark, catalog, batchId, packageExecutionId, spec, fileRow) -> None:
    """Register Inbound File: INSERT INTO etl.FileIngestionLog (... Status = 'Received')."""
    log = tableName(catalog, "etl", feeds.FILE_INGESTION_LOG)
    spark.sql(
        "DELETE FROM %s WHERE PackageExecutionId = %d AND FilePath = '%s' AND FileModifiedAtUtc = TIMESTAMP '%s'"
        % (log, packageExecutionId, _q(fileRow["FilePath"]), _ts(fileRow["FileModifiedAtUtc"]))
    )
    spark.sql(
        "INSERT INTO %s (PackageExecutionId, ObjectName, FileName, FilePath, ReceivedAtUtc, CompletedAtUtc, "
        "DetailRowCount, RejectRowCount, Status, FileSizeBytes, FileModifiedAtUtc, BatchId) "
        "VALUES (%d, '%s', '%s', '%s', current_timestamp(), NULL, NULL, NULL, 'Received', %d, TIMESTAMP '%s', %d)"
        % (log, packageExecutionId, _q(spec.bronzeObjectName()), _q(fileRow["FileName"]), _q(fileRow["FilePath"]),
           int(fileRow["FileSizeBytes"]), _ts(fileRow["FileModifiedAtUtc"]), batchId)
    )


def _updateFileLog(spark, catalog, packageExecutionId, fileRow, status, detailRows, rejectRows, completed) -> None:
    log = tableName(catalog, "etl", feeds.FILE_INGESTION_LOG)
    spark.sql(
        "UPDATE %s SET Status = '%s', DetailRowCount = %d, RejectRowCount = %d%s "
        "WHERE PackageExecutionId = %d AND FilePath = '%s' AND FileModifiedAtUtc = TIMESTAMP '%s'"
        % (log, status, detailRows, rejectRows, ", CompletedAtUtc = current_timestamp()" if completed else "",
           packageExecutionId, _q(fileRow["FilePath"]), _ts(fileRow["FileModifiedAtUtc"]))
    )


def priorFiles(spark, catalog, spec, fileName) -> List[Dict]:
    log = spark.table(tableName(catalog, "etl", feeds.FILE_INGESTION_LOG))
    rows = log.where((F.col("ObjectName") == spec.bronzeObjectName()) & (F.col("FileName") == fileName)).select(
        "FileName", "Status", "FileSizeBytes", "FilePath", "FileModifiedAtUtc"
    ).collect()
    return [r.asDict() for r in rows]


def publishedRates(spark: SparkSession, catalog: str) -> Optional[DataFrame]:
    """SPOT rates of raw.OracleFxRate -> RatePairCode / RateDate / PublishedRate.

    The legacy lookup selected RatePairCode, RateDate, Rate from raw.OracleFxRate;
    those columns do not exist in the raw DDL, so the pair code is composed from
    FROM_CURRENCY_CD / TO_CURRENCY_CD and the rate from CONVERSION_RATE.
    """
    name = tableName(catalog, "bronze", FX_PUBLISHED_RATE_TABLE)
    if not spark.catalog.tableExists(name):
        return None
    return spark.table(name).where(F.col("RATE_TYPE_CD") == "SPOT").select(
        F.concat(F.trim(F.col("FROM_CURRENCY_CD")), F.lit("/"), F.trim(F.col("TO_CURRENCY_CD"))).alias("RatePairCode"),
        F.expr("try_cast(RATE_DT AS date)").alias("RateDate"),
        F.expr("try_cast(CONVERSION_RATE AS %s)" % transforms.RATE).alias("PublishedRate"),
    ).where(F.col("RateDate").isNotNull() & F.col("PublishedRate").isNotNull())


def emptyPublishedRates(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [], T.StructType([
            T.StructField("RatePairCode", T.StringType()),
            T.StructField("RateDate", T.DateType()),
            T.StructField("PublishedRate", schemas.RATE),
        ])
    )


def sidecarRowCount(spark, catalog, fileName) -> Optional[int]:
    """Read Sidecar Control Total: ISNULL(ExpectedRowCount, 0) FROM etl.FileControlTotal WHERE FileName = ?"""
    rows = spark.table(tableName(catalog, "etl", feeds.FILE_CONTROL_TOTAL)).where(F.col("FileName") == fileName).select(
        F.max("ExpectedRowCount").alias("n")
    ).collect()
    value = rows[0]["n"] if rows else None
    return int(value) if value is not None else None


def parseFile(spark, spec: feeds.FeedSpec, fileDf: DataFrame, publishedRatesDf: Optional[DataFrame] = None) -> DataFrame:
    """One file's binaryFile row -> classified, typed rows (all lines, incl. header / footer / rejects)."""
    linesDf = lines.dataLines(lines.explodeLines(fileDf, spec), spec)
    if spec.packageName == feeds.QUARANTINE_MALFORMED.packageName:
        parsed = transforms.classifyQuarantinedRows(
            linesDf.withColumn("ActualColumnCount", F.lit(1)).withColumn("ExpectedColumnCount", F.lit(1))
        )
        return parsed
    split = lines.splitColumns(linesDf, spec)
    parsed = transforms.PARSERS[spec.packageName](split)
    if spec.packageName == feeds.FX_OVERRIDE.packageName:
        rates = publishedRatesDf if publishedRatesDf is not None else emptyPublishedRates(spark)
        parsed = transforms.applyPublishedRate(parsed, rates)
    return parsed


def landedRows(parsed: DataFrame) -> DataFrame:
    return parsed.where((F.col("RecordClass") == transforms.CLASS_DETAIL) & F.col("RejectReasonCode").isNull())


def rejectedRows(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("RejectReasonCode").isNotNull())


def conformToSchema(df: DataFrame, schema: T.StructType, batchId: int, packageExecutionId: int) -> DataFrame:
    df = df.withColumn("BatchId", F.lit(batchId).cast("long")).withColumn(
        "PackageExecutionId", F.lit(packageExecutionId).cast("long")
    ).withColumn("LoadedAtUtc", F.current_timestamp())
    present = set(df.columns)
    return df.select(
        *[
            (F.col(f.name).cast(f.dataType) if f.name in present else F.lit(None).cast(f.dataType)).alias(f.name)
            for f in schema.fields
        ]
    )


def rejectRowsForErrTable(df: DataFrame, spec: feeds.FeedSpec, batchId: int, packageExecutionId: int) -> DataFrame:
    present = set(df.columns)
    df = (
        df.withColumn("RawRowText", F.col("RawLine"))
        .withColumn("DelimiterUsed", F.lit(spec.delimiter))
        .withColumn("DecimalSeparatorUsed", F.lit(spec.decimalSeparator))
        .withColumn("DateFormatAssumed", F.lit(spec.dateFormatAssumed).cast("string"))
        .withColumn("RejectStage", F.lit("Ingest"))
        .withColumn("ReprocessStatusCode", F.lit("NEW"))
        .withColumn("ReprocessAttemptCount", F.lit(0))
        .withColumn("RejectedAtUtc", F.current_timestamp())
        .withColumn(
            "_rescued_data",
            F.to_json(F.struct(
                F.col("RawLine").alias("RawLine"),
                F.col("SourceRowNumber").alias("SourceRowNumber"),
                F.col("ActualColumnCount").alias("ActualColumnCount"),
                F.col("ExpectedColumnCount").alias("ExpectedColumnCount"),
            )),
        )
    )
    if "OriginFeedCode" not in present:
        df = df.withColumn("OriginFeedCode", transforms.originFeedCode(F.col("FileName")))
    if "ReplayEligibleFlag" not in present:
        df = df.withColumn("ReplayEligibleFlag", F.lit(None).cast("string"))
    return conformToSchema(df, schemas.ERR_REJECTED_FILE_ROW, batchId, packageExecutionId).drop("LoadedAtUtc")


def _deleteFileRows(spark, fullName, fileRow) -> None:
    spark.sql(
        "DELETE FROM %s WHERE FilePath = '%s' AND FileModifiedAtUtc = TIMESTAMP '%s' AND FileSizeBytes = %d"
        % (fullName, _q(fileRow["FilePath"]), _ts(fileRow["FileModifiedAtUtc"]), int(fileRow["FileSizeBytes"]))
    )


def _footerValues(parsed: DataFrame, spec: feeds.FeedSpec, totals: ct.FileTotals) -> None:
    footer = parsed.where(F.col("RecordClass") == transforms.CLASS_FOOTER)
    name = spec.packageName
    if name == feeds.PARTNER_SALES_NA.packageName:
        row = footer.select(F.max(F.expr("try_cast(FooterRecordCountText AS int)")).alias("v")).collect()[0]
        totals.footerRowCount = int(row["v"]) if row["v"] is not None else None
    elif name == feeds.PARTNER_SALES_EU.packageName:
        row = footer.select(
            F.max(F.expr("try_cast(regexp_replace(FooterAmountText, ',', '.') AS decimal(19,4))")).alias("v")
        ).collect()[0]
        totals.footerAmountTotal = Decimal(row["v"]) if row["v"] is not None else None
    elif name == feeds.PARTNER_SALES_APAC.packageName:
        row = footer.select(F.max(F.expr("try_cast(FooterTotalText AS decimal(19,4))")).alias("v")).collect()[0]
        totals.footerAmountTotal = Decimal(row["v"]) if row["v"] is not None else None
    elif name == feeds.SUPPLIER_CATALOG.packageName:
        row = footer.select(
            F.max(F.expr("try_cast(FooterRowCountText AS int)")).alias("rows"),
            F.max(F.expr("try_cast(FooterChecksumText AS bigint)")).alias("checksum"),
        ).collect()[0]
        totals.footerRowCount = int(row["rows"]) if row["rows"] is not None else None
        totals.footerChecksum = int(row["checksum"]) if row["checksum"] is not None else None


def _detailTotals(landed: DataFrame, spec: feeds.FeedSpec, totals: ct.FileTotals) -> None:
    name = spec.packageName
    if name in (feeds.PARTNER_SALES_EU.packageName, feeds.PARTNER_SALES_APAC.packageName):
        row = landed.select(F.coalesce(F.sum("GrossAmount"), F.lit(0)).alias("v")).collect()[0]
        totals.detailAmountTotal = Decimal(row["v"])
    elif name == feeds.SUPPLIER_CATALOG.packageName:
        totals.priceChecksum = transforms.priceChecksum(landed)
    elif name == feeds.CARRIER_SCAN.packageName:
        totals.duplicateScanCount = transforms.countDuplicateScans(landed)
    elif name == feeds.FX_OVERRIDE.packageName:
        totals.outOfToleranceCount = 0  # filled from the rejects by the caller


def processFile(spark, catalog, batchId, packageExecutionId, spec, fileRow, contentDf: DataFrame,
                controlTotalMode: str = ct.MODE_LEGACY, publishedRatesDf: Optional[DataFrame] = None) -> ct.FileTotals:
    """One Foreach iteration for one manifest row. ``contentDf`` is the binaryFile-shaped single row."""
    totals = ct.FileTotals(fileName=fileRow["FileName"])
    _registerFile(spark, catalog, batchId, packageExecutionId, spec, fileRow)

    if ct.isDuplicateFile(fileRow["FileName"], fileRow["FileSizeBytes"], priorFiles(spark, catalog, spec, fileRow["FileName"])):
        totals.status = ct.STATUS_DUPLICATE
        totals.warnings.append("%s: duplicate of an already processed file; moved to the duplicate folder." % fileRow["FileName"])
        _updateFileLog(spark, catalog, packageExecutionId, fileRow, ct.STATUS_DUPLICATE, 0, 0, completed=False)
        return totals

    parsed = parseFile(spark, spec, contentDf, publishedRatesDf).cache()
    landed = landedRows(parsed)
    rejected = rejectedRows(parsed)

    targetTable = tableName(catalog, spec.bronzeSchema, spec.bronzeTable)
    errTable = tableName(catalog, "silver", feeds.ERR_REJECTED_FILE_ROW)

    # idempotent re-run of the same file version
    _deleteFileRows(spark, errTable, fileRow)
    if targetTable != errTable:
        _deleteFileRows(spark, targetTable, fileRow)
        conformToSchema(landed, schemas.BRONZE_SCHEMAS[spec.bronzeTable], batchId, packageExecutionId).write.format(
            "delta"
        ).mode("append").saveAsTable(targetTable)
        errRows = rejectRowsForErrTable(rejected, spec, batchId, packageExecutionId)
    else:
        # ING_FILE_QuarantineMalformed lands every line straight into err.RejectedFileRow
        errRows = rejectRowsForErrTable(parsed, spec, batchId, packageExecutionId)
    errRows.write.format("delta").mode("append").saveAsTable(errTable)

    counts = {r["RejectReasonCode"]: r["n"] for r in rejected.groupBy("RejectReasonCode").agg(F.count("*").alias("n")).collect()}
    totals.linesRead = parsed.count()
    totals.detailRowCount = landed.count() if targetTable != errTable else totals.linesRead
    totals.conversionRowCount = sum(
        counts.get(c, 0) for c in (transforms.REASON_COLUMN_COUNT, transforms.REASON_CONVERSION)
    )
    totals.unknownRecordCount = counts.get(transforms.REASON_UNKNOWN_RECORD, 0)
    totals.malformedRowCount = sum(
        n for code, n in counts.items()
        if code not in (transforms.REASON_COLUMN_COUNT, transforms.REASON_CONVERSION,
                        transforms.REASON_UNKNOWN_RECORD, transforms.REASON_FOOTER,
                        transforms.REASON_QUARANTINED, transforms.REASON_EMPTY_LINE)
    )
    if spec.packageName == feeds.FX_OVERRIDE.packageName:
        totals.outOfToleranceCount = counts.get(spec.rejectReasonCode, 0)
    if spec.packageName == feeds.QUARANTINE_MALFORMED.packageName:
        totals.replayEligibleCount = parsed.where(F.col("ReplayEligibleFlag") == "Y").count()
        totals.malformedRowCount = 0
    if spec.packageName == feeds.CARRIER_SCAN.packageName:
        totals.sidecarRowCount = sidecarRowCount(spark, catalog, fileRow["FileName"])
    _footerValues(parsed, spec, totals)
    _detailTotals(landed, spec, totals)
    parsed.unpersist()

    totals.status = ct.fileStatus(spec, totals, controlTotalMode)
    _updateFileLog(spark, catalog, packageExecutionId, fileRow, totals.status, totals.detailRowCount,
                   totals.rejectedRowCount, completed=False)
    return totals


def ingestPending(spark, catalog, batchId, packageExecutionId, spec, controlTotalMode=ct.MODE_LEGACY,
                  summary: Optional[RunSummary] = None) -> RunSummary:
    summary = summary or RunSummary(packageName=spec.packageName)
    rates = publishedRates(spark, catalog) if spec.packageName == feeds.FX_OVERRIDE.packageName else None
    if spec.packageName == feeds.FX_OVERRIDE.packageName and rates is None:
        summary.warnings.append(
            "%s not found: every override is refused as %s until the Oracle FX extract has landed."
            % (tableName(catalog, "bronze", FX_PUBLISHED_RATE_TABLE), transforms.REASON_FX_UNKNOWN_PAIR)
        )
    pending = pendingFiles(spark, catalog, spec)
    fileRows = [r.asDict() for r in pending.drop("Content").collect()]
    summary.filesDiscovered += len(fileRows)
    for fileRow in fileRows:
        contentDf = pending.where(
            (F.col("FilePath") == fileRow["FilePath"]) & (F.col("FileModifiedAtUtc") == F.lit(fileRow["FileModifiedAtUtc"]))
        ).select(
            F.col("FilePath").alias("path"), F.col("FileModifiedAtUtc").alias("modificationTime"),
            F.col("FileSizeBytes").alias("length"), F.col("Content").alias("content"),
        )
        totals = processFile(spark, catalog, batchId, packageExecutionId, spec, fileRow, contentDf,
                             controlTotalMode, rates)
        totals.filePath = fileRow["FilePath"]
        totals.fileModifiedAtUtc = fileRow["FileModifiedAtUtc"]
        summary.fileTotals.append(totals)
        summary.rowsRead += totals.linesRead
        summary.rowsInserted += totals.detailRowCount if totals.status != ct.STATUS_DUPLICATE else 0
        summary.rowsRejected += totals.rejectedRowCount
        summary.warnings.extend(totals.warnings)
    summary.filesProcessed = summary.statusCount(ct.STATUS_PROCESSED) + summary.statusCount(ct.STATUS_SWEPT)
    summary.filesQuarantined = summary.statusCount(ct.STATUS_QUARANTINED) + summary.statusCount(ct.STATUS_UNREADABLE)
    summary.filesDuplicate = summary.statusCount(ct.STATUS_DUPLICATE)
    return summary


# ---------------------------------------------------------------------------
# step 3 - file system tasks + control framework
# ---------------------------------------------------------------------------


def _fileTarget(catalog, spec, totals) -> str:
    if totals.status in (ct.STATUS_PROCESSED, ct.STATUS_SWEPT):
        return ct.archivePath(catalog, spec, totals.fileName, totals.fileModifiedAtUtc)
    if totals.status == ct.STATUS_DUPLICATE:
        return ct.duplicatePath(catalog, spec, totals.fileName)
    return ct.poisonPath(catalog, totals.fileName)


def writeRejectFile(spark, dbutils, catalog, packageExecutionId, spec, totals) -> Optional[str]:
    """Write the .rej companion (raw rejected lines) next to the feed's quarantine folder."""
    if spec.packageName == feeds.QUARANTINE_MALFORMED.packageName or totals.status == ct.STATUS_DUPLICATE:
        return None
    err = spark.table(tableName(catalog, "silver", feeds.ERR_REJECTED_FILE_ROW))
    rows = err.where(
        (F.col("PackageExecutionId") == packageExecutionId) & (F.col("FilePath") == totals.filePath)
        & (F.col("RejectReasonCode") != transforms.REASON_FOOTER)
    ).orderBy("SourceRowNumber").select("SourceRowNumber", "RejectReasonCode", "RawRowText").collect()
    if not rows:
        return None
    path = ct.rejectFilePath(catalog, spec, totals.fileName)
    body = "\n".join("%s|%s|%s" % (r["SourceRowNumber"], r["RejectReasonCode"], r["RawRowText"] or "") for r in rows)
    dbutils.fs.put(path, body + "\n", True)
    return path


def finalizeFiles(spark, dbutils, control, catalog, batchId, packageExecutionId, spec, summary: RunSummary) -> None:
    """Archive / poison moves, .rej files, CompletedAtUtc and the dbx_etl_common reject + row-count logging."""
    for totals in summary.fileTotals:
        target = _fileTarget(catalog, spec, totals)
        writeRejectFile(spark, dbutils, catalog, packageExecutionId, spec, totals)
        _ensureParent(dbutils, target)
        dbutils.fs.mv(totals.filePath, target)
        fileRow = {"FilePath": totals.filePath, "FileModifiedAtUtc": totals.fileModifiedAtUtc}
        _updateFileLog(spark, catalog, packageExecutionId, fileRow, totals.status, totals.detailRowCount,
                       totals.rejectedRowCount, completed=True)

    err = spark.table(tableName(catalog, "silver", feeds.ERR_REJECTED_FILE_ROW)).where(
        (F.col("PackageExecutionId") == packageExecutionId) & (F.col("RejectReasonCode") != transforms.REASON_FOOTER)
    )
    codes = [r["RejectReasonCode"] for r in err.select("RejectReasonCode").distinct().collect()]
    for code in sorted(codes):
        subset = err.where(F.col("RejectReasonCode") == code).select(
            F.col("SourceFileName").alias("BusinessKey"), F.col("RawRowText").alias("RecordPayload"),
            "RejectReason", "SourceRowNumber",
        )
        control.logRejectedRecordSet(
            spark, catalog, spec.bronzeObjectName(), subset, batchId=batchId,
            packageExecutionId=packageExecutionId, sourceSystemCode=spec.sourceSystemCode,
            rejectStage="Ingest", rejectReasonCode=code, businessKeyColumn="BusinessKey",
        )
    for message in summary.warnings:
        control.logError(
            spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, errorSeverity="Warning",
            errorCode="FILE_CONTROL", sourceName=spec.packageName, sourceComponent="Reconcile Control Totals",
            errorDescription=message,
        )
    control.logRowCount(
        spark, catalog, packageExecutionId, spec.bronzeObjectName(),
        sourceRowCount=summary.rowsRead, targetRowCount=summary.rowsInserted,
        insertRowCount=summary.rowsInserted, rejectRowCount=summary.rowsRejected,
    )


def _ensureParent(dbutils, path: str) -> None:
    parent = path.rsplit("/", 1)[0]
    dbutils.fs.mkdirs(parent)


def runPackage(spark, dbutils, control, catalog, batchId, packageExecutionId, spec,
               controlTotalMode=ct.MODE_LEGACY, useAutoLoader=True) -> RunSummary:
    """discoverFiles -> ingestPending -> finalizeFiles for one ING_FILE_* package."""
    ensureTables(spark, catalog)
    discoverFiles(spark, catalog, spec, useAutoLoader=useAutoLoader)
    summary = ingestPending(spark, catalog, batchId, packageExecutionId, spec, controlTotalMode)
    finalizeFiles(spark, dbutils, control, catalog, batchId, packageExecutionId, spec, summary)
    return summary


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------


def _q(value: str) -> str:
    return str(value).replace("'", "''")


def _ts(value) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    return str(value)
