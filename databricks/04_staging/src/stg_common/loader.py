"""Per-package run wrapper: binds a WWI_Staging notebook to dbx_etl_common and
to the Delta bronze/silver tables, and implements the three legacy load
shapes (truncate-and-reload, watermarked incremental, work rebuild)."""

import contextlib
import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params
from stg_common.sources import conformSource

PROJECT_NAME = "WWI_Staging"
BATCH_NAME = "WWI_Staging"
PHASE_STAGE_LOAD = "Stage Load"
PHASE_STAGE_WORK = "Stage Work Tables"
PHASE_ORDER = (PHASE_STAGE_LOAD, PHASE_STAGE_WORK)

REJECT_STANDARD_COLUMNS = (
    "BatchId",
    "PackageExecutionId",
    "SourceSystemCode",
    "RejectReasonCode",
    "RejectReason",
    "RejectStage",
    "RecordPayload",
    "ReprocessStatusCode",
    "ReprocessAttemptCount",
    "RejectedAtUtc",
)


class RunCounters:
    def __init__(self):
        self.rowsRead = 0
        self.rowsInserted = 0
        self.rowsUpdated = 0
        self.rowsDeleted = 0
        self.rowsRejected = 0
        self.watermarkFrom = None
        self.watermarkTo = None


class StagingRun:
    """One legacy package execution.

    Usage inside a notebook::

        run = StagingRun(spark, dbutils, "STG_Load_Customer", sourceSystemCode="ORA_ERP",
                         objectName="stg.Customer")
        with run.execute() as ctx:
            ...
    """

    def __init__(
        self,
        spark,
        dbutils,
        packageName,
        sourceSystemCode,
        objectName,
        phase=PHASE_STAGE_LOAD,
        watermark=False,
        jobParams=None,
    ):
        self.spark = spark
        self.packageName = packageName
        self.sourceSystemCode = sourceSystemCode
        self.objectName = objectName
        self.phase = phase
        self.usesWatermark = watermark
        self.params = jobParams if jobParams is not None else params.getJobParams(dbutils)
        self.catalog = self.params["catalog"]
        self.businessDate = self.params["businessDate"]
        self.reloadFullHistory = bool(self.params["reloadFullHistory"])
        self.environmentCode = self.params["environmentCode"]
        self.restartFromStep = (self.params["restartFromStep"] or "").strip()
        self.batchId = self._resolveBatchId(int(self.params["batchId"]))
        self.packageExecutionId = None
        self.counters = RunCounters()
        self.watermarkFrom = None
        self.watermarkTo = None
        self.skipped = False

    # ------------------------------------------------------------------ lifecycle
    def _resolveBatchId(self, batchId):
        """The legacy master package owns the batch and hands BatchId down. When the
        job is started stand-alone (BatchId = 0) every task adopts the running
        WWI_Staging batch for this BusinessDate, creating it on first use."""
        if batchId > 0:
            return batchId
        return control.startBatch(
            self.spark,
            self.catalog,
            BATCH_NAME,
            batchType="Daily",
            businessDate=self.businessDate,
            environmentCode=self.environmentCode,
            allowAdoptRunning=True,
            notes="Adopted by %s" % self.packageName,
        )

    def shouldSkipForRestart(self):
        """RestartFromStep semantics from Invoke-EstateOrchestration: phases before
        the named step are skipped; a package name restarts at that package."""
        if not self.restartFromStep:
            return False
        if self.restartFromStep in PHASE_ORDER:
            return PHASE_ORDER.index(self.phase) < PHASE_ORDER.index(self.restartFromStep)
        if self.restartFromStep.startswith("STG_") and self.restartFromStep != self.packageName:
            return self._packageOrder(self.packageName) < self._packageOrder(self.restartFromStep)
        return False

    @staticmethod
    def _packageOrder(name):
        return (1 if name.startswith("STG_Work_") else 0, name)

    @contextlib.contextmanager
    def execute(self):
        if self.shouldSkipForRestart():
            self.skipped = True
            print("%s skipped: RestartFromStep=%s" % (self.packageName, self.restartFromStep))
            yield self
            return
        with control.packageRun(
            self.spark,
            self.catalog,
            self.batchId,
            self.packageName,
            projectName=PROJECT_NAME,
            stepName=self.phase,
        ) as pkg:
            self.packageExecutionId = pkg.packageExecutionId
            if self.usesWatermark:
                self.watermarkFrom, self.watermarkTo = control.getWatermark(
                    self.spark,
                    self.catalog,
                    self.sourceSystemCode,
                    self.objectName,
                    reloadFullHistory=self.reloadFullHistory,
                )
            yield self
            if self.usesWatermark and self.counters.watermarkTo is not None:
                control.setWatermark(
                    self.spark,
                    self.catalog,
                    self.sourceSystemCode,
                    self.objectName,
                    self.counters.watermarkTo,
                    packageExecutionId=self.packageExecutionId,
                    allowRewind=self.reloadFullHistory,
                )
            pkg.rowsRead = self.counters.rowsRead
            pkg.rowsInserted = self.counters.rowsInserted
            pkg.rowsUpdated = self.counters.rowsUpdated
            pkg.rowsDeleted = self.counters.rowsDeleted
            pkg.rowsRejected = self.counters.rowsRejected
            pkg.watermarkFrom = self.watermarkFrom
            pkg.watermarkTo = self.counters.watermarkTo

    # ------------------------------------------------------------------ naming
    def bronzeTable(self, name):
        return naming.table(self.catalog, "bronze", name)

    def silverTable(self, name):
        return naming.table(self.catalog, "silver", name)

    # ------------------------------------------------------------------ reads
    def bronze(self, name, currentBatchOnly=True):
        """raw.X for this batch: the `WHERE BatchId = ?` every package source carries."""
        df = conformSource(self.spark.table(self.bronzeTable(name)), name)
        if currentBatchOnly:
            df = df.where(F.col("BatchId") == F.lit(self.batchId))
        return df

    def silver(self, name, currentBatchOnly=False):
        df = self.spark.table(self.silverTable(name))
        if currentBatchOnly:
            df = df.where(F.col("BatchId") == F.lit(self.batchId))
        return df

    def silverIfExists(self, name, schemaLike=None):
        fqn = self.silverTable(name)
        if self.spark.catalog.tableExists(fqn):
            return self.spark.table(fqn)
        if schemaLike is not None:
            return self.spark.createDataFrame([], schemaLike.schema)
        return None

    def watermarkFromTimestamp(self, lookbackDays=0):
        """WatermarkFrom as a timestamp column, epoch when unset or on full reload."""
        if self.reloadFullHistory or not self.watermarkFrom:
            value = datetime.datetime(1900, 1, 1)
        else:
            value = _parseTimestamp(self.watermarkFrom)
        if lookbackDays:
            value = value - datetime.timedelta(days=lookbackDays)
        return F.lit(value.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")

    def watermarkToTimestamp(self):
        """WatermarkTo (the extraction ceiling handed back by usp_GetWatermark)."""
        value = _parseTimestamp(self.watermarkTo) if self.watermarkTo else datetime.datetime(9999, 12, 31)
        return F.lit(value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]).cast("timestamp")

    def applyWatermark(self, df, changeColumn, lookbackDays=0):
        """The package source predicate `ChangeColumn > WatermarkFrom AND
        ChangeColumn <= WatermarkTo` (WatermarkFrom is the epoch on full reload)."""
        stamp = F.col(changeColumn).cast("timestamp")
        return df.where((stamp > self.watermarkFromTimestamp(lookbackDays)) & (stamp <= self.watermarkToTimestamp()))

    def bronzeWatermarked(self, name, changeColumn):
        """raw.X between the watermarks; records MAX(change stamp) as the new WatermarkTo
        and counts the rows read."""
        df = self.applyWatermark(self.bronze(name, currentBatchOnly=False), changeColumn)
        self.recordWatermarkTo(df, changeColumn)
        return df

    def recordWatermarkTo(self, df, changeColumn):
        """Set Watermark = MAX(change stamp) of the rows read this run."""
        row = df.agg(F.max(F.col(changeColumn).cast("timestamp")).alias("m")).collect()[0]
        if row["m"] is not None:
            self.counters.watermarkTo = row["m"].strftime("%Y-%m-%dT%H:%M:%S.") + "%03d" % (row["m"].microsecond // 1000)
        return self.counters.watermarkTo

    # ------------------------------------------------------------------ writes
    def stampControlColumns(self, df):
        cols = {}
        if "SourceSystemCode" not in df.columns:
            cols["SourceSystemCode"] = F.lit(self.sourceSystemCode)
        cols["BatchId"] = F.lit(self.batchId).cast("long")
        cols["PackageExecutionId"] = F.lit(self.packageExecutionId).cast("long")
        cols["LoadedAtUtc"] = F.current_timestamp()
        return df.withColumns(cols)

    def truncateReload(self, df, tableName):
        """TRUNCATE TABLE + fast load: the table only ever holds the current batch."""
        out = self.stampControlColumns(df)
        fqn = self.silverTable(tableName)
        (
            out.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(fqn)
        )
        count = self.spark.table(fqn).count()
        self.counters.rowsInserted += count
        return count

    def rebuildForBatch(self, df, tableName):
        """DELETE ... WHERE BatchId = @BatchId then INSERT: the work.* and per-batch
        append procedures. Re-running a batch replaces its own rows only."""
        out = self.stampControlColumns(df)
        fqn = self.silverTable(tableName)
        if self.spark.catalog.tableExists(fqn):
            deleted = self.spark.table(fqn).where(F.col("BatchId") == F.lit(self.batchId)).count()
            self.spark.sql("DELETE FROM %s WHERE BatchId = %d" % (fqn, self.batchId))
            self.counters.rowsDeleted += deleted
            out.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fqn)
        else:
            out.write.format("delta").mode("overwrite").saveAsTable(fqn)
        count = out.count()
        self.counters.rowsInserted += count
        return count

    def mergeByKey(self, df, tableName, keyColumns):
        """Watermarked incremental append as a Delta MERGE on the business key so a
        re-run of the same BatchId/BusinessDate upserts instead of duplicating."""
        out = self.stampControlColumns(df)
        fqn = self.silverTable(tableName)
        if not self.spark.catalog.tableExists(fqn):
            out.write.format("delta").mode("overwrite").saveAsTable(fqn)
            count = out.count()
            self.counters.rowsInserted += count
            return count
        existingCols = set(self.spark.table(fqn).columns)
        missing = [c for c in out.columns if c not in existingCols]
        if missing:
            self.spark.createDataFrame([], out.select(*missing).schema).write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(fqn)
        cond = " AND ".join("t.`%s` <=> s.`%s`" % (k, k) for k in keyColumns)
        deduped = out.dropDuplicates(list(keyColumns))
        before = self.spark.table(fqn).count()
        view = "stg_merge_src_%d" % abs(hash(fqn))
        deduped.createOrReplaceTempView(view)
        cols = deduped.columns
        updates = ", ".join("t.`%s` = s.`%s`" % (c, c) for c in cols if c not in keyColumns)
        inserts = ", ".join("`%s`" % c for c in cols)
        values = ", ".join("s.`%s`" % c for c in cols)
        self.spark.sql(
            "MERGE INTO %s AS t USING %s AS s ON %s "
            "WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)" % (fqn, view, cond, updates, inserts, values)
        )
        self.spark.catalog.dropTempView(view)
        after = self.spark.table(fqn).count()
        incoming = deduped.count()
        inserted = after - before
        self.counters.rowsInserted += inserted
        self.counters.rowsUpdated += incoming - inserted
        return incoming

    # ------------------------------------------------------------------ rejects
    def reject(
        self,
        df,
        errTable,
        reasonCode,
        reason,
        businessKeyColumn,
        keyColumns=None,
        rejectStage="Stage",
        objectName=None,
        payloadColumns=None,
    ):
        """Route rows to silver.err_* and register them through the control
        framework (one etl.rejected_record row per business key, like the
        legacy reject sweep). `reasonCode`/`reason` may be literals or Columns."""
        if df is None:
            return 0
        payloadCols = payloadColumns or [c for c in df.columns if c not in REJECT_STANDARD_COLUMNS]
        reasonCodeCol = reasonCode if not isinstance(reasonCode, str) else F.lit(reasonCode)
        reasonCol = reason if not isinstance(reason, str) else F.lit(reason)
        out = df
        mapped = dict(keyColumns or {})
        for name, expr in mapped.items():
            out = out.withColumn(name, expr if not isinstance(expr, str) else F.col(expr))
        keep = list(mapped.keys())
        out = out.select(
            *[F.col(c) for c in keep],
            F.lit(self.batchId).cast("long").alias("BatchId"),
            F.lit(self.packageExecutionId).cast("long").alias("PackageExecutionId"),
            (F.col("SourceSystemCode") if "SourceSystemCode" in df.columns else F.lit(self.sourceSystemCode)).alias("SourceSystemCode"),
            reasonCodeCol.cast("string").alias("RejectReasonCode"),
            reasonCol.cast("string").alias("RejectReason"),
            F.lit(rejectStage).alias("RejectStage"),
            F.to_json(F.struct(*[F.col(c) for c in payloadCols])).alias("RecordPayload"),
            F.lit("NEW").alias("ReprocessStatusCode"),
            F.lit(0).cast("smallint").alias("ReprocessAttemptCount"),
            F.current_timestamp().alias("RejectedAtUtc"),
        )
        out = out.cache()
        count = out.count()
        if count == 0:
            out.unpersist()
            return 0
        fqn = self.silverTable(errTable)
        out.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fqn)
        for row in out.select("RejectReasonCode").distinct().collect():
            code = row["RejectReasonCode"]
            subset = out.where(F.col("RejectReasonCode").eqNullSafe(F.lit(code)))
            control.logRejectedRecordSet(
                self.spark,
                self.catalog,
                objectName or self.objectName,
                subset,
                batchId=self.batchId,
                packageExecutionId=self.packageExecutionId,
                sourceSystemCode=self.sourceSystemCode,
                rejectStage=rejectStage,
                rejectReasonCode=code,
                businessKeyColumn=businessKeyColumn,
            )
        out.unpersist()
        self.counters.rowsRejected += count
        return count

    def rejectLookupFailures(self, df, lookupName, lookupColumn, sourceBusinessKey, reason, objectName=None, rejectStage="Transform"):
        """err.RejectedLookupFailure: one row per distinct lookup value with an
        OccurrenceCount, exactly like the procedures' GROUP BY."""
        if df is None:
            return 0
        grouped = (
            df.groupBy(F.col(lookupColumn).cast("string").alias("LookupValue"))
            .agg(
                F.count(F.lit(1)).alias("OccurrenceCount"),
                F.min(F.col(sourceBusinessKey).cast("string")).alias("SourceBusinessKey"),
            )
            .withColumns(
                {
                    "SourceObjectName": F.lit(objectName or self.objectName),
                    "LookupName": F.lit(lookupName),
                    "LookupColumnName": F.lit(lookupColumn),
                    "RoutedToUnknownMember": F.lit(True),
                    "QueuedForLateArrival": F.lit(False),
                }
            )
        )
        return self.reject(
            grouped,
            "err_rejected_lookup_failure",
            "LOOKUP_MISS",
            reason,
            businessKeyColumn="SourceBusinessKey",
            keyColumns={
                c: F.col(c)
                for c in (
                    "SourceObjectName",
                    "SourceBusinessKey",
                    "LookupName",
                    "LookupColumnName",
                    "LookupValue",
                    "RoutedToUnknownMember",
                    "QueuedForLateArrival",
                    "OccurrenceCount",
                )
            },
            rejectStage=rejectStage,
            objectName=objectName,
            payloadColumns=["LookupValue", "OccurrenceCount"],
        )

    def rejectConstraint(self, df, targetObject, constraintName, keyColumn, violatingColumn, reasonCode, reason, constraintType="CHECK", rejectStage="Transform"):
        """err.RejectedConstraintViolation: the Conditional Split outputs the packages
        route there (implausible amounts, zero quantities, both-sides journals...)."""
        return self.reject(
            df,
            "err_rejected_constraint_violation",
            reasonCode,
            reason,
            businessKeyColumn="ViolatingBusinessKey",
            keyColumns={
                "TargetObjectName": F.lit(targetObject),
                "ConstraintName": F.lit(constraintName),
                "ConstraintTypeCode": F.lit(constraintType),
                "ViolatingBusinessKey": F.col(keyColumn).cast("string"),
                "ViolatingColumnName": F.lit(violatingColumn),
                "ViolatingValue": F.col(violatingColumn).cast("string"),
            },
            rejectStage=rejectStage,
            objectName=targetObject,
        )

    # ------------------------------------------------------------------ counts
    def logRowCount(self, objectName=None, sourceRowCount=None, targetRowCount=None, insertRowCount=None, updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
        control.logRowCount(
            self.spark,
            self.catalog,
            self.packageExecutionId,
            objectName or self.objectName,
            sourceRowCount=sourceRowCount if sourceRowCount is not None else self.counters.rowsRead,
            targetRowCount=targetRowCount,
            insertRowCount=insertRowCount if insertRowCount is not None else self.counters.rowsInserted,
            updateRowCount=updateRowCount if updateRowCount is not None else self.counters.rowsUpdated,
            deleteRowCount=deleteRowCount if deleteRowCount is not None else self.counters.rowsDeleted,
            rejectRowCount=rejectRowCount if rejectRowCount is not None else self.counters.rowsRejected,
        )

    def countRead(self, df):
        """The `Count Rows Read` Row Count transform."""
        n = df.count()
        self.counters.rowsRead += n
        return n


def _parseTimestamp(value):
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day)
    text = str(value).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            continue
    return datetime.datetime(1900, 1, 1)


def splitByCondition(df, condition):
    """SSIS Conditional Split with one output and a default: (matching, rest)."""
    return df.where(condition), df.where(~F.coalesce(condition, F.lit(False)))


def lookupLeft(df, lookupDf, joinColumns, outputColumns, lookupAlias="lk"):
    """SSIS Lookup (full/no cache): LEFT JOIN, returning (matched, noMatch)."""
    right = lookupDf.select(*[F.col(c) for c in list(joinColumns) + list(outputColumns)]).dropDuplicates(list(joinColumns))
    joined = df.drop(*outputColumns).join(right, on=list(joinColumns), how="left")
    matchCol = F.col(outputColumns[0])
    return joined.where(matchCol.isNotNull()), joined.where(matchCol.isNull()).drop(*outputColumns)


def lookupIgnore(df, lookupDf, joinColumns, outputColumns, lookupAlias="lk"):
    """SSIS Lookup with 'Ignore failure': LEFT JOIN, unmatched rows keep NULL outputs."""
    right = lookupDf.select(*[F.col(c) for c in list(joinColumns) + list(outputColumns)]).dropDuplicates(list(joinColumns))
    return df.drop(*outputColumns).join(right, on=list(joinColumns), how="left")


def isDataFrame(obj):
    return isinstance(obj, DataFrame)
