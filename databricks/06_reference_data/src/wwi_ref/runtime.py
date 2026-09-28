"""Package lifecycle for the REF_Load_* notebooks on top of dbx_etl_common.

Mirrors the control-flow skeleton emitted by build_reference_packages.py for every package:

    CTL Log Package Start  -> control.logPackageStart
    Truncate <dimension>   -> idempotent MERGE (delta_io.publishScd1 / publishScd2), no truncate
    <pre_tasks>            -> ref_loads.*
    <Data Flow>            -> transforms.* + delta_io.*
    <post_tasks>           -> ref_loads.reportUnmappedCodes
    CTL Log Row Count      -> control.logRowCount
    CTL Log Package Success-> control.logPackageEnd(status="Succeeded")
    OnError handler        -> control.logError + control.logPackageEnd(status="Failed")
"""
import datetime
import json
from contextlib import contextmanager

from pyspark.sql import Window
from pyspark.sql import functions as F

from dbx_etl_common import control, naming

from wwi_ref import PHASES, PROJECT_NAME, delta_io, schemas

BATCH_NAME = "wwi_06_reference_data"
REJECT_STAGE = "Reference"


def widgetOrDefault(dbutils, name, default):
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return default
    return value if value not in (None, "") else default


def parseDate(value, default):
    if value is None or value == "":
        return default
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(str(value)[:10])


def isSkippedByRestart(p, packageName):
    """RestartFromStep names the legacy package (job task) to resume from: everything in an earlier
    phase is skipped, everything in the same or a later phase runs."""
    restartFrom = (p.get("restartFromStep") or "").strip()
    if restartFrom == "" or restartFrom not in PHASES:
        return False
    return PHASES[packageName] < PHASES[restartFrom]


class PackageContext:
    def __init__(self, spark, catalog, batchId, businessDate, environmentCode, packageName, packageExecutionId):
        self.spark = spark
        self.catalog = catalog
        self.batchId = batchId
        self.businessDate = businessDate
        self.environmentCode = environmentCode
        self.packageName = packageName
        self.packageExecutionId = packageExecutionId
        self.currentStep = "CTL Log Package Start"
        self.rowsRead = 0
        self.rowsInserted = 0
        self.rowsUpdated = 0
        self.rowsDeleted = 0
        self.rowsRejected = 0
        self.warningCount = 0
        self.objectCounts = {}

    # ---- naming
    def table(self, legacyName):
        return schemas.fqn(self.catalog, legacyName)

    def etlTable(self, name):
        return naming.table(self.catalog, "etl", name)

    def step(self, name):
        self.currentStep = name
        print("[%s] %s" % (self.packageName, name))

    # ---- counters
    def addCounts(self, objectName, sourceRowCount=None, targetRowCount=None, insertRowCount=None,
                  updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
        self.rowsRead += sourceRowCount or 0
        self.rowsInserted += insertRowCount or 0
        self.rowsUpdated += updateRowCount or 0
        self.rowsDeleted += deleteRowCount or 0
        self.rowsRejected += rejectRowCount or 0
        self.objectCounts[objectName] = {
            "source": sourceRowCount, "target": targetRowCount, "inserted": insertRowCount,
            "updated": updateRowCount, "deleted": deleteRowCount, "rejected": rejectRowCount,
        }
        control.logRowCount(
            self.spark, self.catalog, self.packageExecutionId, objectName,
            sourceRowCount=sourceRowCount, targetRowCount=targetRowCount, insertRowCount=insertRowCount,
            updateRowCount=updateRowCount, deleteRowCount=deleteRowCount, rejectRowCount=rejectRowCount,
        )

    def logWarning(self, description, sourceComponent=None, errorCode=None):
        self.warningCount += 1
        control.logError(
            self.spark, self.catalog, packageExecutionId=self.packageExecutionId, batchId=self.batchId,
            errorSeverity="Warning", errorCode=errorCode, sourceName=self.packageName,
            sourceComponent=sourceComponent or self.currentStep, procedureName=None,
            errorDescription=str(description)[:4000],
        )

    # ---- rejects (SSIS error outputs -> silver.err_* + etl.rejected_record)
    def _nextRejectId(self, tableFqn):
        maxId = self.spark.table(tableFqn).agg(F.max("RejectId")).first()[0]
        return (maxId or 0) + 1

    def rejectLookupFailures(self, df, sourceObjectName, lookupName, lookupColumnName, lookupValueColumn,
                             businessKeyColumn, rejectReasonCode, rejectReason, sourceSystemCodeColumn=None,
                             routedToUnknownMember=False, occurrenceColumn=None, payloadColumns=None):
        """SSIS 'ERR ... ' branch destination into err.RejectedLookupFailure (+ etl.rejected_record)."""
        target = self.table("err.RejectedLookupFailure")
        rejected = df.select(
            F.lit(self.batchId).cast("bigint").alias("BatchId"),
            F.lit(self.packageExecutionId).cast("bigint").alias("PackageExecutionId"),
            F.lit(sourceObjectName).alias("SourceObjectName"),
            F.col(businessKeyColumn).cast("string").alias("SourceBusinessKey"),
            F.lit(lookupName).alias("LookupName"),
            F.lit(lookupColumnName).alias("LookupColumnName"),
            F.col(lookupValueColumn).cast("string").alias("LookupValue"),
            (F.col(sourceSystemCodeColumn).cast("string") if sourceSystemCodeColumn else F.lit(None).cast("string")).alias("SourceSystemCode"),
            F.lit(rejectReasonCode).alias("RejectReasonCode"),
            F.lit(rejectReason).alias("RejectReason"),
            F.lit(REJECT_STAGE).alias("RejectStage"),
            F.lit(routedToUnknownMember).alias("RoutedToUnknownMember"),
            F.lit(False).alias("QueuedForLateArrival"),
            (F.col(occurrenceColumn).cast("int") if occurrenceColumn else F.lit(1)).alias("OccurrenceCount"),
            F.to_json(F.struct(*[F.col(c) for c in (payloadColumns or df.columns)])).alias("RecordPayload"),
            F.lit("Pending").alias("ReprocessStatusCode"),
            F.current_timestamp().alias("RejectedAtUtc"),
        )
        return self._writeRejects(target, rejected, "SourceObjectName", sourceObjectName, rejectReasonCode, "SourceBusinessKey")

    def rejectConstraintViolations(self, df, targetObjectName, businessKeyColumn, violatingColumnName,
                                   violatingValueColumn, rejectReasonCode, rejectReason, payloadColumns=None,
                                   constraintName=None, constraintTypeCode="CHECK"):
        """SSIS 'ERR ... ' branch destination into err.RejectedConstraintViolation (+ etl.rejected_record)."""
        target = self.table("err.RejectedConstraintViolation")
        rejected = df.select(
            F.lit(self.batchId).cast("bigint").alias("BatchId"),
            F.lit(self.packageExecutionId).cast("bigint").alias("PackageExecutionId"),
            F.lit(targetObjectName).alias("TargetObjectName"),
            F.lit(constraintName).cast("string").alias("ConstraintName"),
            F.lit(constraintTypeCode).cast("string").alias("ConstraintTypeCode"),
            F.col(businessKeyColumn).cast("string").alias("ViolatingBusinessKey"),
            F.lit(violatingColumnName).alias("ViolatingColumnName"),
            F.col(violatingValueColumn).cast("string").alias("ViolatingValue"),
            F.lit(None).cast("int").alias("SqlErrorNumber"),
            F.lit(None).cast("string").alias("SqlErrorMessage"),
            F.lit(rejectReasonCode).alias("RejectReasonCode"),
            F.lit(rejectReason).alias("RejectReason"),
            F.lit(REJECT_STAGE).alias("RejectStage"),
            F.to_json(F.struct(*[F.col(c) for c in (payloadColumns or df.columns)])).alias("RecordPayload"),
            F.lit("Pending").alias("ReprocessStatusCode"),
            F.current_timestamp().alias("RejectedAtUtc"),
        )
        return self._writeRejects(target, rejected, "TargetObjectName", targetObjectName, rejectReasonCode, "ViolatingBusinessKey")

    def _writeRejects(self, target, rejected, objectColumn, objectName, rejectReasonCode, businessKeyColumn):
        rejected = rejected.cache()
        rejectedCount = rejected.count()
        # re-runnable for the same BatchId: replace this package's rejects for the object/reason
        self.spark.sql(
            "DELETE FROM %s WHERE BatchId = %d AND %s = '%s' AND RejectReasonCode = '%s'"
            % (target, self.batchId, objectColumn, objectName.replace("'", "''"), rejectReasonCode)
        )
        if rejectedCount > 0:
            startId = self._nextRejectId(target)
            withId = rejected.withColumn("RejectId", F.lit(startId - 1) + F.row_number().over(Window.orderBy(F.lit(1))))
            delta_io.conformToTable(self.spark, target, withId).write.format("delta").mode("append").saveAsTable(target)
            control.logRejectedRecordSet(
                self.spark, self.catalog, objectName, rejected, batchId=self.batchId,
                packageExecutionId=self.packageExecutionId,
                sourceSystemCode=None, rejectStage=REJECT_STAGE, rejectReasonCode=rejectReasonCode,
                businessKeyColumn=businessKeyColumn,
            )
        rejected.unpersist()
        return rejectedCount

    def summary(self):
        return json.dumps({
            "package": self.packageName, "batchId": self.batchId, "packageExecutionId": self.packageExecutionId,
            "rowsRead": self.rowsRead, "rowsInserted": self.rowsInserted, "rowsUpdated": self.rowsUpdated,
            "rowsDeleted": self.rowsDeleted, "rowsRejected": self.rowsRejected, "warnings": self.warningCount,
            "objects": self.objectCounts,
        }, default=str)


def resolveBatchId(spark, p):
    """The weekly master normally hands the BatchId down; a stand-alone run (BatchId 0) opens or adopts the
    project batch so package_execution / row_count_log rows always hang off a real etl.batch row."""
    batchId = int(p.get("batchId") or 0)
    if batchId > 0:
        return batchId
    return control.startBatch(
        spark, p["catalog"], BATCH_NAME, batchType="Weekly", businessDate=p.get("businessDate"),
        environmentCode=p.get("environmentCode"), allowAdoptRunning=True,
        notes="opened by %s (BatchId parameter was 0)" % PROJECT_NAME,
    )


@contextmanager
def packageRun(spark, p, packageName):
    catalog = p["catalog"]
    batchId = resolveBatchId(spark, p)
    schemas.ensureAll(spark, catalog)
    packageExecutionId = control.logPackageStart(
        spark, catalog, batchId, packageName, projectName=PROJECT_NAME,
        stepName="Phase %d" % PHASES[packageName],
    )
    ctx = PackageContext(spark, catalog, batchId, p.get("businessDate"), p.get("environmentCode"),
                         packageName, packageExecutionId)
    try:
        yield ctx
    except Exception as exc:
        control.logError(
            spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, errorSeverity="Error",
            errorCode=type(exc).__name__, sourceName=packageName, sourceComponent=ctx.currentStep,
            procedureName=None, errorDescription=str(exc)[:4000],
        )
        control.logPackageEnd(
            spark, catalog, packageExecutionId, status="Failed", rowsRead=ctx.rowsRead,
            rowsInserted=ctx.rowsInserted, rowsUpdated=ctx.rowsUpdated, rowsDeleted=ctx.rowsDeleted,
            rowsRejected=ctx.rowsRejected,
        )
        raise
    ctx.step("CTL Log Package Success")
    control.logPackageEnd(
        spark, catalog, packageExecutionId, status="Succeeded", rowsRead=ctx.rowsRead,
        rowsInserted=ctx.rowsInserted, rowsUpdated=ctx.rowsUpdated, rowsDeleted=ctx.rowsDeleted,
        rowsRejected=ctx.rowsRejected,
    )


# ---- dimension publishing (legacy "Truncate <dimension>" + OLE DB Destination -> idempotent MERGE)
def publishScd1(ctx, legacyName, df, sourceRowCount, rejectRowCount=0):
    keyCol, businessKeyCols, _ = schemas.DIMENSIONS[legacyName]
    target = ctx.table(legacyName)
    ctx.step("Publish %s" % legacyName)
    counts = delta_io.publishScd1(ctx.spark, target, df, keyCol, businessKeyCols, ctx.packageName, ctx.batchId)
    targetRows = ctx.spark.table(target).where(F.col(keyCol) > 0).count()
    ctx.addCounts(legacyName, sourceRowCount=sourceRowCount, targetRowCount=targetRows, insertRowCount=counts.inserted,
                  updateRowCount=counts.updated, deleteRowCount=counts.deleted, rejectRowCount=rejectRowCount)
    return counts


def publishScd2(ctx, legacyName, df, sourceRowCount, rejectRowCount=0, effectiveFrom=None):
    keyCol, businessKeyCols, _ = schemas.SCD2_DIMENSIONS[legacyName]
    target = ctx.table(legacyName)
    ctx.step("Publish %s" % legacyName)
    effective = effectiveFrom or ctx.businessDate or datetime.date.today()
    counts = delta_io.publishScd2(ctx.spark, target, df, keyCol, businessKeyCols, ctx.packageName, ctx.batchId,
                                  str(effective))
    targetRows = ctx.spark.table(target).where((F.col(keyCol) > 0) & (F.col("IsCurrentRow") == True)).count()  # noqa: E712
    ctx.addCounts(legacyName, sourceRowCount=sourceRowCount, targetRowCount=targetRows, insertRowCount=counts.inserted,
                  updateRowCount=counts.updated, deleteRowCount=counts.deleted, rejectRowCount=rejectRowCount)
    return counts
