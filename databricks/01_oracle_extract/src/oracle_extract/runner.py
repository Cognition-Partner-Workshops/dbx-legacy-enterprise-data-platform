"""Package runner: the control-flow skeleton every EXT_ORA_* package shared.

    Init Batch Variables -> Log Package Start -> [Get Watermark] -> [Read Source Max Key]
    -> [Truncate / Delete scope / Clear window] -> Data Flow(s) -> [post steps]
    -> [Set Watermark] -> Log Row Counts -> Log Package Success
    OnError -> Log Error + Log Package Failure

Every control call goes through dbx_etl_common (session 00); nothing here talks
to the etl.* tables directly.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dbx_etl_common import control

from oracle_extract.bronze_writer import countRows, prepareTarget, tableExists, writeBronze
from oracle_extract.model import WATERMARK_NUMERIC_KEY, ExtractSpec, SourceQuery
from oracle_extract.naming import deltaTable
from oracle_extract.specs import PROJECT_NAME, STEP_NAME
from oracle_extract.transforms import (
    addIngestionMetadata, applyConditionalSplit, applyConstants, applyDerivedColumns, applyLookup,
    assignRowNumberKey, rejectPayload,
)
from oracle_extract.watermark import WatermarkWindow, buildWindow, formatBound


@dataclass
class RunSettings:
    catalog: str
    batchId: int
    reloadFullHistory: bool
    environmentCode: str
    businessDate: Optional[object] = None
    restartFromStep: str = ""
    dryRun: bool = False


@dataclass
class RunSummary:
    packageName: str
    packageExecutionId: Optional[int]
    targetTable: str
    rowsRead: int = 0
    rowsInserted: int = 0
    rowsDeleted: int = 0
    rowsRejected: int = 0
    watermarkFrom: Optional[str] = None
    watermarkTo: Optional[str] = None
    counters: Dict[str, int] = field(default_factory=dict)
    status: str = "Succeeded"


class PackageRunner:
    def __init__(self, spark: SparkSession, reader, settings: RunSettings):
        self.spark = spark
        self.reader = reader
        self.settings = settings

    # -- watermark -----------------------------------------------------------------
    def resolveWindow(self, spec: ExtractSpec) -> Optional[WatermarkWindow]:
        if not spec.isIncremental:
            return None
        watermarkFrom, watermarkTo = control.getWatermark(
            self.spark, self.settings.catalog, spec.sourceSystemCode, spec.watermarkObject,
            reloadFullHistory=self.settings.reloadFullHistory,
        )
        window = buildWindow(spec.watermarkType, watermarkFrom, watermarkTo)
        if spec.watermarkType == WATERMARK_NUMERIC_KEY and spec.numericUpperBoundSql:
            # 'Read Source Max Key' Execute SQL Task (WatermarkTo := NVL(MAX(key), 0))
            maxKey = self.reader.scalar(self.spark, spec.numericUpperBoundSql)
            window = WatermarkWindow(window.watermarkType, window.fromValue, int(maxKey or 0))
        return window

    # -- one data flow ---------------------------------------------------------------
    def runSource(self, spec: ExtractSpec, source: SourceQuery, window: Optional[WatermarkWindow],
                  packageExecutionId: Optional[int], summary: RunSummary, extractedAtUtc: datetime) -> DataFrame:
        df = self.reader.read(self.spark, source, window)
        df = applyDerivedColumns(df, source.derived)
        df = applyConstants(df, spec.constantColumns)

        rowsRead = df.count()
        summary.rowsRead += rowsRead
        summary.counters[source.rowCountVariable] = summary.counters.get(source.rowCountVariable, 0) + rowsRead
        if source.rowCountVariable == "RowsDeleted":
            summary.rowsDeleted += rowsRead

        split = applyConditionalSplit(df, source.split)
        kept = split.matched
        routed = split.default
        if source.split is not None and source.split.defaultIsReject:
            rejected = self.logRejects(spec, routed, source.split.rejectReasonCode, source.split.name,
                                       packageExecutionId, businessKeyColumn=source.columns[0].name)
            summary.rowsRejected += rejected
        elif source.split is not None:
            kept = kept.unionByName(routed)   # both split outputs land in the same raw table

        lookup = applyLookup(kept, source.lookup, self.lookupReference(source))
        kept = lookup.matched
        if source.lookup is not None:
            rejected = self.logRejects(spec, lookup.unmatched, source.lookup.rejectReasonCode, source.lookup.name,
                                       packageExecutionId, reason=source.lookup.rejectReason,
                                       objectName=source.lookup.rejectObjectName,
                                       businessKeyColumn=source.columns[0].name)
            summary.rowsRejected += rejected
            summary.counters["UnmatchedGeographyCount" if "Geography" in source.lookup.name else "RowsRejected"] = rejected

        watermarkFrom = window.fromText if window else None
        watermarkTo = window.toText if window else None
        return addIngestionMetadata(kept, self.settings.batchId, packageExecutionId, spec.sourceSystemCode,
                                    watermarkFrom, watermarkTo, extractedAtUtc)

    def lookupReference(self, source: SourceQuery) -> Optional[DataFrame]:
        if source.lookup is None:
            return None
        fullName = deltaTable(self.settings.catalog, source.lookup.legacyTable)
        return self.spark.table(fullName)

    def logRejects(self, spec: ExtractSpec, rejectedDf: DataFrame, reasonCode: str, component: str,
                   packageExecutionId: Optional[int], reason: str = "", objectName: Optional[str] = None,
                   businessKeyColumn: Optional[str] = None) -> int:
        payload = rejectPayload(rejectedDf, businessKeyColumn, reasonCode, reason or component)
        return int(control.logRejectedRecordSet(
            self.spark, self.settings.catalog, objectName or spec.legacyTargetTable, payload,
            batchId=self.settings.batchId, packageExecutionId=packageExecutionId,
            sourceSystemCode=spec.sourceSystemCode, rejectStage="Extract",
            rejectReasonCode=reasonCode, businessKeyColumn="BusinessKey",
        ) or 0)

    # -- whole package -----------------------------------------------------------------
    def run(self, spec: ExtractSpec) -> RunSummary:
        catalog = self.settings.catalog
        fullName = deltaTable(catalog, spec.legacyTargetTable)
        packageExecutionId = control.logPackageStart(
            self.spark, catalog, self.settings.batchId, spec.packageName, projectName=PROJECT_NAME, stepName=STEP_NAME,
        )
        summary = RunSummary(spec.packageName, packageExecutionId, fullName)
        try:
            window = self.resolveWindow(spec)
            if window is not None:
                summary.watermarkFrom, summary.watermarkTo = window.fromText, window.toText

            mode = prepareTarget(self.spark, spec, fullName, window, self.settings.reloadFullHistory, packageExecutionId)
            extractedAtUtc = datetime.now(timezone.utc).replace(tzinfo=None)

            frames = [self.runSource(spec, s, window, packageExecutionId, summary, extractedAtUtc) for s in spec.sources]
            output = frames[0]
            for extra in frames[1:]:
                output = output.unionByName(extra, allowMissingColumns=True)

            output = self.applyPostSteps(spec, output, summary)
            writeBronze(output, fullName, mode)

            summary.rowsInserted = countRows(self.spark, fullName, f"PackageExecutionId = {int(packageExecutionId)}")
            self.applyTargetPostSteps(spec, fullName, summary)

            if window is not None and window.toText is not None:
                # legacy usp_SetWatermark: NULL WatermarkTo leaves the watermark untouched
                control.setWatermark(self.spark, catalog, spec.sourceSystemCode, spec.watermarkObject, window.toText,
                                     packageExecutionId=packageExecutionId,
                                     allowRewind=self.settings.reloadFullHistory)

            control.logRowCount(
                self.spark, catalog, packageExecutionId, spec.legacyTargetTable,
                sourceRowCount=summary.rowsRead, targetRowCount=summary.rowsInserted,
                insertRowCount=summary.rowsInserted, deleteRowCount=summary.rowsDeleted or None,
                rejectRowCount=summary.rowsRejected or None,
            )
            control.logPackageEnd(
                self.spark, catalog, packageExecutionId, status="Succeeded",
                rowsRead=summary.rowsRead, rowsInserted=summary.rowsInserted, rowsDeleted=summary.rowsDeleted,
                rowsRejected=summary.rowsRejected, watermarkFrom=summary.watermarkFrom, watermarkTo=summary.watermarkTo,
            )
            return summary
        except Exception as exc:  # OnError event handler: Log Error -> Log Package Failure
            summary.status = "Failed"
            control.logError(
                self.spark, catalog, packageExecutionId=packageExecutionId, batchId=self.settings.batchId,
                errorSeverity="Error", sourceName=spec.packageName, sourceComponent=type(exc).__name__,
                procedureName="oracle_extract.runner.PackageRunner.run", errorDescription=str(exc)[:4000],
            )
            control.logPackageEnd(self.spark, catalog, packageExecutionId, status="Failed",
                                  rowsRead=summary.rowsRead, rowsRejected=summary.rowsRejected,
                                  watermarkFrom=summary.watermarkFrom, watermarkTo=summary.watermarkTo)
            raise

    # -- package-specific Execute SQL Tasks after the data flow ------------------------
    def applyPostSteps(self, spec: ExtractSpec, df: DataFrame, summary: RunSummary) -> DataFrame:
        if "assignCostCenterSurrogateKeys" in spec.postSteps:
            df = assignRowNumberKey(df, "CostCenterKey", "COST_CENTER_CD")
        return df

    def applyTargetPostSteps(self, spec: ExtractSpec, fullName: str, summary: RunSummary) -> None:
        if "captureInsertCount" in spec.postSteps:
            summary.counters["RowsInserted"] = summary.rowsInserted
        if "countSuppressedHolds" in spec.postSteps:
            summary.counters["HeldInvoiceCount"] = self.countSuppressedHolds()
        if "countMissingRateDays" in spec.postSteps:
            summary.counters["MissingRateCount"] = self.countMissingRatePairs(fullName)

    def countSuppressedHolds(self) -> int:
        try:
            value = self.reader.scalar(self.spark, "SELECT COUNT(*) AS HELD_COUNT FROM WWI_FIN.AP_INVOICE_HOLD WHERE RELEASE_DT IS NULL")
            return int(value or 0)
        except NotImplementedError:
            return -1

    def countMissingRatePairs(self, fullName: str) -> int:
        """'Count Missing Rate Days': mandatory pairs from etl.Configuration(FxMandatoryPairs)
        that have no row in raw.OracleFxRate. The legacy row-per-pair configuration is
        read as a comma-separated list through control.getConfiguration."""
        try:
            configured = control.getConfiguration(self.spark, self.settings.catalog, "FxMandatoryPairs",
                                                  environmentCode=self.settings.environmentCode)
        except Exception:
            configured = None
        pairs = [p.strip() for p in (configured or "").replace(";", ",").split(",") if p.strip()]
        if not pairs or not tableExists(self.spark, fullName):
            return 0
        present = {r[0] for r in self.spark.table(fullName).select("RatePairCd").distinct().collect()}
        return len([p for p in pairs if p not in present])


def ensureBatch(spark: SparkSession, settings: RunSettings, batchName: str = "WWI_Extract_Oracle") -> int:
    """Packages received BatchId from Master_Daily_ETL. When the bundle job runs on its own
    (BatchId="0") an ad-hoc batch is started (or the running one adopted)."""
    if settings.batchId and int(settings.batchId) > 0:
        return int(settings.batchId)
    return int(control.startBatch(
        spark, settings.catalog, batchName, batchType="Adhoc", businessDate=settings.businessDate,
        environmentCode=settings.environmentCode, allowAdoptRunning=True, notes="started by wwi_01_oracle_extract",
    ))
