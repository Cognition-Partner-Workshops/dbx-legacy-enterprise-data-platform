"""Notebook-side helpers: job parameters, Delta I/O and the package lifecycle wrapper around
the shared ``dbx_etl_common`` control layer. Nothing in here re-implements control logic."""
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dbx_etl_common import control, params

PROJECT_NAME = "WWI_Customer360"


@dataclass
class JobContext:
    catalog: str
    batchId: int
    businessDate: date
    reloadFullHistory: bool
    environmentCode: str
    restartFromStep: str


def getJobContext(dbutils) -> JobContext:
    p = params.getJobParams(dbutils)
    return JobContext(
        catalog=p["catalog"],
        batchId=int(p["batchId"]),
        businessDate=p["businessDate"],
        reloadFullHistory=bool(p["reloadFullHistory"]),
        environmentCode=p["environmentCode"],
        restartFromStep=p["restartFromStep"] or "",
    )


def widget(dbutils, name: str, default: str) -> str:
    """Package-level parameter (legacy $Package::X) passed as a task base_parameter."""
    dbutils.widgets.text(name, default)
    value = dbutils.widgets.get(name)
    return value if value not in (None, "") else default


def asBool(value) -> bool:
    return str(value).strip().lower() in ("1", "true", "t", "yes", "y")


def normalizeColumns(df: DataFrame) -> DataFrame:
    """Legacy warehouse columns carry spaces ([Customer Key]); Delta consumers may have kept
    them or squashed them to PascalCase. Squash so both spellings resolve to ``CustomerKey``."""
    renamed = df
    for c in df.columns:
        squashed = re.sub(r"[^A-Za-z0-9_]", "", c)
        if squashed != c:
            renamed = renamed.withColumnRenamed(c, squashed)
    return renamed


def readTable(spark, fullName: str, required: bool = True):
    if not spark.catalog.tableExists(fullName):
        if required:
            raise RuntimeError(f"Required table {fullName} does not exist")
        return None
    return normalizeColumns(spark.table(fullName))


def readTableOrEmpty(spark, fullName: str, schemaLike: DataFrame) -> DataFrame:
    """Read a stateful work table, or an empty frame with the given schema when it has not been
    created yet (first run)."""
    df = readTable(spark, fullName, required=False)
    if df is None:
        return spark.createDataFrame([], schemaLike.schema)
    return df


def stamp(df: DataFrame, ctx: JobContext) -> DataFrame:
    return df.withColumn("BatchId", F.lit(ctx.batchId).cast("bigint")).withColumn(
        "BusinessDate", F.lit(ctx.businessDate).cast("date")
    )


def overwriteTable(df: DataFrame, fullName: str) -> int:
    """TRUNCATE + INSERT semantics (legacy work / Customer360 rebuild) as one atomic Delta
    overwrite, so a re-run for the same BatchId/BusinessDate is idempotent."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fullName)
    return df.sparkSession.table(fullName).count()


def appendTable(df: DataFrame, fullName: str) -> int:
    rows = df.count()
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fullName)
    return rows


def replaceView(spark, viewName: str, tableName: str):
    """Publish step: repoint the reporting view at the freshly loaded table (the Delta analogue
    of the legacy view / table swap)."""
    spark.sql(f"CREATE OR REPLACE VIEW {viewName} AS SELECT * FROM {tableName}")


class PackageRun:
    def __init__(self, packageExecutionId):
        self.packageExecutionId = packageExecutionId
        self.rowsRead = 0
        self.rowsInserted = 0
        self.rowsUpdated = 0
        self.rowsDeleted = 0
        self.rowsRejected = 0


@contextmanager
def packageLifecycle(spark, ctx: JobContext, packageName: str, stepName: str):
    """Log Package Start -> body -> Log Package Success, or OnError handler
    (Log Error + Mark Execution Failed) and re-raise. Uses the contractual
    ``control.logPackageStart`` / ``logPackageEnd`` / ``logError`` functions."""
    packageExecutionId = control.logPackageStart(
        spark, ctx.catalog, ctx.batchId, packageName, projectName=PROJECT_NAME, stepName=stepName
    )
    run = PackageRun(packageExecutionId)
    try:
        yield run
    except Exception as exc:  # noqa: BLE001 - mirrors the SSIS OnError event handler
        control.logError(
            spark, ctx.catalog, packageExecutionId=packageExecutionId, batchId=ctx.batchId,
            errorSeverity="Error", errorCode=type(exc).__name__, sourceName=packageName,
            sourceComponent=stepName, errorDescription=str(exc)[:4000],
        )
        control.logPackageEnd(
            spark, ctx.catalog, packageExecutionId, status="Failed", rowsRead=run.rowsRead,
            rowsInserted=run.rowsInserted, rowsUpdated=run.rowsUpdated, rowsRejected=run.rowsRejected,
        )
        raise
    control.logPackageEnd(
        spark, ctx.catalog, packageExecutionId, status="Succeeded", rowsRead=run.rowsRead,
        rowsInserted=run.rowsInserted, rowsUpdated=run.rowsUpdated, rowsDeleted=run.rowsDeleted,
        rowsRejected=run.rowsRejected,
    )


def logRowCounts(spark, ctx: JobContext, run: PackageRun, objectName: str):
    """Execute SQL Task 'Log Row Counts' (etl.usp_LogRowCount)."""
    control.logRowCount(
        spark, ctx.catalog, run.packageExecutionId, objectName,
        sourceRowCount=run.rowsRead, targetRowCount=run.rowsInserted,
        insertRowCount=run.rowsInserted, updateRowCount=run.rowsUpdated,
        deleteRowCount=run.rowsDeleted, rejectRowCount=run.rowsRejected,
    )


def logWarning(spark, ctx: JobContext, run: PackageRun, packageName: str, component: str, message: str):
    control.logError(
        spark, ctx.catalog, packageExecutionId=run.packageExecutionId, batchId=ctx.batchId,
        errorSeverity="Warning", errorCode="C360_WARN", sourceName=packageName,
        sourceComponent=component, errorDescription=message,
    )


PACKAGE_ORDER = [
    "C360_Build_CustomerProfile",
    "C360_Build_RollingMetrics",
    "C360_Build_LoyaltyOverlay",
    "C360_Build_ChurnFlags",
    "C360_Publish_Segments",
]


def shouldSkipForRestart(ctx: JobContext, packageName: str) -> bool:
    """RestartFromStep: packages that precede the named step in the legacy sequence are skipped
    (Invoke-EstateOrchestration.ps1 semantics). Unknown / empty step -> run everything."""
    step = (ctx.restartFromStep or "").strip()
    if not step or step not in PACKAGE_ORDER or packageName not in PACKAGE_ORDER:
        return False
    return PACKAGE_ORDER.index(packageName) < PACKAGE_ORDER.index(step)
