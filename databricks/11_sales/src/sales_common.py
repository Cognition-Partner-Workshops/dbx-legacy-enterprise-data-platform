"""Helpers shared by every WWI_Sales notebook: parameters, column resolution,
Delta table maintenance and the RestartFromStep rule.

The transformation modules (sales_commission, sales_quota, sales_promotion,
sales_partner_feed) are pure DataFrame -> DataFrame functions so they can be
unit tested with a local SparkSession; everything that touches the catalog or
the control framework goes through here or through the notebooks.
"""
from __future__ import annotations

import calendar
import re
from datetime import date, datetime, timedelta
from typing import Iterable, Mapping, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

PROJECT_NAME = "WWI_Sales"
STEP_NAME = "Sales Mart"

# Phases of Master_Daily_ETL that run AFTER "Sales Mart" (ssis/orchestration-plan.json,
# sequence > 90). When RestartFromStep names one of them the whole Sales Mart phase is
# skipped, mirroring Invoke-EstateOrchestration.ps1 resuming a failed batch from a later step.
STEPS_AFTER_SALES_MART = (
    "Inventory Mart",
    "Procurement Mart",
    "Customer 360 Build",
    "Customer 360 Publish",
    "Publish Reporting Layer",
    "Failure Handling",
)

SAMPLE_LINE_TYPES = ("SAMPLE", "INTERNAL")
MONEY = T.DecimalType(18, 2)
RATE = T.DecimalType(18, 8)

_TRUE = {"true", "1", "yes", "y", "t"}


# ---------------------------------------------------------------------------
# parameters
# ---------------------------------------------------------------------------
def parseBool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text == "":
        return default
    return text in _TRUE


def parseOptional(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parseCsvList(value) -> list[str]:
    return [item.strip().upper() for item in str(value or "").split(",") if item.strip()]


def monthPeriod(businessDate: date) -> str:
    """CONVERT(char(7), d, 126) -> 'YYYY-MM'."""
    return businessDate.strftime("%Y-%m")


def endOfMonth(businessDate: date) -> date:
    lastDay = calendar.monthrange(businessDate.year, businessDate.month)[1]
    return businessDate.replace(day=lastDay)


def resolveBusinessDate(value) -> date:
    text = parseOptional(value)
    if text is None:
        return datetime.utcnow().date()
    if isinstance(value, date):
        return value
    return datetime.strptime(text, "%Y-%m-%d").date()


def getWidget(dbutils, name: str, default: str = "") -> str:
    """Read a job parameter / widget, defining it first so interactive runs work."""
    try:
        dbutils.widgets.text(name, default)
    except Exception:  # widget already defined with a job parameter value
        pass
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        value = default
    return value if value is not None else default


def shouldSkipForRestart(restartFromStep, packageName: str) -> bool:
    """True when the master batch is being resumed from a step after Sales Mart."""
    step = parseOptional(restartFromStep)
    if step is None or step == packageName or step == STEP_NAME:
        return False
    return step in STEPS_AFTER_SALES_MART


# ---------------------------------------------------------------------------
# column resolution
# ---------------------------------------------------------------------------
def _snake(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)).strip("_").lower()


def legacyColumnCandidates(legacyName: str) -> list[str]:
    """Names a legacy column may carry in Delta: PascalCase without spaces (this
    project's convention), the legacy spelling with spaces, and snake_case."""
    pascal = legacyName.replace(" ", "")
    candidates = [pascal, legacyName, _snake(legacyName)]
    seen: list[str] = []
    for c in candidates:
        if c not in seen:
            seen.append(c)
    return seen


def resolveColumns(
    df: DataFrame,
    columnMap: Mapping[str, Sequence[str] | str],
    optional: Iterable[str] = (),
    keepOthers: bool = False,
) -> DataFrame:
    """Return df with the legacy column names used by the SSIS packages.

    columnMap maps legacy name -> candidate column names in the Delta table (first
    present wins). Missing required columns raise a ValueError naming them, so a
    schema drift in an upstream session fails loudly instead of silently zeroing
    a commission.
    """
    optionalSet = set(optional)
    lowerToActual = {c.lower(): c for c in df.columns}
    selected: list[Column] = []
    missing: list[str] = []
    used: set[str] = set()
    for legacyName, candidates in columnMap.items():
        if isinstance(candidates, str):
            candidates = [candidates]
        actual = None
        for candidate in candidates:
            if candidate.lower() in lowerToActual:
                actual = lowerToActual[candidate.lower()]
                break
        if actual is None:
            if legacyName in optionalSet:
                selected.append(F.lit(None).alias(legacyName))
            else:
                missing.append("%s (tried %s)" % (legacyName, ", ".join(candidates)))
            continue
        used.add(actual)
        selected.append(F.col("`%s`" % actual).alias(legacyName))
    if missing:
        raise ValueError("Source is missing legacy columns: " + "; ".join(missing))
    if keepOthers:
        selected.extend(F.col("`%s`" % c) for c in df.columns if c not in used)
    return df.select(*selected)


def batchFilter(df: DataFrame, batchColumn: str, batchId: int, reloadFullHistory: bool) -> DataFrame:
    """Legacy `WHERE LoadBatchId = ?`; ReloadFullHistory recomputes over every staged row."""
    if reloadFullHistory:
        return df
    return df.where(F.col(batchColumn) == F.lit(int(batchId)))


# ---------------------------------------------------------------------------
# Delta maintenance
# ---------------------------------------------------------------------------
def conformToSchema(df: DataFrame, schema: T.StructType) -> DataFrame:
    """Project df onto schema: missing columns become NULL, types are cast, extras dropped."""
    lowerToActual = {c.lower(): c for c in df.columns}
    cols = []
    for field in schema.fields:
        actual = lowerToActual.get(field.name.lower())
        if actual is None:
            cols.append(F.lit(None).cast(field.dataType).alias(field.name))
        else:
            cols.append(F.col("`%s`" % actual).cast(field.dataType).alias(field.name))
    return df.select(*cols)


def ensureTable(spark: SparkSession, fullName: str, schema: T.StructType, partitionBy: Sequence[str] = ()) -> None:
    ddl = ", ".join("`%s` %s" % (f.name, f.dataType.simpleString()) for f in schema.fields)
    sql = "CREATE TABLE IF NOT EXISTS %s (%s) USING DELTA" % (fullName, ddl)
    if partitionBy:
        sql += " PARTITIONED BY (%s)" % ", ".join("`%s`" % c for c in partitionBy)
    spark.sql(sql)


def overwriteTable(df: DataFrame, fullName: str, schema: T.StructType) -> int:
    """TRUNCATE TABLE + OLE DB destination == Delta overwrite. Returns rows written."""
    conformed = conformToSchema(df, schema).cache()
    rows = conformed.count()
    conformed.write.format("delta").mode("overwrite").saveAsTable(fullName)
    conformed.unpersist()
    return rows


def replaceWhere(df: DataFrame, fullName: str, schema: T.StructType, predicate: str) -> int:
    conformed = conformToSchema(df, schema).cache()
    rows = conformed.count()
    (conformed.write.format("delta").mode("overwrite")
     .option("replaceWhere", predicate).saveAsTable(fullName))
    conformed.unpersist()
    return rows


def mergeInto(spark: SparkSession, df: DataFrame, fullName: str, schema: T.StructType,
              keyColumns: Sequence[str]) -> dict:
    """Idempotent upsert keyed on keyColumns. Returns Delta MERGE metrics when available."""
    conformed = conformToSchema(df, schema)
    viewName = "src_" + re.sub(r"[^0-9a-zA-Z]", "_", fullName)
    conformed.createOrReplaceTempView(viewName)
    on = " AND ".join("t.`%s` <=> s.`%s`" % (k, k) for k in keyColumns)
    result = spark.sql(
        "MERGE INTO %s AS t USING %s AS s ON %s "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *" % (fullName, viewName, on)
    )
    metrics = {"num_affected_rows": None, "num_updated_rows": None, "num_inserted_rows": None}
    try:
        row = result.collect()[0].asDict()
        for k in metrics:
            if k in row:
                metrics[k] = row[k]
    except Exception:
        pass
    spark.catalog.dropTempView(viewName)
    return metrics


def scalarCount(df: DataFrame) -> int:
    return int(df.count())


def yesterday(businessDate: date) -> date:
    return businessDate - timedelta(days=1)


# ---------------------------------------------------------------------------
# control framework boilerplate shared by the six notebooks
# ---------------------------------------------------------------------------
STANDARD_WIDGETS = (
    ("BatchId", "0"), ("BusinessDate", ""), ("ReloadFullHistory", "False"),
    ("EnvironmentCode", "DEV"), ("RestartFromStep", ""), ("catalog", ""),
)


class PackageContext:
    """Resolved standard parameters of a notebook run."""

    def __init__(self, catalog: str, batchId: int, businessDate: date, reloadFullHistory: bool,
                 environmentCode: str, restartFromStep: str, ownsBatch: bool = False):
        self.catalog = catalog
        self.batchId = batchId
        self.businessDate = businessDate
        self.reloadFullHistory = reloadFullHistory
        self.environmentCode = environmentCode
        self.restartFromStep = restartFromStep
        self.ownsBatch = ownsBatch

    def table(self, naming, schema: str, table: str) -> str:
        return naming.table(self.catalog, schema, table)


def resolveContext(spark: SparkSession, dbutils, params, control, packageName: str,
                   extraWidgets: Sequence[tuple[str, str]] = ()) -> PackageContext:
    """Define the widgets, read them through dbx_etl_common.params and, when the notebook is
    run standalone (BatchId = 0), open its own etl.batch so the lifecycle rows have a parent."""
    for name, default in tuple(STANDARD_WIDGETS) + tuple(extraWidgets):
        getWidget(dbutils, name, default)
    p = params.getJobParams(dbutils)
    catalog = parseOptional(p.get("catalog"))
    if catalog is None:
        raise ValueError("Job parameter 'catalog' is required")
    batchId = int(p.get("batchId") or 0)
    businessDate = resolveBusinessDate(p.get("businessDate"))
    environmentCode = parseOptional(p.get("environmentCode")) or "DEV"
    ctx = PackageContext(catalog, batchId, businessDate, parseBool(p.get("reloadFullHistory")),
                         environmentCode, parseOptional(p.get("restartFromStep")) or "")
    if ctx.batchId == 0:
        ctx.batchId = int(control.startBatch(
            spark, catalog, "wwi_11_sales", batchType="Daily", businessDate=businessDate,
            environmentCode=environmentCode, notes="Standalone run of %s" % packageName))
        ctx.ownsBatch = True
    return ctx


class PackageRun:
    """Mutable row counters for one package execution (User::RowsRead etc.)."""

    def __init__(self, packageExecutionId):
        self.packageExecutionId = packageExecutionId
        self.rowsRead = 0
        self.rowsInserted = 0
        self.rowsUpdated = 0
        self.rowsDeleted = 0
        self.rowsRejected = 0
        self.currentTask = "Log Package Start"


class legacyPackageRun:
    """Context manager reproducing the legacy package control flow around the work:

        Log Package Start -> <work> -> Log Package Success
        OnError: Log Error -> Mark Execution Failed (re-raised)

    Implemented purely with dbx_etl_common.control calls (logPackageStart / logPackageEnd /
    logError) so the packageExecutionId is available for control.logRowCount.
    """

    def __init__(self, spark: SparkSession, control, ctx: PackageContext, packageName: str):
        self.spark = spark
        self.control = control
        self.ctx = ctx
        self.packageName = packageName
        self.run: PackageRun | None = None

    def __enter__(self) -> PackageRun:
        packageExecutionId = self.control.logPackageStart(
            self.spark, self.ctx.catalog, self.ctx.batchId, self.packageName,
            projectName=PROJECT_NAME, stepName=STEP_NAME)
        self.run = PackageRun(packageExecutionId)
        return self.run

    def __exit__(self, excType, exc, tb) -> bool:
        run = self.run
        try:
            if exc is None:
                self.control.logPackageEnd(
                    self.spark, self.ctx.catalog, run.packageExecutionId, status="Succeeded",
                    rowsRead=run.rowsRead, rowsInserted=run.rowsInserted, rowsUpdated=run.rowsUpdated,
                    rowsDeleted=run.rowsDeleted, rowsRejected=run.rowsRejected)
            else:
                self.control.logError(
                    self.spark, self.ctx.catalog, packageExecutionId=run.packageExecutionId,
                    batchId=self.ctx.batchId, errorSeverity="Error", errorCode=type(exc).__name__,
                    sourceName=self.packageName, sourceComponent=run.currentTask,
                    errorDescription=str(exc)[:4000])
                self.control.logPackageEnd(self.spark, self.ctx.catalog, run.packageExecutionId,
                                           status="Failed", rowsRead=run.rowsRead,
                                           rowsInserted=run.rowsInserted, rowsUpdated=run.rowsUpdated,
                                           rowsRejected=run.rowsRejected)
        finally:
            if self.ctx.ownsBatch:
                self.control.endBatch(self.spark, self.ctx.catalog, self.ctx.batchId,
                                      forceStatus=None if exc is None else "Failed")
        return False
