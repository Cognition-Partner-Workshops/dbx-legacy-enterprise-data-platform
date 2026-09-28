"""Package lifecycle shared by the twelve ``AGG_Refresh_*`` notebooks.

Mirrors the SSIS control flow emitted by ``build_aggregate_packages.py``:

    Init Refresh Window -> Log Package Start -> Delete Refresh Window
      -> Rebuild Aggregate (data flow: source query, derived columns,
         conditional split to reject / insert outputs, row counts)
      -> Assert Reconciliation -> Log Rejected Records -> Log Row Counts
      -> Log Package Success   (OnError: Log Error -> Mark Execution Failed)

``control`` is the ``dbx_etl_common.control`` module (or the pytest fake); it
is passed in rather than imported so this module stays testable offline.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from agg_common import (
    LEGACY_OBJECT_NAMES, PROJECT_NAME, REJECT_REASON, REJECT_REASON_CODE, SOURCE_SYSTEM_CODE,
    assertAggregateReconciliation, overwriteFull, overwriteWindow, splitRejects, stampRefresh,
)
from agg_sql import AGGREGATE_KEY_COLUMNS


@dataclass
class RefreshSpec:
    packageName: str
    targetKey: str                              # logical name in agg_common.TABLES
    sql: str                                    # full SELECT for the refresh scope
    replacePredicate: Optional[str]             # None => full rebuild (TRUNCATE + INSERT)
    rejectCondition: Optional[Column] = None    # Conditional Split reject output
    rejectReasonCode: str = REJECT_REASON_CODE
    rejectReason: str = REJECT_REASON
    keyMeasures: Sequence[str] = field(default_factory=tuple)
    watermarkTo: Optional[dt.datetime] = None
    stepName: str = "Aggregates"
    postProcess: Optional[Callable[[DataFrame], DataFrame]] = None


@dataclass
class RefreshResult:
    sourceRowCount: int
    targetRowCount: int
    rejectRowCount: int
    measureTotals: Dict[str, float]


def _businessKey(df: DataFrame, keyColumns: Sequence[str]) -> DataFrame:
    return df.withColumn(
        "business_key",
        F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in keyColumns]))


def buildRefreshFrame(spark: SparkSession, spec: RefreshSpec, batchId: int) -> DataFrame:
    df = spark.sql(spec.sql)
    if spec.postProcess is not None:
        df = spec.postProcess(df)
    return stampRefresh(df, batchId)


def runRefresh(spark: SparkSession, control, catalog: str, batchId: int, spec: RefreshSpec,
               targetTable: str) -> RefreshResult:
    objectName = LEGACY_OBJECT_NAMES[spec.targetKey]
    keyColumns = AGGREGATE_KEY_COLUMNS[spec.targetKey]
    with control.packageRun(spark, catalog, batchId, spec.packageName,
                            projectName=PROJECT_NAME, stepName=spec.stepName) as run:
        packageExecutionId = run.packageExecutionId

        df = buildRefreshFrame(spark, spec, batchId).cache()
        sourceRowCount = df.count()

        if spec.rejectCondition is not None:
            accepted, rejected = splitRejects(df, spec.rejectCondition)
            rejectRowCount = control.logRejectedRecordSet(
                spark, catalog, objectName, _businessKey(rejected, keyColumns),
                batchId=batchId, packageExecutionId=packageExecutionId,
                sourceSystemCode=SOURCE_SYSTEM_CODE, rejectStage="Aggregate",
                rejectReasonCode=spec.rejectReasonCode, businessKeyColumn="business_key")
        else:
            accepted, rejectRowCount = df, 0

        if spec.replacePredicate is None:
            targetRowCount = overwriteFull(spark, accepted, targetTable)
        else:
            targetRowCount = overwriteWindow(spark, accepted, targetTable, spec.replacePredicate)

        assertAggregateReconciliation(sourceRowCount, targetRowCount, rejectRowCount, objectName)

        control.logRowCount(spark, catalog, packageExecutionId, objectName,
                            sourceRowCount=sourceRowCount, targetRowCount=targetRowCount,
                            insertRowCount=targetRowCount, rejectRowCount=rejectRowCount)
        # usp_AssertRowCountReconciliation: source - target must equal the rejected rows.
        control.assertRowCountTolerance(spark, catalog, batchId, scope="OBJECT", objectName=objectName,
                                        absoluteTolerance=rejectRowCount, raiseOnFailure=True)

        watermarkTo = spec.watermarkTo or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        control.setWatermark(spark, catalog, SOURCE_SYSTEM_CODE, objectName, watermarkTo,
                             packageExecutionId=packageExecutionId, allowRewind=True)

        totals: Dict[str, float] = {}
        if spec.keyMeasures:
            row = accepted.agg(*[F.sum(F.col(c)).alias(c) for c in spec.keyMeasures]).collect()[0]
            totals = {c: float(row[c]) if row[c] is not None else 0.0 for c in spec.keyMeasures}

        run.rowsRead = sourceRowCount
        run.rowsInserted = targetRowCount
        run.rowsRejected = rejectRowCount
        run.rowsDeleted = 0
        df.unpersist()
    return RefreshResult(sourceRowCount, targetRowCount, rejectRowCount, totals)
