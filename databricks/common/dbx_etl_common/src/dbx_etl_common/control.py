"""Control-framework API (re-implementation of ``sqlserver/control/procedures/*.sql``).

Every function takes ``(spark, catalog, ...)`` and works against ``<catalog>.etl.*`` Delta tables
created by :mod:`dbx_etl_common.bootstrap`. Public signatures are the shared interface contract
and must not change (new keyword arguments may be appended, never renamed/reordered/removed).

Identity values
    Delta ``GENERATED ALWAYS AS IDENTITY`` values are generated at write time and cannot be
    returned by the ``INSERT`` itself, so every "start" function reads the row back with a
    predicate that is unique for the writer (``BatchName + BusinessDate + Status='Running'``,
    ``BatchId + StepName + AttemptNumber``, ``PackageName + BatchId + AttemptNumber``) and takes
    ``max(<id>)``. This is exact as long as one orchestration run does not start the *same*
    batch/step/package twice at the same instant, which mirrors the SSIS semantics
    (``usp_StartBatch`` refuses a second running batch; steps/packages carry ``AttemptNumber``).
    Identity values are unique and increasing but NOT contiguous.

Concurrency
    Delta uses optimistic concurrency; concurrent appends to the same control table never
    conflict, and concurrent ``UPDATE``s of different rows only conflict when the table is not
    partitioned and file-level conflict detection kicks in. All mutating statements are wrapped
    in :func:`_withRetry`, which retries ``ConcurrentAppend/Modification/DeleteRead`` errors with
    exponential back-off (parallel job tasks share ``etl.package_execution``).
"""
from __future__ import annotations

import datetime as _dt
import decimal as _decimal
import random
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, TypeVar

from . import naming
from .schema import TABLES_BY_NAME, Table

T = TypeVar("T")

ISO_MS = "%Y-%m-%dT%H:%M:%S.%f"
DEFAULT_EPOCH = "1900-01-01T00:00:00"
CONCURRENCY_MARKERS = ("Concurrent", "DELTA_CONCURRENT", "MetadataChangedException", "ProtocolChangedException")


class ControlError(RuntimeError):
    """Raised where the legacy procedure used ``THROW`` / ``RAISERROR`` (error number preserved)."""

    def __init__(self, number: int, message: str):
        super().__init__(f"{number}: {message}")
        self.number = number
        self.message = message


# ----------------------------------------------------------------------------- low level helpers

def _utcnow() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)


def _iso(ts: _dt.datetime) -> str:
    """SQL Server ``CONVERT(NVARCHAR, ..., 126)`` for DATETIME2(3): ``yyyy-MM-ddTHH:mm:ss.fff``."""
    return ts.strftime(ISO_MS)[:-3]


def _parseTimestamp(value: Optional[str]) -> Optional[_dt.datetime]:
    """``TRY_CONVERT(DATETIME2(3), value)``."""
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    try:
        ts = _dt.datetime.fromisoformat(v.replace("Z", "+00:00").replace(" ", "T"))
    except ValueError:
        return None
    if ts.tzinfo is not None:
        ts = ts.astimezone(_dt.timezone.utc).replace(tzinfo=None)
    return ts


def _parseInt(value: Optional[str]) -> Optional[int]:
    try:
        return int(str(value).strip()) if value is not None and str(value).strip() != "" else None
    except ValueError:
        return None


def _parseDecimal(value: Optional[str]) -> Optional[_decimal.Decimal]:
    try:
        return _decimal.Decimal(str(value).strip()) if value is not None and str(value).strip() != "" else None
    except (ValueError, _decimal.InvalidOperation):
        return None


def _t(catalog: str, name: str) -> str:
    return naming.controlTable(catalog, name)


def _withRetry(fn: Callable[[], T], attempts: int = 8, baseSleep: float = 0.5) -> T:
    """Retry Delta optimistic-concurrency failures with exponential back-off + jitter."""
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - we inspect the class name / message
            text = f"{type(exc).__name__}: {exc}"
            if attempt == attempts - 1 or not any(m in text for m in CONCURRENCY_MARKERS):
                raise
            time.sleep(baseSleep * (2 ** attempt) + random.uniform(0, 0.25))
    raise AssertionError("unreachable")


def _sql(spark: Any, statement: str, **args: Any) -> Any:
    params = {k: v for k, v in args.items()}
    return _withRetry(lambda: spark.sql(statement, params) if params else spark.sql(statement))


def _scalar(spark: Any, statement: str, **args: Any) -> Any:
    row = _sql(spark, statement, **args).first()
    return None if row is None else row[0]


def _rows(spark: Any, statement: str, **args: Any) -> List[Dict[str, Any]]:
    return [r.asDict() for r in _sql(spark, statement, **args).collect()]


def _insert(spark: Any, catalog: str, tableName: str, rows: Sequence[Dict[str, Any]]) -> int:
    """Append rows to a control table, filling omitted columns with the spec defaults.

    Identity / generated columns are never written; ``current_timestamp()`` / ``current_user()``
    defaults are evaluated by Spark, literal defaults are taken from the spec.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructField, StructType, _parse_datatype_string

    spec: Table = TABLES_BY_NAME[tableName]
    if not rows:
        return 0
    fields, exprCols = [], {}
    for col in spec.writableColumns:
        present = any(col.name in r for r in rows)
        if not present and col.default is not None:
            exprCols[col.name] = col.default
            continue
        fields.append(StructField(col.name, _parse_datatype_string(col.dataType.lower()), True))
    data = [tuple(_coerce(r.get(f.name), f.dataType.simpleString()) for f in fields) for r in rows]
    df = spark.createDataFrame(data, StructType(fields))
    for name, expr in exprCols.items():
        df = df.withColumn(name, F.expr(expr))
    ordered = [c.name for c in spec.writableColumns if c.name in df.columns]
    df = df.select(*ordered)
    _withRetry(lambda: df.write.format("delta").mode("append").saveAsTable(_t(catalog, tableName)))
    return len(rows)


def _coerce(value: Any, simpleType: str) -> Any:
    if value is None:
        return None
    if simpleType.startswith("decimal"):
        return value if isinstance(value, _decimal.Decimal) else _decimal.Decimal(str(value))
    if simpleType in ("bigint", "int", "smallint", "tinyint"):
        return int(value)
    if simpleType == "boolean":
        return bool(value)
    if simpleType == "string":
        return str(value)
    if simpleType == "date" and isinstance(value, _dt.datetime):
        return value.date()
    if simpleType == "timestamp" and isinstance(value, str):
        return _parseTimestamp(value)
    return value


def _nullIfZero(value: Optional[int]) -> Optional[int]:
    return None if value is None or int(value) == 0 else int(value)


# ----------------------------------------------------------------------------- configuration

def getConfigurationValue(spark: Any, catalog: str, configurationKey: str, environmentCode: str) -> Optional[str]:
    """``etl.ufn_GetConfigurationValue``: environment-specific value first, then ``ALL``; never sensitive keys."""
    return _scalar(
        spark,
        f"SELECT ConfigurationValue FROM {_t(catalog, 'configuration')} "
        "WHERE ConfigurationKey = :key AND EnvironmentCode IN (:env, 'ALL') AND IsSensitive = false "
        "ORDER BY CASE WHEN EnvironmentCode = :env THEN 0 ELSE 1 END LIMIT 1",
        key=configurationKey, env=environmentCode,
    )


def getConfiguration(spark: Any, catalog: str, configurationKey: str, environmentCode: Optional[str] = None) -> str:
    """``etl.usp_GetConfiguration``: raises ``ControlError(51020)`` when the key is not defined."""
    if environmentCode is None:
        environmentCode = getConfigurationValue(spark, catalog, "EnvironmentCode", "ALL")
    value = getConfigurationValue(spark, catalog, configurationKey, environmentCode or "DEV")
    if value is None:
        raise ControlError(51020, f"Configuration key {configurationKey} is not defined for environment "
                                  f"{environmentCode or '(unknown)'}.")
    return value


# ----------------------------------------------------------------------------- batch lifecycle

def findRunningBatch(spark: Any, catalog: str, batchName: str, businessDate: _dt.date) -> Optional[int]:
    """Most recent ``Running`` batch for the name/business date (``None`` when there is none)."""
    return _scalar(
        spark,
        f"SELECT max(BatchId) FROM {_t(catalog, 'batch')} "
        "WHERE BatchName = :name AND BusinessDate = :bd AND Status = 'Running'",
        name=batchName, bd=businessDate,
    )


def startBatch(spark: Any, catalog: str, batchName: str, batchType: str = "Daily",
               businessDate: Optional[_dt.date] = None, environmentCode: Optional[str] = None,
               allowAdoptRunning: bool = False, notes: Optional[str] = None) -> int:
    """``etl.usp_StartBatch``. Returns the (new or adopted) ``BatchId``."""
    if businessDate is None:
        businessDate = _utcnow().date()
    elif isinstance(businessDate, str):
        businessDate = _dt.date.fromisoformat(businessDate[:10])
    if environmentCode is None:
        environmentCode = getConfigurationValue(spark, catalog, "EnvironmentCode", "ALL")
    environmentCode = environmentCode or "DEV"

    existing = findRunningBatch(spark, catalog, batchName, businessDate)
    if existing is not None:
        if allowAdoptRunning:
            _sql(spark,
                 f"UPDATE {_t(catalog, 'batch')} SET Notes = concat(coalesce(concat(Notes, ' | '), ''), "
                 "'Adopted by recovery rerun at ', date_format(current_timestamp(), \"yyyy-MM-dd'T'HH:mm:ss.SSS\")) "
                 "WHERE BatchId = :id", id=int(existing))
            return int(existing)
        raise ControlError(
            51001,
            f"Batch {batchName} for business date {businessDate.isoformat()} is already running (BatchId {existing}). "
            "Complete or cancel it, or rerun with allowAdoptRunning=True.")

    _insert(spark, catalog, "batch", [{
        "BatchName": batchName, "BatchType": batchType, "BusinessDate": businessDate,
        "EnvironmentCode": environmentCode, "Status": "Running", "Notes": notes,
    }])
    batchId = findRunningBatch(spark, catalog, batchName, businessDate)
    if batchId is None:
        raise ControlError(51001, f"Batch {batchName} was inserted but could not be read back.")
    return int(batchId)


def getBatch(spark: Any, catalog: str, batchId: int) -> Optional[Dict[str, Any]]:
    rows = _rows(spark, f"SELECT * FROM {_t(catalog, 'batch')} WHERE BatchId = :id", id=int(batchId))
    return rows[0] if rows else None


def endBatch(spark: Any, catalog: str, batchId: int, forceStatus: Optional[str] = None) -> Dict[str, Any]:
    """``etl.usp_EndBatch``. Returns ``{"batchStatus", "failedPackageCount", "warningCount"}``.

    Status precedence: forceStatus > any Failed package > any still-Running package (-> Failed)
    > any Warning in error_log (-> SucceededWithWarnings) > Succeeded. Running steps are failed.
    """
    batchId = int(batchId)
    if getBatch(spark, catalog, batchId) is None:
        raise ControlError(51002, "Unknown BatchId supplied to etl.usp_EndBatch.")
    counts = _rows(spark,
                   "SELECT coalesce(sum(CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END), 0) AS Failed, "
                   "coalesce(sum(CASE WHEN Status = 'Running' THEN 1 ELSE 0 END), 0) AS Running "
                   f"FROM {_t(catalog, 'package_execution')} WHERE BatchId = :id", id=batchId)[0]
    failedPackages, runningPackages = int(counts["Failed"]), int(counts["Running"])
    warningCount = int(_scalar(spark,
                               f"SELECT count(*) FROM {_t(catalog, 'error_log')} "
                               "WHERE BatchId = :id AND ErrorSeverity = 'Warning'", id=batchId) or 0)
    if forceStatus is not None:
        derived = forceStatus
    elif failedPackages > 0 or runningPackages > 0:
        derived = "Failed"
    elif warningCount > 0:
        derived = "SucceededWithWarnings"
    else:
        derived = "Succeeded"
    noteSql = ("concat(coalesce(concat(Notes, ' | '), ''), :running, "
               "' package execution(s) were still marked Running when the batch closed.')"
               if runningPackages > 0 else "Notes")
    _sql(spark,
         f"UPDATE {_t(catalog, 'batch')} SET Status = :status, CompletedAtUtc = current_timestamp(), "
         f"Notes = {noteSql} WHERE BatchId = :id",
         status=derived, id=batchId, **({"running": str(runningPackages)} if runningPackages > 0 else {}))
    _sql(spark,
         f"UPDATE {_t(catalog, 'batch_step')} SET Status = 'Failed', CompletedAtUtc = current_timestamp() "
         "WHERE BatchId = :id AND Status = 'Running'", id=batchId)
    return {"batchStatus": derived, "failedPackageCount": failedPackages, "warningCount": warningCount}


# ----------------------------------------------------------------------------- batch steps

def startBatchStep(spark: Any, catalog: str, batchId: int, stepName: str, stepSequence: int,
                   stepGroup: Optional[str] = None) -> int:
    """``etl.usp_StartBatchStep``. Returns ``BatchStepId``; ``AttemptNumber`` = prior attempts + 1."""
    batchId = int(batchId)
    attempt = int(_scalar(spark,
                          f"SELECT coalesce(max(AttemptNumber), 0) + 1 FROM {_t(catalog, 'batch_step')} "
                          "WHERE BatchId = :id AND StepName = :step", id=batchId, step=stepName))
    _insert(spark, catalog, "batch_step", [{
        "BatchId": batchId, "StepName": stepName, "StepSequence": int(stepSequence), "StepGroup": stepGroup,
        "Status": "Running", "AttemptNumber": attempt,
    }])
    return int(_scalar(spark,
                       f"SELECT max(BatchStepId) FROM {_t(catalog, 'batch_step')} "
                       "WHERE BatchId = :id AND StepName = :step AND AttemptNumber = :att",
                       id=batchId, step=stepName, att=attempt))


def endBatchStep(spark: Any, catalog: str, batchStepId: int, status: str = "Succeeded") -> str:
    """``etl.usp_EndBatchStep``: a ``Succeeded`` step with Failed/Running packages is downgraded to ``Failed``."""
    batchStepId = int(batchStepId)
    if status == "Succeeded":
        bad = _scalar(spark,
                      f"SELECT count(*) FROM {_t(catalog, 'package_execution')} "
                      "WHERE BatchStepId = :id AND Status IN ('Failed', 'Running')", id=batchStepId)
        if int(bad or 0) > 0:
            status = "Failed"
    _sql(spark,
         f"UPDATE {_t(catalog, 'batch_step')} SET Status = :status, CompletedAtUtc = current_timestamp() "
         "WHERE BatchStepId = :id", status=status, id=batchStepId)
    return status


def markBatchStepSkipped(spark: Any, catalog: str, batchId: int, stepName: str, stepSequence: int,
                         stepGroup: Optional[str] = None) -> int:
    """Record a step the orchestrator skipped (``RestartFromStep`` recovery). Returns ``BatchStepId``."""
    stepId = startBatchStep(spark, catalog, batchId, stepName, stepSequence, stepGroup)
    endBatchStep(spark, catalog, stepId, "Skipped")
    return stepId


def getLatestBatchStep(spark: Any, catalog: str, batchId: int, stepName: str) -> Optional[Dict[str, Any]]:
    rows = _rows(spark,
                 f"SELECT * FROM {_t(catalog, 'batch_step')} WHERE BatchId = :id AND StepName = :step "
                 "ORDER BY BatchStepId DESC LIMIT 1", id=int(batchId), step=stepName)
    return rows[0] if rows else None


# ----------------------------------------------------------------------------- package executions

def logPackageStart(spark: Any, catalog: str, batchId: Optional[int], packageName: str,
                    projectName: Optional[str] = None, stepName: Optional[str] = None) -> int:
    """``etl.usp_LogPackageStart``. ``batchId`` 0 is treated as NULL (ad-hoc run). Returns ``PackageExecutionId``."""
    batchId = _nullIfZero(batchId)
    batchStepId = None
    if stepName is not None and batchId is not None:
        batchStepId = _scalar(spark,
                              f"SELECT max(BatchStepId) FROM {_t(catalog, 'batch_step')} "
                              "WHERE BatchId = :id AND StepName = :step", id=batchId, step=stepName)
    batchFilter = "BatchId IS NULL" if batchId is None else "BatchId = :bid"
    args = {"pkg": packageName} if batchId is None else {"pkg": packageName, "bid": batchId}
    attempt = int(_scalar(spark,
                          f"SELECT coalesce(max(AttemptNumber), 0) + 1 FROM {_t(catalog, 'package_execution')} "
                          f"WHERE PackageName = :pkg AND {'true' if batchId is None else batchFilter}", **args))
    _insert(spark, catalog, "package_execution", [{
        "BatchId": batchId, "BatchStepId": None if batchStepId is None else int(batchStepId),
        "PackageName": packageName, "ProjectName": projectName, "Status": "Running", "AttemptNumber": attempt,
        "RowsRead": 0, "RowsInserted": 0, "RowsUpdated": 0, "RowsRejected": 0,
    }])
    return int(_scalar(spark,
                       f"SELECT max(PackageExecutionId) FROM {_t(catalog, 'package_execution')} "
                       f"WHERE PackageName = :pkg AND AttemptNumber = :att AND {batchFilter}",
                       att=attempt, **args))


def logPackageEnd(spark: Any, catalog: str, packageExecutionId: int, status: str = "Succeeded",
                  rowsRead: Optional[int] = None, rowsInserted: Optional[int] = None,
                  rowsUpdated: Optional[int] = None, rowsDeleted: Optional[int] = None,
                  rowsRejected: Optional[int] = None, watermarkFrom: Optional[str] = None,
                  watermarkTo: Optional[str] = None) -> str:
    """``etl.usp_LogPackageEnd``: fails the execution when the reject share exceeds ``MaxRejectPercent``.

    Returns the status actually written.
    """
    packageExecutionId = int(packageExecutionId)
    maxRejectPercent = _parseDecimal(getConfigurationValue(spark, catalog, "MaxRejectPercent", "ALL"))
    if maxRejectPercent is None:
        maxRejectPercent = _decimal.Decimal("5.0")
    read = int(rowsRead or 0)
    rejectPercent = _decimal.Decimal(0) if read == 0 else (_decimal.Decimal(int(rowsRejected or 0)) * 100) / read
    if status == "Succeeded" and rejectPercent > maxRejectPercent:
        status = "Failed"
        pe = _rows(spark, f"SELECT BatchId, PackageName FROM {_t(catalog, 'package_execution')} "
                          "WHERE PackageExecutionId = :id", id=packageExecutionId)
        if pe:
            _insert(spark, catalog, "error_log", [{
                "PackageExecutionId": packageExecutionId, "BatchId": pe[0]["BatchId"], "ErrorSeverity": "Critical",
                "SourceName": pe[0]["PackageName"], "ProcedureName": "etl.usp_LogPackageEnd",
                "ErrorDescription": f"Reject tolerance breached: {rejectPercent.quantize(_decimal.Decimal('0.0001'))}% "
                                    f"of {read} rows rejected, tolerance is {maxRejectPercent}%.",
            }])
    _sql(spark,
         f"UPDATE {_t(catalog, 'package_execution')} SET Status = :status, CompletedAtUtc = current_timestamp(), "
         "RowsRead = coalesce(:rr, RowsRead), RowsInserted = coalesce(:ri, RowsInserted), "
         "RowsUpdated = coalesce(:ru, RowsUpdated), RowsDeleted = coalesce(:rd, RowsDeleted), "
         "RowsRejected = coalesce(:rj, RowsRejected), WatermarkFrom = coalesce(:wf, WatermarkFrom), "
         "WatermarkTo = coalesce(:wt, WatermarkTo) WHERE PackageExecutionId = :id",
         status=status, rr=_optInt(rowsRead), ri=_optInt(rowsInserted), ru=_optInt(rowsUpdated),
         rd=_optInt(rowsDeleted), rj=_optInt(rowsRejected), wf=watermarkFrom, wt=watermarkTo, id=packageExecutionId)
    return status


def _optInt(v: Optional[int]) -> Optional[int]:
    return None if v is None else int(v)


def getPackageExecution(spark: Any, catalog: str, packageExecutionId: int) -> Optional[Dict[str, Any]]:
    rows = _rows(spark, f"SELECT * FROM {_t(catalog, 'package_execution')} WHERE PackageExecutionId = :id",
                 id=int(packageExecutionId))
    return rows[0] if rows else None


def getPackageExecutions(spark: Any, catalog: str, batchId: int, status: Optional[str] = None,
                         packageNames: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
    """Latest attempt of every package in the batch (optionally filtered). Used by the retry driver."""
    where, args = ["pe.BatchId = :id"], {"id": int(batchId)}
    if status is not None:
        where.append("pe.Status = :status")
        args["status"] = status
    if packageNames:
        where.append("pe.PackageName IN (" + ", ".join(f":p{i}" for i in range(len(packageNames))) + ")")
        args.update({f"p{i}": n for i, n in enumerate(packageNames)})
    return _rows(spark,
                 f"SELECT pe.* FROM {_t(catalog, 'package_execution')} AS pe "
                 f"WHERE pe.PackageExecutionId IN (SELECT max(PackageExecutionId) FROM {_t(catalog, 'package_execution')} "
                 f"WHERE BatchId = :id GROUP BY PackageName) AND {' AND '.join(where)} ORDER BY pe.PackageName", **args)


def getFailedPackages(spark: Any, catalog: str, batchId: int) -> List[str]:
    """Package names whose *latest* attempt in the batch is Failed (the retry driver's work list)."""
    return [r["PackageName"] for r in getPackageExecutions(spark, catalog, batchId, status="Failed")]


class PackageRun:
    """Context returned by :func:`packageRun`; set the counters before the block exits."""

    def __init__(self, spark: Any, catalog: str, batchId: Optional[int], packageName: str,
                 projectName: Optional[str], stepName: Optional[str]):
        self.spark, self.catalog = spark, catalog
        self.batchId, self.packageName, self.projectName, self.stepName = batchId, packageName, projectName, stepName
        self.packageExecutionId: Optional[int] = None
        self.rowsRead: Optional[int] = None
        self.rowsInserted: Optional[int] = None
        self.rowsUpdated: Optional[int] = None
        self.rowsDeleted: Optional[int] = None
        self.rowsRejected: Optional[int] = None
        self.watermarkFrom: Optional[str] = None
        self.watermarkTo: Optional[str] = None
        self.status: Optional[str] = None

    def __enter__(self) -> "PackageRun":
        self.packageExecutionId = logPackageStart(self.spark, self.catalog, self.batchId, self.packageName,
                                                  self.projectName, self.stepName)
        return self

    def _end(self, status: str) -> None:
        self.status = logPackageEnd(self.spark, self.catalog, self.packageExecutionId, status,
                                    rowsRead=self.rowsRead, rowsInserted=self.rowsInserted,
                                    rowsUpdated=self.rowsUpdated, rowsDeleted=self.rowsDeleted,
                                    rowsRejected=self.rowsRejected, watermarkFrom=self.watermarkFrom,
                                    watermarkTo=self.watermarkTo)

    def __exit__(self, excType, exc, tb) -> bool:
        if exc is None:
            self._end("Succeeded")
            return False
        try:
            logError(self.spark, self.catalog, packageExecutionId=self.packageExecutionId,
                     batchId=_nullIfZero(self.batchId), errorSeverity="Error", sourceName=self.packageName,
                     sourceComponent=type(exc).__name__, procedureName=None,
                     errorDescription=str(exc)[:4000])
        finally:
            self._end("Failed")
        return False  # re-raise


def packageRun(spark: Any, catalog: str, batchId: Optional[int], packageName: str,
               projectName: Optional[str] = None, stepName: Optional[str] = None) -> PackageRun:
    """``with control.packageRun(spark, catalog, batchId, "EXT_ORA_CustomerMaster", ...) as run:``

    Logs the package start; on normal exit logs ``Succeeded`` with the counters set on ``run``;
    on exception logs an ``Error`` row plus ``Failed`` and re-raises.
    """
    return PackageRun(spark, catalog, batchId, packageName, projectName, stepName)


# ----------------------------------------------------------------------------- logging

def logError(spark: Any, catalog: str, packageExecutionId: Optional[int] = None, batchId: Optional[int] = None,
             errorSeverity: str = "Error", errorCode: Optional[int] = None, sourceName: Optional[str] = None,
             sourceComponent: Optional[str] = None, procedureName: Optional[str] = None,
             errorDescription: Optional[str] = None) -> None:
    """``etl.usp_LogError``: 0 ids -> NULL, BatchId inferred from the package execution; never raises."""
    batchId, packageExecutionId = _nullIfZero(batchId), _nullIfZero(packageExecutionId)
    try:
        if batchId is None and packageExecutionId is not None:
            batchId = _scalar(spark, f"SELECT BatchId FROM {_t(catalog, 'package_execution')} "
                                     "WHERE PackageExecutionId = :id", id=packageExecutionId)
        _insert(spark, catalog, "error_log", [{
            "PackageExecutionId": packageExecutionId, "BatchId": None if batchId is None else int(batchId),
            "ErrorSeverity": errorSeverity, "ErrorCode": errorCode, "SourceName": sourceName,
            "SourceComponent": sourceComponent, "ProcedureName": procedureName, "ErrorDescription": errorDescription,
        }])
    except Exception:  # noqa: BLE001 - the legacy procedure swallows logging failures
        return


def logRowCount(spark: Any, catalog: str, packageExecutionId: int, objectName: str,
                sourceRowCount: Optional[int] = None, targetRowCount: Optional[int] = None,
                insertRowCount: Optional[int] = None, updateRowCount: Optional[int] = None,
                deleteRowCount: Optional[int] = None, rejectRowCount: Optional[int] = None) -> None:
    """``etl.usp_LogRowCount``: appends to ``etl.row_count_audit`` and accumulates the package counters."""
    packageExecutionId = int(packageExecutionId)
    _insert(spark, catalog, "row_count_audit", [{
        "PackageExecutionId": packageExecutionId, "ObjectName": objectName,
        "SourceRowCount": _optInt(sourceRowCount), "TargetRowCount": _optInt(targetRowCount),
        "InsertRowCount": _optInt(insertRowCount), "UpdateRowCount": _optInt(updateRowCount),
        "DeleteRowCount": _optInt(deleteRowCount), "RejectRowCount": _optInt(rejectRowCount),
    }])
    _sql(spark,
         f"UPDATE {_t(catalog, 'package_execution')} SET "
         "RowsRead = coalesce(RowsRead, 0) + :src, RowsInserted = coalesce(RowsInserted, 0) + :ins, "
         "RowsUpdated = coalesce(RowsUpdated, 0) + :upd, RowsDeleted = coalesce(RowsDeleted, 0) + :dele, "
         "RowsRejected = coalesce(RowsRejected, 0) + :rej WHERE PackageExecutionId = :id",
         src=int(sourceRowCount or 0), ins=int(insertRowCount or 0), upd=int(updateRowCount or 0),
         dele=int(deleteRowCount or 0), rej=int(rejectRowCount or 0), id=packageExecutionId)


def logRejectedRecord(spark: Any, catalog: str, objectName: str, rejectReasonCode: str,
                      packageExecutionId: Optional[int] = None, batchId: Optional[int] = None,
                      sourceSystemCode: Optional[str] = None, businessKey: Optional[str] = None,
                      rejectReason: Optional[str] = None, rejectStage: str = "Stage",
                      recordPayload: Optional[str] = None, ssisErrorCode: Optional[int] = None,
                      ssisErrorColumn: Optional[int] = None) -> None:
    """``etl.usp_LogRejectedRecord`` / ``usp_LogReject``: one reject row + ``RowsRejected`` += 1."""
    batchId, packageExecutionId = _nullIfZero(batchId), _nullIfZero(packageExecutionId)
    _insert(spark, catalog, "rejected_record", [{
        "PackageExecutionId": packageExecutionId, "BatchId": batchId, "SourceSystemCode": sourceSystemCode,
        "ObjectName": objectName, "BusinessKey": businessKey, "RejectReasonCode": rejectReasonCode or "UNSPECIFIED",
        "RejectReason": rejectReason, "RejectStage": rejectStage, "SsisErrorCode": ssisErrorCode,
        "SsisErrorColumn": ssisErrorColumn, "RecordPayload": recordPayload,
    }])
    if packageExecutionId is not None:
        _sql(spark, f"UPDATE {_t(catalog, 'package_execution')} SET RowsRejected = coalesce(RowsRejected, 0) + 1 "
                    "WHERE PackageExecutionId = :id", id=packageExecutionId)


def logReject(spark: Any, catalog: str, objectName: str, businessKey: Optional[str] = None,
              rejectReasonCode: str = "UNSPECIFIED", rejectReason: Optional[str] = None,
              batchId: Optional[int] = None, packageExecutionId: Optional[int] = None,
              rejectStage: str = "Stage") -> None:
    """``etl.usp_LogReject`` (thin wrapper kept for parity with the SSIS Execute SQL tasks)."""
    logRejectedRecord(spark, catalog, objectName, rejectReasonCode, packageExecutionId=packageExecutionId,
                      batchId=batchId, businessKey=businessKey, rejectReason=rejectReason, rejectStage=rejectStage)


REJECT_COLUMNS = ("PackageExecutionId", "BatchId", "SourceSystemCode", "ObjectName", "BusinessKey",
                  "RejectReasonCode", "RejectReason", "RejectStage", "SsisErrorCode", "SsisErrorColumn", "RecordPayload")


def _shapeRejects(rejectedDf: Any, objectName: str, batchId: Optional[int], packageExecutionId: Optional[int],
                  sourceSystemCode: Optional[str], rejectStage: str, rejectReasonCode: Optional[str],
                  businessKeyColumn: Optional[str]) -> Any:
    """Project an arbitrary DataFrame onto the ``etl.rejected_record`` columns (``usp_LogRejectedRecordSet``).

    Columns named ``BusinessKey`` (or ``businessKeyColumn``), ``RejectReasonCode``, ``RejectReason``,
    ``RecordPayload``, ``SsisErrorCode``, ``SsisErrorColumn`` are picked up when present; the legacy
    procedure left ``RecordPayload`` NULL when the source had no such column - here the offending row
    is serialised as JSON instead so rejects can be replayed.
    """
    from pyspark.sql import functions as F

    cols = {c.lower(): c for c in rejectedDf.columns}

    def col(name: str) -> Optional[Any]:
        return F.col(f"`{cols[name.lower()]}`") if name.lower() in cols else None

    keyCol = F.col(f"`{businessKeyColumn}`") if businessKeyColumn else col("BusinessKey")
    payload = col("RecordPayload")
    if payload is None:
        payload = F.to_json(F.struct(*[F.col(f"`{c}`") for c in rejectedDf.columns]))
    reason = col("RejectReasonCode")
    reasonExpr = F.coalesce(reason.cast("string"), F.lit(rejectReasonCode), F.lit("UNSPECIFIED")) \
        if reason is not None else F.lit(rejectReasonCode or "UNSPECIFIED")
    return rejectedDf.select(
        F.lit(packageExecutionId).cast("bigint").alias("PackageExecutionId"),
        F.lit(batchId).cast("bigint").alias("BatchId"),
        F.lit(sourceSystemCode).cast("string").alias("SourceSystemCode"),
        F.lit(objectName).alias("ObjectName"),
        (keyCol.cast("string") if keyCol is not None else F.lit(None).cast("string")).alias("BusinessKey"),
        reasonExpr.alias("RejectReasonCode"),
        (col("RejectReason").cast("string") if col("RejectReason") is not None else F.lit(None).cast("string")).alias("RejectReason"),
        F.lit(rejectStage).cast("string").alias("RejectStage"),
        (col("SsisErrorCode").cast("int") if col("SsisErrorCode") is not None else F.lit(None).cast("int")).alias("SsisErrorCode"),
        (col("SsisErrorColumn").cast("int") if col("SsisErrorColumn") is not None else F.lit(None).cast("int")).alias("SsisErrorColumn"),
        payload.cast("string").alias("RecordPayload"),
    )


def stageRejectedRecords(spark: Any, catalog: str, loadTag: str, rejectedDf: Any, objectName: str,
                         batchId: Optional[int] = None, packageExecutionId: Optional[int] = None,
                         sourceSystemCode: Optional[str] = None, rejectStage: str = "Stage",
                         rejectReasonCode: Optional[str] = None, businessKeyColumn: Optional[str] = None) -> int:
    """Land rejects in ``etl.rejected_record_staging`` under ``loadTag`` (the legacy staging path)."""
    from pyspark.sql import functions as F

    shaped = _shapeRejects(rejectedDf, objectName, _nullIfZero(batchId), _nullIfZero(packageExecutionId),
                           sourceSystemCode, rejectStage, rejectReasonCode, businessKeyColumn)
    shaped = shaped.withColumn("LoadTag", F.lit(loadTag)).withColumn("LandedAtUtc", F.current_timestamp())
    shaped = shaped.select("LoadTag", *REJECT_COLUMNS, "LandedAtUtc")
    count = shaped.count()
    _withRetry(lambda: shaped.write.format("delta").mode("append").saveAsTable(_t(catalog, "rejected_record_staging")))
    return int(count)


def logRejectedRecordSet(spark: Any, catalog: str, objectName: str, rejectedDf: Any = None,
                         batchId: Optional[int] = None, packageExecutionId: Optional[int] = None,
                         sourceSystemCode: Optional[str] = None, rejectStage: str = "Stage",
                         rejectReasonCode: Optional[str] = None, businessKeyColumn: Optional[str] = None,
                         loadTag: Optional[str] = None, purgeStaging: bool = True) -> int:
    """``etl.usp_LogRejectedRecordSet``. Returns the number of rejected rows logged.

    * ``rejectedDf`` given  -> the ``@SourceTable`` path: the DataFrame is the (already filtered)
      set of offending rows; see :func:`_shapeRejects` for column detection.
    * ``loadTag`` given     -> the ``@LoadTag`` path: rows previously landed with
      :func:`stageRejectedRecords` are copied into ``etl.rejected_record`` (staging values win over
      the arguments, ``RejectReasonCode`` falls back to ``UNSPECIFIED``) and, when ``purgeStaging``,
      deleted from the staging table.
    ``BatchId`` is inferred from ``packageExecutionId`` when not supplied.
    """
    batchId, packageExecutionId = _nullIfZero(batchId), _nullIfZero(packageExecutionId)
    if rejectedDf is None and loadTag is None:
        raise ControlError(50000, "etl.usp_LogRejectedRecordSet needs either rejectedDf or loadTag.")
    if batchId is None and packageExecutionId is not None:
        batchId = _scalar(spark, f"SELECT BatchId FROM {_t(catalog, 'package_execution')} "
                                 "WHERE PackageExecutionId = :id", id=packageExecutionId)
        batchId = None if batchId is None else int(batchId)

    target = _t(catalog, "rejected_record")
    if loadTag is not None:
        staging = _t(catalog, "rejected_record_staging")
        count = int(_scalar(spark, f"SELECT count(*) FROM {staging} WHERE LoadTag = :tag", tag=loadTag) or 0)
        if count:
            _sql(spark,
                 f"INSERT INTO {target} (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey, "
                 "RejectReasonCode, RejectReason, RejectStage, SsisErrorCode, SsisErrorColumn, RecordPayload) "
                 "SELECT coalesce(s.PackageExecutionId, :pe), coalesce(s.BatchId, :bid), coalesce(s.SourceSystemCode, :ssc), "
                 "coalesce(s.ObjectName, :obj), s.BusinessKey, coalesce(s.RejectReasonCode, :rrc, 'UNSPECIFIED'), "
                 "s.RejectReason, coalesce(s.RejectStage, :stage), s.SsisErrorCode, s.SsisErrorColumn, s.RecordPayload "
                 f"FROM {staging} AS s WHERE s.LoadTag = :tag",
                 pe=packageExecutionId, bid=batchId, ssc=sourceSystemCode, obj=objectName, rrc=rejectReasonCode,
                 stage=rejectStage, tag=loadTag)
        if purgeStaging:
            _sql(spark, f"DELETE FROM {staging} WHERE LoadTag = :tag", tag=loadTag)
        return count

    shaped = _shapeRejects(rejectedDf, objectName, batchId, packageExecutionId, sourceSystemCode, rejectStage,
                           rejectReasonCode, businessKeyColumn).select(*REJECT_COLUMNS).cache()
    try:
        count = int(shaped.count())
        if count:
            _withRetry(lambda: shaped.write.format("delta").mode("append").saveAsTable(target))
    finally:
        shaped.unpersist()
    return count


# ----------------------------------------------------------------------------- watermarks

def getWatermarkRow(spark: Any, catalog: str, sourceSystemCode: str, objectName: str) -> Optional[Dict[str, Any]]:
    rows = _rows(spark, f"SELECT * FROM {_t(catalog, 'watermark')} WHERE SourceSystemCode = :ssc AND ObjectName = :obj",
                 ssc=sourceSystemCode, obj=objectName)
    return rows[0] if rows else None


def getWatermark(spark: Any, catalog: str, sourceSystemCode: str, objectName: str,
                 reloadFullHistory: bool = False) -> Tuple[str, Optional[str]]:
    """``etl.usp_GetWatermark`` -> ``(watermarkFrom, watermarkTo)`` as strings.

    * unknown object -> a ``Timestamp`` watermark at ``WatermarkEpoch`` with 60 min lookback is created
    * ``IsLocked``    -> ``ControlError(51010)``
    * full reload     -> ``0`` (NumericKey) or the epoch
    * NumericKey      -> ``(lastValue, None)``; DateWindow -> ISO dates ``(lastValue::date, today)``;
      Timestamp -> ``(lastValue - LookbackMinutes, now UTC)`` in ``yyyy-MM-ddTHH:mm:ss.fff``.
    """
    epoch = getConfigurationValue(spark, catalog, "WatermarkEpoch", "ALL") or DEFAULT_EPOCH
    row = getWatermarkRow(spark, catalog, sourceSystemCode, objectName)
    if row is None:
        _insert(spark, catalog, "watermark", [{
            "SourceSystemCode": sourceSystemCode, "ObjectName": objectName, "WatermarkType": "Timestamp",
            "LastValue": epoch, "LookbackMinutes": 60, "IsLocked": False,
        }])
        row = {"WatermarkType": "Timestamp", "LastValue": epoch, "LookbackMinutes": 60, "IsLocked": False}
    if row.get("IsLocked"):
        raise ControlError(51010, f"Watermark for {sourceSystemCode}.{objectName} is locked. A previous load left it "
                                  "in an indeterminate state and it must be reviewed before the extract can run again.")
    wmType, lastValue, lookback = row["WatermarkType"], row.get("LastValue"), row.get("LookbackMinutes")
    if reloadFullHistory:
        lastValue = "0" if wmType == "NumericKey" else epoch
    epochTs = _parseTimestamp(epoch) or _parseTimestamp(DEFAULT_EPOCH)
    if wmType == "NumericKey":
        return (lastValue if lastValue is not None else "0"), None
    if wmType == "DateWindow":
        base = _parseTimestamp(lastValue) or epochTs
        return base.date().isoformat(), _utcnow().date().isoformat()
    base = _parseTimestamp(lastValue) or epochTs
    frm = base - _dt.timedelta(minutes=int(lookback or 0))
    return _iso(frm), _iso(_utcnow())


def setWatermark(spark: Any, catalog: str, sourceSystemCode: str, objectName: str, watermarkTo: Optional[str],
                 packageExecutionId: Optional[int] = None, allowRewind: bool = False) -> bool:
    """``etl.usp_SetWatermark``. Returns True when the watermark was advanced / created.

    A backwards move is refused (Warning row in ``etl.error_log``) unless ``allowRewind``. Values that
    do not parse as BIGINT / timestamp compare as NULL -> refused, exactly like ``TRY_CONVERT``.
    """
    if watermarkTo is None or len(str(watermarkTo)) == 0:
        return False
    watermarkTo = str(watermarkTo)
    packageExecutionId = _nullIfZero(packageExecutionId)
    row = getWatermarkRow(spark, catalog, sourceSystemCode, objectName)
    if row is None:
        _insert(spark, catalog, "watermark", [{
            "SourceSystemCode": sourceSystemCode, "ObjectName": objectName, "WatermarkType": "Timestamp",
            "LastValue": watermarkTo, "LastPackageExecutionId": packageExecutionId,
        }])
        return True
    lastValue, wmType = row.get("LastValue"), row["WatermarkType"]
    if lastValue is None:
        movesForward = True
    elif wmType == "NumericKey":
        new, old = _parseInt(watermarkTo), _parseInt(lastValue)
        movesForward = new is not None and old is not None and new >= old
    else:
        new, old = _parseTimestamp(watermarkTo), _parseTimestamp(lastValue)
        movesForward = new is not None and old is not None and new >= old
    if not movesForward and not allowRewind:
        _insert(spark, catalog, "error_log", [{
            "PackageExecutionId": packageExecutionId, "ErrorSeverity": "Warning", "ProcedureName": "etl.usp_SetWatermark",
            "ErrorDescription": f"Refused to rewind watermark for {sourceSystemCode}.{objectName} from {lastValue} to {watermarkTo}.",
        }])
        return False
    _sql(spark,
         f"UPDATE {_t(catalog, 'watermark')} SET PreviousValue = LastValue, LastValue = :new, "
         "LastLoadedAtUtc = current_timestamp(), LastPackageExecutionId = coalesce(:pe, LastPackageExecutionId) "
         "WHERE SourceSystemCode = :ssc AND ObjectName = :obj",
         new=watermarkTo, pe=packageExecutionId, ssc=sourceSystemCode, obj=objectName)
    return True


# ----------------------------------------------------------------------------- reconciliation

def _reconTolerances(spark: Any, catalog: str, absoluteTolerance: Optional[int],
                     percentTolerance: Optional[Any]) -> Tuple[int, _decimal.Decimal]:
    if absoluteTolerance is None:
        absoluteTolerance = _parseInt(getConfigurationValue(spark, catalog, "ReconAbsoluteTolerance", "ALL"))
    if percentTolerance is None:
        percentTolerance = _parseDecimal(getConfigurationValue(spark, catalog, "ReconPercentTolerance", "ALL"))
    return int(absoluteTolerance or 0), _decimal.Decimal(str(percentTolerance or 0))


def _balance(spark: Any, catalog: str, batchId: int, scope: Optional[str] = None,
             objectName: Optional[str] = None) -> List[Dict[str, Any]]:
    """Per-object source/target/reject sums for the batch with variance + exemption flag."""
    where, args = ["pe.BatchId = :id"], {"id": int(batchId)}
    if objectName is not None:
        where.append("a.ObjectName = :obj")
        args["obj"] = objectName
    elif scope is not None and scope != "ALL":
        where.append("(upper(coalesce(pe.PackageName, '')) LIKE :scope OR upper(coalesce(pe.ProjectName, '')) LIKE :scope "
                     "OR upper(a.ObjectName) LIKE :scope)")
        args["scope"] = f"%{scope.upper()}%"
    return _rows(spark, f"""
        SELECT a.ObjectName,
               sum(coalesce(a.SourceRowCount, 0)) AS SourceRows,
               sum(coalesce(a.TargetRowCount, 0)) AS TargetRows,
               sum(coalesce(a.RejectRowCount, 0)) AS RejectedRows,
               sum(coalesce(a.SourceRowCount, 0)) - sum(coalesce(a.TargetRowCount, 0)) - sum(coalesce(a.RejectRowCount, 0)) AS Variance,
               CASE WHEN sum(coalesce(a.SourceRowCount, 0)) = 0 THEN CAST(0 AS DECIMAL(18,6))
                    ELSE abs(CAST(sum(coalesce(a.SourceRowCount, 0)) - sum(coalesce(a.TargetRowCount, 0))
                                  - sum(coalesce(a.RejectRowCount, 0)) AS DECIMAL(18,6))) * 100.0
                         / sum(coalesce(a.SourceRowCount, 0)) END AS VariancePercent,
               CASE WHEN max(rx.ObjectName) IS NOT NULL THEN true ELSE false END AS IsExempt
        FROM {_t(catalog, 'row_count_audit')} AS a
        INNER JOIN {_t(catalog, 'package_execution')} AS pe ON pe.PackageExecutionId = a.PackageExecutionId
        LEFT JOIN {_t(catalog, 'reconciliation_exemption')} AS rx ON rx.ObjectName = a.ObjectName
        WHERE {' AND '.join(where)}
        GROUP BY a.ObjectName
        ORDER BY abs(Variance) DESC, a.ObjectName""", **args)


def _breached(r: Dict[str, Any], absTol: int, pctTol: _decimal.Decimal) -> bool:
    return abs(int(r["Variance"] or 0)) > absTol and _decimal.Decimal(str(r["VariancePercent"] or 0)) > pctTol


def assertRowCountReconciliation(spark: Any, catalog: str, batchId: int, raiseOnFailure: bool = True) -> int:
    """``etl.usp_AssertRowCountReconciliation``. Returns the failed object count.

    ``Variance = Source - Target - Rejected``; a non-exempt object fails when BOTH ``abs(Variance) >
    ReconAbsoluteTolerance`` AND ``VariancePercent > ReconPercentTolerance``. One ``Error`` row per
    failing object; ``ControlError(51030)`` when ``raiseOnFailure`` and anything failed.
    """
    batchId = int(batchId)
    absTol, pctTol = _reconTolerances(spark, catalog, None, None)
    rows = [r for r in _balance(spark, catalog, batchId) if not r["IsExempt"]]
    failed = [r for r in rows if _breached(r, absTol, pctTol)]
    if failed:
        _insert(spark, catalog, "error_log", [{
            "BatchId": batchId, "ErrorSeverity": "Error", "ProcedureName": "etl.usp_AssertRowCountReconciliation",
            "SourceName": r["ObjectName"],
            "ErrorDescription": f"Row count reconciliation variance of {r['Variance']} row(s) "
                                f"({_decimal.Decimal(str(r['VariancePercent'])).quantize(_decimal.Decimal('0.0001'))}%): "
                                f"source {r['SourceRows']}, target {r['TargetRows']}, rejected {r['RejectedRows']}.",
        } for r in failed])
    if failed and raiseOnFailure:
        raise ControlError(51030, f"Row count reconciliation failed for {len(failed)} object(s) in batch {batchId}. "
                                  "See etl.error_log for detail.")
    return len(failed)


def assertRowCountTolerance(spark: Any, catalog: str, batchId: int, scope: str = "ALL",
                            objectName: Optional[str] = None, absoluteTolerance: Optional[int] = None,
                            percentTolerance: Optional[Any] = None, raiseOnFailure: bool = True) -> int:
    """``etl.usp_AssertRowCountTolerance``. Returns the failed object count.

    ``scope='ALL'`` without ``objectName`` delegates to :func:`assertRowCountReconciliation`. Otherwise
    ``scope`` is a case-insensitive substring match on package / project / object name. An empty
    scope logs a Warning and (when ``raiseOnFailure``) raises; breaches log one Critical row.
    """
    if batchId is None:
        raise ControlError(50000, "etl.usp_AssertRowCountTolerance requires a batch id.")
    batchId = int(batchId)
    scope = (scope or "ALL").strip() or None
    if objectName is None and (scope is None or scope == "ALL"):
        return assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure)
    absTol, pctTol = _reconTolerances(spark, catalog, absoluteTolerance, percentTolerance)
    rows = _balance(spark, catalog, batchId, scope, objectName)
    scopeText = objectName or scope or "ALL"
    if not rows:
        msg = f"No row count audit rows for batch {batchId} in scope {scopeText}."
        logError(spark, catalog, batchId=batchId, errorSeverity="Warning",
                 sourceName="etl.usp_AssertRowCountTolerance", errorDescription=msg)
        if raiseOnFailure:
            raise ControlError(50000, msg)
        return 0
    failed = [r for r in rows if not r["IsExempt"] and _breached(r, absTol, pctTol)]
    if failed:
        detail = "".join(f"{r['ObjectName']} src={r['SourceRows']} tgt={r['TargetRows']} rej={r['RejectedRows']} "
                         f"var={r['Variance']}\n" for r in failed)
        msg = f"Row count tolerance breached for scope {scopeText} in batch {batchId} ({len(failed)} object(s)):\n{detail}"
        logError(spark, catalog, batchId=batchId, errorSeverity="Critical",
                 sourceName="etl.usp_AssertRowCountTolerance", errorDescription=msg)
        if raiseOnFailure:
            raise ControlError(50000, msg[:2044])
    return len(failed)


def rowCountBalance(spark: Any, catalog: str, batchId: int, scope: str = "ALL", objectName: Optional[str] = None,
                    absoluteTolerance: Optional[int] = None, percentTolerance: Optional[Any] = None) -> List[Dict[str, Any]]:
    """The result set the legacy procedure returned (``ToleranceStatus`` = Exempt / Breached / WithinTolerance)."""
    absTol, pctTol = _reconTolerances(spark, catalog, absoluteTolerance, percentTolerance)
    rows = _balance(spark, catalog, batchId, None if scope == "ALL" else scope, objectName)
    for r in rows:
        r["ToleranceStatus"] = "Exempt" if r["IsExempt"] else ("Breached" if _breached(r, absTol, pctTol) else "WithinTolerance")
    return rows


# ----------------------------------------------------------------------------- data quality

def evaluateDataQualityRules(spark: Any, catalog: str, batchId: Optional[int] = None,
                             packageExecutionId: Optional[int] = None, ruleGroupCode: Optional[str] = None,
                             objectName: Optional[str] = None, regionCode: Optional[str] = None,
                             businessDate: Optional[_dt.date] = None) -> int:
    """``etl.usp_EvaluateDataQualityRules``. Returns the number of rules with ``ResultStatus = 'Failed'``.

    For every active rule in scope: ``MeasuredValue = count(*) FROM <object> AS <alias> WHERE <RuleExpression>``
    (legacy ``schema.Object`` references are rewritten with :func:`naming.translateLegacyReferences`).
    Status: expression error -> ``NotEvaluated`` (+ Warning in error_log, never raises); measure <=
    threshold -> ``Passed``; an active ``data_quality_rule_exception`` -> ``Warned``; severity ``FAIL``
    -> ``Failed``; otherwise ``Warned``. One ``etl.data_quality_result`` row per rule.
    """
    batchId, packageExecutionId = _nullIfZero(batchId), _nullIfZero(packageExecutionId)
    if businessDate is None:
        businessDate = _utcnow().date()
    elif isinstance(businessDate, str):
        businessDate = _dt.date.fromisoformat(businessDate[:10])
    where, args = ["r.IsActive = true"], {}
    if ruleGroupCode is not None:
        where.append("r.RuleGroupCode = :grp"); args["grp"] = ruleGroupCode
    if objectName is not None:
        where.append("r.ObjectName = :obj"); args["obj"] = objectName
    if regionCode is not None:
        where.append("(r.RegionCode IS NULL OR r.RegionCode = :region)"); args["region"] = regionCode
    rules = _rows(spark,
                  "SELECT r.DataQualityRuleId, r.RuleCode, r.ObjectName, r.RuleExpression, r.SeverityCode, "
                  f"r.ThresholdValue, r.RegionCode FROM {_t(catalog, 'data_quality_rule')} AS r "
                  f"WHERE {' AND '.join(where)} ORDER BY r.RuleGroupCode, r.RuleCode", **args)
    exceptions = _rows(spark, f"SELECT RuleCode, ObjectName, RegionCode FROM {_t(catalog, 'data_quality_rule_exception')} "
                              "WHERE :bd BETWEEN EffectiveFrom AND EffectiveTo", bd=businessDate)

    results, failedRuleCount = [], 0
    for rule in rules:
        measure, rowsEvaluated, detail = None, None, None
        try:
            target = naming.legacyToDelta(catalog, rule["ObjectName"])
            alias = naming.aliasFor(rule["ObjectName"])
            expr = naming.translateLegacyReferences(catalog, rule["RuleExpression"])
            measure = _decimal.Decimal(int(spark.sql(f"SELECT count(*) FROM {target} AS {alias} WHERE {expr}").first()[0]))
            rowsEvaluated = int(spark.sql(f"SELECT count(*) FROM {target}").first()[0])
        except Exception as exc:  # noqa: BLE001 - rule errors must not take the batch down
            measure = _decimal.Decimal(-1)
            detail = ("Rule could not be evaluated: " + str(exc))[:2000]
            logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, errorSeverity="Warning",
                     sourceName="etl.usp_EvaluateDataQualityRules", sourceComponent=rule["ObjectName"],
                     procedureName="etl.usp_EvaluateDataQualityRules", errorDescription=detail)
        threshold = _decimal.Decimal(str(rule["ThresholdValue"]))
        if measure < 0:
            status = "NotEvaluated"
        elif measure <= threshold:
            status = "Passed"
        elif any(x["RuleCode"] == rule["RuleCode"]
                 and (x["ObjectName"] is None or x["ObjectName"] == rule["ObjectName"])
                 and (x["RegionCode"] is None or x["RegionCode"] == rule["RegionCode"]) for x in exceptions):
            status = "Warned"
        elif rule["SeverityCode"] == "FAIL":
            status = "Failed"
        else:
            status = "Warned"
        if status == "Failed":
            failedRuleCount += 1
        results.append({
            "BatchId": batchId, "PackageExecutionId": packageExecutionId, "ObjectName": rule["ObjectName"],
            "RuleCode": rule["RuleCode"], "MeasuredValue": measure, "ThresholdValue": threshold,
            "RowsEvaluated": rowsEvaluated, "ResultStatus": status, "RegionCode": rule["RegionCode"], "DetailText": detail,
        })
    _insert(spark, catalog, "data_quality_result", results)
    return failedRuleCount


# ----------------------------------------------------------------------------- purge

def _deleteChunked(spark: Any, table: str, idColumn: str, predicate: str, chunkSize: int, **args: Any) -> int:
    """``DELETE TOP (@ChunkSize) ... WHILE @@ROWCOUNT > 0`` on Delta.

    The candidate ids are selected first (the predicate may contain subqueries) and then deleted in
    id-list chunks, because Delta ``DELETE`` does not accept subqueries on every runtime.
    """
    ids = [int(r[0]) for r in _sql(spark, f"SELECT {idColumn} FROM {table} WHERE {predicate} ORDER BY {idColumn}", **args).collect()]
    for start in range(0, len(ids), int(chunkSize)):
        chunk = ids[start:start + int(chunkSize)]
        _sql(spark, f"DELETE FROM {table} WHERE {idColumn} IN ({', '.join(str(i) for i in chunk)})")
    return len(ids)


def _monthsAgo(now: _dt.datetime, months: int) -> _dt.datetime:
    m = now.month - 1 - months
    y = now.year + m // 12
    m = m % 12 + 1
    import calendar
    d = min(now.day, calendar.monthrange(y, m)[1])
    return now.replace(year=y, month=m, day=d)


def purgeControlHistory(spark: Any, catalog: str, retentionDays: Optional[int] = None,
                        executionHistoryMonths: int = 13, errorHistoryMonths: int = 24,
                        rejectHistoryMonths: int = 12, qualityHistoryMonths: int = 13, whatIf: bool = False,
                        chunkSize: int = 25000) -> List[Dict[str, Any]]:
    """``etl.usp_PurgeControlHistory``. Returns the audit rows (``TableName, CutoffUtc, RowsDeleted, DurationSeconds``).

    ``retentionDays`` overrides every cutoff; otherwise the per-table month windows apply. Deletes
    children before parents in id-bounded chunks (``chunkSize`` outside 1000..200000 -> 25000), in
    the legacy order: row_count_audit, data_quality_result, reprocessed rejected_record,
    rejected_record_staging older than 7 days, error_log, orphan package_execution, orphan
    batch_step, orphan batch - one ``etl.control_purge_audit`` row each. ``whatIf`` returns the
    counts that would be deleted without writing anything.
    """
    now = _utcnow()
    if retentionDays is not None:
        execCut = errCut = rejCut = dqCut = now - _dt.timedelta(days=int(retentionDays))
    else:
        execCut = _monthsAgo(now, int(executionHistoryMonths if executionHistoryMonths is not None else 13))
        errCut = _monthsAgo(now, int(errorHistoryMonths if errorHistoryMonths is not None else 24))
        rejCut = _monthsAgo(now, int(rejectHistoryMonths if rejectHistoryMonths is not None else 12))
        dqCut = _monthsAgo(now, int(qualityHistoryMonths if qualityHistoryMonths is not None else 13))
    chunkSize = int(chunkSize) if chunkSize is not None and 1000 <= int(chunkSize) <= 200000 else 25000
    t = lambda n: _t(catalog, n)  # noqa: E731

    if whatIf:
        return _rows(spark, f"""
            SELECT 'etl.RowCountAudit' AS TableName, CAST(:execCut AS TIMESTAMP) AS CutoffUtc, count(*) AS RowsThatWouldBeDeleted
            FROM {t('row_count_audit')} AS a INNER JOIN {t('package_execution')} AS pe ON pe.PackageExecutionId = a.PackageExecutionId
            WHERE pe.StartedAtUtc < :execCut
            UNION ALL SELECT 'etl.PackageExecution', CAST(:execCut AS TIMESTAMP), count(*) FROM {t('package_execution')} WHERE StartedAtUtc < :execCut
            UNION ALL SELECT 'etl.ErrorLog', CAST(:errCut AS TIMESTAMP), count(*) FROM {t('error_log')} WHERE LoggedAtUtc < :errCut
            UNION ALL SELECT 'etl.RejectedRecord', CAST(:rejCut AS TIMESTAMP), count(*) FROM {t('rejected_record')} WHERE LoggedAtUtc < :rejCut
            UNION ALL SELECT 'etl.DataQualityResult', CAST(:dqCut AS TIMESTAMP), count(*) FROM {t('data_quality_result')} WHERE EvaluatedAtUtc < :dqCut
            """, execCut=execCut, errCut=errCut, rejCut=rejCut, dqCut=dqCut)

    audit: List[Dict[str, Any]] = []

    def record(tableName: str, cutoff: _dt.datetime, rowsDeleted: int, started: Optional[_dt.datetime]) -> None:
        duration = 0 if started is None else int((_utcnow() - started).total_seconds())
        audit.append({"TableName": tableName, "CutoffUtc": cutoff, "RowsDeleted": int(rowsDeleted), "DurationSeconds": duration})

    # 1. row count audit - child of package execution
    started = _utcnow()
    record("etl.RowCountAudit", execCut,
           _deleteChunked(spark, t("row_count_audit"), "RowCountAuditId",
                          f"PackageExecutionId IN (SELECT pe.PackageExecutionId FROM {t('package_execution')} AS pe WHERE pe.StartedAtUtc < :cut)",
                          chunkSize, cut=execCut), started)
    # 2. data quality results
    started = _utcnow()
    record("etl.DataQualityResult", dqCut,
           _deleteChunked(spark, t("data_quality_result"), "DataQualityResultId", "EvaluatedAtUtc < :cut", chunkSize, cut=dqCut), started)
    # 3. rejected records, only once reprocessed
    started = _utcnow()
    record("etl.RejectedRecord", rejCut,
           _deleteChunked(spark, t("rejected_record"), "RejectedRecordId", "LoggedAtUtc < :cut AND IsReprocessed = true",
                          chunkSize, cut=rejCut), started)
    # 4. reject staging is scratch; anything older than a week is abandoned
    stagingCut = now - _dt.timedelta(days=7)
    started = _utcnow()
    record("etl.RejectedRecordStaging", stagingCut,
           _deleteChunked(spark, t("rejected_record_staging"), "RejectedRecordStagingId", "LandedAtUtc < :cut",
                          chunkSize, cut=stagingCut), started)
    # 5. error log
    started = _utcnow()
    record("etl.ErrorLog", errCut,
           _deleteChunked(spark, t("error_log"), "ErrorLogId", "LoggedAtUtc < :cut", chunkSize, cut=errCut), started)
    # 6. package executions, once their audit children are gone
    started = _utcnow()
    record("etl.PackageExecution", execCut,
           _deleteChunked(spark, t("package_execution"), "PackageExecutionId",
                          f"StartedAtUtc < :cut AND NOT EXISTS (SELECT 1 FROM {t('row_count_audit')} AS a WHERE a.PackageExecutionId = {t('package_execution')}.PackageExecutionId)"
                          f" AND NOT EXISTS (SELECT 1 FROM {t('error_log')} AS e WHERE e.PackageExecutionId = {t('package_execution')}.PackageExecutionId)"
                          f" AND NOT EXISTS (SELECT 1 FROM {t('rejected_record')} AS r WHERE r.PackageExecutionId = {t('package_execution')}.PackageExecutionId)",
                          chunkSize, cut=execCut), started)
    # 7. batch steps and batches, once nothing points at them
    started = _utcnow()
    record("etl.BatchStep", execCut,
           _deleteChunked(spark, t("batch_step"), "BatchStepId",
                          f"BatchId IN (SELECT b.BatchId FROM {t('batch')} AS b WHERE b.StartedAtUtc < :cut "
                          f"AND NOT EXISTS (SELECT 1 FROM {t('package_execution')} AS pe WHERE pe.BatchId = b.BatchId))",
                          chunkSize, cut=execCut), started)
    started = _utcnow()
    record("etl.Batch", execCut,
           _deleteChunked(spark, t("batch"), "BatchId",
                          f"StartedAtUtc < :cut"
                          f" AND NOT EXISTS (SELECT 1 FROM {t('package_execution')} AS pe WHERE pe.BatchId = {t('batch')}.BatchId)"
                          f" AND NOT EXISTS (SELECT 1 FROM {t('batch_step')} AS bs WHERE bs.BatchId = {t('batch')}.BatchId)"
                          f" AND NOT EXISTS (SELECT 1 FROM {t('error_log')} AS e WHERE e.BatchId = {t('batch')}.BatchId)"
                          f" AND NOT EXISTS (SELECT 1 FROM {t('rejected_record')} AS rr WHERE rr.BatchId = {t('batch')}.BatchId)",
                          chunkSize, cut=execCut), started)
    _insert(spark, catalog, "control_purge_audit", audit)
    return audit


__all__ = [
    "ControlError", "PackageRun",
    "startBatch", "endBatch", "startBatchStep", "endBatchStep", "logPackageStart", "logPackageEnd", "packageRun",
    "logError", "logRowCount", "logRejectedRecord", "logReject", "logRejectedRecordSet", "stageRejectedRecords",
    "getWatermark", "setWatermark", "getConfiguration", "getConfigurationValue",
    "assertRowCountReconciliation", "assertRowCountTolerance", "rowCountBalance", "evaluateDataQualityRules",
    "purgeControlHistory",
    "findRunningBatch", "getBatch", "markBatchStepSkipped", "getLatestBatchStep", "getPackageExecution",
    "getPackageExecutions", "getFailedPackages", "getWatermarkRow",
]
