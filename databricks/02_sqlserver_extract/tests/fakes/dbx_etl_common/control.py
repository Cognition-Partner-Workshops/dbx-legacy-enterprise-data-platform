"""In-memory fake of the etl.* control procedures used by the extraction notebooks."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

EPOCH = "1900-01-01T00:00:00"

calls: List[Tuple[str, Dict[str, Any]]] = []
watermarks: Dict[Tuple[str, str], Dict[str, Any]] = {}
configuration: Dict[str, str] = {}
nowUtc: Optional[datetime] = None
_nextPackageExecutionId = 100


def reset() -> None:
    global _nextPackageExecutionId, nowUtc
    calls.clear()
    watermarks.clear()
    configuration.clear()
    nowUtc = None
    _nextPackageExecutionId = 100


def _record(name: str, **kwargs: Any) -> None:
    calls.append((name, kwargs))


def callsNamed(name: str) -> List[Dict[str, Any]]:
    return [kwargs for called, kwargs in calls if called == name]


def registerWatermark(sourceSystemCode, objectName, lastValue, watermarkType="Timestamp", lookbackMinutes=0, isLocked=False):
    watermarks[(sourceSystemCode, objectName)] = {
        "lastValue": lastValue,
        "type": watermarkType,
        "lookbackMinutes": lookbackMinutes,
        "isLocked": isLocked,
    }


def logPackageStart(spark, catalog, batchId, packageName, projectName=None, stepName=None):
    global _nextPackageExecutionId
    _nextPackageExecutionId += 1
    _record("logPackageStart", catalog=catalog, batchId=batchId, packageName=packageName, projectName=projectName, stepName=stepName)
    return _nextPackageExecutionId


def logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=None, rowsInserted=None, rowsUpdated=None,
                  rowsDeleted=None, rowsRejected=None, watermarkFrom=None, watermarkTo=None):
    _record("logPackageEnd", packageExecutionId=packageExecutionId, status=status, rowsRead=rowsRead, rowsInserted=rowsInserted,
            rowsUpdated=rowsUpdated, rowsDeleted=rowsDeleted, rowsRejected=rowsRejected, watermarkFrom=watermarkFrom, watermarkTo=watermarkTo)


def logError(spark, catalog, packageExecutionId=None, batchId=None, errorSeverity="Error", errorCode=None, sourceName=None,
             sourceComponent=None, procedureName=None, errorDescription=None):
    _record("logError", packageExecutionId=packageExecutionId, batchId=batchId, errorSeverity=errorSeverity, sourceName=sourceName,
            sourceComponent=sourceComponent, errorDescription=errorDescription)


def logRowCount(spark, catalog, packageExecutionId, objectName, sourceRowCount=None, targetRowCount=None, insertRowCount=None,
                updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
    _record("logRowCount", packageExecutionId=packageExecutionId, objectName=objectName, sourceRowCount=sourceRowCount,
            targetRowCount=targetRowCount, insertRowCount=insertRowCount, updateRowCount=updateRowCount,
            deleteRowCount=deleteRowCount, rejectRowCount=rejectRowCount)


def logRejectedRecordSet(spark, catalog, objectName, rejectedDf, batchId=None, packageExecutionId=None, sourceSystemCode=None,
                         rejectStage="Stage", rejectReasonCode=None, businessKeyColumn=None):
    count = rejectedDf.count()
    _record("logRejectedRecordSet", objectName=objectName, rejectedRowCount=count, batchId=batchId,
            packageExecutionId=packageExecutionId, sourceSystemCode=sourceSystemCode, rejectStage=rejectStage,
            rejectReasonCode=rejectReasonCode, businessKeyColumn=businessKeyColumn,
            rejectedKeys=[r[businessKeyColumn] for r in rejectedDf.select(businessKeyColumn).collect()] if businessKeyColumn else None)
    return count


def getWatermark(spark, catalog, sourceSystemCode, objectName, reloadFullHistory=False):
    """Mirrors etl.usp_GetWatermark: epoch/0 on full reload, LookbackMinutes on timestamps."""
    entry = watermarks.setdefault((sourceSystemCode, objectName), {"lastValue": None, "type": "Timestamp", "lookbackMinutes": 0, "isLocked": False})
    if entry["isLocked"]:
        raise RuntimeError("Watermark %s/%s is locked" % (sourceSystemCode, objectName))
    lastValue = entry["lastValue"]
    if reloadFullHistory:
        lastValue = "0" if entry["type"] == "NumericKey" else EPOCH
    now = nowUtc or datetime.utcnow()
    _record("getWatermark", sourceSystemCode=sourceSystemCode, objectName=objectName, reloadFullHistory=reloadFullHistory)
    if entry["type"] == "NumericKey":
        return (lastValue if lastValue is not None else "0"), None
    if entry["type"] == "DateWindow":
        start = datetime.fromisoformat(lastValue) if lastValue else datetime.fromisoformat(EPOCH)
        return start.date().isoformat(), now.date().isoformat()
    from datetime import timedelta
    start = datetime.fromisoformat(lastValue) if lastValue else datetime.fromisoformat(EPOCH)
    start = start - timedelta(minutes=entry["lookbackMinutes"] or 0)
    return start.isoformat(timespec="milliseconds"), now.isoformat(timespec="milliseconds")


def setWatermark(spark, catalog, sourceSystemCode, objectName, watermarkTo, packageExecutionId=None, allowRewind=False):
    _record("setWatermark", sourceSystemCode=sourceSystemCode, objectName=objectName, watermarkTo=watermarkTo,
            packageExecutionId=packageExecutionId, allowRewind=allowRewind)
    if watermarkTo is None or str(watermarkTo) == "":
        return
    entry = watermarks.setdefault((sourceSystemCode, objectName), {"lastValue": None, "type": "Timestamp", "lookbackMinutes": 0, "isLocked": False})
    entry["lastValue"] = str(watermarkTo)


def getConfiguration(spark, catalog, configurationKey, environmentCode=None):
    return configuration.get(configurationKey)
