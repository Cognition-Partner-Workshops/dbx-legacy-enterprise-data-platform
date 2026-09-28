"""Boilerplate shared by the seven ING_FILE_* notebooks: parameters, batch adoption, package lifecycle.

Everything control-related goes through ``dbx_etl_common`` (session 00); this module
only sequences the calls the way the legacy packages did (usp_LogPackageStart ->
data flow -> usp_LogPackageEnd, usp_LogError on failure).
"""

from dataclasses import asdict
import json
import traceback
from typing import Dict, Optional

from wwi_file_ingestion import control_totals as ct
from wwi_file_ingestion import feeds, runner

BATCH_NAME = "Master_File_Ingestion"
BATCH_TYPE = "FileIngestion"
STEP_SEQUENCE = {"Partner Drops": 1, "Carrier And Catalog": 2, "Quarantine": 3}
EXTRA_WIDGETS = {"ControlTotalMode": ct.MODE_LEGACY, "UseAutoLoader": "True"}
STANDARD_WIDGETS = {
    "BatchId": "0",
    "BusinessDate": "",
    "ReloadFullHistory": "False",
    "EnvironmentCode": "DEV",
    "RestartFromStep": "",
    "catalog": "",
}


def defineWidgets(dbutils) -> None:
    for name, default in list(STANDARD_WIDGETS.items()) + list(EXTRA_WIDGETS.items()):
        dbutils.widgets.text(name, default)


def readExtraParams(dbutils) -> Dict[str, object]:
    mode = (dbutils.widgets.get("ControlTotalMode") or ct.MODE_LEGACY).strip().lower()
    useAutoLoader = (dbutils.widgets.get("UseAutoLoader") or "True").strip().lower() != "false"
    return {"controlTotalMode": mode, "useAutoLoader": useAutoLoader}


def resolveBatchId(spark, control, catalog: str, params: Dict, businessDate=None) -> (int, bool):
    """BatchId > 0 -> run inside the master's batch; 0 -> start / adopt the Running Master_File_Ingestion batch.

    Returns (batchId, startedHere). With allowAdoptRunning=True every task of a
    stand-alone run adopts the same batch, so the job behaves like one legacy
    master execution.
    """
    batchId = int(params.get("batchId") or 0)
    if batchId > 0:
        return batchId, False
    batchId = control.startBatch(
        spark, catalog, BATCH_NAME, batchType=BATCH_TYPE, businessDate=businessDate,
        environmentCode=params.get("environmentCode"), allowAdoptRunning=True,
        notes="started by wwi_03_file_ingestion (BatchId parameter was 0)",
    )
    return int(batchId), True


def skipForRestart(spec: feeds.FeedSpec, restartFromStep: Optional[str]) -> bool:
    """RestartFromStep names a master phase; packages of earlier phases are skipped."""
    restart = (restartFromStep or "").strip()
    if not restart or restart not in STEP_SEQUENCE:
        return False
    return STEP_SEQUENCE[spec.stepName] < STEP_SEQUENCE[restart]


def summaryJson(summary: runner.RunSummary) -> str:
    payload = asdict(summary)
    payload["fileTotals"] = [
        {"fileName": t.fileName, "status": getattr(t, "status", None), "detailRowCount": t.detailRowCount,
         "rejectedRowCount": t.rejectedRowCount, "warnings": t.warnings}
        for t in summary.fileTotals
    ]
    return json.dumps(payload, default=str)


def runPackageNotebook(spark, dbutils, control, params, spec: feeds.FeedSpec, catalog: str,
                       controlTotalMode: str = ct.MODE_LEGACY, useAutoLoader: bool = True) -> runner.RunSummary:
    """usp_LogPackageStart -> package body -> usp_LogPackageEnd (+ usp_LogError and re-raise on failure)."""
    batchId, startedHere = resolveBatchId(spark, control, catalog, params, params.get("businessDate"))
    if skipForRestart(spec, params.get("restartFromStep")):
        summary = runner.RunSummary(packageName=spec.packageName)
        summary.warnings.append("skipped: RestartFromStep=%s" % params.get("restartFromStep"))
        return summary

    packageExecutionId = control.logPackageStart(
        spark, catalog, batchId, spec.packageName, projectName=feeds.PROJECT_NAME, stepName=spec.stepName
    )
    try:
        summary = runner.runPackage(
            spark, dbutils, control, catalog, batchId, packageExecutionId, spec,
            controlTotalMode=controlTotalMode, useAutoLoader=useAutoLoader,
        )
    except Exception as exc:  # noqa: BLE001 - mirrors the OnError event handler
        control.logError(
            spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, errorSeverity="Error",
            errorCode=type(exc).__name__, sourceName=spec.packageName, sourceComponent="Ingest Files",
            errorDescription=traceback.format_exc()[-4000:],
        )
        control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
        raise
    control.logPackageEnd(
        spark, catalog, packageExecutionId, status="Succeeded", rowsRead=summary.rowsRead,
        rowsInserted=summary.rowsInserted, rowsRejected=summary.rowsRejected,
    )
    if startedHere and spec.packageName == feeds.QUARANTINE_MALFORMED.packageName:
        # last task of the DAG closes the batch this job opened; a master-run batch is closed by session 00
        control.endBatch(spark, catalog, batchId)
    return summary
