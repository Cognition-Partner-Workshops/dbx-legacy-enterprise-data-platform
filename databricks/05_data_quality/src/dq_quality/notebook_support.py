"""Small notebook-side conveniences shared by the ten DQ notebooks.

Nothing here touches the control tables; lifecycle logging always goes through
``dbx_etl_common.control``.
"""
from __future__ import annotations

from typing import Optional

PROJECT_NAME = "WWI_DataQuality"

# Quality phases in ssis/orchestration-plan.json order (Master_Daily_ETL); RestartFromStep
# names one of these and every earlier step is skipped by the master job.
QUALITY_STEPS = ["File Screen", "Data Quality", "Referential Screen",
                 "Reject Routing", "Referential Rescreen"]

PACKAGE_STEP = {
    "DQ_File_Screen": "File Screen",
    "DQ_Rule_Engine": "Data Quality",
    "DQ_Customer_Screen": "Data Quality",
    "DQ_Supplier_Screen": "Data Quality",
    "DQ_OrderLine_Screen": "Data Quality",
    "DQ_InvoiceLine_Screen": "Data Quality",
    "DQ_Payment_Screen": "Data Quality",
    "DQ_Threshold_Gate": "Data Quality",
    "DQ_Referential_Screen": "Referential Screen",
    "DQ_Reject_Reprocess": "Reject Routing",
}

JOB_PARAMETER_DEFAULTS = [
    ("BatchId", "0"),
    ("BusinessDate", ""),
    ("ReloadFullHistory", "False"),
    ("EnvironmentCode", "DEV"),
    ("RestartFromStep", ""),
    ("catalog", ""),
]


def ensureWidgets(dbutils) -> None:
    """Declare the six job parameters as widgets so interactive runs behave like job runs."""
    for name, default in JOB_PARAMETER_DEFAULTS:
        dbutils.widgets.text(name, default)


def shouldSkipForRestart(restartFromStep: Optional[str], stepName: str) -> bool:
    """True when ``RestartFromStep`` names a later quality phase than ``stepName``."""
    if not restartFromStep or not restartFromStep.strip():
        return False
    target = restartFromStep.strip()
    if target not in QUALITY_STEPS or stepName not in QUALITY_STEPS:
        return False
    return QUALITY_STEPS.index(stepName) < QUALITY_STEPS.index(target)


def setTaskValue(dbutils, key: str, value) -> None:
    """Publish a task value for downstream ``condition_task`` / ``{{tasks.X.values.key}}`` consumers."""
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:  # noqa: BLE001 - interactive notebooks have no job context
        pass


def percentLiteral(value) -> str:
    return "%.4f" % float(value if value is not None else 0)
