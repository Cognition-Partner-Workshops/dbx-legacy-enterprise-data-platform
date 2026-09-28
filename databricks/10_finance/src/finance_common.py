"""Runtime helpers for the FIN_* notebooks.

Everything that talks to the control framework goes through ``dbx_etl_common``
(session 00). This module only adds the finance-specific glue: the extra
package parameters the legacy .dtsx files declared, the Master_Finance_Close
phase table used for ``RestartFromStep`` and the batch adoption used when a
notebook is run outside the master job.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from dbx_etl_common import control

PROJECT_NAME = "WWI_Finance"
BATCH_NAME = "Master_Finance_Close"
BATCH_TYPE = "Monthly"

# Master_Finance_Close phases (ssis/orchestration-plan.json) that the finance
# packages participate in. Sequence 50 (Close Aggregates) and 90 (Close
# Escalation) belong to other projects and are documented, not implemented here.
PHASES = {
    "FIN_Load_ApAging": ("Subledger Loads", 10, "Mart"),
    "FIN_Load_WithholdingTax": ("Subledger Loads", 10, "Mart"),
    "FIN_Load_CostAllocation": ("Subledger Loads", 10, "Mart"),
    "FIN_Currency_Revaluation": ("FX Revaluation", 20, "Mart"),
    "FIN_Load_GlPostings": ("General Ledger", 30, "Mart"),
    "FIN_Reconcile_SubledgerToGl": ("Subledger Tie Out", 40, "Mart"),
    "FIN_Close_PeriodLock": ("Period Lock", 60, "Mart"),
}

# Legacy package parameters ($Package::*) and their .dtsx defaults. Blank
# AccountingPeriod / AgingAsOfDate / RevaluationDate mean "derive from
# BusinessDate", which is how Master_Finance_Close supplied them.
FINANCE_PARAMETERS = {
    "AccountingPeriod": "",
    "AgingAsOfDate": "",
    "RevaluationDate": "",
    "IncludeDisputedInvoices": "False",
    "AllowUnbalancedJournals": "False",
    "VarianceTolerance": "1",
    "AllowCloseWithVariance": "False",
    "LedgerScope": "ALL",
    "AllocationRuleSet": "STANDARD",
    "FailOnMissingRate": "True",
    "JurisdictionScope": "ALL",
}


def parseBool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "t", "yes", "y")


def stepFor(packageName: str) -> tuple[str, int, str]:
    return PHASES[packageName]


def shouldSkipForRestart(packageName: str, restartFromStep: str | None) -> bool:
    """RestartFromStep names a Master_Finance_Close phase; every phase that
    sorts before it is skipped, mirroring Invoke-EstateOrchestration.ps1."""
    if not restartFromStep:
        return False
    wanted = restartFromStep.strip().lower()
    restartSeq = None
    for stepName, seq, _group in PHASES.values():
        if stepName.lower() == wanted:
            restartSeq = seq
    if restartSeq is None:
        return False
    return PHASES[packageName][1] < restartSeq


def accountingPeriodFor(businessDate: dt.date) -> str:
    return businessDate.strftime("%Y-%m")


def periodEndDate(accountingPeriod: str) -> dt.date:
    year, month = (int(x) for x in accountingPeriod.split("-"))
    nextMonth = dt.date(year + (month // 12), (month % 12) + 1, 1)
    return nextMonth - dt.timedelta(days=1)


@dataclass
class FinanceParams:
    accountingPeriod: str
    agingAsOfDate: dt.date
    revaluationDate: dt.date
    includeDisputedInvoices: bool
    allowUnbalancedJournals: bool
    varianceTolerance: int
    allowCloseWithVariance: bool
    ledgerScope: str
    allocationRuleSet: str
    failOnMissingRate: bool
    jurisdictionScope: str


def getFinanceParams(dbutils, businessDate: dt.date) -> FinanceParams:
    """Read the finance package parameters from widgets, falling back to the
    .dtsx defaults and deriving the period/dates from BusinessDate."""
    raw = {}
    for name, default in FINANCE_PARAMETERS.items():
        try:
            dbutils.widgets.text(name, default)
        except Exception:  # widget already exists / not in a notebook context
            pass
        try:
            value = dbutils.widgets.get(name)
        except Exception:
            value = default
        raw[name] = (value if value is not None else default).strip()
    return buildFinanceParams(raw, businessDate)


def buildFinanceParams(raw: dict, businessDate: dt.date) -> FinanceParams:
    accountingPeriod = raw.get("AccountingPeriod") or accountingPeriodFor(businessDate)
    asOf = raw.get("AgingAsOfDate") or businessDate.isoformat()
    reval = raw.get("RevaluationDate") or businessDate.isoformat()
    return FinanceParams(
        accountingPeriod=accountingPeriod,
        agingAsOfDate=dt.date.fromisoformat(asOf),
        revaluationDate=dt.date.fromisoformat(reval),
        includeDisputedInvoices=parseBool(raw.get("IncludeDisputedInvoices", "False")),
        allowUnbalancedJournals=parseBool(raw.get("AllowUnbalancedJournals", "False")),
        varianceTolerance=int(raw.get("VarianceTolerance") or 1),
        allowCloseWithVariance=parseBool(raw.get("AllowCloseWithVariance", "False")),
        ledgerScope=(raw.get("LedgerScope") or "ALL").upper(),
        allocationRuleSet=(raw.get("AllocationRuleSet") or "STANDARD").upper(),
        failOnMissingRate=parseBool(raw.get("FailOnMissingRate", "True")),
        jurisdictionScope=(raw.get("JurisdictionScope") or "ALL").upper(),
    )


def resolveBatchId(spark, catalog: str, p: dict) -> tuple[int, bool]:
    """Master_Finance_Close (session 00) normally passes BatchId. When the job
    is run on its own (BatchId = 0) adopt/start the monthly close batch so the
    package executions still hang off a real etl.batch row."""
    batchId = int(p.get("batchId") or 0)
    if batchId > 0:
        return batchId, False
    batchId = control.startBatch(
        spark,
        catalog,
        BATCH_NAME,
        batchType=BATCH_TYPE,
        businessDate=p.get("businessDate"),
        environmentCode=p.get("environmentCode"),
        allowAdoptRunning=True,
        notes="started by wwi_10_finance (BatchId not supplied)",
    )
    return int(batchId), True


def tableExists(spark, fullName: str) -> bool:
    return spark.catalog.tableExists(fullName)


class PackageExecution:
    """Context manager over control.logPackageStart / logPackageEnd / logError.

    The notebooks need the PackageExecutionId for control.logRowCount and
    control.logRejectedRecord*, so the documented primitives are composed here
    instead of control.packageRun (whose run object exposes counters only).
    """

    def __init__(self, spark, catalog: str, batchId: int, packageName: str, stepName: str | None = None):
        self.spark = spark
        self.catalog = catalog
        self.batchId = batchId
        self.packageName = packageName
        self.stepName = stepName
        self.packageExecutionId = None
        self.rowsRead = 0
        self.rowsInserted = 0
        self.rowsUpdated = 0
        self.rowsDeleted = 0
        self.rowsRejected = 0
        self.watermarkFrom = None
        self.watermarkTo = None

    def __enter__(self):
        self.packageExecutionId = control.logPackageStart(
            self.spark, self.catalog, self.batchId, self.packageName, projectName=PROJECT_NAME, stepName=self.stepName
        )
        return self

    def __exit__(self, excType, exc, tb):
        if exc is None:
            status = "Succeeded"
        else:
            status = "Failed"
            control.logError(
                self.spark,
                self.catalog,
                packageExecutionId=self.packageExecutionId,
                batchId=self.batchId,
                errorSeverity="Error",
                errorCode=None,
                sourceName=self.packageName,
                sourceComponent=excType.__name__ if excType else None,
                procedureName=None,
                errorDescription=str(exc)[:4000],
            )
        control.logPackageEnd(
            self.spark,
            self.catalog,
            self.packageExecutionId,
            status=status,
            rowsRead=self.rowsRead,
            rowsInserted=self.rowsInserted,
            rowsUpdated=self.rowsUpdated,
            rowsDeleted=self.rowsDeleted,
            rowsRejected=self.rowsRejected,
            watermarkFrom=self.watermarkFrom,
            watermarkTo=self.watermarkTo,
        )
        return False

    def logRowCount(self, objectName: str, sourceRowCount=None, targetRowCount=None, insertRowCount=None,
                    updateRowCount=None, deleteRowCount=None, rejectRowCount=None):
        control.logRowCount(
            self.spark, self.catalog, self.packageExecutionId, objectName,
            sourceRowCount=sourceRowCount, targetRowCount=targetRowCount, insertRowCount=insertRowCount,
            updateRowCount=updateRowCount, deleteRowCount=deleteRowCount, rejectRowCount=rejectRowCount,
        )
