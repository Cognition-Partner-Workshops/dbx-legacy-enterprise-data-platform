"""Master_* orchestration: the SSIS control-flow plan (config/orchestration-plan.json) executed as a
Lakeflow Job.

Every plan node becomes one (phase: three) job task(s) that all call :class:`Orchestrator`. Tasks are
wired with ``depends_on`` + ``run_if: ALL_DONE``; whether a node really executes is decided here from
the SSIS precedence constraints (Success / Failure / Completion + expression), so the semantics live in
testable Python rather than in Databricks ``condition_task`` chains. Node outcomes and SSIS ``User::``
variables are persisted per job run in ``etl_orchestration_variable`` so tasks can see each other.
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Callable

from platform_control.config import ALL_OWNED_PACKAGES, GROUP_SLUG, PlatformConfig
from platform_control.control import ControlFramework, utcNow
from platform_control.rules import evaluateSsisAssignment, evaluateSsisExpression

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
PLAN_PATH = CONFIG_DIR / "orchestration-plan.json"
SIBLING_PATH = CONFIG_DIR / "sibling_groups.json"

SUCCEEDED = "Succeeded"
FAILED = "Failed"
SKIPPED = "Skipped"
RUNNING = "Running"
UNRESOLVED = "Unresolved"

NODE_STATUS_PREFIX = "node_status:"
CHILD_STATUS_PREFIX = "child_status:"
NODE_REASON_PREFIX = "node_reason:"

SENTINEL_DATE = "1900-01-01"


# ----------------------------------------------------------------------------- plan model
def loadPlan(path: Path | str | None = None) -> dict:
    with open(path or PLAN_PATH, encoding="utf-8") as handle:
        return json.load(handle)


def loadSiblingGroups(path: Path | str | None = None) -> dict[str, str]:
    """package -> sibling group slug (never contains our own packages)."""
    with open(path or SIBLING_PATH, encoding="utf-8") as handle:
        groups = json.load(handle)["groups"]
    return {pkg: slug for slug, pkgs in groups.items() for pkg in pkgs}


def siblingJobName(package: str, siblingGroups: dict[str, str]) -> str:
    slug = GROUP_SLUG if package in ALL_OWNED_PACKAGES else siblingGroups.get(package)
    if slug is None:
        raise KeyError(f"{package} is not assigned to any migration group")
    return f"ssis_{slug}_{package}"


def taskKey(text: str) -> str:
    key = re.sub(r"[^A-Za-z0-9_]+", "_", text).strip("_")
    return key[:100]


class MasterPlan:
    """One ``roots[]`` entry of the orchestration plan with graph helpers."""

    def __init__(self, root: dict):
        self.root: str = root["root"]
        self.project: str = root["project"]
        self.description: str = root["description"]
        self.parameters: dict = dict(root["parameters"])
        self.variables: dict = dict(root["variables"])
        self.nodes: dict[str, dict] = {}
        for node in root["nodes"]:
            self.nodes[node["name"]] = self._normalise(node)
        self.edges: list[dict] = [dict(e) for e in root["edges"]]
        names = set(self.nodes)
        for edge in self.edges:
            if edge["from"] not in names or edge["to"] not in names:
                raise ValueError(f"{self.root}: edge {edge} references an unknown node")

    @staticmethod
    def _normalise(node: dict) -> dict:
        node = dict(node)
        if node["kind"] == "package":
            # A bare Execute Package Task is a one-child phase without its own StepId variable.
            node.update(
                kind="phase",
                sequence=node.get("sequence", 0),
                group="Ingest",
                streams=1,
                step_variable=None,
                children=[{"package": node["package"], "project": node.get("project"), "parameters": node.get("parameters", {})}],
                in_package_children=[],
            )
        return node

    @classmethod
    def load(cls, root: str, plan: dict | None = None) -> "MasterPlan":
        plan = plan or loadPlan()
        for entry in plan["roots"]:
            if entry["root"] == root:
                return cls(entry)
        raise KeyError(f"{root} is not a root in the orchestration plan")

    @classmethod
    def loadAll(cls, plan: dict | None = None) -> list["MasterPlan"]:
        plan = plan or loadPlan()
        return [cls(entry) for entry in plan["roots"]]

    def predecessors(self, name: str) -> list[dict]:
        return [e for e in self.edges if e["to"] == name]

    def successors(self, name: str) -> list[dict]:
        return [e for e in self.edges if e["from"] == name]

    def topologicalOrder(self) -> list[str]:
        indegree = {n: 0 for n in self.nodes}
        for edge in self.edges:
            indegree[edge["to"]] += 1
        ready = [n for n in self.nodes if indegree[n] == 0]
        order: list[str] = []
        while ready:
            current = ready.pop(0)
            order.append(current)
            for edge in self.successors(current):
                indegree[edge["to"]] -= 1
                if indegree[edge["to"]] == 0:
                    ready.append(edge["to"])
        if len(order) != len(self.nodes):
            raise ValueError(f"{self.root}: the plan contains a cycle")
        return order

    def childPackages(self) -> list[str]:
        seen: list[str] = []
        for node in self.nodes.values():
            for child in node.get("children", []):
                if child["package"] not in seen:
                    seen.append(child["package"])
        return seen

    def batchType(self) -> str:
        for node in self.nodes.values():
            if node["kind"] == "batch_start":
                return node["batch_type"]
        return "Adhoc"


# ----------------------------------------------------------------------------- precedence semantics
def edgeSatisfied(edge: dict, predecessorStatus: str, variables: dict, parameters: dict) -> bool:
    value = edge["value"]
    if value == "Success":
        outcomeOk = predecessorStatus == SUCCEEDED
    elif value == "Failure":
        outcomeOk = predecessorStatus == FAILED
    elif value == "Completion":
        outcomeOk = predecessorStatus in (SUCCEEDED, FAILED)
    else:
        raise ValueError(f"Unknown precedence value {value!r}")
    if not outcomeOk:
        return False
    expression = edge.get("expression")
    if not expression:
        return True
    return evaluateSsisExpression(expression, variables, parameters)


def shouldRun(plan: MasterPlan, nodeName: str, statuses: dict[str, str], variables: dict, parameters: dict) -> tuple[bool, str]:
    """Decide whether a node executes given the outcomes of its predecessors.

    SSIS evaluates constraints with LogicalAnd=True, which in the legacy packages would leave nodes such
    as ``End Batch`` (three mutually exclusive incoming branches) unreachable. The intent expressed by the
    plan is: run when at least one incoming constraint is satisfied and no *executed* predecessor left
    its constraint unsatisfied without branching elsewhere (that is a genuine failure of the path).
    """
    incoming = plan.predecessors(nodeName)
    if not incoming:
        return True, "root node"
    satisfied: list[str] = []
    violated: list[str] = []
    for edge in incoming:
        status = statuses.get(edge["from"], SKIPPED)
        if status in (SKIPPED, RUNNING):
            continue
        if edgeSatisfied(edge, status, variables, parameters):
            satisfied.append(edge["from"])
            continue
        branchedElsewhere = any(
            edgeSatisfied(other, status, variables, parameters)
            for other in plan.successors(edge["from"])
            if other is not edge and other["to"] != nodeName
        )
        if not branchedElsewhere:
            violated.append(edge["from"])
    if not satisfied:
        return False, "no incoming precedence constraint was satisfied"
    if violated:
        return False, f"constraint violated by {', '.join(violated)}"
    return True, f"satisfied by {', '.join(satisfied)}"


# ----------------------------------------------------------------------------- control-node translations
def _controlQuery(cf: ControlFramework, node: dict, variables: dict, parameters: dict) -> object:
    """Each legacy Execute SQL query node has a fixed translation against the Delta control tables."""
    name = node["name"]
    batchId = int(variables.get("BatchId") or 0)
    env = parameters.get("EnvironmentCode", cf.cfg.environmentCode)
    if name == "Check Nightly Batch":
        return cf.runningBatchCount("Daily")
    if name == "Read Period Status":
        return cf.getConfiguration("Finance.PeriodStatus", default="", environmentCode=env) or ""
    if name == "Read Subledger Variance":
        return int(
            cf.scalar(
                f"SELECT COALESCE(SUM(ABS(ra.source_row_count - ra.target_row_count)), 0) FROM {cf.t('etl_row_count_audit')} ra "
                f"JOIN {cf.t('etl_package_execution')} pe ON pe.package_execution_id = ra.package_execution_id "
                f"WHERE pe.batch_id = {batchId} AND ra.object_name IN ('Fact.Payment', 'Fact.GL Posting')",
                0,
            )
        )
    if name == "Read On Hand Variance":
        return int(
            cf.scalar(
                f"SELECT COALESCE(MAX(ABS(ra.source_row_count - ra.target_row_count)), 0) FROM {cf.t('etl_row_count_audit')} ra "
                f"JOIN {cf.t('etl_package_execution')} pe ON pe.package_execution_id = ra.package_execution_id "
                f"WHERE pe.batch_id = {batchId} AND ra.object_name = 'Fact.Stock Holding'",
                0,
            )
        )
    if name == "Count Unmapped Codes":
        return cf.count(
            "etl_rejected_record", f"batch_id = {batchId} AND reject_reason_code = 'CODE_UNMAPPED' AND is_reprocessed = false"
        )
    if name == "Count Pending Corrections":
        return cf.count("etl_rejected_record", "is_reprocessed = false AND reject_stage IN ('Fact', 'Dimension')")
    if name == "Count Merge Candidates":
        return cf.count(
            "etl_rejected_record",
            f"batch_id = {batchId} AND object_name = 'work.CustomerDedup' AND reject_reason_code = 'DUP_CANDIDATE'",
        )
    if name == "Count Expected Files Missing":
        return int(
            cf.scalar(
                f"SELECT COUNT(*) FROM {cf.t('etl_configuration')} c WHERE c.configuration_key LIKE 'Ingest.ExpectedFile.%' "
                f"AND c.environment_code IN ('{env}', 'ALL') AND NOT EXISTS (SELECT 1 FROM {cf.t('etl_package_execution')} pe "
                "WHERE pe.package_name = c.configuration_value AND pe.started_at_utc >= current_timestamp() - INTERVAL 24 HOURS "
                "AND pe.status = 'Succeeded')",
                0,
            )
        )
    raise KeyError(f"No translation for control query node {name!r}: {node.get('sql')}")


def _controlStatement(cf: ControlFramework, node: dict, variables: dict, parameters: dict) -> None:
    name = node["name"]
    batchId = int(variables.get("BatchId") or 0)
    if name == "Record Extract Attempt":
        cf.update(
            "etl_batch_step",
            {"attempt_number": int(variables.get("ExtractAttempt", 1))},
            f"batch_id = {batchId} AND step_group = 'Extract' AND status = 'Failed'",
        )
        return
    if name == "Log Stand Down":
        cf.logError(
            None,
            "Intraday run skipped: a Daily batch is still running.",
            severity="Information",
            errorCode=0,
            sourceName="Master_Hourly_Incremental",
        )
        return
    raise KeyError(f"No translation for control statement node {name!r}: {node.get('sql')}")


# ----------------------------------------------------------------------------- runtime
SiblingRunner = Callable[[str, dict], str]  # (jobName, jobParameters) -> Succeeded | Failed | Unresolved
PackageRunner = Callable[[str, dict], dict]  # (package, context) -> result


class Orchestrator:
    def __init__(
        self,
        cf: ControlFramework,
        plan: MasterPlan,
        jobRunId: str,
        parameters: dict | None = None,
        siblingRunner: SiblingRunner | None = None,
        packageRunner: PackageRunner | None = None,
        siblingGroups: dict[str, str] | None = None,
        batchGate: Callable[[], tuple[bool, str]] | None = None,
    ):
        """``batchGate`` mirrors a SQL Agent pre-step that decides whether the master runs at all
        (Month End: ref.FiscalCalendar close-date gate); when it returns False the batch is never started."""
        self.cf = cf
        self.batchGate = batchGate
        self.plan = plan
        self.jobRunId = str(jobRunId)
        self.parameters = dict(plan.parameters)
        self.parameters.update({k: v for k, v in (parameters or {}).items() if v is not None and v != ""})
        self.siblingRunner = siblingRunner or (lambda jobName, params: UNRESOLVED)
        self.packageRunner = packageRunner
        self.siblingGroups = siblingGroups if siblingGroups is not None else loadSiblingGroups()

    # -- state -------------------------------------------------------------------------------------
    def variables(self) -> dict:
        state = dict(self.plan.variables)
        state.update(self.cf.getVariables(self.jobRunId))
        return state

    def setVariable(self, name: str, value) -> None:
        self.cf.setVariable(self.jobRunId, name, value)

    def statuses(self, variables: dict | None = None) -> dict[str, str]:
        variables = variables if variables is not None else self.variables()
        return {k[len(NODE_STATUS_PREFIX) :]: v for k, v in variables.items() if k.startswith(NODE_STATUS_PREFIX)}

    def _setNodeStatus(self, name: str, status: str, reason: str | None = None) -> str:
        self.setVariable(f"{NODE_STATUS_PREFIX}{name}", status)
        if reason:
            self.setVariable(f"{NODE_REASON_PREFIX}{name}", reason)
        return status

    def batchId(self, variables: dict | None = None) -> int:
        variables = variables if variables is not None else self.variables()
        return int(variables.get("BatchId") or 0)

    def _businessDate(self) -> date:
        raw = str(self.parameters.get("BusinessDate", SENTINEL_DATE))
        return utcNow().date() if raw in ("", SENTINEL_DATE) else date.fromisoformat(raw)

    # -- nodes -------------------------------------------------------------------------------------
    def runNode(self, name: str) -> str:
        """Execute a non-child node (batch_start, control, reconcile, batch_end, phase start)."""
        node = self.plan.nodes[name]
        variables = self.variables()
        runnable, reason = shouldRun(self.plan, name, self.statuses(variables), variables, self.parameters)
        if not runnable:
            return self._setNodeStatus(name, SKIPPED, reason)
        kind = node["kind"]
        if kind == "batch_start" and self.batchGate is not None:
            proceed, gateReason = self.batchGate()
            if not proceed:
                return self._setNodeStatus(name, SKIPPED, f"agent gate: {gateReason}")
        try:
            if kind == "batch_start":
                self._startBatch(node)
            elif kind == "control":
                self._runControl(node, variables)
            elif kind == "reconcile":
                from platform_control.errors import reconcileRowCounts

                reconcileRowCounts(self.cf, self.batchId(variables), raiseOnFailure=bool(node.get("raise_on_failure", 1)))
            elif kind == "batch_end":
                self._endBatch(node, variables)
            elif kind == "phase":
                return self._startPhase(node, variables)
            else:
                raise ValueError(f"Unknown node kind {kind!r}")
        except Exception as exc:  # noqa: BLE001 - the outcome must be recorded before re-raising
            self._setNodeStatus(name, FAILED, f"{type(exc).__name__}: {exc}"[:2000])
            raise
        return self._setNodeStatus(name, SUCCEEDED, reason)

    def _startBatch(self, node: dict) -> None:
        restartFrom = str(self.parameters.get("RestartFromStep") or "")
        batchId = self.cf.startBatch(
            batchName=self.plan.root,
            batchType=node["batch_type"],
            businessDate=self._businessDate(),
            restartFromStep=restartFrom or None,
            notes=self.plan.description[:500],
            jobRunId=self.jobRunId,
            allowAdoptRunning=bool(restartFrom),
            initiatedBy=f"lakeflow:{GROUP_SLUG}",
        )
        for key, value in self.plan.variables.items():
            self.setVariable(key, value)
        self.setVariable("BatchId", batchId)

    def _runControl(self, node: dict, variables: dict) -> None:
        control = node["control"]
        if control == "expression":
            name, value = evaluateSsisAssignment(node["assignment"], variables, self.parameters)
            self.setVariable(name, value)
        elif control == "query":
            value = _controlQuery(self.cf, node, variables, self.parameters)
            self.setVariable(node["result_variable"].split("::", 1)[1], value)
        elif control == "statement":
            _controlStatement(self.cf, node, variables, self.parameters)
        else:
            raise ValueError(f"Unknown control type {control!r}")

    def _endBatch(self, node: dict, variables: dict) -> None:
        # sibling children fail without an etl_package_execution row of ours, so a failed node
        # (phase/control) fails the batch exactly like a failed BatchStep did in etl.usp_EndBatch
        failedNodes = [n for n, s in self.statuses(variables).items() if s == FAILED]
        forceStatus = node.get("force_status") or (FAILED if failedNodes else None)
        result = self.cf.endBatch(self.batchId(variables), forceStatus=forceStatus)
        self.setVariable("BatchStatus", result["batchStatus"])

    def finalizeBatch(self) -> dict:
        """Safety net after every batch_end task: a batch left Running because no End Batch node was
        reachable (an upstream failure) is closed as Failed, and a Failed batch fails the job run."""
        variables = self.variables()
        batchId = self.batchId(variables)
        if batchId == 0:
            return {"batchStatus": SKIPPED, "reason": "no batch was started"}
        status = self.cf.scalar(f"SELECT status FROM {self.cf.t('etl_batch')} WHERE batch_id = {batchId}")
        if status == RUNNING:
            failedNodes = [n for n, s in self.statuses(variables).items() if s == FAILED]
            result = self.cf.endBatch(batchId, forceStatus=FAILED if failedNodes else None)
            status = result["batchStatus"]
            self.setVariable("BatchStatus", status)
        summary = {"batchId": batchId, "batchStatus": status, "nodes": self.statuses(variables)}
        if status == FAILED:
            raise RuntimeError(f"{self.plan.root} batch {batchId} finished Failed: {json.dumps(summary['nodes'])}")
        return summary

    # -- phases ------------------------------------------------------------------------------------
    def _startPhase(self, node: dict, variables: dict) -> str:
        stepId = self.cf.startBatchStep(
            self.batchId(variables),
            stepName=node["name"],
            stepSequence=int(node.get("sequence") or 0),
            stepGroup=node["group"],
        )
        if node.get("step_variable"):
            self.setVariable(node["step_variable"], stepId)
        self.setVariable(f"step_id:{node['name']}", stepId)
        return self._setNodeStatus(node["name"], RUNNING, "phase started")

    def runChild(self, phaseName: str, package: str) -> str:
        node = self.plan.nodes[phaseName]
        variables = self.variables()
        key = f"{CHILD_STATUS_PREFIX}{phaseName}|{package}"
        if self.statuses(variables).get(phaseName) != RUNNING:
            self.setVariable(key, SKIPPED)
            return SKIPPED
        child = next(c for c in node["children"] if c["package"] == package)
        batchId = self.batchId(variables)
        childParameters = self._childParameters(child, variables)
        if package in ALL_OWNED_PACKAGES:
            status = self._runOwnPackage(package, phaseName, batchId, variables, childParameters)
        else:
            jobName = siblingJobName(package, self.siblingGroups)
            status = self.siblingRunner(jobName, childParameters)
            if status == UNRESOLVED:
                if self.cf.cfg.siblingDispatchOnMissing == "fail":
                    status = FAILED
                self.cf.logError(
                    batchId,
                    f"Sibling job {jobName} is not deployed in this workspace; child {package} of phase "
                    f"'{phaseName}' was {'skipped' if status == UNRESOLVED else 'failed'} (sibling_dispatch_on_missing="
                    f"{self.cf.cfg.siblingDispatchOnMissing}).",
                    severity="Warning" if status == UNRESOLVED else "Error",
                    errorCode=0,
                    sourceName=self.plan.root,
                )
        self.setVariable(key, status)
        if status == FAILED:
            raise RuntimeError(f"{package} failed in phase '{phaseName}' of {self.plan.root}")
        return status

    def _childParameters(self, child: dict, variables: dict) -> dict:
        resolved = {}
        for name, binding in (child.get("parameters") or {}).items():
            scope, _, variable = str(binding).partition("::")
            if scope == "User":
                resolved[name] = variables.get(variable)
            elif scope == "$Package":
                resolved[name] = self.parameters.get(variable)
            else:
                resolved[name] = binding
        return {k: ("" if v is None else str(v)) for k, v in resolved.items()}

    def _runOwnPackage(self, package: str, phaseName: str, batchId: int, variables: dict, childParameters: dict) -> str:
        if self.packageRunner is None:
            raise RuntimeError(f"No package runner configured for owned package {package}")
        executionId = self.cf.startPackageExecution(
            batchId=batchId,
            batchStepId=int(variables.get(f"step_id:{phaseName}") or 0) or None,
            packageName=package,
            projectName=f"WWI_{GROUP_SLUG}",
            jobName=f"ssis_{GROUP_SLUG}_{self.plan.root}",
            jobRunId=self.jobRunId,
        )
        context = {
            "batchId": batchId,
            "batchStepId": int(variables.get(f"step_id:{phaseName}") or 0) or None,
            "packageExecutionId": executionId,
            "parameters": {**self.parameters, **childParameters},
            "variables": variables,
            "master": self.plan.root,
        }
        try:
            result = self.packageRunner(package, context) or {}
        except Exception as exc:  # noqa: BLE001
            self.cf.endPackageExecution(executionId, status=FAILED, statusDetail=f"{type(exc).__name__}: {exc}"[:4000])
            self.cf.logError(
                batchId,
                str(exc)[:4000],
                severity="Error",
                errorCode=getattr(exc, "errorCode", 0) or 0,
                sourceName=package,
                packageExecutionId=executionId,
            )
            return FAILED
        self.cf.endPackageExecution(
            executionId,
            status=SUCCEEDED,
            rowsRead=result.get("rowsRead"),
            rowsInserted=result.get("rowsInserted"),
            rowsRejected=result.get("rowsRejected"),
            statusDetail=json.dumps(result, default=str)[:4000],
        )
        return SUCCEEDED

    def endPhase(self, phaseName: str) -> str:
        node = self.plan.nodes[phaseName]
        variables = self.variables()
        if self.statuses(variables).get(phaseName) != RUNNING:
            return self.statuses(variables).get(phaseName, SKIPPED)
        prefix = f"{CHILD_STATUS_PREFIX}{phaseName}|"
        outcomes = {k[len(prefix) :]: v for k, v in variables.items() if k.startswith(prefix)}
        expected = [c["package"] for c in node["children"]]
        missing = [p for p in expected if p not in outcomes]
        failed = [p for p, s in outcomes.items() if s == FAILED] + missing
        unresolved = [p for p, s in outcomes.items() if s == UNRESOLVED]
        stepId = int(variables.get(f"step_id:{phaseName}") or 0)
        if failed:
            message = f"{len(failed)} of {len(expected)} children failed: {', '.join(failed)}"
            self.cf.endBatchStep(stepId, FAILED, errorMessage=message)
            self._setNodeStatus(phaseName, FAILED, message)
            raise RuntimeError(f"Phase '{phaseName}' of {self.plan.root} failed: {message}")
        message = None
        if unresolved:
            message = f"{len(unresolved)} sibling job(s) not deployed yet: {', '.join(unresolved)}"
        self.cf.endBatchStep(stepId, SUCCEEDED, errorMessage=message)
        return self._setNodeStatus(phaseName, SUCCEEDED, message or "all children succeeded")

    # -- local end-to-end driver (tests, dry runs) ----------------------------------------------------
    def runAll(self, raiseOnFailure: bool = False) -> dict[str, str]:
        """Run the whole plan sequentially in topological order (what the Lakeflow Job does in parallel).

        Like the job's ``run_if: ALL_DONE`` tasks, a failed phase does not stop the walk: downstream
        Failure/Completion edges still fire. Failures are re-raised at the end when asked.
        """
        failures: list[RuntimeError] = []
        for name in self.plan.topologicalOrder():
            node = self.plan.nodes[name]
            try:
                status = self.runNode(name)
                if node["kind"] == "phase" and status == RUNNING:
                    for child in node["children"]:
                        try:
                            self.runChild(name, child["package"])
                        except RuntimeError:
                            pass
                    self.endPhase(name)
            except RuntimeError as exc:
                failures.append(exc)
        try:
            self.finalizeBatch()
        except RuntimeError as exc:
            failures.append(exc)
        if failures and raiseOnFailure:
            raise failures[0]
        return self.statuses()


# ----------------------------------------------------------------------------- Databricks sibling dispatch
def databricksSiblingRunner(cfg: PlatformConfig, timeoutMinutes: int = 240) -> SiblingRunner:
    """Run ``ssis_<slug>_<package>`` by name through the Jobs API and wait for it."""
    from datetime import timedelta

    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()

    def run(jobName: str, jobParameters: dict) -> str:
        jobs = [j for j in client.jobs.list(name=jobName) if j.settings and j.settings.name == jobName]
        if not jobs:
            return UNRESOLVED
        run = client.jobs.run_now(job_id=jobs[0].job_id, job_parameters=jobParameters or None).result(
            timeout=timedelta(minutes=timeoutMinutes)
        )
        state = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
        return SUCCEEDED if state == "SUCCESS" else FAILED

    return run


def fiscalCalendarGate(cf: ControlFramework, override: bool = False) -> Callable[[], tuple[bool, str]]:
    """SQL Agent WWI_Month_End step 01: run only when today is a PeriodCloseDate in ref.FiscalCalendar."""

    def gate() -> tuple[bool, str]:
        if override:
            return True, "calendar gate overridden by parameter"
        today = utcNow().date()
        regions = [
            r[0]
            for r in cf.spark.sql(
                f"SELECT RegionCode FROM {cf.cfg.legacyStaging('ref', 'FiscalCalendar')} WHERE PeriodCloseDate = DATE'{today}'"
            ).collect()
        ]
        if not regions:
            cf.logError(None, "Not a close date for any region; month end skipped.", severity="Information", sourceName="WWI - Month End")
            return False, f"{today} is not a period close date"
        cf.setWatermark("MonthEnd", "RegionsInScope", ",".join(regions), watermarkType="DateWindow")
        return True, f"regions in scope: {','.join(regions)}"

    return gate
