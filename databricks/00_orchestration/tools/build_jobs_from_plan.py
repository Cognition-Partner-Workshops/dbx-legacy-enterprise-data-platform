#!/usr/bin/env python3
"""Generate one Databricks Job resource per SSIS master from ``ssis/orchestration-plan.json``.

    python3 databricks/00_orchestration/tools/build_jobs_from_plan.py            # (re)write resources/*.yml
    python3 databricks/00_orchestration/tools/build_jobs_from_plan.py --check    # fail if output is stale

How the plan maps (details in docs/migration/00_orchestration-package-mapping.md):

* ``batch_start`` / ``batch_end`` / ``reconcile`` / ``control`` nodes -> one ``notebook_task`` each
  (``notebooks/batch_start.py`` ... ``notebooks/control_node.py``).
* ``phase`` node -> ``<Phase>__start`` (``control.startBatchStep``) -> child package tasks split into
  ``streams`` lanes exactly like the .dtsx (``children[i::streams]``, each lane a Success chain) ->
  ``<Phase>__end`` (``run_if: ALL_DONE``, ``control.endBatchStep``, fails the task if a child failed).
* child package -> ``notebooks/run_child_package.py`` which triggers the owning project's job
  ``wwi_<NN_project>`` with ``only=[<Package>]`` (Jobs API 2.2 ``run-now``) and the mapped
  ``job_parameters``; ``package`` nodes are the same task without a phase around it.
* plain ``Success`` edge -> ``depends_on``; ``Failure`` / ``Completion`` / expression edges -> a
  ``condition_task`` gate (``gate_<n>_<from>__<to>``): its ``run_if`` carries the edge value
  (``ALL_FAILED`` / ``ALL_DONE``) and its condition is the edge expression, evaluated by the
  upstream node's notebook into task value ``edge_<n>``. Nodes with several incoming edges use
  ``run_if: AT_LEAST_ONE_SUCCESS`` (SSIS logical OR, as ``Invoke-EstateOrchestration.ps1`` does).
* every task carries a ``description`` marker (``plan-node`` / ``plan-edge`` / ``plan-child``) so
  ``tools/check_plan_parity.py`` can rebuild the node/edge set from the YAML and diff it with the plan.

Only the standard library is used (the YAML is emitted by a tiny serializer) so the generator runs
anywhere the repo does.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

HERE = Path(__file__).resolve().parent
ORCH_DIR = HERE.parent
REPO = ORCH_DIR.parent.parent
sys.path.insert(0, str(ORCH_DIR / "src"))
from wwi_orchestration import plan_expression, tsql_translate  # noqa: E402

PLAN_PATH = REPO / "ssis" / "orchestration-plan.json"
RESOURCES_DIR = ORCH_DIR / "resources"
NOTEBOOKS = "../notebooks"
ENVIRONMENT_KEY = "default"
WHEEL_GLOB = "../../common/dbx_etl_common/dist/*.whl"

# docs/dependency-maps/etl-dependency-map.md, "The masters" (Quartz cron; timezone is a bundle variable)
SCHEDULES: Dict[str, Tuple[str, bool, str]] = {
    "Master_Daily_ETL": ("0 30 1 * * ?", False, "daily 01:30"),
    "Master_Hourly_Incremental": ("0 0 5-23 * * ?", False, "hourly 05:00-23:00"),
    "Master_Customer_Sync": ("0 15 0 * * ?", False, "daily 00:15"),
    "Master_Month_End": ("0 0 4 * * ?", True, "daily 04:00, calendar-gated, disabled"),
    "Master_Intraday_Inventory": ("0 0/20 * * * ?", False, "every 20 min"),
    "Master_Weekly_Reference_Load": ("0 0 3 ? * SUN", False, "Sunday 03:00"),
    "Master_File_Ingestion": ("0 0/15 * * * ?", False, "every 15 min"),
    "Master_Finance_Close": ("0 0 4 1 * ?", True, "monthly day 1, disabled (time not recorded; 04:00 assumed)"),
    "Master_Weekly_Maintenance": ("0 0 22 ? * SAT", False, "Saturday 22:00"),
}

# plan project (.dtproj name) -> per-project job name (naming contract: wwi_<NN_project>, NN_project = ssis/ directory)
PROJECT_JOBS: Dict[str, str] = {
    "WWI_Extract_Oracle": "wwi_01_oracle_extract",
    "WWI_Extract_SqlServer": "wwi_02_sqlserver_extract",
    "WWI_Ingest_Files": "wwi_03_file_ingestion",
    "WWI_Staging": "wwi_04_staging",
    "WWI_DataQuality": "wwi_05_data_quality",
    "WWI_ReferenceData": "wwi_06_reference_data",
    "WWI_Dimensions": "wwi_07_dimensions",
    "WWI_Facts": "wwi_08_facts",
    "WWI_Aggregates": "wwi_09_aggregates",
    "WWI_Finance": "wwi_10_finance",
    "WWI_Sales": "wwi_11_sales",
    "WWI_Inventory": "wwi_12_inventory",
    "WWI_Procurement": "wwi_13_procurement",
    "WWI_Customer360": "wwi_14_customer_360",
    "WWI_ErrorHandling": "wwi_15_error_handling",
    "WWI_Maintenance": "wwi_99_maintenance",
}
# job parameters every per-project job exposes (shared naming contract)
CHILD_JOB_PARAMETERS = ("BatchId", "BusinessDate", "ReloadFullHistory", "EnvironmentCode", "RestartFromStep", "catalog")
STANDARD_PARAMETERS = ("BatchId", "BusinessDate", "EnvironmentCode", "ReloadFullHistory",
                       "MaxParallelStreams", "MaxExtractAttempts", "RestartFromStep")
MAX_TASK_KEY = 100


def key(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_")


def clip(taskKey: str) -> str:
    if len(taskKey) <= MAX_TASK_KEY:
        return taskKey
    return taskKey[: MAX_TASK_KEY - 9] + "_" + f"{abs(hash(taskKey)) % 10 ** 8:08d}"


def jsonParam(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


# ----------------------------------------------------------------------------- YAML serializer

def _scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_./-]*", text) and text.lower() not in ("true", "false", "null", "yes", "no", "on", "off"):
        return text
    return json.dumps(text, ensure_ascii=False)


def toYaml(value: Any, indent: int = 0) -> List[str]:
    pad = " " * indent
    lines: List[str] = []
    if isinstance(value, dict):
        for k, v in value.items():
            if isinstance(v, dict) and v:
                lines.append(f"{pad}{k}:")
                lines.extend(toYaml(v, indent + 2))
            elif isinstance(v, list) and v:
                lines.append(f"{pad}{k}:")
                lines.extend(toYaml(v, indent + 2))
            elif isinstance(v, (dict, list)):
                lines.append(f"{pad}{k}: {'{}' if isinstance(v, dict) else '[]'}")
            else:
                lines.append(f"{pad}{k}: {_scalar(v)}")
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, dict) and item:
                inner = toYaml(item, indent + 2)
                lines.append(f"{pad}- {inner[0].lstrip()}")
                lines.extend(inner[1:])
            else:
                lines.append(f"{pad}- {_scalar(item)}")
    else:
        lines.append(f"{pad}{_scalar(value)}")
    return lines


# ----------------------------------------------------------------------------- plan model

class Root:
    def __init__(self, root: Dict[str, Any]):
        self.name: str = root["root"]
        self.project: str = root["project"]
        self.description: str = root.get("description", "")
        self.parameters: Dict[str, Any] = root["parameters"]
        self.variables: Dict[str, Any] = root["variables"]
        self.nodes: List[Dict[str, Any]] = root["nodes"]
        self.edges: List[Dict[str, Any]] = root["edges"]
        self.byName = {n["name"]: n for n in self.nodes}
        self.order = {n["name"]: i for i, n in enumerate(self.nodes)}
        self.defaults = plan_expression.variableTable(root)
        self.ancestors = self._ancestors()

    def _ancestors(self) -> Dict[str, Set[str]]:
        parents: Dict[str, Set[str]] = {n["name"]: set() for n in self.nodes}
        for e in self.edges:
            parents[e["to"]].add(e["from"])
        memo: Dict[str, Set[str]] = {}

        def walk(n: str) -> Set[str]:
            if n not in memo:
                acc: Set[str] = set()
                for p in parents[n]:
                    acc.add(p)
                    acc |= walk(p)
                memo[n] = acc
            return memo[n]
        return {n: walk(n) for n in parents}

    def isRetryPhase(self, node: Dict[str, Any]) -> bool:
        """A phase re-running packages an earlier phase of the same root already ran."""
        if node["kind"] != "phase" or not node.get("children"):
            return False
        mine = {c["package"] for c in node["children"]}
        for other in self.nodes:
            if other is node or other["kind"] != "phase" or self.order[other["name"]] >= self.order[node["name"]]:
                continue
            if mine <= {c["package"] for c in other.get("children", [])}:
                return True
        return False


class Builder:
    def __init__(self, root: Root):
        self.root = root
        self.tasks: List[Dict[str, Any]] = []
        self.taskKeys: Set[str] = set()
        self.head: Dict[str, str] = {}
        self.terminal: Dict[str, str] = {}
        self.outgoingExpr: Dict[str, List[Dict[str, str]]] = {n["name"]: [] for n in root.nodes}
        self.childKeys: Dict[str, str] = {}
        self.assigners: Dict[str, List[str]] = {}  # variable -> nodes assigning it
        for n in root.nodes:
            if n["kind"] == "control" and n.get("control") == "expression":
                name, _ = plan_expression.parseAssignment(n["assignment"])
                self.assigners.setdefault(name, []).append(n["name"])
            elif n["kind"] == "control" and n.get("result_variable"):
                self.assigners.setdefault(n["result_variable"], []).append(n["name"])
        self._assignKeys()

    # -- keys --------------------------------------------------------------------------------
    def _assignKeys(self) -> None:
        for n in self.root.nodes:
            k = key(n["name"])
            if n["kind"] == "phase":
                self.head[n["name"]], self.terminal[n["name"]] = clip(k + "__start"), clip(k + "__end")
            else:
                self.head[n["name"]] = self.terminal[n["name"]] = clip(k)
        for n in self.root.nodes:
            if n["kind"] != "phase":
                continue
            for c in n["children"]:
                ck = c["package"] if c["package"] not in self.childKeys.values() else f"{c['package']}__{key(n['name'])}"
                self.childKeys[f"{n['name']}/{c['package']}"] = clip(ck)
        for n in self.root.nodes:
            if n["kind"] == "package":
                self.childKeys[f"{n['name']}/{n['package']}"] = self.head[n["name"]]

    # -- variables ---------------------------------------------------------------------------
    def variableRef(self, node: str, var: str) -> Any:
        """Dynamic value reference (or plan literal) for ``var`` as seen from ``node``."""
        scope, name = var.split("::", 1)
        if scope == "$Package":
            return f"{{{{job.parameters.{name}}}}}"
        anc = self.root.ancestors[node]
        if name == "BatchId":
            starts = [a for a in anc if self.root.byName[a]["kind"] == "batch_start"]
            if starts:
                return f"{{{{tasks.{self.terminal[starts[0]]}.values.BatchId}}}}"
            return "{{job.parameters.BatchId}}"
        if name.startswith("BatchStepId"):
            for n in self.root.nodes:
                if n["kind"] == "phase" and n.get("step_variable") == name and (n["name"] in anc or n["name"] == node):
                    return f"{{{{tasks.{self.head[n['name']]}.values.BatchStepId}}}}"
            return self.root.defaults.get(var, 0)
        candidates = [a for a in self.assigners.get(var, []) if a in anc]
        latest = [a for a in candidates if not any(a in self.root.ancestors[b] for b in candidates if b != a)]
        if latest:
            chosen = sorted(latest, key=lambda a: self.root.order[a])[-1]
            return f"{{{{tasks.{self.terminal[chosen]}.values.{name}}}}}"
        if var in self.root.defaults:
            return self.root.defaults[var]
        raise KeyError(f"{self.root.name}: {node} reads {var}, which the plan does not declare")

    def variablesFor(self, node: str, names: List[str]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for var in sorted(dict.fromkeys(names)):
            out[var] = {"ref": self.variableRef(node, var), "default": self.root.defaults.get(var)}
        return out

    def edgeVariables(self, node: str) -> List[str]:
        names: List[str] = []
        for e in self.outgoingExpr[node]:
            names += plan_expression.referencedVariables(e["expression"])
        return names

    # -- tasks -------------------------------------------------------------------------------
    def notebook(self, taskKey: str, notebook: str, params: Dict[str, Any], description: str, **extra: Any) -> Dict[str, Any]:
        base = {"catalog": "{{job.parameters.catalog}}", "masterName": self.root.name, "jobRunId": "{{job.run_id}}"}
        base.update(params)
        task: Dict[str, Any] = {
            "task_key": taskKey,
            "description": description,
            "notebook_task": {"notebook_path": f"{NOTEBOOKS}/{notebook}.py", "base_parameters": base},
            "environment_key": ENVIRONMENT_KEY,
        }
        task.update(extra)
        return self.add(task)

    def add(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if task["task_key"] in self.taskKeys:
            raise ValueError(f"{self.root.name}: duplicate task_key {task['task_key']}")
        self.taskKeys.add(task["task_key"])
        self.tasks.append(task)
        return task

    def edgesParam(self, node: str) -> str:
        return jsonParam(self.outgoingExpr[node])

    def childTask(self, taskKey: str, node: str, child: Dict[str, Any], phase: Optional[Dict[str, Any]],
                  description: str) -> Dict[str, Any]:
        project = child["project"]
        if project not in PROJECT_JOBS:
            raise KeyError(f"{self.root.name}: no per-project job known for {project}")
        mapped = {}
        for pname, var in child["parameters"].items():
            mapped[pname] = self.variableRef(node, var)
        jobParams: Dict[str, Any] = {}
        for pname in CHILD_JOB_PARAMETERS:
            if pname in mapped:
                jobParams[pname] = mapped[pname]
            elif pname == "BatchId":
                jobParams[pname] = self.variableRef(node, "User::BatchId")
            else:
                jobParams[pname] = f"{{{{job.parameters.{pname}}}}}"
        extraParams = {k: v for k, v in mapped.items() if k not in CHILD_JOB_PARAMETERS}
        retry = phase is not None and self.root.isRetryPhase(phase)
        params = {
            "project": project, "projectJob": PROJECT_JOBS[project], "package": child["package"],
            "jobParameters": jsonParam(jobParams), "extraJobParameters": jsonParam(extraParams),
            "skipIfSucceeded": "true" if retry else "false",
            "stepName": phase["name"] if phase else "",
        }
        extra: Dict[str, Any] = {}
        if retry:
            extra = {"max_retries": "${var.extract_retry_task_retries}",
                     "min_retry_interval_millis": "${var.extract_retry_interval_millis}", "retry_on_timeout": True}
        return self.notebook(taskKey, "run_child_package", params, description, **extra)

    def build(self) -> Dict[str, Any]:
        r = self.root
        # outgoing expression edges are evaluated by the upstream node's notebook
        for i, e in enumerate(r.edges):
            if e.get("expression"):
                self.outgoingExpr[e["from"]].append({"key": f"edge_{i}", "expression": e["expression"]})

        for n in r.nodes:
            kind, name = n["kind"], n["name"]
            marker = f"plan-node: {name} | kind={kind}"
            if kind == "batch_start":
                self.notebook(self.head[name], "batch_start", {
                    "batchType": n["batch_type"], "BatchId": "{{job.parameters.BatchId}}",
                    "BusinessDate": "{{job.parameters.BusinessDate}}", "EnvironmentCode": "{{job.parameters.EnvironmentCode}}",
                    "RestartFromStep": "{{job.parameters.RestartFromStep}}", "edges": self.edgesParam(name),
                    "variables": jsonParam(self.variablesFor(name, self.edgeVariables(name))),
                }, marker)
            elif kind == "batch_end":
                self.notebook(self.head[name], "batch_end", {
                    "BatchId": self.variableRef(name, "User::BatchId"), "forceStatus": n.get("force_status", ""),
                    "edges": self.edgesParam(name),
                    "variables": jsonParam(self.variablesFor(name, self.edgeVariables(name))),
                }, marker)
            elif kind == "reconcile":
                self.notebook(self.head[name], "reconcile", {
                    "BatchId": self.variableRef(name, "User::BatchId"), "raiseOnFailure": "true" if n.get("raise_on_failure") else "false",
                    "edges": self.edgesParam(name),
                    "variables": jsonParam(self.variablesFor(name, self.edgeVariables(name))),
                }, marker)
            elif kind == "control":
                mode = n["control"]
                params: Dict[str, Any] = {"mode": mode, "edges": self.edgesParam(name)}
                reads = self.edgeVariables(name)
                if mode == "expression":
                    varName, expr = plan_expression.parseAssignment(n["assignment"])
                    params["assignment"] = n["assignment"]
                    params["resultVariable"] = varName
                    reads += plan_expression.referencedVariables(expr)
                else:
                    stmt = tsql_translate.translateStatement(n["sql"])
                    params["statement"] = jsonParam(stmt)
                    params["statementParameters"] = jsonParam(list(n.get("parameters") or []))
                    params["resultVariable"] = n.get("result_variable", "")
                    reads += list(n.get("parameters") or [])
                if params["resultVariable"]:
                    reads.append(params["resultVariable"])
                params["variables"] = jsonParam(self.variablesFor(name, reads))
                self.notebook(self.head[name], "control_node", params, f"{marker} control={mode}")
            elif kind == "package":
                self.childTask(self.head[name], name, n, None,
                               f"{marker} | plan-child: {name}/{n['package']} project={n['project']}")
            elif kind == "phase":
                startKey, endKey = self.head[name], self.terminal[name]
                self.notebook(startKey, "step_start", {
                    "BatchId": self.variableRef(name, "User::BatchId"), "stepName": name,
                    "stepSequence": n["sequence"], "stepGroup": n["group"],
                    "restartRecovery": "true" if name == "Restart Recovery" else "false",
                    "RestartFromStep": "{{job.parameters.RestartFromStep}}",
                    "planSteps": jsonParam([p["name"] for p in r.nodes if p["kind"] == "phase"]),
                }, f"{marker} role=start sequence={n['sequence']} group={n['group']} streams={n['streams']}")
                children = n["children"]
                lanes = [children] if n["streams"] <= 1 else [children[i::n["streams"]] for i in range(n["streams"])]
                lanes = [lane for lane in lanes if lane]
                tails: List[str] = []
                laneKeys: List[List[str]] = []
                for laneNo, lane in enumerate(lanes):
                    prev = startKey
                    keys: List[str] = []
                    for c in lane:
                        ck = self.childKeys[f"{name}/{c['package']}"]
                        t = self.childTask(ck, name, c, n, f"plan-child: {name}/{c['package']} project={c['project']} lane={laneNo}")
                        t["depends_on"] = [{"task_key": prev}]
                        prev = ck
                        keys.append(ck)
                    tails.append(prev)
                    laneKeys.append(keys)
                if not children:
                    tails = [startKey]
                self.notebook(endKey, "step_end", {
                    "BatchStepId": f"{{{{tasks.{startKey}.values.BatchStepId}}}}", "stepName": name,
                    "childTaskKeys": jsonParam([k for lane in laneKeys for k in lane]),
                    "edges": self.edgesParam(name),
                    "variables": jsonParam(self.variablesFor(name, self.edgeVariables(name))),
                }, f"{marker} role=end", depends_on=[{"task_key": t} for t in tails], run_if="ALL_DONE")
            else:
                raise ValueError(f"{r.name}: unknown node kind {kind}")

        # edges
        incoming: Dict[str, List[Dict[str, Any]]] = {n["name"]: [] for n in r.nodes}
        for i, e in enumerate(r.edges):
            value = e.get("value", "Success")
            expr = e.get("expression")
            marker = f"plan-edge: {e['from']} -> {e['to']} | value={value}" + (f" expression={expr}" if expr else "")
            if value == "Success" and not expr:
                incoming[e["to"]].append({"task_key": self.terminal[e["from"]]})
                self._byKey(self.head[e["to"]]).setdefault("_edgeMarkers", []).append(marker)
                continue
            gateKey = clip(f"gate_{i}_{key(e['from'])}__{key(e['to'])}")
            gate: Dict[str, Any] = {
                "task_key": gateKey, "description": marker,
                "depends_on": [{"task_key": self.terminal[e["from"]]}],
                "condition_task": {"op": "EQUAL_TO",
                                   "left": f"{{{{tasks.{self.terminal[e['from']]}.values.edge_{i}}}}}" if expr else "true",
                                   "right": "true"},
            }
            if value == "Failure":
                gate["run_if"] = "ALL_FAILED"
            elif value == "Completion":
                gate["run_if"] = "ALL_DONE"
            elif value != "Success":
                raise ValueError(f"{r.name}: unknown edge value {value}")
            self.add(gate)
            incoming[e["to"]].append({"task_key": gateKey, "outcome": "true"})
        for n in r.nodes:
            deps = incoming[n["name"]]
            if not deps:
                continue
            task = self._byKey(self.head[n["name"]])
            task["depends_on"] = deps
            if len(deps) > 1:
                task["run_if"] = "AT_LEAST_ONE_SUCCESS"
        for t in self.tasks:
            markers = t.pop("_edgeMarkers", None)
            if markers:
                t["description"] = t["description"] + " ;; " + " ;; ".join(markers)

        # runner-side "finally": close the batch when the graph ended without reaching a batch_end node
        sinks = [self.terminal[n["name"]] for n in r.nodes if not any(e["from"] == n["name"] for e in r.edges)]
        starts = [n["name"] for n in r.nodes if n["kind"] == "batch_start"]
        self.notebook("zz_close_batch_if_open", "close_batch_if_open", {
            "BatchId": f"{{{{tasks.{self.terminal[starts[0]]}.values.BatchId}}}}" if starts else "{{job.parameters.BatchId}}",
        }, "helper: mirrors the runner's finally block (Invoke-EstateOrchestration.ps1) - not a plan node",
            depends_on=[{"task_key": s} for s in sorted(set(sinks))], run_if="ALL_DONE")

        # order: notebook/gate tasks in creation order is fine for the API; keep deterministic
        cron, paused, cadence = SCHEDULES[r.name]
        parameters = [{"name": p, "default": str(r.parameters[p])} for p in r.parameters]
        parameters.append({"name": "catalog", "default": "${var.catalog}"})
        job = {
            "name": f"wwi_00_{r.name}",
            "description": f"{r.description} [generated from ssis/orchestration-plan.json root {r.name}; legacy cadence: {cadence}]",
            "tags": {"wwi_master": r.name, "wwi_project": r.project, "generated_by": "build_jobs_from_plan.py"},
            "max_concurrent_runs": 1,
            "schedule": {"quartz_cron_expression": cron, "timezone_id": "${var.schedule_timezone}",
                         "pause_status": "PAUSED" if paused else "${var.schedule_pause_status}"},
            "parameters": parameters,
            "environments": [{"environment_key": ENVIRONMENT_KEY, "spec": {"client": "3", "dependencies": [WHEEL_GLOB]}}],
            "tasks": self.tasks,
        }
        return {"resources": {"jobs": {f"wwi_00_{r.name.lower()}": job}}}

    def _byKey(self, taskKey: str) -> Dict[str, Any]:
        for t in self.tasks:
            if t["task_key"] == taskKey:
                return t
        raise KeyError(taskKey)


def render(root: Dict[str, Any]) -> str:
    doc = Builder(Root(root)).build()
    header = [
        f"# Generated by databricks/00_orchestration/tools/build_jobs_from_plan.py from ssis/orchestration-plan.json",
        f"# root: {root['root']} ({root['project']}) - do not edit by hand; re-run the generator.",
    ]
    return "\n".join(header + toYaml(doc)) + "\n"


def loadPlan(path: Path = PLAN_PATH) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plan", type=Path, default=PLAN_PATH)
    ap.add_argument("--out", type=Path, default=RESOURCES_DIR)
    ap.add_argument("--check", action="store_true", help="exit 1 if any resource file differs from the generator output")
    args = ap.parse_args(argv)
    plan = loadPlan(args.plan)
    args.out.mkdir(parents=True, exist_ok=True)
    stale = []
    for root in plan["roots"]:
        target = args.out / f"{root['root']}.yml"
        text = render(root)
        if args.check:
            if not target.exists() or target.read_text(encoding="utf-8") != text:
                stale.append(target.name)
        else:
            target.write_text(text, encoding="utf-8")
            print(f"wrote {target.relative_to(REPO)}")
    if stale:
        print("stale generated resources: " + ", ".join(stale), file=sys.stderr)
        return 1
    if args.check:
        print(f"{len(plan['roots'])} resource file(s) up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
