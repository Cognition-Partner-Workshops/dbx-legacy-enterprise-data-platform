#!/usr/bin/env python3
"""Prove the generated job YAML carries exactly the plan's node and edge sets.

    python3 databricks/00_orchestration/tools/check_plan_parity.py

Every generated task carries a ``description`` marker written by ``build_jobs_from_plan.py``:
``plan-node: <name> | kind=<kind>`` (one per plan node; phases have a ``role=start`` and a
``role=end`` task), ``plan-child: <phase>/<package> project=<project>`` (one per phase child or
``package`` node) and ``plan-edge: <from> -> <to> | value=<value> [expression=<expr>]`` (one per
plan edge - on the gate task for gated edges, appended to the downstream node's head task for
plain Success edges). This script rebuilds those sets from the YAML and diffs them with
``ssis/orchestration-plan.json``; it also checks that the wiring matches the markers (a plain
Success edge is a ``depends_on`` of the downstream head on the upstream terminal, a gated edge is a
``condition_task`` whose ``run_if`` carries the edge value). Exit 1 on any difference.

PyYAML is optional: without it the generator's own model is re-rendered and compared textually,
which is the same guarantee ``build_jobs_from_plan.py --check`` gives.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

HERE = Path(__file__).resolve().parent
ORCH_DIR = HERE.parent
REPO = ORCH_DIR.parent.parent
sys.path.insert(0, str(HERE))
import build_jobs_from_plan as gen  # noqa: E402

NODE = re.compile(r"plan-node: (?P<name>.+?) \| kind=(?P<kind>\w+)")
CHILD = re.compile(r"plan-child: (?P<phase>.+?)/(?P<package>\S+) project=(?P<project>\S+)")
EDGE = re.compile(r"plan-edge: (?P<from>.+?) -> (?P<to>.+?) \| value=(?P<value>\w+)(?: expression=(?P<expr>.*?))?(?= ;; |$)")


def loadYaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError:  # pragma: no cover
        return {}
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def planSets(root: Dict[str, Any]) -> Tuple[Set[Tuple[str, str]], Set[Tuple[str, str, str]], Set[Tuple[str, str, str, str]]]:
    nodes = {(n["name"], n["kind"]) for n in root["nodes"]}
    children = set()
    for n in root["nodes"]:
        for c in n.get("children", []):
            children.add((n["name"], c["package"], c["project"]))
        if n["kind"] == "package":
            children.add((n["name"], n["package"], n["project"]))
    edges = {(e["from"], e["to"], e.get("value", "Success"), e.get("expression") or "") for e in root["edges"]}
    return nodes, children, edges


def yamlSets(job: Dict[str, Any]) -> Tuple[Set[Tuple[str, str]], Set[Tuple[str, str, str]], Set[Tuple[str, str, str, str]], List[str]]:
    nodes: Set[Tuple[str, str]] = set()
    children: Set[Tuple[str, str, str]] = set()
    edges: Set[Tuple[str, str, str, str]] = set()
    problems: List[str] = []
    tasks = {t["task_key"]: t for t in job["tasks"]}
    nodeTasks: Dict[str, Dict[str, str]] = {}
    for t in job["tasks"]:
        desc = t.get("description", "")
        m = NODE.search(desc)
        if m:
            nodes.add((m.group("name"), m.group("kind")))
            role = "end" if "role=end" in desc else "start" if "role=start" in desc else "single"
            nodeTasks.setdefault(m.group("name"), {})[role] = t["task_key"]
        for c in CHILD.finditer(desc):
            children.add((c.group("phase"), c.group("package"), c.group("project")))
        for e in EDGE.finditer(desc):
            edges.add((e.group("from"), e.group("to"), e.group("value"), e.group("expr") or ""))
    terminal = {n: r.get("end") or r["single"] for n, r in nodeTasks.items()}
    head = {n: r.get("start") or r["single"] for n, r in nodeTasks.items()}
    for t in job["tasks"]:
        desc = t.get("description", "")
        for e in EDGE.finditer(desc):
            src, dst, value, expr = e.group("from"), e.group("to"), e.group("value"), e.group("expr") or ""
            deps = {d["task_key"]: d for d in t.get("depends_on", [])}
            if "condition_task" in t:
                if terminal.get(src) not in deps:
                    problems.append(f"gate {t['task_key']} does not depend on terminal of '{src}'")
                want = {"Success": None, "Failure": "ALL_FAILED", "Completion": "ALL_DONE"}[value]
                if t.get("run_if") != want:
                    problems.append(f"gate {t['task_key']} run_if={t.get('run_if')} for value {value}")
                left = t["condition_task"]["left"]
                if expr and left != "{{tasks.%s.values.%s}}" % (terminal[src], re.search(r"edge_\d+", left).group(0)):
                    problems.append(f"gate {t['task_key']} condition does not read the upstream edge value")
                downstream = tasks[head[dst]]
                if not any(d["task_key"] == t["task_key"] and d.get("outcome") == "true" for d in downstream.get("depends_on", [])):
                    problems.append(f"head of '{dst}' does not depend on gate {t['task_key']} outcome true")
            else:
                if t["task_key"] != head.get(dst):
                    problems.append(f"plain edge marker for '{dst}' is on {t['task_key']}, not its head task")
                if terminal.get(src) not in deps:
                    problems.append(f"head of '{dst}' does not depend on terminal of '{src}' ({value} edge)")
        if "condition_task" not in t and "notebook_task" in t and len(t.get("depends_on", [])) > 1 and t.get("run_if") != "AT_LEAST_ONE_SUCCESS":
            if not t["task_key"].endswith("__end") and t["task_key"] != "zz_close_batch_if_open":
                problems.append(f"{t['task_key']} has several incoming edges but run_if={t.get('run_if')}")
    return nodes, children, edges, problems


def main() -> int:
    plan = gen.loadPlan()
    failures = 0
    for root in plan["roots"]:
        path = gen.RESOURCES_DIR / f"{root['root']}.yml"
        if not path.exists():
            print(f"{root['root']}: missing {path}")
            failures += 1
            continue
        doc = loadYaml(path)
        if not doc:
            if path.read_text(encoding="utf-8") != gen.render(root):
                print(f"{root['root']}: generated YAML is stale (PyYAML unavailable, textual comparison)")
                failures += 1
            else:
                print(f"{root['root']}: OK (textual)")
            continue
        job = next(iter(doc["resources"]["jobs"].values()))
        pNodes, pChildren, pEdges = planSets(root)
        yNodes, yChildren, yEdges, problems = yamlSets(job)
        for label, p, y in (("nodes", pNodes, yNodes), ("children", pChildren, yChildren), ("edges", pEdges, yEdges)):
            if p != y:
                problems.append(f"{label}: missing in YAML {sorted(p - y)[:5]} / extra in YAML {sorted(y - p)[:5]}")
        keys = [t["task_key"] for t in job["tasks"]]
        if len(keys) != len(set(keys)):
            problems.append("duplicate task keys")
        if any(len(k) > 100 for k in keys):
            problems.append("task key longer than 100 characters")
        status = "OK" if not problems else "FAIL"
        print(f"{root['root']}: {status} nodes={len(pNodes)} children={len(pChildren)} edges={len(pEdges)} tasks={len(keys)}")
        for p in problems:
            print(f"  - {p}")
        failures += bool(problems)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
