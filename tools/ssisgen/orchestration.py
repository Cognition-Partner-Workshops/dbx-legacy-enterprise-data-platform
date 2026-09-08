"""The estate's orchestration plan: what runs, in what order, from which .ispac.

The 205 packages are deployed as 17 project-deployment .ispac files. An Execute
Package Task with ``UseProjectReference`` resolves the child by name inside the
project that is executing, so a master in ``WWI_Orchestration`` cannot reach a
child that lives in ``WWI_OracleExtract`` - the reference is unresolvable at run
time no matter how the estate is deployed, and collapsing the 17 projects into
one is a change of architecture, not a fix.

So the master packages keep the control-framework skeleton they have always had
(open the batch, open and close a step per phase, reconcile, close the batch)
and every cross-project child invocation is recorded here instead. The plan is
emitted next to the packages as ``ssis/orchestration-plan.json`` and is what
``deployment/ssis/Invoke-EstateOrchestration.ps1`` walks: per root, an ordered
node list and the precedence edges between the nodes, with each child carrying
the project whose .ispac holds it.

A node is one of:

  ``batch_start``   open an etl.Batch and hold its id for the rest of the run
  ``phase``         a batch step wrapping an ordered list of child packages
  ``reconcile``     row-count reconciliation over the batch
  ``batch_end``     close the batch, optionally forcing a status
  ``control``       an in-package task (expression, ad-hoc SQL) that orders the
                    graph but has no external effect the runner reproduces

Edges carry the SSIS precedence value (Success / Failure / Completion) and the
constraint expression where the master gates a path.
"""

from __future__ import annotations

import json
import os

PLAN_FILENAME = "orchestration-plan.json"
PLAN_VERSION = 1

_CATALOG_CACHE = {}


def repo_root():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def project_by_package():
    """package name -> owning SSIS project, from config/estate-catalog.yaml."""
    if not _CATALOG_CACHE:
        import yaml

        path = os.path.join(repo_root(), "config", "estate-catalog.yaml")
        with open(path) as handle:
            catalog = yaml.safe_load(handle)
        _CATALOG_CACHE.update(
            {spec["package"]: spec["project"] for spec in catalog["ssis"]["packages"]})
    return _CATALOG_CACHE


def project_of(package_name):
    try:
        return project_by_package()[package_name]
    except KeyError:
        raise KeyError("package %r is not declared in config/estate-catalog.yaml" % package_name)


class RootPlan:
    """The externally executed half of one master package."""

    def __init__(self, root, project, description=""):
        self.root = root
        self.project = project
        self.description = description
        self.nodes = []
        self.edges = []
        self.variables = {}
        self.parameters = {}
        self._by_name = {}

    # -- declaration --------------------------------------------------------

    def node(self, name, kind, **attributes):
        if name in self._by_name:
            raise ValueError("%s: duplicate plan node %r" % (self.root, name))
        entry = {"name": name, "kind": kind}
        entry.update({key: value for key, value in attributes.items() if value is not None})
        self.nodes.append(entry)
        self._by_name[name] = entry
        return entry

    def child(self, phase_name, package, parameters):
        entry = self._by_name[phase_name]
        entry.setdefault("children", []).append({
            "package": package,
            "project": project_of(package),
            "parameters": dict(parameters),
        })

    def edge(self, source, target, value="Success", expression=None):
        for name in (source, target):
            if name not in self._by_name:
                raise ValueError("%s: precedence edge references unknown node %r" % (self.root, name))
        edge = {"from": source, "to": target, "value": value}
        if expression:
            edge["expression"] = expression
        self.edges.append(edge)

    # -- emission -----------------------------------------------------------

    def to_dict(self):
        return {
            "root": self.root,
            "project": self.project,
            "description": self.description,
            "parameters": self.parameters,
            "variables": self.variables,
            "nodes": self.nodes,
            "edges": self.edges,
        }


class Plan:
    """Every root's external orchestration, written as one generated file."""

    def __init__(self):
        self.roots = []

    def add(self, root_plan):
        self.roots.append(root_plan)
        return root_plan

    def to_dict(self):
        return {
            "version": PLAN_VERSION,
            "generated_by": "ssis/00_orchestration/build_orchestration_packages.py",
            "note": ("Cross-project Execute Package Task edges cannot be expressed as project "
                     "references; deployment/ssis/Invoke-EstateOrchestration.ps1 executes them "
                     "from this plan, one dtexec per package against its own .ispac."),
            "roots": [root.to_dict() for root in self.roots],
        }

    def write(self, path):
        with open(path, "w") as handle:
            json.dump(self.to_dict(), handle, indent=2, sort_keys=False)
            handle.write("\n")
        return path


def load(path=None):
    path = path or os.path.join(repo_root(), "ssis", PLAN_FILENAME)
    with open(path) as handle:
        return json.load(handle)


def child_edges(plan):
    """(root, node, package, project) for every externally executed child.

    A ``package`` node is a child the master invokes on its own; every other
    node carries the children of the phase it wraps.
    """
    out = []
    for root in plan["roots"]:
        for node in root["nodes"]:
            if node["kind"] == "package" and not node.get("in_package"):
                out.append((root["root"], node["name"], node["package"], node["project"]))
            for child in node.get("children", []):
                out.append((root["root"], node["name"], child["package"], child["project"]))
    return out
