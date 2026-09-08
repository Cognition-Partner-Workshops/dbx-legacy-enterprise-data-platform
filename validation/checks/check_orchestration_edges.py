#!/usr/bin/env python3
"""Orchestration edge report and the project-reference invariant.

Two mechanisms invoke a child package, and which one is legal depends only on
where the child is deployed:

  * inside one project, an Execute Package Task with ``UseProjectReference``
    resolves the child by name in the executing .ispac;
  * across projects it cannot resolve at all, so the edge is carried by
    ``ssis/orchestration-plan.json`` and executed by
    ``deployment/ssis/Invoke-EstateOrchestration.ps1``, one dtexec per package
    against the .ispac that holds it.

This check reads the packages on disk and the generated plan, reports the edge
population, and fails when the two disagree with the canonical package ->
project ownership in config/estate-catalog.yaml.

Usage:
    python3 validation/checks/check_orchestration_edges.py [--json]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import xml.etree.ElementTree as ET

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "ssisgen"))

import orchestration  # noqa: E402  (path set above)

DTS_NS = "www.microsoft.com/SqlServer/Dts"


def packages_on_disk():
    """package name -> (repository-relative path, directory project name)."""
    found = {}
    ssis_root = os.path.join(REPO_ROOT, "ssis")
    for dirpath, dirnames, filenames in os.walk(ssis_root):
        dirnames[:] = [d for d in dirnames if d not in ("obj", "bin", "__pycache__")]
        project = None
        for filename in os.listdir(dirpath):
            if filename.endswith(".dtproj"):
                project = filename[: -len(".dtproj")]
        for filename in filenames:
            if filename.endswith(".dtsx"):
                found[filename[: -len(".dtsx")]] = (
                    os.path.relpath(os.path.join(dirpath, filename), REPO_ROOT).replace(os.sep, "/"),
                    project,
                )
    return found


def project_reference_edges(disk):
    """(parent, child, uses_project_reference) for every Execute Package Task."""
    edges = []
    for parent, (rel, _project) in sorted(disk.items()):
        root = ET.parse(os.path.join(REPO_ROOT, rel)).getroot()
        for task in root.iter("ExecutePackageTask"):
            name = task.find("PackageName")
            if name is None:
                continue
            child = (name.text or "").strip()
            if child.endswith(".dtsx"):
                child = child[: -len(".dtsx")]
            reference = task.find("UseProjectReference")
            uses = (reference is not None and (reference.text or "").strip().lower() == "true")
            edges.append((parent, child, uses))
    return edges


def reachable(roots, adjacency):
    seen = set()
    stack = list(roots)
    while stack:
        node = stack.pop()
        for child in adjacency.get(node, ()):
            if child not in seen:
                seen.add(child)
                stack.append(child)
    return seen


def find_cycles(adjacency):
    """Every node that sits on a cycle, by iterative colouring."""
    WHITE, GREY, BLACK = 0, 1, 2
    colour = {}
    on_cycle = set()
    for start in list(adjacency):
        if colour.get(start, WHITE) != WHITE:
            continue
        stack = [(start, iter(adjacency.get(start, ())))]
        colour[start] = GREY
        path = [start]
        while stack:
            node, children = stack[-1]
            advanced = False
            for child in children:
                state = colour.get(child, WHITE)
                if state == GREY:
                    on_cycle.update(path[path.index(child):])
                elif state == WHITE:
                    colour[child] = GREY
                    path.append(child)
                    stack.append((child, iter(adjacency.get(child, ()))))
                    advanced = True
                    break
            if not advanced:
                colour[node] = BLACK
                stack.pop()
                path.pop()
    return sorted(on_cycle)


def analyse():
    disk = packages_on_disk()
    owner = orchestration.project_by_package()
    plan = orchestration.load()
    failures = []

    in_package = project_reference_edges(disk)
    external = orchestration.child_edges(plan)

    adjacency = {}
    intra = []
    cross_as_reference = []
    for parent, child, uses in in_package:
        adjacency.setdefault(parent, []).append(child)
        if child not in owner:
            failures.append("%s: Execute Package Task references %r, which the catalog does not declare"
                            % (parent, child))
            continue
        if owner.get(parent) != owner[child]:
            if uses:
                cross_as_reference.append((parent, child))
                failures.append(
                    "%s -> %s: UseProjectReference crosses projects (%s -> %s); it cannot resolve "
                    "from the parent's .ispac" % (parent, child, owner.get(parent), owner[child]))
        else:
            intra.append((parent, child))

    # (root, package) rather than plan edges: a root reaches some children
    # through more than one plan node, and the runner executes each child once
    # per root, so this is the number of dtexec invocations a full estate run
    # makes for cross-project children.
    invocations = set()
    for root, node, package, project in external:
        adjacency.setdefault(root, []).append(package)
        invocations.add((root, package))
        if owner.get(package) != project:
            failures.append("plan %s/%s: child %s is recorded in %s but the catalog owns it in %s"
                            % (root, node, package, project, owner.get(package)))
        if package not in disk:
            failures.append("plan %s/%s: child %s has no .dtsx on disk" % (root, node, package))

    for package, (rel, project) in sorted(disk.items()):
        if owner.get(package) != project:
            failures.append("%s: sits in project %s, the catalog owns it in %s"
                            % (rel, project, owner.get(package)))

    roots = sorted({root["root"] for root in plan["roots"]})
    invoked = {child for _parent, child, _uses in in_package} | {edge[2] for edge in external}
    orphans = sorted(set(disk) - invoked - set(roots))
    reached = reachable(roots, adjacency) | set(roots)
    unreachable = sorted(set(disk) - reached - set(orphans))
    cycles = find_cycles(adjacency)
    if cycles:
        failures.append("orchestration graph is cyclic through: %s" % ", ".join(cycles))
    for package in unreachable:
        failures.append("%s is neither a root nor reached by one" % package)

    return {
        "packages": len(disk),
        "projects": len({project for _rel, project in disk.values()}),
        "edges_total": len(in_package) + len(external),
        "edges_intra_project": len(intra),
        "edges_cross_project": len(external),
        "edges_cross_project_as_reference": len(cross_as_reference),
        "roots": roots,
        "independent_entry_points": orphans,
        "packages_reachable_from_roots": len(reached),
        "packages_unreachable": unreachable,
        "cycles": cycles,
        "runner_package_invocations": len(invocations),
        "failures": failures,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--json", action="store_true", help="emit machine-readable output")
    args = parser.parse_args()

    report = analyse()
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for key in ("packages", "projects", "edges_total", "edges_intra_project",
                    "edges_cross_project", "edges_cross_project_as_reference",
                    "packages_reachable_from_roots", "runner_package_invocations"):
            print("%-38s %s" % (key, report[key]))
        print("%-38s %s" % ("roots", ", ".join(report["roots"])))
        print("%-38s %s" % ("independent_entry_points",
                            ", ".join(report["independent_entry_points"]) or "none"))
        print("%-38s %s" % ("packages_unreachable", ", ".join(report["packages_unreachable"]) or "none"))
        print("%-38s %s" % ("cycles", ", ".join(report["cycles"]) or "none"))
        print("")
        for failure in report["failures"]:
            print("FAIL  %s" % failure)
        print("%d failure(s)" % len(report["failures"]))
    return 1 if report["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
