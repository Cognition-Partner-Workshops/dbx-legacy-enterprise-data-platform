#!/usr/bin/env python3
"""Extract and analyse the SSIS package dependency graph.

Two edge kinds are extracted from the merged tree:

  * `execute`  - a parent package contains an Execute Package Task whose
                 <PackageName> element names a child .dtsx. These edges are
                 read out of the package XML itself, not from the catalog.
  * `data`     - one package writes an object that another package reads,
                 according to the `source_objects` / `target_objects`
                 declarations in config/estate-catalog.yaml. Wildcards such as
                 `stg.*` are expanded against the catalog inventory.

From the combined graph the script reports cycles (Tarjan strongly connected
components), packages no master ever executes, execute-edges pointing at a
package that does not exist on disk, the depth of each orchestration tree and
the longest execute chain.

Cycles and unresolved execute-edges are errors: an SSIS master with a cyclic
child graph cannot terminate, and an Execute Package Task pointing at a
missing file cannot resolve. Data-edge cycles across load phases are reported
as warnings because a legacy estate legitimately contains feedback loops that
are broken by scheduling rather than by structure.

The graph is emitted as JSON (--json), Graphviz DOT (--dot) or Mermaid
(--mermaid) so it can be pasted into docs/dependency-maps/.

Usage:
    python3 validation/checks/extract_package_dependency_graph.py [--json|--dot|--mermaid]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict, deque

import estatelib as lib

CHECK = "package-graph"


def build_graph(packages, catalog_keys):
    """Return (execute_edges, data_edges) as lists of (parent, child)."""
    execute_edges = []
    unresolved = []
    for package in sorted(packages.values(), key=lambda p: p.name):
        for child in package.child_packages():
            if child in packages:
                execute_edges.append((package.name, child))
            else:
                unresolved.append((package.name, child))

    producers, consumers = lib.package_index_by_object(packages, catalog_keys)
    data_edges = set()
    for key, writers in producers.items():
        for reader in consumers.get(key, []):
            for writer in writers:
                if writer != reader:
                    data_edges.add((writer, reader))
    return execute_edges, sorted(data_edges), unresolved


def find_cycles(nodes, edges):
    """Iterative Tarjan SCC; returns components of size > 1 plus self-loops."""
    adjacency = defaultdict(list)
    for parent, child in edges:
        adjacency[parent].append(child)

    index = {}
    low = {}
    on_stack = set()
    stack = []
    counter = [0]
    components = []

    for root in nodes:
        if root in index:
            continue
        work = [(root, iter(adjacency[root]))]
        index[root] = low[root] = counter[0]
        counter[0] += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            node, children = work[-1]
            advanced = False
            for child in children:
                if child not in index:
                    index[child] = low[child] = counter[0]
                    counter[0] += 1
                    stack.append(child)
                    on_stack.add(child)
                    work.append((child, iter(adjacency[child])))
                    advanced = True
                    break
                if child in on_stack:
                    low[node] = min(low[node], index[child])
            if advanced:
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
            if low[node] == index[node]:
                component = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    component.append(member)
                    if member == node:
                        break
                if len(component) > 1:
                    components.append(sorted(component))
    self_loops = sorted({p for p, c in edges if p == c})
    return components, self_loops


def longest_chain(nodes, edges):
    """Longest path length in a DAG, plus one witness path."""
    adjacency = defaultdict(list)
    indegree = {node: 0 for node in nodes}
    for parent, child in edges:
        adjacency[parent].append(child)
        indegree[child] = indegree.get(child, 0) + 1
    order = deque(node for node, degree in indegree.items() if degree == 0)
    topo = []
    remaining = dict(indegree)
    while order:
        node = order.popleft()
        topo.append(node)
        for child in adjacency[node]:
            remaining[child] -= 1
            if remaining[child] == 0:
                order.append(child)
    if len(topo) != len(indegree):
        return 0, []  # cyclic: no meaningful longest path
    best = {node: (1, [node]) for node in topo}
    for node in topo:
        length, path = best[node]
        for child in adjacency[node]:
            if length + 1 > best[child][0]:
                best[child] = (length + 1, path + [child])
    if not best:
        return 0, []
    winner = max(best.values(), key=lambda item: item[0])
    return winner[0], winner[1]


def run(args):
    report = lib.Report("extract_package_dependency_graph")
    catalog = lib.load_catalog()
    objects = lib.catalog_objects(catalog)
    packages = lib.load_packages(catalog)
    names = sorted(packages)

    execute_edges, data_edges, unresolved = build_graph(packages, set(objects))

    for parent, child in unresolved:
        report.error(CHECK, parent,
                     "Execute Package Task references '%s.dtsx', which is not present in ssis/"
                     % child)

    exec_cycles, exec_self = find_cycles(names, execute_edges)
    for component in exec_cycles:
        report.error(CHECK, component[0],
                     "execute-cycle across %d packages: %s"
                     % (len(component), " -> ".join(component)))
    for node in exec_self:
        report.error(CHECK, node, "package executes itself")

    data_cycles, _ = find_cycles(names, data_edges)
    for component in data_cycles:
        report.warn(CHECK, component[0],
                    "data-flow cycle across %d packages (broken by scheduling, not structure): %s"
                    % (len(component), ", ".join(component[:6])))

    masters = sorted(name for name in names if packages[name].is_master)
    reachable = set()
    frontier = list(masters)
    adjacency = defaultdict(list)
    for parent, child in execute_edges:
        adjacency[parent].append(child)
    while frontier:
        node = frontier.pop()
        for child in adjacency[node]:
            if child not in reachable:
                reachable.add(child)
                frontier.append(child)

    unscheduled = [name for name in names
                   if name not in reachable and name not in masters]
    for name in unscheduled:
        report.warn(CHECK, name,
                    "no master package executes it; it can only run by hand or from an "
                    "Agent job step")

    declared_parents = {name: packages[name].parent for name in names}
    for name in names:
        declared = declared_parents.get(name)
        if not declared:
            continue
        actual = [p for p, c in execute_edges if c == name]
        if declared not in actual:
            report.warn(CHECK, name,
                        "catalog declares parent '%s' but no Execute Package Task in that "
                        "package references it" % declared)

    depth, witness = longest_chain(names, execute_edges)

    report.count("packages", len(names))
    report.count("execute_edges", len(execute_edges))
    report.count("data_edges", len(data_edges))
    report.count("master_packages", len(masters))
    report.count("packages_reachable_from_a_master", len(reachable))
    report.count("packages_no_master_executes", len(unscheduled))
    report.count("execute_cycles", len(exec_cycles) + len(exec_self))
    report.count("data_cycles", len(data_cycles))
    report.count("longest_execute_chain", depth)
    report.detail("masters", masters)
    report.detail("longest_execute_chain_path", witness)
    report.detail("packages_no_master_executes", unscheduled)

    if args.dot or args.mermaid:
        emit_graph(args, packages, execute_edges, masters)
        return 0

    if args.graph_json:
        payload = {
            "packages": {
                name: {
                    "folder": packages[name].folder,
                    "domain": packages[name].domain,
                    "load_type": packages[name].load_type,
                    "parent": packages[name].parent,
                }
                for name in names
            },
            "execute_edges": [list(edge) for edge in execute_edges],
            "data_edges": [list(edge) for edge in data_edges],
            "static_only": True,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def emit_graph(args, packages, execute_edges, masters):
    if args.dot:
        print("digraph estate {")
        print('    rankdir=LR;')
        for master in masters:
            print('    "%s" [shape=box, style=bold];' % master)
        for parent, child in execute_edges:
            print('    "%s" -> "%s";' % (parent, child))
        print("}")
    else:
        print("graph LR")
        for parent, child in execute_edges:
            print("    %s --> %s" % (parent.replace("-", "_"), child.replace("-", "_")))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    parser.add_argument("--dot", action="store_true", help="emit Graphviz DOT of execute edges")
    parser.add_argument("--mermaid", action="store_true", help="emit Mermaid of execute edges")
    parser.add_argument("--graph-json", action="store_true",
                        help="emit the full graph (nodes plus both edge kinds) as JSON")
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
