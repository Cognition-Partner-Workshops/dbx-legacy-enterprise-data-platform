#!/usr/bin/env python3
"""Regenerate the machine-readable estate inventories under docs/inventories/.

Everything here is derived from config/estate-catalog.yaml and from the files
actually checked in, so the inventories cannot drift from the estate. Nothing in
this script connects to anything; it reads the repository and writes five CSVs:

    artifacts.csv               every artifact file, classified by layer
    ssis-packages.csv           one row per SSIS package
    sql-objects.csv             every SQL object the estate creates
    source-target-map.csv       package-level source -> target lineage
    package-dependencies.csv    master -> child execution edges

Run:  python3 tools/inventory/build_inventories.py
"""

from __future__ import annotations

import csv
import os
import re
import sys

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
CATALOG_PATH = os.path.join(REPO_ROOT, "config", "estate-catalog.yaml")
OUT_DIR = os.path.join(REPO_ROOT, "docs", "inventories")

# The Microsoft WideWorldImporters sample this estate was grown from. Its files
# are inventoried as 'base-sample' rather than being claimed as estate work.
SAMPLE_DIRS = (
    "wwi-ssdt", "wwi-dw-ssdt", "wwi-ssis", "wwi-app", "wwi-azure-functions",
    "wwi-ssasmd", "power-bi-dashboards", "sample-scripts", "workload-drivers",
)

SKIP_DIRS = (".git", "__pycache__", ".venv", "node_modules")

LAYERS = (
    ("oracle/", "oracle"),
    ("sqlserver/control/", "etl-control"),
    ("sqlserver/oltp/", "sqlserver-oltp"),
    ("sqlserver/staging/", "sqlserver-staging"),
    ("sqlserver/reference/", "sqlserver-reference"),
    ("sqlserver/warehouse/", "sqlserver-dw"),
    ("sqlserver/procedures/", "sqlserver-dw"),
    ("sqlserver/views/", "sqlserver-dw"),
    ("sqlserver/agent/", "sql-agent"),
    ("sqlserver/security/", "sqlserver-security"),
    ("ssis/", "ssis"),
    ("generators/", "data-generators"),
    ("deploy/", "deployment"),
    ("tools/", "build-tooling"),
    ("deployment/", "deployment"),
    ("infrastructure/", "infrastructure"),
    ("validation/", "validation"),
    ("config/", "configuration"),
    ("docs/", "documentation"),
)

CREATE_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?"
    r"(TABLE|VIEW|PROCEDURE|PROC|FUNCTION|SEQUENCE|SYNONYM|TYPE|TRIGGER|PACKAGE(?:\s+BODY)?|INDEX)\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?"
    # An object name is a dotted list of parts, each either bracketed/quoted
    # (so warehouse names with spaces survive) or a bare identifier.
    r"((?:\[[^\]]+\]|\"[^\"]+\"|\w+)(?:\s*\.\s*(?:\[[^\]]+\]|\"[^\"]+\"|\w+))*)",
    re.IGNORECASE,
)


def load_catalog():
    with open(CATALOG_PATH, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def classify(relative):
    head = relative.split(os.sep)[0]
    if head in SAMPLE_DIRS:
        return "base-sample"
    for prefix, layer in LAYERS:
        if relative.replace(os.sep, "/").startswith(prefix):
            return layer
    return "repository"


def walk_files():
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for filename in sorted(filenames):
            path = os.path.join(dirpath, filename)
            yield path, os.path.relpath(path, REPO_ROOT)


def write_csv(name, header, rows):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, name)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    print("%s (%d rows)" % (os.path.relpath(path, REPO_ROOT), len(rows)))


def owner_of(relative, work_packages):
    normalised = relative.replace(os.sep, "/")
    best = ("", "")
    for wp_id, wp in sorted(work_packages.items()):
        for owned in wp.get("owns", []) or []:
            if normalised.startswith(owned.rstrip("/") + "/") and len(owned) > len(best[1]):
                best = (wp_id, owned)
    return best[0]


def build_artifacts(catalog):
    work_packages = catalog.get("work_packages", {}) or {}
    rows = []
    for path, relative in walk_files():
        extension = os.path.splitext(relative)[1].lower().lstrip(".")
        if extension in ("pyc",):
            continue
        try:
            lines = sum(1 for _ in open(path, encoding="utf-8", errors="ignore"))
        except OSError:
            lines = 0
        rows.append([
            relative.replace(os.sep, "/"),
            classify(relative),
            extension or "none",
            lines,
            os.path.getsize(path),
            owner_of(relative, work_packages),
        ])
    rows.sort()
    write_csv("artifacts.csv",
              ["path", "layer", "extension", "line_count", "size_bytes", "work_package"],
              rows)


def build_ssis_packages(catalog):
    rows = []
    for pkg in catalog["ssis"]["packages"]:
        relative = "ssis/%s/%s.dtsx" % (pkg["folder"], pkg["package"])
        rows.append([
            pkg["package"],
            pkg["project"],
            pkg["folder"],
            pkg.get("domain", ""),
            pkg.get("load_type", ""),
            pkg.get("criticality", ""),
            pkg.get("source_system", ""),
            pkg.get("target_system", ""),
            pkg.get("parent") or "",
            "yes" if os.path.exists(os.path.join(REPO_ROOT, relative)) else "no",
            relative,
            (pkg.get("notes") or "").replace("\n", " "),
        ])
    rows.sort()
    write_csv("ssis-packages.csv",
              ["package", "project", "folder", "domain", "load_type", "criticality",
               "source_system", "target_system", "parent_package", "file_present",
               "path", "notes"],
              rows)


def build_sql_objects():
    rows = []
    for path, relative in walk_files():
        if not relative.lower().endswith(".sql"):
            continue
        if classify(relative) == "base-sample":
            continue
        body = open(path, encoding="utf-8", errors="ignore").read()
        # Strip block and line comments so commented-out DDL is not inventoried.
        body = re.sub(r"/\*.*?\*/", " ", body, flags=re.DOTALL)
        body = re.sub(r"--[^\n]*", " ", body)
        for match in CREATE_RE.finditer(body):
            kind = match.group(1).upper().replace("PROC", "PROCEDURE").replace("PROCEDUREEDURE", "PROCEDURE")
            name = match.group(2).replace("[", "").replace("]", "").replace('"', "")
            if kind == "INDEX":
                continue
            schema, _, bare = name.rpartition(".")
            rows.append([
                schema or "dbo",
                bare,
                kind,
                classify(relative),
                relative.replace(os.sep, "/"),
            ])
    rows.sort()
    write_csv("sql-objects.csv",
              ["schema", "object_name", "object_type", "layer", "path"],
              rows)


def build_source_target_map(catalog):
    rows = []
    for pkg in catalog["ssis"]["packages"]:
        sources = pkg.get("source_objects") or [""]
        targets = pkg.get("target_objects") or [""]
        for source in sources:
            for target in targets:
                if not source and not target:
                    continue
                rows.append([
                    pkg["package"],
                    pkg["folder"],
                    pkg.get("source_system", ""),
                    source,
                    pkg.get("target_system", ""),
                    target,
                    pkg.get("load_type", ""),
                ])
    rows.sort()
    write_csv("source-target-map.csv",
              ["package", "folder", "source_system", "source_object",
               "target_system", "target_object", "load_type"],
              rows)


def build_package_dependencies(catalog):
    rows = []
    for pkg in catalog["ssis"]["packages"]:
        parent = pkg.get("parent")
        if parent:
            rows.append([parent, pkg["package"], "executes", pkg.get("criticality", "")])
    # Data lineage edges: a package that writes an object another package reads.
    writers = {}
    for pkg in catalog["ssis"]["packages"]:
        for target in pkg.get("target_objects") or []:
            writers.setdefault(target, []).append(pkg["package"])
    for pkg in catalog["ssis"]["packages"]:
        for source in pkg.get("source_objects") or []:
            for upstream in writers.get(source, []):
                if upstream != pkg["package"]:
                    rows.append([upstream, pkg["package"], "data:" + source,
                                 pkg.get("criticality", "")])
    rows = sorted(set(tuple(r) for r in rows))
    write_csv("package-dependencies.csv",
              ["upstream_package", "downstream_package", "edge_type", "criticality"],
              [list(r) for r in rows])


def main():
    catalog = load_catalog()
    build_artifacts(catalog)
    build_ssis_packages(catalog)
    build_sql_objects()
    build_source_target_map(catalog)
    build_package_dependencies(catalog)
    return 0


if __name__ == "__main__":
    sys.exit(main())
