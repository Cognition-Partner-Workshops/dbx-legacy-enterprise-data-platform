#!/usr/bin/env python3
"""Shared loading and parsing helpers for the deeper static checks.

Everything here reads files on disk and config/estate-catalog.yaml. Nothing
connects to a database, and nothing infers runtime behaviour: when a check says
"package X calls etl.usp_LogRowCount" it means that string appears in the
package XML, not that the call was observed.
"""

from __future__ import annotations

import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CATALOG_PATH = os.path.join(REPO_ROOT, "config", "estate-catalog.yaml")

DTS_NS = "www.microsoft.com/SqlServer/Dts"

# Pre-existing Microsoft sample content, excluded from every estate check.
LEGACY_SAMPLE_DIRS = (
    "wwi-ssdt", "wwi-dw-ssdt", "wwi-ssis", "wwi-app", "wwi-azure-functions",
    "wwi-ssasmd", "power-bi-dashboards", "sample-scripts", "workload-drivers",
)

# Folders that hold estate SQL, by layer. Used to classify what a SQL file is.
SQL_LAYERS = (
    ("oracle/tables", "oracle-table"),
    ("oracle/views", "oracle-view"),
    ("oracle/packages", "oracle-package"),
    ("oracle/procedures", "oracle-procedure"),
    ("oracle/functions", "oracle-function"),
    ("oracle/reference", "oracle-reference"),
    ("oracle/seed", "oracle-seed"),
    ("oracle/ddl", "oracle-ddl"),
    ("sqlserver/control", "control"),
    ("sqlserver/oltp", "oltp"),
    ("sqlserver/staging", "staging"),
    ("sqlserver/reference", "reference"),
    ("sqlserver/warehouse/dimensions", "dw-dimension"),
    ("sqlserver/warehouse/facts", "dw-fact"),
    ("sqlserver/warehouse/aggregates", "dw-aggregate"),
    ("sqlserver/procedures/dimensions", "dw-dimension-proc"),
    ("sqlserver/procedures/facts", "dw-fact-proc"),
    ("sqlserver/views", "dw-report-view"),
    ("sqlserver/agent", "agent"),
    ("sqlserver/security", "security"),
)

ETL_PROC_RE = re.compile(r"etl\.usp_([A-Za-z0-9_]+)")

# Schemas that ship with the original Microsoft WideWorldImporters sample. The
# estate layers on top of them, so a reference to one is legitimate even though
# the object is not declared in config/estate-catalog.yaml.
BASE_SAMPLE_SCHEMAS = frozenset({
    "APPLICATION", "SALES", "PURCHASING", "WAREHOUSE", "WEBSITE",
    "DATALOADSIMULATION", "REPORTS", "POWERBI", "SEQUENCES",
})

CREATE_TABLE_RE = re.compile(
    r"CREATE\s+(?:OR\s+(?:REPLACE|ALTER)\s+)?(?:TABLE|VIEW)\s+([\[\]\w\.\"]+)", re.I)
CREATE_OBJECT_RE = re.compile(
    r"^\s*CREATE\s+(?:OR\s+(?:REPLACE|ALTER)\s+)?(PROCEDURE|FUNCTION|VIEW|TABLE)\s+"
    r"([\[\]\w\. \-]+?)(?:\s|\(|$)",
    re.I | re.M,
)


class Finding:
    """One check result. `level` is 'error' or 'warning'."""

    def __init__(self, level, check, subject, message):
        self.level = level
        self.check = check
        self.subject = subject
        self.message = message

    def as_dict(self):
        return {
            "level": self.level,
            "check": self.check,
            "subject": self.subject,
            "message": self.message,
        }

    def __str__(self):
        return "%-7s [%s] %s: %s" % (self.level.upper(), self.check, self.subject, self.message)


class Report:
    """Collected findings plus descriptive counters for one check script."""

    def __init__(self, name):
        self.name = name
        self.findings = []
        self.counts = {}
        self.details = {}

    def error(self, check, subject, message):
        self.findings.append(Finding("error", check, subject, message))

    def warn(self, check, subject, message):
        self.findings.append(Finding("warning", check, subject, message))

    def count(self, key, value):
        self.counts[key] = value

    def detail(self, key, value):
        self.details[key] = value

    @property
    def errors(self):
        return [f for f in self.findings if f.level == "error"]

    @property
    def warnings(self):
        return [f for f in self.findings if f.level == "warning"]

    def emit(self, as_json=False, strict=False, show_warnings=True):
        if as_json:
            payload = {
                "check_script": self.name,
                "counts": self.counts,
                "details": self.details,
                "findings": [f.as_dict() for f in self.findings],
                "error_count": len(self.errors),
                "warning_count": len(self.warnings),
                "static_only": True,
            }
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            for finding in self.findings:
                if finding.level == "warning" and not show_warnings:
                    continue
                print(finding)
            print()
            for key in sorted(self.counts):
                print("%-40s %s" % (key, self.counts[key]))
            print()
            print("%d error(s), %d warning(s)" % (len(self.errors), len(self.warnings)))
            print("Static analysis only - nothing here was executed against a live system.")
        failed = bool(self.errors) or (strict and bool(self.warnings))
        return 1 if failed else 0


def load_catalog():
    try:
        import yaml
    except ImportError:
        sys.stderr.write("PyYAML is required: pip install pyyaml\n")
        raise SystemExit(2)
    with open(CATALOG_PATH) as handle:
        return yaml.safe_load(handle)


def walk_files(extensions=None, prefixes=None):
    """Yield (relative_path, absolute_path) for estate files, skipping the sample."""
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        dirnames[:] = [d for d in dirnames
                       if d not in (".git", "__pycache__", ".venv", "node_modules", "output")]
        rel_dir = os.path.relpath(dirpath, REPO_ROOT)
        rel_dir = "" if rel_dir == "." else rel_dir.replace(os.sep, "/")
        if rel_dir.split("/")[0] in LEGACY_SAMPLE_DIRS:
            continue
        for filename in sorted(filenames):
            rel = "%s/%s" % (rel_dir, filename) if rel_dir else filename
            if prefixes and not any(rel.startswith(p) for p in prefixes):
                continue
            if extensions and not filename.lower().endswith(extensions):
                continue
            yield rel, os.path.join(dirpath, filename)


def read_text(path):
    with open(path, errors="replace") as handle:
        return handle.read()


class Package:
    """A generated .dtsx as read from disk, joined to its catalog declaration."""

    def __init__(self, name, rel_path, text, spec):
        self.name = name
        self.path = rel_path
        self.text = text
        self.spec = spec or {}

    @property
    def folder(self):
        return self.path.split("/")[1] if self.path.startswith("ssis/") else ""

    @property
    def load_type(self):
        return self.spec.get("load_type", "unknown")

    @property
    def domain(self):
        return self.spec.get("domain", "unknown")

    @property
    def parent(self):
        return self.spec.get("parent")

    @property
    def source_objects(self):
        return list(self.spec.get("source_objects") or [])

    @property
    def target_objects(self):
        return list(self.spec.get("target_objects") or [])

    @property
    def is_master(self):
        return self.name.startswith("Master_")

    def etl_procedures(self):
        return set(ETL_PROC_RE.findall(self.text))

    def child_packages(self):
        children = []
        try:
            root = ET.fromstring(self.text)
        except ET.ParseError:
            return children
        for node in root.iter("PackageName"):
            child = (node.text or "").strip()
            if child.endswith(".dtsx"):
                child = child[:-5]
            if child:
                children.append(child)
        return children


def load_packages(catalog):
    """Return {package_name: Package} for every .dtsx in ssis/."""
    declared = {p["package"]: p for p in catalog["ssis"]["packages"]}
    packages = {}
    for rel, full in walk_files((".dtsx",), prefixes=("ssis/",)):
        name = os.path.basename(rel)[:-5]
        packages[name] = Package(name, rel, read_text(full), declared.get(name))
    return packages


def load_sql_files():
    """Return [(rel_path, layer, text)] for every estate SQL file."""
    out = []
    for rel, full in walk_files((".sql",)):
        layer = None
        for prefix, label in SQL_LAYERS:
            if rel.startswith(prefix + "/"):
                layer = label
                break
        if layer is None:
            continue
        out.append((rel, layer, read_text(full)))
    return out


def created_objects(sql_files=None):
    """Normalised names of every table and view an estate SQL file creates."""
    created = {}
    for rel, _layer, text in (sql_files if sql_files is not None else load_sql_files()):
        for raw in CREATE_TABLE_RE.findall(text):
            key = normalise_object(raw)
            if key:
                created.setdefault(key, rel)
    return created


def normalise_object(name):
    """Fold an object reference to a comparable key.

    Catalog names, T-SQL bracket quoting and case all vary across the estate;
    comparisons are made on the upper-cased, bracket-stripped, space-collapsed
    form of `schema.object`.
    """
    if not name:
        return ""
    name = name.replace("[", "").replace("]", "").replace('"', "")
    name = re.sub(r"\s+", " ", name).strip()
    return name.upper()


def catalog_objects(catalog):
    """Every physical object the catalog declares, keyed by normalised name.

    The value is a dict with `layer`, `system` and the raw catalog name.
    """
    objects = {}

    def add(name, layer, system):
        objects[normalise_object(name)] = {"name": name, "layer": layer, "system": system}

    for schema in ("WWI_MDM", "WWI_PROC", "WWI_FIN", "WWI_REF", "WWI_AUDIT"):
        block = catalog["oracle"].get(schema) or {}
        for table in block.get("tables", []):
            add("%s.%s" % (schema, table), "oracle-table", "Oracle")
    for view in catalog["oracle"].get("views", []):
        add("%s.%s" % (view["schema"], view["name"]), "oracle-view", "Oracle")

    for schema, tables in (catalog["sqlserver_oltp"].get("new_tables") or {}).items():
        for table in tables:
            add("%s.%s" % (schema, table), "oltp-table", "SQL Server OLTP")
    for view in catalog["sqlserver_oltp"].get("views", []):
        add("%s.%s" % (view["schema"], view["name"]), "oltp-view", "SQL Server OLTP")

    for schema, tables in (catalog["sqlserver_staging"].get("tables") or {}).items():
        for table in tables:
            add("%s.%s" % (schema, table), "staging-%s" % schema, "SQL Server Staging")
    for view in catalog["sqlserver_staging"].get("views", []):
        add("%s.%s" % (view["schema"], view["name"]), "staging-view", "SQL Server Staging")

    for dim in catalog["sqlserver_dw"].get("dimensions", []):
        add("Dimension.%s" % dim["name"], "dw-dimension", "SQL Server DW")
    for fact in catalog["sqlserver_dw"].get("facts", []):
        add("Fact.%s" % fact["name"], "dw-fact", "SQL Server DW")
    for agg in catalog["sqlserver_dw"].get("aggregates", []):
        add("Aggregate.%s" % agg, "dw-aggregate", "SQL Server DW")
    for view in catalog["sqlserver_dw"].get("report_views", []):
        add("Report.%s" % view, "dw-report-view", "SQL Server DW")

    for table in catalog["etl_control"].get("tables", []):
        add("etl.%s" % table, "control-table", "ETL control")

    return objects


def expand_references(names, catalog_keys):
    """Expand wildcard catalog references such as `Dimension.*` to real objects."""
    expanded = set()
    for raw in names:
        key = normalise_object(raw)
        if key.endswith(".*"):
            prefix = key[:-1]
            expanded.update(k for k in catalog_keys if k.startswith(prefix))
        elif "*" in key:
            continue  # file-name glob (partner_sales_na_*.csv and friends)
        else:
            expanded.add(key)
    return expanded


def package_index_by_object(packages, catalog_keys, explicit_only=False):
    """Return (producers, consumers) as {object_key: [package names]}.

    With `explicit_only`, wildcard declarations such as `stg.*` are ignored, so
    the result only contains objects a package names one by one.
    """
    producers = defaultdict(list)
    consumers = defaultdict(list)
    for package in packages.values():
        targets = package.target_objects
        sources = package.source_objects
        if explicit_only:
            targets = [t for t in targets if "*" not in t]
            sources = [s for s in sources if "*" not in s]
        for key in expand_references(targets, catalog_keys):
            producers[key].append(package.name)
        for key in expand_references(sources, catalog_keys):
            consumers[key].append(package.name)
    return producers, consumers


def add_common_arguments(parser):
    parser.add_argument("--json", action="store_true", help="emit machine-readable output")
    parser.add_argument("--strict", action="store_true",
                        help="treat warnings as failures (coverage gaps become errors)")
    parser.add_argument("--quiet", action="store_true", help="print errors only")
    return parser
