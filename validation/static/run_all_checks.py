#!/usr/bin/env python3
"""Static consistency checks for the legacy estate.

These checks are *static*. They read files on disk and the object catalog and
assert that the estate is internally consistent: the XML parses, names follow
the conventions, nothing is duplicated, every reference resolves, the
dependency graph is acyclic, and no credential or forbidden artifact has been
committed.

They prove nothing about runtime behaviour. No check here connects to Oracle,
SQL Server, or anything else, and passing them does not mean a package would
execute or that a SQL object would compile on a real server. Items that can
only be confirmed against live systems are tracked in
docs/known-unvalidated-items.md.

Usage:
    python3 validation/static/run_all_checks.py [--path PREFIX ...] [--json]

    --path  restrict file-scoped checks to one or more repository-relative
            path prefixes (used by work packages to check only what they own).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

import yaml

SOURCE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# The tree the file checks walk. Overridable so the negative fixtures in
# validation/static/run_negative_fixtures.py can point the same checks at a
# scratch copy of a deliberately broken artifact.
REPO_ROOT = os.environ.get("WWI_ESTATE_ROOT") or SOURCE_ROOT
CATALOG_PATH = os.path.join(SOURCE_ROOT, "config", "estate-catalog.yaml")

# Pre-existing Microsoft WideWorldImporters content. Out of scope for the estate
# conventions - it is preserved as shipped.
LEGACY_SAMPLE_DIRS = (
    "wwi-ssdt", "wwi-dw-ssdt", "wwi-ssis", "wwi-app", "wwi-azure-functions",
    "wwi-ssasmd", "power-bi-dashboards", "sample-scripts", "workload-drivers",
)

SSIS_PREFIXES = (
    "Master_", "EXT_ORA_", "EXT_SQL_", "ING_FILE_", "STG_", "DQ_", "REF_",
    "DIM_", "FACT_", "AGG_", "FIN_", "SLS_", "INV_", "PRC_", "C360_", "ERR_", "MNT_",
)

FORBIDDEN_CONTENT = [
    (re.compile(r"\bdatabricks\b", re.I), "Databricks reference"),
    (re.compile(r"\bunity\s+catalog\b", re.I), "Unity Catalog reference"),
    (re.compile(r"\bdelta\s+lake\b", re.I), "Delta Lake reference"),
    (re.compile(r"\blakeflow\b", re.I), "Lakeflow reference"),
    (re.compile(r"\bdbutils\b"), "Databricks utility reference"),
]

# Credential shapes. Deliberately narrow: the estate is full of the *names* of
# credentials, which are fine; assigned literal values are not.
CREDENTIAL_PATTERNS = [
    (re.compile(r"(?i)\b(password|pwd)\s*=\s*(?![\"']?\s*(?:$|;|\"|'))(?![@$])[^\s;\"'<>{}]{3,}"), "assigned password literal"),
    (re.compile(r"(?i)\bintegrated\s+security\s*=\s*false\b"), "connection string with explicit credentials"),
    (re.compile(r"(?i)\b(api[_-]?key|secret[_-]?key|access[_-]?token)\s*[:=]\s*[\"'][^\"']{8,}[\"']"), "embedded API key or token"),
]

CREDENTIAL_ALLOWED_TOKENS = ("@[$Project::", "$(", "%", "<", "{{", "ENV:", "SecretName", "PasswordSecretName")

# Documents whose subject *is* the prohibition, so they necessarily name the
# forbidden technologies. Everything else must not mention them at all.
POLICY_DOCUMENTS = (
    "docs/ESTATE_BUILD_CONTRACT.md",
    "docs/known-unvalidated-items.md",
    "docs/architecture/legacy-architecture.md",
    "validation/static/run_all_checks.py",
    "validation/README.md",
)


class Result:
    def __init__(self):
        self.failures = []
        self.warnings = []
        self.counts = {}

    def fail(self, check, path, message):
        self.failures.append({"check": check, "path": path, "message": message})

    def warn(self, check, path, message):
        self.warnings.append({"check": check, "path": path, "message": message})

    def count(self, key, value):
        self.counts[key] = value


def load_catalog():
    try:
        import yaml
    except ImportError:
        sys.stderr.write("PyYAML is required: pip install pyyaml\n")
        raise SystemExit(2)
    if not os.path.exists(CATALOG_PATH):
        sys.stderr.write("catalog not found at %s; run tools/catalog/build_catalog.py\n" % CATALOG_PATH)
        raise SystemExit(2)
    with open(CATALOG_PATH) as handle:
        return yaml.safe_load(handle)


def walk_files(prefixes, extensions=None):
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        dirnames[:] = [d for d in dirnames
                       if d not in (".git", "__pycache__", ".venv", "node_modules", "obj", "bin")]
        rel_dir = os.path.relpath(dirpath, REPO_ROOT)
        rel_dir = "" if rel_dir == "." else rel_dir.replace(os.sep, "/")
        if rel_dir.split("/")[0] in LEGACY_SAMPLE_DIRS:
            continue
        for filename in filenames:
            rel = "%s/%s" % (rel_dir, filename) if rel_dir else filename
            if prefixes and not any(rel.startswith(p) for p in prefixes):
                continue
            if extensions and not filename.lower().endswith(extensions):
                continue
            yield rel, os.path.join(dirpath, filename)


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------


def check_dtsx_xml(result, prefixes):
    """Every .dtsx / .conmgr / .dtproj / .params file must be well-formed XML."""
    checked = 0
    for rel, full in walk_files(prefixes, (".dtsx", ".conmgr", ".dtproj", ".params")):
        checked += 1
        try:
            ET.parse(full)
        except ET.ParseError as exc:
            result.fail("xml-wellformed", rel, "XML parse error: %s" % exc)
    result.count("xml_files_checked", checked)


def check_dtsx_structure(result, prefixes):
    """Structural expectations for generated packages."""
    ns = {"DTS": "www.microsoft.com/SqlServer/Dts"}
    checked = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        checked += 1
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue  # already reported by check_dtsx_xml
        if not root.tag.endswith("Executable"):
            result.fail("dtsx-structure", rel, "root element is %s, expected DTS:Executable" % root.tag)
            continue
        object_name = root.get("{%s}ObjectName" % ns["DTS"])
        expected = os.path.basename(rel)[: -len(".dtsx")]
        if object_name != expected:
            result.fail("dtsx-structure", rel,
                        "DTS:ObjectName %r does not match file name %r" % (object_name, expected))
        if root.find("DTS:Executables", ns) is None:
            result.fail("dtsx-structure", rel, "package has no DTS:Executables element")
        else:
            tasks = root.find("DTS:Executables", ns)
            if len(list(tasks)) == 0:
                result.fail("dtsx-structure", rel, "package contains no executables")
        if root.find("DTS:EventHandlers", ns) is None:
            result.warn("dtsx-structure", rel, "package has no OnError event handler")
        # No literal credentials in connection strings.
        for cm in root.iter():
            conn = cm.get("{%s}ConnectionString" % ns["DTS"])
            if conn and re.search(r"(?i)password\s*=\s*[^;\s@$]", conn):
                result.fail("dtsx-structure", rel, "connection string contains an inline password")
    result.count("dtsx_files_checked", checked)


def check_dtsx_references(result, prefixes):
    """Precedence constraints and execute-package tasks must resolve."""
    ns = {"DTS": "www.microsoft.com/SqlServer/Dts"}
    all_packages = {os.path.basename(rel)[:-5] for rel, _ in walk_files(None, (".dtsx",))}
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        ref_ids = {e.get("{%s}refId" % ns["DTS"]) for e in root.iter()
                   if e.get("{%s}refId" % ns["DTS"])}
        for constraint in root.iter("{%s}PrecedenceConstraint" % ns["DTS"]):
            for attr in ("From", "To"):
                target = constraint.get("{%s}%s" % (ns["DTS"], attr))
                if target and target not in ref_ids:
                    result.fail("dtsx-references", rel,
                                "precedence constraint %s references unknown executable %r" % (attr, target))
        for node in root.iter("PackageName"):
            child = (node.text or "").strip()
            if child.endswith(".dtsx"):
                child = child[:-5]
            if child and child not in all_packages:
                result.fail("dtsx-references", rel,
                            "Execute Package Task references missing package %r" % child)


def check_dtsx_pipeline(result, prefixes):
    """Data-flow XML must be internally resolvable: unique refIds, real paths."""
    ns = {"DTS": "www.microsoft.com/SqlServer/Dts"}
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for pipeline in root.iter("pipeline"):
            ref_ids = []
            for node in pipeline.iter():
                ref_id = node.get("refId")
                if ref_id:
                    ref_ids.append(ref_id)
            duplicates = sorted({r for r in ref_ids if ref_ids.count(r) > 1})
            for duplicate in duplicates:
                result.fail("dtsx-pipeline", rel, "duplicate refId %r" % duplicate)
            known = set(ref_ids)
            for path in pipeline.iter("path"):
                for attr in ("startId", "endId"):
                    target = path.get(attr)
                    if target and target not in known:
                        result.fail("dtsx-pipeline", rel,
                                    "path %r %s references unknown object %r"
                                    % (path.get("name"), attr, target))
            for component in pipeline.iter("component"):
                if not component.get("componentClassID"):
                    result.fail("dtsx-pipeline", rel,
                                "component %r has no componentClassID" % component.get("name"))
        for log_provider in root.iter("{%s}LogProvider" % ns["DTS"]):
            creation = log_provider.get("{%s}CreationName" % ns["DTS"]) or ""
            if creation.startswith("DTS.LogProviderSQLServer."):
                result.fail("dtsx-pipeline", rel,
                            "log provider creation name %r is not a runtime type" % creation)


def _component_properties(node):
    """name -> text of the <properties> a component or column carries."""
    properties = {}
    for holder in node.findall("properties"):
        for prop in holder.findall("property"):
            properties[prop.get("name")] = (prop.text or "").strip()
    return properties


FLAT_FILE_COLUMN_PROPERTIES = ("FastParse", "UseBinaryFormat")

# Transforms the runtime rejects unless they carry a normal and an error
# output, and nothing else.
TWO_OUTPUT_COMPONENTS = ("Microsoft.DerivedColumn", "Microsoft.DataConvert")


def check_component_contracts(result, prefixes):
    """Pipeline components must carry the custom properties the runtime demands.

    These are the shapes a component validates itself against when the package
    is loaded, so a package missing one fails validation as VS_ISCORRUPT before
    a single row moves - and nothing else in this file would notice. A flat file
    column without FastParse, a derived column without its error output and a
    conditional split whose default output is written as a case have all put a
    package in that state.
    """
    components = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for component in root.iter("component"):
            class_id = component.get("componentClassID") or ""
            name = component.get("name") or component.get("refId") or "?"
            outputs = component.findall("outputs/output")
            components += 1

            if class_id == "Microsoft.FlatFileSource":
                if not component.get("localeId"):
                    result.fail("component-contracts", rel,
                                "flat file source %r has no localeId" % name)
                for output in outputs:
                    if output.get("isErrorOut") == "true":
                        continue
                    for column in output.findall("outputColumns/outputColumn"):
                        properties = _component_properties(column)
                        missing = [p for p in FLAT_FILE_COLUMN_PROPERTIES
                                   if p not in properties]
                        if missing:
                            result.fail("component-contracts", rel,
                                        "flat file source %r column %r is missing the required "
                                        "custom propert%s %s"
                                        % (name, column.get("name"),
                                           "y" if len(missing) == 1 else "ies",
                                           ", ".join(missing)))

            if class_id in TWO_OUTPUT_COMPONENTS:
                if len(outputs) != 2:
                    result.fail("component-contracts", rel,
                                "%s %r declares %d output(s); the runtime requires exactly 2"
                                % (class_id.split(".")[-1], name, len(outputs)))
                error_outputs = [o for o in outputs if o.get("isErrorOut") == "true"]
                if not error_outputs:
                    result.fail("component-contracts", rel,
                                "%s %r has no error output"
                                % (class_id.split(".")[-1], name))
                for output in error_outputs:
                    carried = {c.get("name")
                               for c in output.findall("outputColumns/outputColumn")}
                    missing = [c for c in ("ErrorCode", "ErrorColumn") if c not in carried]
                    if missing:
                        result.fail("component-contracts", rel,
                                    "%s %r error output does not carry %s"
                                    % (class_id.split(".")[-1], name, ", ".join(missing)))

            if class_id == "Microsoft.ConditionalSplit":
                defaults, orders = [], []
                for output in outputs:
                    if output.get("isErrorOut") == "true":
                        continue
                    properties = _component_properties(output)
                    if properties.get("IsDefaultOut", "").lower() == "true":
                        defaults.append(output.get("name"))
                        if properties.get("Expression"):
                            result.fail("component-contracts", rel,
                                        "conditional split %r default output %r carries a case "
                                        "expression" % (name, output.get("name")))
                        continue
                    if not properties.get("Expression"):
                        result.fail("component-contracts", rel,
                                    "conditional split %r output %r has no Expression and is "
                                    "not the default output" % (name, output.get("name")))
                    order = properties.get("EvaluationOrder")
                    if order is not None and order.lstrip("-").isdigit():
                        orders.append(int(order))
                if len(defaults) != 1:
                    result.fail("component-contracts", rel,
                                "conditional split %r has %d default output(s); it must have "
                                "exactly 1" % (name, len(defaults)))
                if orders and sorted(orders) != list(range(len(orders))):
                    result.fail("component-contracts", rel,
                                "conditional split %r case evaluation orders are %s; they must "
                                "be 0..%d" % (name, sorted(orders), len(orders) - 1))
    result.count("pipeline_components_checked", components)


def check_input_column_cache(result, prefixes):
    """Every input column must cache the type of the column arriving on it.

    A component revalidates its cached input metadata against the buffer its
    upstream path offers, and an input column that caches only a name has
    nothing to compare, so the component fails validation as
    VS_NEEDSNEWMETADATA before a row moves:

        Column Normalize Partner Text.Inputs[Derived Column Input]
        .Columns[PartnerCode] does not have a valid cache

    which is how the live STG_Load_PartnerSale validation failed. Derived
    Column replacements, Data Conversion sources and Union All inputs have all
    been emitted in that state.
    """
    columns = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for component in root.iter("component"):
            name = component.get("name") or component.get("refId") or "?"
            for column in component.iter("inputColumn"):
                columns += 1
                if not column.get("cachedDataType"):
                    result.fail("input-column-cache", rel,
                                "input column %r of %r caches no data type"
                                % (column.get("name"), name))
    result.count("input_columns_checked", columns)


def check_written_input_dispositions(result, prefixes):
    """An input column a component writes must say what a failure does to its row.

    A readWrite column is computed in place, so it carries the dispositions the
    computation obeys. Without them the column validates as VS_ISCORRUPT:

        The Normalize Partner Text.Inputs[Derived Column Input]
        .Columns[PartnerCode] has an invalid error or truncation row disposition

    which is how the live STG_Load_PartnerSale validation failed.
    """
    columns = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for component in root.iter("component"):
            name = component.get("name") or component.get("refId") or "?"
            for column in component.iter("inputColumn"):
                if column.get("usageType") != "readWrite":
                    continue
                columns += 1
                missing = [attr for attr in ("errorRowDisposition",
                                             "truncationRowDisposition")
                           if not column.get(attr)]
                if missing:
                    result.fail("input-column-disposition", rel,
                                "written input column %r of %r declares no %s"
                                % (column.get("name"), name, ", ".join(missing)))
    result.count("written_input_columns_checked", columns)


def check_lookup_reference_mapping(result, prefixes):
    """A lookup must map every key and every copied column to its reference query.

    The join lives on the input column as joinToReferenceColumn and the copy on
    the match output column as copyFromReferenceColumn. A lookup missing either
    has no relation to the reference set it names and fails validation with
    0xC0010009, which is how the live STG_Load_Currency validation failed.
    """
    lookups = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for component in root.iter("component"):
            if component.get("componentClassID") != "Microsoft.Lookup":
                continue
            lookups += 1
            name = component.get("name") or component.get("refId") or "?"
            for column in component.iter("inputColumn"):
                if not column.get("joinToReferenceColumn"):
                    result.fail("lookup-reference-mapping", rel,
                                "lookup %r joins input column %r to no reference column"
                                % (name, column.get("name")))
            for output in component.iter("output"):
                if output.get("name") != "Lookup Match Output":
                    continue
                for column in output.iter("outputColumn"):
                    if not column.get("copyFromReferenceColumn"):
                        result.fail("lookup-reference-mapping", rel,
                                    "lookup %r copies output column %r from no reference column"
                                    % (name, column.get("name")))
    result.count("lookups_checked", lookups)


def check_foreach_file_enumerators(result, prefixes):
    """A file loop must enumerate the folder and the files it was configured with.

    The Directory expression belongs to the enumerator: on the loop container it
    is never applied, the enumerator keeps its design-time folder, and the loop
    silently walks whatever that path resolves to. A file specification of
    ``*.*`` has the same effect within a folder - unrelated files are ingested
    and logged as feed files.
    """
    ns = "www.microsoft.com/SqlServer/Dts"
    loops = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for executable in root.iter("{%s}Executable" % ns):
            enumerator = executable.find("{%s}ForEachEnumerator" % ns)
            if enumerator is None:
                continue
            if (enumerator.get("{%s}CreationName" % ns) or "") != "Microsoft.ForEachFileEnumerator":
                continue
            loops += 1
            where = executable.get("{%s}ObjectName" % ns) or executable.get("{%s}refId" % ns)
            on_container = [e for e in executable.findall("{%s}PropertyExpression" % ns)
                            if e.get("{%s}Name" % ns) == "Directory"]
            on_enumerator = [e for e in enumerator.findall("{%s}PropertyExpression" % ns)
                             if e.get("{%s}Name" % ns) == "Directory"]
            if on_container:
                result.fail("foreach-file-enumerator", rel,
                            "foreach loop %r sets the Directory expression on the container; "
                            "the enumerator owns that property" % where)
            if not on_enumerator:
                result.fail("foreach-file-enumerator", rel,
                            "foreach loop %r has no Directory expression on its enumerator, so "
                            "it walks the folder baked in at design time" % where)
            spec = None
            for prop in enumerator.iter("FEFEProperty"):
                if prop.get("FileSpec") is not None:
                    spec = prop.get("FileSpec")
            # A feed loop reads one feed out of a shared drop folder; a sweep
            # over a quarantine or archive folder is meant to take everything.
            if rel.startswith("ssis/03_file_ingestion/") and (not spec or spec in ("*", "*.*")):
                result.fail("foreach-file-enumerator", rel,
                            "foreach loop %r enumerates %r; it must name the feed's files"
                            % (where, spec or ""))
    result.count("foreach_file_loops", loops)


# A single row result set that reads a table returns nothing when the table has
# no matching row, and the assignment to the variable fails. An aggregate over
# the same predicate always returns exactly one row.
AGGREGATE_SELECT_RE = re.compile(
    r"\b(MAX|MIN|COUNT|COUNT_BIG|SUM|AVG|ISNULL|NVL|COALESCE)\b", re.I)


def _outermost(sql):
    """*sql* with every parenthesised subexpression removed.

    A derived table's own GROUP BY says nothing about how many rows the
    statement returns; only the outermost query shape does.
    """
    out, depth = [], 0
    for char in sql:
        if char == "(":
            depth += 1
            out.append(" ")
        elif char == ")":
            depth = max(0, depth - 1)
        elif depth == 0:
            out.append(char)
    return "".join(out)


def check_single_row_result_sets(result, prefixes):
    """An Execute SQL Task taking a single row must be unable to return none."""
    ns = "www.microsoft.com/sqlserver/dts/tasks/sqltask"
    tasks = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for task in root.iter("{%s}SqlTaskData" % ns):
            if (task.get("{%s}ResultType" % ns) or "") != "ResultSetType_SingleRow":
                continue
            tasks += 1
            sql = (task.get("{%s}SqlStatementSource" % ns) or "").strip()
            if not sql.upper().startswith("SELECT"):
                continue
            outer = " ".join(_outermost(sql).split())
            if not re.search(r"\bFROM\b", outer, re.I):
                # A select over no table - a scalar function, a literal - is
                # itself the one row.
                continue
            if (re.search(r"\bGROUP\s+BY\b", outer, re.I)
                    or not AGGREGATE_SELECT_RE.search(sql)):
                result.fail("single-row-result-set", rel,
                            "single row result set reads %r, which returns no row when nothing "
                            "matches" % " ".join(sql.split())[:120])
    result.count("single_row_result_sets", tasks)


def _project_connection_managers(package_full_path):
    """DTSID -> name for the .conmgr files of the project owning a package."""
    ns = {"DTS": "www.microsoft.com/SqlServer/Dts"}
    folder = os.path.dirname(package_full_path)
    managers = {}
    for name in sorted(os.listdir(folder)):
        if not name.endswith(".conmgr"):
            continue
        try:
            root = ET.parse(os.path.join(folder, name)).getroot()
        except ET.ParseError:
            continue
        dtsid = root.get("{%s}DTSID" % ns["DTS"])
        if dtsid:
            managers[dtsid.upper()] = root.get("{%s}ObjectName" % ns["DTS"]) or name[: -len(".conmgr")]
    return managers


def check_dtsx_connection_refs(result, prefixes):
    """Every component connection must name a connection manager that exists.

    A pipeline component addresses a package-scoped connection manager by its
    refId path (``Package.ConnectionManagers[X]``) and a project-scoped one by
    ``{DTSID}:external``. Emitting a package manager's DTSID instead of its
    refId leaves the reference unresolvable and the package fails to load with
    0xC001001C inside CPackage::LoadFromXML.
    """
    ns = {"DTS": "www.microsoft.com/SqlServer/Dts"}
    packages = 0
    references = 0
    affected = set()
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        packages += 1
        package_ids = {}   # refId -> DTSID for package-owned connection managers
        for holder in root.iter("{%s}ConnectionManagers" % ns["DTS"]):
            for cm in holder.findall("{%s}ConnectionManager" % ns["DTS"]):
                ref_id = cm.get("{%s}refId" % ns["DTS"])
                dtsid = cm.get("{%s}DTSID" % ns["DTS"])
                if ref_id:
                    package_ids[ref_id] = (dtsid or "").upper()
        project_ids = _project_connection_managers(full)
        known_dtsids = set(project_ids) | {v for v in package_ids.values() if v}

        def fail(message):
            affected.add(rel)
            result.fail("dtsx-connection-refs", rel, message)

        for pipeline in root.iter("pipeline"):
            for connection in pipeline.iter("connection"):
                references += 1
                manager_id = connection.get("connectionManagerID") or ""
                ref_id = connection.get("connectionManagerRefId") or ""
                where = connection.get("refId") or connection.get("name")
                if manager_id.endswith(":external"):
                    dtsid = manager_id[: -len(":external")].upper()
                    if dtsid not in project_ids:
                        fail("%s references project connection manager %s, which no .conmgr "
                             "in the project declares" % (where, manager_id))
                    elif ref_id != "Project.ConnectionManagers[%s]" % project_ids[dtsid]:
                        fail("%s references %s but names it %r" % (where, manager_id, ref_id))
                elif manager_id in package_ids:
                    if ref_id != manager_id:
                        fail("%s addresses package connection manager %s but names it %r"
                             % (where, manager_id, ref_id))
                elif manager_id.upper() in known_dtsids:
                    fail("%s addresses connection manager %s by DTSID; a package-scoped "
                         "manager must be addressed by its refId path" % (where, manager_id))
                else:
                    fail("%s references connection manager ID %r, which no object in the "
                         "package or its project owns" % (where, manager_id))
        for task in root.iter("{www.microsoft.com/sqlserver/dts/tasks/sqltask}SqlTaskData"):
            manager_id = task.get("{www.microsoft.com/sqlserver/dts/tasks/sqltask}Connection") or ""
            references += 1
            if manager_id.upper() not in known_dtsids:
                fail("Execute SQL Task references connection manager ID %r, which no object "
                     "in the package or its project owns" % manager_id)
    result.count("connection_references_checked", references)
    result.count("packages_with_connection_refs_checked", packages)
    result.count("packages_with_unknown_connection_refs", len(affected))


def _sql_placeholders(sql):
    """Count OLE DB parameter markers, ignoring the ones inside literals."""
    count = 0
    quote = None
    index = 0
    while index < len(sql):
        char = sql[index]
        if quote:
            if char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 1
                else:
                    quote = None
        elif char in "'\"":
            quote = char
        elif char == "?":
            count += 1
        index += 1
    return count


def check_execute_sql_parameters(result, prefixes):
    """Every ? in an Execute SQL Task must have a ParameterBinding.

    The OLE DB provider binds parameters positionally, so a statement with more
    markers than bindings either fails outright or, worse, silently shifts every
    later value into the wrong slot. Result-set bindings are a separate channel
    and never stand in for an OUTPUT parameter marker.
    """
    sq = "www.microsoft.com/sqlserver/dts/tasks/sqltask"
    tasks = 0
    affected = set()
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for task in root.iter("{%s}SqlTaskData" % sq):
            tasks += 1
            sql = task.get("{%s}SqlStatementSource" % sq) or ""
            if (task.get("{%s}SqlStmtSourceType" % sq) or "DirectInput") != "DirectInput":
                continue
            markers = _sql_placeholders(sql)
            bindings = task.findall("{%s}ParameterBinding" % sq)
            names = [b.get("{%s}ParameterName" % sq) for b in bindings]
            if markers != len(bindings):
                affected.add(rel)
                result.fail("execute-sql-parameters", rel,
                            "%r has %d parameter marker(s) but %d ParameterBinding entr(y|ies)"
                            % (sql.strip()[:80], markers, len(bindings)))
                continue
            if sorted(names) != sorted(str(i) for i in range(markers)):
                affected.add(rel)
                result.fail("execute-sql-parameters", rel,
                            "%r binds parameter names %s, expected the positions 0..%d"
                            % (sql.strip()[:80], names, markers - 1))
    result.count("execute_sql_tasks_checked", tasks)
    result.count("packages_with_parameter_mismatch", len(affected))


def check_dtproj_structure(result, prefixes):
    """Project manifests must be the SSDT project-deployment shape SSIS builds."""
    ssis_ns = "www.microsoft.com/SqlServer/SSIS"
    checked = 0
    for rel, full in walk_files(prefixes, (".dtproj",)):
        checked += 1
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        if root.tag != "Project":
            result.fail("dtproj-structure", rel,
                        "root element is %s, expected an unqualified Project" % root.tag)
            continue
        model = root.find("DeploymentModel")
        if model is None or (model.text or "").strip() != "Project":
            result.fail("dtproj-structure", rel, "DeploymentModel is not Project")
        manifest = root.find("DeploymentModelSpecificContent/Manifest")
        if manifest is None:
            result.fail("dtproj-structure", rel,
                        "DeploymentModelSpecificContent/Manifest is missing")
            continue
        if manifest.find("{%s}Project" % ssis_ns) is None:
            result.fail("dtproj-structure", rel, "manifest has no SSIS:Project element")
            continue
        declared = {node.get("{%s}Name" % ssis_ns)
                    for node in manifest.iter("{%s}Package" % ssis_ns)}
        on_disk = {name for name in os.listdir(os.path.dirname(full))
                   if name.endswith(".dtsx")}
        for missing in sorted(on_disk - declared):
            result.fail("dtproj-structure", rel,
                        "package %s is in the project folder but not in the manifest" % missing)
        for absent in sorted(declared - on_disk):
            result.fail("dtproj-structure", rel,
                        "manifest declares %s, which is not in the project folder" % absent)
    result.count("dtproj_files_checked", checked)


# The properties an OLE DB connection manager is retargeted through. Its whole
# ConnectionString is deliberately absent: assigning it rebuilds the connection
# and drops the password the catalog applied to CM.<connection>.Password, which
# is what made every task fail with 'Login failed for user' followed by
# DTS_E_CANNOTACQUIRECONNECTIONFROMCONNECTIONMANAGER.
# Oracle addresses the whole service through ServerName, so it has no catalog
# of its own to retarget.
OLEDB_BOUND_PROPERTIES = {
    "MSOLEDBSQL19.1": ("ServerName", "InitialCatalog", "UserName"),
    "OraOLEDB.Oracle.1": ("ServerName", "UserName"),
}


def check_conmgr_binding(result, prefixes):
    """Connection managers must bind at run time, not carry a literal token."""
    ns = {"DTS": "www.microsoft.com/SqlServer/Dts"}
    checked = 0
    for rel, full in walk_files(prefixes, (".conmgr",)):
        checked += 1
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        kind = root.get("{%s}CreationName" % ns["DTS"]) or ""
        expressed = [node.get("{%s}Name" % ns["DTS"])
                     for node in root.iter("{%s}PropertyExpression" % ns["DTS"])]
        if kind == "OLEDB":
            if "ConnectionString" in expressed:
                result.fail("conmgr-binding", rel,
                            "OLE DB connection manager expresses ConnectionString; "
                            "evaluating it rebuilds the connection and discards the "
                            "password the catalog applied through CM.<connection>.Password")
            literal = ""
            for node in root.iter():
                literal = node.get("{%s}ConnectionString" % ns["DTS"]) or literal
            required = ()
            for provider, properties in OLEDB_BOUND_PROPERTIES.items():
                if provider in literal:
                    required = properties
            for prop in required:
                if prop not in expressed:
                    result.fail("conmgr-binding", rel,
                                "no %s property expression; the connection cannot be "
                                "retargeted at run time" % prop)
        elif "ConnectionString" not in expressed:
            result.fail("conmgr-binding", rel,
                        "no ConnectionString property expression; the connection cannot be "
                        "retargeted at run time")
        for node in root.iter():
            literal = node.get("{%s}ConnectionString" % ns["DTS"])
            if literal and "@[$" in literal:
                result.fail("conmgr-binding", rel,
                            "ConnectionString holds the unevaluated token %r" % literal)
    result.count("conmgr_files_checked", checked)


# OLE DB Driver 19 spells the keyword with spaces; TrustServerCertificate is a
# .NET SqlClient keyword the provider silently ignores, so a connection to an
# instance with an untrusted certificate fails with the chain error instead.
TLS_KEYWORD = "Trust Server Certificate=True;"
TLS_KEYWORD_WRONG = re.compile(r"TrustServerCertificate\s*=")

# The providers the estate is validated against; both are fixed in the
# design-time connection string because no connection manager property carries
# them and rewriting the whole connection string would drop the password.
SQLSERVER_PROVIDER = "MSOLEDBSQL19.1"
ORACLE_PROVIDER = "OraOLEDB.Oracle.1"


SENSITIVE_TOKEN_RE = re.compile(
    r"@\[\$Project::(\w*(?:Password|Secret|Pwd)\w*)\]", re.IGNORECASE)


PASSWORD_KEYWORD_RE = re.compile(r"(?i)\b(password|pwd)\s*=")


def check_conmgr_credentials(result, prefixes):
    """No connection expression may name a credential, and no literal may hold one.

    A Sensitive project parameter cannot be read by the SSIS expression
    evaluator: the package fails validation with 0xC0017010 before the
    credential would ever be used. The password therefore travels on the
    connection manager's own ``CM.<connection>.Password`` project parameter,
    which the runtime applies to the connection manager object and which the
    catalog environment binds by reference. Host, catalog and user are
    retargeted by property expression; the provider and the TLS keyword cannot
    be, so they are asserted on the design-time literal that ships with the
    package and is what the connection actually starts from.
    """
    ns = {"DTS": "www.microsoft.com/SqlServer/Dts"}
    oracle = sql = 0
    for rel, full in walk_files(prefixes, (".conmgr",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        expressions = {}
        for node in root.iter("{%s}PropertyExpression" % ns["DTS"]):
            expressions[node.get("{%s}Name" % ns["DTS"])] = node.text or ""
        for name, expression in sorted(expressions.items()):
            for match in SENSITIVE_TOKEN_RE.finditer(expression):
                result.fail("conmgr-credentials", rel,
                            "the %s expression references the sensitive project parameter "
                            "$Project::%s; a sensitive parameter cannot be read by the "
                            "expression evaluator (0xC0017010) - bind the credential through "
                            "CM.<connection>.Password" % (name, match.group(1)))
        if (root.get("{%s}CreationName" % ns["DTS"]) or "") != "OLEDB":
            continue
        literal = ""
        for node in root.iter():
            literal = node.get("{%s}ConnectionString" % ns["DTS"]) or literal
        if PASSWORD_KEYWORD_RE.search(literal):
            result.fail("conmgr-credentials", rel,
                        "the design-time connection string holds a password keyword")
        if ORACLE_PROVIDER in literal:
            oracle += 1
            if "UserName" not in expressions:
                result.fail("conmgr-credentials", rel,
                            "Oracle connection manager does not retarget UserName")
        if SQLSERVER_PROVIDER in literal:
            sql += 1
            if TLS_KEYWORD not in literal:
                result.fail("conmgr-credentials", rel,
                            "SQL Server connection string does not carry %r" % TLS_KEYWORD)
            if TLS_KEYWORD_WRONG.search(literal):
                result.fail("conmgr-credentials", rel,
                            "SQL Server connection string carries the unspaced TrustServerCertificate "
                            "keyword, which OLE DB Driver 19 ignores")
        elif ORACLE_PROVIDER not in literal:
            result.fail("conmgr-credentials", rel,
                        "OLE DB connection string names no supported provider; the provider "
                        "cannot be retargeted at run time, so it has to be in the literal")
    result.count("oracle_connections_checked", oracle)
    result.count("sqlserver_connections_checked", sql)


def check_catalog_binding_surface(result, prefixes):
    """Every OLE DB connection must expose a sensitive CM.<name>.Password.

    This is the other half of check_conmgr_credentials. Taking the password out
    of the expression only works if something else carries it, and the only
    thing the catalog can bind is the connection manager parameter declared in
    the project manifest. A project whose manifest lacks it deploys happily and
    then connects with an empty password.

    Project.params is checked at the same time: a Sensitive project parameter
    left there would be Required=1 and unbound, which fails the execution at
    validation.
    """
    ssis_ns = "www.microsoft.com/SqlServer/SSIS"
    dts_ns = "www.microsoft.com/SqlServer/Dts"
    projects = 0
    bindings = 0
    for rel, full in walk_files(prefixes, (".dtproj",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        manifest = root.find("DeploymentModelSpecificContent/Manifest")
        if manifest is None:
            continue
        projects += 1
        declared = {}
        for node in manifest.iter("{%s}Parameter" % ssis_ns):
            name = node.get("{%s}Name" % ssis_ns)
            sensitive = "0"
            for prop in node.iter("{%s}Property" % ssis_ns):
                if prop.get("{%s}Name" % ssis_ns) == "Sensitive":
                    sensitive = (prop.text or "0").strip()
            declared[name] = sensitive

        directory = os.path.dirname(full)
        for conmgr in sorted(name for name in os.listdir(directory) if name.endswith(".conmgr")):
            try:
                cm_root = ET.parse(os.path.join(directory, conmgr)).getroot()
            except ET.ParseError:
                continue
            if cm_root.get("{%s}CreationName" % dts_ns) != "OLEDB":
                continue
            parameter = "CM.%s.Password" % conmgr[:-len(".conmgr")]
            if parameter not in declared:
                result.fail("catalog-binding", rel,
                            "manifest does not declare %s, so the catalog has nothing to "
                            "bind the credential to" % parameter)
            elif declared[parameter] != "1":
                result.fail("catalog-binding", rel,
                            "%s is not Sensitive=1" % parameter)
            else:
                bindings += 1

    for rel, full in walk_files(prefixes, (".params",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for node in root.iter("{%s}Parameter" % ssis_ns):
            name = node.get("{%s}Name" % ssis_ns)
            for prop in node.iter("{%s}Property" % ssis_ns):
                if (prop.get("{%s}Name" % ssis_ns) == "Sensitive"
                        and (prop.text or "0").strip() == "1"):
                    result.fail("catalog-binding", rel,
                                "project parameter %s is Sensitive=1; credentials belong on "
                                "CM.<connection>.Password, not on a project parameter" % name)
    result.count("catalog_projects_checked", projects)
    result.count("catalog_password_bindings", bindings)


def check_environment_binding_parity(result, prefixes):
    """Every catalog environment variable must have a parameter to bind to.

    config/environments/<env>.env.yaml is what deployment/ssis/render_environment_sql.py
    turns into `catalog.set_object_parameter_value @value_type = 'R'` calls. A
    variable whose target parameter no longer exists in the project manifest is
    bound to nothing: the deployment still succeeds, and the execution then runs
    with the design-time default - an empty password, in the case that took the
    Master_File_Ingestion run down. A CM.<connection>.ConnectionString target is
    refused outright, because applying it rebuilds the connection manager and
    discards CM.<connection>.Password with it.
    """
    ssis_ns = "www.microsoft.com/SqlServer/SSIS"
    dts_ns = "www.microsoft.com/SqlServer/Dts"
    # The deployable parameter surface of a project is its manifest - the
    # CM.<connection>.* parameters - plus Project.params.
    declared = set()
    credential_parameters = set()
    for _rel, full in walk_files(prefixes, (".dtproj", ".params")):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for node in root.iter("{%s}Parameter" % ssis_ns):
            name = node.get("{%s}Name" % ssis_ns)
            declared.add(name)
            if name.startswith("CM.") and name.endswith(".Password"):
                credential_parameters.add(name)
    if not declared:
        return

    oledb = set()
    for _rel, full in walk_files(prefixes, (".conmgr",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        if (root.get("{%s}CreationName" % dts_ns) or "") == "OLEDB":
            oledb.add(os.path.basename(full)[: -len(".conmgr")])

    environments = 0
    directory = os.path.join(REPO_ROOT, "config", "environments")
    if not os.path.isdir(directory):
        return
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".env.yaml"):
            continue
        rel = "config/environments/%s" % name
        with open(os.path.join(REPO_ROOT, rel)) as handle:
            document = yaml.safe_load(handle)
        environments += 1
        bound = set()
        for variable in document.get("ssis_environment_variables") or []:
            for target in variable.get("binds_to") or [variable["parameter"]]:
                bound.add(target)
                if (target.endswith(".ConnectionString")
                        and target[len("CM."):-len(".ConnectionString")] in oledb):
                    result.fail("environment-binding", rel,
                                "%s binds %s; applying a whole connection string "
                                "rebuilds the connection manager and discards the "
                                "password bound to CM.<connection>.Password"
                                % (variable["parameter"], target))
                elif target not in declared:
                    result.fail("environment-binding", rel,
                                "%s binds %s, which no project manifest declares; the "
                                "binding is skipped at deployment and the execution "
                                "runs on the design-time default"
                                % (variable["parameter"], target))
        for parameter in sorted(credential_parameters - bound):
            result.fail("environment-binding", rel,
                        "no environment variable binds %s, so that connection "
                        "manager would authenticate with an empty password"
                        % parameter)
    result.count("environments_checked", environments)


def check_file_locality(result, prefixes):
    """A file package must resolve its folder and its file at run time.

    Two defects make an ING_FILE_* package succeed while loading nothing. A
    Foreach Loop with only a design-time ``FEFE Folder`` enumerates whatever
    literal path was baked in at generation, so the catalog reads a directory
    that does not exist on the execution host and reports success over zero
    files. And a flat file source pointed at a project-level FILE connection
    manager has no per-iteration file at all; the source needs a FLATFILE
    manager whose ConnectionString expression is the loop variable.
    """
    dts = "www.microsoft.com/SqlServer/Dts"
    loops = 0
    managers = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue

        for executable in root.iter("{%s}Executable" % dts):
            if executable.get("{%s}CreationName" % dts) != "STOCK:FOREACHLOOP":
                continue
            if executable.find(".//{%s}ForEachEnumerator" % dts) is None:
                continue
            enumerator = executable.find(".//{%s}ForEachEnumerator" % dts)
            if enumerator.find(".//FEFEProperty") is None:
                continue  # not a file enumerator
            loops += 1
            name = executable.get("{%s}ObjectName" % dts)
            # Directory belongs to the enumerator; see check_foreach_file_enumerators.
            expressions = [node.text or "" for node in enumerator
                           if node.tag == "{%s}PropertyExpression" % dts
                           and node.get("{%s}Name" % dts) == "Directory"]
            if not expressions:
                result.fail("file-locality", rel,
                            "Foreach loop %r has no Directory property expression, so it "
                            "enumerates the design-time folder on the execution host" % name)
            elif not any("$Project::InboundFileRoot" in text
                         or "$Project::QuarantineFileRoot" in text for text in expressions):
                result.fail("file-locality", rel,
                            "Foreach loop %r derives its Directory from neither "
                            "InboundFileRoot nor QuarantineFileRoot" % name)

        for manager in root.iter("{%s}ConnectionManager" % dts):
            if manager.get("{%s}CreationName" % dts) != "FLATFILE":
                continue
            managers += 1
            name = manager.get("{%s}ObjectName" % dts)
            bound = [node.text or "" for node in manager
                     if node.tag == "{%s}PropertyExpression" % dts
                     and node.get("{%s}Name" % dts) == "ConnectionString"]
            if not any("User::CurrentFilePath" in text for text in bound):
                result.fail("file-locality", rel,
                            "flat file connection manager %r is not bound to "
                            "User::CurrentFilePath, so every iteration reads the same "
                            "design-time file" % name)
    result.count("foreach_file_loops_checked", loops)
    result.count("flatfile_managers_checked", managers)


FILE_SYSTEM_OPERATIONS = ("CopyFile", "MoveFile", "DeleteFile", "RenameFile",
                          "CopyDirectory", "MoveDirectory", "DeleteDirectory",
                          "DeleteDirectoryContent", "CreateDirectory", "SetAttributes")


def check_file_system_tasks(result, prefixes):
    """A File System Task must serialise the attribute names the task host reads.

    The task host is the only component that interprets FileSystemData, and it
    reads TaskOperationType / TaskSourcePath / TaskDestinationPath. An element
    spelled Operation / SourcePath / DestinationPath is still well-formed XML
    and still builds, but the task loads on its defaults - operation CopyFile
    with no paths - and the execution host reports

        "DestinationPath" is not valid on operation type "CopyFile"

    at task validation, which is how the archive step of every ING_FILE_*
    package failed in the Master_File_Ingestion run.
    """
    tasks = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for data in root.iter("FileSystemData"):
            tasks += 1
            operation = data.get("TaskOperationType")
            if operation is None:
                result.fail("file-system-task", rel,
                            "FileSystemData carries %s; the task host reads "
                            "TaskOperationType and loads its CopyFile default instead"
                            % (", ".join(sorted(data.attrib)) or "no attribute"))
                continue
            if operation not in FILE_SYSTEM_OPERATIONS:
                result.fail("file-system-task", rel,
                            "unknown File System Task operation %r" % operation)
            for attribute in ("TaskSourcePath", "TaskIsSourceVariable"):
                if not data.get(attribute):
                    result.fail("file-system-task", rel,
                                "operation %s has no %s" % (operation, attribute))
            if operation.startswith(("Copy", "Move", "Rename")):
                for attribute in ("TaskDestinationPath", "TaskIsDestinationVariable"):
                    if not data.get(attribute):
                        result.fail("file-system-task", rel,
                                    "operation %s has no %s" % (operation, attribute))
    result.count("file_system_tasks_checked", tasks)


def check_variable_driven_file_tasks(result, prefixes):
    """A File System Task fed by variables must not validate at package start.

    CurrentFilePath and ArchiveFilePath hold their design-time empty string
    until the Foreach loop assigns the first file, so a task that validates
    with the package fails before the loop ever runs with

        Variable "CurrentFilePath" is used as a source or destination and is empty.

    which is how executions 81-83 of Master_File_Ingestion failed. The task
    therefore has to carry DelayValidation, and every variable it addresses has
    to be one the run time actually fills: a Foreach variable mapping or an
    expression, never a bare empty literal.
    """
    dts = "www.microsoft.com/SqlServer/Dts"
    assignment = re.compile(r"@\[(\w+::\w+)\]\s*=")
    tasks = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue

        # variable -> the executables that give it a value before anything reads it
        writers = {}
        for variable in root.iter("{%s}Variable" % dts):
            if not variable.get("{%s}Expression" % dts):
                continue
            writers.setdefault("%s::%s" % (variable.get("{%s}Namespace" % dts),
                                           variable.get("{%s}ObjectName" % dts)), set()).add(None)
        for executable in root.iter("{%s}Executable" % dts):
            ref = executable.get("{%s}refId" % dts)
            for task in executable.iter("ExpressionTask"):
                for name in assignment.findall(task.get("Expression") or ""):
                    writers.setdefault(name, set()).add(ref)
            for mapping in executable.findall("{%s}ForEachVariableMappings/{%s}ForEachVariableMapping"
                                              % (dts, dts)):
                writers.setdefault(mapping.get("{%s}VariableName" % dts), set()).add(ref)

        upstream = {}
        for constraint in root.iter("{%s}PrecedenceConstraint" % dts):
            upstream.setdefault(constraint.get("{%s}To" % dts), set()).add(
                constraint.get("{%s}From" % dts))

        def before(ref):
            """Every executable that must complete before *ref* starts."""
            seen, queue = set(), [ref]
            while queue:
                for parent in upstream.get(queue.pop(), ()):
                    if parent not in seen:
                        seen.add(parent)
                        queue.append(parent)
            # a containing loop or sequence has already run its own assignments
            while "\\" in ref:
                ref = ref.rsplit("\\", 1)[0]
                seen.add(ref)
            return seen

        for executable in root.iter("{%s}Executable" % dts):
            data = executable.find("{%s}ObjectData/FileSystemData" % dts)
            if data is None:
                continue
            variables = [data.get("TaskSourcePath")] if data.get("TaskIsSourceVariable") == "True" else []
            if data.get("TaskIsDestinationVariable") == "True":
                variables.append(data.get("TaskDestinationPath"))
            if not variables:
                continue
            tasks += 1
            name = executable.get("{%s}ObjectName" % dts)
            if executable.get("{%s}DelayValidation" % dts) != "True":
                result.fail("file-task-validation", rel,
                            "File System Task %r addresses %s but validates at package "
                            "start, when those variables are still empty"
                            % (name, ", ".join(variables)))
            preceding = before(executable.get("{%s}refId" % dts))
            for variable in variables:
                sources = writers.get(variable)
                if not sources:
                    result.fail("file-task-validation", rel,
                                "File System Task %r reads %s, which no expression, Foreach "
                                "mapping or Expression Task ever fills" % (name, variable))
                elif not sources & (preceding | {None}):
                    result.fail("file-task-validation", rel,
                                "File System Task %r reads %s, which is only assigned by %s - "
                                "nothing that runs before it" % (name, variable,
                                                                 ", ".join(sorted(s for s in sources if s))))
    result.count("variable_driven_file_tasks_checked", tasks)


OLEDB_SOURCE_PROPERTIES = ("OpenRowset", "OpenRowsetVariable", "SqlCommand", "SqlCommandVariable",
                           "DefaultCodePage", "AlwaysUseDefaultCodePage", "AccessMode",
                           "ParameterMapping")


def check_oledb_source_properties(result, prefixes):
    """An OLE DB source must persist the whole custom property set it declares.

    The component asks the run time for every property of its class by name
    during validation, so one omission fails it outright:

        The "RAW File Partner Sales" is missing the required property
        "OpenRowsetVariable" ... validation status "VS_ISCORRUPT"

    which is how execution 84 failed. Beyond presence, a command with ?
    markers needs one ParameterMapping entry per marker, in order, each
    naming a variable or parameter DTSID the package actually declares -
    an unmapped marker cannot be bound and a dangling DTSID binds nothing.
    """
    dts = "www.microsoft.com/SqlServer/Dts"
    mapping_entry = re.compile(r'"Parameter(\d+):(\w+)",\{([0-9A-Fa-f-]{36})\};')
    sources = 0
    parameterised = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue

        declared = set()
        for node in list(root.iter("{%s}Variable" % dts)) + list(root.iter("{%s}PackageParameter" % dts)):
            dtsid = node.get("{%s}DTSID" % dts) or ""
            declared.add(dtsid.strip("{}").upper())

        for component in root.iter("component"):
            if component.get("componentClassID") != "Microsoft.OLEDBSource":
                continue
            sources += 1
            name = component.get("name")
            properties = {node.get("name"): (node.text or "")
                          for node in component.iter("property")}
            for required in OLEDB_SOURCE_PROPERTIES:
                if required not in properties:
                    result.fail("oledb-source-properties", rel,
                                "OLE DB source %r does not persist the required property %r, "
                                "so the component fails validation as VS_ISCORRUPT"
                                % (name, required))
            markers = _sql_placeholders(properties.get("SqlCommand", ""))
            if not markers:
                continue
            parameterised += 1
            entries = mapping_entry.findall(properties.get("ParameterMapping", ""))
            if len(entries) != markers:
                result.fail("oledb-source-properties", rel,
                            "OLE DB source %r has %d parameter marker(s) but %d "
                            "ParameterMapping entr(y|ies)" % (name, markers, len(entries)))
                continue
            if [int(index) for index, _direction, _dtsid in entries] != list(range(markers)):
                result.fail("oledb-source-properties", rel,
                            "OLE DB source %r maps parameters out of placeholder order: %s"
                            % (name, properties.get("ParameterMapping")))
            for _index, _direction, dtsid in entries:
                if dtsid.upper() not in declared:
                    result.fail("oledb-source-properties", rel,
                                "OLE DB source %r binds a parameter to DTSID {%s}, which no "
                                "variable or package parameter owns" % (name, dtsid))
    result.count("oledb_sources_checked", sources)
    result.count("parameterised_oledb_sources_checked", parameterised)


def check_feed_manifest(result, prefixes):
    """The preflight's feed manifest must still match config/landing-zone.yaml.

    The preflight runs in PowerShell on the execution host and reads JSON rather
    than YAML, so the manifest is a rendering of the landing zone document. A
    stale rendering means the preflight proves the wrong layout.
    """
    sys.path.insert(0, os.path.join(REPO_ROOT, "tools", "landing"))
    try:
        import render_feed_manifest
    except ImportError as error:
        result.fail("feed-manifest", "tools/landing/render_feed_manifest.py",
                    "cannot be imported: %s" % error)
        return

    relative = "deployment/preflight/feed-manifest.json"
    path = os.path.join(REPO_ROOT, relative)
    if not os.path.exists(path):
        result.fail("feed-manifest", relative,
                    "is missing; run tools/landing/render_feed_manifest.py")
        return
    with open(path) as handle:
        try:
            current = json.load(handle)
        except ValueError as error:
            result.fail("feed-manifest", relative, "is not valid JSON: %s" % error)
            return
    expected = render_feed_manifest.render()
    if current != expected:
        result.fail("feed-manifest", relative,
                    "is stale against config/landing-zone.yaml; run "
                    "tools/landing/render_feed_manifest.py")
    result.count("feed_manifest_feeds", len(expected["feeds"]))


def check_project_reference_scope(result, prefixes):
    """Project references stay inside a project; the rest is the runner's."""
    if REPO_ROOT != SOURCE_ROOT:
        return  # a scratch copy of one area is not an estate to walk
    sys.path.insert(0, os.path.join(SOURCE_ROOT, "validation", "checks"))
    import check_orchestration_edges

    report = check_orchestration_edges.analyse()
    for failure in report["failures"]:
        result.fail("orchestration-edges", "ssis/orchestration-plan.json", failure)
    result.count("orchestration_edges", report["edges_total"])
    result.count("orchestration_edges_intra", report["edges_intra_project"])
    result.count("orchestration_edges_cross", report["edges_cross_project"])
    result.count("orchestration_roots", len(report["roots"]))
    result.count("orchestration_reachable", report["packages_reachable_from_roots"])


EXEC_ARG_RE = re.compile(
    r"@\w+\s*=\s*(?P<value>[^,;\r\n]+?)\s*(?=,\s*@\w+\s*=|[,;]?\s*(?:\r?\n|$))")
EXEC_STATEMENT_RE = re.compile(r"(?is)\bEXEC(?:UTE)?\s+[\[\]\w.]+\s+(?P<args>.*?);")
# A blanked literal is '' on one line and a bare N (or nothing) where the
# literal ran over several lines.
SIMPLE_ARG_RE = re.compile(
    r"(?is)^(?:@\w+(?:\s+OUTPUT)?|NULL|DEFAULT|N?''|N|-?[\d.]+|0x[0-9a-f]+)?$")
NOISE_RE = re.compile(r"(?P<string>'(?:[^']|'')*')|(?P<line>--[^\n]*)|(?P<block>/\*.*?\*/)",
                      re.DOTALL)


def blank_literals(text):
    """Blank comment and string bodies, keeping every character offset intact.

    Argument checking runs over the result, so a literal collapses to '' and
    can never look like an expression, and nothing inside dynamic SQL or a
    comment is mistaken for a real call.
    """
    def blank(match):
        body = match.group(0)
        blanked = re.sub(r"[^\n]", " ", body)
        if match.lastgroup == "string" and "\n" not in body:
            return "''" + blanked[2:]
        return blanked

    return NOISE_RE.sub(blank, text)


def check_sql_exec_arguments(result, prefixes):
    """Stored procedure arguments must be constants, variables, NULL or DEFAULT."""
    for rel, full in walk_files(prefixes, (".sql",)):
        if not rel.startswith(("sqlserver/", "ssis/")):
            continue
        with open(full, errors="replace") as handle:
            text = blank_literals(handle.read())
        for statement in EXEC_STATEMENT_RE.finditer(text):
            args = statement.group("args")
            if "=" not in args:
                continue
            for arg in EXEC_ARG_RE.finditer(args):
                value = arg.group("value").strip()
                if SIMPLE_ARG_RE.match(value):
                    continue
                line = text[: statement.start() + arg.start()].count("\n") + 1
                result.fail("sql-exec-arguments", "%s:%d" % (rel, line),
                            "EXEC argument value %r is an expression; assign it to a "
                            "variable first" % value)


MERGE_RE = re.compile(r"(?is)\bMERGE\b.*?;")
WHEN_MATCHED_UPDATE_RE = re.compile(r"(?is)WHEN\s+MATCHED\s+THEN\s+UPDATE")


def check_sql_merge_clauses(result, prefixes):
    """A MERGE may carry at most one unconditional WHEN MATCHED THEN UPDATE."""
    for rel, full in walk_files(prefixes, (".sql",)):
        with open(full, errors="replace") as handle:
            text = blank_literals(handle.read())
        for statement in MERGE_RE.finditer(text):
            matches = WHEN_MATCHED_UPDATE_RE.findall(statement.group(0))
            if len(matches) > 1:
                line = text[: statement.start()].count("\n") + 1
                result.fail("sql-merge", "%s:%d" % (rel, line),
                            "MERGE has %d unconditional WHEN MATCHED THEN UPDATE clauses"
                            % len(matches))


def check_catalog_coverage(result, catalog, prefixes):
    """Catalog packages exist on disk, and no undeclared package exists."""
    declared = {p["package"]: p for p in catalog["ssis"]["packages"]}
    on_disk = {}
    for rel, _ in walk_files(None, (".dtsx",)):
        on_disk[os.path.basename(rel)[:-5]] = rel

    def in_scope(path):
        return not prefixes or any(path.startswith(p) for p in prefixes)

    for name, spec in sorted(declared.items()):
        expected_dir = "ssis/%s" % spec["folder"]
        if not in_scope(expected_dir):
            continue
        if name not in on_disk:
            result.fail("catalog-coverage", "%s/%s.dtsx" % (expected_dir, name),
                        "catalog declares this package but it does not exist")
        elif not on_disk[name].startswith(expected_dir + "/"):
            result.fail("catalog-coverage", on_disk[name],
                        "package is in the wrong folder; catalog says %s" % expected_dir)

    for name, rel in sorted(on_disk.items()):
        if not in_scope(rel):
            continue
        if name not in declared:
            result.fail("catalog-coverage", rel, "package is not declared in the catalog")

    result.count("catalog_packages", len(declared))
    result.count("packages_on_disk", len(on_disk))


def check_package_naming(result, catalog, prefixes):
    for rel, _ in walk_files(prefixes, (".dtsx",)):
        name = os.path.basename(rel)[:-5]
        if not name.startswith(SSIS_PREFIXES):
            result.fail("naming", rel, "package name %r does not use an approved prefix" % name)
        if not re.match(r"^[A-Za-z0-9_]+$", name):
            result.fail("naming", rel, "package name %r contains unsupported characters" % name)


def check_sql_naming(result, prefixes):
    """SQL file naming and header conventions."""
    proc_re = re.compile(r"^(?:CREATE|ALTER)\s+(PROCEDURE|FUNCTION|VIEW)\s+([\[\]\w\.]+)", re.I | re.M)
    checked = 0
    for rel, full in walk_files(prefixes, (".sql",)):
        checked += 1
        with open(full, errors="replace") as handle:
            text = handle.read()
        if not text.lstrip().startswith(("/*", "--", "SET ", "USE ")):
            result.warn("sql-header", rel, "file does not begin with a header comment")
        if text and not text.endswith("\n"):
            result.fail("sql-format", rel, "file does not end with a newline")
        if "\t" in text:
            result.fail("sql-format", rel, "file contains tab characters")
        base = os.path.basename(rel)[:-4]
        if rel.startswith("sqlserver/"):
            for kind, obj in proc_re.findall(text):
                obj = obj.replace("[", "").replace("]", "")
                short = obj.split(".")[-1]
                if kind.upper() == "PROCEDURE" and not short.lower().startswith("usp_"):
                    result.fail("naming", rel, "procedure %s does not use the usp_ prefix" % obj)
                if kind.upper() == "VIEW" and not short.lower().startswith("vw_"):
                    result.fail("naming", rel, "view %s does not use the vw_ prefix" % obj)
                if kind.upper() == "FUNCTION" and not short.lower().startswith("ufn_"):
                    result.fail("naming", rel, "function %s does not use the ufn_ prefix" % obj)
            # A file named after a single object should define it.
            if re.match(r"^[a-z]+\.(usp|ufn|vw)_\w+$", base) and base.lower() not in text.lower():
                result.fail("naming", rel, "file name %r does not match any object defined in it" % base)
    result.count("sql_files_checked", checked)


def check_duplicates(result, prefixes):
    """No two files may define the same SSIS package or SQL object."""
    packages = defaultdict(list)
    for rel, _ in walk_files(None, (".dtsx",)):
        packages[os.path.basename(rel)[:-5]].append(rel)
    for name, paths in sorted(packages.items()):
        if len(paths) > 1:
            result.fail("duplicates", paths[0], "package %r is defined in %d files: %s"
                        % (name, len(paths), ", ".join(sorted(paths))))

    objects = defaultdict(list)
    obj_re = re.compile(r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(PROCEDURE|FUNCTION|VIEW|TABLE)\s+([\[\]\w\."
                        r"]+)", re.I | re.M)
    for rel, full in walk_files(None, (".sql",)):
        with open(full, errors="replace") as handle:
            text = handle.read()
        for kind, obj in obj_re.findall(text):
            key = (kind.upper(), obj.replace("[", "").replace("]", "").upper())
            objects[key].append(rel)
    for (kind, obj), paths in sorted(objects.items()):
        unique = sorted(set(paths))
        if len(unique) > 1:
            result.fail("duplicates", unique[0],
                        "%s %s is created in %d files: %s" % (kind, obj, len(unique), ", ".join(unique)))


def check_dependency_cycles(result, catalog):
    """The declared package dependency graph must be acyclic."""
    edges = defaultdict(set)
    for spec in catalog["ssis"]["packages"]:
        parent = spec.get("parent")
        if parent:
            edges[parent].add(spec["package"])
        for dep in spec.get("depends_on", []) or []:
            edges[spec["package"]].add(dep)

    state = {}
    stack = []

    def visit(node):
        if state.get(node) == "done":
            return
        if state.get(node) == "open":
            cycle = stack[stack.index(node):] + [node]
            result.fail("dependency-cycle", "config/estate-catalog.yaml",
                        "cycle: %s" % " -> ".join(cycle))
            return
        state[node] = "open"
        stack.append(node)
        for child in sorted(edges.get(node, ())):
            visit(child)
        stack.pop()
        state[node] = "done"

    for node in sorted(edges):
        visit(node)


def _powershell_statement(text, start):
    """The text of one PowerShell statement beginning at start.

    A statement ends at its line break, except that a backtick continues it, an
    unclosed hash table or parenthesis continues it, and a here-string runs to
    its own terminator - which between them are the shape every catalog batch in
    deployment/ is written in.
    """
    def delta(fragment):
        return (fragment.count("{") + fragment.count("(") + fragment.count("[")
                - fragment.count("}") - fragment.count(")") - fragment.count("]"))

    collected = []
    here = None
    depth = 0
    for line in text[start:].split("\n"):
        collected.append(line)
        if here is not None:
            if not line.startswith(here + "@"):
                continue
            here = None
            depth += delta(line[2:])
        else:
            opened = re.search(r"@([\"'])\s*$", line)
            if opened:
                depth += delta(line[: opened.start()])
                here = opened.group(1)
                continue
            depth += delta(line)
        if depth <= 0 and not line.rstrip().endswith("`"):
            break
    return "\n".join(collected)


def check_runtime_tooling(result, prefixes):
    """The handwritten runtime drivers, checked for the contracts they broke.

    Three defect classes, each of which reached a live run before it was seen:

    - a catalog write issued over a SQL-authenticated connection. Every
      catalog procedure that writes rejects one ("The operation cannot be
      started by an account that uses SQL Server Authentication"), and
      create_execution rejects it before an execution id exists, so the run
      leaves nothing behind in catalog.operation_messages to diagnose;
    - an etl batch opened without a guaranteed close. A fault between
      usp_StartBatch and usp_EndBatch left the batch Running for ever, which
      is indistinguishable from a run still in progress;
    - a driver that executes file ingestion without first proving the landing
      zone exists on the catalog host. A Foreach loop over a missing directory
      succeeds, loads nothing, and reconciles to zero.
    """
    catalog_writers = (
        "catalog.create_execution", "catalog.start_execution",
        "catalog.set_execution_parameter_value", "catalog.deploy_project",
        "catalog.create_folder", "catalog.create_environment",
        "catalog.create_environment_variable", "catalog.create_environment_reference",
        "catalog.set_object_parameter_value", "catalog.delete_project",
    )
    integrated_switches = ("-Integrated", "-ForceIntegrated", "-IntegratedSecurity")
    drivers = 0

    for rel, full in walk_files(prefixes, (".ps1",)):
        if not rel.startswith("deployment/"):
            continue
        with open(full, errors="replace") as handle:
            text = handle.read()
        drivers += 1

        for match in re.finditer(r"\bInvoke-WwiSql(?:Query|NonQuery)\b", text):
            statement = _powershell_statement(text, match.end())
            written = [name for name in catalog_writers if name in statement]
            if not written:
                continue
            if not any(switch in statement for switch in integrated_switches):
                line = text[: match.start()].count("\n") + 1
                result.fail("runtime-tooling", "%s:%d" % (rel, line),
                            "%s is called over a connection that may authenticate with a SQL "
                            "login; the catalog procedures accept a Windows principal only"
                            % written[0])

        opens_batch = re.search(r"\bStart-EtlBatch\s+-BatchType\b", text)
        if opens_batch:
            closes = [block.start() for block in re.finditer(r"(?m)^finally\s*\{", text)]
            guarded = False
            for position in closes:
                depth = 0
                for index in range(text.index("{", position), len(text)):
                    if text[index] == "{":
                        depth += 1
                    elif text[index] == "}":
                        depth -= 1
                        if depth == 0:
                            body = text[position:index]
                            guarded = guarded or "Stop-EtlBatch" in body or "usp_EndBatch" in body
                            break
            if not guarded:
                line = text[: opens_batch.start()].count("\n") + 1
                result.fail("runtime-tooling", "%s:%d" % (rel, line),
                            "opens an etl batch with no finally block that closes it; a fault "
                            "would leave etl.Batch Running with no etl.ErrorLog row")
            if "usp_LogError" not in text:
                line = text[: opens_batch.start()].count("\n") + 1
                result.fail("runtime-tooling", "%s:%d" % (rel, line),
                            "opens an etl batch but never writes etl.usp_LogError, so a runner "
                            "fault would be recorded nowhere in the control framework")

        if "catalog.create_execution" in text and opens_batch:
            for helper, why in (
                ("Assert-WwiCatalogWindowsAuthentication",
                 "the catalog connection is not proved to be a Windows principal before a "
                 "batch is opened"),
                ("Assert-WwiExecutionHostLandingZone",
                 "the landing zone is not proved to exist on the catalog host before a batch "
                 "is opened"),
            ):
                call = re.search(r"\b%s\b" % helper, text)
                if not call or call.start() > opens_batch.start():
                    line = text[: opens_batch.start()].count("\n") + 1
                    result.fail("runtime-tooling", "%s:%d" % (rel, line), why)

    result.count("runtime drivers checked", drivers)


# DTS:DataType on a package parameter is an OLE Automation VARTYPE, and
# catalog.object_parameters stores the CLR name SSIS derives from it. The
# runner has to hand catalog.set_execution_parameter_value a sql_variant whose
# base type is that CLR type: the procedure compares, it does not convert.
SSIS_VARTYPE_CLR = {
    "2": "Int16", "3": "Int32", "4": "Single", "5": "Double", "6": "Decimal",
    "7": "DateTime", "8": "String", "11": "Boolean", "14": "Decimal",
    "16": "SByte", "17": "Byte", "18": "UInt16", "19": "UInt32",
    "20": "Int64", "21": "UInt64",
}


def check_execution_parameter_binding(result, prefixes):
    """Typed catalog parameter binding, batch adoption, and BOM-free SSM payloads.

    Three defects that only a live run has ever shown:

    - a package parameter bound as text. catalog.create_execution commits the
      execution before any parameter is set, so a base-type mismatch faults
      with "The data type of the input value is not compatible with the data
      type of the 'Int32'" and strands the execution in status 1 with nothing
      in catalog.operation_messages;
    - a runner that hardcodes @AllowAdoptRunning = 0, which makes the recovery
      rerun etl.usp_StartBatch documents impossible without editing the runner;
    - an SSM --parameters payload written with Set-Content -Encoding utf8. On
      Windows PowerShell that emits a BOM and the AWS CLI refuses the file with
      "Expected: '=', received: '\\ufeff'".
    """
    runners = 0
    for rel, full in walk_files(prefixes, (".ps1",)):
        if not rel.startswith("deployment/"):
            continue
        with open(full, errors="replace") as handle:
            text = handle.read()

        if "catalog.set_execution_parameter_value" in text:
            runners += 1
            if "ConvertTo-WwiExecutionParameter" not in text:
                result.fail("execution-parameters", rel,
                            "sets execution parameter values without converting them to the "
                            "types catalog.object_parameters declares for the package")
            cast = re.search(r"pvalue\$?\w*\"?\]\s*=\s*\[(string|int|long)\]", text)
            if cast:
                line = text[: cast.start()].count("\n") + 1
                result.fail("execution-parameters", "%s:%d" % (rel, line),
                            "casts every execution parameter to [%s]; the value must carry the "
                            "CLR type the package declares" % cast.group(1))

        adopt = re.search(r"@AllowAdoptRunning\s*=\s*0\b", text)
        if adopt and "usp_StartBatch" in text:
            line = text[: adopt.start()].count("\n") + 1
            result.fail("execution-parameters", "%s:%d" % (rel, line),
                        "hardcodes @AllowAdoptRunning = 0, so a batch left Running by an "
                        "interrupted attempt cannot be adopted without editing the runner")

        for match in re.finditer(r"(?m)^.*Set-Content[^\n]*-Encoding\s+utf8[^\n]*$", text):
            statement = match.group(0)
            variable = re.search(r"-LiteralPath\s+\$(\w+)", statement)
            if not variable:
                continue
            if ("file://" in text and re.search(r"--parameters[^\n]*\$%s\b" % variable.group(1), text)):
                line = text[: match.start()].count("\n") + 1
                result.fail("ssm-payload", "%s:%d" % (rel, line),
                            "writes the SSM --parameters payload with Set-Content -Encoding "
                            "utf8, which emits a BOM the AWS CLI rejects; write it with "
                            "UTF8Encoding($false)")

    result.count("execution parameter binders checked", runners)

    # Every parameter type the estate declares must have a binding in the
    # runner's map, or the first run that passes one faults inside the catalog.
    helper = os.path.join(REPO_ROOT, "deployment", "lib", "SsisCatalog.ps1")
    if not os.path.exists(helper):
        return
    with open(helper, errors="replace") as handle:
        helper_text = handle.read()
    block = re.search(r"WwiSsisParameterClrType\s*=\s*\[ordered\]\s*@\{(.*?)\n\}",
                      helper_text, re.S)
    if not block:
        result.fail("execution-parameters", "deployment/lib/SsisCatalog.ps1",
                    "no $script:WwiSsisParameterClrType map; the runner cannot know what CLR "
                    "type a declared package parameter needs")
        return
    known = set(re.findall(r"'(\w+)'\s*=", block.group(1)))

    declared = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        with open(full, errors="replace") as handle:
            text = handle.read()
        for parameter in re.finditer(r"<DTS:PackageParameter\b(.*?)>", text, re.S):
            attributes = parameter.group(1)
            name = re.search(r'DTS:ObjectName="([^"]+)"', attributes)
            vartype = re.search(r'DTS:DataType="(\d+)"', attributes)
            if not name or not vartype:
                continue
            declared += 1
            clr = SSIS_VARTYPE_CLR.get(vartype.group(1))
            if clr is None:
                result.fail("execution-parameters", rel,
                            "package parameter %s is declared with DTS:DataType %s, which the "
                            "runner has no CLR mapping for" % (name.group(1), vartype.group(1)))
            elif clr not in known:
                result.fail("execution-parameters", rel,
                            "package parameter %s is a %s, which $script:WwiSsisParameterClrType "
                            "in deployment/lib/SsisCatalog.ps1 cannot bind"
                            % (name.group(1), clr))
    result.count("package parameters typed", declared)


def check_no_forbidden_content(result, prefixes):
    for rel, full in walk_files(prefixes, (".sql", ".dtsx", ".py", ".md", ".yaml", ".yml",
                                           ".json", ".ps1", ".sh", ".csv", ".conmgr", ".params")):
        if rel in POLICY_DOCUMENTS:
            continue
        with open(full, errors="replace") as handle:
            text = handle.read()
        for pattern, label in FORBIDDEN_CONTENT:
            match = pattern.search(text)
            if match:
                line = text[: match.start()].count("\n") + 1
                result.fail("forbidden-content", "%s:%d" % (rel, line),
                            "%s: %r" % (label, match.group(0)))


def check_no_credentials(result, prefixes):
    for rel, full in walk_files(prefixes, (".sql", ".dtsx", ".py", ".yaml", ".yml", ".json",
                                           ".ps1", ".sh", ".conmgr", ".params", ".config", ".env")):
        if rel == "validation/static/run_all_checks.py":
            continue
        with open(full, errors="replace") as handle:
            lines = handle.readlines()
        for number, line in enumerate(lines, start=1):
            for pattern, label in CREDENTIAL_PATTERNS:
                match = pattern.search(line)
                if not match:
                    continue
                if any(token in line for token in CREDENTIAL_ALLOWED_TOKENS):
                    continue
                result.fail("credentials", "%s:%d" % (rel, number), "%s" % label)


def check_no_runtime_claims(result, prefixes):
    """Documentation must not assert execution that never happened."""
    claims = [
        re.compile(r"(?i)\b(?:successfully|verified|confirmed)\s+(?:executed|ran|deployed|loaded)"),
        re.compile(r"(?i)\btested\s+against\s+(?:oracle|sql\s*server|a\s+live)"),
        re.compile(r"(?i)\brow\s+counts?\s+(?:were\s+)?(?:verified|confirmed|match(?:ed)?)\s+in\s+production"),
        re.compile(r"(?i)\bdeployment\s+(?:succeeded|was\s+successful)\b"),
    ]
    for rel, full in walk_files(prefixes, (".md",)):
        if "known-unvalidated" in rel:
            continue
        with open(full, errors="replace") as handle:
            text = handle.read()
        for pattern in claims:
            match = pattern.search(text)
            if match:
                line = text[: match.start()].count("\n") + 1
                result.fail("runtime-claims", "%s:%d" % (rel, line),
                            "unsupported runtime claim: %r" % match.group(0))


def check_relative_references(result, prefixes):
    """Scripts referenced from deployment/config files must exist."""
    ref_re = re.compile(r"(?:^|[\s\"'(=:])((?:\./)?(?:oracle|sqlserver|ssis|generators|deployment|"
                        r"config|validation|tools|docs)/[\w./\-]+\.(?:sql|py|dtsx|yaml|yml|json|ps1|sh|csv|md))")
    for rel, full in walk_files(prefixes, (".md", ".ps1", ".sh", ".yaml", ".yml", ".json")):
        with open(full, errors="replace") as handle:
            text = handle.read()
        for match in ref_re.finditer(text):
            target = match.group(1).lstrip("./")
            if "*" in target or "<" in target:
                continue
            if not os.path.exists(os.path.join(REPO_ROOT, target)):
                line = text[: match.start()].count("\n") + 1
                result.fail("broken-reference", "%s:%d" % (rel, line),
                            "references missing file %r" % target)


def check_attribution(result):
    """The WideWorldImporters attribution and licence must survive the expansion."""
    for required in ("ATTRIBUTION.md", "README.md"):
        path = os.path.join(REPO_ROOT, required)
        if not os.path.exists(path):
            result.fail("attribution", required, "required file is missing")
    readme = os.path.join(REPO_ROOT, "README.md")
    if os.path.exists(readme):
        with open(readme, errors="replace") as handle:
            text = handle.read()
        if "wide world importers" not in text.lower() and "wideworldimporters" not in text.lower():
            result.fail("attribution", "README.md",
                        "README no longer references the WideWorldImporters sample")


def _expression_statements(text):
    """Statements the SSIS expression evaluator reads in *text*.

    Deliberately independent of tools/ssisgen: this is the artifact the run time
    parses, so it is checked without asking the generator what it meant.
    """
    count = 1
    in_literal = False
    escaped = False
    for char in text:
        if escaped:
            escaped = False
        elif char == "\\" and in_literal:
            escaped = True
        elif char == '"':
            in_literal = not in_literal
        elif char == ";" and not in_literal:
            count += 1
    return count


def check_expression_task_statements(result, prefixes):
    """An Expression Task holds exactly one expression.

    The task hands its whole Expression attribute to the evaluator, which reads
    one expression and refuses the separator, so several assignments strung
    together with semicolons fail task validation before anything runs:

        Attempt to parse the expression ... failed. The token ";" at line
        number "1", character number "93" was not recognized.

    which is how executions 102-104 failed. Several assignments belong in
    several tasks, sequenced so the order still holds.
    """
    tasks = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for node in root.iter():
            if not node.tag.endswith("ExpressionTask"):
                continue
            tasks += 1
            expression = ""
            for name, value in node.attrib.items():
                if name.endswith("Expression"):
                    expression = value
            statements = _expression_statements(expression)
            if statements > 1:
                result.fail("expression-task-statements", rel,
                            "expression task carries %d statements; the evaluator reads one "
                            "expression and refuses the ';' token: %r"
                            % (statements, expression[:120]))
    result.count("expression_tasks_checked", tasks)


def _table_columns():
    """Column names per schema-qualified table, from the deployed DDL.

    Read from the checked-in tree rather than the walked one: the DDL is the
    contract a package is judged against, and the negative fixtures point the
    checks at a scratch copy holding only the artifact under test.
    """
    create_re = re.compile(r"CREATE\s+TABLE\s+\[?(\w+)\]?\.\[?(\w+)\]?\s*\(", re.I)
    skip = ("CONSTRAINT", "INDEX", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "PERIOD", "WITH")
    tables = {}
    sql_files = []
    for directory, _dirs, names in os.walk(os.path.join(SOURCE_ROOT, "sqlserver")):
        sql_files.extend(os.path.join(directory, name) for name in names
                         if name.endswith(".sql"))
    for full in sorted(sql_files):
        with open(full, errors="replace") as handle:
            text = handle.read()
        for match in create_re.finditer(text):
            table = "%s.%s" % (match.group(1).lower(), match.group(2).lower())
            columns = set()
            depth = 1
            for line in text[match.end():].splitlines():
                depth += line.count("(") - line.count(")")
                token = line.strip().lstrip("[").split(" ")[0].rstrip("],")
                if token.upper() not in skip and re.match(r"^\w+$", token):
                    columns.add(token.lower())
                if depth <= 0:
                    break
            tables.setdefault(table, set()).update(columns)
    return tables


def _sql_contract_baseline():
    """Package/column pairs already known to name a column their table lacks."""
    path = os.path.join(SOURCE_ROOT, "validation", "static",
                        "sql-column-contract-baseline.txt")
    if not os.path.exists(path):
        return set()
    entries = set()
    with open(path, errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#"):
                entries.add(line)
    return entries


def check_sql_column_contract(result, prefixes):
    """A package's SELECT list must name columns the target table has.

    A generated statement is never compiled until the component validates it
    against the server, where a name the table does not carry fails the whole
    data flow:

        "The batch could not be analyzed because of compile errors.";
        "Invalid column name 'AmountText'." ... validation status "VS_ISBROKEN"

    which is how execution 105 failed. Only single-table statements over tables
    this repository owns the DDL for are checked, and only the bare column names
    in their select list, so the check reports a broken contract rather than the
    limits of a SQL parser.

    The estate carries the same defect in packages outside the failing run; those
    pairs are listed in sql-column-contract-baseline.txt and reported as warnings
    so this check guards what has been repaired without hiding the rest. A pair
    that leaves the baseline may not come back.
    """
    baseline = _sql_contract_baseline()
    dts_sq = "www.microsoft.com/sqlserver/dts/tasks/sqltask"
    select_re = re.compile(r"^\s*SELECT\s+(?P<list>.+?)\s+FROM\s+\[?(?P<schema>\w+)\]?\.\[?(?P<table>\w+)\]?"
                           r"(?P<rest>\s|;|$)", re.I | re.S)
    tables = _table_columns()
    statements = 0
    seen = set()
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        sql_texts = []
        for component in root.iter("component"):
            for node in component.iter("property"):
                if node.get("name") == "SqlCommand" and (node.text or "").strip():
                    sql_texts.append((component.get("name"), node.text))
        for task in root.iter("{%s}SqlTaskData" % dts_sq):
            sql = task.get("{%s}SqlStatementSource" % dts_sq)
            if sql:
                sql_texts.append(("Execute SQL Task", sql))
        for name, sql in sql_texts:
            match = select_re.match(sql)
            if not match:
                continue
            table = "%s.%s" % (match.group("schema").lower(), match.group("table").lower())
            known = tables.get(table)
            if not known:
                continue
            tail = sql[match.end("table"):].upper()
            if " JOIN " in tail or "," in sql[match.end("table"):].split("WHERE")[0]:
                continue  # more than one table in play
            column_list = match.group("list")
            if "(" in column_list or "*" in column_list or " SELECT " in column_list.upper():
                continue  # expressions and sub-selects are not a column contract
            statements += 1
            for item in column_list.split(","):
                column = item.strip().strip("[]")
                if not re.match(r"^\w+$", column):
                    continue
                if column.lower() not in known:
                    detail = ("%s selects %r from %s, which has no such column"
                              % (name, column, table))
                    key = "%s|%s" % (rel, detail)
                    if key in baseline:
                        seen.add(key)
                        result.warn("sql-column-contract", rel,
                                    "known contract drift: %s" % detail)
                    else:
                        result.fail("sql-column-contract", rel, detail)
    for key in sorted(baseline - seen):
        rel, _, detail = key.partition("|")
        result.warn("sql-column-contract", rel,
                    "baseline entry no longer reported, remove it: %s" % detail)
    result.count("single_table_selects_checked", statements)
    result.count("sql_contract_drift_baselined", len(seen))


def _ddl_catalog():
    """The deployed DDL as ordered column metadata, or None when unreadable."""
    path = os.path.join(SOURCE_ROOT, "tools", "ssisgen")
    if path not in sys.path:
        sys.path.insert(0, path)
    try:
        import dbschema
    except ImportError:
        return None
    return dbschema


def _destination_debt(dbschema):
    """Destinations recorded as still unable to honour their table contract."""
    path = os.path.join(SOURCE_ROOT, "ssis", "destination-metadata-debt.txt")
    entries = {}
    if not os.path.exists(path):
        return entries
    with open(path, errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            flow, destination, table, reason = (line.split("|") + ["", "", "", ""])[:4]
            entries["%s|%s|%s" % (flow.strip(), destination.strip(),
                                  dbschema.normalise_table(table.strip()))] = reason
    return entries


def check_destination_metadata(result, prefixes):
    """An OLE DB destination's external metadata must be its target table's.

    The destination revalidates its cached external columns against OpenRowset
    when the package starts, and a cached shape the table does not have fails
    the whole task before a row moves:

        "raw FilePartnerSales NA" failed validation and returned validation
        status "VS_NEEDSNEWMETADATA"

    which is how executions 144-147 failed. A destination that publishes its
    buffer's columns instead of its table's - the defect behind that run - is
    reported here, offline, against the DDL this repository deploys. Columns the
    table has and the flow does not supply are legitimate (the server defaults
    them), so only NOT NULL columns without a default are required.

    Destinations still carrying the defect are listed in
    ssis/destination-metadata-debt.txt and reported as warnings; an entry that
    no longer reproduces has to be removed.
    """
    dbschema = _ddl_catalog()
    if dbschema is None:
        result.fail("destination-metadata", "tools/ssisgen/dbschema.py",
                    "the DDL catalog could not be imported, so no destination "
                    "could be checked against its table")
        return
    debt = _destination_debt(dbschema)
    seen = set()
    destinations = 0
    for rel, full in walk_files(prefixes, (".dtsx",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        for component in root.iter("component"):
            if component.get("componentClassID") != "Microsoft.OLEDBDestination":
                continue
            properties = _component_properties(component)
            table = (properties.get("OpenRowset") or "").strip()
            name = component.get("name") or "?"
            ref = component.get("refId") or ""
            flow = ref.split("\\")[1] if ref.count("\\") >= 2 else ""
            if not table or not dbschema.has_table(table):
                continue
            destinations += 1
            key = "%s|%s|%s" % (flow, name, dbschema.normalise_table(table))
            registered = key in debt
            columns = dbschema.table_columns(table)
            declared = [node.get("name") for node in
                        component.iter("externalMetadataColumn")]
            mapped = set()
            for node in component.iter("inputColumn"):
                external = node.get("externalMetadataColumnId") or ""
                if "ExternalColumns[" in external:
                    mapped.add(external.rsplit("ExternalColumns[", 1)[1].rstrip("]"))
            problems = []
            if not mapped:
                # SSIS reports an input with no columns as VS_ISBROKEN when the
                # package validates: "The number of input columns for ...
                # cannot be zero."
                problems.append("has no input column, so its input into %s is empty" % table)
            if declared != [col.name for col in columns]:
                problems.append("declares external columns %s; %s has %s"
                                % (", ".join(declared) or "none", table,
                                   ", ".join(col.name for col in columns)))
            else:
                missing = [col.name for col in columns
                           if col.name not in mapped and not col.nullable
                           and not col.identity and not col.has_default]
                if missing:
                    problems.append("maps no value into NOT NULL column(s) %s of %s"
                                    % (", ".join(missing), table))
            for problem in problems:
                detail = "destination %r in %r %s" % (name, flow, problem)
                if registered:
                    seen.add(key)
                    result.warn("destination-metadata", rel,
                                "known destination debt: %s" % detail)
                else:
                    result.fail("destination-metadata", rel, detail)
    for key in sorted(set(debt) - seen):
        flow, destination, table = key.split("|")
        if prefixes:
            continue  # a restricted walk cannot prove an entry is obsolete
        if not dbschema.has_table(table):
            continue  # the register also pins destinations with no DDL at all
        result.warn("destination-metadata", "ssis/destination-metadata-debt.txt",
                    "entry no longer reproduces, remove it: %s|%s|%s"
                    % (flow, destination, table))
    result.count("oledb_destinations_checked", destinations)


CHECKS = [
    ("xml", check_dtsx_xml),
    ("dtsx-structure", check_dtsx_structure),
    ("dtsx-references", check_dtsx_references),
    ("dtsx-pipeline", check_dtsx_pipeline),
    ("dtsx-connection-refs", check_dtsx_connection_refs),
    ("component-contracts", check_component_contracts),
    ("input-column-cache", check_input_column_cache),
    ("input-column-disposition", check_written_input_dispositions),
    ("lookup-reference-mapping", check_lookup_reference_mapping),
    ("foreach-file-enumerator", check_foreach_file_enumerators),
    ("single-row-result-set", check_single_row_result_sets),
    ("execute-sql-parameters", check_execute_sql_parameters),
    ("expression-task-statements", check_expression_task_statements),
    ("sql-column-contract", check_sql_column_contract),
    ("destination-metadata", check_destination_metadata),
    ("dtproj-structure", check_dtproj_structure),
    ("conmgr-binding", check_conmgr_binding),
    ("conmgr-credentials", check_conmgr_credentials),
    ("catalog-binding", check_catalog_binding_surface),
    ("environment-binding", check_environment_binding_parity),
    ("file-locality", check_file_locality),
    ("file-task-validation", check_variable_driven_file_tasks),
    ("oledb-source-properties", check_oledb_source_properties),
    ("feed-manifest", check_feed_manifest),
    ("orchestration-edges", check_project_reference_scope),
    ("sql-exec-arguments", check_sql_exec_arguments),
    ("sql-merge", check_sql_merge_clauses),
    ("package-naming", check_package_naming),
    ("sql-naming", check_sql_naming),
    ("duplicates", check_duplicates),
    ("runtime-tooling", check_runtime_tooling),
    ("execution-parameters", check_execution_parameter_binding),
    ("ssm-payload", check_execution_parameter_binding),
    ("forbidden-content", check_no_forbidden_content),
    ("credentials", check_no_credentials),
    ("runtime-claims", check_no_runtime_claims),
    ("broken-references", check_relative_references),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--path", action="append", default=[],
                        help="repository-relative path prefix to restrict file checks to")
    parser.add_argument("--json", action="store_true", help="emit machine-readable output")
    args = parser.parse_args()

    prefixes = [p.rstrip("/") for p in args.path] or None
    catalog = load_catalog()
    result = Result()

    check_dtsx_xml(result, prefixes)
    check_dtsx_structure(result, prefixes)
    check_dtsx_references(result, prefixes)
    check_dtsx_pipeline(result, prefixes)
    check_dtsx_connection_refs(result, prefixes)
    check_component_contracts(result, prefixes)
    check_input_column_cache(result, prefixes)
    check_written_input_dispositions(result, prefixes)
    check_lookup_reference_mapping(result, prefixes)
    check_foreach_file_enumerators(result, prefixes)
    check_single_row_result_sets(result, prefixes)
    check_execute_sql_parameters(result, prefixes)
    check_expression_task_statements(result, prefixes)
    check_sql_column_contract(result, prefixes)
    check_destination_metadata(result, prefixes)
    check_dtproj_structure(result, prefixes)
    check_conmgr_binding(result, prefixes)
    check_conmgr_credentials(result, prefixes)
    check_catalog_binding_surface(result, prefixes)
    check_environment_binding_parity(result, prefixes)
    check_file_locality(result, prefixes)
    check_file_system_tasks(result, prefixes)
    check_variable_driven_file_tasks(result, prefixes)
    check_oledb_source_properties(result, prefixes)
    check_feed_manifest(result, prefixes)
    check_project_reference_scope(result, prefixes)
    check_sql_exec_arguments(result, prefixes)
    check_sql_merge_clauses(result, prefixes)
    check_catalog_coverage(result, catalog, prefixes)
    check_package_naming(result, catalog, prefixes)
    check_sql_naming(result, prefixes)
    check_duplicates(result, prefixes)
    check_dependency_cycles(result, catalog)
    check_runtime_tooling(result, prefixes)
    check_execution_parameter_binding(result, prefixes)
    check_no_forbidden_content(result, prefixes)
    check_no_credentials(result, prefixes)
    check_no_runtime_claims(result, prefixes)
    check_relative_references(result, prefixes)
    check_attribution(result)

    if args.json:
        print(json.dumps({"failures": result.failures, "warnings": result.warnings,
                          "counts": result.counts}, indent=2, sort_keys=True))
    else:
        for warning in result.warnings:
            print("WARN  [%s] %s: %s" % (warning["check"], warning["path"], warning["message"]))
        for failure in result.failures:
            print("FAIL  [%s] %s: %s" % (failure["check"], failure["path"], failure["message"]))
        print("")
        for key in sorted(result.counts):
            print("%-24s %s" % (key, result.counts[key]))
        print("")
        print("%d failure(s), %d warning(s)" % (len(result.failures), len(result.warnings)))
        print("Static checks only - nothing here was executed against a live system.")

    return 1 if result.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
