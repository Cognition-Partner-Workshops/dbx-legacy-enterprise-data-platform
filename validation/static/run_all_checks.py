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
        expressions = [node for node in root.iter("{%s}PropertyExpression" % ns["DTS"])
                       if node.get("{%s}Name" % ns["DTS"]) == "ConnectionString"]
        if not expressions:
            result.fail("conmgr-binding", rel,
                        "no ConnectionString property expression; the connection cannot be "
                        "retargeted at run time")
        for node in root.iter():
            literal = node.get("{%s}ConnectionString" % ns["DTS"])
            if literal and "@[$" in literal:
                result.fail("conmgr-binding", rel,
                            "ConnectionString holds the unevaluated token %r" % literal)
    result.count("conmgr_files_checked", checked)


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


CHECKS = [
    ("xml", check_dtsx_xml),
    ("dtsx-structure", check_dtsx_structure),
    ("dtsx-references", check_dtsx_references),
    ("dtsx-pipeline", check_dtsx_pipeline),
    ("dtproj-structure", check_dtproj_structure),
    ("conmgr-binding", check_conmgr_binding),
    ("sql-exec-arguments", check_sql_exec_arguments),
    ("sql-merge", check_sql_merge_clauses),
    ("package-naming", check_package_naming),
    ("sql-naming", check_sql_naming),
    ("duplicates", check_duplicates),
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
    check_dtproj_structure(result, prefixes)
    check_conmgr_binding(result, prefixes)
    check_sql_exec_arguments(result, prefixes)
    check_sql_merge_clauses(result, prefixes)
    check_catalog_coverage(result, catalog, prefixes)
    check_package_naming(result, catalog, prefixes)
    check_sql_naming(result, prefixes)
    check_duplicates(result, prefixes)
    check_dependency_cycles(result, catalog)
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
