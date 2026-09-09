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


# OLE DB Driver 19 spells the keyword with spaces; TrustServerCertificate is a
# .NET SqlClient keyword the provider silently ignores, so a connection to an
# instance with an untrusted certificate fails with the chain error instead.
TLS_KEYWORD = "Trust Server Certificate=True;"
TLS_KEYWORD_WRONG = re.compile(r"TrustServerCertificate\s*=")


SENSITIVE_TOKEN_RE = re.compile(
    r"@\[\$Project::(\w*(?:Password|Secret|Pwd)\w*)\]", re.IGNORECASE)


def check_conmgr_credentials(result, prefixes):
    """No connection expression may name a credential.

    A Sensitive project parameter cannot be read by the SSIS expression
    evaluator: the package fails validation with 0xC0017010 before the
    credential would ever be used. The password therefore travels on the
    connection manager's own ``CM.<connection>.Password`` project parameter,
    which the runtime applies to the connection manager object before the
    ConnectionString expression is evaluated, and which the catalog environment
    binds by reference. Everything else about the connection string - host,
    port, service, catalog, provider, user, TLS - stays in the expression.
    """
    ns = {"DTS": "www.microsoft.com/SqlServer/Dts"}
    oracle = sql = 0
    for rel, full in walk_files(prefixes, (".conmgr",)):
        try:
            root = ET.parse(full).getroot()
        except ET.ParseError:
            continue
        expression = ""
        for node in root.iter("{%s}PropertyExpression" % ns["DTS"]):
            if node.get("{%s}Name" % ns["DTS"]) == "ConnectionString":
                expression = node.text or ""
        if not expression:
            continue
        for match in SENSITIVE_TOKEN_RE.finditer(expression):
            result.fail("conmgr-credentials", rel,
                        "connection expression references the sensitive project parameter "
                        "$Project::%s; a sensitive parameter cannot be read by the "
                        "expression evaluator (0xC0017010) - bind the credential through "
                        "CM.<connection>.Password" % match.group(1))
        if "OracleProvider" in expression:
            oracle += 1
            if "@[$Project::OracleUser]" not in expression:
                result.fail("conmgr-credentials", rel,
                            "Oracle connection expression does not consume $Project::OracleUser")
        if "SqlServerProvider" in expression:
            sql += 1
            if "Integrated Security=SSPI;" not in expression:
                result.fail("conmgr-credentials", rel,
                            "SQL Server connection expression has no Windows authentication branch")
            if TLS_KEYWORD not in expression:
                result.fail("conmgr-credentials", rel,
                            "SQL Server connection expression does not emit %r" % TLS_KEYWORD)
            if TLS_KEYWORD_WRONG.search(expression):
                result.fail("conmgr-credentials", rel,
                            "SQL Server connection expression emits the unspaced TrustServerCertificate "
                            "keyword, which OLE DB Driver 19 ignores")
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
            if enumerator.find(".//FEFE") is None:
                continue  # not a file enumerator
            loops += 1
            name = executable.get("{%s}ObjectName" % dts)
            expressions = [node.text or "" for node in executable
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


CHECKS = [
    ("xml", check_dtsx_xml),
    ("dtsx-structure", check_dtsx_structure),
    ("dtsx-references", check_dtsx_references),
    ("dtsx-pipeline", check_dtsx_pipeline),
    ("dtproj-structure", check_dtproj_structure),
    ("conmgr-binding", check_conmgr_binding),
    ("conmgr-credentials", check_conmgr_credentials),
    ("catalog-binding", check_catalog_binding_surface),
    ("file-locality", check_file_locality),
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
    check_dtproj_structure(result, prefixes)
    check_conmgr_binding(result, prefixes)
    check_conmgr_credentials(result, prefixes)
    check_catalog_binding_surface(result, prefixes)
    check_file_locality(result, prefixes)
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
