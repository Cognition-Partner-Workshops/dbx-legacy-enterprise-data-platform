#!/usr/bin/env python3
"""Extract static SQL, transformation and table lineage from SSIS packages.

Usage:
    python3 validation/checks/extract_sql_lineage.py [--json] [--strict]
    [--quiet] [--no-write]

The check reads checked-in XML and SQL only. It never connects to a database,
SSIS, Oracle or a file share.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict

import estatelib as lib
import tsqllib


DTS = "{%s}" % lib.DTS_NS
SQLTASK = "www.microsoft.com/sqlserver/dts/tasks/sqltask"
SSIS = "www.microsoft.com/SqlServer/SSIS"
EXECUTABLE = DTS + "Executable"

IDENT = r"(?:\[[^\]]+\]|[A-Za-z_][\w$#]*)"
OBJECT = r"%s(?:\s*\.\s*%s){1,2}" % (IDENT, IDENT)
OBJECT_RE = re.compile(r"(?<![\w@#])(%s)(?![\w])" % OBJECT)
KEYWORD_OBJECT_RE = re.compile(
    r"\b(?:FROM|JOIN|USING)\s+(%s)" % OBJECT, re.I)
WRITE_RE = (
    (re.compile(r"\bINSERT\s+INTO\s+(%s)" % OBJECT, re.I), "insert"),
    (re.compile(r"\bUPDATE\s+(%s)" % OBJECT, re.I), "update"),
    (re.compile(r"\bMERGE\s+(?:INTO\s+)?(%s)" % OBJECT, re.I), "merge"),
    (re.compile(r"\bDELETE\s+(?:FROM\s+)?(%s)" % OBJECT, re.I), "delete"),
    (re.compile(r"\bTRUNCATE\s+TABLE\s+(%s)" % OBJECT, re.I), "truncate"),
)
SELECT_INTO_RE = re.compile(
    r"\bSELECT\b.*?\bINTO\s+(%s)" % OBJECT, re.I | re.S)
UPDATE_ALIAS_RE = re.compile(
    r"\bUPDATE\s+([A-Za-z_][\w$]*)\s+SET\b(.*?)(?=;|$)",
    re.I | re.S)
DELETE_ALIAS_RE = re.compile(
    r"\bDELETE\s+([A-Za-z_][\w$]*)\s+FROM\b(.*?)(?=;|$)", re.I | re.S)
ALIAS_SOURCE_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+(%s)(?:\s+AS)?\s+([A-Za-z_][\w$]*)" % OBJECT,
    re.I)
EXEC_RE = re.compile(r"\bEXEC(?:UTE)?\s+(%s)" % OBJECT, re.I)
CALL_RE = re.compile(r"\bCALL\s+(%s)" % OBJECT, re.I)
CONNECTION_REF_RE = re.compile(
    r"Project\.ConnectionManagers\[([^\]]+)\]", re.I)
SQL_CREATE_RE = re.compile(
    r"(?im)^\s*CREATE\s+(?:OR\s+ALTER\s+)?(?:PROC|PROCEDURE)\s+"
    r"(%s)" % OBJECT)
ORACLE_PACKAGE_RE = re.compile(
    r"(?is)\bPACKAGE\s+BODY\s+(%s)\s+AS\b" % OBJECT)
ORACLE_MEMBER_RE = re.compile(
    r"(?im)^\s*(PROCEDURE|FUNCTION)\s+([A-Za-z_][\w$#]*)\b")
CREATE_BOUNDARY_RE = re.compile(r"(?im)^\s*(?:CREATE\b|GO\b)")

LINEAGE_COLUMNS = (
    "package", "folder", "step_order", "step_type", "step_name",
    "component", "connection", "direction", "object", "layer", "system",
    "declared", "via",
)
EDGE_COLUMNS = (
    "from_object", "from_layer", "to_object", "to_layer", "package", "via",
)
ORACLE_SCHEMAS = {
    "WWI_MDM", "WWI_PROC", "WWI_FIN", "WWI_REF", "WWI_AUDIT",
}
KNOWN_SCHEMA_FALLBACKS = {
    "ETL", "ERR", "WORK", "RAW", "STG", "REF", "INTEGRATION",
    "DIMENSION", "FACT", "AGGREGATE", "REPORT", "CUSTOMER360", "DBO",
    "APPLICATION", "SALES", "PURCHASING", "WAREHOUSE", "WEBSITE",
    "POWERBI", "REPORTS", "SEQUENCES", "DATALOADSIMULATION",
}


def _known_schemas():
    schemas = set(KNOWN_SCHEMA_FALLBACKS) | ORACLE_SCHEMAS
    try:
        catalog = lib.load_catalog()
        schemas.update(key.split(".", 1)[0]
                       for key in lib.catalog_objects(catalog))
    except (OSError, KeyError, TypeError):
        pass
    try:
        schemas.update(key.split(".", 1)[0]
                       for key in tsqllib.load_tables())
    except (OSError, KeyError, TypeError):
        pass
    return {schema.upper() for schema in schemas}


KNOWN_SCHEMAS = _known_schemas()
SKIPPED_NON_SCHEMA_REFS = Counter()


def local_name(tag):
    return tag.rsplit("}", 1)[-1]


def attr(element, suffix, default=""):
    for key, value in element.attrib.items():
        if key == suffix or key.endswith("}" + suffix):
            return value
    return default


def normal_object(raw):
    value = lib.normalise_object(raw)
    if not value or "." not in value:
        return ""
    parts = value.split(".")
    if any(part.startswith("@") or part.startswith("#") for part in parts):
        return ""
    if parts[0] not in KNOWN_SCHEMAS:
        SKIPPED_NON_SCHEMA_REFS[value] += 1
        return ""
    if parts[0] in {"SYS", "INFORMATION_SCHEMA"}:
        return ""
    return value


def sql_objects(text):
    """Return normalized (reads, writes, execs) found in pragmatic SQL."""
    clean = tsqllib.strip_comments(text or "")
    reads = set()
    writes = set()
    execs = set()

    def add(target, raw):
        value = normal_object(raw)
        if value:
            target.add(value)

    for pattern, _kind in WRITE_RE:
        for match in pattern.finditer(clean):
            add(writes, match.group(1))
    for match in SELECT_INTO_RE.finditer(clean):
        add(writes, match.group(1))

    for match in KEYWORD_OBJECT_RE.finditer(clean):
        add(reads, match.group(1))
    for match in CALL_RE.finditer(clean):
        add(execs, match.group(1))
    for match in EXEC_RE.finditer(clean):
        add(execs, match.group(1))

    # Capture comma-separated tables in a FROM list without treating
    # qualified column references in JOIN/WHERE expressions as tables.
    from_re = re.compile(
        r"\bFROM\s+(.+?)(?=\bWHERE\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|"
        r"\bHAVING\b|\bUNION\b|\bEXCEPT\b|\bINTERSECT\b|\bJOIN\b|\bON\b|;|$)",
        re.I | re.S)
    for match in from_re.finditer(clean):
        for part in match.group(1).split(","):
            table = re.match(r"\s*(%s)" % OBJECT, part)
            if table:
                add(reads, table.group(1))

    # UPDATE/DELETE aliases are common in the estate. Resolve the alias to
    # the qualified source in the same statement.
    for pattern in (UPDATE_ALIAS_RE, DELETE_ALIAS_RE):
        for match in pattern.finditer(clean):
            alias = match.group(1).upper()
            statement = match.group(0)
            for source in ALIAS_SOURCE_RE.finditer(statement):
                if source.group(2).upper() == alias:
                    add(writes, source.group(1))
                    break
    return sorted(reads), sorted(writes), sorted(execs)


def parse_procedures():
    """Return normalized procedure key -> body across SQL Server and Oracle."""
    procedures = {}
    files = tsqllib.sql_files(("sqlserver", "oracle"))
    for path, text in files:
        clean = tsqllib.strip_comments(text)
        package_matches = list(ORACLE_PACKAGE_RE.finditer(clean))
        package_ranges = []
        for package_match in package_matches:
            package_name = lib.normalise_object(package_match.group(1))
            package_start = package_match.end()
            package_end = len(clean)
            next_package = ORACLE_PACKAGE_RE.search(clean, package_start)
            if next_package:
                package_end = next_package.start()
            package_ranges.append((package_start, package_end, package_name))
            members = list(ORACLE_MEMBER_RE.finditer(clean, package_start,
                                                     package_end))
            for index, member in enumerate(members):
                end = members[index + 1].start() if index + 1 < len(members) else package_end
                key = "%s.%s" % (package_name,
                                 member.group(2))
                procedures[lib.normalise_object(key)] = clean[member.start():end]

        for match in SQL_CREATE_RE.finditer(clean):
            if any(start <= match.start() < end for start, end, _ in package_ranges):
                continue
            end_match = CREATE_BOUNDARY_RE.search(clean, match.end())
            end = end_match.start() if end_match else len(clean)
            procedures[lib.normalise_object(match.group(1))] = clean[match.start():end]
    return procedures


def resolve_procedure(key, procedures, cache=None, depth=0, trail=None):
    """Resolve one procedure transitively, with a bounded cycle-safe walk."""
    cache = cache if cache is not None else {}
    trail = set(trail or ())
    key = lib.normalise_object(key)
    if key not in procedures:
        parts = key.split(".")
        if len(parts) == 2:
            candidate = "%s.USP_%s" % (parts[0], parts[1])
            if candidate in procedures:
                key = candidate
    if key in cache and depth == 0:
        return cache[key]
    if key in trail or depth > 6:
        return {"reads": set(), "writes": set(), "unresolved": set(),
                "resolved": set(), "by_proc": []}
    body = procedures.get(key)
    if body is None:
        return {"reads": set(), "writes": set(), "unresolved": {key},
                "resolved": set(), "by_proc": []}
    reads, writes, execs = sql_objects(body)
    result = {
        "reads": set(reads), "writes": set(writes), "unresolved": set(),
        "resolved": {key}, "by_proc": [(key, set(reads), set(writes))],
    }
    for child in execs:
        nested = resolve_procedure(
            child, procedures, cache, depth + 1, trail | {key})
        result["reads"].update(nested["reads"])
        result["writes"].update(nested["writes"])
        result["unresolved"].update(nested["unresolved"])
        result["resolved"].update(nested["resolved"])
        result["by_proc"].extend(nested["by_proc"])
    if depth == 0:
        cache[key] = result
    return result


def connection_managers(folder, cache):
    if folder in cache:
        return cache[folder]
    by_name, by_id, by_type = {}, {}, {}
    path = os.path.join(lib.REPO_ROOT, "ssis", folder)
    try:
        filenames = sorted(os.listdir(path))
    except OSError:
        filenames = []
    for filename in filenames:
        if not filename.endswith(".conmgr"):
            continue
        try:
            root = ET.parse(os.path.join(path, filename)).getroot()
        except (OSError, ET.ParseError):
            continue
        name = attr(root, "ObjectName")
        manager_id = attr(root, "DTSID")
        creation = attr(root, "CreationName")
        if name and creation:
            by_name[name] = creation
            by_type[name] = creation
        if manager_id and creation:
            by_id[manager_id] = creation
    cache[folder] = (by_name, by_id, by_type)
    return cache[folder]


def resolve_connection(folder, raw, cache):
    by_name, by_id, _types = connection_managers(folder, cache)
    match = CONNECTION_REF_RE.search(raw or "")
    if match:
        name = match.group(1)
        return name, by_name.get(name, "")
    manager_id = (raw or "").split(":", 1)[0]
    return "", by_id.get(manager_id, "")


def property_values(component):
    values = {}
    for element in component.iter():
        if local_name(element.tag) != "property":
            continue
        name = element.attrib.get("name", "")
        if name in {"SqlCommand", "OpenRowset", "AccessMode",
                    "TableOrViewName", "SqlCommandParam"}:
            values[name] = element.text or ""
    return values


def endpoint_component(raw):
    match = re.search(r"\\([^\\]+)\.(?:Outputs|Inputs)\[", raw or "")
    return match.group(1) if match else ""


def pipeline_components(pipeline, folder, cm_cache):
    elements = [e for e in pipeline.iter() if local_name(e.tag) == "component"]
    by_name = {e.attrib.get("name", ""): e for e in elements}
    graph = defaultdict(set)
    indegree = {name: 0 for name in by_name if name}
    for path in pipeline.iter():
        if local_name(path.tag) != "path":
            continue
        start = endpoint_component(attr(path, "startId"))
        end = endpoint_component(attr(path, "endId"))
        if start in by_name and end in by_name and end not in graph[start]:
            graph[start].add(end)
            indegree[end] += 1
    document_order = [e.attrib.get("name", "") for e in elements]
    queue = [name for name in document_order if name in indegree and indegree[name] == 0]
    ordered = []
    while queue:
        name = queue.pop(0)
        ordered.append(name)
        for child in document_order:
            if child in graph[name]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
    ordered.extend(name for name in document_order if name and name not in ordered)

    output = []
    for order, name in enumerate(ordered, 1):
        component = by_name[name]
        values = property_values(component)
        component_class = component.attrib.get("componentClassID", "")
        connections = []
        for element in component.iter():
            if local_name(element.tag) == "connection":
                manager, _kind = resolve_connection(
                    folder, element.attrib.get("connectionManagerRefId", ""),
                    cm_cache)
                if manager:
                    connections.append(manager)
        connection = ";".join(sorted(set(connections)))
        sql_or_table = values.get("SqlCommand") or values.get("OpenRowset")
        reads, writes, _execs = sql_objects(
            values.get("SqlCommand", "") or "")
        if values.get("OpenRowset") and "Destination" in component_class:
            target = normal_object(values["OpenRowset"])
            if target:
                writes = sorted(set(writes) | {target})
        elif values.get("OpenRowset") and "Source" in component_class:
            source = normal_object(values["OpenRowset"])
            if source:
                reads = sorted(set(reads) | {source})
        lookup = "Lookup" in component_class
        if lookup and values.get("OpenRowset"):
            source = normal_object(values["OpenRowset"])
            if source:
                reads = sorted(set(reads) | {source})
        output.append({
            "order": order,
            "class": component_class,
            "name": name,
            "connection": connection,
            "access_mode": values.get("AccessMode", ""),
            "sql_or_table": sql_or_table,
            "reads": sorted(set(reads)),
            "writes": sorted(set(writes)),
            "lookups": sorted(set(reads)) if lookup else [],
        })
    return output


def executable_order(root):
    parents = {}
    nodes = []

    def walk(parent, element):
        for child in list(element):
            parents[id(child)] = parent
            walk(element, child)

    walk(None, root)
    for element in root.iter(EXECUTABLE):
        if element is not root:
            nodes.append(element)
    order = {id(node): index for index, node in enumerate(nodes)}
    refs = {attr(node, "refId"): node for node in nodes if attr(node, "refId")}
    graph = defaultdict(set)
    indegree = {ref: 0 for ref in refs}
    for constraint in root.iter(DTS + "PrecedenceConstraint"):
        source = attr(constraint, "From")
        target = attr(constraint, "To")
        if source in refs and target in refs and target not in graph[source]:
            graph[source].add(target)
            indegree[target] += 1
    queue = sorted((ref for ref, degree in indegree.items() if degree == 0),
                   key=lambda ref: order[id(refs[ref])])
    sorted_refs = []
    while queue:
        ref = queue.pop(0)
        sorted_refs.append(ref)
        for target in sorted(graph[ref], key=lambda value: order[id(refs[value])]):
            indegree[target] -= 1
            if indegree[target] == 0:
                queue.append(target)
        queue.sort(key=lambda value: order[id(refs[value])])
    if len(sorted_refs) != len(refs):
        sorted_refs = [attr(node, "refId") for node in nodes if attr(node, "refId")]
    return [refs[ref] for ref in sorted_refs], parents


def package_steps(package, cm_cache):
    root = ET.fromstring(package.text)
    ordered, parents = executable_order(root)
    steps = []
    manager_names = set()
    manager_types = set()
    sql_statement_count = 0
    pipeline_count = 0
    component_count = 0
    precedence_expressions = []
    for constraint in root.iter(DTS + "PrecedenceConstraint"):
        expression = " ".join(attr(constraint, "Expression").split())
        if expression:
            precedence_expressions.append(expression)

    for step_number, element in enumerate(ordered, 1):
        executable_type = attr(element, "ExecutableType")
        name = attr(element, "ObjectName")
        reference = attr(element, "refId")
        parent = parents.get(id(element))
        container = ""
        event_handler = False
        while parent is not None:
            if local_name(parent.tag) == "EventHandler":
                event_handler = True
            if parent.tag == EXECUTABLE and parent is not root and not container:
                container = attr(parent, "ObjectName")
            parent = parents.get(id(parent))
        if event_handler:
            container = "error_handler"
        reads, writes, execs = set(), set(), set()
        sql_text = ""
        components = []
        connections = set()
        if executable_type == "Microsoft.ExecuteSQLTask":
            for data in element.iter():
                if local_name(data.tag) == "SqlTaskData":
                    for key, value in data.attrib.items():
                        if key.endswith("SqlStatementSource"):
                            sql_text = value or ""
                        if key.endswith("Connection") and value:
                            manager, kind = resolve_connection(
                                package.folder, value, cm_cache)
                            if manager:
                                connections.add(manager)
                            if kind:
                                manager_types.add(kind)
                    break
            reads, writes, execs = map(set, sql_objects(sql_text))
            if sql_text:
                sql_statement_count += 1
        elif executable_type == "Microsoft.Pipeline":
            pipeline_count += 1
            pipeline = next((e for e in element.iter()
                             if local_name(e.tag) == "pipeline"), None)
            if pipeline is not None:
                components = pipeline_components(
                    pipeline, package.folder, cm_cache)
                component_count += len(components)
                for component in components:
                    reads.update(component["reads"])
                    writes.update(component["writes"])
                    if component["sql_or_table"]:
                        sql_statement_count += 1
                    if component["connection"]:
                        connections.update(component["connection"].split(";"))
                sql_text = "\n\n".join(
                    component["sql_or_table"] for component in components
                    if component["sql_or_table"])
        for manager in connections:
            _by_name, _by_id, by_type = connection_managers(
                package.folder, cm_cache)
            manager_names.add(manager)
            if manager in by_type:
                manager_types.add(by_type[manager])
        steps.append({
            "step": step_number,
            "type": executable_type,
            "name": name,
            "container": container,
            "connection": ";".join(sorted(connections)),
            "sql": sql_text,
            "reads": sorted(reads),
            "writes": sorted(writes),
            "execs": sorted(execs),
            "components": components,
            "_ref": reference,
            "_event_handler": event_handler,
        })
    return steps, manager_names, manager_types, sql_statement_count, pipeline_count, component_count, precedence_expressions


def object_record(key, catalog_objects):
    value = lib.normalise_object(key)
    lookup = catalog_objects.get(value)
    if lookup is None and len(value.split(".")) >= 2:
        lookup = catalog_objects.get(".".join(value.split(".")[-2:]))
    schema = value.split(".")[0] if value else ""
    if schema.startswith("WWI_"):
        layer, inferred_system = "oracle", "Oracle"
    elif schema in {"APPLICATION", "SALES", "PURCHASING", "WAREHOUSE",
                    "WEBSITE"}:
        layer, inferred_system = "oltp", "SQL Server OLTP"
    elif schema == "RAW":
        layer, inferred_system = "staging-raw", "SQL Server Staging"
    elif schema == "STG":
        layer, inferred_system = "staging-stg", "SQL Server Staging"
    elif schema == "WORK":
        layer, inferred_system = "staging-work", "SQL Server Staging"
    elif schema == "ERR":
        layer, inferred_system = "staging-err", "SQL Server Staging"
    elif schema == "REF":
        layer, inferred_system = "reference", "SQL Server DW"
    elif schema == "ETL":
        layer, inferred_system = "etl-control", "ETL control"
    elif schema == "DIMENSION":
        layer, inferred_system = "dw-dimension", "SQL Server DW"
    elif schema == "FACT":
        layer, inferred_system = "dw-fact", "SQL Server DW"
    elif schema == "AGGREGATE":
        layer, inferred_system = "dw-aggregate", "SQL Server DW"
    elif schema in {"REPORT", "POWERBI", "REPORTS"}:
        layer, inferred_system = "dw-report", "SQL Server DW"
    elif schema == "INTEGRATION":
        layer, inferred_system = "dw-integration", "SQL Server DW"
    elif schema == "CUSTOMER360":
        layer, inferred_system = "customer360", "SQL Server DW"
    else:
        layer, inferred_system = "unknown", "Unknown"
    if lookup:
        return {
            "object": value,
            "layer": layer,
            "system": lookup["system"],
            "declared": True,
        }
    return {
        "object": value,
        "layer": layer,
        "system": inferred_system,
        "declared": False,
    }


def is_plumbing(value):
    key = lib.normalise_object(value)
    return (key.startswith(("ETL.", "ERR.", "INTEGRATION.")) or
            key.startswith("WORK."))


def declaration_sets(names, catalog_keys):
    normalized = [lib.normalise_object(name) for name in names]
    wildcard_prefixes = {
        value[:-1] for value in normalized if value.endswith(".*")
    }
    explicit = lib.expand_references(
        [value for value in normalized if "*" not in value], catalog_keys)
    return explicit, wildcard_prefixes


def declaration_covers(value, explicit, wildcard_prefixes):
    return value in explicit or any(
        value.startswith(prefix) for prefix in wildcard_prefixes)


def as_records(values, catalog_objects):
    return [object_record(value, catalog_objects) for value in sorted(values)]


def graph_included(record):
    return record["layer"] not in {"etl-control", "staging-err"}


def parse_params():
    folders_by_parameter = defaultdict(set)
    for root, _dirs, files in os.walk(os.path.join(lib.REPO_ROOT, "ssis")):
        if "Project.params" not in files:
            continue
        folder = os.path.relpath(root, os.path.join(lib.REPO_ROOT, "ssis")).replace(os.sep, "/")
        try:
            parsed = ET.parse(os.path.join(root, "Project.params")).getroot()
        except (OSError, ET.ParseError):
            continue
        for parameter in parsed.iter():
            if local_name(parameter.tag) == "Parameter":
                name = attr(parameter, "Name")
                if name:
                    folders_by_parameter[name].add(folder)
    return Counter({name: len(folders) for name, folders in folders_by_parameter.items()})


def package_log_providers(root):
    values = set()
    for element in root.iter():
        if local_name(element.tag) == "LogProvider":
            creation = attr(element, "CreationName")
            config = attr(element, "ConfigString")
            values.add("%s%s" % (creation, " (%s)" % config if config else ""))
    return values


def table_edges(package_data, procedure_data, catalog_objects):
    rows = {}
    for package, data in package_data.items():
        reads = {item["object"] for item in data["reads"]}
        writes = {item["object"] for item in data["writes"]}
        for source in reads:
            for target in writes:
                source_record = object_record(source, catalog_objects)
                target_record = object_record(target, catalog_objects)
                if not graph_included(source_record) or not graph_included(target_record):
                    continue
                rows[(source, target, package, "direct")] = {
                    "from_object": source,
                    "from_layer": source_record["layer"],
                    "to_object": target,
                    "to_layer": target_record["layer"],
                    "package": package,
                    "via": "direct",
                }
        for proc, proc_data in procedure_data.get(package, {}).items():
            for source in proc_data["reads"]:
                for target in proc_data["writes"]:
                    source_record = object_record(source, catalog_objects)
                    target_record = object_record(target, catalog_objects)
                    if not graph_included(source_record) or not graph_included(target_record):
                        continue
                    rows[(source, target, package, "proc:%s" % proc)] = {
                        "from_object": source,
                        "from_layer": source_record["layer"],
                        "to_object": target,
                        "to_layer": target_record["layer"],
                        "package": package,
                        "via": "proc:%s" % proc,
                    }
    return [rows[key] for key in sorted(rows)]


def markdown_graph(package_data, edges, counts, catalog_objects):
    layer_flow = Counter((row["from_layer"], row["to_layer"]) for row in edges)
    reads_by_package = defaultdict(set)
    writes_by_package = defaultdict(set)
    folders = defaultdict(lambda: {"reads": set(), "writes": set()})
    writers = defaultdict(set)
    readers = defaultdict(set)
    plumbing_volume = Counter()
    for package, data in package_data.items():
        folder = data["folder"]
        for item in data["reads"] + data["reads_via_procs"]:
            if item["layer"] == "etl-control":
                plumbing_volume["etl-control reads"] += 1
            if not graph_included(item):
                continue
            reads_by_package[item["object"]].add(package)
            readers[item["object"]].add(package)
            folders[folder]["reads"].add(item["object"])
        for item in data["writes"] + data["writes_via_procs"]:
            if item["layer"] == "etl-control":
                plumbing_volume["etl-control writes"] += 1
            if item["layer"] == "staging-err":
                plumbing_volume["staging-err writes"] += 1
            if not graph_included(item):
                continue
            writes_by_package[item["object"]].add(package)
            writers[item["object"]].add(package)
            folders[folder]["writes"].add(item["object"])

    def hubs(mapping):
        return sorted(mapping.items(), key=lambda pair: (-len(pair[1]), pair[0]))[:25]

    graph_degree = Counter()
    for row in edges:
        graph_degree[row["from_object"]] += 1
        graph_degree[row["to_object"]] += 1
    graph_hubs = sorted(graph_degree.items(), key=lambda pair: (-pair[1], pair[0]))[:25]
    all_read = set(readers)
    all_write = set(writers)
    external = sorted(all_read - all_write)
    dead = sorted(all_write - all_read)
    multi = sorted((obj, sorted(packages)) for obj, packages in writers.items()
                   if len(packages) > 1)
    declared_rows = []
    for package, data in package_data.items():
        recon = data["declared_vs_parsed"]
        total = sum(len(recon[key]) for key in recon)
        if total:
            declared_rows.append((package, total, recon))
    declared_rows.sort(key=lambda row: (-row[1], row[0]))

    lines = [
        "# SSIS table dependency graph",
        "",
        "Generated by `validation/checks/extract_sql_lineage.py`. This is "
        "static-only analysis of checked-in XML and SQL; no package or query "
        "was executed. Control-framework and reject tables are excluded; "
        "see `shared-plumbing.md`.",
        "",
        "## Headline numbers",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
    ]
    for key, label in (
        ("packages", "Packages parsed"), ("sql_steps", "SQL statements"),
        ("pipelines", "Pipelines"), ("components", "Pipeline components"),
        ("objects_read", "Distinct objects read"),
        ("objects_written", "Distinct objects written"),
        ("table_edges", "Table edges"), ("procs_unresolved", "Unresolved procedures"),
    ):
        lines.append("| %s | %s |" % (label, counts[key]))
    lines.extend(["", "## Plumbing volume excluded", "",
                   "| Object class | References |", "| --- | ---: |"])
    for key in ("etl-control reads", "etl-control writes",
                "staging-err writes"):
        lines.append("| %s | %d |" % (key, plumbing_volume[key]))
    lines.extend(["", "## Layer-flow matrix", "",
                   "| From \\ To | " + " | ".join(sorted({x for pair in layer_flow for x in pair})) + " |",
                   "| --- | " + " | ".join("---:" for _ in sorted({x for pair in layer_flow for x in pair})) + " |"])
    layers = sorted({x for pair in layer_flow for x in pair})
    for source in layers:
        lines.append("| %s | %s |" % (source, " | ".join(
            str(layer_flow.get((source, target), 0)) for target in layers)))
    lines.extend(["", "```mermaid", "graph LR"])
    for layer in layers:
        lines.append("    %s[%s]" % (re.sub(r"\W+", "_", layer), layer))
    for (source, target), number in sorted(layer_flow.items()):
        lines.append("    %s -->|%d| %s" % (
            re.sub(r"\W+", "_", source), number,
            re.sub(r"\W+", "_", target)))
    lines.extend(["```", "", "## Hub objects by packages reading", "",
                   "| Object | Packages |", "| --- | ---: |"])
    lines.extend("| %s | %d |" % (obj, len(packages)) for obj, packages in hubs(reads_by_package))
    lines.extend(["", "## Hub objects by packages writing", "",
                   "| Object | Packages |", "| --- | ---: |"])
    lines.extend("| %s | %d |" % (obj, len(packages)) for obj, packages in hubs(writes_by_package))
    lines.extend(["", "## Table graph hubs", "", "| Object | Fan-in + fan-out |",
                  "| --- | ---: |"])
    lines.extend("| %s | %d |" % item for item in graph_hubs)
    lines.extend(["", "## Distinct objects by folder", "",
                   "| Folder | Reads | Writes |", "| --- | ---: | ---: |"])
    for folder in sorted(folders):
        lines.append("| %s | %d | %d |" % (
            folder, len(folders[folder]["reads"]), len(folders[folder]["writes"])))
    lines.extend(["", "## Multi-writer contention", "",
                   "Total objects written by more than one package: %d." % len(multi),
                   "", "| Object | Packages |", "| --- | --- |"])
    lines.extend("| %s | %s |" % (obj, ", ".join(packages))
                  for obj, packages in multi[:40])
    lines.extend(["", "## External inputs", "",
                   "Total objects read but never written: %d." % len(external),
                   "", "| Object |", "| --- |"])
    lines.extend("| %s |" % obj for obj in external[:40])
    lines.extend(["", "## Dead ends", "",
                   "Total objects written but never read: %d." % len(dead),
                   "", "| Object |", "| --- |"])
    lines.extend("| %s |" % obj for obj in dead[:40])
    lines.extend(["", "## Declared versus parsed", "",
                   "| Reconciliation metric | Total |",
                   "| --- | ---: |"])
    for key in (
            "declared_sources_not_parsed", "parsed_reads_not_declared",
            "declared_targets_not_parsed", "parsed_writes_not_declared"):
        lines.append("| %s | %d |" % (key, counts[key]))
    lines.extend(["", "### Top offenders", "",
                   "| Package | Differences |", "| --- | ---: |"])
    lines.extend("| %s | %d |" % (package, total)
                  for package, total, _recon in declared_rows[:25])
    return "\n".join(lines) + "\n"


def markdown_plumbing(package_data, cm_usage, cm_types, params, log_providers,
                      precedence):
    def grouped(prefix, direction=None):
        result = defaultdict(set)
        for package, data in package_data.items():
            values = []
            if direction in (None, "read"):
                values.extend(item["object"] for item in
                              data["reads"] + data["reads_via_procs"])
            if direction in (None, "write"):
                values.extend(item["object"] for item in
                              data["writes"] + data["writes_via_procs"])
            for value in values:
                if value.startswith(prefix):
                    result[value].add(package)
        return {key: values for key, values in result.items()
                if len(values) >= 5}

    control_procs = defaultdict(lambda: defaultdict(set))
    integration = defaultdict(set)
    ref_procs = defaultdict(set)
    for package, data in package_data.items():
        for step in data["chain"]:
            position = ("error_handler" if step["container"] == "error_handler"
                        else "start" if "start" in step["name"].lower()
                        else "end" if "end" in step["name"].lower()
                        else "middle")
            for proc in step["execs"]:
                key = lib.normalise_object(proc)
                if key.startswith("ETL.USP_"):
                    control_procs[key][position].add(package)
                elif key.startswith("INTEGRATION."):
                    integration[key].add(package)
                elif key.startswith("REF.USP_"):
                    ref_procs[key].add(package)
    control_procs = {
        proc: positions for proc, positions in control_procs.items()
        if len(set().union(*positions.values())) >= 5
    }
    integration = {
        proc: packages for proc, packages in integration.items()
        if len(packages) >= 5
    }
    ref_procs = {
        proc: packages for proc, packages in ref_procs.items()
        if len(packages) >= 5
    }

    lines = [
        "# Shared SSIS plumbing",
        "",
        "Generated by `validation/checks/extract_sql_lineage.py`. This is "
        "static-only analysis of checked-in package XML and SQL.",
        "",
        "## Control-framework procedures",
        "",
        "| Procedure | Packages | Positions |", "| --- | ---: | --- |",
    ]
    for proc in sorted(control_procs):
        packages = set().union(*control_procs[proc].values())
        positions = []
        for position in ("start", "middle", "end", "error_handler"):
            if control_procs[proc].get(position):
                positions.append("%s:%d" % (position, len(control_procs[proc][position])))
        lines.append("| %s | %d | %s |" % (proc, len(packages), ", ".join(positions)))

    sections = [
        ("Control tables", grouped("ETL.", "read")),
        ("Integration helper procedures", integration),
        ("Reference procedures", ref_procs),
        ("Reject tables", grouped("ERR.", None)),
        ("Work tables", grouped("WORK.", None)),
    ]
    for title, values in sections:
        lines.extend(["", "## %s" % title, "",
                      "| Object | Packages |", "| --- | ---: |"])
        for key in sorted(values):
            lines.append("| %s | %d |" % (key, len(values[key])))

    lines.extend(["", "## Connection managers", "",
                   "| Name | Packages | Type |", "| --- | ---: | --- |"])
    for name in sorted(cm_usage):
        if len(cm_usage[name]) < 5:
            continue
        lines.append("| %s | %d | %s |" % (
            name, len(cm_usage[name]), cm_types.get(name, "")))
    lines.extend(["", "## Project parameters", "",
                   "| Parameter | Folders |", "| --- | ---: |"])
    lines.extend("| %s | %d |" % item for item in sorted(params.items())
                 if item[1] >= 5)
    lines.extend(["", "## SQL Server log provider", "",
                   "| Provider | Packages |", "| --- | ---: |"])
    for provider, packages in sorted(log_providers.items()):
        if len(packages) < 5:
            continue
        lines.append("| %s | %d |" % (provider, len(packages)))
    lines.extend(["", "## Repeated precedence expressions", "",
                   "| Expression | Packages |", "| --- | ---: |"])
    for expression, packages in precedence.most_common(10):
        lines.append("| `%s` | %d |" % (expression, packages))

    lines.extend(["", "## Databricks mapping", "",
                   "| Legacy plumbing | Databricks mapping |",
                   "| --- | --- |"])
    mappings = (
        ("etl batch/step/watermark procs",
         "Lakeflow Jobs run metadata + a Delta `etl_control` schema written via a shared Python module (job/task run IDs replace BatchId/PackageExecutionId)"),
        ("etl.usp_LogRowCount / usp_AssertRowCountReconciliation",
         "DLT expectations or a post-load reconciliation task writing to Delta"),
        ("etl.usp_LogError / OnError handlers",
         "job failure notifications + system.lakeflow tables"),
        ("err.* reject tables",
         "quarantine Delta tables with `expect_or_drop` / `_rescued_data`"),
        ("Integration.EnsureUnknownMembers / inferred members",
         "shared PySpark dimension helper (MERGE with default -1 member)"),
        ("ref.usp_LoadCodeCrosswalk",
         "Delta reference table + broadcast join"),
        ("work.* tables",
         "temp views / Delta staging within the job"),
        ("connection managers",
         "Unity Catalog connections / Lakehouse Federation (Oracle, SQL Server) and Volumes (file shares)"),
        ("project params", "job parameters / bundle variables"),
    )
    lines.extend("| %s | %s |" % mapping for mapping in mappings)
    return "\n".join(lines) + "\n"


def write_outputs(paths, payload, lineage_rows, edge_rows, graph, plumbing):
    with open(paths["json"], "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    with open(paths["lineage"], "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LINEAGE_COLUMNS,
                                lineterminator="\n")
        writer.writeheader()
        writer.writerows(lineage_rows)
    with open(paths["edges"], "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EDGE_COLUMNS,
                                lineterminator="\n")
        writer.writeheader()
        writer.writerows(edge_rows)
    with open(paths["graph"], "w") as handle:
        handle.write(graph)
    with open(paths["plumbing"], "w") as handle:
        handle.write(plumbing)


def run(args):
    report = lib.Report("extract_sql_lineage")
    SKIPPED_NON_SCHEMA_REFS.clear()
    catalog = lib.load_catalog()
    catalog_objects = lib.catalog_objects(catalog)
    catalog_keys = set(catalog_objects)
    packages = lib.load_packages(catalog)
    procedures = parse_procedures()
    proc_cache = {}
    cm_cache = {}
    package_data = {}
    lineage_rows = []
    procedure_data = {}
    counts = Counter()
    unresolved_by_package = defaultdict(set)
    unresolved_packages = defaultdict(set)
    declared_offenders = []
    cm_usage = defaultdict(set)
    cm_types = {}
    log_providers = defaultdict(set)
    precedence_packages = defaultdict(set)

    for package_name in sorted(packages):
        package = packages[package_name]
        try:
            steps, managers, manager_types, sql_count, pipeline_count, component_count, expressions = package_steps(package, cm_cache)
        except ET.ParseError:
            steps, managers, manager_types, sql_count, pipeline_count, component_count, expressions = [], set(), set(), 0, 0, 0, []
        for manager in managers:
            cm_usage[manager].add(package_name)
        for manager_type in manager_types:
            for manager in managers:
                cm_types.setdefault(manager, manager_type)
        for expression in expressions:
            precedence_packages[expression].add(package_name)
        try:
            root = ET.fromstring(package.text)
            for provider in package_log_providers(root):
                log_providers[provider].add(package_name)
        except ET.ParseError:
            pass

        direct_reads, direct_writes, direct_execs = set(), set(), set()
        proc_results = {}
        for step in steps:
            direct_reads.update(step["reads"])
            direct_writes.update(step["writes"])
            direct_execs.update(step["execs"])
        for proc in sorted(direct_execs):
            result = resolve_procedure(proc, procedures, proc_cache)
            proc_results[proc] = result
            unresolved_by_package[package_name].update(result["unresolved"])
        resolved_reads, resolved_writes = set(), set()
        reads_via, writes_via = set(), set()
        for proc, result in proc_results.items():
            resolved_reads.update(result["reads"])
            resolved_writes.update(result["writes"])
            for resolving_proc, reads, writes in result["by_proc"]:
                reads_via.update((value, resolving_proc) for value in reads)
                writes_via.update((value, resolving_proc) for value in writes)

        all_reads = direct_reads | resolved_reads
        all_writes = direct_writes | resolved_writes
        declared_sources, source_wildcards = declaration_sets(
            package.source_objects, catalog_keys)
        declared_targets, target_wildcards = declaration_sets(
            package.target_objects, catalog_keys)
        parsed_source = all_reads
        parsed_target = all_writes
        reconciliation = {
            "declared_sources_not_parsed": sorted(declared_sources - parsed_source),
            "parsed_reads_not_declared": sorted(
                value for value in parsed_source - declared_sources
                if not is_plumbing(value) and not declaration_covers(
                    value, declared_sources, source_wildcards)),
            "declared_targets_not_parsed": sorted(declared_targets - parsed_target),
            "parsed_writes_not_declared": sorted(
                value for value in parsed_target - declared_targets
                if not is_plumbing(value) and not declaration_covers(
                    value, declared_targets, target_wildcards)),
        }
        if declared_targets and not (declared_targets & parsed_target):
            report.warn(
                "declared-targets-unparsed", package_name,
                "none of the %d declared target object(s) were parsed" %
                len(declared_targets))
        if not all_reads and not all_writes and package.load_type not in {
                "orchestration", "utility", "maintenance"} and (
                    package.folder != "99_maintenance"):
            report.error(
                "empty-lineage", package_name,
                "package has no parsed reads or writes")
        if unresolved_by_package[package_name]:
            for proc in sorted(unresolved_by_package[package_name]):
                unresolved_packages[proc].add(package_name)
        for step in steps:
            if not step["components"]:
                for item in step["reads"]:
                    lineage_rows.append({
                        "package": package_name, "folder": package.folder,
                        "step_order": step["step"], "step_type": step["type"],
                        "step_name": step["name"], "component": "",
                        "connection": step["connection"], "direction": "read",
                        "object": item, **object_record(item, catalog_objects),
                        "via": "direct",
                    })
                for item in step["writes"]:
                    lineage_rows.append({
                        "package": package_name, "folder": package.folder,
                        "step_order": step["step"], "step_type": step["type"],
                        "step_name": step["name"], "component": "",
                        "connection": step["connection"], "direction": "write",
                        "object": item, **object_record(item, catalog_objects),
                        "via": "direct",
                    })
            for item in step["execs"]:
                lineage_rows.append({
                    "package": package_name, "folder": package.folder,
                    "step_order": step["step"], "step_type": step["type"],
                    "step_name": step["name"], "component": "",
                    "connection": step["connection"], "direction": "exec",
                    "object": item, **object_record(item, catalog_objects),
                    "via": "direct",
                })
            for component in step["components"]:
                for direction, values in (
                        ("read", component["reads"]),
                        ("write", component["writes"]),
                        ("lookup", component["lookups"])):
                    for item in values:
                        lineage_rows.append({
                            "package": package_name, "folder": package.folder,
                            "step_order": step["step"], "step_type": step["type"],
                            "step_name": step["name"],
                            "component": component["name"],
                            "connection": component["connection"],
                            "direction": direction, "object": item,
                            **object_record(item, catalog_objects),
                            "via": "direct",
                        })
        package_data[package_name] = {
            "folder": package.folder,
            "load_type": package.load_type,
            "chain": [{key: value for key, value in step.items() if not key.startswith("_")}
                      for step in steps],
            "reads": as_records(direct_reads, catalog_objects),
            "writes": as_records(direct_writes, catalog_objects),
            "execs": sorted(direct_execs),
            "reads_via_procs": [
                dict(object_record(value, catalog_objects), via="proc:%s" % proc)
                for value, proc in sorted(reads_via)],
            "writes_via_procs": [
                dict(object_record(value, catalog_objects), via="proc:%s" % proc)
                for value, proc in sorted(writes_via)],
            "unresolved_procs": sorted(unresolved_by_package[package_name]),
            "declared_vs_parsed": {
                key: [object_record(value, catalog_objects) for value in values]
                for key, values in reconciliation.items()
            },
        }
        procedure_details = defaultdict(lambda: {"reads": set(), "writes": set()})
        for result in proc_results.values():
            for resolving_proc, reads, writes in result["by_proc"]:
                procedure_details[resolving_proc]["reads"].update(reads)
                procedure_details[resolving_proc]["writes"].update(writes)
        procedure_data[package_name] = dict(procedure_details)
        counts["packages"] += 1
        counts["sql_steps"] += sql_count
        counts["pipelines"] += pipeline_count
        counts["components"] += component_count
        counts["objects_read"] = 0
        counts["objects_written"] = 0
        counts["procs_called"] = 0

    edges = table_edges(package_data, procedure_data, catalog_objects)
    for proc, package_names in sorted(unresolved_packages.items()):
        report.warn(
            "unresolved-proc", proc,
            "called by %d package(s)" % len(package_names))
    all_read_objects = {
        item["object"]
        for data in package_data.values()
        for item in data["reads"] + data["reads_via_procs"]
        if graph_included(item)
    }
    all_write_objects = {
        item["object"]
        for data in package_data.values()
        for item in data["writes"] + data["writes_via_procs"]
        if graph_included(item)
    }
    all_called = {proc for data in package_data.values() for proc in data["execs"]}
    all_resolved = {via[5:] for data in package_data.values()
                    for item in data["reads_via_procs"] + data["writes_via_procs"]
                    for via in [item["via"]]}
    all_unresolved = {proc for data in package_data.values()
                      for proc in data["unresolved_procs"]}
    totals = Counter()
    for data in package_data.values():
        for key, values in data["declared_vs_parsed"].items():
            totals[key] += len(values)
    declared_offenders = sorted(
        (package, sum(len(values) for values in data["declared_vs_parsed"].values()))
        for package, data in package_data.items()
        if sum(len(values) for values in data["declared_vs_parsed"].values())
    )
    declared_offenders.sort(key=lambda pair: (-pair[1], pair[0]))
    counts.update({
        "objects_read": len(all_read_objects),
        "objects_written": len(all_write_objects),
        "procs_called": len(all_called),
        "procs_resolved": len(all_resolved),
        "procs_unresolved": len(all_unresolved),
        "table_edges": len(edges),
        "declared_sources_not_parsed": totals["declared_sources_not_parsed"],
        "parsed_reads_not_declared": totals["parsed_reads_not_declared"],
        "declared_targets_not_parsed": totals["declared_targets_not_parsed"],
        "parsed_writes_not_declared": totals["parsed_writes_not_declared"],
        "skipped_non_schema_refs": sum(SKIPPED_NON_SCHEMA_REFS.values()),
    })
    payload = {"static_only": True, "packages": package_data}
    paths = {
        "json": os.path.join(lib.REPO_ROOT, "docs/inventories/ssis-sql-lineage.json"),
        "lineage": os.path.join(lib.REPO_ROOT, "docs/inventories/ssis-table-edges.csv"),
        "edges": os.path.join(lib.REPO_ROOT, "docs/inventories/table-dependency-edges.csv"),
        "graph": os.path.join(lib.REPO_ROOT, "docs/dependency-maps/table-dependency-graph.md"),
        "plumbing": os.path.join(lib.REPO_ROOT, "docs/dependency-maps/shared-plumbing.md"),
    }
    graph = markdown_graph(package_data, edges, counts, catalog_objects)
    params = parse_params()
    precedence = Counter({
        expression: len(package_names)
        for expression, package_names in precedence_packages.items()
    })
    plumbing = markdown_plumbing(
        package_data, cm_usage, cm_types, params, log_providers, precedence)
    if not args.no_write:
        write_outputs(paths, payload, lineage_rows, edges, graph, plumbing)

    for key, value in counts.items():
        report.count(key, value)
    report.detail("unresolved_procs", sorted(all_unresolved))
    report.detail("skipped_non_schema_refs",
                  sorted(SKIPPED_NON_SCHEMA_REFS))
    report.detail("top_hub_objects", sorted(
        Counter(item["object"] for data in package_data.values()
                for item in data["reads"] + data["reads_via_procs"]
                if graph_included(item)).items(),
        key=lambda pair: (-pair[1], pair[0]))[:10])
    report.detail("layer_flow", {
        "%s -> %s" % (source, target): number
        for (source, target), number in Counter(
            (row["from_layer"], row["to_layer"]) for row in edges).items()
    })
    report.detail("declared_reconciliation_offenders", declared_offenders[:25])
    return report.emit(as_json=args.json, strict=args.strict,
                       show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    parser.add_argument("--no-write", action="store_true",
                        help="do not write generated lineage artifacts")
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
