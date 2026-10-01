#!/usr/bin/env python3
"""Reconcile the SSIS package inventory, package XML and orchestration plan.

The check is static-only: it reads the checked-in CSV inventory, catalog,
generated SSIS packages, connection managers and orchestration plan. It does
not connect to SQL Server, Oracle, SSIS or a file share.

Usage:
    python3 validation/checks/reconcile_ssis_inventory.py [--json] [--strict]
    [--quiet] [--output PATH] [--no-write]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import xml.etree.ElementTree as ET

import estatelib as lib


CSV_REL_PATH = os.path.join("docs", "inventories", "ssis-packages.csv")
DEFAULT_OUTPUT = os.path.join("docs", "inventories", "ssis-reconciliation.csv")
DTS = "{%s}" % lib.DTS_NS
EXECUTABLE = DTS + "Executable"
CONNECTION_MANAGER = DTS + "ConnectionManager"
EXECUTABLE_TYPE = DTS + "ExecutableType"
CREATION_NAME = DTS + "CreationName"
OBJECT_NAME = DTS + "ObjectName"
DTSID = DTS + "DTSID"

CONNECTION_REF_RE = re.compile(
    r'connectionManagerRefId="Project\.ConnectionManagers\[([^\]]+)\]"')
SQL_CONNECTION_RE = re.compile(r'SQLTask:Connection="([^"]+)"')

OUTPUT_COLUMNS = (
    "package", "project", "folder", "domain", "load_type", "source_system",
    "target_system", "parent", "parent_edge_source", "actual_parents",
    "file_present", "path",
    "execute_sql_tasks", "data_flow_tasks", "script_tasks",
    "execute_package_tasks", "file_system_tasks", "foreach_loops",
    "send_mail_tasks", "connection_managers", "connection_manager_types",
    "child_packages", "plan_children_count", "on_disk_not_in_inventory",
    "in_inventory_not_on_disk", "parent_mismatch", "unresolved_child",
    "parse_error", "databricks_risk", "databricks_risk_reason",
)


def local_name(tag):
    return tag.rsplit("}", 1)[-1]


def load_inventory():
    path = os.path.join(lib.REPO_ROOT, CSV_REL_PATH)
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))
    return rows


def equivalent_parent(left, right):
    return (left or "") == (right or "")


def inventory_agreement(report, inventory, declared):
    csv_names = set(inventory)
    catalog_names = set(declared)
    disagreements = []
    if csv_names != catalog_names:
        missing_catalog = sorted(csv_names - catalog_names)
        missing_csv = sorted(catalog_names - csv_names)
        if missing_catalog:
            report.error("inventory-agreement", ",".join(missing_catalog),
                         "CSV packages are absent from the catalog")
            disagreements.extend(missing_catalog)
        if missing_csv:
            report.error("inventory-agreement", ",".join(missing_csv),
                         "catalog packages are absent from the CSV")
            disagreements.extend(missing_csv)

    fields = (
        ("project", "project"),
        ("folder", "folder"),
        ("load_type", "load_type"),
        ("source_system", "source_system"),
        ("target_system", "target_system"),
        ("parent_package", "parent"),
    )
    for name in sorted(csv_names & catalog_names):
        csv_row = inventory[name]
        catalog_row = declared[name]
        mismatches = []
        for csv_field, catalog_field in fields:
            left = csv_row.get(csv_field, "")
            right = catalog_row.get(catalog_field)
            if csv_field == "parent_package":
                matches = equivalent_parent(left, right)
            else:
                matches = left == (right or "")
            if not matches:
                mismatches.append("%s CSV=%r catalog=%r" %
                                  (csv_field, left, right))
        if mismatches:
            report.error("inventory-agreement", name, "; ".join(mismatches))
            disagreements.append(name)
    return sorted(set(disagreements))


def parse_connection_managers(folder, cache):
    if folder in cache:
        return cache[folder]
    by_name = {}
    by_id = {}
    folder_path = os.path.join(lib.REPO_ROOT, "ssis", folder)
    try:
        filenames = sorted(os.listdir(folder_path))
    except OSError:
        filenames = []
    for filename in filenames:
        if not filename.endswith(".conmgr"):
            continue
        path = os.path.join(folder_path, filename)
        try:
            root = ET.parse(path).getroot()
        except (ET.ParseError, OSError):
            continue
        name = root.attrib.get(OBJECT_NAME)
        creation_name = root.attrib.get(CREATION_NAME)
        manager_id = root.attrib.get(DTSID)
        if name and creation_name:
            by_name[name] = creation_name
        if manager_id and creation_name:
            by_id[manager_id] = creation_name
    cache[folder] = (by_name, by_id)
    return cache[folder]


def parse_package(package, conmgr_cache):
    facts = {
        "execute_sql_tasks": 0,
        "data_flow_tasks": 0,
        "script_tasks": 0,
        "execute_package_tasks": 0,
        "file_system_tasks": 0,
        "foreach_loops": 0,
        "send_mail_tasks": 0,
        "connection_managers": set(),
        "connection_manager_types": set(),
        "child_packages": package.child_packages(),
        "parse_error": "",
    }
    try:
        root = ET.fromstring(package.text)
    except ET.ParseError:
        facts["parse_error"] = "yes"
        return facts

    executable_types = []
    for element in root.iter(EXECUTABLE):
        executable_type = element.attrib.get(EXECUTABLE_TYPE, "")
        executable_types.append(executable_type)
        if executable_type == "Microsoft.ExecuteSQLTask":
            facts["execute_sql_tasks"] += 1
        if executable_type == "Microsoft.Pipeline":
            facts["data_flow_tasks"] += 1
        if executable_type == "Microsoft.ScriptTask":
            facts["script_tasks"] += 1
        if executable_type == "Microsoft.ExecutePackageTask" or (
                "ExecutePackageTask" in executable_type):
            facts["execute_package_tasks"] += 1
        if executable_type == "Microsoft.FileSystemTask":
            facts["file_system_tasks"] += 1
        if executable_type == "STOCK:FOREACHLOOP":
            facts["foreach_loops"] += 1
        if "SendMailTask" in executable_type:
            facts["send_mail_tasks"] += 1

    for element in root.iter():
        if local_name(element.tag) in ("ScriptTask", "ScriptProject"):
            facts["script_tasks"] += 1
        if element.tag == CONNECTION_MANAGER:
            name = element.attrib.get(OBJECT_NAME)
            creation_name = element.attrib.get(CREATION_NAME)
            if name:
                facts["connection_managers"].add(name)
            if creation_name:
                facts["connection_manager_types"].add(creation_name)

    by_name, by_id = parse_connection_managers(package.folder, conmgr_cache)
    for name in CONNECTION_REF_RE.findall(package.text):
        facts["connection_managers"].add(name)
        creation_name = by_name.get(name)
        if creation_name:
            facts["connection_manager_types"].add(creation_name)
    for manager_id in SQL_CONNECTION_RE.findall(package.text):
        creation_name = by_id.get(manager_id)
        if creation_name:
            facts["connection_manager_types"].add(creation_name)

    return facts


def load_plan():
    path = os.path.join(lib.REPO_ROOT, "ssis", "orchestration-plan.json")
    with open(path) as handle:
        plan = json.load(handle)
    plan_children = set()
    plan_children_by_root = {}
    plan_roots = set()
    for root in plan.get("roots", []):
        root_name = root.get("root")
        if not root_name:
            continue
        plan_roots.add(root_name)
        children = plan_children_by_root.setdefault(root_name, set())
        for node in root.get("nodes", []):
            for child in node.get("children", []):
                child_name = child.get("package")
                if child_name:
                    plan_children.add((root_name, child_name))
                    children.add(child_name)
    return plan_children, plan_children_by_root, plan_roots


def risk_for(name, load_type, source_system, folder, facts, connection_types):
    if facts["script_tasks"] > 0:
        return "HIGH", "Script Task: custom .NET code must be rewritten"
    if name.startswith("ING_FILE_") or load_type == "file_ingest":
        return "HIGH", (
            "flat-file ingestion: file-share pickup, FLATFILE parsing and "
            "quarantine handling need Auto Loader/Volumes redesign")
    if load_type == "SCD2":
        return "HIGH", "SCD2 dimension: history/merge semantics must be re-implemented"
    if source_system == "Oracle" or name.startswith("EXT_ORA_"):
        return "HIGH", (
            "Oracle-sourced extract: needs JDBC/Lakehouse Federation connectivity "
            "and Oracle SQL translation")
    if name.startswith("MNT_"):
        return "HIGH", (
            "engine maintenance (index/statistics/purge/disk) has no direct "
            "Databricks equivalent")
    if (facts["send_mail_tasks"] > 0 or "Notify" in name or "Export" in name or
            "SMTP" in connection_types):
        return "HIGH", (
            "mail/notification or export-to-file: external side effect outside "
            "the lakehouse")
    if facts["file_system_tasks"] > 0 or facts["foreach_loops"] > 0:
        return "HIGH", (
            "File System / ForEach file tasks: file-share operations need redesign")
    if load_type == "orchestration":
        return "MEDIUM", (
            "master orchestration: control-framework batch/step logic maps to "
            "Lakeflow Jobs but needs rework")
    if load_type in {"utility", "correction", "rekey", "dedup"} or (
            folder == "15_error_handling"):
        return "MEDIUM", (
            "utility/error-handling control logic: procedural T-SQL orchestration "
            "to be re-homed")
    return "LOW", (
        "%s: straight T-SQL extract/stage/fact/reference/aggregate load, "
        "translatable to SQL/Delta" % load_type)


def write_csv(path, rows):
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def run(args):
    report = lib.Report("reconcile_ssis_inventory")
    inventory_rows = load_inventory()
    inventory = {row["package"]: row for row in inventory_rows}
    catalog = lib.load_catalog()
    declared = {p["package"]: p for p in catalog["ssis"]["packages"]}
    agreement_names = inventory_agreement(report, inventory, declared)
    packages = lib.load_packages(catalog)
    package_names = set(packages)
    plan_children, plan_children_by_root, plan_roots = load_plan()
    execute_edges = {
        (parent.name, child)
        for parent in packages.values()
        for child in parent.child_packages()
        if child in packages
    }

    on_disk_extra = sorted(package_names - set(inventory))
    for name in on_disk_extra:
        report.error("on-disk-not-in-inventory", name,
                     "package is on disk but absent from the CSV inventory")

    inventory_missing = []
    for name, row in inventory.items():
        full_path = os.path.join(lib.REPO_ROOT, row.get("path", ""))
        if name not in packages or not os.path.exists(full_path):
            inventory_missing.append(name)
            report.error("in-inventory-not-on-disk", name,
                         "CSV inventory path is not present on disk")
    inventory_missing.sort()

    conmgr_cache = {}
    output_rows = []
    parent_mismatch = []
    unresolved_children = []
    parse_errors = []
    risk_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    script_names = []
    data_flow_names = []
    risk_by_folder = {}
    execute_package_total = 0

    for name in sorted(package_names | set(inventory)):
        csv_row = inventory.get(name, {})
        package = packages.get(name)
        catalog_row = declared.get(name, {}) if package else {}
        facts = (parse_package(package, conmgr_cache) if package else {
            "execute_sql_tasks": 0, "data_flow_tasks": 0, "script_tasks": 0,
            "execute_package_tasks": 0, "file_system_tasks": 0,
            "foreach_loops": 0, "send_mail_tasks": 0,
            "connection_managers": set(), "connection_manager_types": set(),
            "child_packages": [], "parse_error": "",
        })
        if facts["parse_error"]:
            parse_errors.append(name)

        if package:
            path = package.path if name in on_disk_extra else csv_row.get("path", package.path)
            file_present = os.path.exists(os.path.join(lib.REPO_ROOT, path))
        else:
            path = csv_row.get("path", "")
            file_present = os.path.exists(os.path.join(lib.REPO_ROOT, path))

        def value(field, catalog_field=None):
            catalog_field = catalog_field or field
            if package:
                return csv_row.get(field, catalog_row.get(catalog_field) or "")
            return csv_row.get(field, "")

        parent = csv_row.get("parent_package", "") or (
            catalog_row.get("parent") or "" if package else "")
        actual_parents = sorted(
            {edge_parent for edge_parent, child in execute_edges if child == name}
            | {root for root, child in plan_children if child == name})
        actual_parents_text = ";".join(actual_parents)
        edge_source = ""
        if parent:
            if (parent, name) in execute_edges:
                edge_source = "execute-package-task"
            elif (parent, name) in plan_children:
                edge_source = "orchestration-plan"
            else:
                parent_mismatch.append(name)
                report.warn(
                    "parent-mismatch", name,
                    "catalog declares parent '%s' but neither an Execute Package "
                    "Task nor ssis/orchestration-plan.json references it "
                    "(actually invoked by: %s)" %
                    (parent, actual_parents_text or "no root or package invokes it"))

        children = facts["child_packages"]
        unresolved = [child for child in children if child not in package_names]
        if unresolved:
            unresolved_children.append(name)
            report.error("unresolved-child", name,
                         "package references missing child package(s): %s" %
                         ", ".join(unresolved))

        load_type = value("load_type")
        folder = value("folder")
        source_system = value("source_system")
        risk, reason = risk_for(
            name, load_type, source_system, folder, facts,
            facts["connection_manager_types"])
        risk_counts[risk] += 1
        risk_by_folder.setdefault(folder, {"HIGH": 0, "MEDIUM": 0, "LOW": 0})[risk] += 1
        if facts["script_tasks"] > 0:
            script_names.append(name)
        if facts["data_flow_tasks"] > 0:
            data_flow_names.append(name)
        execute_package_total += facts["execute_package_tasks"]

        output_rows.append({
            "package": name,
            "project": value("project"),
            "folder": folder,
            "domain": value("domain"),
            "load_type": load_type,
            "source_system": source_system,
            "target_system": value("target_system"),
            "parent": parent,
            "parent_edge_source": edge_source,
            "actual_parents": actual_parents_text,
            "file_present": "yes" if file_present else "no",
            "path": path,
            "execute_sql_tasks": facts["execute_sql_tasks"],
            "data_flow_tasks": facts["data_flow_tasks"],
            "script_tasks": facts["script_tasks"],
            "execute_package_tasks": facts["execute_package_tasks"],
            "file_system_tasks": facts["file_system_tasks"],
            "foreach_loops": facts["foreach_loops"],
            "send_mail_tasks": facts["send_mail_tasks"],
            "connection_managers": ";".join(sorted(facts["connection_managers"])),
            "connection_manager_types": ";".join(
                sorted(facts["connection_manager_types"])),
            "child_packages": ";".join(children),
            "plan_children_count": (
                len(plan_children_by_root.get(name, set()))
                if name in plan_roots else 0),
            "on_disk_not_in_inventory": "yes" if name in on_disk_extra else "",
            "in_inventory_not_on_disk": "yes" if name in inventory_missing else "",
            "parent_mismatch": "yes" if name in parent_mismatch else "",
            "unresolved_child": ";".join(unresolved),
            "parse_error": facts["parse_error"],
            "databricks_risk": risk,
            "databricks_risk_reason": reason,
        })

    if not args.no_write:
        write_csv(args.output, output_rows)
        if not args.json:
            sys.stderr.write("Wrote %s\n" % args.output)

    report.count("rows", len(output_rows))
    report.count("inventory_rows_csv", len(inventory_rows))
    report.count("catalog_rows", len(declared))
    report.count("packages_on_disk", len(packages))
    report.count("inventory_disagreements", len(agreement_names))
    report.count("on_disk_not_in_inventory", len(on_disk_extra))
    report.count("in_inventory_not_on_disk", len(inventory_missing))
    report.count("parent_mismatch", len(parent_mismatch))
    report.count("unresolved_child", len(unresolved_children))
    report.count("parse_errors", len(parse_errors))
    report.count("risk_high", risk_counts["HIGH"])
    report.count("risk_medium", risk_counts["MEDIUM"])
    report.count("risk_low", risk_counts["LOW"])
    report.count("packages_with_script_tasks", len(script_names))
    report.count("packages_with_data_flows", len(data_flow_names))
    report.count("execute_package_tasks_total", execute_package_total)
    report.count("plan_roots", len(plan_roots))
    report.detail("inventory_disagreements", agreement_names)
    report.detail("on_disk_not_in_inventory", on_disk_extra)
    report.detail("in_inventory_not_on_disk", inventory_missing)
    report.detail("parent_mismatch", sorted(set(parent_mismatch)))
    report.detail("unresolved_child", sorted(set(unresolved_children)))
    report.detail("parse_errors", sorted(parse_errors))
    report.detail("packages_with_script_tasks", sorted(script_names))
    report.detail("packages_with_data_flows", sorted(data_flow_names))
    report.detail("risk_by_folder", risk_by_folder)

    return report.emit(
        as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    parser.add_argument("--output", default=os.path.join(lib.REPO_ROOT, DEFAULT_OUTPUT),
                        help="reconciliation CSV path")
    parser.add_argument("--no-write", action="store_true",
                        help="do not write the reconciliation CSV")
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
