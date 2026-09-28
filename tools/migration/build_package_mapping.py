"""Build docs/migration/package-mapping-appendix.md from the checked-in inventories.

Joins docs/inventories/ssis-packages.csv, docs/inventories/source-target-map.csv,
config/landing-zone.yaml and ssis/orchestration-plan.json into one table per
migration layer so every SSIS package has a Databricks target (bronze table,
notebook path, owning child session) and every master has its phase-by-phase
task list. Nothing here connects to anything; it reads the repository only.

Run from the repository root:
    python3 tools/migration/build_package_mapping.py
"""
from __future__ import annotations

import csv
import json
import os
from collections import defaultdict

import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PACKAGES_CSV = os.path.join(REPO_ROOT, "docs", "inventories", "ssis-packages.csv")
SOURCE_TARGET_CSV = os.path.join(REPO_ROOT, "docs", "inventories", "source-target-map.csv")
LANDING_ZONE_YAML = os.path.join(REPO_ROOT, "config", "landing-zone.yaml")
PLAN_JSON = os.path.join(REPO_ROOT, "ssis", "orchestration-plan.json")
OUTPUT_MD = os.path.join(REPO_ROOT, "docs", "migration", "package-mapping-appendix.md")

# Legacy schema -> target schema (section 3 and 10 of the migration plan).
SCHEMA_MAP = {
    "raw": "bronze",
    "stg": "stg",
    "work": "work",
    "err": "err",
    "ref": "ref",
    "etl": "ctl",
    "Dimension": "dim",
    "Fact": "fact",
    "Aggregate": "agg",
    "Report": "report",
}

# Folder -> (owning child session, notebook directory, target schema when the
# source-target map does not say).
FOLDER_MAP = {
    "00_orchestration": ("F", "workflows", None),
    "01_oracle_extract": ("B", "ingest/oracle", "bronze"),
    "02_sqlserver_extract": ("B", "ingest/sqlserver", "bronze"),
    "03_file_ingestion": ("B", "ingest/files", "bronze"),
    "04_staging": ("C", "stage", "stg"),
    "05_data_quality": ("C", "quality", "err"),
    "06_reference_data": ("C", "reference", "ref"),
    "07_dimensions": ("D", "dim", "dim"),
    "08_facts": ("D", "fact", "fact"),
    "09_aggregates": ("D", "agg", "agg"),
    "10_finance": ("E", "mart/finance", "mart_finance"),
    "11_sales": ("E", "mart/sales", "mart_sales"),
    "12_inventory": ("E", "mart/inventory", "mart_inventory"),
    "13_procurement": ("E", "mart/procurement", "mart_procurement"),
    "14_customer_360": ("E", "mart/customer360", "mart_customer360"),
    "15_error_handling": ("C", "ops", "ctl"),
    "99_maintenance": ("F", "ops/maintenance", "ctl"),
}

# Packages whose recorded lineage needs confirming before bronze is named
# (migration plan hazard H5) or that the plan does not reach.
LINEAGE_FLAGS = {
    "EXT_ORA_CodeTranslation": "recorded target raw.OracleCustomerMaster looks wrong for WWI_REF.CODE_TRANSLATION - confirm against the .dtsx (H5)",
    "EXT_ORA_ProductHierarchy": "recorded target raw.OracleProductMaster shared with EXT_ORA_ProductMaster - confirm (H5)",
    "EXT_SQL_LoyaltyLedger": "in source-target-map.csv but not a child of any master - confirm whether orphaned; Fact.Loyalty Points needs it",
}

LOAD_TYPE_PATTERN = {
    "full": "single JDBC read; bronze partition per batch; stg does INSERT OVERWRITE",
    "incremental_timestamp": "JDBC read [from, to) with lookback; stg MERGE",
    "incremental_key": "source MAX(key) read first, then (from, to]; partitioned JDBC read",
    "date_window": "window [business_date - lookback_days, business_date]; replaceWhere on the range",
    "file_ingest": "Auto Loader per feed with declared schema; ctl.file_register",
    "incremental_append": "MERGE on natural key from the bronze batch partition",
    "truncate_reload": "INSERT OVERWRITE from the latest bronze batch partition",
    "work_rebuild": "replaceWhere batch_id = ? into a batch-partitioned work table",
    "quality_screen": "rule evaluation -> ctl.dq_result, err.* + ctl.rejected_record",
    "full_refresh": "INSERT OVERWRITE ref.* (effective-dated where the rules module reads it)",
    "SCD1": "MERGE on natural key",
    "SCD2": "MERGE expire-and-insert on (natural_key, is_current)",
    "rekey": "MERGE from ctl.fact_rekey_queue into each fact on the degenerate key",
    "incremental_fact": "replaceWhere on the date-key window (or MERGE for in-place restatement)",
    "snapshot_fact": "MERGE on process key (accumulating) or replaceWhere snapshot_date_key (periodic)",
    "correction": "append REV/RES pairs (Fact.Sale) or MERGE restatement (Fact.Payment, Fact.Order)",
    "dedup": "window over the degenerate key, deterministic order",
    "aggregate_rebuild": "SQL notebook; replaceWhere on the refreshed window or full overwrite",
    "publish": "validate rules, write ctl.reconciliation_result, upsert report.publish_state",
    "business_rule": "mart notebook reading fact/dim/agg, writing mart_<domain>.*",
    "utility": "operational task on ctl / volumes",
    "orchestration": "parent job generated from ssis/orchestration-plan.json",
}


def to_snake(name: str) -> str:
    out = []
    for i, ch in enumerate(name):
        if ch.isupper() and i and (name[i - 1].islower() or (i + 1 < len(name) and name[i + 1].islower())):
            out.append("_")
        out.append(ch.lower() if ch.isalnum() else "_")
    s = "".join(out)
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


def map_object(legacy: str) -> str:
    if "." not in legacy:
        return legacy
    schema, table = legacy.split(".", 1)
    target_schema = SCHEMA_MAP.get(schema, to_snake(schema))
    return f"{target_schema}.{to_snake(table)}"


def read_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def child_name(child) -> str:
    if isinstance(child, dict):
        return child.get("package") or child.get("name") or json.dumps(child, sort_keys=True)
    return str(child)


def load_plan() -> tuple[dict, dict]:
    with open(PLAN_JSON, encoding="utf-8") as fh:
        plan = json.load(fh)
    roots = {r["root"]: r for r in plan["roots"]}
    membership = defaultdict(set)
    for root in plan["roots"]:
        for node in root["nodes"]:
            for child in (node.get("children") or []) + (node.get("in_package_children") or []):
                membership[child_name(child)].add(root["root"])
    return roots, membership


def md_table(headers: list[str], rows: list[list[str]]) -> str:
    esc = lambda v: str(v).replace("|", "\\|").replace("\n", " ")
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    lines += ["| " + " | ".join(esc(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def main() -> None:
    packages = read_csv(PACKAGES_CSV)
    st_rows = read_csv(SOURCE_TARGET_CSV)
    with open(LANDING_ZONE_YAML, encoding="utf-8") as fh:
        landing = yaml.safe_load(fh)
    roots, membership = load_plan()

    st_by_pkg = defaultdict(list)
    for r in st_rows:
        st_by_pkg[r["package"]].append(r)
    pkg_by_name = {p["package"]: p for p in packages}

    out: list[str] = []
    out.append("# Package mapping appendix\n")
    out.append(
        "Generated by `tools/migration/build_package_mapping.py` from `docs/inventories/ssis-packages.csv`, "
        "`docs/inventories/source-target-map.csv`, `config/landing-zone.yaml` and `ssis/orchestration-plan.json`. "
        "Do not hand-edit; re-run the script. Target names use the placeholder catalog `{catalog}` (decision D2 in "
        "`databricks-migration-plan.md`) and are written `schema.table`. Everything here is read from checked-in "
        "definitions; none of it is runtime evidence.\n"
    )
    out.append(
        f"Inventory: {len(packages)} packages in `ssis-packages.csv`, {len(st_rows)} lineage rows in "
        f"`source-target-map.csv`, {len(roots)} masters in the orchestration plan.\n"
    )
    out.append("Sections: A1 Oracle extracts, A2 SQL Server extracts, A3 file feeds, A4 staging/DQ/reference, "
               "A5 dimensions/facts/aggregates, A6 marts, A7 error handling and maintenance, A8 masters phase by phase, "
               "A9 lineage items to confirm.\n")

    def extract_section(title: str, folder: str, anchor: str) -> None:
        out.append(f"\n## {anchor}. {title}\n")
        rows = []
        names = sorted({p["package"] for p in packages if p["folder"] == folder} | {r["package"] for r in st_rows if r["folder"] == folder})
        for name in names:
            lineage = st_by_pkg.get(name, [])
            pkg = pkg_by_name.get(name)
            masters = ", ".join(sorted(membership.get(name, []))) or "(none)"
            src = "; ".join(sorted({f"{r['source_system']}: {r['source_object']}" for r in lineage})) or "(not in source-target map)"
            tgt_legacy = "; ".join(sorted({r["target_object"] for r in lineage})) or "(none)"
            tgt = "; ".join(sorted({map_object(r["target_object"]) for r in lineage})) or "(confirm)"
            load_type = pkg["load_type"] if pkg else "; ".join(sorted({r["load_type"] for r in lineage}))
            session, nb_dir, _ = FOLDER_MAP[folder]
            flag = LINEAGE_FLAGS.get(name, "")
            if not pkg:
                flag = (flag + "; " if flag else "") + "not in ssis-packages.csv"
            rows.append([name, src, load_type, LOAD_TYPE_PATTERN.get(load_type, ""), tgt_legacy, tgt,
                         f"`{nb_dir}/run_extract` (definition `{name}`)", masters, session, flag])
        out.append(md_table(["Package", "Source", "Load type", "Watermark / write pattern", "Legacy target",
                             "Target table", "Notebook", "Masters", "Session", "Confirm"], rows))

    extract_section("Oracle extracts (`ssis/01_oracle_extract`)", "01_oracle_extract", "A1")
    extract_section("SQL Server extracts (`ssis/02_sqlserver_extract`)", "02_sqlserver_extract", "A2")

    out.append("\n## A3. File feeds (`ssis/03_file_ingestion`, `config/landing-zone.yaml`)\n")
    out.append(f"Landing root variable `{landing['root_variable']}` becomes the external volume root (decision D5); "
               "relative paths are preserved.\n")
    rows = []
    for sub in landing["directories"]["inbound"]["subdirectories"]:
        rows.append([sub["consumed_by"], sub["path"], sub["pattern"], sub["encoding"],
                     {",": "comma", "\t": "tab", "|": "pipe"}.get(sub["delimiter"], repr(sub["delimiter"])),
                     "yes" if sub["header"] else "no", sub["raw_table"], map_object(sub["raw_table"]),
                     "`ingest/files/run_feed`", ", ".join(sorted(membership.get(sub["consumed_by"], []))), "B",
                     sub.get("notes", "")])
    rows.append(["ING_FILE_QuarantineMalformed", "quarantine/{feed}/{yyyyMMdd}/", "*", "-", "-", "-",
                 "err.RejectedFileRow", "err.rejected_file_row", "`ingest/files/quarantine`",
                 ", ".join(sorted(membership.get("ING_FILE_QuarantineMalformed", []))), "B",
                 "copies files failing the structural screen after QuarantineAfterAttempts (2)"])
    out.append(md_table(["Package", "Path", "Pattern", "Encoding", "Delimiter", "Header", "Legacy target",
                         "Target table", "Notebook", "Masters", "Session", "Format notes to preserve"], rows))
    hk = []
    for name, spec in landing["directories"].items():
        if name == "inbound":
            continue
        hk.append([name, spec.get("purpose", ""), spec.get("layout", ""), str(spec.get("retention_days", "")),
                   "; ".join(f"{k}: {v.get('retention_days')} days" for k, v in (spec.get("regional_overrides") or {}).items())])
    out.append("\nWorking and retention directories (`inbound/failed` and `inbound/processed` are drained by "
               "`ERR_Quarantine_BadFiles` and `MNT_Archive_ProcessedFiles`):\n")
    out.append(md_table(["Directory", "Purpose", "Layout", "Retention days", "Regional overrides"], hk))

    def package_section(title: str, folders: list[str], anchor: str) -> None:
        out.append(f"\n## {anchor}. {title}\n")
        rows = []
        for p in sorted((p for p in packages if p["folder"] in folders), key=lambda p: (p["folder"], p["package"])):
            session, nb_dir, default_schema = FOLDER_MAP[p["folder"]]
            lineage = st_by_pkg.get(p["package"], [])
            reads = "; ".join(sorted({map_object(r["source_object"]) if "." in r["source_object"] else r["source_object"] for r in lineage})) or "(see notebook spec)"
            writes = "; ".join(sorted({map_object(r["target_object"]) for r in lineage})) or f"{default_schema}.*"
            masters = ", ".join(sorted(membership.get(p["package"], []))) or "(none - run by Agent job or unreferenced)"
            rows.append([p["package"], p["folder"], p["load_type"], LOAD_TYPE_PATTERN.get(p["load_type"], ""),
                         reads, writes, f"`{nb_dir}/{to_snake(p['package'])}`", masters, session])
        out.append(md_table(["Package", "Folder", "Load type", "Write pattern", "Reads", "Writes", "Notebook",
                             "Masters", "Session"], rows))

    package_section("Staging, data quality and reference (`ssis/04`, `05`, `06`)",
                    ["04_staging", "05_data_quality", "06_reference_data"], "A4")
    package_section("Dimensions, facts and aggregates (`ssis/07`, `08`, `09`)",
                    ["07_dimensions", "08_facts", "09_aggregates"], "A5")
    package_section("Domain marts (`ssis/10`-`14`)",
                    ["10_finance", "11_sales", "12_inventory", "13_procurement", "14_customer_360"], "A6")
    package_section("Error handling and maintenance (`ssis/15`, `ssis/99`)",
                    ["15_error_handling", "99_maintenance"], "A7")

    out.append("\n## A8. Masters, phase by phase\n")
    out.append("One parent job per root. `streams` is the `for_each` concurrency ceiling (bounded by `MaxParallelStreams`). "
               "Edge kinds: S = Success, C = Completion, F = Failure; `expr` edges become condition tasks.\n")
    for root_name in sorted(roots):
        root = roots[root_name]
        out.append(f"\n### {root_name}\n")
        out.append(f"{root['description']}\n")
        params = ", ".join(f"`{k}`={json.dumps(v)}" for k, v in root.get("parameters", {}).items())
        out.append(f"Parameters: {params}\n")
        rows = []
        for node in root["nodes"]:
            kind = node["kind"]
            children = [child_name(c) for c in (node.get("children") or node.get("in_package_children") or [])]
            if kind == "phase":
                detail = f"{len(children)} pkgs, {node.get('streams', 1)} streams: " + ", ".join(children)
                task = "claim_step -> for_each(run_package) -> complete_step"
            elif kind == "batch_start":
                detail = f"batch_type {node.get('batch_type')}"
                task = "wwi_control.start_batch"
            elif kind == "batch_end":
                detail = f"force_status {node.get('force_status')}" if node.get("force_status") else ""
                task = "wwi_control.end_batch"
            elif kind == "control":
                detail = node.get("sql") or node.get("assignment") or ""
                task = "SQL task on ctl.* -> task value" if node.get("control") == "query" else "Python task -> task value"
            elif kind == "reconcile":
                detail = f"raise_on_failure {node.get('raise_on_failure')}"
                task = "wwi_control.assert_row_count_reconciliation"
            else:
                detail, task = "", kind
            rows.append([node["name"], kind, str(node.get("sequence", "")), task, detail])
        out.append(md_table(["Node", "Kind", "Seq", "Databricks task", "Detail"], rows))
        erows = []
        for e in root["edges"]:
            erows.append([e.get("from", e.get("source", "")), e.get("to", e.get("target", "")),
                          (e.get("value") or "")[:1].upper(),
                          e.get("expression") or ""])
        out.append("\nEdges:\n")
        out.append(md_table(["From", "To", "On", "Expression"], erows))

    out.append("\n## A9. Lineage items to confirm\n")
    rows = [[k, v] for k, v in LINEAGE_FLAGS.items()]
    unreferenced = sorted(p["package"] for p in packages
                          if p["folder"] != "00_orchestration" and not membership.get(p["package"]))
    rows.append(["Packages in the inventory not reached by any master",
                 ", ".join(unreferenced) if unreferenced else "(none)"])
    st_only = sorted(set(st_by_pkg) - set(pkg_by_name))
    rows.append(["Packages in source-target-map.csv but not in ssis-packages.csv", ", ".join(st_only) or "(none)"])
    out.append(md_table(["Item", "Detail"], rows))

    with open(OUTPUT_MD, "w", encoding="utf-8") as fh:
        fh.write("\n".join(out) + "\n")
    print(f"wrote {os.path.relpath(OUTPUT_MD, REPO_ROOT)}: {len(packages)} packages, {len(roots)} masters")


if __name__ == "__main__":
    main()
