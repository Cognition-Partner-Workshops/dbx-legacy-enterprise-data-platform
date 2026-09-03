#!/usr/bin/env python3
"""The Oracle partition maintenance runbook against the partitioned tables.

oracle/tables/ZZ_add_future_partitions.sql is hand-run, not deployed, and the
first attempt at running it produced three separate engine errors: a bare
literal where a DATE expression belongs (ORA-14036), a partition name no table
carries (ORA-02149), and a SPLIT against an INTERVAL-partitioned table
(ORA-14080). This check holds it to the tables as they are defined on disk.

Static analysis only. Nothing here connects to Oracle.

    python3 validation/checks/check_oracle_partitions.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import os
import re
import sys

import estatelib as lib
import oraclelib

RUNBOOK_PATH = "oracle/tables/ZZ_add_future_partitions.sql"

SPLIT_RE = re.compile(
    r"ALTER\s+TABLE\s+([A-Z0-9_]+)\.([A-Z0-9_]+)\s+"
    r"SPLIT\s+PARTITION\s+([A-Z0-9_]+)\s+AT\s*\((.*?)\)\s*"
    r"INTO\s*\((.*?)\)", re.I | re.S)

PARTITION_NAME_RE = re.compile(r"PARTITION\s+([A-Z0-9_]+)", re.I)
DATE_EXPRESSION_RE = re.compile(r"^\s*(TO_DATE|DATE)\b", re.I)
INTERVAL_RE = re.compile(r"\bINTERVAL\s*\(", re.I)


def load_table_sources():
    """{SCHEMA.TABLE: (path, text)} for the Oracle table scripts."""
    sources = {}
    root = os.path.join(lib.REPO_ROOT, "oracle", "tables")
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in sorted(filenames):
            if not filename.lower().endswith(".sql"):
                continue
            full = os.path.join(dirpath, filename)
            rel = os.path.relpath(full, lib.REPO_ROOT).replace(os.sep, "/")
            text = lib.read_text(full)
            for table in oraclelib.parse_tables(text, rel):
                sources[table.key] = (rel, text)
    return sources


def run(args):
    report = lib.Report("check_oracle_partitions")
    full_path = os.path.join(lib.REPO_ROOT, RUNBOOK_PATH)
    if not os.path.exists(full_path):
        report.count("oracle_partition_splits_checked", 0)
        return report.emit(as_json=args.json, strict=args.strict,
                           show_warnings=not args.quiet)

    sources = load_table_sources()
    text = oraclelib.strip_comments(lib.read_text(full_path))
    splits = 0
    for schema, name, target, boundary, into in SPLIT_RE.findall(text):
        key = "%s.%s" % (schema.upper(), name.upper())
        splits += 1
        source = sources.get(key)
        if source is None:
            report.error(
                "oracle-partition-table", RUNBOOK_PATH,
                "%s is split here but no oracle/tables script creates it" % key)
            continue
        table_text = oraclelib.strip_comments(source[1])
        declared = {found.upper() for found in PARTITION_NAME_RE.findall(table_text)}

        if INTERVAL_RE.search(table_text):
            report.error(
                "oracle-partition-interval", RUNBOOK_PATH,
                "%s is INTERVAL partitioned; Oracle cuts its partitions itself and "
                "SPLIT raises ORA-14080" % key)
        if target.upper() not in declared:
            report.error(
                "oracle-partition-name", RUNBOOK_PATH,
                "%s has no partition %s (ORA-02149); %s declares %s"
                % (key, target.upper(), source[0], ", ".join(sorted(declared)) or "none"))
        if not DATE_EXPRESSION_RE.match(boundary.strip()):
            report.error(
                "oracle-partition-boundary", RUNBOOK_PATH,
                "the split point for %s is %s; a DATE partition key needs a DATE "
                "expression (ORA-14036)" % (key, boundary.strip()))
        tail = PARTITION_NAME_RE.findall(into)
        if not tail or tail[-1].upper() != target.upper():
            report.error(
                "oracle-partition-tail", RUNBOOK_PATH,
                "%s splits %s but does not keep the tail partition's name; next "
                "year's run would not find it" % (key, target.upper()))

    report.count("oracle_partition_splits_checked", splits)
    report.count("oracle_partitioned_tables_parsed", len(sources))
    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
