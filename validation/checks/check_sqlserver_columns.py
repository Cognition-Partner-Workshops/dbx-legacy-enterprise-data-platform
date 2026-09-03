#!/usr/bin/env python3
"""Column-level contracts inside the SQL Server estate.

Four of the classes of failure the first deployment hit live here, and all four
are visible on disk:

  Msg 207  / 1911  a statement names a column the table does not have
                   (Dimension.Date [DateKey], Dimension.City [Region Code],
                    etl.ReconciliationExemption [IsActive])
  Msg 271          an INSERT writes a PERSISTED computed column
                   (Returns.usp_PostReturnInspection, usp_IssueCreditNote)
  Msg 1778         a foreign key against a differently-typed parent column
                   (Sales.OrderLines.PriceListLineID INT vs BIGINT)
  Msg 313 / 8144   a scalar function called with the wrong argument count
                   (Shipping.ufn_FreightCost, Returns.ufn_RestockingFee)

Static analysis only. Nothing here connects to SQL Server.

    python3 validation/checks/check_sqlserver_columns.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import os
import re
import sys

import estatelib as lib
import tsqllib

# Fact and web-analytics loaders written against a wider fact grain than the
# fact tables carry. None of these reached the engine in the first deployment -
# the fact tables are created after the loaders in the same stage - but they are
# the same Msg 207 defect and they are recorded here so that the check can be
# strict about anything new while the backlog is worked off. The file only ever
# shrinks: an entry that no longer reproduces is reported as an error.
BACKLOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "sqlserver_column_backlog.txt")

# Microsoft's own WideWorldImporters content is preserved as shipped and is not
# part of the estate's contract surface.
SKIP_PREFIXES = ("wwi-", "sample-scripts", "workload-drivers", "power-bi-dashboards")

CALL_RE = re.compile(
    r"\[?(\w+)\]?\s*\.\s*\[?(ufn_\w+)\]?\s*\(", re.I)

INTEGER_FAMILY = ("tinyint", "smallint", "int", "bigint")


def is_estate_file(path):
    return not path.startswith(SKIP_PREFIXES)


def load_backlog():
    if not os.path.exists(BACKLOG_PATH):
        return set()
    entries = set()
    with open(BACKLOG_PATH, encoding="utf-8") as handle:
        for line in handle:
            line = line.split("#", 1)[0].strip()
            if line:
                entries.add(line)
    return entries


def check_inserts(report, path, text, tables, backlog, seen):
    checked = 0
    for match in tsqllib.INSERT_RE.finditer(tsqllib.strip_comments(text)):
        key = "%s.%s" % (tsqllib.unbracket(match.group(1)), tsqllib.unbracket(match.group(2)))
        table = tables.get(key)
        if table is None or not table.columns:
            continue
        for raw in tsqllib.split_top_level(match.group(3)):
            column = tsqllib.unbracket(raw)
            if not column or not re.match(r"^[\w ]+$", column):
                continue
            checked += 1
            if table.is_computed(column):
                report.error(
                    "sqlserver-computed-write", path,
                    "INSERT INTO %s writes [%s], which is a computed column "
                    "(Msg 271 at deploy time)" % (key, column))
            elif not table.has(column) and table.created:
                entry = "%s|%s|%s" % (path, key, column)
                seen.add(entry)
                message = ("INSERT INTO %s names [%s]; the table has no such column "
                           "(Msg 207 at deploy time). Table defined in %s"
                           % (key, column, ", ".join(table.paths)))
                if entry in backlog:
                    report.warn("sqlserver-insert-column-backlog", path, message)
                else:
                    report.error("sqlserver-insert-column", path, message)
    return checked


def check_indexes(report, path, text, tables):
    checked = 0
    for match in tsqllib.CREATE_INDEX_RE.finditer(tsqllib.strip_comments(text)):
        key = "%s.%s" % (tsqllib.unbracket(match.group(1)), tsqllib.unbracket(match.group(2)))
        table = tables.get(key)
        if table is None or not table.columns or not table.created:
            continue
        for raw in tsqllib.split_top_level(match.group(3)):
            column = tsqllib.unbracket(re.sub(r"\b(ASC|DESC)\b", "", raw, flags=re.I))
            if not column or not re.match(r"^[\w ]+$", column):
                continue
            checked += 1
            if not table.has(column):
                report.error(
                    "sqlserver-index-column", path,
                    "index on %s keys on [%s], which the table does not have "
                    "(Msg 1911 at deploy time)" % (key, column))
    return checked


def check_foreign_keys(report, path, text, tables):
    clean = tsqllib.strip_comments(text)
    checked = 0
    for match in tsqllib.CREATE_TABLE_RE.finditer(clean):
        child_key = "%s.%s" % (tsqllib.unbracket(match.group(1)),
                               tsqllib.unbracket(match.group(2)))
        body = tsqllib.balanced_body(clean, match.end())
        checked += _check_fk_body(report, path, child_key, body, tables)
    for alter in re.finditer(
            r"ALTER\s+TABLE\s+(%s)\s*\.\s*(%s)\s+(?:WITH\s+\w+\s+)?ADD\s+CONSTRAINT\b(.*?);"
            % (tsqllib.NAME, tsqllib.NAME), clean, re.I | re.S):
        child_key = "%s.%s" % (tsqllib.unbracket(alter.group(1)),
                               tsqllib.unbracket(alter.group(2)))
        checked += _check_fk_body(report, path, child_key, alter.group(3), tables)
    return checked


def _check_fk_body(report, path, child_key, body, tables):
    child = tables.get(child_key)
    checked = 0
    for child_cols, schema, name, parent_cols in tsqllib.FOREIGN_KEY_RE.findall(body):
        parent_key = "%s.%s" % (tsqllib.unbracket(schema), tsqllib.unbracket(name))
        parent = tables.get(parent_key)
        if child is None or parent is None:
            continue
        pairs = zip(tsqllib.split_top_level(child_cols), tsqllib.split_top_level(parent_cols))
        for raw_child, raw_parent in pairs:
            child_column = tsqllib.unbracket(raw_child)
            parent_column = tsqllib.unbracket(raw_parent)
            child_type = child.type_of(child_column)
            parent_type = parent.type_of(parent_column)
            if not child_type or not parent_type:
                continue
            checked += 1
            if child_type != parent_type:
                report.error(
                    "sqlserver-fk-datatype", path,
                    "%s.[%s] is %s but the referenced %s.[%s] is %s "
                    "(Msg 1778 at deploy time)" % (
                        child_key, child_column, child_type,
                        parent_key, parent_column, parent_type))
    return checked


def check_function_calls(report, path, text, routines):
    clean = tsqllib.strip_comments(text)
    checked = 0
    for match in CALL_RE.finditer(clean):
        key = "%s.%s" % (match.group(1), match.group(2))
        routine = None
        for candidate, value in routines.items():
            if candidate.lower() == key.lower():
                routine = value
                break
        if routine is None or routine.kind != "FUNCTION":
            continue
        arguments = [a for a in tsqllib.split_top_level(
            tsqllib.balanced_body(clean, match.end())) if a.strip()]
        checked += 1
        if len(arguments) < routine.minimum_arguments or len(arguments) > len(routine.parameters):
            report.error(
                "sqlserver-function-arguments", path,
                "%s is called with %d argument(s); %s declares %d parameter(s), "
                "%d of them mandatory (Msg 313/8144 at deploy time)" % (
                    routine.key, len(arguments), routine.path,
                    len(routine.parameters), routine.minimum_arguments))
    return checked


def run(args):
    report = lib.Report("check_sqlserver_columns")
    files = [(path, text) for path, text in tsqllib.sql_files() if is_estate_file(path)]
    tables = tsqllib.load_tables(files)
    routines = tsqllib.load_routines(files)

    backlog = load_backlog()
    seen = set()
    inserts = indexes = keys = calls = 0
    for path, text in files:
        inserts += check_inserts(report, path, text, tables, backlog, seen)
        indexes += check_indexes(report, path, text, tables)
        keys += check_foreign_keys(report, path, text, tables)
        calls += check_function_calls(report, path, text, routines)

    report.count("sqlserver_tables_parsed", len(tables))
    report.count("sqlserver_routines_parsed", len(routines))
    report.count("insert_columns_checked", inserts)
    report.count("index_columns_checked", indexes)
    report.count("foreign_key_columns_checked", keys)
    report.count("function_calls_checked", calls)
    report.count("insert_column_backlog_entries", len(backlog))
    for stale in sorted(backlog - seen):
        report.error(
            "sqlserver-insert-column-backlog", "validation/checks/sqlserver_column_backlog.txt",
            "%s no longer reproduces; remove the entry so the backlog keeps shrinking" % stale)
    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
