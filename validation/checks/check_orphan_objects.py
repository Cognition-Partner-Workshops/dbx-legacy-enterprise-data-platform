#!/usr/bin/env python3
"""Orphan analysis for the staging database and the warehouse.

Where check_source_to_target_coverage.py walks the catalog, this check walks
what is actually on disk: every table and view created by a SQL file under
sqlserver/staging, sqlserver/warehouse and sqlserver/views, and every reject or
work table used by the error-handling layer. An object is an orphan when
nothing in the estate writes it, or nothing in the estate reads it.

Three orphan classes are reported:

  * `no-writer`   - the object is created and read, but no package and no
                    procedure populates it. Downstream code would read an
                    empty table.
  * `no-reader`   - the object is populated but nothing downstream selects
                    from it. Classic accreted dead weight.
  * `isolated`    - the object is created and never mentioned again anywhere
                    in the estate.

Every classification comes from text matching over the checked-in files. A
table populated by dynamic SQL that composes its name at runtime, or read by a
report outside this repository, will look more orphaned than it is; that
caveat is repeated in validation/README.md and in
docs/known-unvalidated-items.md.

Usage:
    python3 validation/checks/check_orphan_objects.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict

import estatelib as lib

CHECK = "orphans"

# Only these layers are analysed: the estate owns them end to end, so an
# orphan there is a real finding rather than an artefact of the base sample.
ANALYSED_PREFIXES = (
    "sqlserver/staging/",
    "sqlserver/warehouse/",
    "sqlserver/views/",
)

WRITE_KEYWORDS = ("INSERT", "MERGE", "UPDATE", "DELETE", "TRUNCATE", "SELECT INTO", "CREATE")


def object_definitions(sql_files):
    """Return {object_key: (raw_name, defining_file)} for the analysed layers."""
    definitions = {}
    for rel, _layer, text in sql_files:
        if not rel.startswith(ANALYSED_PREFIXES):
            continue
        for raw in lib.CREATE_TABLE_RE.findall(text):
            key = lib.normalise_object(raw)
            if "." not in key:
                continue
            definitions.setdefault(key, (raw.replace("[", "").replace("]", ""), rel))
    return definitions


def reference_maps(definitions, sql_files, packages):
    """Return (writers, readers) as {object_key: set(source descriptions)}."""
    writers = defaultdict(set)
    readers = defaultdict(set)

    patterns = {}
    for key in definitions:
        schema, _, obj = key.partition(".")
        patterns[key] = re.compile(
            r"\b%s\s*\.\s*%s\b" % (re.escape(schema), re.escape(obj)), re.I)

    write_re = re.compile(
        r"\b(?:INSERT\s+INTO|INSERT|MERGE\s+INTO|MERGE|UPDATE|DELETE\s+FROM|TRUNCATE\s+TABLE)\s+"
        r"([\[\]\w\.]+)", re.I)
    read_re = re.compile(r"\b(?:FROM|JOIN|USING)\s+([\[\]\w\.]+)", re.I)

    for rel, _layer, text in sql_files:
        squashed = text.replace("[", "").replace("]", "")
        for raw in write_re.findall(squashed):
            key = lib.normalise_object(raw)
            if key in definitions and not rel.endswith(key.split(".")[-1] + ".sql"):
                writers[key].add(rel)
        for raw in read_re.findall(squashed):
            key = lib.normalise_object(raw)
            if key in definitions:
                readers[key].add(rel)

    catalog_keys = set(definitions)
    for package in packages.values():
        for key in lib.expand_references(package.target_objects, catalog_keys):
            if key in definitions:
                writers[key].add(package.path)
        for key in lib.expand_references(package.source_objects, catalog_keys):
            if key in definitions:
                readers[key].add(package.path)
        for key, pattern in patterns.items():
            if pattern.search(package.text):
                readers[key].add(package.path)

    return writers, readers


def run(args):
    report = lib.Report("check_orphan_objects")
    catalog = lib.load_catalog()
    packages = lib.load_packages(catalog)
    sql_files = lib.load_sql_files()

    definitions = object_definitions(sql_files)
    writers, readers = reference_maps(definitions, sql_files, packages)

    no_writer, no_reader, isolated = [], [], []

    for key in sorted(definitions):
        raw, defining_file = definitions[key]
        wrote = writers.get(key) or set()
        read = readers.get(key) or set()
        is_view = key.split(".")[-1].startswith("VW_")
        if not wrote and not read:
            isolated.append(raw)
            report.error(CHECK, raw,
                         "created by %s and never referenced anywhere else in the estate"
                         % defining_file)
        elif not wrote and not is_view:
            no_writer.append(raw)
            report.error(CHECK, raw,
                         "read by %d file(s) but nothing populates it (defined in %s)"
                         % (len(read), defining_file))
        elif not read:
            no_reader.append(raw)
            report.warn(CHECK, raw,
                        "%s by %s but nothing in the estate selects from it"
                        % ("published" if is_view else "populated", ", ".join(sorted(wrote)[:2])))

    report.count("objects_analysed", len(definitions))
    report.count("orphans_isolated", len(isolated))
    report.count("orphans_without_writer", len(no_writer))
    report.count("orphans_without_reader", len(no_reader))
    report.detail("orphans_isolated", sorted(isolated))
    report.detail("orphans_without_writer", sorted(no_writer))
    report.detail("orphans_without_reader", sorted(no_reader))

    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
