#!/usr/bin/env python3
"""Source-to-target coverage across the estate.

For every object the catalog declares, work out whether any generated package
claims to read it and whether any generated package claims to write it, then
report the gaps. This is a *paper* trace: it follows the `source_objects` and
`target_objects` declarations of the 204 packages plus the object references
found in estate SQL. It does not parse data-flow column mappings and it does
not observe a single row moving anywhere.

Errors are raised for structural problems that would break a migration
inventory:

  * a package reads or writes an object that is neither declared in the
    catalog, nor created by an estate SQL file, nor part of the Microsoft
    WideWorldImporters base sample - i.e. a reference that points at nothing;
  * a warehouse dimension, fact or aggregate that no package and no procedure
    populates;
  * a staging object a package names explicitly as a source but that no
    package and no procedure ever populates.

Warnings are raised for coverage gaps that are *expected* in a twenty-year-old
estate and are part of the story the migration has to deal with:

  * source tables in Oracle or the OLTP database that nothing extracts;
  * staging or warehouse objects that are written but never read downstream;
  * objects that exist on disk or in the base sample but are missing from the
    catalog inventory.

Usage:
    python3 validation/checks/check_source_to_target_coverage.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import re
import sys

import estatelib as lib

CHECK = "source-to-target"

SOURCE_LAYERS = ("oracle-table", "oracle-view", "oltp-table", "oltp-view")
STAGING_LAYERS = ("staging-raw", "staging-stg", "staging-work", "staging-err", "staging-view")
WAREHOUSE_LAYERS = ("dw-dimension", "dw-fact", "dw-aggregate", "dw-report-view")


def sql_reference_index(objects, sql_files):
    """Map object key -> set of SQL files whose text mentions the object.

    Matching is on `schema.object` as a whole word, case-insensitively, which
    is deliberately generous: a mention inside a comment counts as a reference.
    """
    index = {key: set() for key in objects}
    patterns = []
    for key, meta in objects.items():
        schema, _, obj = meta["name"].partition(".")
        if not obj:
            continue
        pattern = re.compile(
            r"\b%s\s*\.\s*\[?%s\]?\b" % (re.escape(schema), re.escape(obj.replace(" ", ""))),
            re.I,
        )
        patterns.append((key, pattern))
    for rel, _layer, text in sql_files:
        squashed = text.replace("[", "").replace("]", "")
        for key, pattern in patterns:
            if pattern.search(squashed):
                index[key].add(rel)
    return index


def run(args):
    report = lib.Report("check_source_to_target_coverage")
    catalog = lib.load_catalog()
    objects = lib.catalog_objects(catalog)
    packages = lib.load_packages(catalog)
    producers, consumers = lib.package_index_by_object(packages, set(objects))
    explicit_producers, explicit_consumers = lib.package_index_by_object(
        packages, set(objects), explicit_only=True)

    sql_files = lib.load_sql_files()
    created = lib.created_objects(sql_files)

    # 1. Every package reference must resolve to something real.
    unregistered = []
    dangling = []
    for package in sorted(packages.values(), key=lambda p: p.name):
        for role, names in (("source", package.source_objects), ("target", package.target_objects)):
            for raw in names:
                key = lib.normalise_object(raw)
                if "*" in key or key.startswith("FILE:"):
                    continue  # file feeds are covered by check_orphan_objects
                if key in objects:
                    continue
                schema = key.split(".")[0]
                if key in created:
                    unregistered.append((package.name, raw, created[key]))
                    report.warn(CHECK, package.name,
                                "%s object '%s' is created by %s but is missing from "
                                "config/estate-catalog.yaml" % (role, raw, created[key]))
                elif schema in lib.BASE_SAMPLE_SCHEMAS:
                    report.warn(CHECK, package.name,
                                "%s object '%s' belongs to the Microsoft WideWorldImporters "
                                "base sample and is outside the estate inventory" % (role, raw))
                else:
                    dangling.append((package.name, role, raw))
                    report.error(CHECK, package.name,
                                 "%s object '%s' is not in the catalog, is not created by any "
                                 "estate SQL file, and is not a base sample object"
                                 % (role, raw))

    sql_refs = sql_reference_index(objects, sql_files)

    unloaded_warehouse = []
    unloaded_staging = []
    unread_targets = []
    unextracted_sources = []

    for key, meta in sorted(objects.items()):
        layer = meta["layer"]
        written = producers.get(key) or []
        read = explicit_consumers.get(key) or []
        wildcard_read = consumers.get(key) or []
        referenced_in_sql = sql_refs.get(key) or set()
        loaded_by_sql = [f for f in referenced_in_sql
                         if f.startswith("sqlserver/procedures/")
                         or f.startswith("sqlserver/staging/")
                         or f.startswith("sqlserver/warehouse/")]

        if layer in WAREHOUSE_LAYERS and layer != "dw-report-view":
            if not written:
                if loaded_by_sql:
                    report.warn(CHECK, meta["name"],
                                "no package declares it as a target; only SQL references it (%s)"
                                % ", ".join(sorted(loaded_by_sql)[:2]))
                else:
                    report.error(CHECK, meta["name"],
                                 "warehouse object has no loading package and no loading procedure")
                unloaded_warehouse.append(meta["name"])
        elif layer in STAGING_LAYERS:
            if read and not written and not loaded_by_sql:
                report.error(CHECK, meta["name"],
                             "named as a source by %s but no package or procedure populates it"
                             % ", ".join(sorted(read)[:3]))
                unloaded_staging.append(meta["name"])
            elif written and not read and not wildcard_read and not referenced_in_sql:
                report.warn(CHECK, meta["name"],
                            "populated by %s but nothing downstream reads it"
                            % ", ".join(sorted(written)[:3]))
                unread_targets.append(meta["name"])
        elif layer in SOURCE_LAYERS:
            if not read and not wildcard_read and not referenced_in_sql:
                report.warn(CHECK, meta["name"],
                            "%s source object is not extracted by any package and is not "
                            "referenced by any estate SQL" % meta["system"])
                unextracted_sources.append(meta["name"])

    report.count("catalog_objects", len(objects))
    report.count("objects_created_by_estate_sql", len(created))
    report.count("references_missing_from_catalog", len(unregistered))
    report.count("dangling_object_references", len(dangling))
    report.count("staging_sources_never_populated", len(unloaded_staging))
    report.detail("dangling_object_references",
                  ["%s %s %s" % (p, r, o) for p, r, o in dangling])
    report.detail("staging_sources_never_populated", sorted(unloaded_staging))
    report.count("packages", len(packages))
    report.count("objects_with_a_producer", len([k for k in objects if producers.get(k)]))
    report.count("objects_with_a_consumer", len([k for k in objects if consumers.get(k)]))
    report.count("warehouse_objects_without_a_package", len(unloaded_warehouse))
    report.count("staging_targets_never_read", len(unread_targets))
    report.count("source_objects_never_extracted", len(unextracted_sources))
    report.detail("warehouse_objects_without_a_package", sorted(unloaded_warehouse))
    report.detail("staging_targets_never_read", sorted(unread_targets))
    report.detail("source_objects_never_extracted", sorted(unextracted_sources))

    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
