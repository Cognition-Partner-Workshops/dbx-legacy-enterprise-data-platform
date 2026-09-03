#!/usr/bin/env python3
"""Control-framework integration coverage.

Every load in the estate is supposed to be visible in the etl control schema:
package executions logged, row counts recorded, errors captured, watermarks
read and written by incremental loads, and batches opened and closed by the
orchestration masters. This check reads the .dtsx XML and the load procedures
and asserts that the expected `etl.usp_*` calls are present in the text.

What is checked:

  1. every non-orchestration package references etl.usp_LogPackageStart,
     etl.usp_LogPackageEnd and etl.usp_LogError;
  2. every orchestration master references etl.usp_StartBatch,
     etl.usp_EndBatch, etl.usp_StartBatchStep and etl.usp_EndBatchStep;
  3. every incremental load type reads and writes a watermark;
  4. every quality screen evaluates rules and logs rejects;
  5. every `procs` entry declared for a package in config/estate-catalog.yaml
     appears in the package on disk;
  6. every etl procedure referenced by a package or a load procedure is
     actually created by a file under sqlserver/control/procedures;
  7. every dimension, fact and staging load procedure records a row count or
     an error through the control schema.

A missing call is an error when it breaks the audit trail the runbooks depend
on (1, 2, 6) and a warning when the estate can still be operated without it
(3, 4, 5, 7) - legacy loads that skip watermarking are a migration finding,
not a broken file.

Presence of a call is textual. This check cannot tell whether the call is on a
reachable path, whether its parameters are correct, or whether it would ever
execute.

Usage:
    python3 validation/checks/check_control_framework_integration.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import re
import sys

import estatelib as lib

CHECK = "control-framework"

PACKAGE_REQUIRED = ("LogPackageStart", "LogPackageEnd", "LogError")
MASTER_REQUIRED = ("StartBatch", "EndBatch", "StartBatchStep", "EndBatchStep")
WATERMARK_LOAD_TYPES = (
    "incremental_timestamp", "incremental_key", "incremental_append",
    "incremental_fact", "date_window",
)
QUALITY_LOAD_TYPES = ("quality_screen",)

LOAD_PROCEDURE_PREFIXES = (
    "sqlserver/staging/procedures/",
    "sqlserver/procedures/dimensions/",
    "sqlserver/procedures/facts/",
)

CREATE_PROC_RE = re.compile(
    r"CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE\s+([\[\]\w\.]+)", re.I)


def control_procedures_on_disk(sql_files):
    """Normalised names of the etl procedures created under sqlserver/control."""
    defined = set()
    for rel, _layer, text in sql_files:
        if not rel.startswith("sqlserver/control/"):
            continue
        for raw in CREATE_PROC_RE.findall(text):
            defined.add(lib.normalise_object(raw))
    return defined


def run(args):
    report = lib.Report("check_control_framework_integration")
    catalog = lib.load_catalog()
    packages = lib.load_packages(catalog)
    sql_files = lib.load_sql_files()
    defined = control_procedures_on_disk(sql_files)

    referenced = set()
    packages_missing_watermark = []
    procedures_without_instrumentation = []

    for package in sorted(packages.values(), key=lambda p: p.name):
        calls = package.etl_procedures()
        referenced.update("ETL.USP_" + call.upper() for call in calls)

        if package.load_type == "orchestration":
            for required in MASTER_REQUIRED:
                if required not in calls:
                    report.error(CHECK, package.name,
                                 "orchestration master does not reference etl.usp_%s" % required)
        else:
            for required in PACKAGE_REQUIRED:
                if required not in calls:
                    report.error(CHECK, package.name,
                                 "load package does not reference etl.usp_%s" % required)

        if package.load_type in WATERMARK_LOAD_TYPES:
            missing = [p for p in ("GetWatermark", "SetWatermark") if p not in calls]
            if missing:
                packages_missing_watermark.append(package.name)
                report.warn(CHECK, package.name,
                            "%s load does not reference %s; the reload window is decided "
                            "somewhere other than etl.Watermark"
                            % (package.load_type, " or ".join("etl.usp_" + m for m in missing)))

        if package.load_type in QUALITY_LOAD_TYPES:
            if "EvaluateDataQualityRules" not in calls and "LogRejectedRecord" not in calls:
                report.warn(CHECK, package.name,
                            "quality screen neither evaluates rules nor logs a rejected record")

        for declared in package.spec.get("procs") or []:
            if not declared.startswith("etl."):
                continue
            short = declared.split(".", 1)[1]
            if short not in calls:
                report.warn(CHECK, package.name,
                            "catalog declares '%s' but the package on disk does not "
                            "reference it" % declared)

    for rel, _layer, text in sql_files:
        if rel.startswith(LOAD_PROCEDURE_PREFIXES):
            calls = set(lib.ETL_PROC_RE.findall(text))
            referenced.update("ETL.USP_" + call.upper() for call in calls)
            if not calls:
                procedures_without_instrumentation.append(rel)
                report.warn(CHECK, rel,
                            "load procedure makes no call into the etl control schema")
        elif rel.startswith("sqlserver/"):
            referenced.update("ETL.USP_" + call.upper()
                              for call in lib.ETL_PROC_RE.findall(text))

    for name in sorted(referenced):
        if name not in defined:
            report.error(CHECK, name.lower(),
                         "referenced by the estate but no file under "
                         "sqlserver/control/procedures creates it")

    report.count("packages", len(packages))
    report.count("control_procedures_on_disk", len(defined))
    report.count("control_procedures_referenced", len(referenced))
    report.count("incremental_packages_without_watermark_calls",
                 len(packages_missing_watermark))
    report.count("load_procedures_without_control_calls",
                 len(procedures_without_instrumentation))
    report.detail("incremental_packages_without_watermark_calls", packages_missing_watermark)
    report.detail("load_procedures_without_control_calls",
                  sorted(procedures_without_instrumentation))
    report.detail("control_procedures_on_disk", sorted(defined))

    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
