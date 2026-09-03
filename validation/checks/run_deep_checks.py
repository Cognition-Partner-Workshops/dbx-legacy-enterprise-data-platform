#!/usr/bin/env python3
"""Run every deep static check in validation/checks and summarise the result.

This is the companion to validation/static/run_all_checks.py. The static
checker asks "is the estate internally well formed?"; these checks ask "does
the estate hang together as a data platform?" - does every target have a
source, does every object have a writer and a reader, does the package graph
terminate, and is every load wired into the etl control schema.

Nothing here connects to Oracle, SQL Server, SSIS or a file share.

Usage:
    python3 validation/checks/run_deep_checks.py [--json] [--strict] [--quiet]
"""

from __future__ import annotations

import argparse
import sys

import check_control_framework_integration
import check_orphan_objects
import check_source_to_target_coverage
import extract_package_dependency_graph

CHECKS = (
    ("source-to-target coverage", check_source_to_target_coverage),
    ("staging and warehouse orphans", check_orphan_objects),
    ("package dependency graph", extract_package_dependency_graph),
    ("control-framework integration", check_control_framework_integration),
)


class Options:
    def __init__(self, args, module):
        self.json = args.json
        self.strict = args.strict
        self.quiet = args.quiet
        if module is extract_package_dependency_graph:
            self.dot = False
            self.mermaid = False
            self.graph_json = False


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    exit_codes = {}
    for label, module in CHECKS:
        if not args.json:
            print("=" * 78)
            print(label)
            print("=" * 78)
        exit_codes[label] = module.run(Options(args, module))
        if not args.json:
            print()

    failed = [label for label, code in exit_codes.items() if code]
    if not args.json:
        print("=" * 78)
        for label, code in exit_codes.items():
            print("%-40s %s" % (label, "FAIL" if code else "OK"))
        print("=" * 78)
        print("These are static checks over checked-in files. They say nothing about "
              "whether any of it runs.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
