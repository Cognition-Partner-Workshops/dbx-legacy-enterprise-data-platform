#!/usr/bin/env python3
"""Cross-schema privileges the Oracle views need to compile.

A view compiles against a table in another schema only if the view's owner
holds a direct SELECT on that table. A grant through a role does not count -
Oracle ignores role privileges when compiling stored objects, which is how the
first deployment left WWI_MDM.V_CUSTOMER_ADDRESS_CURRENT and the WWI_PROC AP
views ORA-00942 while the extract role could read the same tables happily.

Static analysis only. Nothing here connects to Oracle.

    python3 validation/checks/check_oracle_grants.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import os
import re
import sys

import estatelib as lib
import oraclelib

GRANT_RE = re.compile(
    r"GRANT\s+([A-Z, ]+?)\s+ON\s+([A-Z0-9_]+)\.([A-Z0-9_]+)\s+TO\s+([A-Z0-9_]+)", re.I)

# Roles are not usable when Oracle compiles a view; only direct grants are.
ROLE_SUFFIX = "_ROLE"

GRANT_DIRECTORIES = ("ddl", "tables", "views", "reference")


def load_direct_grants():
    """{(grantee, "SCHEMA.OBJECT")} for every direct SELECT grant on disk."""
    granted = set()
    for directory in GRANT_DIRECTORIES:
        root = os.path.join(lib.REPO_ROOT, "oracle", directory)
        if not os.path.isdir(root):
            continue
        for dirpath, _dirnames, filenames in os.walk(root):
            for filename in sorted(filenames):
                if not filename.lower().endswith(".sql"):
                    continue
                text = lib.read_text(os.path.join(dirpath, filename))
                for privileges, schema, name, grantee in GRANT_RE.findall(
                        oraclelib.strip_comments(text)):
                    if "SELECT" not in privileges.upper():
                        continue
                    if grantee.upper().endswith(ROLE_SUFFIX):
                        continue
                    granted.add((grantee.upper(),
                                 "%s.%s" % (schema.upper(), name.upper())))
    return granted


def run(args):
    report = lib.Report("check_oracle_grants")
    granted = load_direct_grants()
    views = oraclelib.load_oracle_views()
    view_keys = {view.key for view in views}
    tables = oraclelib.load_oracle_tables()

    checked = 0
    for view in sorted(views, key=lambda item: item.key):
        for alias, target in sorted(view.aliases.items()):
            if alias in view.derived:
                continue
            schema = target.split(".")[0]
            if schema == view.schema:
                continue
            if target not in tables and target not in view_keys:
                continue
            checked += 1
            if (view.schema, target) not in granted:
                report.error(
                    "oracle-cross-schema-grant", view.path,
                    "%s reads %s but %s holds no direct SELECT on it; a role grant "
                    "does not compile a view (ORA-00942 at deploy time)"
                    % (view.key, target, view.schema))

    report.count("oracle_views_parsed", len(views))
    report.count("oracle_direct_grants_parsed", len(granted))
    report.count("cross_schema_references_checked", checked)
    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
