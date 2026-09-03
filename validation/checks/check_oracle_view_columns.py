#!/usr/bin/env python3
"""Every column an Oracle view names on a base table has to exist on it.

This is the offline form of the ORA-00904 wall the first deployment hit: all
21 estate views failed, every one of them because the view and the table it
reads had drifted apart on a column name. It also reports the ORA-00942 half
of that wall - a view that reads another schema's table without a matching
`GRANT SELECT` in `oracle/ddl/05_grant_privileges.sql`.

Static analysis only. Nothing here connects to Oracle.

    python3 validation/checks/check_oracle_view_columns.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import os
import re
import sys

import estatelib as lib
import oraclelib

GRANT_RE = re.compile(
    r"GRANT\s+([A-Z,\s]+?)\s+ON\s+([A-Z0-9_]+)\.([A-Z0-9_]+)\s+TO\s+([A-Z0-9_,\s]+)", re.I)

GRANTS_PATH = "oracle/ddl/05_grant_privileges.sql"


def load_select_grants():
    """{"SCHEMA.OBJECT": {grantee, ...}} from the object-grant DDL."""
    path = os.path.join(oraclelib.REPO_ROOT, *GRANTS_PATH.split("/"))
    grants = {}
    if not os.path.exists(path):
        return grants
    with open(path, errors="replace") as handle:
        text = oraclelib.strip_comments(handle.read())
    for privileges, schema, obj, grantees in GRANT_RE.findall(text):
        if "SELECT" not in privileges.upper():
            continue
        key = "%s.%s" % (schema.upper(), obj.upper())
        for grantee in grantees.replace("\n", " ").split(","):
            grants.setdefault(key, set()).add(grantee.strip().upper())
    return grants


def role_members():
    """{role: {member, ...}} from the role DDL, so grants through a role count."""
    path = os.path.join(oraclelib.REPO_ROOT, "oracle", "ddl", "04_create_roles.sql")
    members = {}
    if not os.path.exists(path):
        return members
    with open(path, errors="replace") as handle:
        text = oraclelib.strip_comments(handle.read())
    for match in re.finditer(r"GRANT\s+([A-Z0-9_]+)\s+TO\s+([A-Z0-9_,\s]+)", text, re.I):
        role = match.group(1).upper()
        for grantee in match.group(2).replace("\n", " ").split(","):
            members.setdefault(role, set()).add(grantee.strip().upper())
    return members


def run(args):
    report = lib.Report("check_oracle_view_columns")
    tables = oraclelib.load_oracle_tables()
    views = oraclelib.load_oracle_views()
    grants = load_select_grants()
    roles = role_members()
    view_names = {view.key for view in views}

    unresolved = []
    checked_columns = 0

    for view in views:
        for alias, column in view.references:
            if alias in view.derived or alias not in view.aliases:
                continue
            target = view.aliases[alias]
            if target in view_names:
                continue                        # view on view: columns are aliased
            table = tables.get(target)
            if table is None:
                unresolved.append("%s -> %s" % (view.key, target))
                continue
            checked_columns += 1
            if column not in table.columns:
                report.error(
                    "oracle-view-column", view.path,
                    "%s reads %s.%s (alias %s); %s has no such column "
                    "(ORA-00904 at deploy time)" % (
                        view.key, target, column, alias, target))

        # Cross-schema reads need an object grant to the view owner.
        for target in sorted(set(view.aliases.values())):
            schema = target.split(".")[0]
            if schema == view.schema or target not in tables:
                continue
            grantees = grants.get(target, set())
            allowed = view.schema in grantees or any(
                view.schema in roles.get(role, set()) for role in grantees)
            if not allowed:
                report.error(
                    "oracle-view-grant", view.path,
                    "%s reads %s across schemas with no GRANT SELECT to %s in %s "
                    "(ORA-00942 at deploy time)" % (
                        view.key, target, view.schema, GRANTS_PATH))

    report.count("oracle_tables_parsed", len(tables))
    report.count("oracle_views_parsed", len(views))
    report.count("qualified_column_references_checked", checked_columns)
    report.count("unresolved_view_sources", len(unresolved))
    if unresolved:
        report.detail("unresolved_view_sources", sorted(set(unresolved)))

    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
