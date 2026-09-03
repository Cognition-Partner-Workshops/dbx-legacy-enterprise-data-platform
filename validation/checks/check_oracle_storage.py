#!/usr/bin/env python3
"""Every tablespace the Oracle DDL writes into has to exist and be quota'd.

The first deployment lost thirteen of the fifteen reference and seed scripts to
ORA-01950: WWI_REF held quota on WWI_REF_DATA and WWI_HIST_DATA while its
indexes are created in WWI_IDX, so the index creation failed and took the
script with it. This check pairs every `TABLESPACE <name>` the estate DDL names
with the owning schema's quota grants in oracle/ddl/03_create_schemas.sql and
with the tablespaces created in oracle/ddl/01_create_tablespaces.sql.

Static analysis only. Nothing here connects to Oracle.

    python3 validation/checks/check_oracle_storage.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import os
import re
import sys

import estatelib as lib
import oraclelib

SCHEMAS_PATH = "oracle/ddl/03_create_schemas.sql"
TABLESPACES_PATH = "oracle/ddl/01_create_tablespaces.sql"

CREATE_USER_RE = re.compile(r"CREATE\s+USER\s+([A-Z0-9_]+)(.*?)(?=CREATE\s+USER\s|\Z)", re.I | re.S)
QUOTA_RE = re.compile(r"QUOTA\s+(UNLIMITED|\d+[KMGT]?)\s+ON\s+([A-Z0-9_]+)", re.I)
DEFAULT_TABLESPACE_RE = re.compile(r"DEFAULT\s+TABLESPACE\s+([A-Z0-9_]+)", re.I)
CREATE_TABLESPACE_RE = re.compile(
    r"CREATE\s+(?:BIGFILE\s+|SMALLFILE\s+)?(?:TEMPORARY\s+|UNDO\s+)?TABLESPACE\s+([A-Z0-9_]+)", re.I)
TABLESPACE_USE_RE = re.compile(r"\bTABLESPACE\s+([A-Z0-9_]+)", re.I)
OWNER_RE = re.compile(
    r"CREATE\s+(?:TABLE|(?:UNIQUE\s+|BITMAP\s+)?INDEX|MATERIALIZED\s+VIEW)\s+([A-Z0-9_]+)\.", re.I)

# Provided by the database, never by this estate's DDL.
BUILTIN_TABLESPACES = frozenset({"TEMP", "SYSTEM", "SYSAUX", "USERS", "UNDOTBS1"})


def read(rel_path):
    path = os.path.join(oraclelib.REPO_ROOT, *rel_path.split("/"))
    if not os.path.exists(path):
        return ""
    with open(path, errors="replace") as handle:
        return oraclelib.strip_comments(handle.read())


def schema_quotas():
    """{schema: {tablespace: quota text}} plus each schema's default tablespace."""
    quotas, defaults = {}, {}
    for schema, body in CREATE_USER_RE.findall(read(SCHEMAS_PATH)):
        name = schema.upper()
        quotas[name] = {ts.upper(): size.upper() for size, ts in QUOTA_RE.findall(body)}
        default = DEFAULT_TABLESPACE_RE.search(body)
        if default:
            defaults[name] = default.group(1).upper()
    return quotas, defaults


def declared_tablespaces():
    return {name.upper() for name in CREATE_TABLESPACE_RE.findall(read(TABLESPACES_PATH))}


def storage_uses():
    """[(owner, tablespace, repo-relative path)] for every storage clause under oracle/."""
    uses = []
    for directory in ("tables", "reference", "seed", "views", "plsql"):
        root = os.path.join(oraclelib.REPO_ROOT, "oracle", directory)
        if not os.path.isdir(root):
            continue
        for dirpath, _dirnames, filenames in os.walk(root):
            for filename in sorted(filenames):
                if not filename.lower().endswith(".sql"):
                    continue
                full = os.path.join(dirpath, filename)
                rel = os.path.relpath(full, oraclelib.REPO_ROOT).replace(os.sep, "/")
                with open(full, errors="replace") as handle:
                    text = oraclelib.strip_comments(handle.read())
                owners = OWNER_RE.findall(text)
                if not owners:
                    continue
                owner = owners[0].upper()
                for tablespace in TABLESPACE_USE_RE.findall(text):
                    uses.append((owner, tablespace.upper(), rel))
    return uses


def run(args):
    report = lib.Report("check_oracle_storage")
    quotas, defaults = schema_quotas()
    created = declared_tablespaces()
    uses = storage_uses()

    for owner, tablespace, path in uses:
        if tablespace not in created and tablespace not in BUILTIN_TABLESPACES:
            report.error(
                "oracle-tablespace-missing", path,
                "%s stores objects in tablespace %s, which %s never creates"
                % (owner, tablespace, TABLESPACES_PATH))
            continue
        if owner not in quotas:
            continue                        # not one of the estate's own schemas
        granted = quotas[owner]
        if tablespace in granted and granted[tablespace] != "0":
            continue
        report.error(
            "oracle-tablespace-quota", path,
            "%s writes into %s but %s grants it %s (ORA-01950 at deploy time)"
            % (owner, tablespace, SCHEMAS_PATH,
               "quota 0" if tablespace in granted else "no quota there"))

    # A default tablespace with no quota is the same failure with no storage
    # clause - but only for a schema that owns segments. WWI_EXTRACT holds
    # QUOTA 0 everywhere on purpose: it reads, it never creates.
    owning_schemas = {table.schema for table in oraclelib.load_oracle_tables().values()}
    for schema, tablespace in sorted(defaults.items()):
        if schema not in owning_schemas:
            continue
        granted = quotas.get(schema, {})
        if granted.get(tablespace, "0") == "0":
            report.error(
                "oracle-tablespace-quota", SCHEMAS_PATH,
                "%s defaults to tablespace %s with no quota on it "
                "(ORA-01950 on its first unqualified segment)" % (schema, tablespace))

    report.count("oracle_schemas_parsed", len(quotas))
    report.count("oracle_tablespaces_created", len(created))
    report.count("oracle_storage_clauses_checked", len(uses))
    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
