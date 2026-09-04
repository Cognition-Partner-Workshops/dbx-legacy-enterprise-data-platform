#!/usr/bin/env python3
"""Prove the loader-contract check fails on the defects the SMALL load hit.

Each fixture reinjects one class of the live loader failure - an extract whose
value no longer fits its Oracle column, a target column the extract cannot
supply, two extracts claiming one landing table, a feed no longer declared in
the landing zone, and a data path that resolves to nowhere - into a scratch
copy of the estate, and asserts check_loader_contracts reports it.

The generator is copied with the estate, so a fixture can break the generator
as well as the schema. Nothing in the repository is modified.

Usage:
    python3 validation/checks/run_loader_fixtures.py [--verbose]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
CHECK = os.path.join(HERE, "check_loader_contracts.py")

COPIED_TREES = ("oracle", "sqlserver", "config", "generators")
IGNORED = shutil.ignore_patterns("output", "__pycache__", "*.pyc")


def sub_once(pattern, replacement, flags=0):
    def mutate(text):
        new, count = re.subn(pattern, replacement, text, count=1, flags=flags)
        return new if count else None
    return mutate


def drop_block(*needles):
    """Remove the first blank-line-separated block containing all needles."""
    def mutate(text):
        for block in text.split("\n\n"):
            if all(needle in block for needle in needles):
                return text.replace(block, "", 1)
        return None
    return mutate


# (label, file the defect goes into, expected finding, mutation)
FIXTURES = (
    ("extract value no longer fits its Oracle column",
     "oracle/tables/WWI_REF.UOM_REF.sql",
     "loader-oracle-contract",
     sub_once(r"UOM_NAME\s+VARCHAR2\(\d+\)", "UOM_NAME              NUMBER(9)")),

    ("Oracle column the extract maps onto disappearing",
     "oracle/tables/WWI_MDM.CUST_MASTER.sql",
     "loader-oracle-contract",
     sub_once(r"\n\s*CUST_NBR\s+VARCHAR2\([^\n]*\n", "\n")),

    ("landing table column the extract cannot supply",
     "sqlserver/staging/tables/11_raw_tables_sqlserver.sql",
     "loader-sql-contract",
     sub_once(r"(CREATE TABLE raw\.SqlOrder\s*\(\s*\n)",
              r"\1        ReconciledBy            NVARCHAR(30)    NOT NULL,\n")),

    ("two extracts claiming one landing table",
     "generators/wwigen/contracts/sql_map.py",
     "loader-sql-collision",
     sub_once(r'"sqlserver\.Sales\.Invoices":\s*"raw\.SqlInvoice"',
              '"sqlserver.Sales.Invoices": "raw.SqlOrder"')),

    ("feed generated under a name no package reads",
     "config/landing-zone.yaml",
     "loader-landing-feed",
     sub_once(r"carrier_scan\]", "carrier_manifest]")),

    ("loader data path resolving to nowhere",
     "generators/wwigen/loaders/bcp.py",
     "loader-path",
     sub_once(r'DATA_ROOT_RELATIVE = "\.\.\\\\\.\.\\\\data"',
              'DATA_ROOT_RELATIVE = "..\\\\..\\\\..\\\\data"')),
)


def prepare(scratch, relative, mutated):
    for tree in COPIED_TREES:
        shutil.copytree(os.path.join(REPO_ROOT, tree), os.path.join(scratch, tree),
                        ignore=IGNORED)
    target = os.path.join(scratch, relative.replace("/", os.sep))
    with open(target, "w", encoding="utf-8", newline="") as handle:
        handle.write(mutated)


def run_check(root):
    result = subprocess.run(
        [sys.executable, CHECK, "--json"],
        cwd=REPO_ROOT, env=dict(os.environ, WWI_ESTATE_ROOT=root),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if result.returncode not in (0, 1):
        raise RuntimeError("check_loader_contracts failed: %s" % result.stderr.strip())
    return json.loads(result.stdout)


def findings(report, level=None):
    return {item["check"] for item in report["findings"]
            if level is None or item["level"] == level}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    passed, failed = 0, []
    for label, relative, expected, mutation in FIXTURES:
        with open(os.path.join(REPO_ROOT, relative.replace("/", os.sep)),
                  encoding="utf-8", errors="replace") as handle:
            original = handle.read()
        mutated = mutation(original)
        if not mutated or mutated == original:
            failed.append("%s: the defect could not be injected into %s" % (label, relative))
            print("FAIL  %-52s not injectable" % label)
            continue

        scratch = tempfile.mkdtemp(prefix="wwi-loader-fixture-")
        try:
            prepare(scratch, relative, mutated)
            broken = run_check(scratch)
            if expected in findings(broken, "error"):
                passed += 1
                print("PASS  %-52s %s" % (label, expected))
                if args.verbose:
                    for item in broken["findings"]:
                        if item["check"] == expected:
                            print("        %s" % item["message"])
            else:
                failed.append("%s: %s did not fire (fired: %s)"
                              % (label, expected, ", ".join(sorted(findings(broken))) or "nothing"))
                print("FAIL  %-52s %s did not fire" % (label, expected))
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

    print("")
    print("%d/%d loader fixtures detected" % (passed, len(FIXTURES)))
    for message in failed:
        print("  %s" % message)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
