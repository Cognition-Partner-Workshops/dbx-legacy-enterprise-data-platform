#!/usr/bin/env python3
"""Prove the estate checks fail on the defects the first deployment hit.

Every fixture below reproduces one class of live engine failure by injecting it
back into a scratch copy of the estate, and asserts that the check written for
that class reports it. A check that cannot fail is not a regression test, and
each of these defects was in the repository once.

Nothing in the repository is modified: the estate is copied to a temporary
tree and the checks are pointed at it through WWI_ESTATE_ROOT.

Usage:
    python3 validation/checks/run_check_fixtures.py [--verbose]
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

# The estate the checks read. Copied per fixture; nothing else is needed.
COPIED_TREES = ("oracle", "sqlserver", "config")


def sub_once(pattern, replacement, flags=0):
    def mutate(text):
        new, count = re.subn(pattern, replacement, text, count=1, flags=flags)
        return new if count else None
    return mutate


def drop_line(*needles):
    def mutate(text):
        for line in text.splitlines(True):
            if all(needle in line for needle in needles):
                return text.replace(line, "", 1)
        return None
    return mutate


# (label, check script, file the defect goes into, expected finding, mutation)
FIXTURES = (
    ("view names a column the table lacks",
     "check_oracle_view_columns.py",
     "oracle/views/WWI_REF.V_CURRENCY_EXTRACT.sql",
     "oracle-view-column",
     sub_once(r"FROM WWI_REF\.FX_RATE_DAILY f",
              "FROM WWI_REF.FX_RATE_DAILY f\n         WHERE f.NO_SUCH_COLUMN IS NOT NULL")),

    ("cross-schema read with no direct grant",
     "check_oracle_grants.py",
     "oracle/ddl/05_grant_privileges.sql",
     "oracle-cross-schema-grant",
     drop_line("GRANT SELECT ON WWI_REF.CITY_REF", "TO WWI_MDM")),

    ("schema with no quota on the tablespace it indexes into",
     "check_oracle_storage.py",
     "oracle/ddl/03_create_schemas.sql",
     "oracle-tablespace-quota",
     drop_line("QUOTA UNLIMITED ON WWI_IDX")),

    ("seed value wider than its column",
     "check_oracle_seed_values.py",
     "oracle/tables/WWI_FIN.TAX_RATE.sql",
     "oracle-seed-width",
     sub_once(r"JURISDICTION_CD\s+VARCHAR2\(\d+\)", "JURISDICTION_CD  VARCHAR2(3)")),

    ("seed row leaving a NOT NULL column null",
     "check_oracle_seed_values.py",
     "oracle/reference/10_chart_of_accounts_and_cost_centers.sql",
     "oracle-seed-not-null",
     sub_once(r"VALUES \(6001, '0000-1000-000-000',\n            '0000',",
              "VALUES (6001, '0000-1000-000-000',\n            NULL,")),

    ("partition split on a literal instead of a DATE",
     "check_oracle_partitions.py",
     "oracle/tables/ZZ_add_future_partitions.sql",
     "oracle-partition-boundary",
     sub_once(r"TO_DATE\('2026-01-01', 'YYYY-MM-DD'\)", "'2026-01-01'")),

    ("partition split naming a partition the table lacks",
     "check_oracle_partitions.py",
     "oracle/tables/ZZ_add_future_partitions.sql",
     "oracle-partition-name",
     sub_once(r"GL_JLINE_PMAX", "GLL_PMAX")),

    ("child foreign key column of a different type",
     "check_sqlserver_columns.py",
     "sqlserver/oltp/02_extensions/2010_Sales.OrderLines.Extensions.sql",
     "sqlserver-fk-datatype",
     sub_once(r"\[PriceListLineID\]\s+BIGINT", "[PriceListLineID]     INT")),

    ("insert writes a computed column",
     "check_sqlserver_columns.py",
     "sqlserver/oltp/06_procedures/6140_Returns.usp_PostReturnInspection.sql",
     "sqlserver-computed-write",
     sub_once(r"\[ReturnLineID\], \[InspectionSequence\],",
              "[ReturnLineID], [QuantityFailed], [InspectionSequence],")),

    ("scalar function called with too few arguments",
     "check_sqlserver_columns.py",
     "sqlserver/oltp/06_procedures/6120_Shipping.usp_RateShipment.sql",
     "sqlserver-function-arguments",
     sub_once(r"\[Shipping\]\.\[ufn_FreightCost\]\(@CarrierID,", "[Shipping].[ufn_FreightCost](")),

    ("warehouse index on the invented date key",
     "check_sqlserver_columns.py",
     "sqlserver/warehouse/dimensions/Dimension.Date.sql",
     "sqlserver-index-column",
     sub_once(r"ON \[Dimension\]\.\[Date\] \(\[Calendar Year\] ASC",
              "ON [Dimension].[Date] ([DateKey] ASC")),

    ("reserved members run before the dimension they extend",
     "check_deployment_order.py",
     "sqlserver/warehouse/dimensions/90_unknown_members.sql",
     "sqlserver-deploy-order",
     sub_once(r"and every Dimension\.\*\.sql table\n\s*\*?\s*script", "and the dimensions")),
)


def prepare(scratch, relative, mutated):
    for tree in COPIED_TREES:
        shutil.copytree(os.path.join(REPO_ROOT, tree), os.path.join(scratch, tree))
    target = os.path.join(scratch, relative.replace("/", os.sep))
    with open(target, "w", encoding="utf-8", newline="") as handle:
        handle.write(mutated)


def run_check(script, root):
    result = subprocess.run(
        [sys.executable, os.path.join(HERE, script), "--json"],
        cwd=REPO_ROOT, env=dict(os.environ, WWI_ESTATE_ROOT=root),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if result.returncode not in (0, 1):
        raise RuntimeError("%s failed: %s" % (script, result.stderr.strip()))
    return json.loads(result.stdout)


def findings(report, level=None):
    return {item["check"] for item in report["findings"]
            if level is None or item["level"] == level}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    passed, failed = 0, []
    for label, script, relative, expected, mutation in FIXTURES:
        with open(os.path.join(REPO_ROOT, relative.replace("/", os.sep)),
                  encoding="utf-8", errors="replace") as handle:
            original = handle.read()
        mutated = mutation(original)
        if not mutated or mutated == original:
            failed.append("%s: the defect could not be injected into %s" % (label, relative))
            print("FAIL  %-52s not injectable" % label)
            continue

        scratch = tempfile.mkdtemp(prefix="wwi-fixture-")
        try:
            prepare(scratch, relative, mutated)
            broken = run_check(script, scratch)
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
    print("%d/%d fixtures detected" % (passed, len(FIXTURES)))
    for message in failed:
        print("  %s" % message)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
