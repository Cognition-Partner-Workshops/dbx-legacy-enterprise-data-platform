#!/usr/bin/env python3
"""Prove the loader checks fail on the defects the SMALL loads hit.

Each fixture reinjects one class of the live loader failure - an extract whose
value no longer fits its Oracle column, a target column the extract cannot
supply, two extracts claiming one landing table, a feed no longer declared in
the landing zone, a data path that resolves to nowhere, a bcp field size the
target type cannot take, and generated values that stop being held to the
deployed CHECK, precision, key and partition contracts - into a scratch copy
of the estate, and asserts the owning check reports it.

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
CONTRACTS = os.path.join(HERE, "check_loader_contracts.py")
VALUES = os.path.join(HERE, "check_generated_values.py")

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


def replace_body(signature, body):
    """Replace the body of one method with ``body``, keeping its signature."""
    def mutate(text):
        start = text.find(signature)
        if start < 0:
            return None
        opening = text.find("\n", start) + 1
        end = text.find("\n    def ", opening)
        if end < 0:
            return None
        return text[:opening] + body + text[end:]
    return mutate


# (label, file the defect goes into, expected findings, mutation[, check])
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

    ("driver argument interpolating a property",
     "generators/wwigen/loaders/sqlloader.py",
     "loader-driver",
     sub_once(r"& sqlldr @arguments", "& sqlldr $connect control=$load.Control")),

    ("loader data path resolving to nowhere",
     "generators/wwigen/loaders/bcp.py",
     "loader-path",
     sub_once(r'DATA_ROOT_RELATIVE = "\.\.\\\\\.\.\\\\data"',
              'DATA_ROOT_RELATIVE = "..\\\\..\\\\..\\\\data"')),

    ("bcp field size given to a (max) column",
     "generators/wwigen/loaders/bcp.py",
     "loader-format-size",
     sub_once(r"^UNBOUNDED = 0$", "UNBOUNDED = 16000", re.M)),

    ("bcp field too narrow for the target type",
     "generators/wwigen/loaders/bcp.py",
     "loader-format-size",
     sub_once(r'"bigint": 20,', '"bigint": 4,')),

    ("generated rows no longer held to the table's constraints",
     "generators/wwigen/valuecontract.py",
     ("generated-oracle-check", "generated-oracle-foreign-key"),
     replace_body("    def apply(self, values, index):",
                  "        return self._fit(values)\n"),
     VALUES),

    ("generated numbers no longer fitted to NUMBER(p,s)",
     "generators/wwigen/valuecontract.py",
     "generated-oracle-precision",
     replace_body("    def _fit(self, values):", "        return values\n"),
     VALUES),

    ("generated keys no longer kept clear of the seeded rows",
     "generators/wwigen/valuecontract.py",
     "generated-oracle-seed-key",
     sub_once(r"self\.seeded = \{key: self\.seed\.keys_of\(table\.key, key\)\s*\n"
              r"\s*for key in self\.unique_keys\}",
              "self.seeded = {key: frozenset() for key in self.unique_keys}"),
     VALUES),

    ("posted journals no longer constructed in balance",
     "generators/wwigen/contracts/oracle_map.py",
     "generated-oracle-construction",
     sub_once(r'"TOTAL_CREDIT_AMT": copy_of\("CONTROL_TOTAL_AMT", 0\)',
              '"TOTAL_CREDIT_AMT": copy_of("NO_SUCH_TOTAL", 0)'),
     VALUES),

    # The row the live load rejected 618 times: POSTING_STATUS_CD = 'POST'
    # with totals that do not agree. It takes an extract that stops building
    # the totals together and a contract that stops steering the row onto a
    # branch of CK_GL_JHDR_BALANCED - what must not happen is either one
    # passing the row off as loadable.
    ("cross-column CHECK left unsatisfied on the completed row",
     ("generators/wwigen/contracts/oracle_map.py",
      "generators/wwigen/valuecontract.py"),
     "generated-oracle-check",
     (sub_once(r'"TOTAL_CREDIT_AMT": copy_of\("CONTROL_TOTAL_AMT", 0\)',
               '"TOTAL_CREDIT_AMT": copy_of("NO_SUCH_TOTAL", 0)'),
      replace_body("    def _repair_checks(self, values, index):",
                   "        return values\n")),
     VALUES),

    # TAX_RATE and PARTY_XREF were rejected with ORA-00001 on a tuple whose
    # last column the extract never writes: the index holds the DEFAULT, so
    # a key space that only counts written columns sees no duplicate.
    ("composite key space ignoring a column left to its DEFAULT",
     "generators/wwigen/valuecontract.py",
     "generated-oracle-key",
     sub_once(r"else self\.constant\.get\(name\) for name in key\)",
              "else None for name in key)"),
     VALUES),

    # Every deployed partitioned table ends in MAXVALUE or an interval, so a
    # row outside the declared ranges takes both a table that closes and a
    # generator that stops fitting the key to it.
    ("generated partition key outside the declared ranges",
     ("oracle/tables/WWI_FIN.AP_INVOICE_HDR.sql",
      "generators/wwigen/valuecontract.py"),
     "generated-oracle-partition",
     (sub_once(r"PARTITION AP_HDR_2019.*?\n\)",
               "PARTITION AP_HDR_2019 VALUES LESS THAN "
               "(TO_DATE('2020-01-01', 'YYYY-MM-DD')) TABLESPACE WWI_HIST_DATA\n)",
               re.S),
      replace_body("    def _fit_partition(self, values):", "        return values\n")),
     VALUES),
)


def prepare(scratch, written):
    for tree in COPIED_TREES:
        shutil.copytree(os.path.join(REPO_ROOT, tree), os.path.join(scratch, tree),
                        ignore=IGNORED)
    for relative, text in written:
        target = os.path.join(scratch, relative.replace("/", os.sep))
        with open(target, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)


def inject(relative, mutation):
    """One file's mutated text, or ``None`` if the defect does not apply."""
    with open(os.path.join(REPO_ROOT, relative.replace("/", os.sep)),
              encoding="utf-8", errors="replace") as handle:
        original = handle.read()
    mutated = mutation(original)
    return None if not mutated or mutated == original else mutated


def run_check(root, check):
    result = subprocess.run(
        [sys.executable, check, "--json"],
        cwd=REPO_ROOT, env=dict(os.environ, WWI_ESTATE_ROOT=root),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if result.returncode not in (0, 1):
        raise RuntimeError("%s failed: %s"
                           % (os.path.basename(check), result.stderr.strip()))
    return json.loads(result.stdout)


def findings(report, level=None):
    return {item["check"] for item in report["findings"]
            if level is None or item["level"] == level}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    passed, failed = 0, []
    for fixture in FIXTURES:
        label, where, expected, mutation = fixture[:4]
        check = fixture[4] if len(fixture) > 4 else CONTRACTS
        wanted = (expected,) if isinstance(expected, str) else expected
        files = (where,) if isinstance(where, str) else where
        mutations = (mutation,) if callable(mutation) else mutation
        written = [(relative, inject(relative, one))
                   for relative, one in zip(files, mutations)]
        stuck = [relative for relative, text in written if text is None]
        if stuck:
            failed.append("%s: the defect could not be injected into %s"
                          % (label, ", ".join(stuck)))
            print("FAIL  %-52s not injectable" % label)
            continue

        scratch = tempfile.mkdtemp(prefix="wwi-loader-fixture-")
        try:
            prepare(scratch, written)
            broken = run_check(scratch, check)
            fired = findings(broken, "error")
            missing = [code for code in wanted if code not in fired]
            if not missing:
                passed += 1
                print("PASS  %-52s %s" % (label, ", ".join(wanted)))
                if args.verbose:
                    for item in broken["findings"]:
                        if item["check"] in wanted:
                            print("        %s" % item["message"])
            else:
                failed.append("%s: %s did not fire (fired: %s)"
                              % (label, ", ".join(missing),
                                 ", ".join(sorted(findings(broken))) or "nothing"))
                print("FAIL  %-52s %s did not fire" % (label, ", ".join(missing)))
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

    print("")
    print("%d/%d loader fixtures detected" % (passed, len(FIXTURES)))
    for message in failed:
        print("  %s" % message)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
