#!/usr/bin/env python3
"""Every generated Oracle row has to be one the deployed table would accept.

The second SMALL load reached SQL*Loader and was rejected row by row: 41 of
the 46 extracts wrote values outside a deployed CHECK domain, past a
NUMBER(p,s), onto a key the deployment had already seeded, or at a parent row
that does not exist. The columns were right - PR #8 fixed those - and the
values were not, so the first thing that knew was Oracle.

This check produces every Oracle extract in full and puts each row back
through the deployed contract before any loader runs:

* NOT NULL columns hold a value;
* numbers fit their NUMBER(p,s) and dates and strings fit their column;
* every CHECK constraint over the row evaluates true;
* primary and unique keys are unique within the run and clear of the keys the
  reference and seed scripts already create;
* every foreign key names a row that will exist - one this run generates for
  the parent, or one the deployment seeds;
* a partitioned table's key falls inside a declared partition.

Static analysis only: the extracts are produced in memory. Nothing here
connects to Oracle, and no data file is written.

    python3 validation/checks/check_generated_values.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import os
import sys

import estatelib as lib

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
ESTATE_ROOT = os.environ.get("WWI_ESTATE_ROOT") or REPO_ROOT

GENERATORS = os.path.join(ESTATE_ROOT, "generators")
if not os.path.isdir(GENERATORS):
    GENERATORS = os.path.join(REPO_ROOT, "generators")
sys.path.insert(0, GENERATORS)

from wwigen import canon, config, context, oracheck, schema, tables, valuecontract  # noqa: E402
from wwigen.conform import ContractError  # noqa: E402

# One finding code per class of rejection the live load produced, so a
# regression is reported as the defect it is rather than as "a bad row".
CODES = {
    "nullability": "generated-oracle-nullability",
    "precision": "generated-oracle-precision",
    "scale": "generated-oracle-precision",
    "width": "generated-oracle-width",
    "check": "generated-oracle-check",
    "foreign-key": "generated-oracle-foreign-key",
    "partition": "generated-oracle-partition",
}

# An extract that cannot produce a loadable row says why in Oracle's own
# terms, so a refusal is reported as the same class as a row that slipped
# through would have been.
ORA_CODES = (
    ("ORA-01400", "nullability"),
    ("ORA-01438", "precision"),
    ("ORA-12899", "width"),
    ("ORA-02290", "check"),
    ("ORA-02291", "foreign-key"),
    ("ORA-14400", "partition"),
)

# Rows reported per extract before the rest are counted silently: a broken
# domain breaks every row, and the first few say everything the fix needs.
REPORTED_ROWS = 3

# A row a table-level CHECK constrains can be made loadable two ways: by
# constructing it so the constraint holds, or by steering it onto the branch
# where the constraint says nothing. The second passes every rule below while
# emitting an estate nobody would recognise - a general ledger whose journals
# are never posted - so the extracts whose live rejections were of that class
# state what they are expected to construct.
#
#   extract -> (label, row is in scope, row is constructed correctly)
CONSTRUCTED = {
    "WWI_FIN.GL_JOURNAL_HDR": (
        "posted journals whose debits equal their credits",
        lambda row: row.get("POSTING_STATUS_CD") == "POST",
        lambda row: row.get("TOTAL_DEBIT_AMT") == row.get("TOTAL_CREDIT_AMT")),
}


def oracle_specs():
    return [spec for spec in tables.all_specs() if spec.system == schema.ORACLE]


def origin_of(spec):
    return "oracle/tables/%s.%s.sql" % (spec.schema, spec.name)


class Counters:
    """What the run proved, by class, for the summary."""

    def __init__(self):
        self.rows = 0
        self.extracts = 0
        self.keys = 0
        self.foreign_keys = 0
        self.partitioned = 0
        self.constructed = 0
        self.failed = set()


def check_extract(report, spec, cfg, ctx, counters):
    """Produce one extract in full and put every row back through the table."""
    key = "%s.%s" % (spec.schema, spec.name)
    table = canon.oracle_tables().get(key)
    origin = origin_of(spec)
    if table is None:
        report.error("generated-oracle-table", origin,
                     "%s has no canonical table" % spec.key)
        counters.failed.add(spec.key)
        return

    names = [column.name for column in spec.columns]
    written = [table.column(name) for name in names]
    missing = [name for name, column in zip(names, written) if column is None]
    if missing:
        report.error("generated-oracle-table", origin,
                     "%s writes %s, which %s does not have"
                     % (spec.key, ", ".join(missing), key))
        counters.failed.add(spec.key)
        return
    contract = valuecontract.ValueContract(table, spec.key, written, cfg, ctx)
    # Read from the seed scripts here rather than from the contract: the point
    # is to catch a generator that has stopped avoiding the deployed keys.
    seed = canon.oracle_seed()
    seeded = {unique: seed.keys_of(table.key, unique)
              for unique in contract.unique_keys}
    seen = {unique: set() for unique in contract.unique_keys}
    reported = 0

    try:
        rows = list(spec.produce(cfg, ctx))
    except (ContractError, ValueError) as error:
        for code in refusal_codes(str(error)):
            report.error(code, origin, str(error))
        counters.failed.add(spec.key)
        return

    for row in rows:
        values = dict(zip(names, row))
        for violation in contract.violations(values):
            counters.failed.add(spec.key)
            reported += 1
            if reported <= REPORTED_ROWS:
                report.error(CODES.get(violation.kind, "generated-oracle-value"),
                             origin, "%s: %s" % (spec.key, violation))
        for unique in contract.unique_keys:
            candidate = indexed_tuple(table, unique, values)
            if candidate is None:
                continue
            counters.keys += 1
            where = "%s (%s)" % (spec.key, ", ".join(unique))
            if candidate in seeded[unique]:
                counters.failed.add(spec.key)
                report.error("generated-oracle-seed-key", origin,
                             "%s repeats a key the deployment seeds: %s"
                             % (where, ", ".join(str(part) for part in candidate)))
            elif candidate in seen[unique]:
                counters.failed.add(spec.key)
                report.error("generated-oracle-key", origin,
                             "%s emits the same key twice: %s"
                             % (where, ", ".join(str(part) for part in candidate)))
            seen[unique].add(candidate)

    check_construction(report, spec, origin, names, rows, counters)
    counters.rows += len(rows)
    counters.extracts += 1
    counters.foreign_keys += sum(1 for name in table.foreign_keys
                                 if name in contract.writable)
    if table.partition is not None:
        counters.partitioned += 1


def indexed_tuple(table, unique, values):
    """The tuple Oracle will index for ``unique``, or None if it holds a null.

    Derived from the canonical table here rather than from the contract under
    test: a column the extract does not write takes its DEFAULT, and both
    TAX_RATE and PARTY_XREF were rejected on exactly the part of their unique
    key that the extract had left to one.
    """
    parts = []
    for name in unique:
        if name in values:
            parts.append(values[name])
            continue
        column = table.column(name)
        default = column.default_value if column is not None else None
        parts.append(None if isinstance(default, oracheck.Unknowable) else default)
    return None if any(part is None for part in parts) else tuple(parts)


def check_construction(report, spec, origin, names, rows, counters):
    """Prove the extract constructs the rows its table-level CHECKs expect."""
    expectation = CONSTRUCTED.get("%s.%s" % (spec.schema, spec.name))
    if expectation is None:
        return
    label, in_scope, constructed = expectation
    scoped = [row for row in (dict(zip(names, row)) for row in rows)
              if in_scope(row)]
    counters.constructed += len(scoped)
    if not scoped:
        counters.failed.add(spec.key)
        report.error("generated-oracle-construction", origin,
                     "%s emits no %s" % (spec.key, label))
        return
    wrong = [row for row in scoped if not constructed(row)]
    if wrong:
        counters.failed.add(spec.key)
        report.error("generated-oracle-construction", origin,
                     "%s emits %d of %d rows that are not %s"
                     % (spec.key, len(wrong), len(scoped), label))


def refusal_codes(message):
    """The finding codes an extract's refusal to produce a row belongs under."""
    found = [CODES[kind] for ora, kind in ORA_CODES if ora in message]
    return sorted(set(found)) or ["generated-oracle-value"]


def run(args):
    report = lib.Report("check_generated_values")
    canon.reset_cache()
    cfg = config.build_run_config("small")
    ctx = context.RunContext(cfg)
    counters = Counters()

    specs = oracle_specs()
    for spec in specs:
        check_extract(report, spec, cfg, ctx, counters)

    report.count("oracle_extracts", len(specs))
    report.count("oracle_value_contracts_passed", len(specs) - len(counters.failed))
    report.count("oracle_rows_validated", counters.rows)
    report.count("oracle_keys_validated", counters.keys)
    report.count("oracle_foreign_keys_resolved", counters.foreign_keys)
    report.count("oracle_partitioned_tables", counters.partitioned)
    report.count("oracle_constructed_rows_validated", counters.constructed)
    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
