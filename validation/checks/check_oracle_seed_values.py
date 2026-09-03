#!/usr/bin/env python3
"""Seed rows have to fit the table they are inserted into.

Two of the estate's first-deployment stoppers were seed content, not DDL:
ORA-12899 where a reference literal was wider than the column
(WWI_FIN.TAX_RATE.JURISDICTION_CD, TAX_CODE_CD) and ORA-01400 where a NOT NULL
column was absent from the insert column list (WWI_FIN.GL_ACCOUNT.SEGMENT_1_CD).
This check reproduces both offline, over every row under oracle/reference and
oracle/seed.

Static analysis only. Nothing here connects to Oracle.

    python3 validation/checks/check_oracle_seed_values.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import sys

import estatelib as lib
import oraclelib


def run(args):
    report = lib.Report("check_oracle_seed_values")
    tables = oraclelib.load_oracle_tables()
    rows = oraclelib.load_oracle_seed_rows()

    unknown_tables = set()
    checked_values = 0

    for row in rows:
        table = tables.get(row.key)
        if table is None:
            unknown_tables.add(row.key)
            continue

        for column, value in row.values.items():
            if column not in table.columns:
                report.error(
                    "oracle-seed-column", row.path,
                    "line %d: %s has no column %s (ORA-00904 at deploy time)" % (
                        row.line, row.key, column))
                continue
            checked_values += 1

            if oraclelib.is_null_literal(value) and column in table.not_null:
                report.error(
                    "oracle-seed-not-null", row.path,
                    "line %d: %s.%s is NOT NULL but the seed row inserts NULL "
                    "(ORA-01400 at deploy time)" % (row.line, row.key, column))
                continue

            literal = oraclelib.string_literal(value)
            width = table.width(column)
            if literal is not None and width is not None and len(literal) > width:
                report.error(
                    "oracle-seed-width", row.path,
                    "line %d: %s.%s is %s(%d) but the seed value is %d characters "
                    "(%r) (ORA-12899 at deploy time)" % (
                        row.line, row.key, column,
                        table.types[column][0], width, len(literal), literal))

        # A NOT NULL column with no default has to appear in the column list.
        for column in table.not_null:
            if column in table.has_default or column in row.values:
                continue
            report.error(
                "oracle-seed-not-null", row.path,
                "line %d: %s.%s is NOT NULL with no default and the seed row "
                "omits it (ORA-01400 at deploy time)" % (row.line, row.key, column))

    report.count("oracle_seed_rows_parsed", len(rows))
    report.count("oracle_seed_values_checked", checked_values)
    report.count("oracle_seed_unknown_tables", len(unknown_tables))
    if unknown_tables:
        report.detail("oracle_seed_unknown_tables", sorted(unknown_tables))

    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
