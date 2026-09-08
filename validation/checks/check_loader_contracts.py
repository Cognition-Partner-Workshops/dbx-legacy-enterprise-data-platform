#!/usr/bin/env python3
"""Every generated loader has to match the object it loads into.

The first SMALL load never inserted a row. All 46 Oracle control files named
columns the deployed tables do not have and left required ones unfed, all 30
SQL Server format files described a different shape from the raw tables, 30
extracts collided onto 17 targets, the file feeds used names no ingestion
package reads, and the data path in the driver resolved to nothing. Every one
of those is visible without a database, which is what this check does:

* each Oracle extract is projected onto its table in ``oracle/tables`` - the
  columns must exist, be unique, cover every required column, and hold values
  the column's type accepts;
* each SQL Server extract is projected onto its landing table in
  ``sqlserver/staging``, and no two extracts may claim the same table;
* each file feed must be declared in ``config/landing-zone.yaml``, with the
  encoding, delimiter, header and extension the ingestion package reads;
* the emitted control, format and driver artefacts must resolve their data
  file, exactly once, to the file the generator actually writes.

Static analysis only: the loaders are emitted into a temporary directory and
read back. Nothing here connects to Oracle or SQL Server, and no data file is
generated.

    python3 validation/checks/check_loader_contracts.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import tempfile

import estatelib as lib

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
ESTATE_ROOT = os.environ.get("WWI_ESTATE_ROOT") or REPO_ROOT

# The generator is part of the estate under test, so a fixture that copies the
# estate gets its generator checked too.
GENERATORS = os.path.join(ESTATE_ROOT, "generators")
if not os.path.isdir(GENERATORS):
    GENERATORS = os.path.join(REPO_ROOT, "generators")
sys.path.insert(0, GENERATORS)

from wwigen import canon, config, context, contracts, landing, schema  # noqa: E402
from wwigen.conform import ContractError  # noqa: E402
from wwigen.contracts import sql_map  # noqa: E402
from wwigen.loaders import bcp, landingzone, sqlloader  # noqa: E402

INFILE_RE = re.compile(r"^INFILE '([^']+)'", re.M)
DATA_ENTRY_RE = re.compile(r"Data = '([^']+)'")
DATA_ROOT_RE = re.compile(r"\$DataRoot = Join-Path \$LoaderRoot '([^']+)'")
FORMAT_HEADER_RE = re.compile(r"^\d+\s+SQLCHAR\s+\d+\s+\d+\s+\"[^\"]*\"\s+(\d+)\s+(\S+)", re.M)
PROPERTY_ARGUMENT_RE = re.compile(r"(?<![\"'])\b[A-Za-z][\w-]*=\$\w+\.\w+")

# Types that can hold the extract's serialised form of the other.
COMPATIBLE = {
    (schema.STRING, "VARCHAR2"), (schema.STRING, "CHAR"), (schema.STRING, "CLOB"),
    (schema.FLAG, "VARCHAR2"), (schema.FLAG, "CHAR"),
    (schema.INTEGER, "NUMBER"), (schema.DECIMAL, "NUMBER"), (schema.INTEGER, "INTEGER"),
    (schema.DATE, "DATE"), (schema.TIMESTAMP, "DATE"), (schema.TIMESTAMP, "TIMESTAMP"),
    (schema.DATE, "TIMESTAMP"),
}


def conformed_specs(report):
    """Every producer spec in its loadable form, reporting the ones that fail."""
    good = []
    for spec in raw_specs():
        try:
            good.append(contracts.conform_spec(spec))
        except (ContractError, ValueError) as error:
            report.error(_contract_code(spec), _origin(spec), str(error))
    return good


def raw_specs():
    from wwigen import tables
    return tables.raw_specs()


def _contract_code(spec):
    if spec.system == schema.ORACLE:
        return "loader-oracle-contract"
    if spec.system == schema.SQLSERVER:
        return "loader-sql-contract"
    return "loader-landing-feed"


def _origin(spec):
    if spec.system == schema.ORACLE:
        return "oracle/tables/%s.%s.sql" % (spec.schema, spec.name)
    if spec.system == schema.SQLSERVER:
        return "sqlserver/staging/tables"
    return "config/landing-zone.yaml"


def check_oracle(report, specs):
    tables = canon.oracle_tables()
    for spec in specs:
        key = "%s.%s" % (spec.schema, spec.name)
        table = tables.get(key)
        origin = "oracle/tables/%s.sql" % key
        if table is None:
            report.error("loader-oracle-table", origin,
                         "%s loads into %s, which oracle/tables does not create"
                         % (spec.key, key))
            continue
        columns = {column.name.upper(): column for column in table.columns}
        seen = set()
        for column in spec.columns:
            name = column.name.upper()
            if name in seen:
                report.error("loader-oracle-duplicate", origin,
                             "%s writes %s twice; SQL*Loader takes the last one"
                             % (spec.key, column.name))
            seen.add(name)
            canonical = columns.get(name)
            if canonical is None:
                report.error("loader-oracle-column", origin,
                             "%s writes %s, which %s does not have (ORA-00904)"
                             % (spec.key, column.name, key))
                continue
            family = canonical.type_name.split("(")[0].upper()
            if (column.type, family) not in COMPATIBLE:
                report.error("loader-oracle-type", origin,
                             "%s writes %s as %s but %s is %s"
                             % (spec.key, column.name, column.type, key, canonical.type_name))
            if column.nullable and not canonical.nullable and not canonical.has_default:
                report.error("loader-oracle-nullability", origin,
                             "%s may write %s empty but %s declares it NOT NULL (ORA-01400)"
                             % (spec.key, column.name, key))
        for canonical in table.columns:
            if canonical.required and canonical.name.upper() not in seen:
                report.error("loader-oracle-required", origin,
                             "%s never writes %s, which is NOT NULL with no default "
                             "(ORA-01400)" % (spec.key, canonical.name))
    report.count("oracle_loader_contracts", len(specs))


def check_sqlserver(report, specs):
    tables = canon.sqlserver_landing_tables()
    claimed = {}
    landed = [spec for spec in specs if contracts.is_landed(spec)]
    for spec in landed:
        origin = "sqlserver/staging/tables"
        table = tables.get(spec.target_object)
        if table is None:
            report.error("loader-sql-table", origin,
                         "%s loads into %s, which sqlserver/staging does not create"
                         % (spec.key, spec.target_object))
            continue
        claimed.setdefault(spec.target_object, []).append(spec.key)
        columns = {column.name.upper(): column for column in table.columns}
        seen = set()
        for column in spec.columns:
            name = column.name.upper()
            if name in seen:
                report.error("loader-sql-duplicate", origin,
                             "%s writes %s twice" % (spec.key, column.name))
            seen.add(name)
            canonical = columns.get(name)
            if canonical is None:
                report.error("loader-sql-column", origin,
                             "%s writes %s, which %s does not have"
                             % (spec.key, column.name, spec.target_object))
            elif canonical.computed:
                report.error("loader-sql-computed", origin,
                             "%s writes %s, a computed column of %s"
                             % (spec.key, column.name, spec.target_object))
        for name in table.required_columns:
            if name.upper() not in seen:
                report.error("loader-sql-required", origin,
                             "%s never writes %s, which %s declares NOT NULL with "
                             "no default" % (spec.key, name, spec.target_object))

    # From the map itself, so a collision is reported even when one of the
    # colliding extracts also fails its contract.
    for key, target in sql_map.TARGETS.items():
        claimed.setdefault(target, [])
        if key not in claimed[target]:
            claimed[target].append(key)
    for target, keys in sorted(claimed.items()):
        if len(keys) > 1:
            report.error("loader-sql-collision", "generators/wwigen/contracts/sql_map.py",
                         "%s is the target of %s; the later load overwrites the "
                         "earlier one" % (target, ", ".join(sorted(keys))))

    unlanded = [spec.key for spec in specs if not contracts.is_landed(spec)]
    report.count("sqlserver_loader_contracts", len(landed))
    report.count("sqlserver_unlanded_extracts", len(unlanded))


def check_feeds(report, specs):
    declared = landing.feeds()
    for spec in specs:
        feed = declared.get(spec.name)
        origin = "config/landing-zone.yaml"
        if feed is None:
            report.error("loader-landing-feed", origin,
                         "%s is generated but no feed of that name is declared; "
                         "no ingestion package would read it" % spec.name)
            continue
        if spec.encoding != feed.codec:
            report.error("loader-landing-encoding", origin,
                         "%s is written as %s but the feed declares %s"
                         % (spec.name, spec.encoding, feed.encoding))
        if spec.delimiter != feed.delimiter:
            report.error("loader-landing-delimiter", origin,
                         "%s is written with %r but the feed declares %r"
                         % (spec.name, spec.delimiter, feed.delimiter))
        if spec.header != feed.header:
            report.error("loader-landing-header", origin,
                         "%s %s a header row; the feed declares the opposite"
                         % (spec.name, "writes" if spec.header else "omits"))
        if spec.extension != feed.extension:
            report.error("loader-landing-extension", origin,
                         "%s is written as .%s but the feed's pattern is %s"
                         % (spec.name, spec.extension, feed.pattern))
        if not feed.raw_table:
            report.error("loader-landing-target", origin,
                         "%s declares no raw table" % spec.name)
    report.count("file_feeds", len(specs))


def check_paths(report, specs, cfg):
    """Emit the loaders into a scratch tree and resolve every data path."""
    scratch = tempfile.mkdtemp(prefix="wwi-loader-paths-")
    try:
        expected = {spec.key: os.path.normpath(
            os.path.join(scratch, "data", spec.relative_path().replace("/", os.sep)))
            for spec in specs}
        oracle_specs = [spec for spec in specs if spec.system == schema.ORACLE]
        sql_specs = [spec for spec in specs if spec.system == schema.SQLSERVER]
        sqlloader.emit(scratch, oracle_specs)
        bcp.emit(scratch, sql_specs)

        for spec in oracle_specs:
            control = os.path.join(scratch, "loaders", "oracle", spec.schema,
                                   "%s.ctl" % spec.file_stem)
            with open(control, encoding="utf-8") as handle:
                text = handle.read()
            infile = INFILE_RE.search(text)
            if infile is None:
                report.error("loader-path", control, "%s has no INFILE" % spec.key)
                continue
            resolved = os.path.normpath(os.path.join(os.path.dirname(control),
                                                     infile.group(1).replace("/", os.sep)))
            if resolved != expected[spec.key]:
                report.error("loader-path", "generators/wwigen/loaders/sqlloader.py",
                             "%s reads %s, which resolves to %s, not %s"
                             % (spec.key, infile.group(1), resolved, expected[spec.key]))

        driver = os.path.join(scratch, "loaders", "sqlserver", "Load-SqlServer.ps1")
        if sql_specs and os.path.exists(driver):
            with open(driver, encoding="utf-8") as handle:
                text = handle.read()
            root = DATA_ROOT_RE.search(text)
            if root is None:
                report.error("loader-path", "generators/wwigen/loaders/bcp.py",
                             "the driver does not derive a data root")
                return
            base = os.path.join(scratch, "loaders", "sqlserver",
                                root.group(1).replace("\\", os.sep))
            for relative in DATA_ENTRY_RE.findall(text):
                resolved = os.path.normpath(os.path.join(base, relative.replace("\\", os.sep)))
                if resolved not in expected.values():
                    report.error("loader-path", "generators/wwigen/loaders/bcp.py",
                                 "the driver reads %s, which resolves to %s and is "
                                 "not a generated extract" % (relative, resolved))
            if "SQLSERVER_PASSWORD" not in text:
                report.error("loader-credentials",
                             "generators/wwigen/loaders/bcp.py",
                             "the driver does not read the password from the environment")

        check_driver_arguments(report, scratch, oracle_specs, sql_specs)
        check_formats(report, scratch, sql_specs)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def check_driver_arguments(report, scratch, oracle_specs, sql_specs):
    """A keyword argument may not interpolate a property.

    PowerShell expands only the bare variable in an argument written as
    ``control=$load.Control``, so sqlldr received
    ``control=System.Collections.Hashtable.Control`` and every table failed.
    """
    drivers = []
    if oracle_specs:
        drivers.append((os.path.join(scratch, "loaders", "oracle", "Load-Oracle.ps1"),
                        "generators/wwigen/loaders/sqlloader.py"))
    if sql_specs:
        drivers.append((os.path.join(scratch, "loaders", "sqlserver", "Load-SqlServer.ps1"),
                        "generators/wwigen/loaders/bcp.py"))
    for path, owner in drivers:
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
        for line in text.splitlines():
            if line.lstrip().startswith("#"):
                continue
            for match in PROPERTY_ARGUMENT_RE.finditer(line):
                report.error("loader-driver", owner,
                             "%s interpolates a property in the argument %s, which "
                             "PowerShell expands as the object's type name"
                             % (os.path.basename(path), match.group(0).strip()))


def check_formats(report, scratch, specs):
    """The format file has to describe the extract, in the target's ordinals."""
    tables = canon.sqlserver_landing_tables()
    for spec in specs:
        if not contracts.is_landed(spec):
            continue
        path = os.path.join(scratch, "loaders", "sqlserver", "formats",
                            "%s.fmt" % spec.file_stem)
        if not os.path.exists(path):
            report.error("loader-format", "generators/wwigen/loaders/bcp.py",
                         "%s has no format file" % spec.key)
            continue
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
        fields = FORMAT_HEADER_RE.findall(text)
        if len(fields) != len(spec.columns):
            report.error("loader-format", "generators/wwigen/loaders/bcp.py",
                         "%s writes %d fields but its format file describes %d"
                         % (spec.key, len(spec.columns), len(fields)))
            continue
        table = tables.get(spec.target_object)
        ordinals = {column.name.upper(): index
                    for index, column in enumerate(table.columns, start=1)}
        for (ordinal, name), column in zip(fields, spec.columns):
            if name.upper() != column.name.upper():
                report.error("loader-format", "generators/wwigen/loaders/bcp.py",
                             "%s writes %s where its format file expects %s"
                             % (spec.key, column.name, name))
            elif int(ordinal) != ordinals.get(name.upper(), -1):
                report.error("loader-format", "generators/wwigen/loaders/bcp.py",
                             "%s maps %s to column %s of %s, which is column %s"
                             % (spec.key, name, ordinal, spec.target_object,
                                ordinals.get(name.upper())))


def check_rows(report, specs, cfg, sample=25):
    """Produce a slice of every extract: values have to fit the target column.

    The projection is only a promise until rows run through it - a business
    code handed to a numeric column, or a required column the producer leaves
    empty, fails here rather than as ORA-01722 half way through a load.
    """
    ctx = context.RunContext(cfg)
    for spec in specs:
        try:
            for index, _row in enumerate(spec.produce(cfg, ctx)):
                if index >= sample:
                    break
        except (ContractError, ValueError) as error:
            report.error(_contract_code(spec), _origin(spec), str(error))
    report.count("sampled_extracts", len(specs))


def check_landing_placement(report, specs, cfg):
    """Every feed has to land inside the landing root, under its own feed path."""
    for spec, feed, target in landingzone.placements(cfg, specs):
        if os.path.isabs(target) or ".." in target.split("\\"):
            report.error("loader-landing-path", "config/landing-zone.yaml",
                         "%s lands at %s, which is not inside the landing root"
                         % (spec.name, target))
        if not target.lower().startswith(feed.directory.replace("/", "\\").lower()):
            report.error("loader-landing-path", "config/landing-zone.yaml",
                         "%s lands at %s, not in its declared directory %s"
                         % (spec.name, target, feed.directory))


def run(args):
    report = lib.Report("check_loader_contracts")
    canon.reset_cache()
    landing.reset_cache()
    specs = conformed_specs(report)
    check_oracle(report, [spec for spec in specs if spec.system == schema.ORACLE])
    check_sqlserver(report, [spec for spec in specs if spec.system == schema.SQLSERVER])
    feeds = [spec for spec in specs if spec.system == schema.FILE_FEED]
    check_feeds(report, feeds)
    cfg = config.build_run_config("small")
    check_landing_placement(report, feeds, cfg)
    check_paths(report, specs, cfg)
    check_rows(report, specs, cfg)
    return report.emit(as_json=args.json, strict=args.strict, show_warnings=not args.quiet)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    lib.add_common_arguments(parser)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
