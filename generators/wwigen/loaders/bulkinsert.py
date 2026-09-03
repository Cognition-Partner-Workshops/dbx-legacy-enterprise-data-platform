"""SQL Server bcp format files and BULK INSERT scripts.

For every SQL Server and file-feed extract the generator emits:

* a non-XML bcp format file (version 14.0) describing the field order, the
  terminators and the target column ordinals;
* a ``BULK INSERT`` statement per table, batched and with an error file, in one
  script per target schema;
* a driver script that runs the scripts through ``sqlcmd`` using the documented
  environment variables.

The file feeds load into their ``raw.File*`` landing tables exactly as the
ingestion packages expect, with ``MAXERRORS`` set high enough that the
deliberately malformed rows land in the error file rather than aborting the
load.

Nothing here has been run against SQL Server.
"""

from __future__ import annotations

import os

from .. import schema

FORMAT_VERSION = "14.0"


def _escape_terminator(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def format_file_text(spec: schema.TableSpec) -> str:
    """Non-XML bcp format file for one extract."""
    lines = [FORMAT_VERSION, str(len(spec.columns))]
    for index, column in enumerate(spec.columns, start=1):
        last = index == len(spec.columns)
        terminator = "\\n" if last else _escape_terminator(spec.delimiter)
        width = max(column.length, 12) * (4 if column.type == schema.STRING else 1)
        lines.append('%-4d SQLCHAR 0 %-6d "%s" %-4d %-34s SQL_Latin1_General_CP1_CI_AS'
                     % (index, width, terminator, index, column.name))
    return "\n".join(lines) + "\n"


SCRIPT_HEADER = """-- BULK INSERT script for the {label} extracts.
-- Deploy target: {database}
-- Depends on: the target tables existing, and the generated data files being
-- reachable from the SQL Server service account.
-- Called by: generators/output/<scale>/loaders/sqlserver/load_all.sh, or by hand.
-- Set :setvar DataRoot to the directory holding the generated data files.
--
-- Nothing in this repository has been run against a SQL Server instance.
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

"""


def script_text(label: str, database_token: str, specs, data_prefix: str) -> str:
    parts = [SCRIPT_HEADER.format(label=label, database=database_token)]
    for spec in specs:
        target = spec.target_object or spec.qualified_name
        data_file = "%s%s" % (data_prefix, spec.relative_path())
        format_file = "formats/%s.fmt" % spec.file_stem
        parts.append("PRINT 'loading %s';" % target)
        parts.append("BULK INSERT %s" % target)
        parts.append("FROM '$(DataRoot)/%s'" % data_file)
        parts.append("WITH (")
        parts.append("    FORMATFILE = '$(LoaderRoot)/%s'," % format_file)
        parts.append("    BATCHSIZE = 50000,")
        parts.append("    MAXERRORS = 100000,")
        parts.append("    ERRORFILE = '$(ErrorRoot)/%s.err'," % spec.file_stem)
        parts.append("    CODEPAGE = '65001',")
        parts.append("    TABLOCK")
        parts.append(");")
        parts.append("GO")
        parts.append("")
    return "\n".join(parts) + "\n"


DRIVER = """#!/bin/sh
# Run the generated BULK INSERT scripts against the staging database.
# Connection details come from the environment; no credential is stored here.
#   SQLSERVER_HOST SQLSERVER_PORT SQLSERVER_USER SQLSERVER_PASSWORD
#   SQLSERVER_STAGING_DB
# Nothing in this repository has been run against a SQL Server instance.
set -e

: "${SQLSERVER_HOST:?set SQLSERVER_HOST}"
: "${SQLSERVER_PORT:?set SQLSERVER_PORT}"
: "${SQLSERVER_USER:?set SQLSERVER_USER}"
: "${SQLSERVER_PASSWORD:?set SQLSERVER_PASSWORD}"
: "${SQLSERVER_STAGING_DB:?set SQLSERVER_STAGING_DB}"

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_ROOT="${DATA_ROOT:-$(cd "${BASE_DIR}/../../data" && pwd)}"
ERROR_ROOT="${ERROR_ROOT:-${BASE_DIR}/errors}"
mkdir -p "${ERROR_ROOT}"

for script in "${BASE_DIR}"/load_*.sql; do
    echo "running $(basename "${script}")"
    sqlcmd -S "${SQLSERVER_HOST},${SQLSERVER_PORT}" -U "${SQLSERVER_USER}" \\
        -d "${SQLSERVER_STAGING_DB}" -b -I \\
        -v DataRoot="${DATA_ROOT}" LoaderRoot="${BASE_DIR}" ErrorRoot="${ERROR_ROOT}" \\
        -i "${script}"
done

echo "bulk load scripts complete"
"""


def emit(output_dir: str, specs) -> list:
    """Write format files, per-group BULK INSERT scripts and the driver."""
    written = []
    relevant = [spec for spec in specs if spec.system in (schema.SQLSERVER, schema.FILE_FEED)]
    if not relevant:
        return written

    root = os.path.join(output_dir, "loaders", "sqlserver")
    formats = os.path.join(root, "formats")
    os.makedirs(formats, exist_ok=True)

    for spec in sorted(relevant, key=lambda item: item.key):
        path = os.path.join(formats, "%s.fmt" % spec.file_stem)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(format_file_text(spec))
        written.append(os.path.relpath(path, output_dir).replace(os.sep, "/"))

    by_schema = {}
    for spec in relevant:
        by_schema.setdefault(spec.schema, []).append(spec)

    for schema_name in sorted(by_schema):
        group = sorted(by_schema[schema_name], key=lambda item: item.name)
        path = os.path.join(root, "load_%s.sql" % schema_name.lower())
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(script_text(schema_name, "$(SQLSERVER_STAGING_DB)", group, "../../data/"))
        written.append(os.path.relpath(path, output_dir).replace(os.sep, "/"))

    driver = os.path.join(root, "load_all.sh")
    with open(driver, "w", encoding="utf-8") as handle:
        handle.write(DRIVER)
    os.chmod(driver, 0o755)
    written.append(os.path.relpath(driver, output_dir).replace(os.sep, "/"))
    return sorted(written)
