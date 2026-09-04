"""Client-side bcp artefacts for the SQL Server extracts.

The SQL Server instance is not the machine the extracts are generated on, so a
server-side ``BULK INSERT`` can never see the files: the load runs from the
generating host with ``bcp``, which reads the file locally and sends the rows
over the ordinary client connection.

For every extract that has a canonical landing table the generator emits:

* a non-XML bcp format file whose field order is the extract's own and whose
  server column ordinals are the target table's, so a subset of the table's
  columns loads without the untouched columns losing their ``DEFAULT``;
* one PowerShell driver that walks the extracts in a fixed order, resolves each
  data file exactly once from a single root, and refuses to start a load whose
  file is missing.

An extract with no canonical landing table is recorded as unlanded rather than
pointed at somebody else's table; :mod:`wwigen.contracts.sql_map` owns that
decision and ``unlanded.txt`` reports it next to the loaders.

The driver reads the connection from the documented environment variables and
passes the password through ``$env:SQLSERVER_PASSWORD`` at the call, so no
credential is written into a generated artefact.

Nothing here has been run against SQL Server.
"""

from __future__ import annotations

import os

from .. import canon, contracts, schema

FORMAT_VERSION = "14.0"
CHAR_COLLATION = "SQL_Latin1_General_CP1_CI_AS"

# The data directory relative to the loader directory. Written once here and
# joined once in the driver: the load path that failed before was assembled
# twice, so it carried the traversal twice.
DATA_ROOT_RELATIVE = "..\\..\\data"


def _escape_terminator(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _host_width(column: schema.Column) -> int:
    """Bytes bcp should allow for the field's character representation."""
    if column.type == schema.STRING:
        return max(column.length, 12) * 4
    if column.type == schema.TIMESTAMP:
        return 27
    if column.type == schema.DATE:
        return 12
    return max(column.length, 40)


def format_file_text(spec: schema.TableSpec, table: canon.SqlTable) -> str:
    """Non-XML bcp format file mapping the extract onto its landing table."""
    ordinals = {column.name.upper(): index
                for index, column in enumerate(table.columns, start=1)}
    lines = [FORMAT_VERSION, str(len(spec.columns))]
    for index, column in enumerate(spec.columns, start=1):
        ordinal = ordinals.get(column.name.upper())
        if ordinal is None:
            raise ValueError("%s: %s has no column %s"
                             % (spec.key, table.key, column.name))
        last = index == len(spec.columns)
        terminator = "\\n" if last else _escape_terminator(spec.delimiter)
        collation = CHAR_COLLATION if column.type in (schema.STRING, schema.FLAG) else '""'
        lines.append('%-4d SQLCHAR 0 %-6d "%s" %-4d %-34s %s'
                     % (index, _host_width(column), terminator, ordinal,
                        column.name, collation))
    return "\n".join(lines) + "\n"


DRIVER_HEADER = '''#Requires -Version 5.1
<#
.SYNOPSIS
    Load the generated SQL Server extracts into the staging landing tables.

.DESCRIPTION
    Client-side bcp: every data file is read on this machine and the rows are
    sent over the SQL Server client connection, because the instance cannot
    reach the directory the generator writes to.

    Connection details come from the environment and no credential is stored
    in this file:

        SQLSERVER_HOST SQLSERVER_PORT SQLSERVER_USER SQLSERVER_PASSWORD
        SQLSERVER_STAGING_DB

    Nothing in this repository has been run against a SQL Server instance.
#>
[CmdletBinding()]
param(
    # Root holding the generated data files. Resolved once, here.
    [string] $DataRoot,
    # Where bcp writes the per-table error files.
    [string] $ErrorRoot,
    # Print the loads and their resolved files without running them.
    [switch] $ListOnly
)

$ErrorActionPreference = 'Stop'

foreach ($name in 'SQLSERVER_HOST', 'SQLSERVER_PORT', 'SQLSERVER_USER',
                  'SQLSERVER_PASSWORD', 'SQLSERVER_STAGING_DB') {
    if (-not (Test-Path "env:$name")) { throw "set $name" }
}

$LoaderRoot = $PSScriptRoot
if (-not $DataRoot) { $DataRoot = Join-Path $LoaderRoot '@DATA_ROOT@' }
$DataRoot = (Resolve-Path -LiteralPath $DataRoot).Path
if (-not $ErrorRoot) { $ErrorRoot = Join-Path $LoaderRoot 'errors' }
New-Item -ItemType Directory -Force -Path $ErrorRoot | Out-Null

$Server = '{0},{1}' -f $env:SQLSERVER_HOST, $env:SQLSERVER_PORT

$Loads = @(
'''

DRIVER_FOOTER = ''')

$failed = @()
foreach ($load in $Loads) {
    $data = Join-Path $DataRoot $load.Data
    $format = Join-Path $LoaderRoot $load.Format
    $errors = Join-Path $ErrorRoot ($load.Name + '.err')
    if (-not (Test-Path -LiteralPath $data)) {
        throw "extract file not found: $data"
    }
    if (-not (Test-Path -LiteralPath $format)) {
        throw "format file not found: $format"
    }
    if ($ListOnly) {
        '{0,-28} {1}' -f $load.Table, $data
        continue
    }
    Write-Host ("loading {0} from {1}" -f $load.Table, $load.Data)
    & bcp $load.Table 'in' $data `
        -S $Server -d $env:SQLSERVER_STAGING_DB `
        -U $env:SQLSERVER_USER -P $env:SQLSERVER_PASSWORD `
        -f $format -F $load.FirstRow -e $errors -b 10000 -m 100000
    if ($LASTEXITCODE -ne 0) {
        $failed += ('{0} (bcp exit {1}, errors in {2})' -f $load.Table, $LASTEXITCODE, $errors)
    }
}

if ($failed) {
    throw ("bcp failed for: " + ($failed -join '; '))
}
Write-Host 'sql server extracts loaded'
'''


def _entry(spec: schema.TableSpec) -> str:
    return ("    @{ Name = '%s'; Table = '%s'; Data = '%s'; "
            "Format = '%s'; FirstRow = %d }"
            % (spec.file_stem, spec.target_object,
               spec.relative_path().replace("/", "\\"),
               "formats\\%s.fmt" % spec.file_stem,
               2 if spec.header else 1))


def driver_text(specs, data_root: str) -> str:
    body = "\n".join(_entry(spec) for spec in specs)
    header = DRIVER_HEADER.replace("@DATA_ROOT@", data_root)
    return header + body + "\n" + DRIVER_FOOTER


UNLANDED_HEADER = """The extracts below are generated but not loaded.

Each is OLTP-side content with no landing table in sqlserver/staging: the
estate never extracts it ahead of the ETL. Pointing it at another table's
loader would be a collision, so it is recorded here instead. Adding a landing
table for any of them is a schema change, not a generator change.

"""


def emit(output_dir: str, specs) -> list:
    """Write the format files, the unlanded record and the PowerShell driver."""
    written = []
    sql_specs = [spec for spec in specs if spec.system == schema.SQLSERVER]
    if not sql_specs:
        return written

    landed = sorted([spec for spec in sql_specs if contracts.is_landed(spec)],
                    key=lambda item: item.key)
    unlanded = sorted([spec for spec in sql_specs if not contracts.is_landed(spec)],
                      key=lambda item: item.key)

    root = os.path.join(output_dir, "loaders", "sqlserver")
    formats = os.path.join(root, "formats")
    os.makedirs(formats, exist_ok=True)

    tables = canon.sqlserver_landing_tables()
    for spec in landed:
        table = tables.get(spec.target_object)
        if table is None:
            raise ValueError("%s targets %s, which is not a landing table"
                             % (spec.key, spec.target_object))
        path = os.path.join(formats, "%s.fmt" % spec.file_stem)
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(format_file_text(spec, table))
        written.append(os.path.relpath(path, output_dir).replace(os.sep, "/"))

    driver = os.path.join(root, "Load-SqlServer.ps1")
    with open(driver, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(driver_text(landed, DATA_ROOT_RELATIVE))
    written.append(os.path.relpath(driver, output_dir).replace(os.sep, "/"))

    record = os.path.join(root, "unlanded.txt")
    with open(record, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(UNLANDED_HEADER)
        for spec in unlanded:
            handle.write("%-44s %s\n" % (spec.key, "data/" + spec.relative_path()))
    written.append(os.path.relpath(record, output_dir).replace(os.sep, "/"))
    return sorted(written)
