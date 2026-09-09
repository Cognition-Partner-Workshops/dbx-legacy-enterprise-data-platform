"""Emit SSIS project scaffolding: .dtproj, .conmgr and Project.params.

The .dtproj shape follows the SSDT project-deployment format used by the
original ``wwi-ssis/wwi-ssis/Daily ETL.dtproj``: a ``DeploymentModelSpecificContent``
manifest carrying ``SSIS:Project`` with its packages, connection managers and
deployment info. Anything else is rejected by the SSIS build tooling.

Connection managers never contain credentials, and no credential is ever named
by an expression. The connection string is a property expression built from the
non-sensitive project parameters - host, port, service, catalog, provider, user
- so the deployed package picks the environment up at runtime; the literal
``ConnectionString`` attribute is only the design-time default.

The password is not part of that expression. A sensitive parameter read from an
expression fails the package at validation with 0xC0017010 ("the variable is
used in an expression but is not accessible"), because the expression evaluator
refuses to read a sensitive value. The supported binding surface is the
connection manager's own parameter, ``CM.<connection>.Password``, a project
parameter the SSIS runtime applies to the connection manager object itself
before the ConnectionString expression is evaluated; the catalog environment
binds it by reference. That is why the OLE DB connection managers emit
``CM.<name>.Password`` / ``CM.<name>.UserName`` into the project manifest and
why no ``;Password=`` fragment appears in any expression.

File roots come from the landing zone, not from this module: the SSIS roots are
derived from ``generators.wwigen.landing.DEFAULT_ROOT`` so the packages, the
loaders and the staged feeds cannot point at different trees.
"""

from __future__ import annotations

import os
import sys

from ssisgen import guid
from xml.sax.saxutils import escape, quoteattr

_GENERATORS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "generators")
if _GENERATORS not in sys.path:
    sys.path.insert(0, _GENERATORS)
from wwigen.landing import DEFAULT_ROOT as LANDING_ROOT  # noqa: E402

_CHECKS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "validation", "checks")
if _CHECKS not in sys.path:
    sys.path.insert(0, _CHECKS)
from connection_expression import assert_no_sensitive  # noqa: E402

# The three file roots the packages know about, all beneath the one landing
# root. 'reject' is not a landing-zone directory - malformed records go to
# quarantine - so the parameter is QuarantineFileRoot.
INBOUND_ROOT = os.path.join(LANDING_ROOT, "inbound").replace("/", "\\")
ARCHIVE_ROOT = os.path.join(LANDING_ROOT, "archive").replace("/", "\\")
QUARANTINE_ROOT = os.path.join(LANDING_ROOT, "quarantine").replace("/", "\\")

# Provider used by the SQL Server connection managers. The estate was authored
# against SQLNCLI11.1, which is out of support and absent from current build
# and runtime hosts; MSOLEDBSQL19 is the supported successor and is what the
# packages are validated against.
SQLSERVER_PROVIDER = "MSOLEDBSQL19.1"
ORACLE_PROVIDER = "OraOLEDB.Oracle.1"

# OLE DB Driver 19 spells the keyword with spaces. The unspaced SqlClient form
# `TrustServerCertificate` parses without error and is then ignored, so an
# untrusted certificate chain still fails the connect; the estate's SQL Server
# certificate is self-signed, which is how the difference surfaces.
SQLSERVER_TRUST_KEYWORD = "Trust Server Certificate=True;"

# name -> (kind, description, design-time connection string, ConnectionString expression)
#
# The expression is evaluated by the SSIS runtime; the literal string is what
# the designer shows before evaluation. Both are derived from the same project
# parameters so they cannot drift.


def _sql_connection(catalog_param):
    literal = (
        "Data Source=sqlserver.internal.example,1433;Initial Catalog=%s;"
        "Provider=%s;Auto Translate=False;" % (catalog_param[1], SQLSERVER_PROVIDER)
    )
    expression = (
        '"Data Source=" + @[$Project::SqlServerHost] + "," + (DT_WSTR, 12) @[$Project::SqlServerPort] '
        '+ ";Initial Catalog=" + @[$Project::%s] '
        '+ ";Provider=" + @[$Project::SqlServerProvider] '
        '+ ";Auto Translate=False;" '
        '+ (LEN(@[$Project::SqlServerUser]) > 0 ? "User ID=" + @[$Project::SqlServerUser] '
        '+ ";" : "Integrated Security=SSPI;") '
        '+ (@[$Project::SqlServerTrustServerCertificate] ? "%s" : "")'
        % (catalog_param[0], SQLSERVER_TRUST_KEYWORD)
    )
    return literal, expression


_OLTP = _sql_connection(("SqlServerOltpDb", "WideWorldImporters"))
_STAGING = _sql_connection(("SqlServerStagingDb", "WideWorldImporters_Staging"))
_DW = _sql_connection(("SqlServerDwDb", "WideWorldImportersDW"))

_ORACLE_LITERAL = (
    "Data Source=oracle-erp.internal.example:1521/WWIGERP;User ID=WWI_EXTRACT;Provider=%s;" % ORACLE_PROVIDER
)
_ORACLE_EXPRESSION = (
    '"Data Source=" + @[$Project::OracleHost] + ":" + (DT_WSTR, 12) @[$Project::OraclePort] '
    '+ "/" + @[$Project::OracleService] '
    '+ ";User ID=" + @[$Project::OracleUser] '
    '+ ";Provider=" + @[$Project::OracleProvider] + ";"'
)

CONNECTION_MANAGERS = {
    "WWI_Oracle_ERP": (
        "OLEDB",
        "Oracle ERP source (customer/supplier/product master, procurement, finance).",
        _ORACLE_LITERAL,
        _ORACLE_EXPRESSION,
    ),
    "WWI_Source_DB": (
        "OLEDB",
        "WideWorldImporters OLTP source database.",
        _OLTP[0],
        _OLTP[1],
    ),
    "WWI_Staging_DB": (
        "OLEDB",
        "WideWorldImporters staging database (raw, stg, work, err, etl schemas).",
        _STAGING[0],
        _STAGING[1],
    ),
    "WWI_DW_Destination_DB": (
        "OLEDB",
        "WideWorldImportersDW warehouse destination.",
        _DW[0],
        _DW[1],
    ),
    "WWI_Inbound_Files": (
        "FILE",
        "Inbound partner/carrier/bank file drop root.",
        INBOUND_ROOT,
        '@[$Project::InboundFileRoot]',
    ),
    "WWI_Archive_Files": (
        "FILE",
        "Archive root for successfully processed inbound files.",
        ARCHIVE_ROOT,
        '@[$Project::ArchiveFileRoot]',
    ),
    "WWI_Quarantine_Files": (
        "FILE",
        "Quarantine root for malformed inbound records.",
        QUARANTINE_ROOT,
        '@[$Project::QuarantineFileRoot]',
    ),
}

# name -> (type, default, sensitive, description)
PROJECT_PARAMETERS = [
    ("OracleHost", "String", "oracle-erp.internal.example", False, "ORACLE_HOST"),
    ("OraclePort", "Int32", "1521", False, "ORACLE_PORT"),
    ("OracleService", "String", "WWIGERP", False, "ORACLE_SERVICE"),
    ("OracleUser", "String", "WWI_EXTRACT", False, "ORACLE_USER"),
    ("OracleProvider", "String", ORACLE_PROVIDER, False, "Oracle OLE DB provider progid"),
    ("SqlServerHost", "String", "sqlserver.internal.example", False, "SQLSERVER_HOST"),
    ("SqlServerPort", "Int32", "1433", False, "SQLSERVER_PORT"),
    ("SqlServerUser", "String", "", False, "SQLSERVER_USER - empty selects Windows authentication"),
    ("SqlServerProvider", "String", SQLSERVER_PROVIDER, False, "SQL Server OLE DB provider progid"),
    ("SqlServerTrustServerCertificate", "Boolean", "false", False,
     "Trust a server certificate that is not chain-validated (non-production only)"),
    ("SqlServerOltpDb", "String", "WideWorldImporters", False, "SQLSERVER_OLTP_DB"),
    ("SqlServerStagingDb", "String", "WideWorldImporters_Staging", False, "SQLSERVER_STAGING_DB"),
    ("SqlServerDwDb", "String", "WideWorldImportersDW", False, "SQLSERVER_DW_DB"),
    ("InboundFileRoot", "String", INBOUND_ROOT, False, "ETL_INBOUND_FILE_ROOT"),
    ("ArchiveFileRoot", "String", ARCHIVE_ROOT, False, "ETL_ARCHIVE_FILE_ROOT"),
    ("QuarantineFileRoot", "String", QUARANTINE_ROOT, False, "ETL_QUARANTINE_FILE_ROOT"),
    ("DefaultBatchSize", "Int32", "100000", False, "ETL_DEFAULT_BATCH_SIZE"),
    ("SourceQueryTimeoutSeconds", "Int32", "3600", False, "ETL_SOURCE_QUERY_TIMEOUT_SECONDS"),
    ("MaxRejectPercent", "Int32", "5", False, "ETL_MAX_REJECT_PERCENT"),
    ("EnvironmentCode", "String", "DEV", False, "ETL_ENVIRONMENT_CODE"),
]

# SSIS parameter type codes as persisted in Project.params / the project manifest.
PARAM_TYPE_CODES = {"String": "18", "Int32": "9", "Boolean": "3"}

PROJECT_CREATION_DATE = "2016-04-10T11:13:17.6000465+10:00"
PROJECT_CREATOR = "WWI\\etl_build"
PROJECT_COMPUTER = "WWIBUILD01"


def connection_string(name):
    """Design-time connection string literal for a connection manager."""
    return CONNECTION_MANAGERS[name][2]


def assert_expressions_are_credential_free():
    """Generation-time guard: no connection expression may name a secret.

    Called before anything is written, so a generator change that puts a
    password back into an expression fails the build rather than shipping a
    package that dies at validation with 0xC0017010.
    """
    for name, (_kind, _description, _literal, expression) in sorted(CONNECTION_MANAGERS.items()):
        assert_no_sensitive(expression, "connection manager %s" % name)
    sensitive = [entry[0] for entry in PROJECT_PARAMETERS if entry[3]]
    if sensitive:
        raise ValueError(
            "sensitive project parameters %s are still declared; credentials "
            "belong on CM.<connection>.Password" % ", ".join(sensitive))


assert_expressions_are_credential_free()


def write_conmgr(directory, name):
    kind, description, conn, expression = CONNECTION_MANAGERS[name]
    assert_no_sensitive(expression, "connection manager %s" % name)
    body = (
        '<?xml version="1.0"?>\n'
        '<DTS:ConnectionManager xmlns:DTS="www.microsoft.com/SqlServer/Dts"\n'
        '  DTS:ObjectName=%s\n'
        '  DTS:DTSID="%s"\n'
        '  DTS:CreationName="%s"\n'
        '  DTS:Description=%s>\n'
        '  <DTS:PropertyExpression DTS:Name="ConnectionString">%s</DTS:PropertyExpression>\n'
        '  <DTS:ObjectData>\n'
        '    <DTS:ConnectionManager\n'
        '      DTS:ConnectionString=%s />\n'
        '  </DTS:ObjectData>\n'
        '</DTS:ConnectionManager>\n'
        % (quoteattr(name), guid("cm:" + name), kind, quoteattr(description),
           escape(expression), quoteattr(conn))
    )
    path = os.path.join(directory, name + ".conmgr")
    with open(path, "w") as handle:
        handle.write(body)
    return path


def write_project_params(directory):
    out = ['<?xml version="1.0"?>', '<SSIS:Parameters xmlns:SSIS="www.microsoft.com/SqlServer/SSIS">']
    for name, dtype, default, sensitive, description in PROJECT_PARAMETERS:
        out.append('  <SSIS:Parameter SSIS:Name=%s>' % quoteattr(name))
        out.append('    <SSIS:Properties>')
        out.append('      <SSIS:Property SSIS:Name="ID">%s</SSIS:Property>' % guid("param:" + name))
        out.append('      <SSIS:Property SSIS:Name="CreationName"></SSIS:Property>')
        out.append('      <SSIS:Property SSIS:Name="Description">%s</SSIS:Property>' % escape(description))
        out.append('      <SSIS:Property SSIS:Name="IncludeInDebugDump">0</SSIS:Property>')
        out.append('      <SSIS:Property SSIS:Name="Required">%s</SSIS:Property>' % ("1" if sensitive else "0"))
        out.append('      <SSIS:Property SSIS:Name="Sensitive">%s</SSIS:Property>' % ("1" if sensitive else "0"))
        if not sensitive:
            out.append('      <SSIS:Property SSIS:Name="Value">%s</SSIS:Property>' % escape(default))
        out.append('      <SSIS:Property SSIS:Name="DataType">%s</SSIS:Property>' % PARAM_TYPE_CODES[dtype])
        out.append('    </SSIS:Properties>')
        out.append('  </SSIS:Parameter>')
    out.append('</SSIS:Parameters>')
    path = os.path.join(directory, "Project.params")
    with open(path, "w") as handle:
        handle.write("\n".join(out) + "\n")
    return path


def _manifest_parameter(indent, name, value, type_code, sensitive=False):
    pad = " " * indent
    out = [
        '%s<SSIS:Parameter SSIS:Name=%s>' % (pad, quoteattr(name)),
        '%s  <SSIS:Properties>' % pad,
        '%s    <SSIS:Property SSIS:Name="ID"></SSIS:Property>' % pad,
        '%s    <SSIS:Property SSIS:Name="CreationName"></SSIS:Property>' % pad,
        '%s    <SSIS:Property SSIS:Name="Description"></SSIS:Property>' % pad,
        '%s    <SSIS:Property SSIS:Name="IncludeInDebugDump">0</SSIS:Property>' % pad,
        '%s    <SSIS:Property SSIS:Name="Required">0</SSIS:Property>' % pad,
        '%s    <SSIS:Property SSIS:Name="Sensitive">%s</SSIS:Property>' % (pad, "1" if sensitive else "0"),
    ]
    if not sensitive:
        out.append('%s    <SSIS:Property SSIS:Name="Value">%s</SSIS:Property>' % (pad, escape(value)))
    out.append('%s    <SSIS:Property SSIS:Name="DataType">%s</SSIS:Property>' % (pad, type_code))
    out.append('%s  </SSIS:Properties>' % pad)
    out.append('%s</SSIS:Parameter>' % pad)
    return out


def _connection_parameters(indent, connection_names):
    """CM.<name>.<property> parameters: how the catalog overrides a connection at deploy time."""
    out = []
    for name in connection_names:
        kind = CONNECTION_MANAGERS[name][0]
        out.extend(_manifest_parameter(indent, "CM.%s.ConnectionString" % name,
                                       CONNECTION_MANAGERS[name][2], "18"))
        if kind == "OLEDB":
            out.extend(_manifest_parameter(indent, "CM.%s.Password" % name, "", "18", sensitive=True))
            out.extend(_manifest_parameter(indent, "CM.%s.RetainSameConnection" % name, "false", "3"))
            out.extend(_manifest_parameter(indent, "CM.%s.UserName" % name, "", "18"))
    return out


def write_dtproj(directory, project_name, package_names, connection_names):
    out = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<Project xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">',
        '  <DeploymentModel>Project</DeploymentModel>',
        '  <ProductVersion>13.0.4001.0</ProductVersion>',
        '  <SchemaVersion>9.0.1.0</SchemaVersion>',
        '  <Database>',
        '    <Name>%s.database</Name>' % escape(project_name),
        '    <FullPath>%s.database</FullPath>' % escape(project_name),
        '  </Database>',
        '  <DataSources />',
        '  <DataSourceViews />',
        '  <DeploymentModelSpecificContent>',
        '    <Manifest>',
        '      <SSIS:Project SSIS:ProtectionLevel="DontSaveSensitive" xmlns:SSIS="www.microsoft.com/SqlServer/SSIS">',
        '        <SSIS:Properties>',
        '          <SSIS:Property SSIS:Name="ID">%s</SSIS:Property>' % guid("project:" + project_name),
        '          <SSIS:Property SSIS:Name="Name">%s</SSIS:Property>' % escape(project_name),
        '          <SSIS:Property SSIS:Name="VersionMajor">1</SSIS:Property>',
        '          <SSIS:Property SSIS:Name="VersionMinor">0</SSIS:Property>',
        '          <SSIS:Property SSIS:Name="VersionBuild">1</SSIS:Property>',
        '          <SSIS:Property SSIS:Name="VersionComments"></SSIS:Property>',
        '          <SSIS:Property SSIS:Name="CreationDate">%s</SSIS:Property>' % PROJECT_CREATION_DATE,
        '          <SSIS:Property SSIS:Name="CreatorName">%s</SSIS:Property>' % escape(PROJECT_CREATOR),
        '          <SSIS:Property SSIS:Name="CreatorComputerName">%s</SSIS:Property>' % PROJECT_COMPUTER,
        '          <SSIS:Property SSIS:Name="Description"></SSIS:Property>',
        '          <SSIS:Property SSIS:Name="FormatVersion">1</SSIS:Property>',
        '        </SSIS:Properties>',
        '        <SSIS:Packages>',
    ]
    for name in package_names:
        out.append('          <SSIS:Package SSIS:Name="%s.dtsx" SSIS:EntryPoint="%s" />'
                   % (escape(name), "1" if name.startswith("Master_") else "0"))
    out.append('        </SSIS:Packages>')
    out.append('        <SSIS:ConnectionManagers>')
    for name in connection_names:
        out.append('          <SSIS:ConnectionManager SSIS:Name="%s.conmgr" />' % escape(name))
    out.append('        </SSIS:ConnectionManagers>')
    out.append('        <SSIS:DeploymentInfo>')
    out.append('          <SSIS:ProjectConnectionParameters>')
    out.extend(_connection_parameters(12, connection_names))
    out.append('          </SSIS:ProjectConnectionParameters>')
    out.append('          <SSIS:PackageInfo>')
    for name in package_names:
        out.append('            <SSIS:PackageMetaData SSIS:Name="%s.dtsx">' % escape(name))
        out.append('              <SSIS:Properties>')
        out.append('                <SSIS:Property SSIS:Name="ID">%s</SSIS:Property>' % guid("pkg:" + name))
        out.append('                <SSIS:Property SSIS:Name="Name">%s</SSIS:Property>' % escape(name))
        out.append('                <SSIS:Property SSIS:Name="VersionMajor">1</SSIS:Property>')
        out.append('                <SSIS:Property SSIS:Name="VersionMinor">0</SSIS:Property>')
        out.append('                <SSIS:Property SSIS:Name="VersionBuild">1</SSIS:Property>')
        out.append('                <SSIS:Property SSIS:Name="VersionComments"></SSIS:Property>')
        out.append('                <SSIS:Property SSIS:Name="VersionGUID">%s</SSIS:Property>' % guid("ver:" + name))
        out.append('                <SSIS:Property SSIS:Name="PackageFormatVersion">8</SSIS:Property>')
        out.append('                <SSIS:Property SSIS:Name="Description"></SSIS:Property>')
        out.append('                <SSIS:Property SSIS:Name="ProtectionLevel">0</SSIS:Property>')
        out.append('              </SSIS:Properties>')
        out.append('              <SSIS:Parameters />')
        out.append('            </SSIS:PackageMetaData>')
    out.append('          </SSIS:PackageInfo>')
    out.append('        </SSIS:DeploymentInfo>')
    out.append('      </SSIS:Project>')
    out.append('    </Manifest>')
    out.append('  </DeploymentModelSpecificContent>')
    out.append('  <ControlFlowParts />')
    out.append('  <Miscellaneous />')
    out.append('  <Configurations>')
    for env in ("Development", "Test", "Production"):
        out.append('    <Configuration>')
        out.append('      <Name>%s</Name>' % env)
        out.append('      <Options>')
        out.append('        <OutputPath>bin</OutputPath>')
        out.append('        <ConnectionMappings />')
        out.append('        <ConnectionProviderMappings />')
        out.append('        <ConnectionSecurityMappings />')
        out.append('        <DatabaseStorageLocations />')
        out.append('        <TargetServerVersion>SQLServer2016</TargetServerVersion>')
        out.append('      </Options>')
        out.append('    </Configuration>')
    out.append('  </Configurations>')
    out.append('</Project>')
    path = os.path.join(directory, project_name + ".dtproj")
    with open(path, "w") as handle:
        handle.write("\n".join(out) + "\n")
    return path


def write_project(directory, project_name, package_names, connection_names):
    if not os.path.isdir(directory):
        os.makedirs(directory)
    written = [write_dtproj(directory, project_name, package_names, connection_names),
               write_project_params(directory)]
    for name in connection_names:
        written.append(write_conmgr(directory, name))
    return written
