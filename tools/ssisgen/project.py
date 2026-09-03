"""Emit SSIS project scaffolding: .dtproj, .conmgr and Project.params.

The .dtproj shape follows the SSDT project-deployment format used by the
original ``wwi-ssis/wwi-ssis/Daily ETL.dtproj``: a ``DeploymentModelSpecificContent``
manifest carrying ``SSIS:Project`` with its packages, connection managers and
deployment info. Anything else is rejected by the SSIS build tooling.

Connection managers never contain credentials. The connection string is a
property expression built from project parameters, so the deployed package
picks the environment up at runtime; the literal ``ConnectionString`` attribute
is only the design-time default. Passwords are supplied at deployment time
through the ``CM.<name>.Password`` project connection parameters (see
config/README.md and .env.example).
"""

from __future__ import annotations

import os

from ssisgen import guid
from xml.sax.saxutils import escape, quoteattr

# Provider used by the SQL Server connection managers. The estate was authored
# against SQLNCLI11.1, which is out of support and absent from current build
# and runtime hosts; MSOLEDBSQL19 is the supported successor and is what the
# packages are validated against.
SQLSERVER_PROVIDER = "MSOLEDBSQL19.1"
ORACLE_PROVIDER = "OraOLEDB.Oracle.1"

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
        '+ (LEN(@[$Project::SqlServerUser]) > 0 ? "User ID=" + @[$Project::SqlServerUser] + ";" : "Integrated Security=SSPI;") '
        '+ (@[$Project::SqlServerTrustServerCertificate] ? "TrustServerCertificate=True;" : "")' % catalog_param[0]
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
        "D:\\WWI\\inbound",
        '@[$Project::InboundFileRoot]',
    ),
    "WWI_Archive_Files": (
        "FILE",
        "Archive root for successfully processed inbound files.",
        "D:\\WWI\\archive",
        '@[$Project::ArchiveFileRoot]',
    ),
    "WWI_Reject_Files": (
        "FILE",
        "Reject root for malformed inbound records.",
        "D:\\WWI\\reject",
        '@[$Project::RejectFileRoot]',
    ),
}

# name -> (type, default, sensitive, description)
PROJECT_PARAMETERS = [
    ("OracleHost", "String", "oracle-erp.internal.example", False, "ORACLE_HOST"),
    ("OraclePort", "Int32", "1521", False, "ORACLE_PORT"),
    ("OracleService", "String", "WWIGERP", False, "ORACLE_SERVICE"),
    ("OracleUser", "String", "WWI_EXTRACT", False, "ORACLE_USER"),
    ("OracleProvider", "String", ORACLE_PROVIDER, False, "Oracle OLE DB provider progid"),
    ("OraclePassword", "String", "", True, "ORACLE_PASSWORD - supplied at deploy time, never committed"),
    ("SqlServerHost", "String", "sqlserver.internal.example", False, "SQLSERVER_HOST"),
    ("SqlServerPort", "Int32", "1433", False, "SQLSERVER_PORT"),
    ("SqlServerUser", "String", "", False, "SQLSERVER_USER - empty selects Windows authentication"),
    ("SqlServerProvider", "String", SQLSERVER_PROVIDER, False, "SQL Server OLE DB provider progid"),
    ("SqlServerTrustServerCertificate", "Boolean", "false", False,
     "Trust a server certificate that is not chain-validated (non-production only)"),
    ("SqlServerPassword", "String", "", True, "SQLSERVER_PASSWORD - supplied at deploy time, never committed"),
    ("SqlServerOltpDb", "String", "WideWorldImporters", False, "SQLSERVER_OLTP_DB"),
    ("SqlServerStagingDb", "String", "WideWorldImporters_Staging", False, "SQLSERVER_STAGING_DB"),
    ("SqlServerDwDb", "String", "WideWorldImportersDW", False, "SQLSERVER_DW_DB"),
    ("InboundFileRoot", "String", "D:\\WWI\\inbound", False, "ETL_INBOUND_FILE_ROOT"),
    ("ArchiveFileRoot", "String", "D:\\WWI\\archive", False, "ETL_ARCHIVE_FILE_ROOT"),
    ("RejectFileRoot", "String", "D:\\WWI\\reject", False, "ETL_REJECT_FILE_ROOT"),
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


def write_conmgr(directory, name):
    kind, description, conn, expression = CONNECTION_MANAGERS[name]
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
