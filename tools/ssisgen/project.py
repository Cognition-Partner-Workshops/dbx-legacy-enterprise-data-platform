"""Emit SSIS project scaffolding: .dtproj, .conmgr and Project.params.

Connection managers never contain credentials. Connection strings are built from
project parameters that are supplied at deployment time from environment
variables (see config/README.md and .env.example).
"""

from __future__ import annotations

import os

from ssisgen import guid
from xml.sax.saxutils import escape, quoteattr

# name -> (provider, description, parameterised connection string expression)
CONNECTION_MANAGERS = {
    "WWI_Oracle_ERP": (
        "ORACLE",
        "Oracle ERP source (customer/supplier/product master, procurement, finance).",
        "Data Source=@[$Project::OracleHost]:@[$Project::OraclePort]/@[$Project::OracleService];"
        "User ID=@[$Project::OracleUser];Provider=OraOLEDB.Oracle.1;",
    ),
    "WWI_Source_DB": (
        "OLEDB",
        "WideWorldImporters OLTP source database.",
        "Data Source=@[$Project::SqlServerHost],@[$Project::SqlServerPort];"
        "Initial Catalog=@[$Project::SqlServerOltpDb];Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False;",
    ),
    "WWI_Staging_DB": (
        "OLEDB",
        "WideWorldImporters staging database (raw, stg, work, err, etl schemas).",
        "Data Source=@[$Project::SqlServerHost],@[$Project::SqlServerPort];"
        "Initial Catalog=@[$Project::SqlServerStagingDb];Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False;",
    ),
    "WWI_DW_Destination_DB": (
        "OLEDB",
        "WideWorldImportersDW warehouse destination.",
        "Data Source=@[$Project::SqlServerHost],@[$Project::SqlServerPort];"
        "Initial Catalog=@[$Project::SqlServerDwDb];Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False;",
    ),
    "WWI_Inbound_Files": (
        "FILE",
        "Inbound partner/carrier/bank file drop root.",
        "@[$Project::InboundFileRoot]",
    ),
    "WWI_Archive_Files": (
        "FILE",
        "Archive root for successfully processed inbound files.",
        "@[$Project::ArchiveFileRoot]",
    ),
    "WWI_Reject_Files": (
        "FILE",
        "Reject root for malformed inbound records.",
        "@[$Project::RejectFileRoot]",
    ),
}

# name -> (type, default, sensitive, description)
PROJECT_PARAMETERS = [
    ("OracleHost", "String", "oracle-erp.internal.example", False, "ORACLE_HOST"),
    ("OraclePort", "Int32", "1521", False, "ORACLE_PORT"),
    ("OracleService", "String", "WWIERP", False, "ORACLE_SERVICE"),
    ("OracleUser", "String", "WWI_ETL", False, "ORACLE_USER"),
    ("OraclePassword", "String", "", True, "ORACLE_PASSWORD - supplied at deploy time, never committed"),
    ("SqlServerHost", "String", "sqlserver.internal.example", False, "SQLSERVER_HOST"),
    ("SqlServerPort", "Int32", "1433", False, "SQLSERVER_PORT"),
    ("SqlServerUser", "String", "WWI_ETL", False, "SQLSERVER_USER"),
    ("SqlServerPassword", "String", "", True, "SQLSERVER_PASSWORD - supplied at deploy time, never committed"),
    ("SqlServerOltpDb", "String", "WideWorldImporters", False, "SQLSERVER_OLTP_DB"),
    ("SqlServerStagingDb", "String", "WideWorldImportersStaging", False, "SQLSERVER_STAGING_DB"),
    ("SqlServerDwDb", "String", "WideWorldImportersDW", False, "SQLSERVER_DW_DB"),
    ("InboundFileRoot", "String", "D:\\WWI\\inbound", False, "ETL_INBOUND_FILE_ROOT"),
    ("ArchiveFileRoot", "String", "D:\\WWI\\archive", False, "ETL_ARCHIVE_FILE_ROOT"),
    ("RejectFileRoot", "String", "D:\\WWI\\reject", False, "ETL_REJECT_FILE_ROOT"),
    ("DefaultBatchSize", "Int32", "100000", False, "ETL_DEFAULT_BATCH_SIZE"),
    ("SourceQueryTimeoutSeconds", "Int32", "3600", False, "ETL_SOURCE_QUERY_TIMEOUT_SECONDS"),
    ("MaxRejectPercent", "Int32", "5", False, "ETL_MAX_REJECT_PERCENT"),
    ("EnvironmentCode", "String", "DEV", False, "ETL_ENVIRONMENT_CODE"),
]


def write_conmgr(directory, name):
    provider, description, conn = CONNECTION_MANAGERS[name]
    creation = {"OLEDB": "OLEDB", "ORACLE": "OLEDB", "FILE": "FILE"}[provider]
    body = (
        '<?xml version="1.0"?>\n'
        '<DTS:ConnectionManager xmlns:DTS="www.microsoft.com/SqlServer/Dts"\n'
        '  DTS:ObjectName=%s\n'
        '  DTS:DTSID="%s"\n'
        '  DTS:CreationName="%s"\n'
        '  DTS:Description=%s>\n'
        '  <DTS:ObjectData>\n'
        '    <DTS:ConnectionManager\n'
        '      DTS:ConnectionString=%s />\n'
        '  </DTS:ObjectData>\n'
        '</DTS:ConnectionManager>\n'
        % (quoteattr(name), guid("cm:" + name), creation, quoteattr(description), quoteattr(conn))
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
        out.append('      <SSIS:Property SSIS:Name="Required">%s</SSIS:Property>' % ("True" if sensitive else "False"))
        out.append('      <SSIS:Property SSIS:Name="Sensitive">%s</SSIS:Property>' % ("True" if sensitive else "False"))
        out.append('      <SSIS:Property SSIS:Name="Value">%s</SSIS:Property>' % escape(default))
        out.append('      <SSIS:Property SSIS:Name="DataType">%s</SSIS:Property>' % dtype)
        out.append('      <SSIS:Property SSIS:Name="Name">%s</SSIS:Property>' % escape(name))
        out.append('    </SSIS:Properties>')
        out.append('  </SSIS:Parameter>')
    out.append('</SSIS:Parameters>')
    path = os.path.join(directory, "Project.params")
    with open(path, "w") as handle:
        handle.write("\n".join(out) + "\n")
    return path


def write_dtproj(directory, project_name, package_names, connection_names):
    out = ['<?xml version="1.0" encoding="utf-8"?>',
           '<Project xmlns="www.microsoft.com/SqlServer/SSIS" ToolsVersion="13.0">',
           '  <DeploymentModel>Project</DeploymentModel>',
           '  <ProductVersion>13.0.4001.0</ProductVersion>',
           '  <SchemaVersion>9.0.1.0</SchemaVersion>',
           '  <Database>',
           '    <Name>%s.ispac</Name>' % escape(project_name),
           '  </Database>',
           '  <DeploymentInfoXml>',
           '    <ProjectProtectionLevel>DontSaveSensitive</ProjectProtectionLevel>',
           '  </DeploymentInfoXml>',
           '  <Packages>']
    for name in package_names:
        out.append('    <Package>')
        out.append('      <Name>%s.dtsx</Name>' % escape(name))
        out.append('      <EntryPoint>%s</EntryPoint>' % ("true" if name.startswith("Master_") else "false"))
        out.append('    </Package>')
    out.append('  </Packages>')
    out.append('  <ConnectionManagers>')
    for name in connection_names:
        out.append('    <ConnectionManager>')
        out.append('      <Name>%s.conmgr</Name>' % escape(name))
        out.append('    </ConnectionManager>')
    out.append('  </ConnectionManagers>')
    out.append('  <Configurations>')
    for env in ("Development", "Test", "Production"):
        out.append('    <Configuration>')
        out.append('      <Name>%s</Name>' % env)
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
