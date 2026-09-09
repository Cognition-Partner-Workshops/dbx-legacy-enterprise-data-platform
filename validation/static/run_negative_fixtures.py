#!/usr/bin/env python3
"""Prove the static checks actually fail on broken artifacts.

run_all_checks.py reporting zero failures only means something if it would
report a failure when an artifact is wrong. This script copies real estate
artifacts into a scratch tree, injects one defect at a time - the defect
classes that were shipped in the generated estate and later fixed - and
asserts that the matching check reports it.

Nothing in the repository is modified: the scratch tree is a temporary copy
and run_all_checks.py is pointed at it through WWI_ESTATE_ROOT.

Usage:
    python3 validation/static/run_negative_fixtures.py [--verbose]
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
CHECKER = os.path.join(HERE, "run_all_checks.py")


def mutate_dtsx_duplicate_refid(text):
    match = re.search(r'<component refId="([^"]+)"', text)
    if not match:
        return None
    component = re.search(r"<component refId=\"%s\".*?</component>"
                          % re.escape(match.group(1)), text, re.DOTALL)
    return text.replace(component.group(0), component.group(0) * 2, 1)


def mutate_dtsx_dangling_path(text):
    return re.sub(r'(<path refId="[^"]+" endId=")[^"]+(")',
                  r"\1Package\\No Such Component.Inputs[Nowhere]\2", text, count=1)


def mutate_dtsx_root(text):
    text = re.sub(r"<DTS:Executable\b", "<DTS:Container", text, count=1)
    head, sep, tail = text.rpartition("</DTS:Executable>")
    return head + "</DTS:Container>" + tail if sep else None


def mutate_dtproj_deployment_model(text):
    return text.replace("<DeploymentModel>Project</DeploymentModel>",
                        "<DeploymentModel>Package</DeploymentModel>", 1)


def mutate_dtproj_manifest(text):
    return re.sub(r"<DeploymentModelSpecificContent>.*?</DeploymentModelSpecificContent>",
                  "<DeploymentModelSpecificContent />", text, count=1, flags=re.DOTALL)


def mutate_conmgr_drop_expression(text):
    return re.sub(r"\s*<DTS:PropertyExpression.*?</DTS:PropertyExpression>", "",
                  text, count=1, flags=re.DOTALL)


def mutate_conmgr_literal_token(text):
    return re.sub(r'(<DTS:ConnectionManager\s+DTS:ConnectionString=")[^"]*(")',
                  r"\1@[$Project::SqlServerHost]\2", text, count=1)


def mutate_conmgr_expression_password(text):
    """Put the credential back into an expression - the 0xC0017010 defect."""
    if "<DTS:ObjectData>" not in text:
        return None
    return text.replace(
        "  <DTS:ObjectData>",
        '  <DTS:PropertyExpression DTS:Name="Password">'
        '@[$Project::SqlServerPassword]</DTS:PropertyExpression>\n'
        "  <DTS:ObjectData>", 1)


def mutate_conmgr_expressed_connection_string(text):
    """Retarget the whole connection string - the live login failure.

    An OLE DB connection manager whose ConnectionString is expressed is
    rebuilt from that expression when the connection opens, which drops the
    password the catalog applied to CM.<connection>.Password, so every task
    fails with 'Login failed for user' and then
    DTS_E_CANNOTACQUIRECONNECTIONFROMCONNECTIONMANAGER.
    """
    match = re.search(r'(?s)\n[ \t]*<DTS:PropertyExpression DTS:Name="ServerName">.*?'
                      r'</DTS:PropertyExpression>', text)
    if not match or "MSOLEDBSQL19.1" not in text:
        return None
    return text.replace(
        match.group(0),
        '\n  <DTS:PropertyExpression DTS:Name="ConnectionString">'
        '"Data Source=" + @[$Project::SqlServerHost] + ";Provider=MSOLEDBSQL19.1;"'
        "</DTS:PropertyExpression>", 1)


def mutate_dtproj_drop_cm_password(text):
    """Remove a CM.<connection>.Password parameter from the project manifest."""
    match = re.search(
        r'(?s)\s*<SSIS:Parameter SSIS:Name="CM\.[^"]*\.Password">.*?</SSIS:Parameter>', text)
    if not match:
        return None
    return text.replace(match.group(0), "", 1)


def mutate_params_sensitive_parameter(text):
    """Re-declare a Sensitive project parameter, which would be Required and unbound."""
    if "</SSIS:Parameters>" not in text:
        return None
    return text.replace("</SSIS:Parameters>", """  <SSIS:Parameter SSIS:Name="SqlServerPassword">
    <SSIS:Properties>
      <SSIS:Property SSIS:Name="ID">{00000000-0000-0000-0000-000000000000}</SSIS:Property>
      <SSIS:Property SSIS:Name="CreationName"></SSIS:Property>
      <SSIS:Property SSIS:Name="Description">re-added by a negative fixture</SSIS:Property>
      <SSIS:Property SSIS:Name="IncludeInDebugDump">0</SSIS:Property>
      <SSIS:Property SSIS:Name="Required">1</SSIS:Property>
      <SSIS:Property SSIS:Name="Sensitive">1</SSIS:Property>
      <SSIS:Property SSIS:Name="DataType">18</SSIS:Property>
    </SSIS:Properties>
  </SSIS:Parameter>
</SSIS:Parameters>""", 1)


def mutate_conmgr_unspaced_tls(text):
    return text.replace("Trust Server Certificate=True;",
                        "TrustServerCertificate=True;", 1)


def mutate_dtsx_drop_directory_expression(text):
    """A Foreach loop left with only its design-time folder."""
    match = re.search(r'\n\s*<DTS:PropertyExpression DTS:Name="Directory">.*?'
                      r'</DTS:PropertyExpression>', text, re.S)
    if not match:
        return None
    return text.replace(match.group(0), "", 1)


def mutate_dtsx_unbound_flatfile(text):
    """A flat file manager pinned to whatever file existed at generation."""
    if "@[User::CurrentFilePath]</DTS:PropertyExpression>" not in text:
        return None
    return text.replace(
        '<DTS:PropertyExpression DTS:Name="ConnectionString">'
        '@[User::CurrentFilePath]</DTS:PropertyExpression>',
        '<DTS:PropertyExpression DTS:Name="ConnectionString">'
        '@[$Project::InboundFileRoot]</DTS:PropertyExpression>', 1)


def mutate_dtsx_file_system_attributes(text):
    """Spell FileSystemData the way the task host does not read."""
    if "TaskOperationType=" not in text:
        return None
    for serialised, ignored in (("TaskOperationType", "Operation"),
                               ("TaskSourcePath", "SourcePath"),
                               ("TaskIsSourceVariable", "IsSourcePathVariable"),
                               ("TaskDestinationPath", "DestinationPath"),
                               ("TaskIsDestinationVariable", "IsDestinationPathVariable")):
        text = text.replace(serialised + "=", ignored + "=")
    return text


def mutate_dtsx_file_task_validates_early(text):
    """Validate a path-variable File System Task at package start.

    The variables are still empty then, which is how executions 81-83 of
    Master_File_Ingestion failed on 'Variable "CurrentFilePath" is used as a
    source or destination and is empty.'
    """
    index = text.find("<FileSystemData")
    if index < 0:
        return None
    start = text.rfind("<DTS:Executable", 0, index)
    head = re.sub(r'\s*DTS:DelayValidation="True"', "", text[start:index], count=1)
    return text[:start] + head + text[index:]


def mutate_dtsx_drop_oledb_property(text):
    """Omit OpenRowsetVariable from an OLE DB source - the VS_ISCORRUPT defect."""
    return re.sub(r'\s*<property [^>]*name="OpenRowsetVariable"[^>]*>(?:</property>)?', "",
                  text, count=1) or None


def mutate_dtsx_drop_oledb_parameter_mapping(text):
    """Leave a ? in an OLE DB command with nothing bound to it."""
    return re.sub(r'(name="ParameterMapping">)[^<]+(</property>)', r"\1\2", text, count=1)


def mutate_dtsx_cm_id_by_dtsid(text):
    """Address a package connection manager by DTSID - the 0xC001001C defect."""
    match = re.search(r'<connection refId="[^"]+" connectionManagerID="'
                      r'(Package\.ConnectionManagers\[([^\]]+)\])"', text)
    if not match:
        return None
    dtsid = re.search(r'DTS:refId="Package.ConnectionManagers\[%s\]"\s+'
                      r'DTS:CreationName="[^"]*"\s+DTS:DTSID="([^"]+)"'
                      % re.escape(match.group(2)), text)
    if not dtsid:
        return None
    return text.replace('connectionManagerID="%s"' % match.group(1),
                        'connectionManagerID="%s"' % dtsid.group(1), 1)


def mutate_dtsx_cm_id_unknown(text):
    """Point a component at a connection manager nothing declares."""
    match = re.search(r'connectionManagerID="([^"]+)"', text)
    if not match:
        return None
    return text.replace('connectionManagerID="%s"' % match.group(1),
                        'connectionManagerID="{DEADBEEF-0000-0000-0000-000000000000}"', 1)


def mutate_xml_malformed(text):
    return text.replace("</DTS:Executable>", "</DTS:Executabl>", 1)


def mutate_sql_exec_expression(text):
    return text + ("\nEXEC etl.usp_LogRowCount\n"
                   "    @TargetRowCount = @InsertedCount + @UpdatedCount;\n")


def mutate_ps1_catalog_sql_auth(text):
    """Let a catalog write fall back to SQL authentication - the live defect."""
    if "-Database 'SSISDB' -Integrated" not in text:
        return None
    return text.replace("-Database 'SSISDB' -Integrated", "-Database 'SSISDB'", 1)


def mutate_ps1_unclosed_batch(text):
    """Drop the finally that closes the batch, leaving it Running after a fault."""
    match = re.search(r"(?ms)^finally \{.*?^\}\n", text)
    if not match or "Stop-EtlBatch" not in match.group(0):
        return None
    return text.replace(match.group(0), "", 1)


def mutate_ps1_missing_landing_assert(text):
    """Execute file ingestion without proving the landing zone is on the host."""
    match = re.search(r"(?m)^.*Assert-WwiExecutionHostLandingZone .*\n", text)
    if not match:
        return None
    return text.replace(match.group(0), "", 1)


def mutate_ps1_missing_auth_assert(text):
    """Open a batch before knowing the catalog will accept the connection."""
    match = re.search(r"(?m)^.*Assert-WwiCatalogWindowsAuthentication .*\n", text)
    if not match:
        return None
    return text.replace(match.group(0), "", 1)


def mutate_ps1_string_execution_parameter(text):
    """Bind every package parameter as text - the Int32 sql_variant defect."""
    if '$arguments["pvalue$index"] = $bound[$name]' not in text:
        return None
    return text.replace('$arguments["pvalue$index"] = $bound[$name]',
                        '$arguments["pvalue$index"] = [string] $bound[$name]', 1)


def mutate_ps1_hardcoded_adoption(text):
    """Pin @AllowAdoptRunning to 0, so an interrupted batch can never be rerun."""
    if "@AllowAdoptRunning = $adopt" not in text:
        return None
    return text.replace("@AllowAdoptRunning = $adopt", "@AllowAdoptRunning = 0", 1)


def mutate_ps1_bom_ssm_payload(text):
    """Write the SSM --parameters payload with a BOM the AWS CLI rejects."""
    match = re.search(r"(?m)^(\s*)\[System\.IO\.File\]::WriteAllText\(\$temporary,[^\n]*\n", text)
    if not match:
        return None
    return text.replace(
        match.group(0),
        "%sSet-Content -LiteralPath $temporary -Value $payload -Encoding utf8\n" % match.group(1), 1)


def mutate_dtsx_drop_parameter_binding(text):
    """Leave a ? unbound - the positional Execute SQL parameter defect."""
    match = re.search(r'(?s)<SQLTask:SqlTaskData[^>]*SQLTask:SqlStatementSource="[^"]*\?[^"]*".*?'
                      r'(<SQLTask:ParameterBinding [^>]*/>)', text)
    if not match:
        return None
    return text.replace(match.group(1), "", 1)


def mutate_dtsx_shift_parameter_index(text):
    """Bind the same statement's parameters to non-positional names."""
    match = re.search(r'<SQLTask:ParameterBinding SQLTask:ParameterName="0"', text)
    if not match:
        return None
    return text.replace(match.group(0),
                        '<SQLTask:ParameterBinding SQLTask:ParameterName="7"', 1)


def mutate_sql_duplicate_when_matched(text):
    match = re.search(r"(?s)\n\s*WHEN MATCHED THEN UPDATE SET.*?(?=\n\s*(?:WHEN\b|OUTPUT\b|;))",
                      text)
    if not match:
        return None
    return text.replace(match.group(0), match.group(0) * 2, 1)


def mutate_dtsx_multi_statement_expression(text):
    """String a second assignment onto an Expression Task, as execution 102 hit."""
    match = re.search(r'<ExpressionTask Expression="([^"]+)"', text)
    if not match:
        return None
    return text.replace(
        match.group(0),
        '<ExpressionTask Expression="%s; @[User::CurrentFileName] = &quot;x&quot;"'
        % match.group(1), 1)


def mutate_dtsx_unknown_source_column(text):
    """Select a column raw.FilePartnerSales does not have, as execution 105 did."""
    match = re.search(r'(SELECT\s+)(\w+)(,[^<(]*?FROM raw\.FilePartnerSales)', text)
    if not match:
        return None
    return text.replace(match.group(0),
                        "%sAmountText%s" % (match.group(1), match.group(3)), 1)


def mutate_dtsx_renamed_unknown_column(text):
    """Rename a column the table does not have, as STG_Load_PartnerSale did.

    The lookup that selected SourceCode AS CustomerRef out of ref.CodeCrosswalk
    loaded no metadata against the live server (0x80040E14, VS_ISBROKEN); an
    alias hides the missing column from a check that reads the select list
    verbatim.
    """
    if "ConformedCodeValue AS " not in text:
        return None
    return text.replace("ConformedCodeValue AS ", "TargetCode AS ", 1)


def mutate_dtsx_unknown_filter_column(text):
    """Filter on a column the table does not have, as STG_Load_PartnerSale did."""
    if "WHERE CodeDomainCode = " not in text:
        return None
    return text.replace("WHERE CodeDomainCode = ", "WHERE CodeSetName = ", 1)


def mutate_dtsx_drop_fastparse(text):
    """Drop FastParse from a flat file column, as execution 123 failed on."""
    if 'name="FastParse"' not in text:
        return None
    return re.sub(r'\s*<property [^>]*name="FastParse">[^<]*</property>', "", text, count=1)


def mutate_dtsx_drop_derived_error_output(text):
    """Leave a Derived Column with one output, as execution 126 failed on."""
    match = re.search(r'(?s)<component [^>]*componentClassID="Microsoft.DerivedColumn".*?</component>',
                      text)
    if not match:
        return None
    component = match.group(0)
    error_output = re.search(r'(?s)\s*<output [^>]*isErrorOut="true".*?</output>', component)
    if not error_output:
        return None
    return text.replace(component, component.replace(error_output.group(0), "", 1), 1)


def mutate_dtsx_default_output_as_case(text):
    """Write a conditional split's default output as another case."""
    if 'name="IsDefaultOut">true' not in text:
        return None
    return text.replace('name="IsDefaultOut">true', 'name="IsDefaultOut">false', 1)


def mutate_dtsx_directory_expression_on_container(text):
    """Move the Directory expression onto the loop, where it is never applied.

    This is how etl.FileIngestionLog came to record
    C:\\$WINRE_BACKUP_PARTITION.MARKER as a feed file.
    """
    match = re.search(r'(?s)(<DTS:ForEachEnumerator\b.*?>)(\s*<DTS:PropertyExpression '
                      r'DTS:Name="Directory">.*?</DTS:PropertyExpression>)', text)
    if not match:
        return None
    return text.replace(match.group(0), match.group(1), 1).replace(
        "<DTS:ForEachEnumerator", match.group(2).strip() + "\n      <DTS:ForEachEnumerator", 1)


def mutate_dtsx_single_row_without_aggregate(text):
    """Read a table directly into a single row result set, as execution 124 did."""
    match = re.search(r'SQLTask:SqlStatementSource="SELECT ISNULL\(MAX\((\w+)\), 0\) AS (\w+)'
                      r' *FROM +([\w\.]+)([^"]*)"', text)
    if not match:
        return None
    return text.replace(
        match.group(0),
        'SQLTask:SqlStatementSource="SELECT %s AS %s FROM %s%s"'
        % (match.group(1), match.group(2), match.group(3), match.group(4)), 1)


def mutate_dtsx_buffer_shaped_destination(text):
    """Publish a buffer column as external metadata, as executions 144-147 did.

    The destination then advertises a column its table does not have, which is
    what SSIS answers VS_NEEDSNEWMETADATA to when it revalidates at run time.
    """
    match = re.search(r'(<externalMetadataColumn refId="[^"]*\.Inputs\[[^"]*ExternalColumns\[)(\w+)'
                      r'(\][^>]*name=")(\w+)(" />)', text)
    if not match:
        return None
    return text.replace(
        match.group(0),
        "%sRecordType%sRecordType%s" % (match.group(1), match.group(3), match.group(5)), 1)


def mutate_dtsx_uncached_input_column(text):
    """Cache only a name on a replaced Derived Column input.

    The component then has nothing to compare against the buffer and answers
    VS_NEEDSNEWMETADATA at validation - 'does not have a valid cache' - which
    is how the live STG_Load_PartnerSale validation failed.
    """
    match = re.search(r'<inputColumn ([^>]*?)usageType="readWrite"', text)
    if not match or "cachedDataType" not in match.group(1):
        return None
    stripped = re.sub(r'cached(?!Name)\w+="[^"]*" ', "", match.group(1))
    return text.replace(match.group(0),
                        '<inputColumn %susageType="readWrite"' % stripped, 1)


def mutate_dtsx_undisposed_written_column(text):
    """Drop the dispositions from a written Derived Column input.

    The computed column then validates as VS_ISCORRUPT - 'has an invalid error
    or truncation row disposition' - which is how the live STG_Load_PartnerSale
    validation failed once its cache was repaired.
    """
    match = re.search(r'<inputColumn [^>]*?errorRowDisposition="[^"]*"[^>]*?'
                      r'usageType="readWrite"[^>]*?>', text)
    if not match:
        return None
    stripped = re.sub(r'(errorOrTruncationOperation|errorRowDisposition|'
                      r'truncationRowDisposition)="[^"]*" ', "", match.group(0))
    return text.replace(match.group(0), stripped, 1)


def mutate_dtsx_undisposed_lookup_output(text):
    """Drop the dispositions from a column a lookup copies out of its reference.

    The column then names a copy whose failure has no consequence and the
    lookup validates as VS_ISCORRUPT - 'has an invalid error or truncation row
    disposition' - which is how the live STG_Load_Currency validation failed
    once its property set was complete.
    """
    match = re.search(r'<outputColumn [^>]*?errorOrTruncationOperation="Copy Column"'
                      r'[^>]*?truncationRowDisposition="[^"]*"[^>]*?>', text)
    if not match:
        return None
    stripped = re.sub(r'(errorOrTruncationOperation|'
                      r'truncationRowDisposition)="[^"]*" ?', "", match.group(0))
    return text.replace(match.group(0), stripped, 1)


def mutate_dtsx_attributed_lookup_copy(text):
    """Name the copied reference column in an attribute instead of a property.

    The lookup reads the copy off the output column's CopyFromReferenceColumn
    property and never off an attribute, so a column written this way copies
    nothing and the component fails validation with 0xC0010009 - which is how
    the live STG_Load_Currency and STG_Load_PartnerSale validations failed.
    """
    match = re.search(r'\s*<properties>\s*<property [^>]*name="CopyFromReferenceColumn"[^>]*>'
                      r'([^<]*)</property>\s*</properties>', text)
    if not match:
        return None
    return text.replace(match.group(0),
                        ' copyFromReferenceColumn="%s"' % match.group(1), 1)


def mutate_dtsx_undeclared_lookup_property(text):
    """Drop a property the lookup gives itself from a lookup component.

    The runtime restores the component from what the package holds and fills
    nothing in, so the component fails with DTS_E_ELEMENTNOTFOUND (0xC0010009)
    as soon as it reads the property, whatever its value would have been. That
    is how the live STG_Load_Currency and STG_Load_PartnerSale validations
    failed on a catalog that matched the built packages byte for byte.
    """
    match = re.search(r'<property [^>]*name="MaxMemoryUsage64"[^>]*>[^<]*</property>\s*', text)
    if not match:
        return None
    return text.replace(match.group(0), "", 1)


def mutate_dtsx_unjoined_lookup_column(text):
    """Drop a lookup's join to its reference column.

    The component then has no relation to the reference set it queries and
    fails validation with 0xC0010009, which is how the live STG_Load_Currency
    validation failed.
    """
    match = re.search(r'<inputColumn [^>]*?joinToReferenceColumn="[^"]*"[^>]*?/>', text)
    if not match:
        return None
    return text.replace(
        match.group(0),
        re.sub(r'joinToReferenceColumn="[^"]*" ', "", match.group(0)), 1)


def _destination_debt_names():
    """The (data flow, destination) pairs the debt register already excuses."""
    path = os.path.join(REPO_ROOT, "ssis", "destination-metadata-debt.txt")
    pairs = set()
    if not os.path.exists(path):
        return pairs
    with open(path, errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#"):
                fields = line.split("|")
                if len(fields) >= 2:
                    pairs.add((fields[0].strip(), fields[1].strip()))
    return pairs


def mutate_dtsx_empty_destination_input(text):
    """Leave an OLE DB destination input with no columns at all.

    'The number of input columns for ... cannot be zero' is what SSIS reports
    for it, as VS_ISBROKEN, and is how the live STG_Load_Currency validation
    failed.
    """
    debt = _destination_debt_names()
    for match in re.finditer(r'(?s)(<input refId="([^"]*)\.Inputs\[OLE DB Destination Input\]"'
                             r'[^>]*>\s*<inputColumns>)(.*?)(</inputColumns>)', text):
        if "<inputColumn " not in match.group(3):
            continue
        parts = match.group(2).split("\\")
        if len(parts) >= 3 and (parts[1], parts[2]) in debt:
            continue  # a destination the register already excuses stays a warning
        return text.replace(match.group(0), match.group(1) + match.group(4), 1)
    return None


# (label, artifact glob root, extension, expected check, mutation)
FIXTURES = (
    ("duplicate pipeline refId", "ssis/07_dimensions", ".dtsx", "dtsx-pipeline",
     mutate_dtsx_duplicate_refid),
    ("data-flow path to nowhere", "ssis/07_dimensions", ".dtsx", "dtsx-pipeline",
     mutate_dtsx_dangling_path),
    ("wrong package root element", "ssis/00_orchestration", ".dtsx", "dtsx-structure",
     mutate_dtsx_root),
    ("package XML not well formed", "ssis/00_orchestration", ".dtsx", "xml-wellformed",
     mutate_xml_malformed),
    ("project not project-deployment", "ssis/04_staging", ".dtproj", "dtproj-structure",
     mutate_dtproj_deployment_model),
    ("project manifest missing", "ssis/04_staging", ".dtproj", "dtproj-structure",
     mutate_dtproj_manifest),
    ("connection manager cannot bind", "ssis/04_staging", ".conmgr", "conmgr-binding",
     mutate_conmgr_drop_expression),
    ("connection string left as a token", "ssis/04_staging", ".conmgr", "conmgr-binding",
     mutate_conmgr_literal_token),
    ("credential named by an expression", "ssis/04_staging", ".conmgr", "conmgr-credentials",
     mutate_conmgr_expression_password),
    ("whole connection string expressed", "ssis/04_staging", ".conmgr", "conmgr-binding",
     mutate_conmgr_expressed_connection_string),
    ("CM password parameter removed", "ssis/04_staging", ".dtproj", "catalog-binding",
     mutate_dtproj_drop_cm_password),
    ("sensitive project parameter re-added", "ssis/04_staging", ".params", "catalog-binding",
     mutate_params_sensitive_parameter),
    ("Foreach loop without a Directory expression", "ssis/03_file_ingestion", ".dtsx",
     "file-locality", mutate_dtsx_drop_directory_expression),
    ("flat file manager not bound to the loop variable", "ssis/03_file_ingestion", ".dtsx",
     "file-locality", mutate_dtsx_unbound_flatfile),
    ("File System Task the task host ignores", "ssis/03_file_ingestion", ".dtsx",
     "file-system-task", mutate_dtsx_file_system_attributes),
    ("File System Task validating before the loop runs", "ssis/03_file_ingestion", ".dtsx",
     "file-task-validation", mutate_dtsx_file_task_validates_early),
    ("OLE DB source without OpenRowsetVariable", "ssis/05_data_quality", ".dtsx",
     "oledb-source-properties", mutate_dtsx_drop_oledb_property),
    ("OLE DB marker with no ParameterMapping", "ssis/05_data_quality", ".dtsx",
     "oledb-source-properties", mutate_dtsx_drop_oledb_parameter_mapping),
    ("package connection addressed by DTSID", "ssis/03_file_ingestion", ".dtsx",
     "dtsx-connection-refs", mutate_dtsx_cm_id_by_dtsid),
    ("component connection nothing declares", "ssis/07_dimensions", ".dtsx",
     "dtsx-connection-refs", mutate_dtsx_cm_id_unknown),
    ("Execute SQL marker with no binding", "ssis/00_orchestration", ".dtsx",
     "execute-sql-parameters", mutate_dtsx_drop_parameter_binding),
    ("Execute SQL binding out of position", "ssis/00_orchestration", ".dtsx",
     "execute-sql-parameters", mutate_dtsx_shift_parameter_index),
    ("TLS keyword OLE DB 19 ignores", "ssis/04_staging", ".conmgr", "conmgr-credentials",
     mutate_conmgr_unspaced_tls),
    ("EXEC argument is an expression", "sqlserver/procedures/facts", ".sql",
     "sql-exec-arguments", mutate_sql_exec_expression),  # appended, so any file carries it
    ("MERGE with two WHEN MATCHED updates", "sqlserver/procedures/dimensions", ".sql",
     "sql-merge", mutate_sql_duplicate_when_matched),
    ("catalog write over SQL authentication", "deployment/ssis", ".ps1",
     "runtime-tooling", mutate_ps1_catalog_sql_auth),
    ("batch opened with no guaranteed close", "deployment/ssis", ".ps1",
     "runtime-tooling", mutate_ps1_unclosed_batch),
    ("file ingestion without a landing zone check", "deployment/ssis", ".ps1",
     "runtime-tooling", mutate_ps1_missing_landing_assert),
    ("batch opened before the catalog auth check", "deployment/ssis", ".ps1",
     "runtime-tooling", mutate_ps1_missing_auth_assert),
    ("package parameter bound as text", "deployment/ssis", ".ps1",
     "execution-parameters", mutate_ps1_string_execution_parameter),
    ("batch adoption hardcoded off", "deployment/ssis", ".ps1",
     "execution-parameters", mutate_ps1_hardcoded_adoption),
    ("SSM payload written with a BOM", "deployment/lib", ".ps1",
     "ssm-payload", mutate_ps1_bom_ssm_payload),
    ("Expression Task with two assignments", "ssis/03_file_ingestion", ".dtsx",
     "expression-task-statements", mutate_dtsx_multi_statement_expression),
    ("source selects a column the table lacks", "ssis/05_data_quality", ".dtsx",
     "sql-column-contract", mutate_dtsx_unknown_source_column),
    ("renamed source column the table lacks", "ssis/04_staging", ".dtsx",
     "sql-column-contract", mutate_dtsx_renamed_unknown_column),
    ("filter on a column the table lacks", "ssis/04_staging", ".dtsx",
     "sql-column-contract", mutate_dtsx_unknown_filter_column),
    ("flat file column without FastParse", "ssis/03_file_ingestion", ".dtsx",
     "component-contracts", mutate_dtsx_drop_fastparse),
    ("Derived Column without its error output", "ssis/05_data_quality", ".dtsx",
     "component-contracts", mutate_dtsx_drop_derived_error_output),
    ("conditional split default output as a case", "ssis/03_file_ingestion", ".dtsx",
     "component-contracts", mutate_dtsx_default_output_as_case),
    ("Directory expression on the loop container", "ssis/03_file_ingestion", ".dtsx",
     "foreach-file-enumerator", mutate_dtsx_directory_expression_on_container),
    ("single row result set over a table", "ssis/03_file_ingestion", ".dtsx",
     "single-row-result-set", mutate_dtsx_single_row_without_aggregate),
    ("destination metadata shaped by its buffer", "ssis/03_file_ingestion", ".dtsx",
     "destination-metadata", mutate_dtsx_buffer_shaped_destination),
    ("input column that caches only a name", "ssis/04_staging", ".dtsx",
     "input-column-cache", mutate_dtsx_uncached_input_column),
    ("destination input with no columns", "ssis/04_staging", ".dtsx",
     "destination-metadata", mutate_dtsx_empty_destination_input),
    ("written input column without dispositions", "ssis/04_staging", ".dtsx",
     "input-column-disposition", mutate_dtsx_undisposed_written_column),
    ("lookup key joined to no reference column", "ssis/04_staging", ".dtsx",
     "lookup-reference-mapping", mutate_dtsx_unjoined_lookup_column),
    ("lookup missing a property of its own set", "ssis/04_staging", ".dtsx",
     "component-property-set", mutate_dtsx_undeclared_lookup_property),
    ("copied lookup column without dispositions", "ssis/04_staging", ".dtsx",
     "lookup-reference-mapping", mutate_dtsx_undisposed_lookup_output),
    ("lookup copy named in an attribute", "ssis/04_staging", ".dtsx",
     "lookup-reference-mapping", mutate_dtsx_attributed_lookup_copy),
)


def pick_artifact(relative_dir, extension, mutation):
    """First artifact in relative_dir the mutation actually applies to."""
    directory = os.path.join(REPO_ROOT, relative_dir.replace("/", os.sep))
    for name in sorted(os.listdir(directory)):
        if not name.endswith(extension):
            continue
        path = os.path.join(directory, name)
        with open(path, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
        mutated = mutation(text)
        if mutated and mutated != text:
            return "%s/%s" % (relative_dir, name), text, mutated
    return None, None, None


def run_checker(root, prefix):
    result = subprocess.run(
        [sys.executable, CHECKER, "--json", "--path", prefix],
        cwd=REPO_ROOT, env=dict(os.environ, WWI_ESTATE_ROOT=root),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if result.returncode not in (0, 1) or not result.stdout.strip():
        raise RuntimeError("checker failed: %s" % (result.stderr.strip() or result.stdout.strip()))
    return json.loads(result.stdout)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    passed = 0
    failed = []
    for label, relative_dir, extension, expected, mutation in FIXTURES:
        relative, original, mutated = pick_artifact(relative_dir, extension, mutation)
        if relative is None:
            failed.append("%s: no artifact under %s could carry the defect"
                          % (label, relative_dir))
            continue
        scratch = tempfile.mkdtemp(prefix="wwi-negative-")
        try:
            target = os.path.join(scratch, relative.replace("/", os.sep))
            shutil.copytree(os.path.join(REPO_ROOT, relative_dir.replace("/", os.sep)),
                            os.path.dirname(target))
            with open(target, "w", encoding="utf-8", newline="") as handle:
                handle.write(mutated)
            report = run_checker(scratch, relative_dir)
            checks = {failure["check"] for failure in report["failures"]}
            if expected in checks:
                passed += 1
                print("PASS  %-38s %s detected %s" % (label, expected, relative))
                if args.verbose:
                    for failure in report["failures"]:
                        print("        %s: %s" % (failure["check"], failure["message"]))
            else:
                failed.append("%s: %s did not fire on %s (fired: %s)"
                              % (label, expected, relative, ", ".join(sorted(checks)) or "nothing"))
                print("FAIL  %-38s %s did not fire" % (label, expected))
            # The unmutated original must still pass the same check.
            with open(target, "w", encoding="utf-8", newline="") as handle:
                handle.write(original)
            clean = run_checker(scratch, relative_dir)
            if expected in {failure["check"] for failure in clean["failures"]}:
                failed.append("%s: %s also fires on the unmodified %s"
                              % (label, expected, relative))
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

    print("")
    print("%d/%d negative fixtures detected" % (passed, len(FIXTURES)))
    for message in failed:
        print("  %s" % message)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
