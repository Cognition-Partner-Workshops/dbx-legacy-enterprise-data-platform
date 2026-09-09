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
    """Put the credential back into the expression - the 0xC0017010 defect."""
    if '+ ";" : "Integrated Security=SSPI;"' not in text:
        return None
    return text.replace('+ ";" : "Integrated Security=SSPI;"',
                        '+ ";Password=" + @[$Project::SqlServerPassword] + ";" '
                        ': "Integrated Security=SSPI;"', 1)


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


def mutate_sql_duplicate_when_matched(text):
    match = re.search(r"(?s)\n\s*WHEN MATCHED THEN UPDATE SET.*?(?=\n\s*(?:WHEN\b|OUTPUT\b|;))",
                      text)
    if not match:
        return None
    return text.replace(match.group(0), match.group(0) * 2, 1)


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
    ("CM password parameter removed", "ssis/04_staging", ".dtproj", "catalog-binding",
     mutate_dtproj_drop_cm_password),
    ("sensitive project parameter re-added", "ssis/04_staging", ".params", "catalog-binding",
     mutate_params_sensitive_parameter),
    ("Foreach loop without a Directory expression", "ssis/03_file_ingestion", ".dtsx",
     "file-locality", mutate_dtsx_drop_directory_expression),
    ("flat file manager not bound to the loop variable", "ssis/03_file_ingestion", ".dtsx",
     "file-locality", mutate_dtsx_unbound_flatfile),
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
    if result.returncode not in (0, 1):
        raise RuntimeError("checker failed: %s" % result.stderr.strip())
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
