#!/usr/bin/env python3
"""Render SSIS catalogue environment scripts from the per-environment YAML.

`config/environments/<env>.env.yaml` is the source of truth for what a DEV,
TEST or PROD deployment looks like. The SSIS catalogue, however, is configured
through `SSISDB.catalog.*` procedure calls, and the Windows deploy hosts the
platform team uses have sqlcmd but not always Python. So the SQL is rendered
here and committed under `deployment/ssis/environments/`, and the deploy scripts
run the committed file.

Re-run this whenever an environment YAML changes:

    python3 deployment/ssis/render_environment_sql.py --all

Nothing in this file connects to a database; it reads YAML and writes SQL.
"""

from __future__ import annotations

import argparse
import os
import sys

try:
    import yaml
except ImportError:  # pragma: no cover - the message is the useful part
    sys.stderr.write("PyYAML is required: pip install pyyaml\n")
    raise

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIG_DIR = os.path.join(REPO_ROOT, "config", "environments")
OUTPUT_DIR = os.path.join(REPO_ROOT, "deployment", "ssis", "environments")
SSIS_DIR = os.path.join(REPO_ROOT, "ssis")

ENVIRONMENTS = ("dev", "test", "prod")

HEADER = """\
/*
    Object          : SSIS catalogue environment {environment_name}
    Deploy target   : SSISDB on the SSIS catalogue instance
    Deploy order    : after deployment/ssis/Deploy-SsisCatalog.ps1
    Called by       : deployment/ssis/Deploy-SsisEnvironment.ps1
    Notes           : GENERATED FILE - do not edit by hand. Rendered from
                      config/environments/{source_name} by
                      deployment/ssis/render_environment_sql.py.

                      Creates the environment, its variables and the project
                      parameter references, then binds every reference. Values
                      marked sensitive are passed in as sqlcmd variables and
                      are never stored in this repository.

                      Idempotent. Not executed against any catalogue.

    sqlcmd variables required: {secret_vars}
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE SSISDB;
GO

DECLARE @FolderName      NVARCHAR(128) = N'{folder}';
DECLARE @EnvironmentName NVARCHAR(128) = N'{environment_name}';

IF NOT EXISTS (SELECT 1 FROM catalog.folders WHERE name = @FolderName)
    EXEC catalog.create_folder @folder_name = @FolderName;

IF NOT EXISTS (SELECT 1
               FROM catalog.environments AS e
               INNER JOIN catalog.folders AS f ON f.folder_id = e.folder_id
               WHERE e.name = @EnvironmentName AND f.name = @FolderName)
BEGIN
    EXEC catalog.create_environment
         @folder_name      = @FolderName,
         @environment_name = @EnvironmentName,
         @environment_description = N'{description}';
END
GO
"""

VARIABLE_TEMPLATE = """\
/* {parameter} ({type}{sensitive_note}) <- {env_var} */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'{parameter}' AND e.name = N'{environment_name}' AND f.name = N'{folder}')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'{folder}', @environment_name = N'{environment_name}',
         @variable_name = N'{parameter}';
END

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'{folder}',
     @environment_name = N'{environment_name}',
     @variable_name    = N'{parameter}',
     @data_type        = N'{type}',
     @sensitive        = {sensitive_flag},
     @value            = {value_expression},
     @description      = N'Bound to project parameter {parameter}. Source: {env_var}.';
GO
"""

REFERENCE_TEMPLATE = """\
/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'{project}' AND f.name = N'{folder}'
                 AND r.environment_name = N'{environment_name}')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'{folder}',
         @project_name      = N'{project}',
         @environment_name  = N'{environment_name}',
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO
"""

BINDING_TEMPLATE = """\
EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'{folder}',
     @project_name   = N'{project}',
     @parameter_name = N'{parameter}',
     @parameter_value = N'{parameter}',
     @value_type     = 'R';                    /* referenced environment variable */
GO
"""

FOOTER = """\
/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'{folder}'
  AND pr.name = N'{project}'
  AND p.object_type = 20
ORDER BY p.parameter_name;
GO
"""


def discovered_projects():
    """Every area project under ssis/, which is what gets deployed to the folder."""
    names = []
    for entry in sorted(os.listdir(SSIS_DIR)):
        area = os.path.join(SSIS_DIR, entry)
        if not os.path.isdir(area):
            continue
        for candidate in sorted(os.listdir(area)):
            if candidate.endswith(".dtproj"):
                names.append(candidate[: -len(".dtproj")])
    return names


def load_environment(code):
    path = os.path.join(CONFIG_DIR, "%s.env.yaml" % code)
    with open(path) as handle:
        return yaml.safe_load(handle), os.path.basename(path)


def sql_literal(value, data_type):
    """Render a YAML value as a SQL literal for create_environment_variable."""
    if data_type == "Int32":
        return "%d" % int(value)
    text = str(value).replace("'", "''")
    return "N'%s'" % text


def render(code):
    document, source_name = load_environment(code)
    environment = document["environment"]
    variables = document["ssis_environment_variables"]

    folder = environment["ssis_folder"]
    # The estate deploys one project per area; the environment is created once
    # and referenced by all of them.
    projects = environment.get("ssis_projects") or discovered_projects()
    environment_name = environment["ssis_environment_name"]
    description = " ".join(environment["description"].split()).replace("'", "''")

    secrets = [v["env_var"] for v in variables if v.get("secret")]

    parts = [HEADER.format(environment_name=environment_name,
                           source_name=source_name,
                           folder=folder,
                           description=description,
                           secret_vars=", ".join(secrets) or "none")]

    for variable in variables:
        sensitive = bool(variable.get("secret"))
        if sensitive:
            # The value arrives as a sqlcmd variable named after the parameter,
            # supplied by the deploy driver from the environment variable.
            value_expression = "N'$(%s)'" % variable["parameter"]
        else:
            value_expression = sql_literal(variable["value"], variable["type"])

        parts.append(VARIABLE_TEMPLATE.format(
            parameter=variable["parameter"],
            type=variable["type"],
            env_var=variable["env_var"],
            sensitive_note=", sensitive" if sensitive else "",
            sensitive_flag=1 if sensitive else 0,
            value_expression=value_expression,
            environment_name=environment_name,
            folder=folder))

    for project in projects:
        parts.append(REFERENCE_TEMPLATE.format(folder=folder, project=project,
                                               environment_name=environment_name))

        for variable in variables:
            parts.append(BINDING_TEMPLATE.format(folder=folder, project=project,
                                                 parameter=variable["parameter"]))

        parts.append(FOOTER.format(folder=folder, project=project))
    return "\n".join(parts)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--environment", choices=ENVIRONMENTS,
                        help="render a single environment")
    parser.add_argument("--all", action="store_true", help="render every environment")
    parser.add_argument("--stdout", action="store_true",
                        help="write to stdout instead of deployment/ssis/environments/")
    args = parser.parse_args()

    if not args.all and not args.environment:
        parser.error("pass --environment CODE or --all")

    codes = ENVIRONMENTS if args.all else (args.environment,)

    if not args.stdout and not os.path.isdir(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    for code in codes:
        text = render(code)
        if args.stdout:
            sys.stdout.write(text)
            continue
        target = os.path.join(OUTPUT_DIR, "%s_environment.sql" % code)
        with open(target, "w") as handle:
            handle.write(text)
        sys.stdout.write("wrote %s\n" % os.path.relpath(target, REPO_ROOT))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
