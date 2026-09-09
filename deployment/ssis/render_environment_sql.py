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

                      Every value arrives as a sqlcmd variable, supplied by the
                      deploy driver from the environment variable named in the
                      comment above it, falling back to the YAML default. No
                      host, account or credential value is baked into this file.

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

DECLARE @Value {sql_type} = {value_expression};
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'{folder}',
     @environment_name = N'{environment_name}',
     @variable_name    = N'{parameter}',
     @data_type        = N'{type}',
     @sensitive        = {sensitive_flag},
     @value            = @Value,
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
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO
"""

BINDING_TEMPLATE = """\
/* {target} <- environment variable {parameter} */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'{folder}' AND pr.name = N'{project}'
             AND op.object_type = 20 AND op.parameter_name = N'{target}')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'{folder}',
         @project_name   = N'{project}',
         @parameter_name = N'{target}',
         @parameter_value = N'{parameter}',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO
"""

POSTCONDITION = """\
/* ---------------------------------------------------------------------------
   Post-conditions. These are the reason this script is worth running twice:
   a binding that silently did not take looks exactly like a working one until
   an execution fails with an empty password.
   --------------------------------------------------------------------------- */

/* 1. Every reference resolves inside this folder - that is what local means,
      whatever letter the catalog chose to record for reference_type. */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_references AS r
           INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
           INNER JOIN SSISDB.catalog.folders  AS f ON f.folder_id  = p.folder_id
           WHERE f.name = N'{folder}'
             AND ISNULL(r.environment_folder_name, N'{folder}') <> N'{folder}')
    THROW 50001, 'An environment reference in this folder points outside it.', 1;

/* 2. Every project in the folder has exactly one reference to the environment. */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.projects AS p
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
           LEFT JOIN SSISDB.catalog.environment_references AS r
                  ON r.project_id = p.project_id
                 AND r.environment_name = N'{environment_name}'
           WHERE f.name = N'{folder}'
           GROUP BY p.project_id
           HAVING COUNT(r.reference_id) <> 1)
    THROW 50002, 'A project in this folder does not have exactly one reference to the environment.', 1;

/* 3. No sensitive parameter carries a literal value. */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f ON f.folder_id  = p.folder_id
           WHERE f.name = N'{folder}' AND op.sensitive = 1 AND op.value_type = 'V')
    THROW 50003, 'A sensitive parameter holds a literal value instead of an environment reference.', 1;

/* 4. Every referenced variable actually exists in the environment. */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f ON f.folder_id  = p.folder_id
           WHERE f.name = N'{folder}' AND op.value_type = 'R'
             AND NOT EXISTS (SELECT 1
                             FROM SSISDB.catalog.environment_variables AS v
                             INNER JOIN SSISDB.catalog.environments AS e
                                     ON e.environment_id = v.environment_id
                             INNER JOIN SSISDB.catalog.folders AS ef
                                     ON ef.folder_id = e.folder_id
                             WHERE ef.name = N'{folder}'
                               AND e.name = N'{environment_name}'
                               AND v.name = op.referenced_variable_name))
    THROW 50004, 'A parameter references an environment variable that does not exist.', 1;

/* 5. Every CM.<connection>.Password in the folder is bound by reference. */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f ON f.folder_id  = p.folder_id
           WHERE f.name = N'{folder}' AND op.object_type = 20
             AND op.parameter_name LIKE 'CM.%.Password'
             AND op.value_type <> 'R')
    THROW 50005, 'A connection manager password parameter is not bound to the environment.', 1;

SELECT p.name          AS project_name,
       op.parameter_name,
       op.value_type,
       op.sensitive,
       op.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS op
INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = op.project_id
INNER JOIN SSISDB.catalog.folders  AS f ON f.folder_id  = p.folder_id
WHERE f.name = N'{folder}'
  AND op.object_type = 20
ORDER BY p.name, op.parameter_name;
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
    if data_type == "Boolean":
        return "1" if value in (True, "true", "True", 1) else "0"
    text = str(value).replace("'", "''")
    return "N'%s'" % text


SQL_TYPES = {
    "Int32": "INT",
    "Boolean": "BIT",
    "String": "NVARCHAR(4000)",
}


def sql_type(variable):
    """The local declared for the value, so the catalog gets a typed sql_variant."""
    return SQL_TYPES.get(variable["type"], "NVARCHAR(4000)")


def value_expression(variable):
    """Every value is a substituted token, so nothing about the estate is committed.

    The deploy driver resolves each one from its declared environment variable
    and falls back to the YAML default, which keeps real host names and account
    names out of the repository while leaving the file readable. The value is
    assigned to a typed local rather than passed inline: create_environment_variable
    is a procedure, and a procedure argument cannot be an expression.
    """
    return "N'$(%s)'" % variable["parameter"]


def binding_targets(variable):
    """The project parameters this environment variable is bound to."""
    return list(variable.get("binds_to") or [variable["parameter"]])


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
        parts.append(VARIABLE_TEMPLATE.format(
            parameter=variable["parameter"],
            type=variable["type"],
            env_var=variable["env_var"],
            sensitive_note=", sensitive" if sensitive else "",
            sensitive_flag=1 if sensitive else 0,
            sql_type=sql_type(variable),
            value_expression=value_expression(variable),
            environment_name=environment_name,
            folder=folder))

    for project in projects:
        parts.append(REFERENCE_TEMPLATE.format(folder=folder, project=project,
                                               environment_name=environment_name))

        for variable in variables:
            for target in binding_targets(variable):
                parts.append(BINDING_TEMPLATE.format(folder=folder, project=project,
                                                     parameter=variable["parameter"],
                                                     target=target))

    parts.append(POSTCONDITION.format(folder=folder, environment_name=environment_name))
    return "\n".join(parts)


def render_variable_map(code):
    """The sqlcmd variable table the deploy driver reads.

    Every environment value is now a sqlcmd variable, so the driver needs to
    know, without Python and without the YAML parser it does not have, which
    environment variable supplies each one and what to fall back to. Secrets
    appear here as a name and a source only - never a value.
    """
    document, source_name = load_environment(code)
    lines = [
        "# GENERATED FILE - do not edit by hand. Rendered from",
        "# config/environments/%s by deployment/ssis/render_environment_sql.py." % source_name,
        "#",
        "# sqlcmd variable -> (environment variable, default). A Secret entry has no",
        "# default: the deploy fails if its environment variable is unset.",
        "@{",
    ]
    for variable in document["ssis_environment_variables"]:
        secret = bool(variable.get("secret"))
        if secret:
            default = "$null"
        elif variable["type"] == "Boolean":
            default = "'%s'" % ("1" if variable["value"] in (True, "true", 1) else "0")
        else:
            default = "'%s'" % str(variable["value"]).replace("'", "''")
        lines.append("    '%s' = @{ EnvVar = '%s'; Default = %s; Secret = $%s }"
                     % (variable["parameter"], variable["env_var"], default,
                        "true" if secret else "false"))
    lines.append("}")
    return "\n".join(lines) + "\n"


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

        variables = os.path.join(OUTPUT_DIR, "%s_environment.vars.psd1" % code)
        with open(variables, "w") as handle:
            handle.write(render_variable_map(code))
        sys.stdout.write("wrote %s\n" % os.path.relpath(variables, REPO_ROOT))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
