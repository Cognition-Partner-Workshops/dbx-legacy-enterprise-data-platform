#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# SSIS stage driver, Linux/container variant.
#
# The SSIS project itself can only be *built* on a Windows host with the
# Integration Services tooling, so this script covers the two parts that do not
# need it:
#
#   * deploying every pre-built .ispac through SSISDB.catalog.deploy_project,
#     one catalogue project per area project;
#   * creating the catalogue environment and binding parameters, from the
#     rendered SQL in deployment/ssis/environments/.
#
# For the build itself, use deployment/ssis/Build-SsisProject.ps1 on a Windows
# deploy host. This script refuses to guess.
#
# Nothing here has been executed against an SSIS catalogue.
#
# Usage: deployment/ssis/deploy-ssis.sh [--dry-run] [--ispac PATH]... [--skip-project]
# ---------------------------------------------------------------------------

source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
WWI_LOG_PREFIX="wwi-deploy-ssis"

ISPAC_PATHS=()
SKIP_PROJECT=0

while (( $# > 0 )); do
    case "$1" in
        --dry-run|--what-if) DRY_RUN=1; shift ;;
        --ispac) ISPAC_PATHS+=("${2:-}"); shift 2 ;;
        --skip-project) SKIP_PROJECT=1; shift ;;
        --help|-h) sed -n '2,19p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) wwi_fail "unknown argument '$1'." ;;
    esac
done

wwi_require_env SSIS_SERVER SSIS_FOLDER SQLSERVER_USER SQLSERVER_PASSWORD \
                ORACLE_PASSWORD
wwi_confirm_prod
wwi_require_tool sqlcmd "Install the mssql-tools package."

REPO_ROOT="$(wwi_repo_root)"
ENV_CODE="$(wwi_environment_code)"
ENV_LOWER="$(printf '%s' "${ENV_CODE}" | tr '[:upper:]' '[:lower:]')"
ENV_SQL="${REPO_ROOT}/deployment/ssis/environments/${ENV_LOWER}_environment.sql"

export SQLCMDPASSWORD="${SQLSERVER_PASSWORD}"

if (( ${#ISPAC_PATHS[@]} == 0 )); then
    while IFS= read -r artifact; do
        ISPAC_PATHS+=("${artifact}")
    done < <(find "${REPO_ROOT}/artifacts" -maxdepth 1 -name '*.ispac' -type f 2>/dev/null | sort)
fi

# --- folder --------------------------------------------------------------

FOLDER_SQL="IF NOT EXISTS (SELECT 1 FROM SSISDB.catalog.folders WHERE name = N'${SSIS_FOLDER}')
    EXEC SSISDB.catalog.create_folder @folder_name = N'${SSIS_FOLDER}';"

if wwi_is_dry_run; then
    wwi_log "WHATIF ensure catalogue folder [${SSIS_FOLDER}] on ${SSIS_SERVER}"
else
    wwi_log "ensuring catalogue folder [${SSIS_FOLDER}] exists"
    sqlcmd -S "${SSIS_SERVER}" -U "${SQLSERVER_USER}" -b -I -X1 -Q "${FOLDER_SQL}" \
        || wwi_fail "could not create or verify catalogue folder [${SSIS_FOLDER}]."
fi

# --- project -------------------------------------------------------------

if (( SKIP_PROJECT == 0 )); then
    if (( ${#ISPAC_PATHS[@]} == 0 )) && ! wwi_is_dry_run; then
        wwi_fail "no .ispac files in ${REPO_ROOT}/artifacts. Build them on a Windows host with deployment/ssis/Build-SsisProject.ps1, or pass --skip-project."
    fi

    for ispac in "${ISPAC_PATHS[@]}"; do
        project_name="$(basename "${ispac}" .ispac)"

        if [[ ! -f "${ispac}" ]] && ! wwi_is_dry_run; then
            wwi_fail "${ispac} does not exist. Build it on a Windows host with deployment/ssis/Build-SsisProject.ps1."
        fi

        # OPENROWSET reads the file from the SQL Server host, not from here, so
        # the path must be one the database engine's service account can see. On
        # the estate's deploy runbook that means copying the .ispac to the
        # server first.
        DEPLOY_SQL="DECLARE @ProjectBinary VARBINARY(MAX);
DECLARE @OperationId BIGINT;
SELECT @ProjectBinary = CAST(BulkColumn AS VARBINARY(MAX))
FROM OPENROWSET(BULK N'${ispac}', SINGLE_BLOB) AS ProjectFile;
EXEC SSISDB.catalog.deploy_project
     @folder_name = N'${SSIS_FOLDER}',
     @project_name = N'${project_name}',
     @project_stream = @ProjectBinary,
     @operation_id = @OperationId OUTPUT;
SELECT m.message_time, m.message
FROM SSISDB.catalog.operation_messages AS m
WHERE m.operation_id = @OperationId AND m.message_type IN (120, 130)
ORDER BY m.message_time;"

        if wwi_is_dry_run; then
            wwi_log "WHATIF deploy ${ispac} to /SSISDB/${SSIS_FOLDER}/${project_name}"
        else
            wwi_log "deploying ${project_name} to /SSISDB/${SSIS_FOLDER}"
            sqlcmd -S "${SSIS_SERVER}" -U "${SQLSERVER_USER}" -b -I -X1 -Q "${DEPLOY_SQL}" \
                || wwi_fail "catalog.deploy_project reported a failure for ${project_name}."
        fi
    done
else
    wwi_log "project deployment skipped by request"
fi

# --- environment ---------------------------------------------------------

[[ -f "${ENV_SQL}" ]] || wwi_fail "${ENV_SQL#"${REPO_ROOT}/"} is missing. Run: python3 deployment/ssis/render_environment_sql.py --all"

if wwi_is_dry_run; then
    wwi_log "WHATIF apply deployment/ssis/environments/${ENV_LOWER}_environment.sql"
    wwi_log "WHATIF sensitive parameters supplied from ORACLE_PASSWORD and SQLSERVER_PASSWORD"
else
    wwi_log "applying SSIS environment for ${ENV_CODE}"
    # The two -v arguments carry secrets; they are never logged.
    sqlcmd -S "${SSIS_SERVER}" -U "${SQLSERVER_USER}" -b -I -X1 -j \
           -v "OraclePassword=${ORACLE_PASSWORD}" \
           -v "SqlServerPassword=${SQLSERVER_PASSWORD}" \
           -i "${ENV_SQL}" \
        || wwi_fail "the ${ENV_CODE} SSIS environment script failed."
fi

wwi_log "ssis stage complete$(wwi_is_dry_run && printf ' (dry run)' || true)"
