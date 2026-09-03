#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# SQL Server stage driver.
#
# Deploys the SQL Server side of the estate in the only order that works:
#
#   1. control     - etl.* framework tables, procedures and views (staging + DW)
#   2. oltp        - extract views and change-tracking extensions on WideWorldImporters
#   3. staging     - raw/work/stg/err/ref schemas and load procedures
#   4. reference   - shared reference data load procedures (staging)
#   5. warehouse   - dimensions, facts, aggregates and their load procedures
#   6. security    - roles, principals and grants (needs the schemas to exist)
#   7. agent       - msdb jobs (needs the SSIS folder name, not the packages)
#
# Connection details come from SQLSERVER_HOST, SQLSERVER_PORT, SQLSERVER_USER,
# SQLSERVER_PASSWORD, SQLSERVER_OLTP_DB, SQLSERVER_STAGING_DB and
# SQLSERVER_DW_DB. The password is passed to sqlcmd through SQLCMDPASSWORD so it
# does not appear in the process table.
#
# This script has not been executed against any SQL Server instance.
#
# Usage: deployment/sqlserver/deploy-sqlserver.sh [--dry-run] [--stage NAME] [--from N]
# ---------------------------------------------------------------------------

source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
WWI_LOG_PREFIX="wwi-deploy-sqlserver"

ONLY_STAGE=""
START_AT=1

while (( $# > 0 )); do
    case "$1" in
        --dry-run|--what-if) DRY_RUN=1; shift ;;
        --stage) ONLY_STAGE="${2:-}"; shift 2 ;;
        --from) START_AT="${2:-1}"; shift 2 ;;
        --help|-h) sed -n '2,26p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) wwi_fail "unknown argument '$1'." ;;
    esac
done

wwi_require_env SQLSERVER_HOST SQLSERVER_PORT SQLSERVER_USER SQLSERVER_PASSWORD \
                SQLSERVER_OLTP_DB SQLSERVER_STAGING_DB SQLSERVER_DW_DB
wwi_confirm_prod
wwi_require_tool sqlcmd "Install the mssql-tools package or Azure Data Studio's command-line tools."

REPO_ROOT="$(wwi_repo_root)"
ENV_CODE="$(wwi_environment_code)"
SERVER="${SQLSERVER_HOST},${SQLSERVER_PORT}"

# sqlcmd reads the password from this variable; it is never echoed.
export SQLCMDPASSWORD="${SQLSERVER_PASSWORD}"

# sqlcmd variables every script may reference. Values that are optional in the
# environment fall back to the estate's documented defaults.
SQLCMD_VARS=(
    "OltpDatabase=${SQLSERVER_OLTP_DB}"
    "StagingDatabase=${SQLSERVER_STAGING_DB}"
    "DwDatabase=${SQLSERVER_DW_DB}"
    "EnvironmentCode=${ENV_CODE}"
    "DomainPrefix=${WWI_DOMAIN_PREFIX:-CONTOSO}"
    "EtlServiceAccount=${WWI_ETL_SERVICE_ACCOUNT:-svc-wwi-etl}"
    "AppServiceAccount=${WWI_APP_SERVICE_ACCOUNT:-svc-wwi-app}"
    "ReportServiceAccount=${WWI_REPORT_SERVICE_ACCOUNT:-svc-wwi-report}"
    "SqlLoginSecret=${SQLSERVER_PASSWORD}"
    "OracleHost=${ORACLE_HOST:-}"
    "OraclePort=${ORACLE_PORT:-}"
    "OracleService=${ORACLE_SERVICE:-}"
    "OracleUser=${ORACLE_USER:-}"
    "OracleLinkSecret=${ORACLE_PASSWORD:-}"
    "SsisServer=${SSIS_SERVER:-${SQLSERVER_HOST}}"
    "SsisFolder=${SSIS_FOLDER:-WWI_${ENV_CODE}}"
    "SsisProxyAccount=${WWI_SSIS_PROXY_ACCOUNT:-svc-wwi-etl}"
    "FileProxyAccount=${WWI_FILE_PROXY_ACCOUNT:-svc-wwi-files}"
    "SsisProxySecret=${WWI_SSIS_PROXY_PASSWORD:-}"
    "FileProxySecret=${WWI_FILE_PROXY_PASSWORD:-}"
    "AgentLogRoot=${WWI_AGENT_LOG_ROOT:-D:\\WWI\\Logs\\Agent}"
    "InboundFileRoot=${ETL_INBOUND_FILE_ROOT:-D:\\WWI\\inbound}"
    "QuarantineFileRoot=${ETL_REJECT_FILE_ROOT:-D:\\WWI\\reject}"
    "EtlOperatorEmail=${WWI_ETL_OPERATOR_EMAIL:-wwi-etl-oncall@example.internal}"
    "DbaOperatorEmail=${WWI_DBA_OPERATOR_EMAIL:-wwi-dba@example.internal}"
    "FinanceOperatorEmail=${WWI_FINANCE_OPERATOR_EMAIL:-wwi-finance-systems@example.internal}"
)

build_var_args() {
    local out=()
    local kv
    for kv in "${SQLCMD_VARS[@]}"; do
        out+=(-v "${kv}")
    done
    printf '%s\0' "${out[@]}"
}

run_script() {
    local database="$1"
    local file="$2"
    local relative="${file#"${REPO_ROOT}/"}"

    if wwi_is_dry_run; then
        wwi_log "WHATIF sqlcmd -S ${SERVER} -d ${database} -i ${relative}"
        return 0
    fi

    wwi_log "RUN    [${database}] ${relative}"
    local var_args=()
    while IFS= read -r -d '' arg; do var_args+=("${arg}"); done < <(build_var_args)

    # -b makes sqlcmd exit non-zero on error, -I enables quoted identifiers.
    # -X is not passed: it suppresses the SQLCMDPASSWORD the login relies on.
    sqlcmd -S "${SERVER}" -U "${SQLSERVER_USER}" -d "${database}" \
           -b -I -j "${var_args[@]}" -i "${file}" \
        || wwi_fail "${relative} failed against ${database}."
}

# A script header may name the scripts it has to follow, as
# "Deploy order : after etl.usp_LogRejectedRecord.sql", and may use a wildcard
# for a whole family, as "after Dimension.*.sql". Name order alone puts several
# callers ahead of the procedures they call, which leaves the estate deployed
# but full of unresolved references.
emit_with_prerequisites() {
    local sql_file="$1"
    local base
    base="$(basename "${sql_file}")"

    case " ${EMITTED} " in *" ${base} "*) return 0 ;; esac
    case " ${VISITING} " in *" ${base} "*) return 0 ;; esac
    VISITING="${VISITING} ${base}"

    local header prerequisite prerequisite_path candidate
    header="$(sed -n '1,20p' "${sql_file}" \
            | awk '/Deploy order/ { inside = 1; print; next }
                   inside && /^[[:space:]]*[A-Za-z][A-Za-z ]*:/ { inside = 0 }
                   inside { print }' \
            | sed 's/[Bb]efore .*//')"

    # Half the warehouse scripts are named "Dimension.Buying Group.sql", so the
    # header is searched for each candidate's own name rather than tokenised on
    # whitespace, and wildcards are matched separately.
    for prerequisite_path in "${DIRECTORY_FILES[@]}"; do
        candidate="$(basename "${prerequisite_path}")"
        [[ "${candidate}" == "${base}" ]] && continue
        if [[ "${header}" == *"${candidate}"* ]]; then
            emit_with_prerequisites "${prerequisite_path}"
        fi
    done
    for prerequisite in $(printf '%s\n' "${header}" \
            | grep -o '[A-Za-z0-9_.]*[*][A-Za-z0-9_.*]*\.sql' | sort -u); do
        for prerequisite_path in "${DIRECTORY_FILES[@]}"; do
            candidate="$(basename "${prerequisite_path}")"
            [[ "${candidate}" == "${base}" ]] && continue
            # shellcheck disable=SC2053 - the pattern side holds the wildcard
            if [[ "${candidate}" == ${prerequisite} ]]; then
                emit_with_prerequisites "${prerequisite_path}"
            fi
        done
    done

    VISITING="${VISITING% ${base}}"
    EMITTED="${EMITTED} ${base}"
    run_script "${DATABASE}" "${sql_file}"
}

run_directory() {
    DATABASE="$1"
    local directory="$2"

    if [[ ! -d "${directory}" ]]; then
        wwi_warn "${directory#"${REPO_ROOT}/"} is not present; skipping."
        return 0
    fi

    DIRECTORY_FILES=()
    while IFS= read -r -d '' sql_file; do
        # The agent stage has its own :r driver; do not run its members twice.
        [[ "$(basename "${sql_file}")" == 90_install_all_agent_jobs.sql ]] && continue
        DIRECTORY_FILES+=("${sql_file}")
    done < <(find "${directory}" -maxdepth 2 -type f -name '*.sql' -print0 | sort -z)

    EMITTED=""
    VISITING=""
    local file
    for file in "${DIRECTORY_FILES[@]}"; do
        emit_with_prerequisites "${file}"
    done
}

stage_control() {
    wwi_log "--- stage 1: control framework ---"
    run_directory "${SQLSERVER_STAGING_DB}" "${REPO_ROOT}/sqlserver/control"
    # The control framework is deployed twice on purpose: the warehouse keeps
    # its own etl.* copy so a staging outage cannot stop a close reconciliation.
    run_directory "${SQLSERVER_DW_DB}" "${REPO_ROOT}/sqlserver/control"
}

stage_oltp()      { wwi_log "--- stage 2: OLTP extensions ---"; run_directory "${SQLSERVER_OLTP_DB}"    "${REPO_ROOT}/sqlserver/oltp"; }
stage_staging()   { wwi_log "--- stage 3: staging ---";         run_directory "${SQLSERVER_STAGING_DB}" "${REPO_ROOT}/sqlserver/staging"; }

stage_reference() { wwi_log "--- stage 4: reference data ---";  run_directory "${SQLSERVER_STAGING_DB}" "${REPO_ROOT}/sqlserver/reference"; }

stage_warehouse() {
    wwi_log "--- stage 5: warehouse ---"
    run_directory "${SQLSERVER_DW_DB}" "${REPO_ROOT}/sqlserver/warehouse/dimensions"
    run_directory "${SQLSERVER_DW_DB}" "${REPO_ROOT}/sqlserver/warehouse/facts"
    run_directory "${SQLSERVER_DW_DB}" "${REPO_ROOT}/sqlserver/warehouse/aggregates"
    run_directory "${SQLSERVER_DW_DB}" "${REPO_ROOT}/sqlserver/procedures/dimensions"
    run_directory "${SQLSERVER_DW_DB}" "${REPO_ROOT}/sqlserver/procedures/facts"
    run_directory "${SQLSERVER_DW_DB}" "${REPO_ROOT}/sqlserver/views"
}

stage_security() {
    wwi_log "--- stage 6: security ---"
    # Every security script sets its own database with USE, so it is submitted
    # against master.
    run_directory "master" "${REPO_ROOT}/sqlserver/security"
}

stage_agent() {
    wwi_log "--- stage 7: agent ---"
    run_directory "msdb" "${REPO_ROOT}/sqlserver/agent"
}

STAGE_NAMES=(control oltp staging reference warehouse security agent)
STAGE_INDEX=0

for stage in "${STAGE_NAMES[@]}"; do
    STAGE_INDEX=$(( STAGE_INDEX + 1 ))
    [[ -n "${ONLY_STAGE}" && "${ONLY_STAGE}" != "${stage}" ]] && continue
    (( STAGE_INDEX < START_AT )) && continue
    "stage_${stage}"
done

wwi_log "sql server stage complete$(wwi_is_dry_run && printf ' (dry run)' || true)"
