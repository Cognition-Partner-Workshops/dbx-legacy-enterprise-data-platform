#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Preflight checks for a WWI estate deployment.
#
# Checks the things that are knowable without contacting a server: required
# tooling, required environment variables, repository layout, landing-zone
# directories and the presence of the per-environment configuration files.
#
# It deliberately does NOT open a connection to Oracle, SQL Server or the SSIS
# catalogue. Connectivity is the operator's responsibility and is confirmed
# outside this repository. Nothing here has been executed against a server.
#
# Usage: deployment/preflight/preflight.sh [--stage oracle|sqlserver|ssis|all]
# Exit codes: 0 all checks passed, 1 one or more checks failed.
# ---------------------------------------------------------------------------

source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
WWI_LOG_PREFIX="wwi-preflight"

STAGE="all"
while (( $# > 0 )); do
    case "$1" in
        --stage) STAGE="${2:-}"; shift 2 ;;
        --help|-h) sed -n '2,20p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) wwi_fail "unknown argument '$1'." ;;
    esac
done

REPO_ROOT="$(wwi_repo_root)"
FAILURES=0
CHECKS=0

check() {
    local description="$1"; shift
    CHECKS=$(( CHECKS + 1 ))
    if "$@"; then
        printf '  PASS  %s\n' "${description}"
    else
        printf '  FAIL  %s\n' "${description}"
        FAILURES=$(( FAILURES + 1 ))
    fi
}

has_tool() { command -v "$1" >/dev/null 2>&1; }
has_env()  { [[ -n "${!1:-}" ]]; }
has_path() { [[ -e "${REPO_ROOT}/$1" ]]; }
has_dir()  { [[ -d "${1}" ]]; }

wwi_log "environment $(wwi_environment_code), stage ${STAGE}"

echo "Repository layout"
check "docs/ESTATE_BUILD_CONTRACT.md present"  has_path "docs/ESTATE_BUILD_CONTRACT.md"
check "config/estate-catalog.yaml present"     has_path "config/estate-catalog.yaml"
check "sqlserver/control present"              has_path "sqlserver/control"
check "sqlserver/agent present"                has_path "sqlserver/agent"
check "config/.env.example present"            has_path "config/.env.example"

ENV_CODE="$(wwi_environment_code)"
check "config/environments/${ENV_CODE,,}.env.yaml present" has_path "config/environments/${ENV_CODE,,}.env.yaml"

echo "Common environment variables"
for var in WWI_ENVIRONMENT; do
    check "${var} is set" has_env "${var}"
done

if [[ "${STAGE}" == "all" || "${STAGE}" == "oracle" ]]; then
    echo "Oracle stage"
    check "sqlplus or sql (sqlcl) on PATH" bash -c 'command -v sqlplus >/dev/null 2>&1 || command -v sql >/dev/null 2>&1'
    for var in ORACLE_HOST ORACLE_PORT ORACLE_SERVICE ORACLE_USER ORACLE_PASSWORD; do
        check "${var} is set" has_env "${var}"
    done
    check "oracle/ source tree present" has_path "oracle"
fi

if [[ "${STAGE}" == "all" || "${STAGE}" == "sqlserver" ]]; then
    echo "SQL Server stage"
    check "sqlcmd on PATH" has_tool sqlcmd
    for var in SQLSERVER_HOST SQLSERVER_PORT SQLSERVER_USER SQLSERVER_PASSWORD \
               SQLSERVER_OLTP_DB SQLSERVER_STAGING_DB SQLSERVER_DW_DB; do
        check "${var} is set" has_env "${var}"
    done
    check "sqlserver/control/ scripts present"  has_path "sqlserver/control"
    check "sqlserver/security/ scripts present" has_path "sqlserver/security"
fi

if [[ "${STAGE}" == "all" || "${STAGE}" == "ssis" ]]; then
    echo "SSIS stage"
    check "dtutil or ISDeploymentWizard on PATH (Windows deploy host)" \
        bash -c 'command -v dtutil >/dev/null 2>&1 || command -v ISDeploymentWizard.exe >/dev/null 2>&1 || [[ "${WWI_ALLOW_MISSING_SSIS_TOOLS:-0}" == "1" ]]'
    for var in SSIS_SERVER SSIS_FOLDER SSIS_PROJECT; do
        check "${var} is set" has_env "${var}"
    done
    check "ssis/ project tree present" has_path "ssis"
fi

echo "Landing zone"
if [[ -n "${WWI_LANDING_ROOT:-}" ]]; then
    for sub in inbound archive quarantine work outbound; do
        check "landing zone ${sub} directory exists" has_dir "${WWI_LANDING_ROOT}/${sub}"
    done
else
    printf '  SKIP  landing zone checks (WWI_LANDING_ROOT not set)\n'
fi

echo
wwi_log "${CHECKS} checks, ${FAILURES} failures"
if (( FAILURES > 0 )); then
    wwi_fail "preflight failed. Fix the items above before running deploy-all."
fi
wwi_log "preflight passed. Connectivity itself is not checked here."
