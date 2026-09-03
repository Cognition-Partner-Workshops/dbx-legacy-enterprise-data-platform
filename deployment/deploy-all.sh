#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# WWI estate - single deployment entry point.
#
# Order (documented in deployment/README.md, and it matters):
#
#   0. preflight     tooling, environment variables, repository layout
#   1. oracle        WWIGERP schema objects, in dependency order
#   2. sqlserver     control -> OLTP extensions -> staging -> warehouse ->
#                    security -> agent
#   3. ssis          catalogue folder, project, environment and bindings
#
# Oracle runs first because the SQL Server extract views and the SSIS project
# parameters name Oracle objects; SSIS runs last because its environment binds
# to databases that must already exist.
#
# Every connection detail comes from the environment variables documented in
# config/.env.example. No script in deployment/ contains a credential, and none
# of them has been executed against a server.
#
# Usage:
#   deployment/deploy-all.sh --dry-run
#   deployment/deploy-all.sh --stage sqlserver
#   WWI_ENVIRONMENT=PROD WWI_CONFIRM_PROD=I-UNDERSTAND deployment/deploy-all.sh
# ---------------------------------------------------------------------------

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
WWI_LOG_PREFIX="wwi-deploy-all"

STAGES=(preflight oracle sqlserver ssis)
ONLY_STAGE=""
SKIP_PREFLIGHT=0
CONTINUE_ON_ERROR=0

while (( $# > 0 )); do
    case "$1" in
        --dry-run|--what-if) DRY_RUN=1; export DRY_RUN; shift ;;
        --stage) ONLY_STAGE="${2:-}"; shift 2 ;;
        --skip-preflight) SKIP_PREFLIGHT=1; shift ;;
        --continue-on-error) CONTINUE_ON_ERROR=1; shift ;;
        --help|-h) sed -n '2,26p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) wwi_fail "unknown argument '$1'. Try --help." ;;
    esac
done

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_CODE="$(wwi_environment_code)"
wwi_confirm_prod

wwi_log "environment ${ENV_CODE}$(wwi_is_dry_run && printf ', dry run' || true)"

STAGE_ARGS=()
wwi_is_dry_run && STAGE_ARGS+=(--dry-run)

FAILED_STAGES=()

run_stage() {
    local name="$1"; shift
    if [[ -n "${ONLY_STAGE}" && "${ONLY_STAGE}" != "${name}" ]]; then
        return 0
    fi
    wwi_log "======== stage: ${name} ========"
    if "$@"; then
        wwi_log "stage ${name} finished"
        return 0
    fi
    FAILED_STAGES+=("${name}")
    if (( CONTINUE_ON_ERROR == 1 )); then
        wwi_warn "stage ${name} failed; continuing because --continue-on-error was given"
        return 0
    fi
    wwi_fail "stage ${name} failed. Fix it and re-run, optionally with --stage ${name}."
}

if (( SKIP_PREFLIGHT == 0 )); then
    run_stage preflight "${HERE}/preflight/preflight.sh" --stage all
fi

run_stage oracle    "${HERE}/oracle/deploy-oracle.sh"       "${STAGE_ARGS[@]}"
run_stage sqlserver "${HERE}/sqlserver/deploy-sqlserver.sh" "${STAGE_ARGS[@]}"
run_stage ssis      "${HERE}/ssis/deploy-ssis.sh"           "${STAGE_ARGS[@]}"

if (( ${#FAILED_STAGES[@]} > 0 )); then
    wwi_fail "finished with failures in: ${FAILED_STAGES[*]}"
fi

wwi_log "deploy-all finished$(wwi_is_dry_run && printf ' (dry run - nothing was submitted)' || true)"
wwi_log "post-deployment steps are listed in deployment/README.md"
