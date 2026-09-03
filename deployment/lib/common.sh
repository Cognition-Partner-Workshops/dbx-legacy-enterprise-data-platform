#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Shared helpers for the WWI estate deployment drivers.
#
# Sourced by deploy-all.sh and every stage driver. Provides environment
# variable checking, dry-run handling, logging and a uniform failure exit.
#
# No script in deployment/ has been executed against a database server. The
# helpers below only build and echo commands; when DRY_RUN is set they never
# invoke a client at all.
# ---------------------------------------------------------------------------

set -o errexit
set -o nounset
set -o pipefail

WWI_LOG_PREFIX="${WWI_LOG_PREFIX:-wwi-deploy}"
DRY_RUN="${DRY_RUN:-0}"

wwi_log()  { printf '[%s] %s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${WWI_LOG_PREFIX}" "$*"; }
wwi_warn() { printf '[%s] %s WARN  %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${WWI_LOG_PREFIX}" "$*" >&2; }
wwi_fail() { printf '[%s] %s ERROR %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${WWI_LOG_PREFIX}" "$*" >&2; exit 1; }

# wwi_require_env VAR [VAR...]
# Fails with a single message listing every variable that is unset or empty,
# rather than stopping at the first one.
wwi_require_env() {
    local missing=()
    local name
    for name in "$@"; do
        if [[ -z "${!name:-}" ]]; then
            missing+=("${name}")
        fi
    done
    if (( ${#missing[@]} > 0 )); then
        wwi_fail "required environment variables are not set: ${missing[*]}. See config/.env.example."
    fi
}

# wwi_require_tool NAME HINT
wwi_require_tool() {
    local tool="$1"
    local hint="${2:-}"
    if ! command -v "${tool}" >/dev/null 2>&1; then
        wwi_fail "${tool} is not on PATH. ${hint}"
    fi
}

wwi_is_dry_run() { [[ "${DRY_RUN}" == "1" ]]; }

# wwi_run DESCRIPTION -- command args...
# In dry-run mode the command is printed and skipped. Secrets are never part of
# the printed form because every driver passes them through the client's
# environment-variable or stdin interface, not as an argument.
wwi_run() {
    local description="$1"; shift
    [[ "${1:-}" == "--" ]] && shift
    if wwi_is_dry_run; then
        wwi_log "WHATIF ${description}"
        printf '           %q ' "$@"; printf '\n'
        return 0
    fi
    wwi_log "RUN    ${description}"
    "$@"
}

# wwi_environment_code -> DEV | TEST | PROD
wwi_environment_code() {
    local code="${WWI_ENVIRONMENT:-DEV}"
    case "${code}" in
        DEV|TEST|PROD) printf '%s' "${code}" ;;
        *) wwi_fail "WWI_ENVIRONMENT must be DEV, TEST or PROD (got '${code}')." ;;
    esac
}

# Guard against the classic accident of running a PROD deployment from a
# workstation that still has DEV variables loaded.
wwi_confirm_prod() {
    if [[ "$(wwi_environment_code)" == "PROD" && "${WWI_CONFIRM_PROD:-}" != "I-UNDERSTAND" ]]; then
        wwi_fail "PROD deployments require WWI_CONFIRM_PROD=I-UNDERSTAND."
    fi
}

wwi_repo_root() {
    cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
}
