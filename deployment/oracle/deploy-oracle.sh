#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Oracle stage driver for the WWIGERP schema.
#
# Runs everything under oracle/ in dependency order using SQL*Plus, or sqlcl
# when SQL*Plus is not installed (the newer build agents only have sqlcl).
#
# Connection details come from ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE,
# ORACLE_USER and ORACLE_PASSWORD. The password is never passed as an argument
# - it is written to the client's stdin along with the connect string, so it
# does not appear in the process table or in this driver's dry-run output.
#
# This script has not been executed against any Oracle instance.
#
# Usage: deployment/oracle/deploy-oracle.sh [--dry-run] [--from ORDER] [--only DIR]
# ---------------------------------------------------------------------------

source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
WWI_LOG_PREFIX="wwi-deploy-oracle"

ONLY_DIR=""
START_AT=1

while (( $# > 0 )); do
    case "$1" in
        --dry-run|--what-if) DRY_RUN=1; shift ;;
        --from) START_AT="${2:-1}"; shift 2 ;;
        --only) ONLY_DIR="${2:-}"; shift 2 ;;
        --help|-h) sed -n '2,20p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) wwi_fail "unknown argument '$1'." ;;
    esac
done

wwi_require_env ORACLE_HOST ORACLE_PORT ORACLE_SERVICE ORACLE_USER ORACLE_PASSWORD
wwi_confirm_prod

REPO_ROOT="$(wwi_repo_root)"
ORACLE_ROOT="${REPO_ROOT}/oracle"
[[ -d "${ORACLE_ROOT}" ]] || wwi_fail "oracle/ is not present in ${REPO_ROOT}."

# Pick a client. sqlcl accepts the same connect string and the same @file
# syntax, which is why the estate standardised on that subset years ago.
if command -v sqlplus >/dev/null 2>&1; then
    ORA_CLIENT="sqlplus"
    ORA_CLIENT_ARGS=(-S -L /nolog)
elif command -v sql >/dev/null 2>&1; then
    ORA_CLIENT="sql"
    ORA_CLIENT_ARGS=(-S /nolog)
else
    wwi_fail "neither sqlplus nor sql (sqlcl) is on PATH."
fi

# Dependency order. Types and DDL first, reference data before the packages
# that read it, seed data last because it calls the packages.
# oracle/ddl/03_create_schemas.sql creates each account with a
# &&WWI_<schema>_SECRET substitution variable supplied from the environment.
SCHEMA_SECRET_VARIABLES=(
    "WWI_MDM_SECRET"
    "WWI_PROC_SECRET"
    "WWI_FIN_SECRET"
    "WWI_REF_SECRET"
    "WWI_AUDIT_SECRET"
    "WWI_EXTRACT_SECRET"
)

# An undefined substitution variable makes SQL*Plus read its value from the next
# line of standard input, so a missing secret silently creates the account with a
# fragment of the script as its password instead of failing. Require them all.
if ! wwi_is_dry_run && [[ -z "${ONLY_DIR}" || "${ONLY_DIR}" == "ddl" ]]; then
    wwi_require_env "${SCHEMA_SECRET_VARIABLES[@]}"
fi

# 05_grant_privileges.sql and 07_create_synonyms.sql name the objects in
# oracle/tables one by one, so they run as their own "grants" stage after those
# exist and before the views and packages that compile against them. Each entry
# is "stage name:directory".
LATE_DDL_FILES=("05_grant_privileges.sql" "07_create_synonyms.sql")

# 08_grant_function_execute.sql grants EXECUTE on the scalar functions, so it
# runs as its own "funcgrants" stage between the functions and the views that
# call them.
FUNC_DDL_FILES=("08_grant_function_execute.sql")

# 09_grant_package_execute.sql grants EXECUTE on the packages, so it runs as its
# own "pkggrants" stage between the package specifications and the bodies and
# procedures that call across schemas.
PKG_DDL_FILES=("09_grant_package_execute.sql")

# ZZ_add_future_partitions.sql is the December runbook script, run by hand
# against a populated estate; it is not part of creating the objects.
HAND_RUN_FILES=("ZZ_add_future_partitions.sql")

# Views select through the scalar functions in oracle/functions, so functions
# compile first.
ORACLE_STAGES=(
    "ddl:ddl"
    "tables:tables"
    "grants:ddl"
    "functions:functions"
    "funcgrants:ddl"
    "views:views"
    "pkgspecs:packages"
    "pkggrants:ddl"
    "pkgbodies:packages"
    "procedures:procedures"
    "reference:reference"
    "seed:seed"
)

file_is_in() {
    local candidate="$1"; shift
    local name
    for name in "$@"; do
        [[ "$(basename "${candidate}")" == "${name}" ]] && return 0
    done
    return 1
}

# Every generated artifact stamps "Deploy order : <n>" in its header. File names
# sort alphabetically, which is not dependency order: WWI_FIN tables carry
# foreign keys into WWI_MDM tables that sort after them.
deploy_order() {
    local order
    order="$(head -n 15 "$1" | sed -n 's/.*Deploy order[[:space:]]*:[[:space:]]*\([0-9]\{1,\}\).*/\1/p' | head -n 1)"
    printf '%s' "${order:-999999}"
}

run_sql_file() {
    local file="$1"
    local relative="${file#"${REPO_ROOT}/"}"

    if wwi_is_dry_run; then
        wwi_log "WHATIF ${ORA_CLIENT} @${relative}"
        return 0
    fi

    wwi_log "RUN    ${relative}"
    local defines=""
    local undefines=""
    local name value
    for name in "${SCHEMA_SECRET_VARIABLES[@]}"; do
        value="${!name:-}"
        defines+="DEFINE ${name} = \"${value}\""$'\n'
        undefines+="UNDEFINE ${name}"$'\n'
    done

    # WHENEVER SQLERROR EXIT makes the client return non-zero on the first
    # error, which is the only reliable way to stop an ordered run. DEFINE stays
    # on for the DDL substitution variables; the data scripts turn it off
    # themselves around any literal ampersand.
    "${ORA_CLIENT}" "${ORA_CLIENT_ARGS[@]}" <<SQLPLUS
WHENEVER SQLERROR EXIT SQL.SQLCODE
WHENEVER OSERROR EXIT 9
CONNECT ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SERVICE}
SET DEFINE ON
SET VERIFY OFF
${defines}SET ECHO OFF
SET FEEDBACK ON
SET SERVEROUTPUT ON SIZE UNLIMITED
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET CURRENT_SCHEMA = ${ORACLE_USER};
@${file}
SHOW ERRORS
${undefines}EXIT SQL.SQLCODE
SQLPLUS
}

STAGE_INDEX=0
FILE_COUNT=0

for stage in "${ORACLE_STAGES[@]}"; do
    stage_name="${stage%%:*}"
    dir="${stage#*:}"
    STAGE_INDEX=$(( STAGE_INDEX + 1 ))
    [[ -n "${ONLY_DIR}" && "${ONLY_DIR}" != "${stage_name}" ]] && continue
    (( STAGE_INDEX < START_AT )) && continue

    stage_path="${ORACLE_ROOT}/${dir}"
    if [[ ! -d "${stage_path}" ]]; then
        wwi_warn "oracle/${dir} is not present; skipping stage ${STAGE_INDEX}."
        continue
    fi

    wwi_log "--- stage ${STAGE_INDEX}: ${stage_name} ---"
    # Files run in declared deploy order, name breaking ties.
    while IFS= read -r -d '' sql_file; do
        if [[ "${dir}" == "ddl" ]]; then
            if [[ "${stage_name}" == "grants" ]]; then
                file_is_in "${sql_file}" "${LATE_DDL_FILES[@]}" || continue
            elif [[ "${stage_name}" == "funcgrants" ]]; then
                file_is_in "${sql_file}" "${FUNC_DDL_FILES[@]}" || continue
            elif [[ "${stage_name}" == "pkggrants" ]]; then
                file_is_in "${sql_file}" "${PKG_DDL_FILES[@]}" || continue
            else
                file_is_in "${sql_file}" "${LATE_DDL_FILES[@]}" && continue
                file_is_in "${sql_file}" "${FUNC_DDL_FILES[@]}" && continue
                file_is_in "${sql_file}" "${PKG_DDL_FILES[@]}" && continue
            fi
        fi
        if [[ "${dir}" == "packages" ]]; then
            case "${stage_name}" in
                pkgspecs)  [[ "${sql_file}" == *.pks.sql ]] || continue ;;
                pkgbodies) [[ "${sql_file}" == *.pkb.sql ]] || continue ;;
            esac
        fi
        if [[ "${dir}" == "tables" ]] && file_is_in "${sql_file}" "${HAND_RUN_FILES[@]}"; then
            continue
        fi
        run_sql_file "${sql_file}"
        FILE_COUNT=$(( FILE_COUNT + 1 ))
    done < <(find "${stage_path}" -maxdepth 2 -type f -name '*.sql' |
        while IFS= read -r found; do printf '%06d\t%s\n' "$(deploy_order "${found}")" "${found}"; done |
        sort -t$'\t' -k1,1n -k2,2 | cut -f2- | tr '\n' '\0')
done

# Invalid objects are expected mid-run and recompiled at the end; the estate has
# always relied on this rather than getting the package order strictly right.
if ! wwi_is_dry_run; then
    wwi_log "recompiling invalid objects"
    "${ORA_CLIENT}" "${ORA_CLIENT_ARGS[@]}" <<SQLPLUS
WHENEVER SQLERROR EXIT SQL.SQLCODE
CONNECT ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SERVICE}
SET SERVEROUTPUT ON
BEGIN
    FOR s IN (SELECT username FROM dba_users WHERE username LIKE 'WWI\_%' ESCAPE '\') LOOP
        DBMS_UTILITY.COMPILE_SCHEMA(schema => s.username, compile_all => FALSE);
    END LOOP;
END;
/
SELECT owner, object_type, object_name FROM dba_objects
 WHERE owner LIKE 'WWI\_%' ESCAPE '\' AND status = 'INVALID'
 ORDER BY owner, object_type, object_name;
EXIT SQL.SQLCODE
SQLPLUS
else
    wwi_log "WHATIF recompile invalid objects"
fi

wwi_log "oracle stage complete: ${FILE_COUNT} file(s) processed$(wwi_is_dry_run && printf ' (dry run)' || true)"
