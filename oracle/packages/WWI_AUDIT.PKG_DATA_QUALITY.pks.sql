/* ============================================================================
 * Object      : WWI_AUDIT.PKG_DATA_QUALITY (package specification)
 * Schema      : WWI_AUDIT
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_AUDIT.INTERFACE_ERROR
 * Called by   : every other WWI package - this is the estate's only error
 *               sink - plus the nightly data quality job
 *               WWI_AUDIT.PRC_RUN_DQ_CHECKS.
 * Notes       : log_error uses an autonomous transaction so a caller can log
 *               and then roll back its own work. That was added in 2009 after
 *               an entire night's rejects were lost to a rollback.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_AUDIT.PKG_DATA_QUALITY AS

    e_threshold_breached EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_threshold_breached, -20501);

    PROCEDURE log_error
    (
        p_source_txt IN WWI_AUDIT.INTERFACE_ERROR.SRC_OBJECT_NAME%TYPE,
        p_key_txt    IN WWI_AUDIT.INTERFACE_ERROR.SRC_KEY_TXT%TYPE,
        p_message    IN WWI_AUDIT.INTERFACE_ERROR.ERROR_MSG_TXT%TYPE
    );

    PROCEDURE log_reject
    (
        p_extract_name IN WWI_AUDIT.INTERFACE_ERROR.EXTRACT_NAME%TYPE,
        p_source_txt   IN WWI_AUDIT.INTERFACE_ERROR.SRC_OBJECT_NAME%TYPE,
        p_key_txt      IN WWI_AUDIT.INTERFACE_ERROR.SRC_KEY_TXT%TYPE,
        p_rule_cd      IN WWI_AUDIT.INTERFACE_ERROR.RULE_CD%TYPE,
        p_message      IN WWI_AUDIT.INTERFACE_ERROR.ERROR_MSG_TXT%TYPE,
        p_severity_cd  IN WWI_AUDIT.INTERFACE_ERROR.SEVERITY_CD%TYPE DEFAULT 'W'
    );

    PROCEDURE resolve_errors
    (
        p_extract_name IN  WWI_AUDIT.INTERFACE_ERROR.EXTRACT_NAME%TYPE,
        p_resolved_by  IN  VARCHAR2,
        p_resolved_cnt OUT PLS_INTEGER
    );

    PROCEDURE run_orphan_checks
    (
        p_region_cd IN  VARCHAR2 DEFAULT NULL,
        p_issue_cnt OUT PLS_INTEGER
    );

    PROCEDURE assert_reject_rate
    (
        p_extract_name IN WWI_AUDIT.INTERFACE_ERROR.EXTRACT_NAME%TYPE,
        p_row_count    IN NUMBER,
        p_max_pct      IN NUMBER DEFAULT 2
    );

END PKG_DATA_QUALITY;
/
