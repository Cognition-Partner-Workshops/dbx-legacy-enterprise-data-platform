/* ============================================================================
 * Object      : WWI_AUDIT.PKG_EXTRACT_CONTROL (package specification)
 * Schema      : WWI_AUDIT
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_AUDIT.EXTRACT_CONTROL, WWI_AUDIT.CHANGE_LOG,
 *               WWI_AUDIT.PURGE_LOG
 * Called by   : the SSIS Oracle extract packages (EXT_ORA_*) in their
 *               pre-execute and post-execute steps, and the ETL-facing
 *               preparation procedures in WWI_AUDIT.
 * Notes       : Watermarks are held as text because some extracts watermark
 *               on LAST_UPD_DT and some on a surrogate key. The SSIS side
 *               casts the value itself; do not change the column type.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_AUDIT.PKG_EXTRACT_CONTROL AS

    e_extract_unknown  EXCEPTION;
    e_extract_disabled EXCEPTION;
    e_watermark_regress EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_extract_unknown,   -20511);
    PRAGMA EXCEPTION_INIT(e_extract_disabled,  -20512);
    PRAGMA EXCEPTION_INIT(e_watermark_regress, -20513);

    FUNCTION get_watermark
    (
        p_extract_name IN WWI_AUDIT.EXTRACT_CONTROL.EXTRACT_NAME%TYPE
    ) RETURN WWI_AUDIT.V_EXTRACT_WATERMARK.LAST_EXTRACT_VALUE_TXT%TYPE;

    FUNCTION get_watermark_dt
    (
        p_extract_name IN WWI_AUDIT.EXTRACT_CONTROL.EXTRACT_NAME%TYPE
    ) RETURN DATE;

    PROCEDURE begin_extract
    (
        p_extract_name IN  WWI_AUDIT.EXTRACT_CONTROL.EXTRACT_NAME%TYPE,
        p_run_id       OUT NUMBER,
        p_from_value   OUT WWI_AUDIT.V_EXTRACT_WATERMARK.LAST_EXTRACT_VALUE_TXT%TYPE,
        p_to_value     OUT WWI_AUDIT.V_EXTRACT_WATERMARK.LAST_EXTRACT_VALUE_TXT%TYPE
    );

    PROCEDURE end_extract
    (
        p_extract_name IN WWI_AUDIT.EXTRACT_CONTROL.EXTRACT_NAME%TYPE,
        p_run_id       IN NUMBER,
        p_row_count    IN WWI_AUDIT.EXTRACT_CONTROL.LAST_ROW_COUNT%TYPE,
        p_to_value     IN WWI_AUDIT.V_EXTRACT_WATERMARK.LAST_EXTRACT_VALUE_TXT%TYPE,
        p_status_cd    IN WWI_AUDIT.EXTRACT_CONTROL.LAST_STATUS_CD%TYPE DEFAULT 'SUCCESS'
    );

    PROCEDURE fail_extract
    (
        p_extract_name IN WWI_AUDIT.EXTRACT_CONTROL.EXTRACT_NAME%TYPE,
        p_run_id       IN NUMBER,
        p_message      IN VARCHAR2
    );

    PROCEDURE mark_changes_extracted
    (
        p_schema_name IN  WWI_AUDIT.CHANGE_LOG.SCHEMA_NAME%TYPE,
        p_object_name IN  WWI_AUDIT.CHANGE_LOG.TABLE_NAME%TYPE,
        p_thru_dt     IN  DATE,
        p_marked_cnt  OUT PLS_INTEGER
    );

    PROCEDURE purge_change_log
    (
        p_retain_days IN  PLS_INTEGER DEFAULT 90,
        p_purged_cnt  OUT PLS_INTEGER
    );

END PKG_EXTRACT_CONTROL;
/
