/* ============================================================================
 * Object      : WWI_AUDIT.PRC_PREPARE_CUSTOMER_EXTRACT (procedure)
 * Schema      : WWI_AUDIT
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_AUDIT.PKG_EXTRACT_CONTROL, WWI_AUDIT.CHANGE_LOG,
 *               WWI_MDM.V_CUSTOMER_EXTRACT, WWI_MDM.CUST_MASTER,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : SSIS EXT_ORA_Customer (pre-execute task)
 * Notes       : ETL facing. The SSIS package reads WWI_MDM.V_CUSTOMER_EXTRACT
 *               filtered on LAST_UPD_DT between the two out parameters, so
 *               this procedure must be called first in the same session.
 *               EU rows whose consent was withdrawn are pushed as deletes
 *               through the change log rather than simply disappearing.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_AUDIT.PRC_PREPARE_CUSTOMER_EXTRACT
(
    p_run_id     OUT NUMBER,
    p_from_dt    OUT DATE,
    p_to_dt      OUT DATE,
    p_row_est    OUT PLS_INTEGER
)
IS
    l_from_val WWI_AUDIT.V_EXTRACT_WATERMARK.LAST_EXTRACT_VALUE_TXT%TYPE;
    l_to_val   WWI_AUDIT.V_EXTRACT_WATERMARK.LAST_EXTRACT_VALUE_TXT%TYPE;
    l_suppressed PLS_INTEGER;
BEGIN
    WWI_AUDIT.PKG_EXTRACT_CONTROL.begin_extract('EXT_ORA_CUSTOMER_MASTER', p_run_id,
                                                l_from_val, l_to_val);

    p_from_dt := NVL(TO_DATE(l_from_val, 'YYYY-MM-DD HH24:MI:SS'),
                     DATE '1900-01-01');
    p_to_dt   := TO_DATE(l_to_val, 'YYYY-MM-DD HH24:MI:SS');

    SELECT COUNT(*)
      INTO p_row_est
      FROM WWI_MDM.CUST_MASTER
     WHERE UPDATED_DT > p_from_dt
       AND UPDATED_DT <= p_to_dt;

    /* consent withdrawals must reach the warehouse as an explicit delete */
    INSERT INTO WWI_AUDIT.CHANGE_LOG
        (CHANGE_LOG_ID, SCHEMA_NAME, TABLE_NAME, PK_VALUE_TXT,
         OPERATION_CD, CHANGE_TS, REASON_TXT, EXTRACTED_FLG, CHANGED_BY)
    SELECT WWI_AUDIT.SEQ_CHANGE_LOG.NEXTVAL, 'WWI_MDM', 'CUST_MASTER',
           TO_CHAR(c.CUST_ID), 'D', SYSTIMESTAMP,
           'consent withdrawn ' || TO_CHAR(c.CONSENT_CAPTURED_DT, 'YYYY-MM-DD'),
           'N', USER
      FROM WWI_MDM.CUST_MASTER c
     WHERE c.REGION_CD = 'EU'
       AND NVL(c.CONSENT_MARKETING_FLG, 'N') = 'N'
       AND c.CONSENT_CAPTURED_DT > p_from_dt
       AND c.CONSENT_CAPTURED_DT <= p_to_dt
       AND NOT EXISTS (SELECT 1
                         FROM WWI_AUDIT.CHANGE_LOG cl
                        WHERE cl.TABLE_NAME   = 'CUST_MASTER'
                          AND cl.PK_VALUE_TXT = TO_CHAR(c.CUST_ID)
                          AND cl.OPERATION_CD = 'D');

    l_suppressed := SQL%ROWCOUNT;

    IF l_suppressed > 0 THEN
        WWI_AUDIT.PKG_DATA_QUALITY.log_reject('EXT_ORA_CUSTOMER_MASTER',
            'WWI_MDM.CUST_MASTER', NULL, 'CONSENT_DELETE',
            l_suppressed || ' EU customer(s) queued as downstream deletes', 'W');
    END IF;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_EXTRACT_CONTROL.fail_extract('EXT_ORA_CUSTOMER_MASTER', p_run_id,
                                                   SQLERRM);
        RAISE;
END PRC_PREPARE_CUSTOMER_EXTRACT;
/
