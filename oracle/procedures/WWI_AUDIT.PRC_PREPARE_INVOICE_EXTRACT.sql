/* ============================================================================
 * Object      : WWI_AUDIT.PRC_PREPARE_INVOICE_EXTRACT (procedure)
 * Schema      : WWI_AUDIT
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_AUDIT.PKG_EXTRACT_CONTROL, WWI_AUDIT.PKG_DATA_QUALITY,
 *               WWI_FIN.AP_INVOICE_HDR, WWI_FIN.V_AP_INVOICE_EXTRACT,
 *               WWI_FIN.GL_PERIOD_STATUS
 * Called by   : SSIS EXT_ORA_ApInvoice (pre-execute task)
 * Notes       : Invoices are watermarked on INVOICE_ID rather than a date
 *               because the 2009 re-keying exercise back-dated thousands of
 *               invoice dates and the date watermark skipped them all.
 *               Invoices in a still open period are excluded so the warehouse
 *               never sees a figure finance has not signed off.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_AUDIT.PRC_PREPARE_INVOICE_EXTRACT
(
    p_run_id      OUT NUMBER,
    p_from_key    OUT NUMBER,
    p_to_key      OUT NUMBER,
    p_row_est     OUT PLS_INTEGER
)
IS
    l_from_val WWI_AUDIT.V_EXTRACT_WATERMARK.LAST_EXTRACT_VALUE_TXT%TYPE;
    l_to_val   WWI_AUDIT.V_EXTRACT_WATERMARK.LAST_EXTRACT_VALUE_TXT%TYPE;
    l_open_cnt PLS_INTEGER;
BEGIN
    WWI_AUDIT.PKG_EXTRACT_CONTROL.begin_extract('EXT_ORA_AP_INVOICE_HDR', p_run_id,
                                                l_from_val, l_to_val);

    p_from_key := NVL(TO_NUMBER(l_from_val), 0);

    SELECT NVL(MAX(INVOICE_ID), p_from_key)
      INTO p_to_key
      FROM WWI_FIN.AP_INVOICE_HDR
     WHERE INVOICE_STATUS_CD NOT IN ('EN', 'CN');

    SELECT COUNT(*)
      INTO p_row_est
      FROM WWI_FIN.AP_INVOICE_HDR h
     WHERE h.INVOICE_ID > p_from_key
       AND h.INVOICE_ID <= p_to_key
       AND h.INVOICE_STATUS_CD NOT IN ('EN', 'CN');

    SELECT COUNT(*)
      INTO l_open_cnt
      FROM WWI_FIN.AP_INVOICE_HDR h
      JOIN WWI_FIN.GL_PERIOD_STATUS p
        ON p.REGION_CD = h.REGION_CD
       AND p.PERIOD_CD = WWI_REF.FN_FISCAL_PERIOD(h.INVOICE_DT, h.REGION_CD)
     WHERE h.INVOICE_ID > p_from_key
       AND h.INVOICE_ID <= p_to_key
       AND p.AP_STATUS_CD = 'OPEN';

    IF l_open_cnt > 0 THEN
        WWI_AUDIT.PKG_DATA_QUALITY.log_reject('EXT_ORA_AP_INVOICE_HDR',
            'WWI_FIN.AP_INVOICE_HDR', NULL, 'OPEN_PERIOD',
            l_open_cnt || ' invoice(s) sit in a period that is still open', 'W');
    END IF;

    IF p_row_est = 0 THEN
        /* an empty window is normal at weekends; the extract is ended here
           so the watermark does not sit in RUNNING until Monday          */
        WWI_AUDIT.PKG_EXTRACT_CONTROL.end_extract('EXT_ORA_AP_INVOICE_HDR', p_run_id,
                                                  0, TO_CHAR(p_to_key), 'EMPTY');
    END IF;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_EXTRACT_CONTROL.fail_extract('EXT_ORA_AP_INVOICE_HDR', p_run_id,
                                                   SQLERRM);
        RAISE;
END PRC_PREPARE_INVOICE_EXTRACT;
/
