/* ============================================================================
 * Object      : WWI_PROC.PRC_CLOSE_STALE_PO (procedure)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PKG_PURCHASE_ORDER, WWI_PROC.PURCHASE_ORDER_HDR,
 *               WWI_PROC.FN_PO_OPEN_QTY, WWI_FIN.AP_INVOICE_LINE,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : DBMS_JOB 'ERP_PO_HOUSEKEEPING' (first Sunday of the month)
 * Notes       : Regional stale windows: NA 180 days, EU 120 (the audit
 *               committee asked for a tighter commitment register), APAC 365
 *               because sea freight orders legitimately sit open for a year.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_PROC.PRC_CLOSE_STALE_PO
(
    p_region_cd   IN  VARCHAR2 DEFAULT NULL,
    p_closed_cnt  OUT PLS_INTEGER,
    p_skipped_cnt OUT PLS_INTEGER
)
IS
    TYPE t_region_tab IS TABLE OF VARCHAR2(4);
    l_regions t_region_tab := t_region_tab('NA', 'EU', 'APAC');
    l_region  VARCHAR2(4);
    l_days    PLS_INTEGER;
    l_cnt     PLS_INTEGER;
    l_partial PLS_INTEGER;
BEGIN
    p_closed_cnt  := 0;
    p_skipped_cnt := 0;

    FOR i IN 1 .. l_regions.COUNT LOOP
        l_region := l_regions(i);

        IF p_region_cd IS NOT NULL AND p_region_cd <> l_region THEN
            CONTINUE;
        END IF;

        l_days := CASE l_region
                      WHEN 'EU'   THEN 120
                      WHEN 'APAC' THEN 365
                      ELSE 180
                  END;

        /* partially received orders are never auto-closed; they are counted
           here so the buyers get a work list out of the job log           */
        SELECT COUNT(*)
          INTO l_partial
          FROM WWI_PROC.PURCHASE_ORDER_HDR h
         WHERE h.REGION_CD = l_region
           AND h.STATUS_CD = 'OPEN'
           AND h.ORDER_DT < TRUNC(SYSDATE) - l_days
           AND EXISTS (SELECT 1
                         FROM WWI_PROC.PURCHASE_ORDER_LINE pl
                        WHERE pl.PO_ID = h.PO_ID
                          AND NVL(pl.RECEIVED_QTY, 0) > 0
                          AND WWI_PROC.FN_PO_OPEN_QTY(pl.PO_LINE_ID) > 0);

        p_skipped_cnt := p_skipped_cnt + l_partial;

        BEGIN
            WWI_PROC.PKG_PURCHASE_ORDER.close_stale_pos(l_region, l_days, l_cnt);
            p_closed_cnt := p_closed_cnt + l_cnt;
        EXCEPTION
            WHEN OTHERS THEN
                WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL,
                    'WWI_PROC.PURCHASE_ORDER_HDR', l_region, 'CLOSE_STALE_FAILED',
                    SQLERRM, 'E');
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('stale PO close: closed=' || p_closed_cnt
                         || ' partial_skipped=' || p_skipped_cnt);
EXCEPTION
    WHEN OTHERS THEN
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_CLOSE_STALE_PO',
                                             NVL(p_region_cd, 'ALL'), SQLERRM);
        RAISE;
END PRC_CLOSE_STALE_PO;
/
