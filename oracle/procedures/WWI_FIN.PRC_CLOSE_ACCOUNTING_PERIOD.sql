/* ============================================================================
 * Object      : WWI_FIN.PRC_CLOSE_ACCOUNTING_PERIOD (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.GL_PERIOD_STATUS, WWI_FIN.GL_JOURNAL_HDR,
 *               WWI_FIN.AP_INVOICE_HDR, WWI_FIN.PKG_GL_POSTING,
 *               WWI_REF.FN_FISCAL_PERIOD, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : WWI_FIN.PRC_RUN_PERIOD_CLOSE, finance close checklist step 9
 * Notes       : Closing is per region because the three ledgers run on
 *               different fiscal calendars (NA calendar year, EU calendar
 *               year with a 13th adjustment period, APAC April-March).
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_FIN.PRC_CLOSE_ACCOUNTING_PERIOD
(
    p_region_cd   IN  VARCHAR2,
    p_period_cd   IN  WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE,
    p_force_flg   IN  VARCHAR2 DEFAULT 'N',
    p_closed_by   IN  VARCHAR2,
    p_blocker_cnt OUT PLS_INTEGER
)
IS
    l_status      VARCHAR2(20);
    l_unposted    PLS_INTEGER;
    l_unvalidated PLS_INTEGER;
    l_held        PLS_INTEGER;
    l_next_period WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE;
BEGIN
    p_blocker_cnt := 0;

    l_status := WWI_FIN.PKG_GL_POSTING.period_status(p_period_cd, p_region_cd);

    IF l_status = 'CLOSED' THEN
        RAISE_APPLICATION_ERROR(-20605,
            'PRC_CLOSE_ACCOUNTING_PERIOD: ' || p_period_cd || ' is already closed for '
            || p_region_cd);
    END IF;

    SELECT COUNT(*)
      INTO l_unposted
      FROM WWI_FIN.GL_JOURNAL_HDR
     WHERE REGION_CD = p_region_cd
       AND PERIOD_CD = p_period_cd
       AND POSTING_STATUS_CD <> 'P';

    SELECT COUNT(*)
      INTO l_unvalidated
      FROM WWI_FIN.AP_INVOICE_HDR
     WHERE REGION_CD = p_region_cd
       AND INVOICE_STATUS_CD = 'EN'
       AND WWI_REF.FN_FISCAL_PERIOD(INVOICE_DT, p_region_cd) = p_period_cd;

    SELECT COUNT(*)
      INTO l_held
      FROM WWI_FIN.AP_INVOICE_HDR
     WHERE REGION_CD = p_region_cd
       AND INVOICE_STATUS_CD = 'HO'
       AND WWI_REF.FN_FISCAL_PERIOD(INVOICE_DT, p_region_cd) = p_period_cd;

    p_blocker_cnt := l_unposted + l_unvalidated;

    IF l_unposted > 0 THEN
        WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL, 'WWI_FIN.GL_JOURNAL_HDR',
            p_region_cd || '/' || p_period_cd, 'UNPOSTED_JOURNALS',
            l_unposted || ' journal(s) not posted', 'E');
    END IF;

    IF l_unvalidated > 0 THEN
        WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL, 'WWI_FIN.AP_INVOICE_HDR',
            p_region_cd || '/' || p_period_cd, 'UNVALIDATED_INVOICES',
            l_unvalidated || ' invoice(s) still entered but not validated', 'E');
    END IF;

    /* EU will not close over invoices sitting on hold; NA and APAC accept
       them and carry the hold into the next period                       */
    IF p_region_cd = 'EU' AND l_held > 0 THEN
        p_blocker_cnt := p_blocker_cnt + l_held;
        WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL, 'WWI_FIN.AP_INVOICE_HDR',
            p_region_cd || '/' || p_period_cd, 'HELD_INVOICES',
            l_held || ' invoice(s) on hold', 'E');
    END IF;

    IF p_blocker_cnt > 0 AND NVL(p_force_flg, 'N') <> 'Y' THEN
        RAISE_APPLICATION_ERROR(-20606,
            'PRC_CLOSE_ACCOUNTING_PERIOD: ' || p_blocker_cnt
            || ' blocker(s) for ' || p_region_cd || ' ' || p_period_cd);
    END IF;

    UPDATE WWI_FIN.GL_PERIOD_STATUS
       SET AP_STATUS_CD   = 'CLOSED',
           CLOSED_DT     = SYSDATE,
           CLOSED_BY_CD  = p_closed_by,
           SOFT_CLOSE_DT = CASE WHEN p_blocker_cnt > 0 THEN SYSDATE END,
           UPDATED_DT = SYSDATE
     WHERE REGION_CD = p_region_cd
       AND PERIOD_CD = p_period_cd;

    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20607,
            'PRC_CLOSE_ACCOUNTING_PERIOD: no period row for ' || p_region_cd
            || ' ' || p_period_cd);
    END IF;

    /* the next period is opened here rather than by a calendar job; that is
       why a missed close leaves the ledger with nothing open at all       */
    SELECT MIN(PERIOD_CD)
      INTO l_next_period
      FROM WWI_FIN.GL_PERIOD_STATUS
     WHERE REGION_CD = p_region_cd
       AND PERIOD_CD > p_period_cd;

    IF l_next_period IS NOT NULL THEN
        UPDATE WWI_FIN.GL_PERIOD_STATUS
           SET AP_STATUS_CD   = 'OPEN',
               UPDATED_DT = SYSDATE
         WHERE REGION_CD = p_region_cd
           AND PERIOD_CD = l_next_period
           AND AP_STATUS_CD = 'FUTURE';
    END IF;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_CLOSE_ACCOUNTING_PERIOD',
                                             p_region_cd || '/' || p_period_cd,
                                             SQLERRM);
        RAISE;
END PRC_CLOSE_ACCOUNTING_PERIOD;
/
