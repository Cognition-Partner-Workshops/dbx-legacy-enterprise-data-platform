/* ============================================================================
 * Object      : WWI_FIN.PRC_RUN_PERIOD_CLOSE (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.PKG_GL_POSTING, WWI_FIN.PRC_ACCRUE_UNINVOICED_RECEIPTS,
 *               WWI_FIN.PRC_REVALUE_AP_BALANCES, WWI_FIN.PRC_RUN_COST_ALLOCATION,
 *               WWI_FIN.PRC_BUILD_AGING_SNAPSHOT,
 *               WWI_FIN.PRC_CLOSE_ACCOUNTING_PERIOD, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : finance close checklist, DBMS_JOB 'ERP_PERIOD_CLOSE'
 * Notes       : Step order is not the same in every region. APAC allocates
 *               before revaluing because the Singapore ledger books the
 *               allocation in local currency; EU does the reverse. Changing
 *               either order changes the reported result.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_FIN.PRC_RUN_PERIOD_CLOSE
(
    p_region_cd IN  VARCHAR2,
    p_period_cd IN  WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE,
    p_closed_by IN  VARCHAR2,
    p_force_flg IN  VARCHAR2 DEFAULT 'N',
    p_status_cd OUT VARCHAR2
)
IS
    l_accrued   PLS_INTEGER := 0;
    l_reversed  PLS_INTEGER := 0;
    l_rules     PLS_INTEGER := 0;
    l_lines     PLS_INTEGER := 0;
    l_posted    PLS_INTEGER := 0;
    l_failed    PLS_INTEGER := 0;
    l_rows      PLS_INTEGER := 0;
    l_blockers  PLS_INTEGER := 0;
    l_journal   WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE;
    l_gain_amt  NUMBER;
BEGIN
    p_status_cd := 'RUNNING';

    WWI_FIN.PKG_GL_POSTING.reverse_accruals(p_region_cd, p_period_cd, l_reversed);
    WWI_FIN.PRC_ACCRUE_UNINVOICED_RECEIPTS(p_region_cd, p_period_cd, 'N', l_accrued);

    IF p_region_cd = 'APAC' THEN
        WWI_FIN.PRC_RUN_COST_ALLOCATION(p_region_cd, p_period_cd, l_rules, l_lines);
        WWI_FIN.PRC_REVALUE_AP_BALANCES(p_region_cd, p_period_cd, l_journal,
                                        l_gain_amt);
    ELSE
        WWI_FIN.PRC_REVALUE_AP_BALANCES(p_region_cd, p_period_cd, l_journal,
                                        l_gain_amt);
        WWI_FIN.PRC_RUN_COST_ALLOCATION(p_region_cd, p_period_cd, l_rules, l_lines);
    END IF;

    WWI_FIN.PKG_GL_POSTING.post_pending_journals(p_region_cd, p_period_cd,
                                                 l_posted, l_failed);

    WWI_FIN.PRC_BUILD_AGING_SNAPSHOT(p_region_cd,
                                     CASE p_region_cd
                                         WHEN 'EU' THEN LAST_DAY(SYSDATE)
                                         ELSE TRUNC(SYSDATE)
                                     END,
                                     'Y', l_rows);

    BEGIN
        WWI_FIN.PRC_CLOSE_ACCOUNTING_PERIOD(p_region_cd, p_period_cd, p_force_flg,
                                            p_closed_by, l_blockers);
        p_status_cd := CASE WHEN l_blockers > 0 THEN 'FORCED_CLOSE' ELSE 'CLOSED' END;
    EXCEPTION
        WHEN OTHERS THEN
            p_status_cd := 'CLOSE_BLOCKED';
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RUN_PERIOD_CLOSE.close',
                                                 p_region_cd || '/' || p_period_cd,
                                                 SQLERRM);
    END;

    DBMS_OUTPUT.PUT_LINE(p_region_cd || ' ' || p_period_cd
                         || ' reversed=' || l_reversed
                         || ' accrued=' || l_accrued
                         || ' alloc_rules=' || l_rules
                         || ' posted=' || l_posted
                         || ' failed=' || l_failed
                         || ' aging_rows=' || l_rows
                         || ' status=' || p_status_cd);
EXCEPTION
    WHEN OTHERS THEN
        p_status_cd := 'FAILED';
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RUN_PERIOD_CLOSE',
                                             p_region_cd || '/' || p_period_cd,
                                             SQLERRM);
        RAISE;
END PRC_RUN_PERIOD_CLOSE;
/
