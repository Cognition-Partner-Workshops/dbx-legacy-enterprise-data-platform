/* ============================================================================
 * Object      : WWI_FIN.PRC_RUN_NIGHTLY_AP (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.PKG_AP_INVOICE, WWI_FIN.PKG_GL_POSTING,
 *               WWI_FIN.PRC_LOAD_INVOICE_INTERFACE,
 *               WWI_FIN.PRC_ACCRUE_UNINVOICED_RECEIPTS,
 *               WWI_AUDIT.PKG_DATA_QUALITY, WWI_REF.FN_FISCAL_PERIOD
 * Called by   : DBMS_JOB 'ERP_NIGHTLY_AP' (23:10 NA, 22:10 EU, 20:40 APAC)
 * History     : 1998 original single-region job; regions were bolted on in
 *               2004 and 2011, which is why each one still has its own
 *               ordering below rather than a driving table.
 * Notes       : Each region commits independently. A failure in one region
 *               must not stop the others, so every step is wrapped.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_FIN.PRC_RUN_NIGHTLY_AP
(
    p_region_cd IN  VARCHAR2,
    p_run_dt    IN  DATE DEFAULT TRUNC(SYSDATE),
    p_error_cnt OUT PLS_INTEGER
)
IS
    l_period_cd     VARCHAR2(10);
    l_loaded_cnt    PLS_INTEGER := 0;
    l_rejected_cnt  PLS_INTEGER := 0;
    l_validated_cnt PLS_INTEGER := 0;
    l_held_cnt      PLS_INTEGER := 0;
    l_posted_cnt    PLS_INTEGER := 0;
    l_failed_cnt    PLS_INTEGER := 0;
    l_accrued_cnt   PLS_INTEGER := 0;
BEGIN
    p_error_cnt := 0;

    IF p_region_cd NOT IN ('NA', 'EU', 'APAC') THEN
        RAISE_APPLICATION_ERROR(-20601,
            'PRC_RUN_NIGHTLY_AP: unknown region ' || p_region_cd);
    END IF;

    l_period_cd := WWI_REF.FN_FISCAL_PERIOD(p_run_dt, p_region_cd);

    BEGIN
        WWI_FIN.PRC_LOAD_INVOICE_INTERFACE(p_region_cd, l_loaded_cnt, l_rejected_cnt);
    EXCEPTION
        WHEN OTHERS THEN
            p_error_cnt := p_error_cnt + 1;
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RUN_NIGHTLY_AP.load',
                                                 p_region_cd, SQLERRM);
    END;

    BEGIN
        WWI_FIN.PKG_AP_INVOICE.validate_batch(p_region_cd, 100000,
                                              l_validated_cnt, l_held_cnt);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_error_cnt := p_error_cnt + 1;
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RUN_NIGHTLY_AP.validate',
                                                 p_region_cd, SQLERRM);
    END;

    /* APAC accrues receipts nightly because the shared service centre keys
       invoices two days behind; NA and EU only accrue at period end.      */
    IF p_region_cd = 'APAC' THEN
        BEGIN
            WWI_FIN.PRC_ACCRUE_UNINVOICED_RECEIPTS(p_region_cd, l_period_cd,
                                                   'N', l_accrued_cnt);
        EXCEPTION
            WHEN OTHERS THEN
                p_error_cnt := p_error_cnt + 1;
                WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RUN_NIGHTLY_AP.accrue',
                                                     p_region_cd, SQLERRM);
        END;
    END IF;

    BEGIN
        WWI_FIN.PKG_GL_POSTING.post_pending_journals(p_region_cd, l_period_cd,
                                                     l_posted_cnt, l_failed_cnt);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_error_cnt := p_error_cnt + 1;
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RUN_NIGHTLY_AP.post',
                                                 p_region_cd, SQLERRM);
    END;

    IF l_failed_cnt > 0 THEN
        WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL, 'WWI_FIN.GL_JOURNAL_HDR',
                                              p_region_cd || '/' || l_period_cd,
                                              'JOURNAL_POST_FAILED',
                                              l_failed_cnt || ' journal(s) not posted',
                                              'E');
    END IF;

    /* the operators read this from the job log every morning */
    DBMS_OUTPUT.PUT_LINE(p_region_cd || ' ' || l_period_cd
                         || ' loaded=' || l_loaded_cnt
                         || ' rejected=' || l_rejected_cnt
                         || ' validated=' || l_validated_cnt
                         || ' held=' || l_held_cnt
                         || ' accrued=' || l_accrued_cnt
                         || ' posted=' || l_posted_cnt
                         || ' failed=' || l_failed_cnt);
EXCEPTION
    WHEN OTHERS THEN
        p_error_cnt := p_error_cnt + 1;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RUN_NIGHTLY_AP',
                                             p_region_cd, SQLERRM);
        RAISE;
END PRC_RUN_NIGHTLY_AP;
/
