/* ============================================================================
 * Object      : WWI_AUDIT.PRC_RUN_DQ_CHECKS (procedure)
 * Schema      : WWI_AUDIT
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_AUDIT.PKG_DATA_QUALITY, WWI_REF.PKG_FX,
 *               WWI_REF.PKG_CODE_TRANSLATION, WWI_FIN.AP_INVOICE_HDR,
 *               WWI_MDM.SUPP_BANK_ACCOUNT, WWI_AUDIT.INTERFACE_ERROR
 * Called by   : DBMS_JOB 'ERP_DQ_NIGHTLY' (03:30), run before the extracts
 * Notes       : The checks accumulated one incident at a time; each block
 *               below traces back to a specific production problem, which is
 *               why they do not share a common rule engine.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_AUDIT.PRC_RUN_DQ_CHECKS
(
    p_region_cd IN  VARCHAR2 DEFAULT NULL,
    p_issue_cnt OUT PLS_INTEGER
)
IS
    l_orphans   PLS_INTEGER := 0;
    l_stale_fx  PLS_INTEGER := 0;
    l_unmapped  PLS_INTEGER := 0;
    l_negatives PLS_INTEGER := 0;
    l_dup_bank  PLS_INTEGER := 0;
    l_set_cnt   PLS_INTEGER := 0;
BEGIN
    p_issue_cnt := 0;

    WWI_AUDIT.PKG_DATA_QUALITY.run_orphan_checks(p_region_cd, l_orphans);

    WWI_REF.PKG_FX.check_rate_freshness(3, l_stale_fx);

    FOR cs IN (SELECT DISTINCT CODE_SET_CD
                 FROM WWI_REF.CODE_TRANSLATION
                WHERE NVL(ACTIVE_FLAG, 'Y') = 'Y') LOOP
        WWI_REF.PKG_CODE_TRANSLATION.report_unmapped(cs.CODE_SET_CD, l_set_cnt);
        l_unmapped := l_unmapped + l_set_cnt;
    END LOOP;

    /* negative gross invoices that are not flagged as credit notes; the 2011
       supplier rebate load created several thousand of them                */
    FOR rec IN (SELECT h.INVOICE_ID, h.GROSS_AMT
                  FROM WWI_FIN.AP_INVOICE_HDR h
                 WHERE h.GROSS_AMT < 0
                   AND NVL(h.CREDIT_NOTE_FLAG, 'N') <> 'Y'
                   AND (p_region_cd IS NULL OR h.REGION_CD = p_region_cd)
                   AND h.STATUS_CD <> 'CN') LOOP
        WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL, 'WWI_FIN.AP_INVOICE_HDR',
            TO_CHAR(rec.INVOICE_ID), 'NEGATIVE_NOT_CREDIT',
            'gross ' || rec.GROSS_AMT || ' but credit note flag is not Y', 'W');
        l_negatives := l_negatives + 1;
    END LOOP;

    /* the same bank account registered against more than one supplier is a
       fraud signal and is escalated at severity F                        */
    FOR rec IN (SELECT b.ACCOUNT_MASK_TXT, COUNT(DISTINCT b.SUPP_ID) AS SUPP_CNT
                  FROM WWI_MDM.SUPP_BANK_ACCOUNT b
                 WHERE NVL(b.ACTIVE_FLAG, 'Y') = 'Y'
                   AND b.ACCOUNT_MASK_TXT IS NOT NULL
                 GROUP BY b.ACCOUNT_MASK_TXT
                HAVING COUNT(DISTINCT b.SUPP_ID) > 1) LOOP
        WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL, 'WWI_MDM.SUPP_BANK_ACCOUNT',
            rec.ACCOUNT_MASK_TXT, 'SHARED_BANK_ACCOUNT',
            'account shared by ' || rec.SUPP_CNT || ' suppliers', 'F');
        l_dup_bank := l_dup_bank + 1;
    END LOOP;

    p_issue_cnt := l_orphans + l_stale_fx + l_unmapped + l_negatives + l_dup_bank;

    DBMS_OUTPUT.PUT_LINE('DQ ' || NVL(p_region_cd, 'ALL')
                         || ' orphans=' || l_orphans
                         || ' stale_fx=' || l_stale_fx
                         || ' unmapped=' || l_unmapped
                         || ' negative_invoices=' || l_negatives
                         || ' shared_bank=' || l_dup_bank);

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RUN_DQ_CHECKS',
                                             NVL(p_region_cd, 'ALL'), SQLERRM);
        RAISE;
END PRC_RUN_DQ_CHECKS;
/
