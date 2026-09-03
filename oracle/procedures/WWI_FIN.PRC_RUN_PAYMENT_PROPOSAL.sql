/* ============================================================================
 * Object      : WWI_FIN.PRC_RUN_PAYMENT_PROPOSAL (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.PKG_AP_PAYMENT, WWI_FIN.AP_PAYMENT,
 *               WWI_MDM.SUPP_BANK_ACCOUNT, WWI_MDM.PKG_SUPPLIER_MASTER,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : DBMS_JOB 'ERP_PAYMENT_RUN' (NA Tue/Fri, EU Thu, APAC 15th
 *               and month end)
 * Notes       : Builds the proposal, drops anything the region will not pay
 *               electronically, and leaves the run in proposal status. A
 *               human releases it; the job never pays.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_FIN.PRC_RUN_PAYMENT_PROPOSAL
(
    p_region_cd    IN  VARCHAR2,
    p_pay_thru_dt  IN  DATE DEFAULT TRUNC(SYSDATE) + 7,
    p_run_id       OUT WWI_FIN.AP_PAYMENT.PAYMENT_RUN_ID%TYPE,
    p_selected_cnt OUT PLS_INTEGER,
    p_dropped_cnt  OUT PLS_INTEGER
)
IS
    CURSOR c_proposed (p_run NUMBER) IS
        SELECT p.PAYMENT_ID, p.SUPP_ID, p.PAYMENT_AMT, p.PAYMENT_METHOD_CD
          FROM WWI_FIN.AP_PAYMENT p
         WHERE p.PAYMENT_RUN_ID = p_run
           AND p.STATUS_CD = 'PROP'
         ORDER BY p.SUPP_ID;

    l_total_amt   NUMBER;
    l_bank_cnt    PLS_INTEGER;
    l_cert_status VARCHAR2(20);
    l_max_amt     NUMBER;
BEGIN
    p_dropped_cnt := 0;

    WWI_FIN.PKG_AP_PAYMENT.build_payment_proposal(p_region_cd, p_pay_thru_dt,
                                                  p_run_id, p_selected_cnt,
                                                  l_total_amt);

    /* single payment ceiling above which treasury wants a manual wire */
    l_max_amt := CASE p_region_cd
                     WHEN 'EU'   THEN 250000
                     WHEN 'APAC' THEN 100000
                     ELSE 500000
                 END;

    FOR rec IN c_proposed(p_run_id) LOOP
        SELECT COUNT(*)
          INTO l_bank_cnt
          FROM WWI_MDM.SUPP_BANK_ACCOUNT b
         WHERE b.SUPP_ID = rec.SUPP_ID
           AND NVL(b.ACTIVE_FLAG, 'Y') = 'Y'
           AND NVL(b.VERIFIED_FLAG, 'N') = 'Y';

        IF l_bank_cnt = 0 AND rec.PAYMENT_METHOD_CD <> 'CHECK' THEN
            UPDATE WWI_FIN.AP_PAYMENT
               SET STATUS_CD      = 'DROP',
                   VOID_REASON_CD = 'NO_VERIFIED_BANK',
                   LAST_UPD_DT    = SYSDATE
             WHERE PAYMENT_ID = rec.PAYMENT_ID;

            p_dropped_cnt := p_dropped_cnt + 1;
            WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL, 'WWI_FIN.AP_PAYMENT',
                TO_CHAR(rec.PAYMENT_ID), 'NO_VERIFIED_BANK',
                'supplier ' || rec.SUPP_ID || ' has no verified bank account', 'E');
            CONTINUE;
        END IF;

        IF NVL(rec.PAYMENT_AMT, 0) > l_max_amt THEN
            UPDATE WWI_FIN.AP_PAYMENT
               SET STATUS_CD      = 'MANUAL',
                   LAST_UPD_DT    = SYSDATE
             WHERE PAYMENT_ID = rec.PAYMENT_ID;

            p_dropped_cnt := p_dropped_cnt + 1;
            CONTINUE;
        END IF;

        /* APAC will not pay a supplier whose local registration lapsed */
        IF p_region_cd = 'APAC' THEN
            l_cert_status :=
                WWI_MDM.PKG_SUPPLIER_MASTER.certification_status(rec.SUPP_ID,
                                                                 'LOCALREG');

            IF l_cert_status IN ('MISSING', 'EXPIRED') THEN
                UPDATE WWI_FIN.AP_PAYMENT
                   SET STATUS_CD      = 'DROP',
                       VOID_REASON_CD = 'CERT_' || l_cert_status,
                       LAST_UPD_DT    = SYSDATE
                 WHERE PAYMENT_ID = rec.PAYMENT_ID;

                p_dropped_cnt := p_dropped_cnt + 1;
            END IF;
        END IF;
    END LOOP;

    p_selected_cnt := p_selected_cnt - p_dropped_cnt;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RUN_PAYMENT_PROPOSAL',
                                             p_region_cd, SQLERRM);
        RAISE;
END PRC_RUN_PAYMENT_PROPOSAL;
/
