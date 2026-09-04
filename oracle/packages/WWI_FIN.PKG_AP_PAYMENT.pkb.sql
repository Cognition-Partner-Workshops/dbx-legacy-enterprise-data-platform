/* ============================================================================
 * Object      : WWI_FIN.PKG_AP_PAYMENT (package body)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_FIN.PKG_AP_PAYMENT, WWI_FIN.AP_PAYMENT,
 *               WWI_FIN.AP_PAYMENT_APPLY, WWI_FIN.AP_INVOICE_HDR,
 *               WWI_FIN.WITHHOLDING_RULE, WWI_MDM.SUPP_BANK_ACCOUNT,
 *               WWI_FIN.PKG_GL_POSTING, WWI_AUDIT.PKG_DATA_QUALITY,
 *               WWI_FIN.FN_CONVERT_AMOUNT
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_FIN.PKG_AP_PAYMENT AS

    FUNCTION discount_available
    (
        p_invoice_id IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_pay_dt     IN DATE DEFAULT SYSDATE
    ) RETURN NUMBER
    IS
        l_disc_dt   WWI_FIN.AP_INVOICE_HDR.DISCOUNT_DUE_DT%TYPE;
        l_disc_amt  WWI_FIN.AP_INVOICE_HDR.DISCOUNT_TAKEN_AMT%TYPE;
        l_region_cd WWI_FIN.AP_INVOICE_HDR.REGION_CD%TYPE;
    BEGIN
        SELECT DISCOUNT_DUE_DT, NVL(DISCOUNT_TAKEN_AMT, 0), REGION_CD
          INTO l_disc_dt, l_disc_amt, l_region_cd
          FROM WWI_FIN.AP_INVOICE_HDR
         WHERE INVOICE_ID = p_invoice_id;

        IF l_disc_dt IS NULL OR l_disc_amt = 0 THEN
            RETURN 0;
        END IF;

        /* NA allows a three-day grace on the discount date because cheques
           were posted; EU is strict; APAC never took discounts at all.     */
        IF l_region_cd = 'APAC' THEN
            RETURN 0;
        ELSIF l_region_cd = 'NA' AND TRUNC(p_pay_dt) <= l_disc_dt + 3 THEN
            RETURN l_disc_amt;
        ELSIF TRUNC(p_pay_dt) <= l_disc_dt THEN
            RETURN l_disc_amt;
        END IF;

        RETURN 0;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 0;
    END discount_available;

    FUNCTION withholding_amount
    (
        p_supp_id     IN WWI_FIN.AP_INVOICE_HDR.SUPP_ID%TYPE,
        p_region_cd   IN VARCHAR2,
        p_gross_amt   IN NUMBER,
        p_income_type IN VARCHAR2 DEFAULT 'SERVICES'
    ) RETURN NUMBER
    IS
        l_rate      WWI_FIN.WITHHOLDING_RULE.WHT_RATE_PCT%TYPE;
        l_threshold WWI_FIN.WITHHOLDING_RULE.THRESHOLD_AMT%TYPE;
        l_exempt    VARCHAR2(1);
    BEGIN
        SELECT NVL(w.WHT_RATE_PCT, 0),
               NVL(w.THRESHOLD_AMT, 0),
               CASE WHEN NVL(s.WITHHOLDING_FLG, 'N') = 'Y' THEN 'N' ELSE 'Y' END
          INTO l_rate, l_threshold, l_exempt
          FROM WWI_FIN.WITHHOLDING_RULE w
          JOIN WWI_MDM.SUPP_MASTER s
            ON s.SUPP_ID = p_supp_id
         WHERE w.REGION_CD     = p_region_cd
           AND w.INCOME_TYPE_CD = p_income_type
           AND w.COUNTRY_CD     = s.COUNTRY_CD
           AND TRUNC(SYSDATE) BETWEEN w.EFFECTIVE_FROM_DT
                                  AND NVL(w.EFFECTIVE_TO_DT, DATE '4712-12-31');

        IF l_exempt = 'Y' OR p_gross_amt < l_threshold THEN
            RETURN 0;
        END IF;

        RETURN ROUND(p_gross_amt * l_rate / 100, 2);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            /* no rule on file means no withholding - this has been the
               behaviour since 2006 and the tax team is aware of it        */
            RETURN 0;
        WHEN TOO_MANY_ROWS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_AP_PAYMENT.withholding_amount',
                                                 TO_CHAR(p_supp_id),
                                                 'multiple withholding rules matched');
            RETURN 0;
    END withholding_amount;

    PROCEDURE build_payment_proposal
    (
        p_region_cd    IN  VARCHAR2,
        p_pay_thru_dt  IN  DATE,
        p_run_id       OUT WWI_FIN.AP_PAYMENT.PAYMENT_BATCH_NBR%TYPE,
        p_selected_cnt OUT PLS_INTEGER,
        p_total_amt    OUT NUMBER
    )
    IS
        CURSOR c_due IS
            SELECT i.INVOICE_ID, i.SUPP_ID, i.INVOICE_CURR_CD, i.DUE_DT,
                   i.GROSS_AMT - NVL(i.PAID_AMT, 0) AS OPEN_AMT
              FROM WWI_FIN.AP_INVOICE_HDR i
             WHERE i.INVOICE_STATUS_CD IN ('AP', 'PP')
               AND i.REGION_CD = p_region_cd
               AND i.DUE_DT   <= p_pay_thru_dt
               AND i.GROSS_AMT - NVL(i.PAID_AMT, 0) > 0
               AND NOT EXISTS (SELECT 1
                                 FROM WWI_FIN.AP_INVOICE_HOLD h
                                WHERE h.INVOICE_ID = i.INVOICE_ID
                                  AND h.RELEASED_DT IS NULL)
             ORDER BY i.SUPP_ID, i.DUE_DT;

        TYPE t_due_tab IS TABLE OF c_due%ROWTYPE INDEX BY PLS_INTEGER;
        l_due          t_due_tab;
        l_payment_id   WWI_FIN.AP_PAYMENT.PAYMENT_ID%TYPE;
        l_prev_supp    WWI_FIN.AP_INVOICE_HDR.SUPP_ID%TYPE := NULL;
        l_prev_ccy     WWI_FIN.AP_INVOICE_HDR.INVOICE_CURR_CD%TYPE := NULL;
        l_bank_acct_id WWI_MDM.SUPP_BANK_ACCOUNT.SUPP_BANK_ID%TYPE;
        l_method_cd    WWI_FIN.AP_PAYMENT.PAYMENT_METHOD_CD%TYPE;
        l_discount     NUMBER;
        l_withholding  NUMBER;
        l_net_amt      NUMBER;
    BEGIN
        p_selected_cnt := 0;
        p_total_amt    := 0;
        p_run_id       := WWI_FIN.SEQ_PAYMENT_RUN.NEXTVAL;

        /* payment method by region - the EU entity moved to SEPA credit
           transfer in 2010, APAC still issues local bank transfers and NA
           prints cheques for anyone without an ACH mandate                 */
        l_method_cd := CASE p_region_cd
                           WHEN 'EU'   THEN 'SEPA'
                           WHEN 'APAC' THEN 'LOCALXFER'
                           ELSE 'ACH'
                       END;

        OPEN c_due;
        LOOP
            FETCH c_due BULK COLLECT INTO l_due LIMIT c_bulk_limit;
            EXIT WHEN l_due.COUNT = 0;

            FOR i IN 1 .. l_due.COUNT LOOP
                IF l_prev_supp IS NULL
                   OR l_due(i).SUPP_ID <> l_prev_supp
                   OR l_due(i).INVOICE_CURR_CD <> l_prev_ccy THEN

                    BEGIN
                        SELECT b.SUPP_BANK_ID
                          INTO l_bank_acct_id
                          FROM WWI_MDM.SUPP_BANK_ACCOUNT b
                         WHERE b.SUPP_ID     = l_due(i).SUPP_ID
                           AND b.ACCOUNT_CURR_CD = l_due(i).INVOICE_CURR_CD
                           AND NVL(b.ACTIVE_FLG, 'N') = 'Y'
                           AND ROWNUM = 1;
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            IF p_region_cd = 'NA' THEN
                                l_bank_acct_id := NULL;   /* cheque fallback */
                                l_method_cd    := 'CHECK';
                            ELSE
                                WWI_AUDIT.PKG_DATA_QUALITY.log_error(
                                    'PKG_AP_PAYMENT.build_payment_proposal',
                                    TO_CHAR(l_due(i).SUPP_ID),
                                    'no active bank account for currency '
                                    || l_due(i).INVOICE_CURR_CD);
                                CONTINUE;
                            END IF;
                    END;

                    l_payment_id := WWI_FIN.SEQ_AP_PAYMENT.NEXTVAL;

                    INSERT INTO WWI_FIN.AP_PAYMENT
                        (PAYMENT_ID, PAYMENT_NBR, SUPP_ID, REGION_CD, PAYMENT_DT,
                         PAYMENT_CURR_CD, PAYMENT_AMT, WITHHELD_AMT, DISCOUNT_TAKEN_AMT,
                         PAYMENT_METHOD_CD, SUPP_BANK_ACCOUNT_ID, PAYMENT_BATCH_NBR,
                         PAYMENT_STATUS_CD, CREATED_DT, CREATED_BY, UPDATED_DT)
                    VALUES
                        (l_payment_id, 'PRP' || TO_CHAR(l_payment_id), l_due(i).SUPP_ID,
                         p_region_cd, TRUNC(SYSDATE), l_due(i).INVOICE_CURR_CD, 0, 0, 0,
                         l_method_cd, l_bank_acct_id, p_run_id, 'PROPOSED',
                         SYSDATE, USER, SYSDATE);

                    l_prev_supp := l_due(i).SUPP_ID;
                    l_prev_ccy  := l_due(i).INVOICE_CURR_CD;
                END IF;

                l_discount    := discount_available(l_due(i).INVOICE_ID, TRUNC(SYSDATE));
                l_withholding := withholding_amount(l_due(i).SUPP_ID, p_region_cd,
                                                    l_due(i).OPEN_AMT);
                l_net_amt     := l_due(i).OPEN_AMT - l_discount - l_withholding;

                INSERT INTO WWI_FIN.AP_PAYMENT_APPLY
                    (APPLY_ID, PAYMENT_ID, INVOICE_ID, APPLIED_AMT, DISCOUNT_AMT,
                     WITHHELD_AMT, APPLY_DT, REVERSED_FLG, CREATED_BY)
                VALUES
                    (WWI_FIN.SEQ_AP_PAYMENT_APPLY.NEXTVAL, l_payment_id,
                     l_due(i).INVOICE_ID, l_net_amt, l_discount,
                     l_withholding, TRUNC(SYSDATE), 'N', USER);

                UPDATE WWI_FIN.AP_PAYMENT
                   SET PAYMENT_AMT        = PAYMENT_AMT + l_net_amt,
                       WITHHELD_AMT       = WITHHELD_AMT + l_withholding,
                       DISCOUNT_TAKEN_AMT = DISCOUNT_TAKEN_AMT + l_discount,
                       UPDATED_DT        = SYSDATE
                 WHERE PAYMENT_ID = l_payment_id;

                p_selected_cnt := p_selected_cnt + 1;
                p_total_amt    := p_total_amt + l_net_amt;
            END LOOP;

            EXIT WHEN c_due%NOTFOUND;
        END LOOP;
        CLOSE c_due;
    EXCEPTION
        WHEN OTHERS THEN
            IF c_due%ISOPEN THEN
                CLOSE c_due;
            END IF;
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_AP_PAYMENT.build_payment_proposal',
                                                 p_region_cd, SQLERRM);
            RAISE;
    END build_payment_proposal;

    PROCEDURE apply_payment
    (
        p_payment_id IN WWI_FIN.AP_PAYMENT.PAYMENT_ID%TYPE,
        p_invoice_id IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_amount     IN NUMBER
    )
    IS
        l_open_amt   NUMBER;
        l_unapplied  NUMBER;
        l_void_flag  WWI_FIN.AP_PAYMENT.VOID_REASON_CD%TYPE;
    BEGIN
        SELECT NVL(p.VOID_REASON_CD, 'N'),
               p.PAYMENT_AMT - NVL((SELECT SUM(a.APPLIED_AMT)
                                      FROM WWI_FIN.AP_PAYMENT_APPLY a
                                     WHERE a.PAYMENT_ID = p.PAYMENT_ID
                                       AND NVL(a.REVERSED_FLG, 'N') = 'N'), 0)
          INTO l_void_flag, l_unapplied
          FROM WWI_FIN.AP_PAYMENT p
         WHERE p.PAYMENT_ID = p_payment_id
           FOR UPDATE;

        IF l_void_flag = 'Y' THEN
            RAISE_APPLICATION_ERROR(-20114,
                'PKG_AP_PAYMENT.apply_payment: payment ' || p_payment_id || ' is void');
        END IF;

        SELECT GROSS_AMT - NVL(PAID_AMT, 0)
          INTO l_open_amt
          FROM WWI_FIN.AP_INVOICE_HDR
         WHERE INVOICE_ID = p_invoice_id
           FOR UPDATE;

        IF p_amount > l_open_amt + 0.01 THEN
            RAISE_APPLICATION_ERROR(-20112,
                'PKG_AP_PAYMENT.apply_payment: amount ' || p_amount
                || ' exceeds open balance ' || l_open_amt);
        END IF;

        IF p_amount > l_unapplied + 0.01 THEN
            RAISE_APPLICATION_ERROR(-20112,
                'PKG_AP_PAYMENT.apply_payment: amount exceeds unapplied payment cash');
        END IF;

        INSERT INTO WWI_FIN.AP_PAYMENT_APPLY
            (APPLY_ID, PAYMENT_ID, INVOICE_ID, APPLIED_AMT, DISCOUNT_AMT, WITHHELD_AMT,
             APPLY_DT, REVERSED_FLG, CREATED_BY)
        VALUES
            (WWI_FIN.SEQ_AP_PAYMENT_APPLY.NEXTVAL, p_payment_id, p_invoice_id,
             p_amount, 0, 0, SYSDATE, 'N', USER);

        UPDATE WWI_FIN.AP_INVOICE_HDR
           SET PAID_AMT    = NVL(PAID_AMT, 0) + p_amount,
               INVOICE_STATUS_CD   = CASE
                                 WHEN NVL(PAID_AMT, 0) + p_amount >= GROSS_AMT - 0.01
                                     THEN 'PD'
                                 ELSE 'PP'
                             END,
               UPDATED_DT = SYSDATE,
               UPDATED_BY = USER
         WHERE INVOICE_ID = p_invoice_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20111,
                'PKG_AP_PAYMENT.apply_payment: payment or invoice not found');
    END apply_payment;

    PROCEDURE auto_apply_payment
    (
        p_payment_id  IN  WWI_FIN.AP_PAYMENT.PAYMENT_ID%TYPE,
        p_applied_cnt OUT PLS_INTEGER,
        p_residual    OUT NUMBER
    )
    IS
        CURSOR c_open (cp_supp_id WWI_FIN.AP_INVOICE_HDR.SUPP_ID%TYPE,
                       cp_ccy     WWI_FIN.AP_INVOICE_HDR.INVOICE_CURR_CD%TYPE) IS
            SELECT i.INVOICE_ID, i.GROSS_AMT - NVL(i.PAID_AMT, 0) AS OPEN_AMT
              FROM WWI_FIN.AP_INVOICE_HDR i
             WHERE i.SUPP_ID     = cp_supp_id
               AND i.INVOICE_CURR_CD = cp_ccy
               AND i.INVOICE_STATUS_CD IN ('AP', 'PP')
               AND i.GROSS_AMT - NVL(i.PAID_AMT, 0) > 0
             ORDER BY i.DUE_DT, i.INVOICE_ID;

        l_supp_id  WWI_FIN.AP_PAYMENT.SUPP_ID%TYPE;
        l_ccy      WWI_FIN.AP_PAYMENT.PAYMENT_CURR_CD%TYPE;
        l_apply    NUMBER;
    BEGIN
        p_applied_cnt := 0;

        SELECT p.SUPP_ID,
               p.PAYMENT_CURR_CD,
               p.PAYMENT_AMT - NVL((SELECT SUM(a.APPLIED_AMT)
                                      FROM WWI_FIN.AP_PAYMENT_APPLY a
                                     WHERE a.PAYMENT_ID = p.PAYMENT_ID
                                       AND NVL(a.REVERSED_FLG, 'N') = 'N'), 0)
          INTO l_supp_id, l_ccy, p_residual
          FROM WWI_FIN.AP_PAYMENT p
         WHERE p.PAYMENT_ID = p_payment_id;

        /* oldest-invoice-first application, one row at a time. It has always
           been row-by-row because the clerks wanted to be able to stop it
           halfway through and see partial results.                          */
        FOR r IN c_open(l_supp_id, l_ccy) LOOP
            EXIT WHEN p_residual <= 0.01;

            l_apply := LEAST(r.OPEN_AMT, p_residual);
            apply_payment(p_payment_id, r.INVOICE_ID, l_apply);

            p_residual    := p_residual - l_apply;
            p_applied_cnt := p_applied_cnt + 1;
        END LOOP;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20111,
                'PKG_AP_PAYMENT.auto_apply_payment: payment ' || p_payment_id
                || ' not found');
    END auto_apply_payment;

    PROCEDURE void_payment
    (
        p_payment_id  IN WWI_FIN.AP_PAYMENT.PAYMENT_ID%TYPE,
        p_reason_cd   IN WWI_FIN.AP_PAYMENT.VOID_REASON_CD%TYPE,
        p_voided_by   IN WWI_FIN.AP_PAYMENT.UPDATED_BY%TYPE
    )
    IS
        CURSOR c_applied IS
            SELECT a.INVOICE_ID, a.APPLIED_AMT
              FROM WWI_FIN.AP_PAYMENT_APPLY a
             WHERE a.PAYMENT_ID = p_payment_id
               AND NVL(a.REVERSED_FLG, 'N') = 'N'
               FOR UPDATE;
    BEGIN
        FOR r IN c_applied LOOP
            UPDATE WWI_FIN.AP_INVOICE_HDR
               SET PAID_AMT    = GREATEST(NVL(PAID_AMT, 0) - r.APPLIED_AMT, 0),
                   INVOICE_STATUS_CD   = CASE
                                     WHEN NVL(PAID_AMT, 0) - r.APPLIED_AMT <= 0.01 THEN 'AP'
                                     ELSE 'PP'
                                 END,
                   UPDATED_DT = SYSDATE,
                   UPDATED_BY = p_voided_by
             WHERE INVOICE_ID = r.INVOICE_ID;

            UPDATE WWI_FIN.AP_PAYMENT_APPLY
               SET REVERSED_FLG = 'Y',
                   REVERSAL_DT   = SYSDATE
             WHERE CURRENT OF c_applied;
        END LOOP;

        UPDATE WWI_FIN.AP_PAYMENT
           SET VOID_DT            = SYSDATE,
               VOID_REASON_CD     = p_reason_cd,
               PAYMENT_STATUS_CD  = 'VOID',
               UPDATED_DT    = SYSDATE,
               UPDATED_BY    = p_voided_by
         WHERE PAYMENT_ID = p_payment_id;

        WWI_FIN.PKG_GL_POSTING.reverse_document_journal('AP_PAY', p_payment_id,
                                                        'Payment voided: ' || p_reason_cd);
    EXCEPTION
        WHEN OTHERS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_AP_PAYMENT.void_payment',
                                                 TO_CHAR(p_payment_id), SQLERRM);
            RAISE;
    END void_payment;

END PKG_AP_PAYMENT;
/
