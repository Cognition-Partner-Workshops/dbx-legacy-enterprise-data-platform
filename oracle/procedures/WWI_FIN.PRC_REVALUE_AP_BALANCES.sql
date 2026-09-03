/* ============================================================================
 * Object      : WWI_FIN.PRC_REVALUE_AP_BALANCES (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.AP_INVOICE_HDR, WWI_FIN.AP_PAYMENT_APPLY,
 *               WWI_REF.PKG_FX, WWI_FIN.PKG_GL_POSTING, WWI_FIN.GL_ACCOUNT,
 *               WWI_REF.FN_FISCAL_PERIOD, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : WWI_FIN.PRC_RUN_PERIOD_CLOSE
 * Notes       : Unrealised gain/loss on open foreign currency payables. The
 *               journal is created as an accrual so PKG_GL_POSTING reverses
 *               it on the first day of the next period.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_FIN.PRC_REVALUE_AP_BALANCES
(
    p_region_cd   IN  VARCHAR2,
    p_period_cd   IN  WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE,
    p_journal_id  OUT WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE,
    p_net_gain_amt OUT NUMBER
)
IS
    CURSOR c_open_fx IS
        SELECT h.CURRENCY_CD,
               SUM(NVL(h.GROSS_AMT, 0)
                   - NVL((SELECT SUM(a.APPLIED_AMT)
                            FROM WWI_FIN.AP_PAYMENT_APPLY a
                           WHERE a.INVOICE_ID = h.INVOICE_ID
                             AND NVL(a.VOIDED_FLAG, 'N') = 'N'), 0)) AS OPEN_AMT,
               SUM(NVL(h.BASE_AMT, 0)
                   - NVL((SELECT SUM(a.APPLIED_BASE_AMT)
                            FROM WWI_FIN.AP_PAYMENT_APPLY a
                           WHERE a.INVOICE_ID = h.INVOICE_ID
                             AND NVL(a.VOIDED_FLAG, 'N') = 'N'), 0)) AS BOOKED_BASE_AMT
          FROM WWI_FIN.AP_INVOICE_HDR h
         WHERE h.REGION_CD = p_region_cd
           AND h.STATUS_CD NOT IN ('CN', 'PD')
         GROUP BY h.CURRENCY_CD;

    l_base_ccy    VARCHAR2(3);
    l_rate        NUMBER;
    l_revalued    NUMBER;
    l_delta       NUMBER;
    l_gain_acct   WWI_FIN.GL_ACCOUNT.ACCOUNT_CD%TYPE;
    l_loss_acct   WWI_FIN.GL_ACCOUNT.ACCOUNT_CD%TYPE;
    l_liab_acct   WWI_FIN.GL_ACCOUNT.ACCOUNT_CD%TYPE;
    l_line_cnt    PLS_INTEGER := 0;
BEGIN
    p_net_gain_amt := 0;

    IF WWI_FIN.PKG_GL_POSTING.period_status(p_period_cd, p_region_cd) <> 'OPEN' THEN
        RAISE_APPLICATION_ERROR(-20603,
            'PRC_REVALUE_AP_BALANCES: period ' || p_period_cd || ' is not open for '
            || p_region_cd);
    END IF;

    l_base_ccy := CASE p_region_cd
                      WHEN 'EU'   THEN 'EUR'
                      WHEN 'APAC' THEN 'SGD'
                      ELSE 'USD'
                  END;

    /* the account map was never centralised; each region keeps its own */
    l_gain_acct := CASE p_region_cd WHEN 'EU' THEN '7401' WHEN 'APAC' THEN '7411' ELSE '7400' END;
    l_loss_acct := CASE p_region_cd WHEN 'EU' THEN '7501' WHEN 'APAC' THEN '7511' ELSE '7500' END;
    l_liab_acct := CASE p_region_cd WHEN 'EU' THEN '2101' WHEN 'APAC' THEN '2111' ELSE '2100' END;

    p_journal_id := WWI_FIN.PKG_GL_POSTING.create_journal_header('AP', 'REVAL',
                                                                 p_region_cd,
                                                                 SYSDATE, 'Y');

    FOR rec IN c_open_fx LOOP
        IF rec.CURRENCY_CD = l_base_ccy OR NVL(rec.OPEN_AMT, 0) = 0 THEN
            CONTINUE;
        END IF;

        BEGIN
            l_rate := WWI_REF.PKG_FX.month_end_rate(rec.CURRENCY_CD, l_base_ccy,
                                                    p_period_cd);
        EXCEPTION
            WHEN OTHERS THEN
                WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL, 'WWI_REF.FX_RATE_DAILY',
                    rec.CURRENCY_CD || '/' || l_base_ccy, 'NO_MONTH_END_RATE',
                    'revaluation skipped for ' || p_period_cd, 'E');
                CONTINUE;
        END;

        l_revalued := WWI_REF.PKG_FX.round_to_minor_unit(rec.OPEN_AMT * l_rate,
                                                         l_base_ccy);
        l_delta    := l_revalued - NVL(rec.BOOKED_BASE_AMT, 0);

        IF l_delta = 0 THEN
            CONTINUE;
        END IF;

        IF l_delta > 0 THEN
            /* payable worth more in base currency: unrealised loss */
            WWI_FIN.PKG_GL_POSTING.add_journal_line(p_journal_id, l_loss_acct, NULL,
                l_base_ccy, l_delta, 0,
                'FX reval ' || rec.CURRENCY_CD || ' ' || p_period_cd, 'REVAL', NULL);
            WWI_FIN.PKG_GL_POSTING.add_journal_line(p_journal_id, l_liab_acct, NULL,
                l_base_ccy, 0, l_delta,
                'FX reval ' || rec.CURRENCY_CD || ' ' || p_period_cd, 'REVAL', NULL);
        ELSE
            WWI_FIN.PKG_GL_POSTING.add_journal_line(p_journal_id, l_liab_acct, NULL,
                l_base_ccy, ABS(l_delta), 0,
                'FX reval ' || rec.CURRENCY_CD || ' ' || p_period_cd, 'REVAL', NULL);
            WWI_FIN.PKG_GL_POSTING.add_journal_line(p_journal_id, l_gain_acct, NULL,
                l_base_ccy, 0, ABS(l_delta),
                'FX reval ' || rec.CURRENCY_CD || ' ' || p_period_cd, 'REVAL', NULL);
        END IF;

        p_net_gain_amt := p_net_gain_amt - l_delta;
        l_line_cnt     := l_line_cnt + 2;
    END LOOP;

    IF l_line_cnt = 0 THEN
        DELETE FROM WWI_FIN.GL_JOURNAL_HDR WHERE JOURNAL_ID = p_journal_id;
        p_journal_id := NULL;
    ELSE
        WWI_FIN.PKG_GL_POSTING.post_journal(p_journal_id);
    END IF;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_REVALUE_AP_BALANCES',
                                             p_region_cd || '/' || p_period_cd,
                                             SQLERRM);
        RAISE;
END PRC_REVALUE_AP_BALANCES;
/
