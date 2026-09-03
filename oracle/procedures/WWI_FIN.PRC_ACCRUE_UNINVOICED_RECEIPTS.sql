/* ============================================================================
 * Object      : WWI_FIN.PRC_ACCRUE_UNINVOICED_RECEIPTS (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PO_RECEIPT_LINE, WWI_PROC.PO_RECEIPT_HDR,
 *               WWI_PROC.PURCHASE_ORDER_LINE, WWI_PROC.PURCHASE_ORDER_HDR,
 *               WWI_FIN.AP_INVOICE_LINE, WWI_FIN.PKG_GL_POSTING,
 *               WWI_REF.PKG_FX, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : WWI_FIN.PRC_RUN_PERIOD_CLOSE, WWI_FIN.PRC_RUN_NIGHTLY_AP (APAC)
 * Notes       : Goods received not invoiced. The journal is flagged as an
 *               accrual so it is reversed on the first day of the following
 *               period by WWI_FIN.PKG_GL_POSTING.reverse_accruals.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_FIN.PRC_ACCRUE_UNINVOICED_RECEIPTS
(
    p_region_cd   IN  VARCHAR2,
    p_period_cd   IN  WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE,
    p_dry_run     IN  VARCHAR2 DEFAULT 'N',
    p_accrued_cnt OUT PLS_INTEGER
)
IS
    CURSOR c_grni IS
        SELECT rl.RECEIPT_LINE_ID,
               rl.PO_LINE_ID,
               pl.PRODUCT_ID,
               pl.UNIT_PRICE_AMT,
               ph.CURRENCY_CD,
               ph.PO_ID,
               pl.ACCRUAL_ACCOUNT_CD,
               NVL(rl.ACCEPTED_QTY, rl.RECEIVED_QTY) AS ACCRUABLE_QTY,
               NVL((SELECT SUM(il.MATCHED_QTY)
                      FROM WWI_FIN.AP_INVOICE_LINE il
                     WHERE il.PO_LINE_ID = rl.PO_LINE_ID), 0) AS INVOICED_QTY
          FROM WWI_PROC.PO_RECEIPT_LINE rl
          JOIN WWI_PROC.PO_RECEIPT_HDR rh
            ON rh.RECEIPT_ID = rl.RECEIPT_ID
          JOIN WWI_PROC.PURCHASE_ORDER_LINE pl
            ON pl.PO_LINE_ID = rl.PO_LINE_ID
          JOIN WWI_PROC.PURCHASE_ORDER_HDR ph
            ON ph.PO_ID = pl.PO_ID
         WHERE ph.REGION_CD = p_region_cd
           AND rh.RECEIPT_DT <= LAST_DAY(TO_DATE(SUBSTR(p_period_cd, 1, 7)
                                                 || '-01', 'YYYY-MM-DD'))
           AND NVL(rl.ACCRUAL_POSTED_FLAG, 'N') = 'N'
           AND NVL(rl.ACCEPTED_QTY, rl.RECEIVED_QTY) > 0
         ORDER BY rl.RECEIPT_LINE_ID;

    TYPE t_grni_tab IS TABLE OF c_grni%ROWTYPE;
    TYPE t_id_tab IS TABLE OF WWI_PROC.PO_RECEIPT_LINE.RECEIPT_LINE_ID%TYPE;

    l_batch      t_grni_tab;
    l_posted_ids t_id_tab := t_id_tab();
    l_journal_id WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE;
    l_open_qty   NUMBER;
    l_accr_amt   NUMBER;
    l_base_amt   NUMBER;
    l_total_amt  NUMBER := 0;
    l_base_ccy   VARCHAR2(3);
    l_grni_acct  WWI_FIN.GL_ACCOUNT.ACCOUNT_CD%TYPE;
    l_min_amt    NUMBER;
BEGIN
    p_accrued_cnt := 0;

    l_base_ccy := CASE p_region_cd
                      WHEN 'EU'   THEN 'EUR'
                      WHEN 'APAC' THEN 'SGD'
                      ELSE 'USD'
                  END;

    /* EU accrues everything; NA and APAC ignore small balances because the
       reversal noise was not worth the audit questions                    */
    l_min_amt := CASE p_region_cd WHEN 'EU' THEN 0 WHEN 'APAC' THEN 50 ELSE 25 END;

    l_grni_acct := CASE p_region_cd
                       WHEN 'EU'   THEN '2201'
                       WHEN 'APAC' THEN '2211'
                       ELSE '2200'
                   END;

    l_journal_id := WWI_FIN.PKG_GL_POSTING.create_journal_header('AP', 'ACCRUAL',
                                                                 p_region_cd,
                                                                 SYSDATE, 'Y');

    OPEN c_grni;
    LOOP
        FETCH c_grni BULK COLLECT INTO l_batch LIMIT 500;
        EXIT WHEN l_batch.COUNT = 0;

        FOR i IN 1 .. l_batch.COUNT LOOP
            l_open_qty := l_batch(i).ACCRUABLE_QTY - l_batch(i).INVOICED_QTY;

            IF l_open_qty <= 0 THEN
                CONTINUE;
            END IF;

            l_accr_amt := ROUND(l_open_qty * NVL(l_batch(i).UNIT_PRICE_AMT, 0), 2);

            IF l_accr_amt < l_min_amt THEN
                CONTINUE;
            END IF;

            l_base_amt := WWI_REF.PKG_FX.round_to_minor_unit(
                              l_accr_amt
                              * WWI_REF.PKG_FX.get_rate(l_batch(i).CURRENCY_CD,
                                                        l_base_ccy, SYSDATE,
                                                        'CORP'),
                              l_base_ccy);

            WWI_FIN.PKG_GL_POSTING.add_journal_line(l_journal_id,
                NVL(l_batch(i).ACCRUAL_ACCOUNT_CD, '5000'), NULL, l_base_ccy,
                l_base_amt, 0,
                'GRNI accrual receipt line ' || l_batch(i).RECEIPT_LINE_ID,
                'RECEIPT', l_batch(i).RECEIPT_LINE_ID);

            l_total_amt   := l_total_amt + l_base_amt;
            p_accrued_cnt := p_accrued_cnt + 1;

            l_posted_ids.EXTEND;
            l_posted_ids(l_posted_ids.COUNT) := l_batch(i).RECEIPT_LINE_ID;
        END LOOP;
    END LOOP;
    CLOSE c_grni;

    IF p_accrued_cnt = 0 THEN
        DELETE FROM WWI_FIN.GL_JOURNAL_HDR WHERE JOURNAL_ID = l_journal_id;
        COMMIT;
        RETURN;
    END IF;

    WWI_FIN.PKG_GL_POSTING.add_journal_line(l_journal_id, l_grni_acct, NULL,
        l_base_ccy, 0, l_total_amt,
        'GRNI accrual ' || p_period_cd || ' ' || p_region_cd, 'ACCRUAL', NULL);

    IF NVL(p_dry_run, 'N') = 'Y' THEN
        ROLLBACK;
        RETURN;
    END IF;

    WWI_FIN.PKG_GL_POSTING.post_journal(l_journal_id);

    FORALL i IN 1 .. l_posted_ids.COUNT
        UPDATE WWI_PROC.PO_RECEIPT_LINE
           SET ACCRUAL_POSTED_FLAG = 'Y',
               ACCRUAL_PERIOD_CD   = p_period_cd,
               LAST_UPD_DT         = SYSDATE
         WHERE RECEIPT_LINE_ID = l_posted_ids(i);

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        IF c_grni%ISOPEN THEN
            CLOSE c_grni;
        END IF;
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_ACCRUE_UNINVOICED_RECEIPTS',
                                             p_region_cd || '/' || p_period_cd,
                                             SQLERRM);
        RAISE;
END PRC_ACCRUE_UNINVOICED_RECEIPTS;
/
