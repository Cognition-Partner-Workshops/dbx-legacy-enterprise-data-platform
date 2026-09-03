/* ============================================================================
 * Object      : WWI_FIN.PRC_BUILD_AGING_SNAPSHOT (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.AP_INVOICE_HDR, WWI_FIN.AP_PAYMENT_APPLY,
 *               WWI_FIN.AP_AGING_SNAPSHOT, WWI_FIN.FN_AGING_BUCKET,
 *               WWI_FIN.FN_CONVERT_AMOUNT, WWI_REF.FN_FISCAL_PERIOD,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : WWI_FIN.PRC_LOAD_AP_AGING, month-end close checklist step 4
 * Notes       : Row-by-row on purpose: the 2003 rewrite that tried to do this
 *               as one INSERT SELECT could not reproduce the bucket rounding
 *               finance signs off on, and was backed out.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_FIN.PRC_BUILD_AGING_SNAPSHOT
(
    p_region_cd   IN  VARCHAR2,
    p_as_of_dt    IN  DATE DEFAULT TRUNC(SYSDATE),
    p_rebuild_flg IN  VARCHAR2 DEFAULT 'N',
    p_row_cnt     OUT PLS_INTEGER
)
IS
    CURSOR c_open_invoices IS
        SELECT h.INVOICE_ID,
               h.SUPP_ID,
               h.REGION_CD,
               h.CURRENCY_CD,
               h.INVOICE_DT,
               h.DUE_DT,
               h.GROSS_AMT,
               NVL((SELECT SUM(a.APPLIED_AMT)
                      FROM WWI_FIN.AP_PAYMENT_APPLY a
                     WHERE a.INVOICE_ID = h.INVOICE_ID
                       AND a.APPLY_DT <= p_as_of_dt
                       AND NVL(a.VOIDED_FLAG, 'N') = 'N'), 0) AS APPLIED_AMT
          FROM WWI_FIN.AP_INVOICE_HDR h
         WHERE h.REGION_CD = p_region_cd
           AND h.STATUS_CD NOT IN ('CN', 'PD')
           AND h.INVOICE_DT <= p_as_of_dt
         ORDER BY h.SUPP_ID, h.DUE_DT;

    TYPE t_inv_tab IS TABLE OF c_open_invoices%ROWTYPE;
    l_batch       t_inv_tab;
    l_period_cd   VARCHAR2(10);
    l_open_amt    NUMBER;
    l_base_amt    NUMBER;
    l_days_late   NUMBER;
    l_bucket_cd   VARCHAR2(20);
    l_base_ccy    VARCHAR2(3);
    l_exists_cnt  PLS_INTEGER;
BEGIN
    p_row_cnt   := 0;
    l_period_cd := WWI_REF.FN_FISCAL_PERIOD(p_as_of_dt, p_region_cd);

    /* reporting currency has never been harmonised */
    l_base_ccy := CASE p_region_cd
                      WHEN 'EU'   THEN 'EUR'
                      WHEN 'APAC' THEN 'SGD'
                      ELSE 'USD'
                  END;

    SELECT COUNT(*)
      INTO l_exists_cnt
      FROM WWI_FIN.AP_AGING_SNAPSHOT
     WHERE REGION_CD = p_region_cd
       AND SNAPSHOT_DT = p_as_of_dt;

    IF l_exists_cnt > 0 THEN
        IF NVL(p_rebuild_flg, 'N') <> 'Y' THEN
            RAISE_APPLICATION_ERROR(-20602,
                'PRC_BUILD_AGING_SNAPSHOT: snapshot for ' || p_region_cd
                || ' on ' || TO_CHAR(p_as_of_dt, 'YYYY-MM-DD')
                || ' already exists; pass p_rebuild_flg = ''Y'' to replace it');
        END IF;

        DELETE FROM WWI_FIN.AP_AGING_SNAPSHOT
         WHERE REGION_CD = p_region_cd
           AND SNAPSHOT_DT = p_as_of_dt;
    END IF;

    OPEN c_open_invoices;
    LOOP
        FETCH c_open_invoices BULK COLLECT INTO l_batch LIMIT 500;
        EXIT WHEN l_batch.COUNT = 0;

        FOR i IN 1 .. l_batch.COUNT LOOP
            l_open_amt := NVL(l_batch(i).GROSS_AMT, 0) - l_batch(i).APPLIED_AMT;

            /* fully applied invoices whose header was never flipped to PD
               are a known data issue; they are logged, not suppressed     */
            IF l_open_amt <= 0 THEN
                WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL,
                    'WWI_FIN.AP_INVOICE_HDR', TO_CHAR(l_batch(i).INVOICE_ID),
                    'OPEN_ZERO_BALANCE',
                    'invoice not closed but balance is ' || l_open_amt, 'W');
                CONTINUE;
            END IF;

            l_days_late := TRUNC(p_as_of_dt) - TRUNC(l_batch(i).DUE_DT);
            l_bucket_cd := WWI_FIN.FN_AGING_BUCKET(l_days_late, p_region_cd);

            l_base_amt := WWI_FIN.FN_CONVERT_AMOUNT(l_open_amt,
                                                    l_batch(i).CURRENCY_CD,
                                                    l_base_ccy,
                                                    p_as_of_dt,
                                                    'CORP');

            INSERT INTO WWI_FIN.AP_AGING_SNAPSHOT
                (AGING_SNAPSHOT_ID, SNAPSHOT_DT, PERIOD_CD, REGION_CD, SUPP_ID,
                 INVOICE_ID, CURRENCY_CD, OPEN_AMT, BASE_CURRENCY_CD,
                 BASE_OPEN_AMT, DUE_DT, DAYS_PAST_DUE, AGING_BUCKET_CD,
                 CREATED_DT, CREATED_BY)
            VALUES
                (WWI_FIN.SEQ_AP_AGING_SNAPSHOT.NEXTVAL, p_as_of_dt, l_period_cd,
                 p_region_cd, l_batch(i).SUPP_ID, l_batch(i).INVOICE_ID,
                 l_batch(i).CURRENCY_CD, l_open_amt, l_base_ccy, l_base_amt,
                 l_batch(i).DUE_DT, l_days_late, l_bucket_cd, SYSDATE, USER);

            p_row_cnt := p_row_cnt + 1;
        END LOOP;

        COMMIT;
    END LOOP;
    CLOSE c_open_invoices;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        IF c_open_invoices%ISOPEN THEN
            CLOSE c_open_invoices;
        END IF;
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_BUILD_AGING_SNAPSHOT',
                                             p_region_cd || '/'
                                             || TO_CHAR(p_as_of_dt, 'YYYY-MM-DD'),
                                             SQLERRM);
        RAISE;
END PRC_BUILD_AGING_SNAPSHOT;
/
