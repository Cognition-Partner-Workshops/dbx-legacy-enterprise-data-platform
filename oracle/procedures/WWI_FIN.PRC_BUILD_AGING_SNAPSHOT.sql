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
               h.INVOICE_CURR_CD AS CURRENCY_CD,
               h.INVOICE_DT,
               h.DUE_DT,
               h.GROSS_AMT,
               NVL((SELECT SUM(a.APPLIED_AMT)
                      FROM WWI_FIN.AP_PAYMENT_APPLY a
                     WHERE a.INVOICE_ID = h.INVOICE_ID
                       AND a.APPLY_DT <= p_as_of_dt
                       AND NVL(a.REVERSED_FLG, 'N') = 'N'), 0) AS APPLIED_AMT
          FROM WWI_FIN.AP_INVOICE_HDR h
         WHERE h.REGION_CD = p_region_cd
           AND h.INVOICE_STATUS_CD NOT IN ('CN', 'PD')
           AND h.INVOICE_DT <= p_as_of_dt
         ORDER BY h.SUPP_ID, h.DUE_DT;

    TYPE t_inv_tab IS TABLE OF c_open_invoices%ROWTYPE;

    /* the snapshot is held per supplier and currency, so the row by row pass
       accumulates the buckets before a single row per group is written      */
    TYPE t_bucket_rec IS RECORD (
        supp_id      WWI_FIN.AP_INVOICE_HDR.SUPP_ID%TYPE,
        curr_cd      WWI_FIN.AP_INVOICE_HDR.INVOICE_CURR_CD%TYPE,
        current_amt  NUMBER := 0,
        bucket_1_amt NUMBER := 0,
        bucket_2_amt NUMBER := 0,
        bucket_3_amt NUMBER := 0,
        bucket_4_amt NUMBER := 0,
        total_amt    NUMBER := 0,
        base_amt     NUMBER := 0,
        invoice_cnt  PLS_INTEGER := 0,
        days_sum     NUMBER := 0,
        oldest_dt    DATE
    );
    TYPE t_bucket_tab IS TABLE OF t_bucket_rec INDEX BY VARCHAR2(64);

    l_buckets     t_bucket_tab;
    l_key         VARCHAR2(64);
    l_agg         t_bucket_rec;
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

            l_key := l_batch(i).SUPP_ID || '~' || l_batch(i).CURRENCY_CD;

            IF l_buckets.EXISTS(l_key) THEN
                l_agg := l_buckets(l_key);
            ELSE
                l_agg.oldest_dt   := NULL;
                l_agg.supp_id     := l_batch(i).SUPP_ID;
                l_agg.curr_cd     := l_batch(i).CURRENCY_CD;
                l_agg.current_amt := 0;
                l_agg.bucket_1_amt := 0;
                l_agg.bucket_2_amt := 0;
                l_agg.bucket_3_amt := 0;
                l_agg.bucket_4_amt := 0;
                l_agg.total_amt   := 0;
                l_agg.base_amt    := 0;
                l_agg.invoice_cnt := 0;
                l_agg.days_sum    := 0;
            END IF;

            CASE
                WHEN l_days_late <= 0  THEN l_agg.current_amt  := l_agg.current_amt + l_open_amt;
                WHEN l_days_late <= 30 THEN l_agg.bucket_1_amt := l_agg.bucket_1_amt + l_open_amt;
                WHEN l_days_late <= 60 THEN l_agg.bucket_2_amt := l_agg.bucket_2_amt + l_open_amt;
                WHEN l_days_late <= 90 THEN l_agg.bucket_3_amt := l_agg.bucket_3_amt + l_open_amt;
                ELSE                        l_agg.bucket_4_amt := l_agg.bucket_4_amt + l_open_amt;
            END CASE;

            l_agg.total_amt   := l_agg.total_amt + l_open_amt;
            l_agg.base_amt    := l_agg.base_amt + l_base_amt;
            l_agg.invoice_cnt := l_agg.invoice_cnt + 1;
            l_agg.days_sum    := l_agg.days_sum + l_days_late;
            l_agg.oldest_dt   := LEAST(NVL(l_agg.oldest_dt, l_batch(i).INVOICE_DT),
                                       l_batch(i).INVOICE_DT);

            l_buckets(l_key) := l_agg;
        END LOOP;
    END LOOP;
    CLOSE c_open_invoices;

    l_key := l_buckets.FIRST;
    WHILE l_key IS NOT NULL LOOP
        l_agg := l_buckets(l_key);

        INSERT INTO WWI_FIN.AP_AGING_SNAPSHOT
            (SNAPSHOT_ID, SNAPSHOT_DT, PERIOD_CD, SUPP_ID, REGION_CD,
             BALANCE_CURR_CD, CURRENT_AMT, BUCKET_1_AMT, BUCKET_2_AMT,
             BUCKET_3_AMT, BUCKET_4_AMT, TOTAL_OUTSTANDING_AMT,
             OPEN_INVOICE_CNT, OLDEST_INVOICE_DT, AVG_DAYS_OUTSTANDING,
             BUCKET_DEFINITION_CD, REPORTING_AMT_USD, CALCULATED_DT,
             CREATED_DT, CREATED_BY)
        VALUES
            (WWI_FIN.SEQ_AP_AGING_SNAPSHOT.NEXTVAL, p_as_of_dt, l_period_cd,
             l_agg.supp_id, p_region_cd, l_agg.curr_cd, l_agg.current_amt,
             l_agg.bucket_1_amt, l_agg.bucket_2_amt, l_agg.bucket_3_amt,
             l_agg.bucket_4_amt, l_agg.total_amt, l_agg.invoice_cnt,
             l_agg.oldest_dt,
             ROUND(l_agg.days_sum / GREATEST(l_agg.invoice_cnt, 1), 2),
             'STD-30-60-90', l_agg.base_amt, SYSDATE, SYSDATE, USER);

        p_row_cnt := p_row_cnt + 1;
        l_key := l_buckets.NEXT(l_key);
    END LOOP;

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
