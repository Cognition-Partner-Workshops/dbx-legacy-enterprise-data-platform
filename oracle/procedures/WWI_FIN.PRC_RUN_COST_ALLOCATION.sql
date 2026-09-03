/* ============================================================================
 * Object      : WWI_FIN.PRC_RUN_COST_ALLOCATION (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.COST_ALLOCATION_RULE, WWI_FIN.COST_CENTER,
 *               WWI_FIN.GL_JOURNAL_LINE, WWI_FIN.PKG_GL_POSTING,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : WWI_FIN.PRC_RUN_PERIOD_CLOSE
 * Notes       : Rules are applied in RULE_SEQ_NBR order and a rule may allocate
 *               the output of an earlier rule, which is why this cannot be
 *               done in a single statement. Basis percentages that do not
 *               add up to 100 are allocated pro-rata and logged.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_FIN.PRC_RUN_COST_ALLOCATION
(
    p_region_cd  IN  VARCHAR2,
    p_period_cd  IN  WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE,
    p_rule_cnt   OUT PLS_INTEGER,
    p_line_cnt   OUT PLS_INTEGER
)
IS
    /* a rule set is one pool: every active row in the set is a target share */
    CURSOR c_rules IS
        SELECT r.RULE_SET_CD,
               MIN(r.RULE_SEQ_NBR)          AS RULE_SEQ_NBR,
               MIN(r.SOURCE_ACCOUNT_MASK)   AS SOURCE_ACCOUNT_MASK,
               MIN(r.SOURCE_COST_CENTER_CD) AS SOURCE_COST_CENTER_CD,
               MIN(r.TARGET_ACCOUNT_CD)     AS TARGET_ACCOUNT_CD,
               MIN(r.ALLOCATION_METHOD_CD)  AS ALLOCATION_METHOD_CD
          FROM WWI_FIN.COST_ALLOCATION_RULE r
         WHERE r.REGION_CD = p_region_cd
           AND NVL(r.ACTIVE_FLG, 'Y') = 'Y'
         GROUP BY r.RULE_SET_CD
         ORDER BY MIN(r.RULE_SEQ_NBR);

    CURSOR c_targets (p_rule_set_cd VARCHAR2) IS
        SELECT t.TARGET_COST_CENTER_CD, t.TARGET_ACCOUNT_CD, t.ALLOCATION_PCT
          FROM WWI_FIN.COST_ALLOCATION_RULE t
         WHERE t.RULE_SET_CD = p_rule_set_cd
           AND t.REGION_CD = p_region_cd
           AND NVL(t.ACTIVE_FLG, 'Y') = 'Y'
         ORDER BY t.RULE_SEQ_NBR;

    l_journal_id  WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE;
    l_pool_amt    NUMBER;
    l_pct_total   NUMBER;
    l_share_amt   NUMBER;
    l_alloc_total NUMBER;
    l_base_ccy    VARCHAR2(3);
BEGIN
    p_rule_cnt := 0;
    p_line_cnt := 0;

    IF WWI_FIN.PKG_GL_POSTING.period_status(p_period_cd, p_region_cd) <> 'OPEN' THEN
        RAISE_APPLICATION_ERROR(-20604,
            'PRC_RUN_COST_ALLOCATION: period ' || p_period_cd
            || ' is not open for ' || p_region_cd);
    END IF;

    l_base_ccy := CASE p_region_cd
                      WHEN 'EU'   THEN 'EUR'
                      WHEN 'APAC' THEN 'SGD'
                      ELSE 'USD'
                  END;

    l_journal_id := WWI_FIN.PKG_GL_POSTING.create_journal_header('ALLOC', 'ALLOC',
                                                                 p_region_cd,
                                                                 SYSDATE, 'N');

    FOR rule IN c_rules LOOP
        IF rule.SOURCE_ACCOUNT_MASK IS NULL THEN
            CONTINUE;
        END IF;

        SELECT NVL(SUM(NVL(l.ENTERED_DEBIT_AMT, 0) - NVL(l.ENTERED_CREDIT_AMT, 0)), 0)
          INTO l_pool_amt
          FROM WWI_FIN.GL_JOURNAL_LINE l
          JOIN WWI_FIN.GL_JOURNAL_HDR h
            ON h.JOURNAL_ID = l.JOURNAL_ID
         WHERE h.REGION_CD = p_region_cd
           AND h.PERIOD_CD = p_period_cd
           AND h.POSTING_STATUS_CD = 'P'
           AND l.ACCOUNT_CD LIKE rule.SOURCE_ACCOUNT_MASK
           AND (rule.SOURCE_COST_CENTER_CD IS NULL
                OR l.COST_CENTER_CD = rule.SOURCE_COST_CENTER_CD);

        IF l_pool_amt = 0 THEN
            CONTINUE;
        END IF;

        SELECT NVL(SUM(t.ALLOCATION_PCT), 0)
          INTO l_pct_total
          FROM WWI_FIN.COST_ALLOCATION_RULE t
         WHERE t.RULE_SET_CD = rule.RULE_SET_CD
           AND t.REGION_CD = p_region_cd
           AND NVL(t.ACTIVE_FLG, 'Y') = 'Y';

        IF l_pct_total = 0 THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL,
                'WWI_FIN.COST_ALLOCATION_RULE', rule.RULE_SET_CD, 'NO_TARGETS',
                'rule has no active target rows', 'E');
            CONTINUE;
        END IF;

        IF ROUND(l_pct_total, 4) <> 100 THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL,
                'WWI_FIN.COST_ALLOCATION_RULE', rule.RULE_SET_CD, 'PCT_NOT_100',
                'basis totals ' || l_pct_total || '%, allocated pro-rata', 'W');
        END IF;

        l_alloc_total := 0;

        FOR tgt IN c_targets(rule.RULE_SET_CD) LOOP
            l_share_amt := ROUND(l_pool_amt * tgt.ALLOCATION_PCT / l_pct_total, 2);
            l_alloc_total := l_alloc_total + l_share_amt;

            WWI_FIN.PKG_GL_POSTING.add_journal_line(l_journal_id,
                NVL(tgt.TARGET_ACCOUNT_CD, rule.TARGET_ACCOUNT_CD),
                tgt.TARGET_COST_CENTER_CD, l_base_ccy,
                GREATEST(l_share_amt, 0), GREATEST(-l_share_amt, 0),
                'ALLOC ' || rule.RULE_SET_CD || ' ' || p_period_cd, 'ALLOC',
                NULL);

            p_line_cnt := p_line_cnt + 1;
        END LOOP;

        /* rounding difference stays in the source cost centre */
        WWI_FIN.PKG_GL_POSTING.add_journal_line(l_journal_id,
            rule.TARGET_ACCOUNT_CD, rule.SOURCE_COST_CENTER_CD, l_base_ccy,
            GREATEST(-l_alloc_total, 0), GREATEST(l_alloc_total, 0),
            'ALLOC offset ' || rule.RULE_SET_CD, 'ALLOC', NULL);

        p_line_cnt := p_line_cnt + 1;
        p_rule_cnt := p_rule_cnt + 1;
    END LOOP;

    IF p_line_cnt = 0 THEN
        DELETE FROM WWI_FIN.GL_JOURNAL_HDR WHERE JOURNAL_ID = l_journal_id;
    ELSE
        WWI_FIN.PKG_GL_POSTING.post_journal(l_journal_id);
    END IF;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RUN_COST_ALLOCATION',
                                             p_region_cd || '/' || p_period_cd,
                                             SQLERRM);
        RAISE;
END PRC_RUN_COST_ALLOCATION;
/
