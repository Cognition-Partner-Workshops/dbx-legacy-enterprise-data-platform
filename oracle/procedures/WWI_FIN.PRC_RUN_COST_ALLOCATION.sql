/* ============================================================================
 * Object      : WWI_FIN.PRC_RUN_COST_ALLOCATION (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.COST_ALLOCATION_RULE, WWI_FIN.COST_CENTER,
 *               WWI_FIN.GL_JOURNAL_LINE, WWI_FIN.PKG_GL_POSTING,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : WWI_FIN.PRC_RUN_PERIOD_CLOSE
 * Notes       : Rules are applied in RULE_SEQ order and a rule may allocate
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
    CURSOR c_rules IS
        SELECT r.ALLOCATION_RULE_ID,
               r.RULE_CD,
               r.RULE_SEQ,
               r.SOURCE_ACCOUNT_CD,
               r.SOURCE_COST_CENTER_ID,
               r.TARGET_ACCOUNT_CD,
               r.BASIS_CD
          FROM WWI_FIN.COST_ALLOCATION_RULE r
         WHERE r.REGION_CD = p_region_cd
           AND NVL(r.ACTIVE_FLAG, 'Y') = 'Y'
         ORDER BY r.RULE_SEQ;

    CURSOR c_targets (p_rule_id NUMBER) IS
        SELECT t.COST_CENTER_ID, t.ALLOCATION_PCT
          FROM WWI_FIN.COST_ALLOCATION_RULE t
         WHERE t.PARENT_RULE_ID = p_rule_id
           AND NVL(t.ACTIVE_FLAG, 'Y') = 'Y';

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
        IF rule.SOURCE_ACCOUNT_CD IS NULL THEN
            CONTINUE;
        END IF;

        SELECT NVL(SUM(NVL(l.DEBIT_AMT, 0) - NVL(l.CREDIT_AMT, 0)), 0)
          INTO l_pool_amt
          FROM WWI_FIN.GL_JOURNAL_LINE l
          JOIN WWI_FIN.GL_JOURNAL_HDR h
            ON h.JOURNAL_ID = l.JOURNAL_ID
         WHERE h.REGION_CD = p_region_cd
           AND h.PERIOD_CD = p_period_cd
           AND h.STATUS_CD = 'P'
           AND l.ACCOUNT_CD = rule.SOURCE_ACCOUNT_CD
           AND (rule.SOURCE_COST_CENTER_ID IS NULL
                OR l.COST_CENTER_ID = rule.SOURCE_COST_CENTER_ID);

        IF l_pool_amt = 0 THEN
            CONTINUE;
        END IF;

        SELECT NVL(SUM(t.ALLOCATION_PCT), 0)
          INTO l_pct_total
          FROM WWI_FIN.COST_ALLOCATION_RULE t
         WHERE t.PARENT_RULE_ID = rule.ALLOCATION_RULE_ID
           AND NVL(t.ACTIVE_FLAG, 'Y') = 'Y';

        IF l_pct_total = 0 THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL,
                'WWI_FIN.COST_ALLOCATION_RULE', rule.RULE_CD, 'NO_TARGETS',
                'rule has no active target rows', 'E');
            CONTINUE;
        END IF;

        IF ROUND(l_pct_total, 4) <> 100 THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL,
                'WWI_FIN.COST_ALLOCATION_RULE', rule.RULE_CD, 'PCT_NOT_100',
                'basis totals ' || l_pct_total || '%, allocated pro-rata', 'W');
        END IF;

        l_alloc_total := 0;

        FOR tgt IN c_targets(rule.ALLOCATION_RULE_ID) LOOP
            l_share_amt := ROUND(l_pool_amt * tgt.ALLOCATION_PCT / l_pct_total, 2);
            l_alloc_total := l_alloc_total + l_share_amt;

            WWI_FIN.PKG_GL_POSTING.add_journal_line(l_journal_id,
                NVL(rule.TARGET_ACCOUNT_CD, rule.SOURCE_ACCOUNT_CD),
                tgt.COST_CENTER_ID, l_base_ccy,
                GREATEST(l_share_amt, 0), GREATEST(-l_share_amt, 0),
                'ALLOC ' || rule.RULE_CD || ' ' || p_period_cd, 'ALLOC',
                rule.ALLOCATION_RULE_ID);

            p_line_cnt := p_line_cnt + 1;
        END LOOP;

        /* rounding difference stays in the source cost centre */
        WWI_FIN.PKG_GL_POSTING.add_journal_line(l_journal_id,
            rule.SOURCE_ACCOUNT_CD, rule.SOURCE_COST_CENTER_ID, l_base_ccy,
            GREATEST(-l_alloc_total, 0), GREATEST(l_alloc_total, 0),
            'ALLOC offset ' || rule.RULE_CD, 'ALLOC', rule.ALLOCATION_RULE_ID);

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
