/* ============================================================================
 * Object      : WWI_PROC.PKG_SUPPLIER_PERF (package body)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_PROC.PKG_SUPPLIER_PERF, WWI_PROC.SUPPLIER_SCORECARD,
 *               WWI_PROC.PO_RECEIPT_HDR, WWI_PROC.PO_RECEIPT_LINE,
 *               WWI_PROC.PURCHASE_ORDER_HDR, WWI_PROC.PURCHASE_ORDER_LINE,
 *               WWI_PROC.GOODS_RETURN_LINE, WWI_MDM.SUPP_MASTER,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_PROC.PKG_SUPPLIER_PERF AS

    FUNCTION on_time_pct
    (
        p_supp_id   IN WWI_PROC.SUPPLIER_SCORECARD.SUPP_ID%TYPE,
        p_from_dt   IN DATE,
        p_to_dt     IN DATE,
        p_region_cd IN VARCHAR2
    ) RETURN NUMBER
    IS
        l_grace_days PLS_INTEGER;
        l_total      PLS_INTEGER;
        l_on_time    PLS_INTEGER;
    BEGIN
        /* the "on time" window is not the same anywhere: NA allows two days
           late, EU demands the exact need-by date, APAC allows a week because
           of customs clearance                                              */
        l_grace_days := CASE p_region_cd
                            WHEN 'EU'   THEN 0
                            WHEN 'APAC' THEN 7
                            ELSE 2
                        END;

        SELECT COUNT(*),
               SUM(CASE WHEN rh.RECEIPT_DT <= pl.NEED_BY_DT + l_grace_days
                        THEN 1 ELSE 0 END)
          INTO l_total, l_on_time
          FROM WWI_PROC.PO_RECEIPT_HDR rh
          JOIN WWI_PROC.PO_RECEIPT_LINE rl ON rl.RECEIPT_ID = rh.RECEIPT_ID
          JOIN WWI_PROC.PURCHASE_ORDER_LINE pl ON pl.PO_LINE_ID = rl.PO_LINE_ID
         WHERE rh.SUPP_ID = p_supp_id
           AND rh.RECEIPT_DT BETWEEN p_from_dt AND p_to_dt;

        IF NVL(l_total, 0) = 0 THEN
            RAISE_APPLICATION_ERROR(-20321,
                'PKG_SUPPLIER_PERF.on_time_pct: no receipts for supplier '
                || p_supp_id || ' in the period');
        END IF;

        RETURN ROUND(l_on_time * 100 / l_total, 2);
    END on_time_pct;

    FUNCTION quality_pct
    (
        p_supp_id IN WWI_PROC.SUPPLIER_SCORECARD.SUPP_ID%TYPE,
        p_from_dt IN DATE,
        p_to_dt   IN DATE
    ) RETURN NUMBER
    IS
        l_received NUMBER;
        l_bad      NUMBER;
    BEGIN
        SELECT NVL(SUM(rl.RECEIVED_QTY), 0),
               NVL(SUM(NVL(rl.REJECTED_QTY, 0)), 0)
                 + NVL(SUM((SELECT NVL(SUM(g.RETURN_QTY), 0)
                              FROM WWI_PROC.GOODS_RETURN_LINE g
                             WHERE g.RECEIPT_LINE_ID = rl.RECEIPT_LINE_ID)), 0)
          INTO l_received, l_bad
          FROM WWI_PROC.PO_RECEIPT_HDR rh
          JOIN WWI_PROC.PO_RECEIPT_LINE rl ON rl.RECEIPT_ID = rh.RECEIPT_ID
         WHERE rh.SUPP_ID = p_supp_id
           AND rh.RECEIPT_DT BETWEEN p_from_dt AND p_to_dt;

        IF l_received = 0 THEN
            RETURN NULL;
        END IF;

        RETURN ROUND((l_received - l_bad) * 100 / l_received, 2);
    END quality_pct;

    FUNCTION composite_score
    (
        p_region_cd   IN VARCHAR2,
        p_on_time_pct IN NUMBER,
        p_quality_pct IN NUMBER,
        p_price_var   IN NUMBER
    ) RETURN NUMBER
    IS
        l_price_score NUMBER;
    BEGIN
        l_price_score := GREATEST(0, 100 - ABS(NVL(p_price_var, 0)) * 5);

        IF p_region_cd = 'EU' THEN
            /* EU weights quality hardest after the 2015 recall */
            RETURN ROUND(NVL(p_on_time_pct, 0) * 0.30
                       + NVL(p_quality_pct, 0) * 0.55
                       + l_price_score        * 0.15, 2);
        ELSIF p_region_cd = 'APAC' THEN
            RETURN ROUND(NVL(p_on_time_pct, 0) * 0.50
                       + NVL(p_quality_pct, 0) * 0.30
                       + l_price_score        * 0.20, 2);
        END IF;

        RETURN ROUND(NVL(p_on_time_pct, 0) * 0.40
                   + NVL(p_quality_pct, 0) * 0.35
                   + l_price_score        * 0.25, 2);
    END composite_score;

    PROCEDURE build_scorecards
    (
        p_period_cd  IN  WWI_PROC.SUPPLIER_SCORECARD.PERIOD_CD%TYPE,
        p_region_cd  IN  VARCHAR2,
        p_built_cnt  OUT PLS_INTEGER
    )
    IS
        CURSOR c_suppliers IS
            SELECT s.SUPP_ID, s.REGION_CD
              FROM WWI_MDM.SUPP_MASTER s
             WHERE s.REGION_CD = p_region_cd
               AND s.STATUS_CD IN ('A', 'P')
             ORDER BY s.SUPP_ID;

        TYPE t_supp_tab IS TABLE OF c_suppliers%ROWTYPE INDEX BY PLS_INTEGER;
        l_supps    t_supp_tab;
        l_from_dt  DATE;
        l_to_dt    DATE;
        l_on_time  NUMBER;
        l_quality  NUMBER;
        l_price    NUMBER;
        l_score    NUMBER;
        l_rating   VARCHAR2(20);
    BEGIN
        p_built_cnt := 0;

        BEGIN
            l_from_dt := TO_DATE(p_period_cd, 'YYYY-MM');
        EXCEPTION
            WHEN OTHERS THEN
                RAISE_APPLICATION_ERROR(-20322,
                    'PKG_SUPPLIER_PERF.build_scorecards: period ' || p_period_cd
                    || ' is not in YYYY-MM form');
        END;

        l_to_dt := LAST_DAY(l_from_dt);

        OPEN c_suppliers;
        LOOP
            FETCH c_suppliers BULK COLLECT INTO l_supps LIMIT c_bulk_limit;
            EXIT WHEN l_supps.COUNT = 0;

            FOR i IN 1 .. l_supps.COUNT LOOP
                BEGIN
                    l_on_time := on_time_pct(l_supps(i).SUPP_ID, l_from_dt, l_to_dt,
                                             l_supps(i).REGION_CD);
                EXCEPTION
                    WHEN OTHERS THEN
                        l_on_time := NULL;
                END;

                l_quality := quality_pct(l_supps(i).SUPP_ID, l_from_dt, l_to_dt);

                SELECT NVL(AVG(WWI_PROC.FN_RECEIPT_VARIANCE_PCT(rl.RECEIPT_LINE_ID)), 0)
                  INTO l_price
                  FROM WWI_PROC.PO_RECEIPT_HDR rh
                  JOIN WWI_PROC.PO_RECEIPT_LINE rl ON rl.RECEIPT_ID = rh.RECEIPT_ID
                 WHERE rh.SUPP_ID = l_supps(i).SUPP_ID
                   AND rh.RECEIPT_DT BETWEEN l_from_dt AND l_to_dt;

                IF l_on_time IS NULL AND l_quality IS NULL THEN
                    CONTINUE;   /* no activity - leave last month's card standing */
                END IF;

                l_score := composite_score(l_supps(i).REGION_CD, l_on_time,
                                           l_quality, l_price);

                l_rating := CASE
                                WHEN l_supps(i).REGION_CD = 'EU' THEN
                                    CASE WHEN l_score >= 85 THEN 'PREFERRED'
                                         WHEN l_score >= 65 THEN 'APPROVED'
                                         ELSE 'REVIEW' END
                                WHEN l_supps(i).REGION_CD = 'APAC' THEN
                                    CASE WHEN l_score >= 80 THEN 'PREFERRED'
                                         WHEN l_score >= 60 THEN 'APPROVED'
                                         ELSE 'REVIEW' END
                                ELSE
                                    CASE WHEN l_score >= 90 THEN 'PREFERRED'
                                         WHEN l_score >= 70 THEN 'APPROVED'
                                         ELSE 'REVIEW' END
                            END;

                MERGE INTO WWI_PROC.SUPPLIER_SCORECARD t
                USING (SELECT l_supps(i).SUPP_ID AS SUPP_ID,
                              p_period_cd        AS PERIOD_CD
                         FROM DUAL) s
                   ON (t.SUPP_ID = s.SUPP_ID AND t.PERIOD_CD = s.PERIOD_CD)
                 WHEN MATCHED THEN
                    UPDATE SET t.ON_TIME_PCT     = l_on_time,
                               t.QUALITY_PCT     = l_quality,
                               t.PRICE_VAR_PCT   = l_price,
                               t.COMPOSITE_SCORE = l_score,
                               t.RATING_CD       = l_rating,
                               t.LAST_UPD_DT     = SYSDATE
                 WHEN NOT MATCHED THEN
                    INSERT (SCORECARD_ID, SUPP_ID, PERIOD_CD, REGION_CD, ON_TIME_PCT,
                            QUALITY_PCT, PRICE_VAR_PCT, COMPOSITE_SCORE, RATING_CD,
                            CREATED_DT, LAST_UPD_DT)
                    VALUES (WWI_PROC.SEQ_SUPPLIER_SCORECARD.NEXTVAL, s.SUPP_ID,
                            s.PERIOD_CD, l_supps(i).REGION_CD, l_on_time, l_quality,
                            l_price, l_score, l_rating, SYSDATE, SYSDATE);

                p_built_cnt := p_built_cnt + 1;
            END LOOP;

            COMMIT;
            EXIT WHEN c_suppliers%NOTFOUND;
        END LOOP;
        CLOSE c_suppliers;
    EXCEPTION
        WHEN OTHERS THEN
            IF c_suppliers%ISOPEN THEN
                CLOSE c_suppliers;
            END IF;
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_SUPPLIER_PERF.build_scorecards',
                                                 p_region_cd || '/' || p_period_cd,
                                                 SQLERRM);
            RAISE;
    END build_scorecards;

END PKG_SUPPLIER_PERF;
/
