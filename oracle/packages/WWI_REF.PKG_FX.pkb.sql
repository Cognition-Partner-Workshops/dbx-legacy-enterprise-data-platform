/* ============================================================================
 * Object      : WWI_REF.PKG_FX (package body)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_REF.PKG_FX, WWI_REF.FX_RATE_DAILY, WWI_REF.CURRENCY_CODE,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_REF.PKG_FX AS

    /* how far back each rate type may be carried forward when the loader has
       not delivered a rate for the requested date                          */
    FUNCTION backoff_days (p_rate_type_cd IN VARCHAR2) RETURN PLS_INTEGER
    IS
    BEGIN
        RETURN CASE p_rate_type_cd
                   WHEN 'SPOT'  THEN 1
                   WHEN 'CORP'  THEN 7
                   WHEN 'MEND'  THEN 0
                   ELSE 5
               END;
    END backoff_days;

    FUNCTION direct_rate
    (
        p_from_ccy     IN WWI_REF.FX_RATE_DAILY.FROM_CURRENCY_CD%TYPE,
        p_to_ccy       IN WWI_REF.FX_RATE_DAILY.TO_CURRENCY_CD%TYPE,
        p_rate_dt      IN DATE,
        p_rate_type_cd IN WWI_REF.FX_RATE_DAILY.RATE_TYPE_CD%TYPE
    ) RETURN NUMBER
    IS
        l_rate NUMBER;
    BEGIN
        SELECT RATE_NUM
          INTO l_rate
          FROM (SELECT r.RATE_NUM
                  FROM WWI_REF.FX_RATE_DAILY r
                 WHERE r.FROM_CURRENCY_CD = p_from_ccy
                   AND r.TO_CURRENCY_CD   = p_to_ccy
                   AND r.RATE_TYPE_CD     = p_rate_type_cd
                   AND r.RATE_DT BETWEEN TRUNC(p_rate_dt)
                                         - backoff_days(p_rate_type_cd)
                                     AND TRUNC(p_rate_dt)
                 ORDER BY r.RATE_DT DESC)
         WHERE ROWNUM = 1;

        RETURN l_rate;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
    END direct_rate;

    FUNCTION get_rate
    (
        p_from_ccy     IN WWI_REF.FX_RATE_DAILY.FROM_CURRENCY_CD%TYPE,
        p_to_ccy       IN WWI_REF.FX_RATE_DAILY.TO_CURRENCY_CD%TYPE,
        p_rate_dt      IN DATE DEFAULT TRUNC(SYSDATE),
        p_rate_type_cd IN WWI_REF.FX_RATE_DAILY.RATE_TYPE_CD%TYPE DEFAULT 'CORP'
    ) RETURN NUMBER
    IS
        l_rate     NUMBER;
        l_leg_from NUMBER;
        l_leg_to   NUMBER;
    BEGIN
        IF p_from_ccy = p_to_ccy THEN
            RETURN 1;
        END IF;

        l_rate := direct_rate(p_from_ccy, p_to_ccy, p_rate_dt, p_rate_type_cd);
        IF l_rate IS NOT NULL THEN
            RETURN l_rate;
        END IF;

        /* some sources only ever publish one side of the pair */
        l_rate := direct_rate(p_to_ccy, p_from_ccy, p_rate_dt, p_rate_type_cd);
        IF l_rate IS NOT NULL AND l_rate <> 0 THEN
            RETURN ROUND(1 / l_rate, 10);
        END IF;

        /* last resort: triangulate through USD */
        l_leg_from := direct_rate(p_from_ccy, c_pivot_currency, p_rate_dt, p_rate_type_cd);
        l_leg_to   := direct_rate(c_pivot_currency, p_to_ccy, p_rate_dt, p_rate_type_cd);

        IF l_leg_from IS NULL THEN
            l_leg_from := direct_rate(c_pivot_currency, p_from_ccy, p_rate_dt,
                                      p_rate_type_cd);
            IF l_leg_from IS NOT NULL AND l_leg_from <> 0 THEN
                l_leg_from := 1 / l_leg_from;
            END IF;
        END IF;

        IF l_leg_from IS NOT NULL AND l_leg_to IS NOT NULL THEN
            RETURN ROUND(l_leg_from * l_leg_to, 10);
        END IF;

        RAISE_APPLICATION_ERROR(-20401,
            'PKG_FX.get_rate: no ' || p_rate_type_cd || ' rate for '
            || p_from_ccy || '/' || p_to_ccy || ' on '
            || TO_CHAR(p_rate_dt, 'YYYY-MM-DD'));
    END get_rate;

    FUNCTION month_end_rate
    (
        p_from_ccy  IN WWI_REF.FX_RATE_DAILY.FROM_CURRENCY_CD%TYPE,
        p_to_ccy    IN WWI_REF.FX_RATE_DAILY.TO_CURRENCY_CD%TYPE,
        p_period_cd IN VARCHAR2
    ) RETURN NUMBER
    IS
        l_period_end DATE;
    BEGIN
        l_period_end := LAST_DAY(TO_DATE(p_period_cd, 'YYYY-MM'));
        RETURN get_rate(p_from_ccy, p_to_ccy, l_period_end, 'MEND');
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE = -20401 THEN
                /* month-end close will not wait for treasury: fall back to
                   the corporate rate and let the reval report show it       */
                RETURN get_rate(p_from_ccy, p_to_ccy, l_period_end, 'CORP');
            END IF;
            RAISE;
    END month_end_rate;

    FUNCTION round_to_minor_unit
    (
        p_amount      IN NUMBER,
        p_currency_cd IN WWI_REF.CURRENCY_CODE.CURRENCY_CD%TYPE
    ) RETURN NUMBER
    IS
        l_minor PLS_INTEGER;
    BEGIN
        SELECT NVL(MINOR_UNIT_NUM, 2)
          INTO l_minor
          FROM WWI_REF.CURRENCY_CODE
         WHERE CURRENCY_CD = p_currency_cd;

        RETURN ROUND(p_amount, l_minor);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20402,
                'PKG_FX.round_to_minor_unit: unknown currency ' || p_currency_cd);
    END round_to_minor_unit;

    PROCEDURE upsert_rate
    (
        p_from_ccy     IN WWI_REF.FX_RATE_DAILY.FROM_CURRENCY_CD%TYPE,
        p_to_ccy       IN WWI_REF.FX_RATE_DAILY.TO_CURRENCY_CD%TYPE,
        p_rate_dt      IN DATE,
        p_rate_type_cd IN WWI_REF.FX_RATE_DAILY.RATE_TYPE_CD%TYPE,
        p_rate_num     IN WWI_REF.FX_RATE_DAILY.RATE_NUM%TYPE,
        p_src_system_cd    IN WWI_REF.FX_RATE_DAILY.SRC_SYSTEM_CD%TYPE
    )
    IS
        l_prior NUMBER;
    BEGIN
        IF NVL(p_rate_num, 0) <= 0 THEN
            RAISE_APPLICATION_ERROR(-20401,
                'PKG_FX.upsert_rate: rate must be positive');
        END IF;

        l_prior := direct_rate(p_from_ccy, p_to_ccy, TRUNC(p_rate_dt) - 1,
                               p_rate_type_cd);

        /* a move of more than 10% overnight is almost always a bad feed file */
        IF l_prior IS NOT NULL
           AND ABS(p_rate_num - l_prior) / l_prior > 0.10 THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_FX.upsert_rate',
                                                 p_from_ccy || '/' || p_to_ccy,
                                                 'rate moved from ' || l_prior
                                                 || ' to ' || p_rate_num);
        END IF;

        MERGE INTO WWI_REF.FX_RATE_DAILY t
        USING (SELECT p_from_ccy     AS FROM_CURRENCY_CD,
                      p_to_ccy       AS TO_CURRENCY_CD,
                      TRUNC(p_rate_dt) AS RATE_DT,
                      p_rate_type_cd AS RATE_TYPE_CD
                 FROM DUAL) s
           ON (    t.FROM_CURRENCY_CD = s.FROM_CURRENCY_CD
               AND t.TO_CURRENCY_CD   = s.TO_CURRENCY_CD
               AND t.RATE_DT          = s.RATE_DT
               AND t.RATE_TYPE_CD     = s.RATE_TYPE_CD)
         WHEN MATCHED THEN
            UPDATE SET t.RATE_NUM    = p_rate_num,
                       t.SRC_SYSTEM_CD   = p_src_system_cd,
                       t.LAST_UPD_DT = SYSDATE
         WHEN NOT MATCHED THEN
            INSERT (FROM_CURRENCY_CD, TO_CURRENCY_CD, RATE_DT, RATE_TYPE_CD,
                    RATE_NUM, INVERSE_RATE_NUM, SRC_SYSTEM_CD, LOADED_DT, LAST_UPD_DT)
            VALUES (s.FROM_CURRENCY_CD, s.TO_CURRENCY_CD, s.RATE_DT, s.RATE_TYPE_CD,
                    p_rate_num, ROUND(1 / p_rate_num, 10), p_src_system_cd,
                    SYSDATE, SYSDATE);
    END upsert_rate;

    PROCEDURE check_rate_freshness
    (
        p_max_age_days IN  PLS_INTEGER DEFAULT 3,
        p_stale_cnt    OUT PLS_INTEGER
    )
    IS
        CURSOR c_pairs IS
            SELECT r.FROM_CURRENCY_CD, r.TO_CURRENCY_CD, r.RATE_TYPE_CD,
                   MAX(r.RATE_DT) AS LAST_RATE_DT
              FROM WWI_REF.FX_RATE_DAILY r
              JOIN WWI_REF.CURRENCY_CODE c
                ON c.CURRENCY_CD = r.FROM_CURRENCY_CD
             WHERE NVL(c.ACTIVE_FLAG, 'Y') = 'Y'
               AND r.RATE_TYPE_CD IN ('CORP', 'SPOT')
             GROUP BY r.FROM_CURRENCY_CD, r.TO_CURRENCY_CD, r.RATE_TYPE_CD
            HAVING MAX(r.RATE_DT) < TRUNC(SYSDATE) - p_max_age_days;
    BEGIN
        p_stale_cnt := 0;

        FOR rec IN c_pairs LOOP
            p_stale_cnt := p_stale_cnt + 1;
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_FX.check_rate_freshness',
                                                 rec.FROM_CURRENCY_CD || '/'
                                                 || rec.TO_CURRENCY_CD,
                                                 'last ' || rec.RATE_TYPE_CD
                                                 || ' rate is '
                                                 || TO_CHAR(rec.LAST_RATE_DT,
                                                            'YYYY-MM-DD'));
        END LOOP;
    END check_rate_freshness;

END PKG_FX;
/
