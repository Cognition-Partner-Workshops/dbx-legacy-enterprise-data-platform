/* ============================================================================
 * Object      : WWI_REF.PRC_LOAD_FX_RATES (procedure)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.PKG_FX, WWI_REF.CURRENCY_CODE, WWI_REF.FX_RATE_DAILY,
 *               WWI_REF.SOURCE_SYSTEM_REF, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : DBMS_JOB 'REF_FX_DAILY' (06:15 local, before the payment run)
 * Notes       : Corporate rates come from treasury once a month and are held
 *               flat for the whole period; spot rates arrive daily. A missing
 *               spot rate rolls the previous day forward rather than failing,
 *               which is why weekend valuations look flat.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_REF.PRC_LOAD_FX_RATES
(
    p_rate_dt      IN  DATE DEFAULT TRUNC(SYSDATE),
    p_rate_type_cd IN  WWI_REF.FX_RATE_DAILY.RATE_TYPE_CD%TYPE DEFAULT 'SPOT',
    p_loaded_cnt   OUT PLS_INTEGER,
    p_rolled_cnt   OUT PLS_INTEGER,
    p_stale_cnt    OUT PLS_INTEGER
)
IS
    TYPE t_ref IS REF CURSOR;
    TYPE t_rate_rec IS RECORD (
        from_ccy WWI_REF.FX_RATE_DAILY.FROM_CURR_CD%TYPE,
        to_ccy   WWI_REF.FX_RATE_DAILY.TO_CURR_CD%TYPE,
        rate_num WWI_REF.FX_RATE_DAILY.RATE%TYPE
    );
    TYPE t_rate_tab IS TABLE OF t_rate_rec;

    CURSOR c_expected IS
        SELECT c.CURR_CD
          FROM WWI_REF.CURRENCY_CODE c
         WHERE NVL(c.ACTIVE_FLG, 'Y') = 'Y'
           AND c.CURR_CD <> WWI_REF.PKG_FX.c_pivot_currency;

    l_link_name WWI_REF.SOURCE_SYSTEM_REF.CONNECTION_PARAM_NAME%TYPE;
    l_sql       VARCHAR2(4000);
    l_cur       t_ref;
    l_rows      t_rate_tab;
    l_prior     NUMBER;
    l_have_cnt  PLS_INTEGER;
BEGIN
    p_loaded_cnt := 0;
    p_rolled_cnt := 0;
    p_stale_cnt  := 0;

    SELECT CONNECTION_PARAM_NAME
      INTO l_link_name
      FROM WWI_REF.SOURCE_SYSTEM_REF
     WHERE SOURCE_SYS_CD = 'TREASURY';

    l_sql := 'SELECT from_ccy, to_ccy, rate_num FROM fx_feed@' || l_link_name
          || ' WHERE rate_dt = :d AND rate_type_cd = :t';

    OPEN l_cur FOR l_sql USING TRUNC(p_rate_dt), p_rate_type_cd;
    LOOP
        FETCH l_cur BULK COLLECT INTO l_rows LIMIT 500;
        EXIT WHEN l_rows.COUNT = 0;

        FOR i IN 1 .. l_rows.COUNT LOOP
            BEGIN
                IF NVL(l_rows(i).rate_num, 0) <= 0 THEN
                    RAISE_APPLICATION_ERROR(-20621, 'non positive rate');
                END IF;

                WWI_REF.PKG_FX.upsert_rate(l_rows(i).from_ccy, l_rows(i).to_ccy,
                                           TRUNC(p_rate_dt), p_rate_type_cd,
                                           l_rows(i).rate_num, 'TREASURY');

                p_loaded_cnt := p_loaded_cnt + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    WWI_AUDIT.PKG_DATA_QUALITY.log_reject('TREASURY', 'fx_feed',
                        l_rows(i).from_ccy || '/' || l_rows(i).to_ccy,
                        'RATE_REJECTED', SQLERRM, 'E');
            END;
        END LOOP;

        COMMIT;
    END LOOP;
    CLOSE l_cur;

    /* roll forward anything the feed did not send today */
    FOR rec IN c_expected LOOP
        SELECT COUNT(*)
          INTO l_have_cnt
          FROM WWI_REF.FX_RATE_DAILY
         WHERE FROM_CURR_CD = rec.CURR_CD
           AND TO_CURR_CD   = WWI_REF.PKG_FX.c_pivot_currency
           AND RATE_TYPE_CD     = p_rate_type_cd
           AND RATE_DT          = TRUNC(p_rate_dt);

        IF l_have_cnt > 0 THEN
            CONTINUE;
        END IF;

        BEGIN
            SELECT RATE
              INTO l_prior
              FROM (SELECT RATE
                      FROM WWI_REF.FX_RATE_DAILY
                     WHERE FROM_CURR_CD = rec.CURR_CD
                       AND TO_CURR_CD   = WWI_REF.PKG_FX.c_pivot_currency
                       AND RATE_TYPE_CD     = p_rate_type_cd
                       AND RATE_DT          < TRUNC(p_rate_dt)
                     ORDER BY RATE_DT DESC)
             WHERE ROWNUM = 1;

            WWI_REF.PKG_FX.upsert_rate(rec.CURR_CD,
                                       WWI_REF.PKG_FX.c_pivot_currency,
                                       TRUNC(p_rate_dt), p_rate_type_cd, l_prior,
                                       'ROLLFWD');

            p_rolled_cnt := p_rolled_cnt + 1;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                WWI_AUDIT.PKG_DATA_QUALITY.log_reject('TREASURY', 'fx_feed',
                    rec.CURR_CD, 'NO_RATE_EVER',
                    'no ' || p_rate_type_cd || ' rate has ever been loaded', 'E');
        END;
    END LOOP;

    COMMIT;

    WWI_REF.PKG_FX.check_rate_freshness(3, p_stale_cnt);
EXCEPTION
    WHEN OTHERS THEN
        IF l_cur%ISOPEN THEN
            CLOSE l_cur;
        END IF;
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_LOAD_FX_RATES',
                                             TO_CHAR(p_rate_dt, 'YYYY-MM-DD'),
                                             SQLERRM);
        RAISE;
END PRC_LOAD_FX_RATES;
/
