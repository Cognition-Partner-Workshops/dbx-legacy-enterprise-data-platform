/* ============================================================================
 * Object      : WWI_FIN.FN_CONVERT_AMOUNT (function)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.FX_RATE_DAILY, WWI_REF.CURRENCY_CODE
 * Called by   : WWI_FIN.PKG_AP_INVOICE, WWI_FIN.PKG_AP_PAYMENT,
 *               WWI_FIN.PKG_GL_POSTING, WWI_FIN.PRC_REVALUE_AP_BALANCES,
 *               WWI_FIN.V_AP_INVOICE_EXTRACT, WWI_FIN.V_AP_AGING_CURRENT
 * Errors      : -20031 no usable rate found
 * History     : 1997 original (single SPOT rate); 1999 back-off window;
 *               2002 inverse-rate fallback; 2008 USD triangulation.
 * Notes       : The four-step fallback below is the single most copied piece of
 *               logic in the estate. Do not "simplify" it - reported balances
 *               depend on the exact order of the attempts.
 * ========================================================================= */

CREATE OR REPLACE FUNCTION WWI_FIN.FN_CONVERT_AMOUNT
(
    p_amount        IN NUMBER,
    p_from_ccy      IN WWI_REF.CURRENCY_CODE.CURRENCY_CD%TYPE,
    p_to_ccy        IN WWI_REF.CURRENCY_CODE.CURRENCY_CD%TYPE,
    p_rate_dt       IN DATE,
    p_rate_type_cd  IN WWI_REF.FX_RATE_DAILY.RATE_TYPE_CD%TYPE DEFAULT 'CORP',
    p_max_back_days IN PLS_INTEGER DEFAULT 7
)
RETURN NUMBER
IS
    l_rate       WWI_REF.FX_RATE_DAILY.RATE_NUM%TYPE;
    l_rate_from  WWI_REF.FX_RATE_DAILY.RATE_NUM%TYPE;
    l_rate_to    WWI_REF.FX_RATE_DAILY.RATE_NUM%TYPE;
    l_minor_unit WWI_REF.CURRENCY_CODE.MINOR_UNIT_NUM%TYPE;
BEGIN
    IF p_amount IS NULL THEN
        RETURN NULL;
    END IF;

    IF p_from_ccy = p_to_ccy THEN
        RETURN p_amount;
    END IF;

    BEGIN
        SELECT MINOR_UNIT_NUM
          INTO l_minor_unit
          FROM WWI_REF.CURRENCY_CODE
         WHERE CURRENCY_CD = p_to_ccy;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_minor_unit := 2;
    END;

    /* 1. exact date, requested rate type */
    BEGIN
        SELECT r.RATE_NUM
          INTO l_rate
          FROM WWI_REF.FX_RATE_DAILY r
         WHERE r.FROM_CURRENCY_CD = p_from_ccy
           AND r.TO_CURRENCY_CD   = p_to_ccy
           AND r.RATE_TYPE_CD     = p_rate_type_cd
           AND r.RATE_DT          = TRUNC(p_rate_dt);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_rate := NULL;
        WHEN TOO_MANY_ROWS THEN
            SELECT MAX(r.RATE_NUM)
              INTO l_rate
              FROM WWI_REF.FX_RATE_DAILY r
             WHERE r.FROM_CURRENCY_CD = p_from_ccy
               AND r.TO_CURRENCY_CD   = p_to_ccy
               AND r.RATE_TYPE_CD     = p_rate_type_cd
               AND r.RATE_DT          = TRUNC(p_rate_dt);
    END;

    /* 2. most recent rate inside the back-off window */
    IF l_rate IS NULL THEN
        SELECT MAX(r.RATE_NUM) KEEP (DENSE_RANK LAST ORDER BY r.RATE_DT)
          INTO l_rate
          FROM WWI_REF.FX_RATE_DAILY r
         WHERE r.FROM_CURRENCY_CD = p_from_ccy
           AND r.TO_CURRENCY_CD   = p_to_ccy
           AND r.RATE_TYPE_CD     = p_rate_type_cd
           AND r.RATE_DT BETWEEN TRUNC(p_rate_dt) - NVL(p_max_back_days, 7)
                             AND TRUNC(p_rate_dt);
    END IF;

    /* 3. inverse quote */
    IF l_rate IS NULL THEN
        SELECT MAX(r.RATE_NUM) KEEP (DENSE_RANK LAST ORDER BY r.RATE_DT)
          INTO l_rate
          FROM WWI_REF.FX_RATE_DAILY r
         WHERE r.FROM_CURRENCY_CD = p_to_ccy
           AND r.TO_CURRENCY_CD   = p_from_ccy
           AND r.RATE_TYPE_CD     = p_rate_type_cd
           AND r.RATE_DT BETWEEN TRUNC(p_rate_dt) - NVL(p_max_back_days, 7)
                             AND TRUNC(p_rate_dt);
        IF l_rate IS NOT NULL AND l_rate <> 0 THEN
            l_rate := 1 / l_rate;
        END IF;
    END IF;

    /* 4. triangulate through USD - added for the 2008 APAC roll-out */
    IF l_rate IS NULL THEN
        SELECT MAX(r.RATE_NUM) KEEP (DENSE_RANK LAST ORDER BY r.RATE_DT)
          INTO l_rate_from
          FROM WWI_REF.FX_RATE_DAILY r
         WHERE r.FROM_CURRENCY_CD = p_from_ccy
           AND r.TO_CURRENCY_CD   = 'USD'
           AND r.RATE_TYPE_CD     = p_rate_type_cd
           AND r.RATE_DT BETWEEN TRUNC(p_rate_dt) - 30 AND TRUNC(p_rate_dt);

        SELECT MAX(r.RATE_NUM) KEEP (DENSE_RANK LAST ORDER BY r.RATE_DT)
          INTO l_rate_to
          FROM WWI_REF.FX_RATE_DAILY r
         WHERE r.FROM_CURRENCY_CD = 'USD'
           AND r.TO_CURRENCY_CD   = p_to_ccy
           AND r.RATE_TYPE_CD     = p_rate_type_cd
           AND r.RATE_DT BETWEEN TRUNC(p_rate_dt) - 30 AND TRUNC(p_rate_dt);

        IF l_rate_from IS NOT NULL AND l_rate_to IS NOT NULL THEN
            l_rate := l_rate_from * l_rate_to;
        END IF;
    END IF;

    IF l_rate IS NULL THEN
        RAISE_APPLICATION_ERROR(-20031,
            'FN_CONVERT_AMOUNT: no rate for ' || p_from_ccy || '->' || p_to_ccy
            || ' type ' || p_rate_type_cd || ' on ' || TO_CHAR(p_rate_dt, 'YYYY-MM-DD'));
    END IF;

    RETURN ROUND(p_amount * l_rate, NVL(l_minor_unit, 2));
END FN_CONVERT_AMOUNT;
/
