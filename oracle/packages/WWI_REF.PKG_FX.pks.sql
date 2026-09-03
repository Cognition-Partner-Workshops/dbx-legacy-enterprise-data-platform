/* ============================================================================
 * Object      : WWI_REF.PKG_FX (package specification)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.FX_RATE_DAILY, WWI_REF.CURRENCY_CODE, WWI_REF.SOURCE_SYSTEM_REF
 * Called by   : WWI_FIN.FN_CONVERT_AMOUNT, WWI_FIN.PKG_GL_POSTING,
 *               WWI_FIN.PRC_REVALUE_AP_BALANCES and the nightly rate loader
 *               WWI_REF.PRC_LOAD_FX_RATES.
 * History     : 1999 daily corporate rate only; 2006 spot and month-end
 *               rate types; 2011 triangulation through USD; the inverse
 *               quoting convention differs by rate source and has never
 *               been normalised.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_REF.PKG_FX AS

    e_rate_missing     EXCEPTION;
    e_currency_unknown EXCEPTION;
    e_rate_stale       EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_rate_missing,     -20401);
    PRAGMA EXCEPTION_INIT(e_currency_unknown, -20402);
    PRAGMA EXCEPTION_INIT(e_rate_stale,       -20403);

    c_pivot_currency CONSTANT VARCHAR2(3) := 'USD';

    FUNCTION get_rate
    (
        p_from_ccy     IN WWI_REF.FX_RATE_DAILY.FROM_CURRENCY_CD%TYPE,
        p_to_ccy       IN WWI_REF.FX_RATE_DAILY.TO_CURRENCY_CD%TYPE,
        p_rate_dt      IN DATE DEFAULT TRUNC(SYSDATE),
        p_rate_type_cd IN WWI_REF.FX_RATE_DAILY.RATE_TYPE_CD%TYPE DEFAULT 'CORP'
    ) RETURN NUMBER;

    FUNCTION month_end_rate
    (
        p_from_ccy  IN WWI_REF.FX_RATE_DAILY.FROM_CURRENCY_CD%TYPE,
        p_to_ccy    IN WWI_REF.FX_RATE_DAILY.TO_CURRENCY_CD%TYPE,
        p_period_cd IN VARCHAR2
    ) RETURN NUMBER;

    FUNCTION round_to_minor_unit
    (
        p_amount      IN NUMBER,
        p_currency_cd IN WWI_REF.CURRENCY_CODE.CURRENCY_CD%TYPE
    ) RETURN NUMBER;

    PROCEDURE upsert_rate
    (
        p_from_ccy     IN WWI_REF.FX_RATE_DAILY.FROM_CURRENCY_CD%TYPE,
        p_to_ccy       IN WWI_REF.FX_RATE_DAILY.TO_CURRENCY_CD%TYPE,
        p_rate_dt      IN DATE,
        p_rate_type_cd IN WWI_REF.FX_RATE_DAILY.RATE_TYPE_CD%TYPE,
        p_rate_num     IN WWI_REF.FX_RATE_DAILY.RATE_NUM%TYPE,
        p_src_system_cd    IN WWI_REF.FX_RATE_DAILY.SRC_SYSTEM_CD%TYPE
    );

    PROCEDURE check_rate_freshness
    (
        p_max_age_days IN  PLS_INTEGER DEFAULT 3,
        p_stale_cnt    OUT PLS_INTEGER
    );

END PKG_FX;
/
