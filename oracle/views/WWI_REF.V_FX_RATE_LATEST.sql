/* ============================================================================
 * Object      : WWI_REF.V_FX_RATE_LATEST (view)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.FX_RATE_DAILY, WWI_REF.CURRENCY_CODE
 * Called by   : SSIS EXT_ORA_FxRate, WWI_REF.PKG_FX, finance reporting
 * Warning     : Correlated subquery per currency pair / rate type. The daily
 *               rate table has never been purged, so this scans roughly
 *               twenty years of rows every time it is opened.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_REF.V_FX_RATE_LATEST AS
SELECT f.FROM_CURRENCY_CD,
       f.TO_CURRENCY_CD,
       f.RATE_TYPE_CD,
       f.RATE_DT,
       f.RATE_NUM,
       CASE WHEN f.RATE_NUM = 0 THEN NULL ELSE ROUND(1 / f.RATE_NUM, 10) END AS INVERSE_RATE_NUM,
       TRUNC(SYSDATE) - f.RATE_DT                                AS RATE_AGE_DAYS,
       CASE
           WHEN TRUNC(SYSDATE) - f.RATE_DT > 7  THEN 'STALE'
           WHEN TRUNC(SYSDATE) - f.RATE_DT > 2  THEN 'AGING'
           ELSE 'CURRENT'
       END                                                       AS RATE_FRESHNESS_CD,
       f.SRC_SYSTEM_CD,
       f.LOADED_DT,
       fc.MINOR_UNIT_NUM                                         AS FROM_MINOR_UNIT_NUM,
       tc.MINOR_UNIT_NUM                                         AS TO_MINOR_UNIT_NUM
  FROM WWI_REF.FX_RATE_DAILY f
  LEFT OUTER JOIN WWI_REF.CURRENCY_CODE fc
    ON fc.CURRENCY_CD = f.FROM_CURRENCY_CD
  LEFT OUTER JOIN WWI_REF.CURRENCY_CODE tc
    ON tc.CURRENCY_CD = f.TO_CURRENCY_CD
 WHERE f.RATE_DT = (SELECT MAX(f2.RATE_DT)
                      FROM WWI_REF.FX_RATE_DAILY f2
                     WHERE f2.FROM_CURRENCY_CD = f.FROM_CURRENCY_CD
                       AND f2.TO_CURRENCY_CD   = f.TO_CURRENCY_CD
                       AND f2.RATE_TYPE_CD     = f.RATE_TYPE_CD
                       AND f2.RATE_DT         <= TRUNC(SYSDATE))
/
