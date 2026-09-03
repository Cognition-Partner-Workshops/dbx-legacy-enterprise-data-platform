/* ============================================================================
 * Object      : WWI_REF.V_FX_RATE_LATEST (view)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.FX_RATE_DAILY, WWI_REF.CURRENCY_CODE
 * Called by   : SSIS EXT_ORA_FxRate, WWI_REF.PKG_FX, finance reporting
 * Warning     : Correlated subquery per currency pair / rate type. The daily
 *               rate table has never been purged, so this scans roughly
 *               twenty years of rows every time it is opened.
 * Notes       : Superseded rows are excluded; the stored INVERSE_RATE is used
 *               when the feed supplied one.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_REF.V_FX_RATE_LATEST AS
SELECT f.FROM_CURR_CD                                            AS FROM_CURRENCY_CD,
       f.TO_CURR_CD                                              AS TO_CURRENCY_CD,
       f.RATE_TYPE_CD,
       f.RATE_DT,
       f.RATE                                                    AS RATE_NUM,
       COALESCE(f.INVERSE_RATE,
                CASE WHEN f.RATE = 0 THEN NULL ELSE ROUND(1 / f.RATE, 10) END)
                                                                 AS INVERSE_RATE_NUM,
       f.BID_RATE,
       f.ASK_RATE,
       f.INTERPOLATED_FLG,
       TRUNC(SYSDATE) - f.RATE_DT                                AS RATE_AGE_DAYS,
       CASE
           WHEN TRUNC(SYSDATE) - f.RATE_DT > 7  THEN 'STALE'
           WHEN TRUNC(SYSDATE) - f.RATE_DT > 2  THEN 'AGING'
           ELSE 'CURRENT'
       END                                                       AS RATE_FRESHNESS_CD,
       f.RATE_SOURCE_CD,
       f.FEED_REGION_CD,
       f.SOURCE_SYS                                              AS SRC_SYSTEM_CD,
       CAST(f.LOADED_TS AS DATE)                                 AS LOADED_DT,
       fc.MINOR_UNIT_DIGITS                                      AS FROM_MINOR_UNIT_NUM,
       tc.MINOR_UNIT_DIGITS                                      AS TO_MINOR_UNIT_NUM
  FROM WWI_REF.FX_RATE_DAILY f
  LEFT OUTER JOIN WWI_REF.CURRENCY_CODE fc
    ON fc.CURR_CD = f.FROM_CURR_CD
  LEFT OUTER JOIN WWI_REF.CURRENCY_CODE tc
    ON tc.CURR_CD = f.TO_CURR_CD
 WHERE NVL(f.SUPERSEDED_FLG, 'N') = 'N'
   AND f.RATE_DT = (SELECT MAX(f2.RATE_DT)
                      FROM WWI_REF.FX_RATE_DAILY f2
                     WHERE f2.FROM_CURR_CD  = f.FROM_CURR_CD
                       AND f2.TO_CURR_CD    = f.TO_CURR_CD
                       AND f2.RATE_TYPE_CD  = f.RATE_TYPE_CD
                       AND NVL(f2.SUPERSEDED_FLG, 'N') = 'N'
                       AND f2.RATE_DT      <= TRUNC(SYSDATE))
/
