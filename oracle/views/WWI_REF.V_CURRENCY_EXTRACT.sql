/* ============================================================================
 * Object      : WWI_REF.V_CURRENCY_EXTRACT (view)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.CURRENCY_CODE, WWI_REF.FX_RATE_DAILY,
 *               WWI_REF.COUNTRY_REF
 * Called by   : SSIS EXT_ORA_Currency (weekly full refresh)
 * Notes       : Legacy currencies (DEM, FRF, ITL ...) are still carried so old
 *               journals can be reported; MIGRATED_TO_CCY_CD points at EUR.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_REF.V_CURRENCY_EXTRACT AS
SELECT c.CURRENCY_CD,
       c.CURRENCY_NAME,
       c.CURRENCY_SYMBOL,
       c.MINOR_UNIT_NUM,
       c.ISO_NUMERIC_CD,
       c.ACTIVE_FLAG,
       c.MIGRATED_TO_CCY_CD,
       CASE
           WHEN NVL(c.ACTIVE_FLAG, 'N') = 'N' AND c.MIGRATED_TO_CCY_CD IS NOT NULL
               THEN 'LEGACY_MIGRATED'
           WHEN NVL(c.ACTIVE_FLAG, 'N') = 'N' THEN 'RETIRED'
           ELSE 'ACTIVE'
       END                                                AS LIFECYCLE_CD,
       ctry.COUNTRY_COUNT,
       rate.LAST_RATE_DT,
       rate.RATE_TYPE_COUNT,
       CASE
           WHEN NVL(c.ACTIVE_FLAG, 'N') = 'Y'
                AND (rate.LAST_RATE_DT IS NULL
                     OR rate.LAST_RATE_DT < TRUNC(SYSDATE) - 5) THEN 'Y'
           ELSE 'N'
       END                                                AS STALE_RATE_FLAG,
       c.LAST_UPD_DT
  FROM WWI_REF.CURRENCY_CODE c
  LEFT OUTER JOIN (
        SELECT r.COUNTRY_CURRENCY_CD AS CURRENCY_CD, COUNT(*) AS COUNTRY_COUNT
          FROM WWI_REF.COUNTRY_REF r
         GROUP BY r.COUNTRY_CURRENCY_CD
       ) ctry
    ON ctry.CURRENCY_CD = c.CURRENCY_CD
  LEFT OUTER JOIN (
        SELECT f.FROM_CURRENCY_CD                AS CURRENCY_CD,
               MAX(f.RATE_DT)                    AS LAST_RATE_DT,
               COUNT(DISTINCT f.RATE_TYPE_CD)    AS RATE_TYPE_COUNT
          FROM WWI_REF.FX_RATE_DAILY f
         GROUP BY f.FROM_CURRENCY_CD
       ) rate
    ON rate.CURRENCY_CD = c.CURRENCY_CD
/
