/* ============================================================================
 * Object      : WWI_REF.V_CURRENCY_EXTRACT (view)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.CURRENCY_CODE, WWI_REF.FX_RATE_DAILY,
 *               WWI_REF.COUNTRY_REF
 * Called by   : SSIS EXT_ORA_Currency (weekly full refresh)
 * Notes       : Legacy currencies (DEM, FRF, ITL ...) are still carried so old
 *               journals can be reported; they are flagged EURO_LEGACY_FLG and
 *               the extract derives the migration target (EUR) from that flag.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_REF.V_CURRENCY_EXTRACT AS
SELECT c.CURR_CD                                          AS CURRENCY_CD,
       c.CURR_NAME                                        AS CURRENCY_NAME,
       c.CURR_SYMBOL                                      AS CURRENCY_SYMBOL,
       c.MINOR_UNIT_DIGITS                                AS MINOR_UNIT_NUM,
       c.CURR_NUM_CD                                      AS ISO_NUMERIC_CD,
       c.ROUNDING_RULE_CD,
       c.PRIMARY_COUNTRY_CD,
       c.REGION_CD,
       c.ACTIVE_FLG                                       AS ACTIVE_FLAG,
       c.TRADING_ALLOWED_FLG,
       CASE WHEN NVL(c.EURO_LEGACY_FLG, 'N') = 'Y' THEN 'EUR' END
                                                          AS MIGRATED_TO_CCY_CD,
       c.EURO_FIXED_RATE,
       c.EURO_CONVERSION_DT,
       c.RETIRED_DT,
       CASE
           WHEN NVL(c.ACTIVE_FLG, 'N') = 'N' AND NVL(c.EURO_LEGACY_FLG, 'N') = 'Y'
               THEN 'LEGACY_MIGRATED'
           WHEN NVL(c.ACTIVE_FLG, 'N') = 'N' THEN 'RETIRED'
           ELSE 'ACTIVE'
       END                                                AS LIFECYCLE_CD,
       NVL(ctry.COUNTRY_COUNT, 0)                         AS COUNTRY_COUNT,
       rate.LAST_RATE_DT,
       NVL(rate.RATE_TYPE_COUNT, 0)                       AS RATE_TYPE_COUNT,
       CASE
           WHEN NVL(c.ACTIVE_FLG, 'N') = 'Y'
                AND (rate.LAST_RATE_DT IS NULL
                     OR rate.LAST_RATE_DT < TRUNC(SYSDATE) - 5) THEN 'Y'
           ELSE 'N'
       END                                                AS STALE_RATE_FLAG,
       NVL(c.UPDATED_DT, c.CREATED_DT)                    AS LAST_UPD_DT
  FROM WWI_REF.CURRENCY_CODE c
  LEFT OUTER JOIN (
        SELECT r.DEFAULT_CURR_CD AS CURR_CD, COUNT(*) AS COUNTRY_COUNT
          FROM WWI_REF.COUNTRY_REF r
         GROUP BY r.DEFAULT_CURR_CD
       ) ctry
    ON ctry.CURR_CD = c.CURR_CD
  LEFT OUTER JOIN (
        SELECT f.FROM_CURR_CD                    AS CURR_CD,
               MAX(f.RATE_DT)                    AS LAST_RATE_DT,
               COUNT(DISTINCT f.RATE_TYPE_CD)    AS RATE_TYPE_COUNT
          FROM WWI_REF.FX_RATE_DAILY f
         GROUP BY f.FROM_CURR_CD
       ) rate
    ON rate.CURR_CD = c.CURR_CD
/
