/* ============================================================================
 * Object      : WWI_REF.V_GEOGRAPHY_EXTRACT (view)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.COUNTRY_REF, WWI_REF.REGION_REF, WWI_REF.CITY_REF,
 *               WWI_REF.POSTAL_REF, WWI_REF.LANGUAGE_REF
 * Called by   : SSIS EXT_ORA_Geography (weekly full refresh)
 * Notes       : One row per postal code where postal reference data exists,
 *               otherwise one row per city. The DW geography dimension has
 *               tolerated the mixed grain since 2008.
 *
 *               Retention and consent are regional policy attributes and come
 *               from REGION_REF; the country row carries only the EU flags.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_REF.V_GEOGRAPHY_EXTRACT AS
SELECT ct.COUNTRY_CD,
       ct.COUNTRY_NAME,
       ct.COUNTRY_CD_3                                    AS ISO3_CD,
       ct.DEFAULT_CURR_CD                                 AS COUNTRY_CURRENCY_CD,
       ct.EU_MEMBER_FLG                                   AS EU_MEMBER_FLAG,
       ct.EU_VAT_AREA_FLG,
       ct.SANCTIONED_FLG,
       rg.REGION_CD,
       rg.REGION_NAME,
       ci.CITY_ID,
       ci.CITY_NAME,
       ci.STATE_PROV_CD                                   AS STATE_PROVINCE_CD,
       ci.PREFECTURE_TXT,
       ci.LATITUDE,
       ci.LONGITUDE,
       po.POSTAL_CD,
       po.POSTAL_CD_NORM,
       po.DISTRICT_TXT                                    AS POSTAL_AREA_NAME,
       po.TAX_JURISDICTION_CD,
       CASE
           WHEN po.POSTAL_CD IS NULL THEN 'CITY'
           ELSE 'POSTAL'
       END                                                AS GRAIN_CD,
       lg.LANGUAGE_CD,
       lg.LANGUAGE_NAME,
       /* the address-format hint the DW uses when rendering labels */
       CASE rg.REGION_CD
           WHEN 'NA'   THEN 'CITY_STATE_ZIP'
           WHEN 'EU'   THEN 'ZIP_CITY'
           WHEN 'APAC' THEN 'ZIP_PREFECTURE_CITY'
           ELSE 'FREEFORM'
       END                                                AS ADDRESS_FORMAT_CD,
       rg.RETENTION_MONTHS                                AS DATA_RETENTION_MONTHS,
       CASE WHEN rg.CONSENT_REGIME_CD IS NULL THEN 'N' ELSE 'Y' END
                                                          AS CONSENT_REQUIRED_FLAG,
       NVL(ct.UPDATED_DT, ct.CREATED_DT)                  AS LAST_UPD_DT
  FROM WWI_REF.COUNTRY_REF ct
  LEFT OUTER JOIN WWI_REF.REGION_REF rg
    ON rg.REGION_CD = ct.REGION_CD
  LEFT OUTER JOIN WWI_REF.CITY_REF ci
    ON ci.COUNTRY_CD = ct.COUNTRY_CD
  LEFT OUTER JOIN WWI_REF.POSTAL_REF po
    ON po.CITY_ID = ci.CITY_ID
  LEFT OUTER JOIN WWI_REF.LANGUAGE_REF lg
    ON lg.LANGUAGE_CD = ct.DEFAULT_LANGUAGE_CD
/
