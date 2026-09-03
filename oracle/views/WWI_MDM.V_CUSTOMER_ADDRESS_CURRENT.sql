/* ============================================================================
 * Object      : WWI_MDM.V_CUSTOMER_ADDRESS_CURRENT (view)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.CUST_ADDRESS, WWI_MDM.CUST_MASTER,
 *               WWI_REF.POSTAL_REF, WWI_REF.CITY_REF, WWI_REF.COUNTRY_REF
 * Called by   : SSIS EXT_ORA_CustomerAddress, WWI_MDM.PKG_CUSTOMER_MASTER
 * History     : 2005 original; 2008 EU postal spacing rule; 2012 APAC
 *               prefecture handling; 2016 NA ZIP+4 split.
 * Notes       : Postal standardisation is inline in the view because the
 *               packaged version was written later and only the batch loader
 *               was ever switched over to it.
 *
 *               Reads WWI_REF; the SELECT grants live in
 *               oracle/ddl/05_grant_privileges.sql.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_MDM.V_CUSTOMER_ADDRESS_CURRENT AS
SELECT a.CUST_ADDR_ID                                          AS ADDR_ID,
       a.CUST_ID,
       m.CUST_NBR                                              AS CUST_NUM,
       m.REGION_CD,
       a.ADDR_TYPE_CD,
       a.ADDR_SEQ_NBR,
       a.ADDR_LINE_1                                           AS ADDR_LINE1,
       a.ADDR_LINE_2                                           AS ADDR_LINE2,
       a.ADDR_LINE_3                                           AS ADDR_LINE3,
       a.CITY_TXT                                              AS CITY_NAME,
       a.STATE_PROV_CD,
       a.POSTAL_CD,
       /* region specific postal normalisation, all three variants inline */
       CASE m.REGION_CD
           WHEN 'NA' THEN
               CASE
                   WHEN REGEXP_LIKE(a.POSTAL_CD, '^[0-9]{9}$')
                       THEN SUBSTR(a.POSTAL_CD, 1, 5) || '-' || SUBSTR(a.POSTAL_CD, 6, 4)
                   WHEN REGEXP_LIKE(a.POSTAL_CD, '^[0-9]{5}$')
                       THEN a.POSTAL_CD
                   WHEN a.COUNTRY_CD = 'CA'
                       THEN UPPER(REGEXP_REPLACE(a.POSTAL_CD, '[^A-Za-z0-9]', ''))
                   ELSE UPPER(TRIM(a.POSTAL_CD))
               END
           WHEN 'EU' THEN
               /* country prefix was dropped in 2008 but legacy rows still carry
                  it; strip it and collapse internal spacing */
               UPPER(REGEXP_REPLACE(REGEXP_REPLACE(a.POSTAL_CD, '^[A-Z]{1,2}-', ''),
                                    '[[:space:]]+', ' '))
           WHEN 'APAC' THEN
               CASE
                   WHEN a.COUNTRY_CD = 'JP' AND LENGTH(REGEXP_REPLACE(a.POSTAL_CD, '[^0-9]', '')) = 7
                       THEN SUBSTR(REGEXP_REPLACE(a.POSTAL_CD, '[^0-9]', ''), 1, 3) || '-'
                            || SUBSTR(REGEXP_REPLACE(a.POSTAL_CD, '[^0-9]', ''), 4, 4)
                   ELSE UPPER(REPLACE(a.POSTAL_CD, ' ', ''))
               END
           ELSE UPPER(TRIM(a.POSTAL_CD))
       END                                                     AS POSTAL_CD_STD,
       a.ZIP4_CD,
       p.POSTAL_ID,
       ct.CITY_ID,
       a.COUNTRY_CD,
       cr.COUNTRY_NAME,
       a.PRIMARY_FLG                                           AS PRIMARY_FLAG,
       a.ADDR_VERIFIED_FLG                                     AS VERIFIED_FLAG,
       CASE WHEN a.POSTAL_CD_NORM IS NOT NULL THEN 'Y' ELSE 'N' END AS NORMALIZED_FLAG,
       a.VALID_FROM_DT,
       a.VALID_TO_DT,
       NVL(a.UPDATED_DT, a.CREATED_DT)                         AS LAST_UPD_DT,
       CASE WHEN p.POSTAL_ID IS NULL THEN 'Y' ELSE 'N' END     AS UNMATCHED_POSTAL_FLAG
  FROM WWI_MDM.CUST_ADDRESS a
  JOIN WWI_MDM.CUST_MASTER m
    ON m.CUST_ID = a.CUST_ID
  LEFT OUTER JOIN WWI_REF.POSTAL_REF p
    ON p.POSTAL_CD_NORM = NVL(a.POSTAL_CD_NORM, UPPER(REPLACE(a.POSTAL_CD, ' ', '')))
   AND p.COUNTRY_CD     = a.COUNTRY_CD
  LEFT OUTER JOIN WWI_REF.CITY_REF ct
    ON ct.CITY_ID = p.CITY_ID
  LEFT OUTER JOIN WWI_REF.COUNTRY_REF cr
    ON cr.COUNTRY_CD = a.COUNTRY_CD
 WHERE NVL(a.VALID_TO_DT, DATE '4712-12-31') >= TRUNC(SYSDATE)
   AND a.VALID_FROM_DT <= TRUNC(SYSDATE)
   AND NVL(a.DELETED_FLG, 'N') = 'N'
/
