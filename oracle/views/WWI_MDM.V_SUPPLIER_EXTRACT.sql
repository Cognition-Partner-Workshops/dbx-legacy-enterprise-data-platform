/* ============================================================================
 * Object      : WWI_MDM.V_SUPPLIER_EXTRACT (view)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.SUPP_MASTER, WWI_MDM.SUPP_ADDRESS,
 *               WWI_MDM.SUPP_CERTIFICATION, WWI_PROC.VENDOR_CONTRACT,
 *               WWI_FIN.WITHHOLDING_RULE, WWI_MDM.FN_NORMALIZE_NAME
 * Called by   : SSIS EXT_ORA_SupplierMaster
 * History     : 2004 original; 2011 certification expiry exposed; 2015
 *               withholding rate denormalised in for the DW.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_MDM.V_SUPPLIER_EXTRACT AS
SELECT s.SUPP_ID,
       s.SUPP_NUM,
       s.SUPP_NAME,
       WWI_MDM.FN_NORMALIZE_NAME(s.SUPP_NAME, s.REGION_CD)  AS SUPP_NAME_MATCH_KEY,
       s.REGION_CD,
       s.COUNTRY_CD,
       s.CURRENCY_CD,
       s.STATUS_CD,
       s.HOLD_FLAG,
       s.HOLD_REASON_CD,
       s.PAYMENT_TERMS_CD,
       s.TAX_REG_NUM,
       /* EU suppliers without a VAT registration are treated as unregistered
          and cannot be reverse charged; APAC uses the same column for the GST
          registration, NA leaves it null for most suppliers */
       CASE
           WHEN s.REGION_CD = 'EU'   AND s.TAX_REG_NUM IS NULL THEN 'UNREGISTERED'
           WHEN s.REGION_CD = 'EU'                             THEN 'VAT_REGISTERED'
           WHEN s.REGION_CD = 'APAC' AND s.TAX_REG_NUM IS NULL THEN 'GST_EXEMPT'
           WHEN s.REGION_CD = 'APAC'                           THEN 'GST_REGISTERED'
           ELSE 'NA_NOT_APPLICABLE'
       END                                                  AS TAX_REG_STATUS_CD,
       s.WITHHOLDING_CD,
       w.RATE_PCT                                           AS WITHHOLDING_RATE_PCT,
       addr.ADDR_LINE1                                      AS REMIT_ADDR_LINE1,
       addr.CITY_NAME                                       AS REMIT_CITY_NAME,
       addr.STATE_PROV_CD                                   AS REMIT_STATE_PROV_CD,
       addr.POSTAL_CD                                       AS REMIT_POSTAL_CD,
       cert.CERT_COUNT,
       cert.EXPIRED_CERT_COUNT,
       cert.NEXT_EXPIRY_DT,
       ctr.OPEN_CONTRACT_COUNT,
       ctr.TOTAL_COMMIT_AMT,
       s.SRC_SYSTEM_CD,
       s.CREATED_DT,
       s.LAST_UPD_DT
  FROM WWI_MDM.SUPP_MASTER s
  LEFT OUTER JOIN (
        SELECT a.SUPP_ID, a.ADDR_LINE1, a.CITY_NAME, a.STATE_PROV_CD, a.POSTAL_CD
          FROM WWI_MDM.SUPP_ADDRESS a
         WHERE a.ADDR_TYPE_CD = 'REMIT'
           AND NVL(a.PRIMARY_FLAG, 'N') = 'Y'
           AND NVL(a.VALID_TO_DT, DATE '4712-12-31') >= TRUNC(SYSDATE)
       ) addr
    ON addr.SUPP_ID = s.SUPP_ID
  LEFT OUTER JOIN (
        SELECT c.SUPP_ID,
               COUNT(*)                                                    AS CERT_COUNT,
               COUNT(CASE WHEN c.EXPIRY_DT < TRUNC(SYSDATE) THEN 1 END)    AS EXPIRED_CERT_COUNT,
               MIN(CASE WHEN c.EXPIRY_DT >= TRUNC(SYSDATE) THEN c.EXPIRY_DT END) AS NEXT_EXPIRY_DT
          FROM WWI_MDM.SUPP_CERTIFICATION c
         WHERE c.STATUS_CD <> 'REVOKED'
         GROUP BY c.SUPP_ID
       ) cert
    ON cert.SUPP_ID = s.SUPP_ID
  LEFT OUTER JOIN (
        SELECT v.SUPP_ID,
               COUNT(*)              AS OPEN_CONTRACT_COUNT,
               SUM(v.COMMIT_AMT)     AS TOTAL_COMMIT_AMT
          FROM WWI_PROC.VENDOR_CONTRACT v
         WHERE v.STATUS_CD = 'ACTIVE'
           AND NVL(v.END_DT, DATE '4712-12-31') >= TRUNC(SYSDATE)
         GROUP BY v.SUPP_ID
       ) ctr
    ON ctr.SUPP_ID = s.SUPP_ID
  LEFT OUTER JOIN WWI_FIN.WITHHOLDING_RULE w
    ON w.WITHHOLDING_CD = s.WITHHOLDING_CD
   AND w.COUNTRY_CD     = s.COUNTRY_CD
   AND TRUNC(SYSDATE) BETWEEN w.EFF_FROM_DT AND NVL(w.EFF_TO_DT, DATE '4712-12-31')
 WHERE s.STATUS_CD <> 'T'
/
