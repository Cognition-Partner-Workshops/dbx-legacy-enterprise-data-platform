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
 * Notes       : Suppliers carry a withholding flag rather than a rule code;
 *               the rule is resolved by country and supplier type.
 *               Reads WWI_PROC and WWI_FIN; the SELECT grants live in
 *               oracle/ddl/05_grant_privileges.sql.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_MDM.V_SUPPLIER_EXTRACT AS
SELECT s.SUPP_ID,
       s.SUPP_NBR                                           AS SUPP_NUM,
       s.SUPP_NAME,
       WWI_MDM.FN_NORMALIZE_NAME(s.SUPP_NAME, s.REGION_CD)  AS SUPP_NAME_MATCH_KEY,
       s.REGION_CD,
       s.COUNTRY_CD,
       s.SUPP_TYPE_CD,
       s.DEFAULT_CURR_CD                                    AS CURRENCY_CD,
       s.SUPP_STATUS_CD                                     AS STATUS_CD,
       s.APPROVAL_STATUS_CD,
       s.HOLD_ALL_FLG                                       AS HOLD_FLAG,
       s.HOLD_REASON_CD,
       s.PAYMENT_TERMS_CD,
       s.PAYMENT_METHOD_CD,
       NVL(s.VAT_REG_NBR, s.TAX_ID_NBR)                     AS TAX_REG_NUM,
       /* EU suppliers without a VAT registration are treated as unregistered
          and cannot be reverse charged; APAC uses the same column for the GST
          registration, NA leaves it null for most suppliers */
       CASE
           WHEN s.REGION_CD = 'EU'   AND s.VAT_REG_NBR IS NULL THEN 'UNREGISTERED'
           WHEN s.REGION_CD = 'EU'                             THEN 'VAT_REGISTERED'
           WHEN s.REGION_CD = 'APAC' AND s.VAT_REG_NBR IS NULL THEN 'GST_EXEMPT'
           WHEN s.REGION_CD = 'APAC'                           THEN 'GST_REGISTERED'
           ELSE 'NA_NOT_APPLICABLE'
       END                                                  AS TAX_REG_STATUS_CD,
       s.WITHHOLDING_FLG,
       w.WHT_RULE_CD                                        AS WITHHOLDING_CD,
       w.WHT_RATE_PCT                                       AS WITHHOLDING_RATE_PCT,
       addr.ADDR_LINE_1                                     AS REMIT_ADDR_LINE1,
       addr.CITY_TXT                                        AS REMIT_CITY_NAME,
       addr.STATE_PROV_CD                                   AS REMIT_STATE_PROV_CD,
       addr.POSTAL_CD                                       AS REMIT_POSTAL_CD,
       NVL(cert.CERT_COUNT, 0)                              AS CERT_COUNT,
       NVL(cert.EXPIRED_CERT_COUNT, 0)                      AS EXPIRED_CERT_COUNT,
       cert.NEXT_EXPIRY_DT,
       NVL(ctr.OPEN_CONTRACT_COUNT, 0)                      AS OPEN_CONTRACT_COUNT,
       ctr.TOTAL_COMMIT_AMT,
       s.SOURCE_SYS                                         AS SRC_SYSTEM_CD,
       s.CREATED_DT,
       NVL(s.UPDATED_DT, s.CREATED_DT)                      AS LAST_UPD_DT
  FROM WWI_MDM.SUPP_MASTER s
  LEFT OUTER JOIN (
        SELECT a.SUPP_ID, a.ADDR_LINE_1, a.CITY_TXT, a.STATE_PROV_CD, a.POSTAL_CD
          FROM WWI_MDM.SUPP_ADDRESS a
         WHERE a.SITE_TYPE_CD = 'REMIT'
           AND NVL(a.PRIMARY_FLG, 'N') = 'Y'
           AND NVL(a.ACTIVE_FLG, 'N') = 'Y'
       ) addr
    ON addr.SUPP_ID = s.SUPP_ID
  LEFT OUTER JOIN (
        SELECT c.SUPP_ID,
               COUNT(*)                                                    AS CERT_COUNT,
               COUNT(CASE WHEN c.EXPIRY_DT < TRUNC(SYSDATE) THEN 1 END)    AS EXPIRED_CERT_COUNT,
               MIN(CASE WHEN c.EXPIRY_DT >= TRUNC(SYSDATE) THEN c.EXPIRY_DT END) AS NEXT_EXPIRY_DT
          FROM WWI_MDM.SUPP_CERTIFICATION c
         WHERE c.CERT_STATUS_CD <> 'SUSP'
         GROUP BY c.SUPP_ID
       ) cert
    ON cert.SUPP_ID = s.SUPP_ID
  LEFT OUTER JOIN (
        SELECT v.SUPP_ID,
               COUNT(*)               AS OPEN_CONTRACT_COUNT,
               SUM(v.COMMITTED_AMT)   AS TOTAL_COMMIT_AMT
          FROM WWI_PROC.VENDOR_CONTRACT v
         WHERE v.CONTRACT_STATUS_CD = 'ACTV'
           AND NVL(v.END_DT, DATE '4712-12-31') >= TRUNC(SYSDATE)
         GROUP BY v.SUPP_ID
       ) ctr
    ON ctr.SUPP_ID = s.SUPP_ID
  LEFT OUTER JOIN WWI_FIN.WITHHOLDING_RULE w
    ON w.COUNTRY_CD       = s.COUNTRY_CD
   AND w.SUPPLIER_TYPE_CD = s.SUPP_TYPE_CD
   AND NVL(w.ACTIVE_FLG, 'N') = 'Y'
   AND TRUNC(SYSDATE) BETWEEN w.EFFECTIVE_FROM_DT
                          AND NVL(w.EFFECTIVE_TO_DT, DATE '4712-12-31')
 WHERE s.SUPP_STATUS_CD <> 'IN'
   AND NVL(s.DELETED_FLG, 'N') = 'N'
/
