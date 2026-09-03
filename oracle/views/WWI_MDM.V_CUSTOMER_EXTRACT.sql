/* ============================================================================
 * Object      : WWI_MDM.V_CUSTOMER_EXTRACT (view)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.CUST_MASTER, WWI_MDM.CUST_ADDRESS,
 *               WWI_MDM.CUST_CREDIT_PROFILE, WWI_MDM.CUST_CLASSIFICATION,
 *               WWI_MDM.CUST_SEGMENT_ASSIGN, WWI_REF.COUNTRY_REF,
 *               WWI_MDM.FN_NORMALIZE_NAME, WWI_MDM.FN_CUSTOMER_STATUS,
 *               WWI_REF.FN_TRANSLATE_CODE
 * Called by   : SSIS EXT_ORA_CustomerMaster (incremental on LAST_UPD_DT),
 *               WWI_AUDIT.PRC_PREPARE_CUSTOMER_EXTRACT
 * History     : 2004 original; 2010 consent columns; 2017 EU suppression rule.
 * Warning     : Expensive. Four correlated scalar subqueries per row plus two
 *               PL/SQL calls. The nightly extract has always run it with a
 *               LAST_UPD_DT predicate; a full scan is a multi-hour statement.
 * Notes       : Reads WWI_REF; the SELECT grants live in
 *               oracle/ddl/05_grant_privileges.sql.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_MDM.V_CUSTOMER_EXTRACT AS
SELECT c.CUST_ID,
       c.CUST_NBR                                               AS CUST_NUM,
       c.LEGACY_CUST_CD,
       c.CUST_NAME,
       WWI_MDM.FN_NORMALIZE_NAME(c.CUST_NAME, c.REGION_CD)      AS CUST_NAME_MATCH_KEY,
       c.REGION_CD,
       c.COUNTRY_CD,
       cr.COUNTRY_NAME,
       c.PRIMARY_CURR_CD                                        AS CURRENCY_CD,
       c.CUST_STATUS_CD                                         AS STORED_STATUS_CD,
       WWI_MDM.FN_CUSTOMER_STATUS(c.CUST_ID)                    AS DERIVED_STATUS_CD,
       WWI_REF.FN_TRANSLATE_CODE('CUST_STATUS', c.CUST_STATUS_CD, 'ORAERP', 'WWIDW',
                                 c.REGION_CD)                   AS DW_STATUS_CD,
       c.PAYMENT_TERMS_CD,
       cp.CREDIT_LIMIT_AMT,
       cp.CREDIT_LIMIT_CURR_CD                                  AS CREDIT_CURRENCY_CD,
       cp.RISK_CLASS_CD,
       cp.AVG_DAYS_LATE                                         AS DAYS_BEYOND_TERMS_NUM,
       /* the billing address is the primary one, unless there is none, in which
          case the oldest legal address is used - 2007 rule, never revisited */
       (SELECT MAX(a.ADDR_LINE_1) KEEP (DENSE_RANK FIRST
                ORDER BY DECODE(a.ADDR_TYPE_CD, 'BILL', 1, 'LEGAL', 2, 3), a.CUST_ADDR_ID)
          FROM WWI_MDM.CUST_ADDRESS a
         WHERE a.CUST_ID = c.CUST_ID
           AND NVL(a.VALID_TO_DT, DATE '4712-12-31') >= TRUNC(SYSDATE))  AS BILL_ADDR_LINE1,
       (SELECT MAX(a.CITY_TXT) KEEP (DENSE_RANK FIRST
                ORDER BY DECODE(a.ADDR_TYPE_CD, 'BILL', 1, 'LEGAL', 2, 3), a.CUST_ADDR_ID)
          FROM WWI_MDM.CUST_ADDRESS a
         WHERE a.CUST_ID = c.CUST_ID
           AND NVL(a.VALID_TO_DT, DATE '4712-12-31') >= TRUNC(SYSDATE))  AS BILL_CITY_NAME,
       (SELECT MAX(a.POSTAL_CD) KEEP (DENSE_RANK FIRST
                ORDER BY DECODE(a.ADDR_TYPE_CD, 'BILL', 1, 'LEGAL', 2, 3), a.CUST_ADDR_ID)
          FROM WWI_MDM.CUST_ADDRESS a
         WHERE a.CUST_ID = c.CUST_ID
           AND NVL(a.VALID_TO_DT, DATE '4712-12-31') >= TRUNC(SYSDATE))  AS BILL_POSTAL_CD,
       (SELECT MAX(cl.CLASS_CD) KEEP (DENSE_RANK LAST ORDER BY cl.EFFECTIVE_DT)
          FROM WWI_MDM.CUST_CLASSIFICATION cl
         WHERE cl.CUST_ID         = c.CUST_ID
           AND cl.CLASS_SCHEME_CD = 'BUYING_GROUP'
           AND TRUNC(SYSDATE) BETWEEN cl.EFFECTIVE_DT
                                  AND NVL(cl.EXPIRY_DT, DATE '4712-12-31'))  AS BUYING_GROUP_CD,
       (SELECT MAX(sa.SEGMENT_CD) KEEP (DENSE_RANK LAST ORDER BY sa.EFFECTIVE_DT)
          FROM WWI_MDM.CUST_SEGMENT_ASSIGN sa
         WHERE sa.CUST_ID = c.CUST_ID)                            AS SEGMENT_CD,
       /* consent handling diverges: EU suppresses the name and the address for
          withdrawn consent, APAC keeps the record but flags it, NA ignores it */
       CASE
           WHEN c.REGION_CD = 'EU'   AND NVL(c.CONSENT_MARKETING_FLG, 'N') = 'N' THEN 'SUPPRESS'
           WHEN c.REGION_CD = 'APAC' AND NVL(c.CONSENT_MARKETING_FLG, 'N') = 'N' THEN 'FLAG'
           ELSE 'NONE'
       END                                                        AS CONSENT_ACTION_CD,
       c.CONSENT_MARKETING_FLG                                    AS CONSENT_FLAG,
       c.CONSENT_CAPTURED_DT                                      AS CONSENT_DT,
       c.RETENTION_UNTIL_DT,
       c.SOURCE_SYS                                               AS SRC_SYSTEM_CD,
       c.CREATED_DT,
       NVL(c.UPDATED_DT, c.CREATED_DT)                            AS LAST_UPD_DT,
       NVL(c.UPDATED_BY, c.CREATED_BY)                            AS LAST_UPD_BY
  FROM WWI_MDM.CUST_MASTER c
  LEFT OUTER JOIN WWI_MDM.CUST_CREDIT_PROFILE cp
    ON cp.CUST_ID = c.CUST_ID
  LEFT OUTER JOIN WWI_REF.COUNTRY_REF cr
    ON cr.COUNTRY_CD = c.COUNTRY_CD
 WHERE c.CUST_STATUS_CD <> 'CL'
   AND NVL(c.DELETED_FLG, 'N') = 'N'
/
