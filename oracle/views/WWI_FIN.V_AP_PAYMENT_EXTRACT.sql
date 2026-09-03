/* ============================================================================
 * Object      : WWI_FIN.V_AP_PAYMENT_EXTRACT (view)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.AP_PAYMENT, WWI_FIN.AP_PAYMENT_APPLY,
 *               WWI_FIN.AP_INVOICE_HDR, WWI_MDM.SUPP_MASTER,
 *               WWI_REF.PAYMENT_METHOD_REF, WWI_FIN.FN_CONVERT_AMOUNT
 * Called by   : SSIS EXT_ORA_ApPayment (incremental on LAST_UPD_DT)
 * History     : 2000 original; 2005 void handling; 2013 unapplied amount added
 *               after treasury found payments on account nobody could see.
 * Notes       : AP_PAYMENT has no void flag - a payment is void when VOID_DT
 *               is set - so the flag the extract contract promises is derived
 *               here rather than stored.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_FIN.V_AP_PAYMENT_EXTRACT AS
SELECT p.PAYMENT_ID,
       p.PAYMENT_NBR                                           AS PAYMENT_NUM,
       p.SUPP_ID,
       s.SUPP_NBR                                              AS SUPP_NUM,
       s.SUPP_NAME,
       p.REGION_CD,
       p.LEGAL_ENTITY_CD                                       AS ORG_CD,
       p.PAYMENT_DT,
       p.GL_DATE,
       p.PERIOD_CD,
       p.CLEARED_DT,
       p.PAYMENT_CURR_CD                                       AS CURRENCY_CD,
       p.PAYMENT_AMT,
       p.ACCOUNTED_AMT,
       p.WITHHELD_AMT                                          AS WITHHOLDING_AMT,
       p.DISCOUNT_TAKEN_AMT,
       NVL(ap.APPLIED_AMT, 0)                                  AS APPLIED_AMT,
       p.PAYMENT_AMT - NVL(ap.APPLIED_AMT, 0)                  AS UNAPPLIED_AMT,
       WWI_FIN.FN_CONVERT_AMOUNT(p.PAYMENT_AMT, p.PAYMENT_CURR_CD, 'USD',
                                 p.PAYMENT_DT, 'CORP')         AS PAYMENT_AMT_USD,
       p.PAYMENT_METHOD_CD,
       pm.METHOD_NAME                                          AS PAYMENT_METHOD_NAME,
       p.SUPP_BANK_ACCOUNT_ID                                  AS BANK_ACCT_ID,
       p.BANK_ACCOUNT_CD,
       p.PAYMENT_BATCH_NBR                                     AS PAYMENT_RUN_ID,
       p.PAYMENT_STATUS_CD                                     AS STATUS_CD,
       CASE WHEN p.VOID_DT IS NOT NULL THEN 'Y' ELSE 'N' END   AS VOID_FLAG,
       p.VOID_DT,
       p.VOID_REASON_CD,
       NVL(ap.INVOICE_COUNT, 0)                                AS INVOICE_COUNT,
       ap.EARLIEST_INVOICE_DT,
       /* EU payments settle through SEPA and are considered final two days
        * after value date; NA cheques stay outstanding until they clear. */
       CASE
           WHEN p.VOID_DT IS NOT NULL                                     THEN 'VOID'
           WHEN p.REGION_CD = 'EU'  AND p.PAYMENT_DT <= TRUNC(SYSDATE) - 2 THEN 'SETTLED'
           WHEN p.REGION_CD = 'APAC' AND p.CLEARED_DT IS NOT NULL         THEN 'SETTLED'
           WHEN p.CLEARED_DT IS NOT NULL                                  THEN 'SETTLED'
           ELSE 'IN_FLIGHT'
       END                                                     AS SETTLEMENT_STATUS_CD,
       p.CREATED_DT,
       NVL(p.UPDATED_DT, p.CREATED_DT)                         AS LAST_UPD_DT
  FROM WWI_FIN.AP_PAYMENT p
  JOIN WWI_MDM.SUPP_MASTER s
    ON s.SUPP_ID = p.SUPP_ID
  LEFT OUTER JOIN WWI_REF.PAYMENT_METHOD_REF pm
    ON pm.PAYMENT_METHOD_CD = p.PAYMENT_METHOD_CD
   AND pm.COUNTRY_CD = s.COUNTRY_CD
  LEFT OUTER JOIN (
        SELECT a.PAYMENT_ID,
               SUM(a.APPLIED_AMT)                  AS APPLIED_AMT,
               COUNT(DISTINCT a.INVOICE_ID)        AS INVOICE_COUNT,
               MIN(i.INVOICE_DT)                   AS EARLIEST_INVOICE_DT
          FROM WWI_FIN.AP_PAYMENT_APPLY a
          JOIN WWI_FIN.AP_INVOICE_HDR i
            ON i.INVOICE_ID = a.INVOICE_ID
         WHERE NVL(a.REVERSED_FLG, 'N') = 'N'
         GROUP BY a.PAYMENT_ID
       ) ap
    ON ap.PAYMENT_ID = p.PAYMENT_ID
/
