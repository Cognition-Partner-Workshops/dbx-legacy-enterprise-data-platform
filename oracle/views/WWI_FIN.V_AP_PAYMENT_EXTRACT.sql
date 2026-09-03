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
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_FIN.V_AP_PAYMENT_EXTRACT AS
SELECT p.PAYMENT_ID,
       p.PAYMENT_NUM,
       p.SUPP_ID,
       s.SUPP_NUM,
       s.SUPP_NAME,
       p.REGION_CD,
       p.ORG_CD,
       p.PAYMENT_DT,
       p.CLEARED_DT,
       p.CURRENCY_CD,
       p.PAYMENT_AMT,
       p.WITHHOLDING_AMT,
       p.DISCOUNT_TAKEN_AMT,
       NVL(ap.APPLIED_AMT, 0)                                  AS APPLIED_AMT,
       p.PAYMENT_AMT - NVL(ap.APPLIED_AMT, 0)                  AS UNAPPLIED_AMT,
       WWI_FIN.FN_CONVERT_AMOUNT(p.PAYMENT_AMT, p.CURRENCY_CD, 'USD',
                                 p.PAYMENT_DT, 'CORP')         AS PAYMENT_AMT_USD,
       p.PAYMENT_METHOD_CD,
       pm.METHOD_NAME                                          AS PAYMENT_METHOD_NAME,
       p.BANK_ACCT_ID,
       p.PAYMENT_RUN_ID,
       p.STATUS_CD,
       p.VOID_FLAG,
       p.VOID_DT,
       p.VOID_REASON_CD,
       NVL(ap.INVOICE_COUNT, 0)                                AS INVOICE_COUNT,
       ap.EARLIEST_INVOICE_DT,
       /* EU payments settle through SEPA and are considered final two days
        * after value date; NA cheques stay outstanding until they clear. */
       CASE
           WHEN NVL(p.VOID_FLAG, 'N') = 'Y'                             THEN 'VOID'
           WHEN p.REGION_CD = 'EU'  AND p.PAYMENT_DT <= TRUNC(SYSDATE) - 2 THEN 'SETTLED'
           WHEN p.REGION_CD = 'APAC' AND p.CLEARED_DT IS NOT NULL         THEN 'SETTLED'
           WHEN p.CLEARED_DT IS NOT NULL                                  THEN 'SETTLED'
           ELSE 'IN_FLIGHT'
       END                                                     AS SETTLEMENT_STATUS_CD,
       p.CREATED_DT,
       p.LAST_UPD_DT
  FROM WWI_FIN.AP_PAYMENT p
  JOIN WWI_MDM.SUPP_MASTER s
    ON s.SUPP_ID = p.SUPP_ID
  LEFT OUTER JOIN WWI_REF.PAYMENT_METHOD_REF pm
    ON pm.PAYMENT_METHOD_CD = p.PAYMENT_METHOD_CD
  LEFT OUTER JOIN (
        SELECT a.PAYMENT_ID,
               SUM(a.APPLIED_AMT)                  AS APPLIED_AMT,
               COUNT(DISTINCT a.INVOICE_ID)        AS INVOICE_COUNT,
               MIN(i.INVOICE_DT)                   AS EARLIEST_INVOICE_DT
          FROM WWI_FIN.AP_PAYMENT_APPLY a
          JOIN WWI_FIN.AP_INVOICE_HDR i
            ON i.INVOICE_ID = a.INVOICE_ID
         WHERE NVL(a.REVERSED_FLAG, 'N') = 'N'
         GROUP BY a.PAYMENT_ID
       ) ap
    ON ap.PAYMENT_ID = p.PAYMENT_ID
/
