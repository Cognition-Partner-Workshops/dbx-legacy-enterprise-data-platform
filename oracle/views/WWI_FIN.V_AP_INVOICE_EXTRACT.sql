/* ============================================================================
 * Object      : WWI_FIN.V_AP_INVOICE_EXTRACT (view)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.AP_INVOICE_HDR, WWI_FIN.AP_INVOICE_HOLD,
 *               WWI_FIN.AP_PAYMENT_APPLY, WWI_MDM.SUPP_MASTER,
 *               WWI_FIN.FN_CONVERT_AMOUNT, WWI_FIN.FN_DUE_DATE,
 *               WWI_FIN.FN_AGING_BUCKET, WWI_REF.FN_FISCAL_PERIOD
 * Called by   : SSIS EXT_ORA_ApInvoiceHdr (incremental on LAST_UPD_DT),
 *               WWI_AUDIT.PRC_PREPARE_INVOICE_EXTRACT
 * History     : 1999 original; 2007 reverse-charge flag; 2011 hold summary;
 *               2016 the DW asked for the base amount to be pre-converted.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_FIN.V_AP_INVOICE_EXTRACT AS
SELECT i.INVOICE_ID,
       i.INVOICE_NUM,
       i.SUPP_ID,
       s.SUPP_NUM,
       s.SUPP_NAME,
       i.PO_ID,
       i.REGION_CD,
       i.ORG_CD,
       i.INVOICE_DT,
       i.RECEIVED_DT,
       i.GL_DATE,
       NVL(i.PERIOD_CD, WWI_REF.FN_FISCAL_PERIOD(i.GL_DATE, i.REGION_CD)) AS PERIOD_CD,
       i.CURRENCY_CD,
       i.EXCHANGE_RATE_NUM,
       i.INVOICE_AMT,
       i.TAX_AMT,
       i.WITHHOLDING_AMT,
       NVL(i.PAID_AMT, 0)                                       AS PAID_AMT,
       i.INVOICE_AMT - NVL(i.PAID_AMT, 0)                       AS OUTSTANDING_AMT,
       NVL(i.BASE_AMT,
           WWI_FIN.FN_CONVERT_AMOUNT(i.INVOICE_AMT, i.CURRENCY_CD, 'USD',
                                     i.GL_DATE, 'CORP'))        AS BASE_AMT_USD,
       i.PAYMENT_TERMS_CD,
       NVL(i.DUE_DT, WWI_FIN.FN_DUE_DATE(i.INVOICE_DT, i.PAYMENT_TERMS_CD, i.REGION_CD))
                                                                AS DUE_DT,
       i.DISCOUNT_DT,
       i.DISCOUNT_AMT,
       WWI_FIN.FN_AGING_BUCKET(TRUNC(SYSDATE) - NVL(i.DUE_DT, i.INVOICE_DT), i.REGION_CD)
                                                                AS AGING_BUCKET_CD,
       i.STATUS_CD,
       i.MATCH_TYPE_CD,
       i.REVERSE_CHARGE_FLAG,
       i.TAX_REG_NUM,
       NVL(hold.ACTIVE_HOLD_COUNT, 0)                           AS ACTIVE_HOLD_COUNT,
       hold.HOLD_CODES_TXT,
       NVL(app.APPLIED_COUNT, 0)                                AS PAYMENT_APPLY_COUNT,
       app.LAST_APPLY_DT,
       i.APPROVED_BY,
       i.APPROVED_DT,
       i.SRC_SYSTEM_CD,
       i.INTERFACE_BATCH_ID,
       i.CREATED_DT,
       i.LAST_UPD_DT
  FROM WWI_FIN.AP_INVOICE_HDR i
  JOIN WWI_MDM.SUPP_MASTER s
    ON s.SUPP_ID = i.SUPP_ID
  LEFT OUTER JOIN (
        SELECT h.INVOICE_ID,
               COUNT(*)                                          AS ACTIVE_HOLD_COUNT,
               LISTAGG(h.HOLD_CD, ',') WITHIN GROUP (ORDER BY h.HOLD_CD) AS HOLD_CODES_TXT
          FROM WWI_FIN.AP_INVOICE_HOLD h
         WHERE NVL(h.ACTIVE_FLAG, 'Y') = 'Y'
           AND h.RELEASED_DT IS NULL
         GROUP BY h.INVOICE_ID
       ) hold
    ON hold.INVOICE_ID = i.INVOICE_ID
  LEFT OUTER JOIN (
        SELECT a.INVOICE_ID, COUNT(*) AS APPLIED_COUNT, MAX(a.APPLY_DT) AS LAST_APPLY_DT
          FROM WWI_FIN.AP_PAYMENT_APPLY a
         WHERE NVL(a.REVERSED_FLAG, 'N') = 'N'
         GROUP BY a.INVOICE_ID
       ) app
    ON app.INVOICE_ID = i.INVOICE_ID
 WHERE i.STATUS_CD <> 'EN'
/
