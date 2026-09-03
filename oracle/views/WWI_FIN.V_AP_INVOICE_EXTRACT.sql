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
 * Notes       : The extract column names are the downstream contract and are
 *               kept stable; the ERP column names behind them are not.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_FIN.V_AP_INVOICE_EXTRACT AS
SELECT i.INVOICE_ID,
       i.INVOICE_NBR                                            AS INVOICE_NUM,
       i.SUPP_ID,
       s.SUPP_NBR                                               AS SUPP_NUM,
       s.SUPP_NAME,
       i.PO_ID,
       i.REGION_CD,
       i.LEGAL_ENTITY_CD                                        AS ORG_CD,
       i.INVOICE_TYPE_CD,
       i.INVOICE_DT,
       i.RECEIVED_DT,
       i.GL_DATE,
       NVL(i.PERIOD_CD, WWI_REF.FN_FISCAL_PERIOD(i.GL_DATE, i.REGION_CD)) AS PERIOD_CD,
       i.INVOICE_CURR_CD                                        AS CURRENCY_CD,
       i.FX_RATE                                                AS EXCHANGE_RATE_NUM,
       i.GROSS_AMT                                              AS INVOICE_AMT,
       i.NET_AMT,
       i.TAX_AMT,
       i.FREIGHT_AMT,
       i.WITHHELD_AMT                                           AS WITHHOLDING_AMT,
       NVL(i.PAID_AMT, 0)                                       AS PAID_AMT,
       NVL(i.BALANCE_AMT, i.GROSS_AMT - NVL(i.PAID_AMT, 0))     AS OUTSTANDING_AMT,
       WWI_FIN.FN_CONVERT_AMOUNT(i.GROSS_AMT, i.INVOICE_CURR_CD, 'USD',
                                 i.GL_DATE, 'CORP')             AS BASE_AMT_USD,
       i.PAYMENT_TERMS_CD,
       NVL(i.DUE_DT,
           WWI_FIN.FN_DUE_DATE(i.INVOICE_DT, i.PAYMENT_TERMS_CD, i.REGION_CD)) AS DUE_DT,
       i.DISCOUNT_DUE_DT                                        AS DISCOUNT_DT,
       i.DISCOUNT_TAKEN_AMT                                     AS DISCOUNT_AMT,
       WWI_FIN.FN_AGING_BUCKET(TRUNC(SYSDATE) - NVL(i.DUE_DT, i.INVOICE_DT), i.REGION_CD)
                                                                AS AGING_BUCKET_CD,
       i.INVOICE_STATUS_CD                                      AS STATUS_CD,
       i.MATCH_STATUS_CD                                        AS MATCH_TYPE_CD,
       i.APPROVAL_STATUS_CD,
       NVL(i.EU_SELF_BILLING_FLG, 'N')                          AS REVERSE_CHARGE_FLAG,
       COALESCE(i.EU_SUPPLIER_VAT_NBR, i.APAC_GST_REG_NBR)      AS TAX_REG_NUM,
       i.CANCELLED_FLG,
       NVL(hold.ACTIVE_HOLD_COUNT, 0)                           AS ACTIVE_HOLD_COUNT,
       hold.HOLD_CODES_TXT,
       NVL(app.APPLIED_COUNT, 0)                                AS PAYMENT_APPLY_COUNT,
       app.LAST_APPLY_DT,
       i.ENTERED_BY_CD,
       i.SOURCE_SYS                                             AS SRC_SYSTEM_CD,
       i.CREATED_DT,
       NVL(i.UPDATED_DT, i.CREATED_DT)                          AS LAST_UPD_DT
  FROM WWI_FIN.AP_INVOICE_HDR i
  JOIN WWI_MDM.SUPP_MASTER s
    ON s.SUPP_ID = i.SUPP_ID
  LEFT OUTER JOIN (
        SELECT h.INVOICE_ID,
               COUNT(*)                                          AS ACTIVE_HOLD_COUNT,
               LISTAGG(h.HOLD_CODE_CD, ',') WITHIN GROUP (ORDER BY h.HOLD_CODE_CD)
                                                                 AS HOLD_CODES_TXT
          FROM WWI_FIN.AP_INVOICE_HOLD h
         WHERE NVL(h.RELEASED_FLG, 'N') = 'N'
           AND h.RELEASED_DT IS NULL
         GROUP BY h.INVOICE_ID
       ) hold
    ON hold.INVOICE_ID = i.INVOICE_ID
  LEFT OUTER JOIN (
        SELECT a.INVOICE_ID, COUNT(*) AS APPLIED_COUNT, MAX(a.APPLY_DT) AS LAST_APPLY_DT
          FROM WWI_FIN.AP_PAYMENT_APPLY a
         WHERE NVL(a.REVERSED_FLG, 'N') = 'N'
         GROUP BY a.INVOICE_ID
       ) app
    ON app.INVOICE_ID = i.INVOICE_ID
 WHERE i.INVOICE_STATUS_CD <> 'ENTR'
/
