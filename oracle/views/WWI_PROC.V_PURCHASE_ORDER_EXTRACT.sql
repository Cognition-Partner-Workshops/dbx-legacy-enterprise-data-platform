/* ============================================================================
 * Object      : WWI_PROC.V_PURCHASE_ORDER_EXTRACT (view)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PURCHASE_ORDER_HDR, WWI_PROC.PURCHASE_ORDER_LINE,
 *               WWI_PROC.PO_CHANGE_ORDER, WWI_MDM.SUPP_MASTER,
 *               WWI_PROC.VENDOR_CONTRACT, WWI_REF.FN_TRANSLATE_CODE,
 *               WWI_REF.FN_FISCAL_PERIOD, WWI_FIN.FN_CONVERT_AMOUNT
 * Called by   : SSIS EXT_ORA_PurchaseOrderHdr (incremental on LAST_UPD_DT)
 * History     : 2001 original; 2009 change-order counters; 2014 base amount
 *               added so the DW stopped doing its own conversion.
 * Notes       : A PO line is open while its LINE_STATUS_CD is OPEN or PART;
 *               there is no closed flag. A change order counts once it has an
 *               approval date.
 *               Reads WWI_MDM; the SELECT grants live in
 *               oracle/ddl/05_grant_privileges.sql.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_PROC.V_PURCHASE_ORDER_EXTRACT AS
SELECT h.PO_ID,
       h.PO_NBR                                         AS PO_NUM,
       h.PO_TYPE_CD,
       h.SUPP_ID,
       s.SUPP_NBR                                       AS SUPP_NUM,
       s.SUPP_NAME,
       h.REGION_CD,
       h.BILL_TO_LOCATION_CD,
       h.SHIP_TO_LOCATION_CD,
       h.ORDER_CURR_CD                                  AS CURRENCY_CD,
       h.PO_STATUS_CD                                   AS STATUS_CD,
       h.APPROVAL_STATUS_CD,
       WWI_REF.FN_TRANSLATE_CODE('PO_STATUS', h.PO_STATUS_CD, 'ORAERP', 'WWIDW',
                                 h.REGION_CD)           AS DW_STATUS_CD,
       h.ORDER_DT,
       h.PROMISED_DT,
       WWI_REF.FN_FISCAL_PERIOD(h.ORDER_DT, h.REGION_CD) AS ORDER_PERIOD_CD,
       h.PAYMENT_TERMS_CD,
       h.INCOTERM_CD,
       h.BUYER_CD,
       h.CONTRACT_ID,
       vc.CONTRACT_NBR                                  AS CONTRACT_NUM,
       h.SUBTOTAL_AMT,
       h.FREIGHT_AMT,
       h.TAX_AMT,
       h.TOTAL_AMT,
       WWI_FIN.FN_CONVERT_AMOUNT(h.TOTAL_AMT, h.ORDER_CURR_CD, 'USD',
                                 NVL(h.ORDER_DT, SYSDATE), 'CORP')  AS TOTAL_AMT_USD,
       lines.LINE_COUNT,
       lines.OPEN_LINE_COUNT,
       lines.ORDERED_QTY,
       lines.RECEIVED_QTY,
       lines.INVOICED_QTY,
       NVL(chg.CHANGE_COUNT, 0)                          AS CHANGE_ORDER_COUNT,
       chg.LAST_CHANGE_DT,
       CASE
           WHEN h.PO_STATUS_CD IN ('CLSD', 'CANC')                      THEN 'N'
           WHEN lines.OPEN_LINE_COUNT > 0                               THEN 'Y'
           ELSE 'N'
       END                                               AS OPEN_FLAG,
       CASE
           WHEN h.PROMISED_DT < TRUNC(SYSDATE) AND lines.OPEN_LINE_COUNT > 0
               THEN TRUNC(SYSDATE) - h.PROMISED_DT
           ELSE 0
       END                                               AS DAYS_LATE_NUM,
       h.CANCELLED_FLG,
       h.CANCEL_REASON_CD,
       h.CLOSED_DT,
       h.CREATED_DT,
       NVL(h.UPDATED_DT, h.CREATED_DT)                   AS LAST_UPD_DT
  FROM WWI_PROC.PURCHASE_ORDER_HDR h
  JOIN WWI_MDM.SUPP_MASTER s
    ON s.SUPP_ID = h.SUPP_ID
  LEFT OUTER JOIN WWI_PROC.VENDOR_CONTRACT vc
    ON vc.CONTRACT_ID = h.CONTRACT_ID
  LEFT OUTER JOIN (
        SELECT pl.PO_ID,
               COUNT(*)                                                 AS LINE_COUNT,
               COUNT(CASE WHEN pl.LINE_STATUS_CD IN ('OPEN', 'PART') THEN 1 END)
                                                                        AS OPEN_LINE_COUNT,
               SUM(pl.ORDER_QTY)                                        AS ORDERED_QTY,
               SUM(NVL(pl.RECEIVED_QTY, 0))                             AS RECEIVED_QTY,
               SUM(NVL(pl.BILLED_QTY, 0))                               AS INVOICED_QTY
          FROM WWI_PROC.PURCHASE_ORDER_LINE pl
         GROUP BY pl.PO_ID
       ) lines
    ON lines.PO_ID = h.PO_ID
  LEFT OUTER JOIN (
        SELECT c.PO_ID, COUNT(*) AS CHANGE_COUNT, MAX(c.APPROVED_DT) AS LAST_CHANGE_DT
          FROM WWI_PROC.PO_CHANGE_ORDER c
         WHERE c.APPROVED_DT IS NOT NULL
         GROUP BY c.PO_ID
       ) chg
    ON chg.PO_ID = h.PO_ID
 WHERE h.PO_STATUS_CD <> 'CANC'
/
