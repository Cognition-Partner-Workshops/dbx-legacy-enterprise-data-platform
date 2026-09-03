/* ============================================================================
 * Object      : WWI_PROC.V_OPEN_PO_BALANCE (view)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PURCHASE_ORDER_HDR, WWI_PROC.PURCHASE_ORDER_LINE,
 *               WWI_PROC.PO_RECEIPT_LINE, WWI_FIN.AP_INVOICE_LINE,
 *               WWI_FIN.FN_CONVERT_AMOUNT
 * Called by   : WWI_FIN.PRC_ACCRUE_UNINVOICED_RECEIPTS, month-end commitment
 *               reporting, WWI_PROC.PRC_CLOSE_STALE_PO
 * Warning     : Deliberately expensive. Three nested inline views, one
 *               correlated subquery per row and a per-row currency conversion.
 *               Month-end runs this against the whole order book.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_PROC.V_OPEN_PO_BALANCE AS
SELECT b.PO_ID,
       b.PO_NUM,
       b.SUPP_ID,
       b.REGION_CD,
       b.CURRENCY_CD,
       b.ORDER_DT,
       b.PROMISED_DT,
       b.OPEN_LINE_COUNT,
       b.ORDERED_AMT,
       b.RECEIVED_AMT,
       b.INVOICED_AMT,
       b.ORDERED_AMT - b.INVOICED_AMT                            AS OPEN_COMMITMENT_AMT,
       GREATEST(b.RECEIVED_AMT - b.INVOICED_AMT, 0)              AS ACCRUAL_CANDIDATE_AMT,
       WWI_FIN.FN_CONVERT_AMOUNT(GREATEST(b.RECEIVED_AMT - b.INVOICED_AMT, 0),
                                 b.CURRENCY_CD, 'USD', TRUNC(SYSDATE), 'CORP')
                                                                 AS ACCRUAL_CANDIDATE_USD,
       (SELECT MAX(rl.RECEIPT_DT)
          FROM WWI_PROC.PO_RECEIPT_LINE rl
          JOIN WWI_PROC.PURCHASE_ORDER_LINE pl2
            ON pl2.PO_LINE_ID = rl.PO_LINE_ID
         WHERE pl2.PO_ID = b.PO_ID)                              AS LAST_RECEIPT_DT,
       CASE
           WHEN b.ORDERED_AMT = 0 THEN 0
           ELSE ROUND(b.INVOICED_AMT / b.ORDERED_AMT * 100, 2)
       END                                                       AS PCT_INVOICED
  FROM (
        SELECT h.PO_ID,
               h.PO_NUM,
               h.SUPP_ID,
               h.REGION_CD,
               h.CURRENCY_CD,
               h.ORDER_DT,
               h.PROMISED_DT,
               COUNT(CASE WHEN NVL(l.CLOSED_FLAG, 'N') = 'N' THEN 1 END) AS OPEN_LINE_COUNT,
               SUM(l.LINE_AMT)                                            AS ORDERED_AMT,
               SUM(NVL(rec.RECEIVED_AMT, 0))                              AS RECEIVED_AMT,
               SUM(NVL(inv.INVOICED_AMT, 0))                              AS INVOICED_AMT
          FROM WWI_PROC.PURCHASE_ORDER_HDR h
          JOIN WWI_PROC.PURCHASE_ORDER_LINE l
            ON l.PO_ID = h.PO_ID
          LEFT OUTER JOIN (
                SELECT rl.PO_LINE_ID,
                       SUM(NVL(rl.ACCEPTED_QTY, rl.RECEIVED_QTY) * pl.UNIT_PRICE_AMT) AS RECEIVED_AMT
                  FROM WWI_PROC.PO_RECEIPT_LINE rl
                  JOIN WWI_PROC.PURCHASE_ORDER_LINE pl
                    ON pl.PO_LINE_ID = rl.PO_LINE_ID
                 WHERE NVL(rl.INSPECTION_STATUS_CD, 'ACC') <> 'REJ'
                 GROUP BY rl.PO_LINE_ID
               ) rec
            ON rec.PO_LINE_ID = l.PO_LINE_ID
          LEFT OUTER JOIN (
                SELECT il.PO_LINE_ID, SUM(il.LINE_AMT) AS INVOICED_AMT
                  FROM WWI_FIN.AP_INVOICE_LINE il
                  JOIN WWI_FIN.AP_INVOICE_HDR ih
                    ON ih.INVOICE_ID = il.INVOICE_ID
                 WHERE ih.STATUS_CD NOT IN ('CN', 'EN')
                 GROUP BY il.PO_LINE_ID
               ) inv
            ON inv.PO_LINE_ID = l.PO_LINE_ID
         WHERE h.STATUS_CD IN ('AP', 'OP')
         GROUP BY h.PO_ID, h.PO_NUM, h.SUPP_ID, h.REGION_CD, h.CURRENCY_CD,
                  h.ORDER_DT, h.PROMISED_DT
       ) b
 WHERE b.OPEN_LINE_COUNT > 0
    OR b.ORDERED_AMT > b.INVOICED_AMT
/
