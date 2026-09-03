/* ============================================================================
 * Object      : WWI_PROC.V_RECEIPT_EXTRACT (view)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PO_RECEIPT_LINE, WWI_PROC.PO_RECEIPT_HDR,
 *               WWI_PROC.PURCHASE_ORDER_LINE, WWI_PROC.PURCHASE_ORDER_HDR,
 *               WWI_PROC.GOODS_RETURN_LINE, WWI_PROC.FN_RECEIPT_VARIANCE_PCT
 * Called by   : SSIS EXT_ORA_ReceiptLine (incremental on RECEIPT_LINE_ID)
 * History     : 2002 original; 2008 inspection status; 2012 returns netted in.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_PROC.V_RECEIPT_EXTRACT AS
SELECT rl.RECEIPT_LINE_ID,
       rl.RECEIPT_ID,
       rh.RECEIPT_NUM,
       rh.PO_ID,
       ph.PO_NUM,
       rh.SUPP_ID,
       ph.REGION_CD,
       rh.WAREHOUSE_CD,
       rh.RECEIPT_DT,
       rh.RECEIVED_BY,
       rh.PACKING_SLIP_NUM,
       rl.PO_LINE_ID,
       pl.LINE_NUM                                          AS PO_LINE_NUM,
       rl.PRODUCT_ID,
       rl.RECEIVED_QTY,
       NVL(rl.ACCEPTED_QTY, rl.RECEIVED_QTY)                AS ACCEPTED_QTY,
       NVL(rl.REJECTED_QTY, 0)                              AS REJECTED_QTY,
       rl.REJECT_REASON_CD,
       rl.UOM_CD,
       rl.INSPECTION_STATUS_CD,
       pl.ORDER_QTY,
       pl.UNIT_PRICE_AMT,
       ROUND(NVL(rl.ACCEPTED_QTY, rl.RECEIVED_QTY) * pl.UNIT_PRICE_AMT, 2) AS RECEIPT_VALUE_AMT,
       WWI_PROC.FN_RECEIPT_VARIANCE_PCT(rl.RECEIPT_LINE_ID) AS RECEIPT_VARIANCE_PCT,
       NVL(ret.RETURNED_QTY, 0)                             AS RETURNED_QTY,
       CASE
           WHEN rh.RECEIPT_DT <= pl.NEED_BY_DT                        THEN 'ON_TIME'
           WHEN rh.RECEIPT_DT <= pl.NEED_BY_DT + 3                    THEN 'LATE_TOLERATED'
           ELSE 'LATE'
       END                                                  AS DELIVERY_TIMING_CD,
       rh.STATUS_CD                                         AS RECEIPT_STATUS_CD
  FROM WWI_PROC.PO_RECEIPT_LINE rl
  JOIN WWI_PROC.PO_RECEIPT_HDR rh
    ON rh.RECEIPT_ID = rl.RECEIPT_ID
  JOIN WWI_PROC.PURCHASE_ORDER_LINE pl
    ON pl.PO_LINE_ID = rl.PO_LINE_ID
  JOIN WWI_PROC.PURCHASE_ORDER_HDR ph
    ON ph.PO_ID = pl.PO_ID
  LEFT OUTER JOIN (
        SELECT g.RECEIPT_LINE_ID, SUM(g.RETURN_QTY) AS RETURNED_QTY
          FROM WWI_PROC.GOODS_RETURN_LINE g
         GROUP BY g.RECEIPT_LINE_ID
       ) ret
    ON ret.RECEIPT_LINE_ID = rl.RECEIPT_LINE_ID
 WHERE rh.STATUS_CD <> 'CANCELLED'
/
