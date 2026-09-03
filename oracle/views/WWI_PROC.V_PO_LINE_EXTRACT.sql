/* ============================================================================
 * Object      : WWI_PROC.V_PO_LINE_EXTRACT (view)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PURCHASE_ORDER_LINE, WWI_PROC.PURCHASE_ORDER_HDR,
 *               WWI_PROC.VENDOR_CONTRACT_LINE, WWI_MDM.PRODUCT_MASTER,
 *               WWI_PROC.FN_PO_OPEN_QTY
 * Called by   : SSIS EXT_ORA_PurchaseOrderLine (incremental on PO_LINE_ID)
 * History     : 2001 original; 2010 contract price variance added for the
 *               procurement savings dashboard.
 * Notes       : Emits PO_LINE_ID ascending; the extract watermark is the key,
 *               so corrections to old lines are only picked up by the weekly
 *               full reload. This is a known and accepted gap.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_PROC.V_PO_LINE_EXTRACT AS
SELECT pl.PO_LINE_ID,
       pl.PO_ID,
       h.PO_NUM,
       h.SUPP_ID,
       h.REGION_CD,
       h.CURRENCY_CD,
       pl.LINE_NUM,
       pl.PRODUCT_ID,
       pm.PRODUCT_NUM,
       pm.PRODUCT_NAME,
       pl.ORDER_QTY,
       NVL(pl.RECEIVED_QTY, 0)                              AS RECEIVED_QTY,
       NVL(pl.INVOICED_QTY, 0)                              AS INVOICED_QTY,
       NVL(pl.CANCELLED_QTY, 0)                             AS CANCELLED_QTY,
       WWI_PROC.FN_PO_OPEN_QTY(pl.PO_LINE_ID)               AS OPEN_QTY,
       pl.UOM_CD,
       pl.UNIT_PRICE_AMT,
       pl.LINE_AMT,
       cl.CONTRACT_PRICE_AMT,
       CASE
           WHEN cl.CONTRACT_PRICE_AMT IS NULL OR cl.CONTRACT_PRICE_AMT = 0 THEN NULL
           ELSE ROUND((pl.UNIT_PRICE_AMT - cl.CONTRACT_PRICE_AMT)
                      / cl.CONTRACT_PRICE_AMT * 100, 4)
       END                                                  AS CONTRACT_PRICE_VAR_PCT,
       CASE
           WHEN cl.CONTRACT_PRICE_AMT IS NULL THEN 'OFF_CONTRACT'
           WHEN pl.UNIT_PRICE_AMT > cl.CONTRACT_PRICE_AMT
                * (1 + NVL(cl.PRICE_TOLERANCE_PCT, 0) / 100) THEN 'OVER_CONTRACT'
           ELSE 'ON_CONTRACT'
       END                                                  AS CONTRACT_COMPLIANCE_CD,
       pl.TAX_CD,
       pl.NEED_BY_DT,
       pl.STATUS_CD,
       pl.CLOSED_FLAG,
       pl.LAST_UPD_DT
  FROM WWI_PROC.PURCHASE_ORDER_LINE pl
  JOIN WWI_PROC.PURCHASE_ORDER_HDR h
    ON h.PO_ID = pl.PO_ID
  LEFT OUTER JOIN WWI_MDM.PRODUCT_MASTER pm
    ON pm.PRODUCT_ID = pl.PRODUCT_ID
  LEFT OUTER JOIN WWI_PROC.VENDOR_CONTRACT_LINE cl
    ON cl.CONTRACT_ID = h.CONTRACT_ID
   AND cl.PRODUCT_ID  = pl.PRODUCT_ID
   AND NVL(h.ORDER_DT, TRUNC(SYSDATE)) BETWEEN cl.EFF_FROM_DT
                                           AND NVL(cl.EFF_TO_DT, DATE '4712-12-31')
 WHERE h.STATUS_CD <> 'DR'
/
