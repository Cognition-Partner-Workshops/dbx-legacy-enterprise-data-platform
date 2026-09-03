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
 *
 *               Contract lines carry no price tolerance, so any unit price
 *               above the contract price is over-contract.
 *               Reads WWI_MDM; the SELECT grants live in
 *               oracle/ddl/05_grant_privileges.sql.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_PROC.V_PO_LINE_EXTRACT AS
SELECT pl.PO_LINE_ID,
       pl.PO_ID,
       h.PO_NBR                                             AS PO_NUM,
       h.SUPP_ID,
       h.REGION_CD,
       pl.LINE_CURR_CD                                      AS CURRENCY_CD,
       pl.LINE_NBR                                          AS LINE_NUM,
       pl.PRODUCT_ID,
       pm.ITEM_NBR                                          AS PRODUCT_NUM,
       pm.ITEM_DESC_SHORT                                   AS PRODUCT_NAME,
       pl.SUPPLIER_ITEM_CD,
       pl.ORDER_QTY,
       NVL(pl.RECEIVED_QTY, 0)                              AS RECEIVED_QTY,
       NVL(pl.BILLED_QTY, 0)                                AS INVOICED_QTY,
       NVL(pl.RETURNED_QTY, 0)                              AS RETURNED_QTY,
       NVL(pl.CANCELLED_QTY, 0)                             AS CANCELLED_QTY,
       WWI_PROC.FN_PO_OPEN_QTY(pl.PO_LINE_ID)               AS OPEN_QTY,
       pl.UOM_CD,
       pl.UNIT_PRICE                                        AS UNIT_PRICE_AMT,
       pl.DISCOUNT_PCT,
       pl.LINE_AMT,
       pl.TAX_AMT,
       cl.CONTRACT_PRICE                                    AS CONTRACT_PRICE_AMT,
       CASE
           WHEN cl.CONTRACT_PRICE IS NULL OR cl.CONTRACT_PRICE = 0 THEN NULL
           ELSE ROUND((pl.UNIT_PRICE - cl.CONTRACT_PRICE)
                      / cl.CONTRACT_PRICE * 100, 4)
       END                                                  AS CONTRACT_PRICE_VAR_PCT,
       CASE
           WHEN cl.CONTRACT_PRICE IS NULL              THEN 'OFF_CONTRACT'
           WHEN pl.UNIT_PRICE > cl.CONTRACT_PRICE      THEN 'OVER_CONTRACT'
           ELSE 'ON_CONTRACT'
       END                                                  AS CONTRACT_COMPLIANCE_CD,
       pl.TAX_CODE_CD                                       AS TAX_CD,
       pl.GL_ACCOUNT_CD,
       pl.COST_CENTER_CD,
       pl.NEED_BY_DT,
       pl.PROMISED_DT,
       pl.LAST_RECEIPT_DT,
       pl.LINE_STATUS_CD                                    AS STATUS_CD,
       CASE WHEN pl.LINE_STATUS_CD IN ('OPEN', 'PART') THEN 'N' ELSE 'Y' END
                                                            AS CLOSED_FLAG,
       NVL(pl.UPDATED_DT, pl.CREATED_DT)                    AS LAST_UPD_DT
  FROM WWI_PROC.PURCHASE_ORDER_LINE pl
  JOIN WWI_PROC.PURCHASE_ORDER_HDR h
    ON h.PO_ID = pl.PO_ID
  LEFT OUTER JOIN WWI_MDM.PRODUCT_MASTER pm
    ON pm.PRODUCT_ID = pl.PRODUCT_ID
  LEFT OUTER JOIN WWI_PROC.VENDOR_CONTRACT_LINE cl
    ON cl.CONTRACT_ID = h.CONTRACT_ID
   AND cl.PRODUCT_ID  = pl.PRODUCT_ID
   AND NVL(h.ORDER_DT, TRUNC(SYSDATE)) BETWEEN cl.PRICE_EFFECTIVE_DT
                                           AND NVL(cl.PRICE_END_DT, DATE '4712-12-31')
 WHERE h.PO_STATUS_CD <> 'CANC'
/
