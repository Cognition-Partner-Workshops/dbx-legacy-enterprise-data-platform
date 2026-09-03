/* ============================================================================
 * Object      : WWI_PROC.FN_RECEIPT_VARIANCE_PCT (function)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PO_RECEIPT_LINE, WWI_PROC.PURCHASE_ORDER_LINE,
 *               WWI_MDM.PRODUCT_UOM_CONV
 * Called by   : WWI_PROC.PKG_RECEIPTS, WWI_PROC.PKG_SUPPLIER_PERF,
 *               WWI_PROC.V_RECEIPT_EXTRACT, WWI_FIN.PKG_AP_INVOICE
 * History     : 2000 original; 2009 UOM conversion added after the APAC
 *               warehouses started receiving in cases against each-based lines.
 * Notes       : Positive percentage means over-receipt. Tolerance evaluation
 *               lives in PKG_RECEIPTS; this function only measures.
 * ========================================================================= */

CREATE OR REPLACE FUNCTION WWI_PROC.FN_RECEIPT_VARIANCE_PCT
(
    p_receipt_line_id IN WWI_PROC.PO_RECEIPT_LINE.RECEIPT_LINE_ID%TYPE
)
RETURN NUMBER
IS
    l_received_qty  WWI_PROC.PO_RECEIPT_LINE.RECEIVED_QTY%TYPE;
    l_receipt_uom   WWI_PROC.PO_RECEIPT_LINE.UOM_CD%TYPE;
    l_product_id    WWI_PROC.PO_RECEIPT_LINE.PRODUCT_ID%TYPE;
    l_order_qty     WWI_PROC.PURCHASE_ORDER_LINE.ORDER_QTY%TYPE;
    l_order_uom     WWI_PROC.PURCHASE_ORDER_LINE.UOM_CD%TYPE;
    l_factor        WWI_MDM.PRODUCT_UOM_CONV.CONV_FACTOR%TYPE := 1;
BEGIN
    SELECT rl.RECEIVED_QTY, rl.UOM_CD, rl.PRODUCT_ID, pl.ORDER_QTY, pl.UOM_CD
      INTO l_received_qty, l_receipt_uom, l_product_id, l_order_qty, l_order_uom
      FROM WWI_PROC.PO_RECEIPT_LINE rl
      JOIN WWI_PROC.PURCHASE_ORDER_LINE pl
        ON pl.PO_LINE_ID = rl.PO_LINE_ID
     WHERE rl.RECEIPT_LINE_ID = p_receipt_line_id;

    IF NVL(l_order_qty, 0) = 0 THEN
        RETURN NULL;
    END IF;

    IF l_receipt_uom <> l_order_uom THEN
        BEGIN
            SELECT c.CONV_FACTOR
              INTO l_factor
              FROM WWI_MDM.PRODUCT_UOM_CONV c
             WHERE c.PRODUCT_ID  = l_product_id
               AND c.FROM_UOM_CD = l_receipt_uom
               AND c.TO_UOM_CD   = l_order_uom
               AND SYSDATE BETWEEN c.EFFECTIVE_DT
                               AND NVL(c.END_DT, DATE '4712-12-31')
               AND c.EFFECTIVE_DT = (SELECT MAX(c2.EFFECTIVE_DT)
                                       FROM WWI_MDM.PRODUCT_UOM_CONV c2
                                      WHERE c2.PRODUCT_ID  = c.PRODUCT_ID
                                        AND c2.FROM_UOM_CD = c.FROM_UOM_CD
                                        AND c2.TO_UOM_CD   = c.TO_UOM_CD
                                        AND c2.EFFECTIVE_DT <= SYSDATE);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                /* 2009 note: an unconvertible receipt is reported as a 999%
                   variance so that it lands on the exception report. */
                RETURN 999;
        END;
    END IF;

    RETURN ROUND(((l_received_qty * NVL(l_factor, 1)) - l_order_qty) / l_order_qty * 100, 4);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END FN_RECEIPT_VARIANCE_PCT;
/
