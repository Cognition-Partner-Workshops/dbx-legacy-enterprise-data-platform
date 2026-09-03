/* ============================================================================
 * Object      : WWI_PROC.FN_PO_OPEN_QTY (function)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PURCHASE_ORDER_LINE, WWI_PROC.PO_RECEIPT_LINE,
 *               WWI_PROC.GOODS_RETURN_LINE
 * Called by   : WWI_PROC.V_OPEN_PO_BALANCE, WWI_PROC.PKG_PURCHASE_ORDER,
 *               WWI_PROC.PRC_CLOSE_STALE_PO, WWI_FIN.PKG_AP_INVOICE
 * History     : 1999 original; 2006 returns netted back into open quantity;
 *               2014 accepted-quantity basis replaced received quantity.
 * Notes       : Returns the quantity still expected on a purchase order line.
 *               Returned goods re-open the line, which is why a closed line can
 *               show a positive open quantity until the nightly job runs.
 * ========================================================================= */

CREATE OR REPLACE FUNCTION WWI_PROC.FN_PO_OPEN_QTY
(
    p_po_line_id      IN WWI_PROC.PURCHASE_ORDER_LINE.PO_LINE_ID%TYPE,
    p_include_returns IN VARCHAR2 DEFAULT 'Y'
)
RETURN NUMBER
IS
    l_order_qty     WWI_PROC.PURCHASE_ORDER_LINE.ORDER_QTY%TYPE;
    l_cancelled_qty WWI_PROC.PURCHASE_ORDER_LINE.CANCELLED_QTY%TYPE;
    l_closed_flag   WWI_PROC.PURCHASE_ORDER_LINE.CLOSED_FLAG%TYPE;
    l_accepted_qty  NUMBER := 0;
    l_returned_qty  NUMBER := 0;
    l_open_qty      NUMBER;
BEGIN
    SELECT pl.ORDER_QTY, NVL(pl.CANCELLED_QTY, 0), NVL(pl.CLOSED_FLAG, 'N')
      INTO l_order_qty, l_cancelled_qty, l_closed_flag
      FROM WWI_PROC.PURCHASE_ORDER_LINE pl
     WHERE pl.PO_LINE_ID = p_po_line_id;

    SELECT NVL(SUM(NVL(rl.ACCEPTED_QTY, rl.RECEIVED_QTY)), 0)
      INTO l_accepted_qty
      FROM WWI_PROC.PO_RECEIPT_LINE rl
     WHERE rl.PO_LINE_ID = p_po_line_id
       AND NVL(rl.INSPECTION_STATUS_CD, 'ACC') <> 'REJ';

    IF NVL(p_include_returns, 'Y') = 'Y' THEN
        SELECT NVL(SUM(gl.RETURN_QTY), 0)
          INTO l_returned_qty
          FROM WWI_PROC.GOODS_RETURN_LINE gl
          JOIN WWI_PROC.PO_RECEIPT_LINE rl
            ON rl.RECEIPT_LINE_ID = gl.RECEIPT_LINE_ID
         WHERE rl.PO_LINE_ID = p_po_line_id;
    END IF;

    l_open_qty := l_order_qty - l_cancelled_qty - l_accepted_qty + l_returned_qty;

    IF l_closed_flag = 'Y' AND l_returned_qty = 0 THEN
        RETURN 0;
    END IF;

    RETURN GREATEST(NVL(l_open_qty, 0), 0);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
END FN_PO_OPEN_QTY;
/
