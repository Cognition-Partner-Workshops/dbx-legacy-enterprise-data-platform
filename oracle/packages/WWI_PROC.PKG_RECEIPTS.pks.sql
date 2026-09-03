/* ============================================================================
 * Object      : WWI_PROC.PKG_RECEIPTS (package specification)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PO_RECEIPT_HDR, WWI_PROC.PO_RECEIPT_LINE,
 *               WWI_PROC.GOODS_RETURN_LINE, WWI_PROC.PURCHASE_ORDER_LINE
 * Called by   : the warehouse receiving screen, the EDI ASN loader
 *               (WWI_PROC.PRC_LOAD_ASN_INTERFACE) and WWI_FIN.PKG_AP_INVOICE
 *               (three-way match reads the accepted quantities).
 * History     : 2002 created; 2008 inspection workflow; 2012 returns.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_PROC.PKG_RECEIPTS AS

    e_receipt_not_found  EXCEPTION;
    e_over_receipt       EXCEPTION;
    e_line_not_open      EXCEPTION;
    e_return_exceeds     EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_receipt_not_found, -20311);
    PRAGMA EXCEPTION_INIT(e_over_receipt,      -20312);
    PRAGMA EXCEPTION_INIT(e_line_not_open,     -20313);
    PRAGMA EXCEPTION_INIT(e_return_exceeds,    -20314);

    FUNCTION over_receipt_tolerance_pct
    (
        p_region_cd IN VARCHAR2
    ) RETURN NUMBER;

    PROCEDURE create_receipt
    (
        p_po_id        IN  WWI_PROC.PO_RECEIPT_HDR.PO_ID%TYPE,
        p_warehouse_cd IN  WWI_PROC.PO_RECEIPT_HDR.WAREHOUSE_CD%TYPE,
        p_packing_slip IN  WWI_PROC.PO_RECEIPT_HDR.PACKING_SLIP_NUM%TYPE,
        p_received_by  IN  WWI_PROC.PO_RECEIPT_HDR.RECEIVED_BY%TYPE,
        p_receipt_id   OUT WWI_PROC.PO_RECEIPT_HDR.RECEIPT_ID%TYPE
    );

    PROCEDURE receive_line
    (
        p_receipt_id      IN  WWI_PROC.PO_RECEIPT_LINE.RECEIPT_ID%TYPE,
        p_po_line_id      IN  WWI_PROC.PO_RECEIPT_LINE.PO_LINE_ID%TYPE,
        p_received_qty    IN  WWI_PROC.PO_RECEIPT_LINE.RECEIVED_QTY%TYPE,
        p_uom_cd          IN  WWI_PROC.PO_RECEIPT_LINE.UOM_CD%TYPE,
        p_receipt_line_id OUT WWI_PROC.PO_RECEIPT_LINE.RECEIPT_LINE_ID%TYPE
    );

    PROCEDURE record_inspection
    (
        p_receipt_line_id IN WWI_PROC.PO_RECEIPT_LINE.RECEIPT_LINE_ID%TYPE,
        p_accepted_qty    IN WWI_PROC.PO_RECEIPT_LINE.ACCEPTED_QTY%TYPE,
        p_rejected_qty    IN WWI_PROC.PO_RECEIPT_LINE.REJECTED_QTY%TYPE,
        p_reject_reason   IN WWI_PROC.PO_RECEIPT_LINE.REJECT_REASON_CD%TYPE DEFAULT NULL,
        p_inspector       IN VARCHAR2
    );

    PROCEDURE return_goods
    (
        p_receipt_line_id IN  WWI_PROC.GOODS_RETURN_LINE.RECEIPT_LINE_ID%TYPE,
        p_return_qty      IN  WWI_PROC.GOODS_RETURN_LINE.RETURN_QTY%TYPE,
        p_reason_cd       IN  WWI_PROC.GOODS_RETURN_LINE.REASON_CD%TYPE,
        p_returned_by     IN  VARCHAR2,
        p_return_line_id  OUT WWI_PROC.GOODS_RETURN_LINE.RETURN_LINE_ID%TYPE
    );

END PKG_RECEIPTS;
/
