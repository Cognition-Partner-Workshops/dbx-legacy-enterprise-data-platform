/* ============================================================================
 * Object      : WWI_PROC.PKG_PURCHASE_ORDER (package specification)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PURCHASE_ORDER_HDR, WWI_PROC.PURCHASE_ORDER_LINE,
 *               WWI_PROC.PO_CHANGE_ORDER, WWI_PROC.VENDOR_CONTRACT_LINE,
 *               WWI_MDM.PKG_SUPPLIER_MASTER
 * Called by   : the buyer workbench, the requisition-to-PO batch job
 *               (WWI_PROC.PRC_RELEASE_REQUISITIONS) and the nightly close
 *               job WWI_PROC.PRC_CLOSE_STALE_PO.
 * History     : 2001 created; 2006 change orders; 2010 contract pricing;
 *               2017 APAC approval hierarchy hard-coded here after the
 *               workflow engine licence lapsed.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_PROC.PKG_PURCHASE_ORDER AS

    e_po_not_found      EXCEPTION;
    e_invalid_transition EXCEPTION;
    e_supplier_rejected EXCEPTION;
    e_line_closed       EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_po_not_found,       -20301);
    PRAGMA EXCEPTION_INIT(e_invalid_transition, -20302);
    PRAGMA EXCEPTION_INIT(e_supplier_rejected,  -20303);
    PRAGMA EXCEPTION_INIT(e_line_closed,        -20304);

    c_bulk_limit CONSTANT PLS_INTEGER := 500;

    FUNCTION approval_level
    (
        p_region_cd IN VARCHAR2,
        p_amount    IN NUMBER,
        p_currency_cd IN VARCHAR2 DEFAULT 'USD'
    ) RETURN VARCHAR2;

    FUNCTION contract_price
    (
        p_contract_id IN WWI_PROC.VENDOR_CONTRACT_LINE.CONTRACT_ID%TYPE,
        p_product_id  IN WWI_PROC.VENDOR_CONTRACT_LINE.PRODUCT_ID%TYPE,
        p_order_dt    IN DATE DEFAULT TRUNC(SYSDATE)
    ) RETURN NUMBER;

    PROCEDURE create_po
    (
        p_supp_id     IN  WWI_PROC.PURCHASE_ORDER_HDR.SUPP_ID%TYPE,
        p_region_cd   IN  WWI_PROC.PURCHASE_ORDER_HDR.REGION_CD%TYPE,
        p_currency_cd IN  WWI_PROC.PURCHASE_ORDER_HDR.ORDER_CURR_CD%TYPE,
        p_buyer_id    IN  WWI_PROC.PURCHASE_ORDER_HDR.BUYER_CD%TYPE,
        p_contract_id IN  WWI_PROC.PURCHASE_ORDER_HDR.CONTRACT_ID%TYPE DEFAULT NULL,
        p_po_id       OUT WWI_PROC.PURCHASE_ORDER_HDR.PO_ID%TYPE
    );

    PROCEDURE add_po_line
    (
        p_po_id       IN  WWI_PROC.PURCHASE_ORDER_LINE.PO_ID%TYPE,
        p_product_id  IN  WWI_PROC.PURCHASE_ORDER_LINE.PRODUCT_ID%TYPE,
        p_order_qty   IN  WWI_PROC.PURCHASE_ORDER_LINE.ORDER_QTY%TYPE,
        p_uom_cd      IN  WWI_PROC.PURCHASE_ORDER_LINE.UOM_CD%TYPE,
        p_unit_price  IN  WWI_PROC.PURCHASE_ORDER_LINE.UNIT_PRICE%TYPE DEFAULT NULL,
        p_need_by_dt  IN  DATE DEFAULT NULL,
        p_po_line_id  OUT WWI_PROC.PURCHASE_ORDER_LINE.PO_LINE_ID%TYPE
    );

    PROCEDURE approve_po
    (
        p_po_id       IN WWI_PROC.PURCHASE_ORDER_HDR.PO_ID%TYPE,
        p_approved_by IN WWI_PROC.PURCHASE_ORDER_HDR.UPDATED_BY%TYPE
    );

    PROCEDURE apply_change_order
    (
        p_po_line_id  IN WWI_PROC.PURCHASE_ORDER_LINE.PO_LINE_ID%TYPE,
        p_new_qty     IN WWI_PROC.PURCHASE_ORDER_LINE.ORDER_QTY%TYPE,
        p_new_price   IN WWI_PROC.PURCHASE_ORDER_LINE.UNIT_PRICE%TYPE,
        p_reason_cd   IN WWI_PROC.PO_CHANGE_ORDER.CHANGE_REASON_CD%TYPE,
        p_changed_by  IN VARCHAR2
    );

    PROCEDURE close_stale_pos
    (
        p_region_cd  IN  VARCHAR2,
        p_stale_days IN  PLS_INTEGER DEFAULT 180,
        p_closed_cnt OUT PLS_INTEGER
    );

END PKG_PURCHASE_ORDER;
/
