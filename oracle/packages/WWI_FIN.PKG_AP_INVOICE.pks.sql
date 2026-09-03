/* ============================================================================
 * Object      : WWI_FIN.PKG_AP_INVOICE (package specification)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.AP_INVOICE_HDR, WWI_FIN.AP_INVOICE_LINE,
 *               WWI_FIN.AP_INVOICE_HOLD, WWI_PROC.PURCHASE_ORDER_LINE,
 *               WWI_PROC.PO_RECEIPT_LINE, WWI_FIN.PKG_TAX, WWI_FIN.PKG_GL_POSTING
 * Called by   : AP clerk forms, the supplier invoice interface loader
 *               (WWI_FIN.PRC_LOAD_INVOICE_INTERFACE) and the nightly job
 *               WWI_FIN.PRC_RUN_NIGHTLY_AP.
 * History     : 1999 created; 2004 three-way match; 2008 EU reverse charge;
 *               2012 tolerance bands moved out to WWI_FIN.MATCH_TOLERANCE
 *               (still overridden here for APAC - see match_line).
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_FIN.PKG_AP_INVOICE AS

    /* invoice status codes: EN entered, VA validated, HO on hold,
       AP approved, PP part paid, PD paid, CN cancelled                     */

    e_invoice_not_found     EXCEPTION;
    e_period_closed         EXCEPTION;
    e_duplicate_invoice     EXCEPTION;
    e_match_failed          EXCEPTION;
    e_invalid_status        EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_invoice_not_found, -20101);
    PRAGMA EXCEPTION_INIT(e_period_closed,     -20102);
    PRAGMA EXCEPTION_INIT(e_duplicate_invoice, -20103);
    PRAGMA EXCEPTION_INIT(e_match_failed,      -20104);
    PRAGMA EXCEPTION_INIT(e_invalid_status,    -20105);

    c_bulk_limit    CONSTANT PLS_INTEGER := 500;

    TYPE t_match_result IS RECORD (
        po_line_id      WWI_PROC.PURCHASE_ORDER_LINE.PO_LINE_ID%TYPE,
        qty_variance    NUMBER,
        price_variance  NUMBER,
        amount_variance NUMBER,
        match_status_cd VARCHAR2(10),
        hold_cd         VARCHAR2(20)
    );

    FUNCTION is_duplicate
    (
        p_supp_id     IN WWI_FIN.AP_INVOICE_HDR.SUPP_ID%TYPE,
        p_invoice_num IN WWI_FIN.AP_INVOICE_HDR.INVOICE_NBR%TYPE,
        p_invoice_id  IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE DEFAULT NULL
    ) RETURN BOOLEAN;

    FUNCTION match_line
    (
        p_invoice_line_id IN WWI_FIN.AP_INVOICE_LINE.INVOICE_LINE_ID%TYPE,
        p_match_type_cd   IN VARCHAR2 DEFAULT '3WAY'
    ) RETURN t_match_result;

    PROCEDURE validate_invoice
    (
        p_invoice_id  IN  WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_hold_count  OUT PLS_INTEGER,
        p_status_cd   OUT WWI_FIN.AP_INVOICE_HDR.INVOICE_STATUS_CD%TYPE
    );

    PROCEDURE apply_hold
    (
        p_invoice_id IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_hold_cd    IN WWI_FIN.AP_INVOICE_HOLD.HOLD_CODE_CD%TYPE,
        p_hold_desc  IN WWI_FIN.AP_INVOICE_HOLD.HOLD_REASON_TXT%TYPE DEFAULT NULL
    );

    PROCEDURE release_hold
    (
        p_invoice_id  IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_hold_cd     IN WWI_FIN.AP_INVOICE_HOLD.HOLD_CODE_CD%TYPE,
        p_released_by IN WWI_FIN.AP_INVOICE_HOLD.RELEASED_BY_CD%TYPE
    );

    PROCEDURE approve_invoice
    (
        p_invoice_id  IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_approved_by IN WWI_FIN.AP_INVOICE_HDR.UPDATED_BY%TYPE
    );

    PROCEDURE cancel_invoice
    (
        p_invoice_id   IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_reason_cd    IN VARCHAR2,
        p_cancelled_by IN WWI_FIN.AP_INVOICE_HDR.UPDATED_BY%TYPE
    );

    PROCEDURE validate_batch
    (
        p_region_cd     IN  VARCHAR2 DEFAULT NULL,
        p_max_rows      IN  PLS_INTEGER DEFAULT 100000,
        p_validated_cnt OUT PLS_INTEGER,
        p_held_cnt      OUT PLS_INTEGER
    );

END PKG_AP_INVOICE;
/
