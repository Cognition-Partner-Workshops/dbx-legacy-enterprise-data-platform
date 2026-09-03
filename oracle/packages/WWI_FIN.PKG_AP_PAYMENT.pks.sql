/* ============================================================================
 * Object      : WWI_FIN.PKG_AP_PAYMENT (package specification)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.AP_PAYMENT, WWI_FIN.AP_PAYMENT_APPLY,
 *               WWI_FIN.AP_INVOICE_HDR, WWI_FIN.WITHHOLDING_RULE,
 *               WWI_MDM.SUPP_BANK_ACCOUNT
 * Called by   : WWI_FIN.PRC_RUN_PAYMENT_PROPOSAL, the treasury payment form,
 *               and WWI_FIN.PRC_RUN_NIGHTLY_AP
 * History     : 2000 created; 2006 withholding; 2010 SEPA support for EU;
 *               2015 APAC per-bank cut-off times bolted on.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_FIN.PKG_AP_PAYMENT AS

    e_payment_not_found  EXCEPTION;
    e_overapplication    EXCEPTION;
    e_no_bank_account    EXCEPTION;
    e_payment_voided     EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_payment_not_found, -20111);
    PRAGMA EXCEPTION_INIT(e_overapplication,   -20112);
    PRAGMA EXCEPTION_INIT(e_no_bank_account,   -20113);
    PRAGMA EXCEPTION_INIT(e_payment_voided,    -20114);

    c_bulk_limit CONSTANT PLS_INTEGER := 250;

    FUNCTION discount_available
    (
        p_invoice_id IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_pay_dt     IN DATE DEFAULT SYSDATE
    ) RETURN NUMBER;

    FUNCTION withholding_amount
    (
        p_supp_id     IN WWI_FIN.AP_INVOICE_HDR.SUPP_ID%TYPE,
        p_region_cd   IN VARCHAR2,
        p_gross_amt   IN NUMBER,
        p_income_type IN VARCHAR2 DEFAULT 'SERVICES'
    ) RETURN NUMBER;

    PROCEDURE build_payment_proposal
    (
        p_region_cd    IN  VARCHAR2,
        p_pay_thru_dt  IN  DATE,
        p_run_id       OUT WWI_FIN.AP_PAYMENT.PAYMENT_RUN_ID%TYPE,
        p_selected_cnt OUT PLS_INTEGER,
        p_total_amt    OUT NUMBER
    );

    PROCEDURE apply_payment
    (
        p_payment_id IN WWI_FIN.AP_PAYMENT.PAYMENT_ID%TYPE,
        p_invoice_id IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_amount     IN NUMBER
    );

    PROCEDURE auto_apply_payment
    (
        p_payment_id  IN  WWI_FIN.AP_PAYMENT.PAYMENT_ID%TYPE,
        p_applied_cnt OUT PLS_INTEGER,
        p_residual    OUT NUMBER
    );

    PROCEDURE void_payment
    (
        p_payment_id  IN WWI_FIN.AP_PAYMENT.PAYMENT_ID%TYPE,
        p_reason_cd   IN WWI_FIN.AP_PAYMENT.VOID_REASON_CD%TYPE,
        p_voided_by   IN WWI_FIN.AP_PAYMENT.LAST_UPD_BY%TYPE
    );

END PKG_AP_PAYMENT;
/
