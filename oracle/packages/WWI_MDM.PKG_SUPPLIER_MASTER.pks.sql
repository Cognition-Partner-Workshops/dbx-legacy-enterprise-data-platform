/* ============================================================================
 * Object      : WWI_MDM.PKG_SUPPLIER_MASTER (package specification)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.SUPP_MASTER, WWI_MDM.SUPP_ADDRESS,
 *               WWI_MDM.SUPP_BANK_ACCOUNT, WWI_MDM.SUPP_CERTIFICATION
 * Called by   : the supplier onboarding form, the procurement portal
 *               interface, and WWI_PROC.PKG_PURCHASE_ORDER (approval check).
 * History     : 2003 created; 2008 bank verification workflow; 2016 APAC
 *               certification expiry rules bolted on for the audit.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_MDM.PKG_SUPPLIER_MASTER AS

    e_supplier_not_found  EXCEPTION;
    e_bank_unverified     EXCEPTION;
    e_cert_expired        EXCEPTION;
    e_supplier_blocked    EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_supplier_not_found, -20211);
    PRAGMA EXCEPTION_INIT(e_bank_unverified,    -20212);
    PRAGMA EXCEPTION_INIT(e_cert_expired,       -20213);
    PRAGMA EXCEPTION_INIT(e_supplier_blocked,   -20214);

    FUNCTION is_approved_for_po
    (
        p_supp_id   IN WWI_MDM.SUPP_MASTER.SUPP_ID%TYPE,
        p_region_cd IN VARCHAR2,
        p_amount    IN NUMBER DEFAULT 0
    ) RETURN VARCHAR2;

    FUNCTION certification_status
    (
        p_supp_id IN WWI_MDM.SUPP_MASTER.SUPP_ID%TYPE,
        p_cert_cd IN WWI_MDM.SUPP_CERTIFICATION.CERT_TYPE_CD%TYPE
    ) RETURN VARCHAR2;

    PROCEDURE onboard_supplier
    (
        p_supp_num    IN  WWI_MDM.SUPP_MASTER.SUPP_NBR%TYPE,
        p_supp_name   IN  WWI_MDM.SUPP_MASTER.SUPP_NAME%TYPE,
        p_region_cd   IN  WWI_MDM.SUPP_MASTER.REGION_CD%TYPE,
        p_country_cd  IN  WWI_MDM.SUPP_MASTER.COUNTRY_CD%TYPE,
        p_tax_reg_num IN  WWI_MDM.SUPP_MASTER.TAX_ID_NBR%TYPE,
        p_supp_id     OUT WWI_MDM.SUPP_MASTER.SUPP_ID%TYPE
    );

    PROCEDURE register_bank_account
    (
        p_supp_id      IN  WWI_MDM.SUPP_BANK_ACCOUNT.SUPP_ID%TYPE,
        p_bank_name    IN  WWI_MDM.SUPP_BANK_ACCOUNT.BANK_NAME%TYPE,
        p_acct_num     IN  VARCHAR2,
        p_iban         IN  VARCHAR2 DEFAULT NULL,
        p_swift_cd     IN  WWI_MDM.SUPP_BANK_ACCOUNT.BIC_CD%TYPE DEFAULT NULL,
        p_currency_cd  IN  WWI_MDM.SUPP_BANK_ACCOUNT.ACCOUNT_CURR_CD%TYPE,
        p_bank_acct_id OUT WWI_MDM.SUPP_BANK_ACCOUNT.SUPP_BANK_ID%TYPE
    );

    PROCEDURE block_supplier
    (
        p_supp_id    IN WWI_MDM.SUPP_MASTER.SUPP_ID%TYPE,
        p_reason_cd  IN WWI_MDM.SUPP_MASTER.HOLD_REASON_CD%TYPE,
        p_blocked_by IN VARCHAR2
    );

    PROCEDURE expire_certifications
    (
        p_expired_cnt OUT PLS_INTEGER
    );

END PKG_SUPPLIER_MASTER;
/
