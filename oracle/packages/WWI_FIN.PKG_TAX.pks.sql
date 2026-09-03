/* ============================================================================
 * Object      : WWI_FIN.PKG_TAX (package specification)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.TAX_RATE, WWI_FIN.TAX_JURISDICTION, WWI_MDM.SUPP_MASTER,
 *               WWI_REF.COUNTRY_REF
 * Called by   : WWI_FIN.PKG_AP_INVOICE, WWI_PROC.PKG_PURCHASE_ORDER,
 *               WWI_FIN.FN_TAX_AMOUNT (jurisdiction resolution)
 * History     : 2001 US sales tax only; 2004 EU VAT; 2007 reverse charge;
 *               2013 APAC GST. Each regime was added by a different team and
 *               the three code paths never got unified.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_FIN.PKG_TAX AS

    e_rate_not_found       EXCEPTION;
    e_jurisdiction_unknown EXCEPTION;
    e_vat_id_invalid       EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_rate_not_found,       -20131);
    PRAGMA EXCEPTION_INIT(e_jurisdiction_unknown, -20132);
    PRAGMA EXCEPTION_INIT(e_vat_id_invalid,       -20133);

    TYPE t_tax_line IS RECORD (
        tax_cd          WWI_FIN.TAX_RATE.TAX_CODE_CD%TYPE,
        jurisdiction_id WWI_FIN.TAX_JURISDICTION.TAX_JURISDICTION_ID%TYPE,
        rate_pct        NUMBER,
        taxable_amt     NUMBER,
        tax_amt         NUMBER,
        recoverable_amt NUMBER,
        regime_cd       VARCHAR2(10)
    );

    TYPE t_tax_line_tab IS TABLE OF t_tax_line INDEX BY PLS_INTEGER;

    FUNCTION resolve_jurisdiction
    (
        p_region_cd  IN VARCHAR2,
        p_country_cd IN WWI_REF.COUNTRY_REF.COUNTRY_CD%TYPE,
        p_state_cd   IN VARCHAR2 DEFAULT NULL,
        p_postal_cd  IN VARCHAR2 DEFAULT NULL
    ) RETURN WWI_FIN.TAX_JURISDICTION.TAX_JURISDICTION_ID%TYPE;

    FUNCTION resolve_rate
    (
        p_tax_cd          IN WWI_FIN.TAX_RATE.TAX_CODE_CD%TYPE,
        p_jurisdiction_id IN WWI_FIN.TAX_JURISDICTION.TAX_JURISDICTION_ID%TYPE,
        p_tax_dt          IN DATE DEFAULT SYSDATE
    ) RETURN NUMBER;

    FUNCTION is_reverse_charge
    (
        p_supp_id     IN WWI_MDM.SUPP_MASTER.SUPP_ID%TYPE,
        p_buyer_ctry  IN WWI_REF.COUNTRY_REF.COUNTRY_CD%TYPE,
        p_service_flg IN VARCHAR2 DEFAULT 'N'
    ) RETURN VARCHAR2;

    FUNCTION validate_vat_id
    (
        p_vat_id     IN VARCHAR2,
        p_country_cd IN WWI_REF.COUNTRY_REF.COUNTRY_CD%TYPE
    ) RETURN VARCHAR2;

    PROCEDURE determine_tax
    (
        p_region_cd       IN  VARCHAR2,
        p_line_amt        IN  NUMBER,
        p_tax_cd          IN  WWI_FIN.TAX_RATE.TAX_CODE_CD%TYPE,
        p_jurisdiction_id IN  WWI_FIN.TAX_JURISDICTION.TAX_JURISDICTION_ID%TYPE,
        p_tax_dt          IN  DATE,
        p_reverse_charge  IN  VARCHAR2,
        p_tax_lines       OUT t_tax_line_tab,
        p_total_tax_amt   OUT NUMBER
    );

END PKG_TAX;
/
