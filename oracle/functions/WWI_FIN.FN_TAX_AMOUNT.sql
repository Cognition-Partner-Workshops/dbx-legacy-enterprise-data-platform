/* ============================================================================
 * Object      : WWI_FIN.FN_TAX_AMOUNT (function)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.TAX_RATE, WWI_FIN.TAX_JURISDICTION,
 *               WWI_REF.COUNTRY_REF
 * Called by   : WWI_FIN.PKG_TAX, WWI_FIN.PKG_AP_INVOICE,
 *               WWI_PROC.PKG_PURCHASE_ORDER, WWI_FIN.V_AP_INVOICE_EXTRACT
 * Errors      : -20041 unknown tax regime for region
 * History     : 1996 NA sales tax; 2003 EU VAT; 2007 EU reverse charge;
 *               2010 APAC GST (tax-inclusive pricing).
 * Notes       : Three regimes, one function, because in 2003 nobody wanted to
 *               change the twelve call sites. NA sums the jurisdiction stack
 *               (state + county + city), EU takes a single rate and honours the
 *               reverse charge, APAC treats the line amount as tax inclusive.
 * ========================================================================= */

CREATE OR REPLACE FUNCTION WWI_FIN.FN_TAX_AMOUNT
(
    p_line_amt            IN NUMBER,
    p_region_cd           IN VARCHAR2,
    p_tax_cd              IN WWI_FIN.TAX_RATE.TAX_CD%TYPE,
    p_jurisdiction_id     IN WWI_FIN.TAX_JURISDICTION.JURISDICTION_ID%TYPE DEFAULT NULL,
    p_tax_dt              IN DATE DEFAULT SYSDATE,
    p_reverse_charge_flag IN VARCHAR2 DEFAULT 'N'
)
RETURN NUMBER
IS
    l_rate_pct   WWI_FIN.TAX_RATE.RATE_PCT%TYPE;
    l_total_pct  NUMBER := 0;
    l_tax_amt    NUMBER := 0;
BEGIN
    IF p_line_amt IS NULL OR p_line_amt = 0 THEN
        RETURN 0;
    END IF;

    CASE UPPER(p_region_cd)

        WHEN 'NA' THEN
            /* Sum every level that covers the jurisdiction. The CONNECT BY walks
               city -> county -> state -> country; rates are additive. */
            SELECT NVL(SUM(tr.RATE_PCT), 0)
              INTO l_total_pct
              FROM WWI_FIN.TAX_JURISDICTION j
              JOIN WWI_FIN.TAX_RATE tr
                ON tr.JURISDICTION_ID = j.JURISDICTION_ID
               AND tr.TAX_TYPE_CD     = 'SALES'
               AND tr.TAX_CD          = p_tax_cd
               AND TRUNC(p_tax_dt) BETWEEN tr.EFF_FROM_DT AND NVL(tr.EFF_TO_DT, DATE '4712-12-31')
             WHERE j.JURISDICTION_ID IN (
                       SELECT jj.JURISDICTION_ID
                         FROM WWI_FIN.TAX_JURISDICTION jj
                        START WITH jj.JURISDICTION_ID = p_jurisdiction_id
                      CONNECT BY PRIOR jj.COUNTRY_CD = jj.COUNTRY_CD
                              AND PRIOR jj.LEVEL_CD <> jj.LEVEL_CD
                              AND LEVEL <= 4);
            l_tax_amt := ROUND(p_line_amt * l_total_pct / 100, 2);

        WHEN 'EU' THEN
            IF NVL(p_reverse_charge_flag, 'N') = 'Y' THEN
                /* Article 196 supplies: the buyer self-accounts, the payable is
                   zero but the notional rate is still resolved upstream. */
                RETURN 0;
            END IF;
            BEGIN
                SELECT tr.RATE_PCT
                  INTO l_rate_pct
                  FROM WWI_FIN.TAX_RATE tr
                 WHERE tr.TAX_CD      = p_tax_cd
                   AND tr.TAX_TYPE_CD = 'VAT'
                   AND tr.REGION_CD   = 'EU'
                   AND TRUNC(p_tax_dt) BETWEEN tr.EFF_FROM_DT
                                           AND NVL(tr.EFF_TO_DT, DATE '4712-12-31')
                   AND ROWNUM = 1;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    l_rate_pct := NULL;
            END;
            IF l_rate_pct IS NULL THEN
                RAISE_APPLICATION_ERROR(-20041,
                    'FN_TAX_AMOUNT: no EU VAT rate for code ' || p_tax_cd
                    || ' on ' || TO_CHAR(p_tax_dt, 'YYYY-MM-DD'));
            END IF;
            l_tax_amt := ROUND(p_line_amt * l_rate_pct / 100, 2);

        WHEN 'APAC' THEN
            /* GST territories quote tax-inclusive prices; the ERP stores the
               gross amount on the line and backs the tax out of it. */
            BEGIN
                SELECT tr.RATE_PCT
                  INTO l_rate_pct
                  FROM WWI_FIN.TAX_RATE tr
                 WHERE tr.TAX_CD      = p_tax_cd
                   AND tr.TAX_TYPE_CD = 'GST'
                   AND tr.REGION_CD   = 'APAC'
                   AND TRUNC(p_tax_dt) BETWEEN tr.EFF_FROM_DT
                                           AND NVL(tr.EFF_TO_DT, DATE '4712-12-31')
                   AND ROWNUM = 1;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    l_rate_pct := 0;
            END;
            l_tax_amt := ROUND(p_line_amt - (p_line_amt / (1 + NVL(l_rate_pct, 0) / 100)), 2);

        ELSE
            RAISE_APPLICATION_ERROR(-20041,
                'FN_TAX_AMOUNT: unknown tax regime for region ' || p_region_cd);
    END CASE;

    RETURN l_tax_amt;
END FN_TAX_AMOUNT;
/
