/* ============================================================================
 * Object      : WWI_FIN.PKG_TAX (package body)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_FIN.PKG_TAX, WWI_FIN.TAX_RATE,
 *               WWI_FIN.TAX_JURISDICTION, WWI_MDM.SUPP_MASTER,
 *               WWI_REF.COUNTRY_REF, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_FIN.PKG_TAX AS

    FUNCTION resolve_jurisdiction
    (
        p_region_cd  IN VARCHAR2,
        p_country_cd IN WWI_REF.COUNTRY_REF.COUNTRY_CD%TYPE,
        p_state_cd   IN VARCHAR2 DEFAULT NULL,
        p_postal_cd  IN VARCHAR2 DEFAULT NULL
    ) RETURN WWI_FIN.TAX_JURISDICTION.TAX_JURISDICTION_ID%TYPE
    IS
        l_id WWI_FIN.TAX_JURISDICTION.TAX_JURISDICTION_ID%TYPE;
    BEGIN
        IF p_region_cd = 'NA' THEN
            /* NA resolves to the most specific jurisdiction that matches the
               ZIP prefix, falling back to state, then country. The ZIP table
               is loaded from a vendor file that stopped being refreshed in
               2019 - see the note in the AP runbook.                        */
            BEGIN
                SELECT TAX_JURISDICTION_ID
                  INTO l_id
                  FROM (SELECT j.TAX_JURISDICTION_ID
                          FROM WWI_FIN.TAX_JURISDICTION j
                         WHERE j.COUNTRY_CD = p_country_cd
                           AND (j.STATE_PROV_CD  = p_state_cd OR j.STATE_PROV_CD IS NULL)
                           AND (p_postal_cd IS NULL
                                OR j.POSTAL_FROM_CD IS NULL
                                OR p_postal_cd LIKE j.POSTAL_FROM_CD || '%')
                         ORDER BY NVL(LENGTH(j.POSTAL_FROM_CD), 0) DESC,
                                  NVL2(j.STATE_PROV_CD, 0, 1))
                 WHERE ROWNUM = 1;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RAISE_APPLICATION_ERROR(-20132,
                        'PKG_TAX.resolve_jurisdiction: no NA jurisdiction for '
                        || p_country_cd || '/' || p_state_cd);
            END;
        ELSE
            /* EU and APAC are country-level only */
            BEGIN
                SELECT j.TAX_JURISDICTION_ID
                  INTO l_id
                  FROM WWI_FIN.TAX_JURISDICTION j
                 WHERE j.COUNTRY_CD = p_country_cd
                   AND j.STATE_PROV_CD IS NULL
                   AND ROWNUM = 1;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RAISE_APPLICATION_ERROR(-20132,
                        'PKG_TAX.resolve_jurisdiction: no jurisdiction for country '
                        || p_country_cd);
            END;
        END IF;

        RETURN l_id;
    END resolve_jurisdiction;

    FUNCTION resolve_rate
    (
        p_tax_cd          IN WWI_FIN.TAX_RATE.TAX_CODE_CD%TYPE,
        p_jurisdiction_id IN WWI_FIN.TAX_JURISDICTION.TAX_JURISDICTION_ID%TYPE,
        p_tax_dt          IN DATE DEFAULT SYSDATE
    ) RETURN NUMBER
    IS
        l_rate NUMBER;
    BEGIN
        SELECT r.RATE_PCT
          INTO l_rate
          FROM WWI_FIN.TAX_RATE r
         WHERE r.TAX_CODE_CD = p_tax_cd
           AND NVL(r.JURISDICTION_CD, NVL(p_jurisdiction_id, -1))
               = NVL(p_jurisdiction_id, -1)
           AND TRUNC(p_tax_dt) BETWEEN r.EFFECTIVE_FROM_DT
                                   AND NVL(r.EFFECTIVE_TO_DT, DATE '4712-12-31')
           AND ROWNUM = 1;

        RETURN l_rate;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20131,
                'PKG_TAX.resolve_rate: no rate for ' || p_tax_cd
                || ' in jurisdiction ' || p_jurisdiction_id
                || ' on ' || TO_CHAR(p_tax_dt, 'YYYY-MM-DD'));
    END resolve_rate;

    FUNCTION is_reverse_charge
    (
        p_supp_id     IN WWI_MDM.SUPP_MASTER.SUPP_ID%TYPE,
        p_buyer_ctry  IN WWI_REF.COUNTRY_REF.COUNTRY_CD%TYPE,
        p_service_flg IN VARCHAR2 DEFAULT 'N'
    ) RETURN VARCHAR2
    IS
        l_supp_ctry  WWI_MDM.SUPP_MASTER.COUNTRY_CD%TYPE;
        l_supp_vat   WWI_MDM.SUPP_MASTER.TAX_ID_NBR%TYPE;
        l_supp_eu    WWI_REF.COUNTRY_REF.EU_MEMBER_FLG%TYPE;
        l_buyer_eu   WWI_REF.COUNTRY_REF.EU_MEMBER_FLG%TYPE;
    BEGIN
        SELECT s.COUNTRY_CD, s.TAX_ID_NBR, NVL(c.EU_MEMBER_FLG, 'N')
          INTO l_supp_ctry, l_supp_vat, l_supp_eu
          FROM WWI_MDM.SUPP_MASTER s
          LEFT OUTER JOIN WWI_REF.COUNTRY_REF c
            ON c.COUNTRY_CD = s.COUNTRY_CD
         WHERE s.SUPP_ID = p_supp_id;

        SELECT NVL(EU_MEMBER_FLG, 'N')
          INTO l_buyer_eu
          FROM WWI_REF.COUNTRY_REF
         WHERE COUNTRY_CD = p_buyer_ctry;

        IF l_buyer_eu = 'Y'
           AND l_supp_eu = 'Y'
           AND l_supp_ctry <> p_buyer_ctry
           AND l_supp_vat IS NOT NULL THEN
            RETURN 'Y';
        END IF;

        /* imported services from outside the EU are also reverse charged */
        IF l_buyer_eu = 'Y' AND l_supp_eu = 'N' AND NVL(p_service_flg, 'N') = 'Y' THEN
            RETURN 'Y';
        END IF;

        RETURN 'N';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 'N';
    END is_reverse_charge;

    FUNCTION validate_vat_id
    (
        p_vat_id     IN VARCHAR2,
        p_country_cd IN WWI_REF.COUNTRY_REF.COUNTRY_CD%TYPE
    ) RETURN VARCHAR2
    IS
        l_clean VARCHAR2(30);
    BEGIN
        l_clean := UPPER(REGEXP_REPLACE(NVL(p_vat_id, ' '), '[^A-Za-z0-9]', ''));

        IF l_clean IS NULL THEN
            RETURN 'N';
        END IF;

        /* format-only checks, copied off the 2004 VIES specification sheet.
           No online validation is performed from the ERP.                  */
        IF p_country_cd = 'DE' THEN
            RETURN CASE WHEN REGEXP_LIKE(l_clean, '^DE[0-9]{9}$') THEN 'Y' ELSE 'N' END;
        ELSIF p_country_cd = 'FR' THEN
            RETURN CASE WHEN REGEXP_LIKE(l_clean, '^FR[0-9A-Z]{2}[0-9]{9}$') THEN 'Y' ELSE 'N' END;
        ELSIF p_country_cd = 'GB' THEN
            RETURN CASE WHEN REGEXP_LIKE(l_clean, '^GB[0-9]{9}([0-9]{3})?$') THEN 'Y' ELSE 'N' END;
        ELSIF p_country_cd = 'NL' THEN
            RETURN CASE WHEN REGEXP_LIKE(l_clean, '^NL[0-9]{9}B[0-9]{2}$') THEN 'Y' ELSE 'N' END;
        END IF;

        RETURN 'U';   /* unknown format - not checked */
    END validate_vat_id;

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
    )
    IS
        l_idx  PLS_INTEGER := 0;
        l_rate NUMBER;
        l_line t_tax_line;
    BEGIN
        p_total_tax_amt := 0;

        IF p_region_cd = 'NA' THEN
            /* sales tax is additive across state, county and city rows in
               the jurisdiction table; each contributes its own tax line     */
            FOR j IN (SELECT j.TAX_JURISDICTION_ID, j.JURISDICTION_LEVEL_CD
                        FROM WWI_FIN.TAX_JURISDICTION j
                       START WITH j.TAX_JURISDICTION_ID = p_jurisdiction_id
                     CONNECT BY PRIOR j.PARENT_JURISDICTION_CD = j.TAX_JURISDICTION_ID) LOOP

                BEGIN
                    l_rate := resolve_rate(p_tax_cd, j.TAX_JURISDICTION_ID, p_tax_dt);
                EXCEPTION
                    WHEN OTHERS THEN
                        l_rate := 0;
                END;

                IF l_rate <> 0 THEN
                    l_idx := l_idx + 1;
                    l_line.tax_cd          := p_tax_cd;
                    l_line.jurisdiction_id := j.TAX_JURISDICTION_ID;
                    l_line.rate_pct        := l_rate;
                    l_line.taxable_amt     := p_line_amt;
                    l_line.tax_amt         := ROUND(p_line_amt * l_rate / 100, 2);
                    l_line.recoverable_amt := 0;   /* US sales tax is a cost */
                    l_line.regime_cd       := 'SALESTAX';
                    p_tax_lines(l_idx)     := l_line;
                    p_total_tax_amt        := p_total_tax_amt + l_line.tax_amt;
                END IF;
            END LOOP;

        ELSIF p_region_cd = 'EU' THEN
            l_rate := resolve_rate(p_tax_cd, p_jurisdiction_id, p_tax_dt);
            l_idx  := 1;

            l_line.tax_cd          := p_tax_cd;
            l_line.jurisdiction_id := p_jurisdiction_id;
            l_line.rate_pct        := l_rate;
            l_line.taxable_amt     := p_line_amt;
            l_line.tax_amt         := CASE WHEN NVL(p_reverse_charge, 'N') = 'Y'
                                           THEN 0
                                           ELSE ROUND(p_line_amt * l_rate / 100, 2)
                                      END;
            l_line.recoverable_amt := ROUND(p_line_amt * l_rate / 100, 2);
            l_line.regime_cd       := CASE WHEN NVL(p_reverse_charge, 'N') = 'Y'
                                           THEN 'VAT_RC' ELSE 'VAT' END;
            p_tax_lines(l_idx)     := l_line;
            p_total_tax_amt        := l_line.tax_amt;

        ELSIF p_region_cd = 'APAC' THEN
            /* APAC prices are tax inclusive, so the tax is extracted from the
               line amount rather than added to it                           */
            l_rate := resolve_rate(p_tax_cd, p_jurisdiction_id, p_tax_dt);
            l_idx  := 1;

            l_line.tax_cd          := p_tax_cd;
            l_line.jurisdiction_id := p_jurisdiction_id;
            l_line.rate_pct        := l_rate;
            l_line.taxable_amt     := ROUND(p_line_amt / (1 + l_rate / 100), 2);
            l_line.tax_amt         := ROUND(p_line_amt - p_line_amt / (1 + l_rate / 100), 2);
            l_line.recoverable_amt := l_line.tax_amt;
            l_line.regime_cd       := 'GST';
            p_tax_lines(l_idx)     := l_line;
            p_total_tax_amt        := l_line.tax_amt;

        ELSE
            RAISE_APPLICATION_ERROR(-20132,
                'PKG_TAX.determine_tax: unsupported region ' || p_region_cd);
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_TAX.determine_tax',
                                                 p_region_cd || ':' || p_tax_cd, SQLERRM);
            RAISE;
    END determine_tax;

END PKG_TAX;
/
