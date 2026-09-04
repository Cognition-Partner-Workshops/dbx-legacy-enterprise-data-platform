/* ============================================================================
 * Object      : WWI_FIN.FN_DUE_DATE (function)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.PAYMENT_TERMS, WWI_REF.CALENDAR_FISCAL
 * Called by   : WWI_FIN.PKG_AP_INVOICE, WWI_FIN.PKG_AP_PAYMENT,
 *               WWI_FIN.PRC_RUN_DUNNING, WWI_FIN.V_AP_AGING_CURRENT
 * Errors      : -20051 unknown payment terms code
 * History     : 1996 NET terms; 2001 proximo; 2005 EU end-of-month rule;
 *               2012 APAC twice-monthly payment runs.
 * Notes       : Region drives the day-roll: NA rolls a weekend due date forward
 *               to Monday, EU rolls it back to Friday (bank cut-off), APAC
 *               snaps to the next 15th or month end.
 * ========================================================================= */

CREATE OR REPLACE FUNCTION WWI_FIN.FN_DUE_DATE
(
    p_base_dt          IN DATE,
    p_payment_terms_cd IN WWI_FIN.PAYMENT_TERMS.PAYMENT_TERMS_CD%TYPE,
    p_region_cd        IN VARCHAR2 DEFAULT 'NA'
)
RETURN DATE
IS
    l_net_days      WWI_FIN.PAYMENT_TERMS.NET_DAYS%TYPE;
    l_day_of_month  WWI_FIN.PAYMENT_TERMS.DUE_DAY_OF_MONTH_NBR%TYPE;
    l_months_fwd    WWI_FIN.PAYMENT_TERMS.MONTHS_FORWARD_NBR%TYPE;
    l_term_basis_cd WWI_FIN.PAYMENT_TERMS.TERM_BASIS_CD%TYPE;
    l_due_dt        DATE;
    l_day_name      VARCHAR2(12);
BEGIN
    IF p_base_dt IS NULL THEN
        RETURN NULL;
    END IF;

    BEGIN
        SELECT t.NET_DAYS, t.DUE_DAY_OF_MONTH_NBR, t.MONTHS_FORWARD_NBR, t.TERM_BASIS_CD
          INTO l_net_days, l_day_of_month, l_months_fwd, l_term_basis_cd
          FROM WWI_FIN.PAYMENT_TERMS t
         WHERE t.PAYMENT_TERMS_CD = p_payment_terms_cd
           AND NVL(t.ACTIVE_FLG, 'Y') = 'Y';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20051,
                'FN_DUE_DATE: unknown payment terms ' || p_payment_terms_cd);
    END;

    IF l_term_basis_cd = 'PREPAY' THEN
        l_due_dt := TRUNC(p_base_dt);
    ELSIF l_term_basis_cd = 'DOM' THEN
        /* proximo: day N of the Nth month forward, or month end if shorter */
        l_due_dt := LEAST(ADD_MONTHS(TRUNC(p_base_dt, 'MM'), NVL(l_months_fwd, 1))
                              + (NVL(l_day_of_month, 1) - 1),
                          LAST_DAY(ADD_MONTHS(p_base_dt, NVL(l_months_fwd, 1))));
    ELSIF l_term_basis_cd = 'EOM' THEN
        l_due_dt := LAST_DAY(ADD_MONTHS(TRUNC(p_base_dt), NVL(l_months_fwd, 1)));
    ELSIF UPPER(p_region_cd) = 'EU' AND l_day_of_month IS NOT NULL THEN
        /* EU: net days counted from month end, then pinned to the terms day */
        l_due_dt := LAST_DAY(p_base_dt) + NVL(l_net_days, 0);
        l_due_dt := LEAST(TRUNC(l_due_dt, 'MM') + (l_day_of_month - 1),
                          LAST_DAY(l_due_dt));
    ELSE
        l_due_dt := TRUNC(p_base_dt) + NVL(l_net_days, 30);
    END IF;

    l_day_name := TRIM(TO_CHAR(l_due_dt, 'DAY', 'NLS_DATE_LANGUAGE=ENGLISH'));

    CASE UPPER(p_region_cd)
        WHEN 'NA' THEN
            IF l_day_name = 'SATURDAY' THEN
                l_due_dt := l_due_dt + 2;
            ELSIF l_day_name = 'SUNDAY' THEN
                l_due_dt := l_due_dt + 1;
            END IF;
        WHEN 'EU' THEN
            IF l_day_name = 'SATURDAY' THEN
                l_due_dt := l_due_dt - 1;
            ELSIF l_day_name = 'SUNDAY' THEN
                l_due_dt := l_due_dt - 2;
            END IF;
        WHEN 'APAC' THEN
            IF TO_NUMBER(TO_CHAR(l_due_dt, 'DD')) <= 15 THEN
                l_due_dt := TRUNC(l_due_dt, 'MM') + 14;
            ELSE
                l_due_dt := LAST_DAY(l_due_dt);
            END IF;
        ELSE
            NULL;
    END CASE;

    RETURN l_due_dt;
END FN_DUE_DATE;
/
