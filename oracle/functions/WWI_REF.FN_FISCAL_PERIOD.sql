/* ============================================================================
 * Object      : WWI_REF.FN_FISCAL_PERIOD (function)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.CALENDAR_FISCAL, WWI_REF.REGION_REF
 * Called by   : WWI_FIN.PKG_GL_POSTING, WWI_FIN.PKG_AP_INVOICE,
 *               WWI_FIN.PRC_CLOSE_ACCOUNTING_PERIOD, WWI_FIN.PRC_LOAD_AP_AGING,
 *               WWI_FIN.V_GL_JOURNAL_EXTRACT
 * Errors      : -20061 no fiscal calendar row and no derivable period
 * History     : 1996 NA 4-4-5; 2004 EU calendar month; 2011 APAC April year
 *               start; 2013 adjustment period 13 introduced for NA only.
 * Notes       : Table lookup first. The arithmetic fallback exists because the
 *               calendar has never been loaded more than two years ahead.
 * ========================================================================= */

CREATE OR REPLACE FUNCTION WWI_REF.FN_FISCAL_PERIOD
(
    p_dt        IN DATE,
    p_region_cd IN VARCHAR2 DEFAULT 'NA'
)
RETURN VARCHAR2
IS
    l_calendar_cd WWI_REF.REGION_REF.FISCAL_CALENDAR_CD%TYPE;
    l_period_cd   WWI_REF.CALENDAR_FISCAL.PERIOD_CD%TYPE;
    l_year_num    NUMBER;
    l_period_num  NUMBER;
BEGIN
    IF p_dt IS NULL THEN
        RETURN NULL;
    END IF;

    BEGIN
        SELECT r.FISCAL_CALENDAR_CD
          INTO l_calendar_cd
          FROM WWI_REF.REGION_REF r
         WHERE r.REGION_CD = UPPER(p_region_cd);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_calendar_cd := 'GREG';
    END;

    BEGIN
        SELECT c.PERIOD_CD
          INTO l_period_cd
          FROM WWI_REF.CALENDAR_FISCAL c
         WHERE c.CALENDAR_CD = l_calendar_cd
           AND c.CALENDAR_DT = TRUNC(p_dt)
           AND NVL(c.ADJUSTMENT_PERIOD_FLG, 'N') = 'N';
        RETURN l_period_cd;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL;
        WHEN TOO_MANY_ROWS THEN
            SELECT MIN(c.PERIOD_CD)
              INTO l_period_cd
              FROM WWI_REF.CALENDAR_FISCAL c
             WHERE c.CALENDAR_CD = l_calendar_cd
               AND c.CALENDAR_DT = TRUNC(p_dt);
            RETURN l_period_cd;
    END;

    /* --- arithmetic fallback ------------------------------------------- */
    CASE UPPER(p_region_cd)
        WHEN 'APAC' THEN
            /* fiscal year starts 1 April and is named for the closing year */
            l_year_num   := TO_NUMBER(TO_CHAR(p_dt, 'YYYY'))
                            + CASE WHEN TO_NUMBER(TO_CHAR(p_dt, 'MM')) >= 4 THEN 1 ELSE 0 END;
            l_period_num := MOD(TO_NUMBER(TO_CHAR(p_dt, 'MM')) + 8, 12) + 1;
        WHEN 'NA' THEN
            /* 4-4-5: period from the ISO week, capped at 12 */
            l_year_num   := TO_NUMBER(TO_CHAR(p_dt, 'YYYY'));
            l_period_num := LEAST(CEIL(TO_NUMBER(TO_CHAR(p_dt, 'IW')) / 4.333), 12);
        ELSE
            l_year_num   := TO_NUMBER(TO_CHAR(p_dt, 'YYYY'));
            l_period_num := TO_NUMBER(TO_CHAR(p_dt, 'MM'));
    END CASE;

    IF l_year_num IS NULL OR l_period_num IS NULL THEN
        RAISE_APPLICATION_ERROR(-20061,
            'FN_FISCAL_PERIOD: cannot derive a period for '
            || TO_CHAR(p_dt, 'YYYY-MM-DD') || ' region ' || p_region_cd);
    END IF;

    RETURN TO_CHAR(l_year_num) || '-' || LPAD(TO_CHAR(l_period_num), 2, '0');
END FN_FISCAL_PERIOD;
/
