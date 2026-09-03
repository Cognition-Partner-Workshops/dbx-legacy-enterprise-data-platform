/* ============================================================================
 * Object      : WWI_FIN.PRC_LOAD_AP_AGING (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.PRC_BUILD_AGING_SNAPSHOT, WWI_FIN.AP_AGING_SNAPSHOT,
 *               WWI_AUDIT.PKG_EXTRACT_CONTROL, WWI_AUDIT.PKG_DATA_QUALITY,
 *               WWI_REF.FN_FISCAL_PERIOD, WWI_FIN.FN_AGING_BUCKET
 * Called by   : SSIS EXT_ORA_ApAging (pre-execute), month-end checklist
 * Notes       : ETL facing wrapper. It builds the snapshot each region needs
 *               on that region's own calendar and then advances the extract
 *               watermark so the SSIS package only reads the new rows.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_FIN.PRC_LOAD_AP_AGING
(
    p_as_of_dt  IN  DATE DEFAULT TRUNC(SYSDATE),
    p_region_cd IN  VARCHAR2 DEFAULT NULL,
    p_total_cnt OUT PLS_INTEGER
)
IS
    TYPE t_region_tab IS TABLE OF VARCHAR2(4);
    l_regions   t_region_tab := t_region_tab('NA', 'EU', 'APAC');
    l_run_id    NUMBER;
    l_from_val  WWI_AUDIT.EXTRACT_CONTROL.LAST_EXTRACT_VALUE_TXT%TYPE;
    l_to_val    WWI_AUDIT.EXTRACT_CONTROL.LAST_EXTRACT_VALUE_TXT%TYPE;
    l_region    VARCHAR2(4);
    l_snap_dt   DATE;
    l_cnt       PLS_INTEGER;
BEGIN
    p_total_cnt := 0;

    WWI_AUDIT.PKG_EXTRACT_CONTROL.begin_extract('EXT_ORA_ApAging', l_run_id,
                                                l_from_val, l_to_val);

    FOR i IN 1 .. l_regions.COUNT LOOP
        l_region := l_regions(i);

        IF p_region_cd IS NOT NULL AND p_region_cd <> l_region THEN
            CONTINUE;
        END IF;

        /* EU reports on the calendar month end, APAC on the 25th cut-off it
           inherited from the Singapore ledger, NA on the run date          */
        l_snap_dt := CASE l_region
                         WHEN 'EU'   THEN LAST_DAY(p_as_of_dt)
                         WHEN 'APAC' THEN TRUNC(p_as_of_dt, 'MM') + 24
                         ELSE TRUNC(p_as_of_dt)
                     END;

        IF l_snap_dt > TRUNC(SYSDATE) THEN
            l_snap_dt := TRUNC(SYSDATE);
        END IF;

        BEGIN
            WWI_FIN.PRC_BUILD_AGING_SNAPSHOT(l_region, l_snap_dt, 'Y', l_cnt);
            p_total_cnt := p_total_cnt + l_cnt;
        EXCEPTION
            WHEN OTHERS THEN
                WWI_AUDIT.PKG_DATA_QUALITY.log_reject('EXT_ORA_ApAging',
                    'WWI_FIN.AP_AGING_SNAPSHOT', l_region, 'BUILD_FAILED',
                    SQLERRM, 'E');
        END;
    END LOOP;

    IF p_total_cnt = 0 THEN
        WWI_AUDIT.PKG_EXTRACT_CONTROL.fail_extract('EXT_ORA_ApAging', l_run_id,
            'aging build produced no rows for ' || TO_CHAR(p_as_of_dt, 'YYYY-MM-DD'));
    ELSE
        WWI_AUDIT.PKG_EXTRACT_CONTROL.end_extract('EXT_ORA_ApAging', l_run_id,
            p_total_cnt, TO_CHAR(SYSDATE - 1 / 1440, 'YYYY-MM-DD HH24:MI:SS'));
    END IF;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_EXTRACT_CONTROL.fail_extract('EXT_ORA_ApAging', l_run_id,
                                                   SQLERRM);
        RAISE;
END PRC_LOAD_AP_AGING;
/
