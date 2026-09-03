/* ============================================================================
 * Object      : WWI_PROC.PRC_BUILD_SUPPLIER_SCORECARD (procedure)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PKG_SUPPLIER_PERF, WWI_PROC.SUPPLIER_SCORECARD,
 *               WWI_MDM.SUPP_MASTER, WWI_MDM.PKG_SUPPLIER_MASTER,
 *               WWI_REF.FN_FISCAL_PERIOD, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : DBMS_JOB 'ERP_SCORECARD_MONTHLY', SSIS EXT_ORA_SupplierScore
 * Notes       : Runs the scorecard build for the period each region considers
 *               closed, then demotes suppliers whose rating dropped two
 *               grades in a row. The demotion rule only exists in EU and APAC.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_PROC.PRC_BUILD_SUPPLIER_SCORECARD
(
    p_period_cd   IN  WWI_PROC.SUPPLIER_SCORECARD.PERIOD_CD%TYPE DEFAULT NULL,
    p_region_cd   IN  VARCHAR2 DEFAULT NULL,
    p_built_cnt   OUT PLS_INTEGER,
    p_demoted_cnt OUT PLS_INTEGER
)
IS
    TYPE t_region_tab IS TABLE OF VARCHAR2(4);
    l_regions  t_region_tab := t_region_tab('NA', 'EU', 'APAC');
    l_region   VARCHAR2(4);
    l_period   WWI_PROC.SUPPLIER_SCORECARD.PERIOD_CD%TYPE;
    l_cnt      PLS_INTEGER;
BEGIN
    p_built_cnt   := 0;
    p_demoted_cnt := 0;

    FOR i IN 1 .. l_regions.COUNT LOOP
        l_region := l_regions(i);

        IF p_region_cd IS NOT NULL AND p_region_cd <> l_region THEN
            CONTINUE;
        END IF;

        l_period := NVL(p_period_cd,
                        WWI_REF.FN_FISCAL_PERIOD(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1),
                                                 l_region));

        BEGIN
            WWI_PROC.PKG_SUPPLIER_PERF.build_scorecards(l_period, l_region, l_cnt);
            p_built_cnt := p_built_cnt + l_cnt;
        EXCEPTION
            WHEN OTHERS THEN
                WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL,
                    'WWI_PROC.SUPPLIER_SCORECARD', l_region || '/' || l_period,
                    'SCORECARD_FAILED', SQLERRM, 'E');
                CONTINUE;
        END;

        IF l_region IN ('EU', 'APAC') THEN
            FOR rec IN (SELECT sc.SUPP_ID, sc.RATING_CD, prev.RATING_CD AS PREV_RATING
                          FROM WWI_PROC.SUPPLIER_SCORECARD sc
                          JOIN WWI_PROC.SUPPLIER_SCORECARD prev
                            ON prev.SUPP_ID = sc.SUPP_ID
                           AND prev.PERIOD_CD = (SELECT MAX(p2.PERIOD_CD)
                                                   FROM WWI_PROC.SUPPLIER_SCORECARD p2
                                                  WHERE p2.SUPP_ID = sc.SUPP_ID
                                                    AND p2.PERIOD_CD < sc.PERIOD_CD)
                         WHERE sc.PERIOD_CD = l_period
                           AND sc.REGION_CD = l_region
                           AND sc.RATING_CD = 'WATCH'
                           AND prev.RATING_CD IN ('WATCH', 'APPROVED')) LOOP

                WWI_MDM.PKG_SUPPLIER_MASTER.block_supplier(
                    p_supp_id    => rec.SUPP_ID,
                    p_reason_cd  => 'PERF_DEMOTION',
                    p_blocked_by => 'ERP_SCORECARD_MONTHLY');

                p_demoted_cnt := p_demoted_cnt + 1;
            END LOOP;

            COMMIT;
        END IF;
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_BUILD_SUPPLIER_SCORECARD',
                                             NVL(p_region_cd, 'ALL'), SQLERRM);
        RAISE;
END PRC_BUILD_SUPPLIER_SCORECARD;
/
