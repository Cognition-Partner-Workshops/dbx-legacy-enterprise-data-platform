/* ============================================================================
 * Object      : WWI_AUDIT.PRC_PURGE_ERP_HISTORY (procedure)
 * Schema      : WWI_AUDIT
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_AUDIT.PKG_EXTRACT_CONTROL, WWI_AUDIT.INTERFACE_ERROR,
 *               WWI_AUDIT.PURGE_LOG, WWI_FIN.AP_AGING_SNAPSHOT,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : DBMS_JOB 'ERP_PURGE_QUARTERLY'
 * Notes       : Archive first, delete second, and never delete anything an
 *               extract has not consumed. Aging snapshots are kept for seven
 *               years for NA tax audits, five for APAC and two for EU where
 *               the retention policy is the binding constraint.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_AUDIT.PRC_PURGE_ERP_HISTORY
(
    p_dry_run       IN  VARCHAR2 DEFAULT 'Y',
    p_change_cnt    OUT PLS_INTEGER,
    p_error_cnt     OUT PLS_INTEGER,
    p_snapshot_cnt  OUT PLS_INTEGER
)
IS
    TYPE t_region_tab IS TABLE OF VARCHAR2(4);
    l_regions   t_region_tab := t_region_tab('NA', 'EU', 'APAC');
    l_region    VARCHAR2(4);
    l_months    PLS_INTEGER;
    l_cutoff_dt DATE;
    l_batch     PLS_INTEGER;
BEGIN
    p_change_cnt   := 0;
    p_error_cnt    := 0;
    p_snapshot_cnt := 0;

    IF NVL(p_dry_run, 'Y') = 'Y' THEN
        SELECT COUNT(*)
          INTO p_change_cnt
          FROM WWI_AUDIT.CHANGE_LOG
         WHERE CHANGE_TS < TRUNC(SYSDATE) - 90
           AND NVL(EXTRACTED_FLG, 'N') = 'Y';
    ELSE
        WWI_AUDIT.PKG_EXTRACT_CONTROL.purge_change_log(90, p_change_cnt);
    END IF;

    /* resolved interface errors are kept for a year, unresolved ones for
       ever - deleting an open reject has burned this team before        */
    IF NVL(p_dry_run, 'Y') <> 'Y' THEN
        DELETE FROM WWI_AUDIT.INTERFACE_ERROR
         WHERE RESOLVED_TS IS NOT NULL
           AND RESOLVED_TS < ADD_MONTHS(TRUNC(SYSDATE), -12);

        p_error_cnt := SQL%ROWCOUNT;
        COMMIT;
    ELSE
        SELECT COUNT(*)
          INTO p_error_cnt
          FROM WWI_AUDIT.INTERFACE_ERROR
         WHERE RESOLVED_TS IS NOT NULL
           AND RESOLVED_TS < ADD_MONTHS(TRUNC(SYSDATE), -12);
    END IF;

    FOR i IN 1 .. l_regions.COUNT LOOP
        l_region := l_regions(i);
        l_months := CASE l_region
                        WHEN 'EU'   THEN 24
                        WHEN 'APAC' THEN 60
                        ELSE 84
                    END;
        l_cutoff_dt := ADD_MONTHS(TRUNC(SYSDATE), -l_months);

        IF NVL(p_dry_run, 'Y') = 'Y' THEN
            SELECT COUNT(*)
              INTO l_batch
              FROM WWI_FIN.AP_AGING_SNAPSHOT
             WHERE REGION_CD   = l_region
               AND SNAPSHOT_DT < l_cutoff_dt;

            p_snapshot_cnt := p_snapshot_cnt + l_batch;
            CONTINUE;
        END IF;

        LOOP
            DELETE FROM WWI_FIN.AP_AGING_SNAPSHOT
             WHERE REGION_CD   = l_region
               AND SNAPSHOT_DT < l_cutoff_dt
               AND ROWNUM <= 5000;

            l_batch        := SQL%ROWCOUNT;
            p_snapshot_cnt := p_snapshot_cnt + l_batch;
            COMMIT;

            EXIT WHEN l_batch = 0;
        END LOOP;

        INSERT INTO WWI_AUDIT.PURGE_LOG
            (PURGE_LOG_ID, SCHEMA_NAME, TABLE_NAME, PURGE_RUN_TS, CUTOFF_DT,
             ROWS_PURGED_CNT, RUN_BY)
        VALUES
            (WWI_AUDIT.SEQ_PURGE_LOG.NEXTVAL, 'WWI_FIN', 'AP_AGING_SNAPSHOT',
             SYSDATE, l_cutoff_dt, p_snapshot_cnt, USER);
        COMMIT;
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_PURGE_ERP_HISTORY', NULL,
                                             SQLERRM);
        RAISE;
END PRC_PURGE_ERP_HISTORY;
/
