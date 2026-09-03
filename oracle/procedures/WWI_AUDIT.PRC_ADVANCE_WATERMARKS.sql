/* ============================================================================
 * Object      : WWI_AUDIT.PRC_ADVANCE_WATERMARKS (procedure)
 * Schema      : WWI_AUDIT
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_AUDIT.PKG_EXTRACT_CONTROL, WWI_AUDIT.EXTRACT_CONTROL,
 *               WWI_AUDIT.V_EXTRACT_WATERMARK, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : SSIS master package ETL_ORA_Master (post-execute), operators
 *               when an extract has to be re-pointed by hand
 * Notes       : Closes out every extract the SSIS run reported as complete
 *               and marks the change log rows those extracts consumed. An
 *               extract left in RUNNING for more than the SLA is failed here
 *               so the next run is not blocked by a crashed package.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_AUDIT.PRC_ADVANCE_WATERMARKS
(
    p_extract_name IN  WWI_AUDIT.EXTRACT_CONTROL.EXTRACT_NAME%TYPE DEFAULT NULL,
    p_row_count    IN  NUMBER DEFAULT NULL,
    p_to_value     IN  VARCHAR2 DEFAULT NULL,
    p_advanced_cnt OUT PLS_INTEGER,
    p_stuck_cnt    OUT PLS_INTEGER
)
IS
    CURSOR c_running IS
        SELECT ec.EXTRACT_NAME,
               ec.CURRENT_RUN_ID,
               ec.RUN_STARTED_DT,
               ec.WATERMARK_TYPE_CD,
               ec.SRC_SCHEMA_NAME,
               ec.SRC_OBJECT_NAME,
               NVL(ec.SLA_HOURS_NUM, 26) AS SLA_HOURS_NUM
          FROM WWI_AUDIT.EXTRACT_CONTROL ec
         WHERE ec.LAST_STATUS_CD = 'RUNNING'
           AND (p_extract_name IS NULL OR ec.EXTRACT_NAME = p_extract_name)
         ORDER BY ec.EXTRACT_NAME;

    l_marked PLS_INTEGER;
    l_to_val WWI_AUDIT.EXTRACT_CONTROL.LAST_EXTRACT_VALUE_TXT%TYPE;
BEGIN
    p_advanced_cnt := 0;
    p_stuck_cnt    := 0;

    FOR rec IN c_running LOOP
        IF rec.RUN_STARTED_DT < SYSDATE - rec.SLA_HOURS_NUM / 24 THEN
            WWI_AUDIT.PKG_EXTRACT_CONTROL.fail_extract(rec.EXTRACT_NAME,
                rec.CURRENT_RUN_ID,
                'run started ' || TO_CHAR(rec.RUN_STARTED_DT, 'YYYY-MM-DD HH24:MI')
                || ' exceeded the ' || rec.SLA_HOURS_NUM || ' hour SLA');

            p_stuck_cnt := p_stuck_cnt + 1;
            COMMIT;
            CONTINUE;
        END IF;

        IF p_extract_name IS NULL THEN
            /* bulk mode only closes the extracts the caller can prove are
               finished, so anything without an explicit value is skipped  */
            CONTINUE;
        END IF;

        l_to_val := NVL(p_to_value,
                        CASE rec.WATERMARK_TYPE_CD
                            WHEN 'DATE' THEN TO_CHAR(SYSDATE - 1 / 1440,
                                                     'YYYY-MM-DD HH24:MI:SS')
                            ELSE NULL
                        END);

        IF l_to_val IS NULL THEN
            RAISE_APPLICATION_ERROR(-20631,
                'PRC_ADVANCE_WATERMARKS: ' || rec.EXTRACT_NAME
                || ' is key watermarked and needs an explicit p_to_value');
        END IF;

        WWI_AUDIT.PKG_EXTRACT_CONTROL.end_extract(rec.EXTRACT_NAME,
                                                  rec.CURRENT_RUN_ID,
                                                  NVL(p_row_count, 0), l_to_val);

        IF rec.SRC_SCHEMA_NAME IS NOT NULL AND rec.WATERMARK_TYPE_CD = 'DATE' THEN
            WWI_AUDIT.PKG_EXTRACT_CONTROL.mark_changes_extracted(
                rec.SRC_SCHEMA_NAME, rec.SRC_OBJECT_NAME,
                TO_DATE(l_to_val, 'YYYY-MM-DD HH24:MI:SS'), l_marked);
        END IF;

        p_advanced_cnt := p_advanced_cnt + 1;
        COMMIT;
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_ADVANCE_WATERMARKS',
                                             NVL(p_extract_name, 'ALL'), SQLERRM);
        RAISE;
END PRC_ADVANCE_WATERMARKS;
/
