/* ============================================================================
 * Object      : WWI_AUDIT.PKG_EXTRACT_CONTROL (package body)
 * Schema      : WWI_AUDIT
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_AUDIT.PKG_EXTRACT_CONTROL, WWI_AUDIT.EXTRACT_CONTROL,
 *               WWI_AUDIT.CHANGE_LOG, WWI_AUDIT.PURGE_LOG,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_AUDIT.PKG_EXTRACT_CONTROL AS

    c_bulk_limit CONSTANT PLS_INTEGER := 5000;

    FUNCTION get_watermark
    (
        p_extract_name IN WWI_AUDIT.EXTRACT_CONTROL.EXTRACT_NAME%TYPE
    ) RETURN WWI_AUDIT.EXTRACT_CONTROL.LAST_EXTRACT_VALUE_TXT%TYPE
    IS
        l_value WWI_AUDIT.EXTRACT_CONTROL.LAST_EXTRACT_VALUE_TXT%TYPE;
    BEGIN
        SELECT LAST_EXTRACT_VALUE_TXT
          INTO l_value
          FROM WWI_AUDIT.EXTRACT_CONTROL
         WHERE EXTRACT_NAME = p_extract_name;

        RETURN l_value;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20511,
                'PKG_EXTRACT_CONTROL.get_watermark: extract ' || p_extract_name
                || ' is not registered');
    END get_watermark;

    FUNCTION get_watermark_dt
    (
        p_extract_name IN WWI_AUDIT.EXTRACT_CONTROL.EXTRACT_NAME%TYPE
    ) RETURN DATE
    IS
        l_value WWI_AUDIT.EXTRACT_CONTROL.LAST_EXTRACT_VALUE_TXT%TYPE;
        l_type  WWI_AUDIT.EXTRACT_CONTROL.WATERMARK_TYPE_CD%TYPE;
    BEGIN
        SELECT LAST_EXTRACT_VALUE_TXT, WATERMARK_TYPE_CD
          INTO l_value, l_type
          FROM WWI_AUDIT.EXTRACT_CONTROL
         WHERE EXTRACT_NAME = p_extract_name;

        IF l_type <> 'DATE' OR l_value IS NULL THEN
            /* a key-watermarked extract has no meaningful date; callers use
               the epoch so their BETWEEN predicate still works             */
            RETURN DATE '1900-01-01';
        END IF;

        RETURN TO_DATE(l_value, 'YYYY-MM-DD HH24:MI:SS');
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20511,
                'PKG_EXTRACT_CONTROL.get_watermark_dt: extract ' || p_extract_name
                || ' is not registered');
        WHEN OTHERS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_EXTRACT_CONTROL.get_watermark_dt',
                                                 p_extract_name, SQLERRM);
            RETURN DATE '1900-01-01';
    END get_watermark_dt;

    PROCEDURE begin_extract
    (
        p_extract_name IN  WWI_AUDIT.EXTRACT_CONTROL.EXTRACT_NAME%TYPE,
        p_run_id       OUT NUMBER,
        p_from_value   OUT WWI_AUDIT.EXTRACT_CONTROL.LAST_EXTRACT_VALUE_TXT%TYPE,
        p_to_value     OUT WWI_AUDIT.EXTRACT_CONTROL.LAST_EXTRACT_VALUE_TXT%TYPE
    )
    IS
        l_ctl WWI_AUDIT.EXTRACT_CONTROL%ROWTYPE;
    BEGIN
        SELECT * INTO l_ctl
          FROM WWI_AUDIT.EXTRACT_CONTROL
         WHERE EXTRACT_NAME = p_extract_name
           FOR UPDATE;

        IF NVL(l_ctl.ENABLED_FLAG, 'N') = 'N' THEN
            RAISE_APPLICATION_ERROR(-20512,
                'PKG_EXTRACT_CONTROL.begin_extract: extract ' || p_extract_name
                || ' is disabled');
        END IF;

        p_run_id     := WWI_AUDIT.SEQ_EXTRACT_RUN.NEXTVAL;
        p_from_value := l_ctl.LAST_EXTRACT_VALUE_TXT;

        /* the upper bound is frozen here so a long running extract does not
           lose rows written while it is streaming. The one minute lag is a
           1990s hedge against clock skew between the ERP and the ETL server. */
        p_to_value := CASE l_ctl.WATERMARK_TYPE_CD
                          WHEN 'DATE' THEN TO_CHAR(SYSDATE - 1 / 1440,
                                                   'YYYY-MM-DD HH24:MI:SS')
                          ELSE TO_CHAR(WWI_AUDIT.SEQ_CHANGE_LOG.CURRVAL)
                      END;

        UPDATE WWI_AUDIT.EXTRACT_CONTROL
           SET CURRENT_RUN_ID  = p_run_id,
               RUN_STARTED_DT  = SYSDATE,
               LAST_STATUS_CD  = 'RUNNING',
               LAST_UPD_DT     = SYSDATE
         WHERE EXTRACT_NAME = p_extract_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20511,
                'PKG_EXTRACT_CONTROL.begin_extract: extract ' || p_extract_name
                || ' is not registered');
    END begin_extract;

    PROCEDURE end_extract
    (
        p_extract_name IN WWI_AUDIT.EXTRACT_CONTROL.EXTRACT_NAME%TYPE,
        p_run_id       IN NUMBER,
        p_row_count    IN WWI_AUDIT.EXTRACT_CONTROL.LAST_ROW_COUNT%TYPE,
        p_to_value     IN WWI_AUDIT.EXTRACT_CONTROL.LAST_EXTRACT_VALUE_TXT%TYPE,
        p_status_cd    IN WWI_AUDIT.EXTRACT_CONTROL.LAST_STATUS_CD%TYPE DEFAULT 'SUCCESS'
    )
    IS
        l_prior WWI_AUDIT.EXTRACT_CONTROL.LAST_EXTRACT_VALUE_TXT%TYPE;
        l_type  WWI_AUDIT.EXTRACT_CONTROL.WATERMARK_TYPE_CD%TYPE;
    BEGIN
        SELECT LAST_EXTRACT_VALUE_TXT, WATERMARK_TYPE_CD
          INTO l_prior, l_type
          FROM WWI_AUDIT.EXTRACT_CONTROL
         WHERE EXTRACT_NAME = p_extract_name
           FOR UPDATE;

        IF l_prior IS NOT NULL AND p_to_value IS NOT NULL THEN
            IF (l_type = 'KEY' AND TO_NUMBER(p_to_value) < TO_NUMBER(l_prior))
               OR (l_type = 'DATE'
                   AND TO_DATE(p_to_value, 'YYYY-MM-DD HH24:MI:SS')
                       < TO_DATE(l_prior, 'YYYY-MM-DD HH24:MI:SS')) THEN
                RAISE_APPLICATION_ERROR(-20513,
                    'PKG_EXTRACT_CONTROL.end_extract: watermark for '
                    || p_extract_name || ' would move backwards from '
                    || l_prior || ' to ' || p_to_value);
            END IF;
        END IF;

        UPDATE WWI_AUDIT.EXTRACT_CONTROL
           SET LAST_EXTRACT_VALUE_TXT = p_to_value,
               LAST_EXTRACT_DT        = SYSDATE,
               LAST_ROW_COUNT         = p_row_count,
               LAST_STATUS_CD         = p_status_cd,
               LAST_RUN_ID            = p_run_id,
               CURRENT_RUN_ID         = NULL,
               CONSECUTIVE_FAIL_CNT   = 0,
               LAST_UPD_DT            = SYSDATE
         WHERE EXTRACT_NAME = p_extract_name;

        WWI_AUDIT.PKG_DATA_QUALITY.assert_reject_rate(p_extract_name, p_row_count);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20511,
                'PKG_EXTRACT_CONTROL.end_extract: extract ' || p_extract_name
                || ' is not registered');
    END end_extract;

    PROCEDURE fail_extract
    (
        p_extract_name IN WWI_AUDIT.EXTRACT_CONTROL.EXTRACT_NAME%TYPE,
        p_run_id       IN NUMBER,
        p_message      IN VARCHAR2
    )
    IS
        l_fail_cnt PLS_INTEGER;
    BEGIN
        UPDATE WWI_AUDIT.EXTRACT_CONTROL
           SET LAST_STATUS_CD       = 'FAILED',
               LAST_RUN_ID          = p_run_id,
               CURRENT_RUN_ID       = NULL,
               CONSECUTIVE_FAIL_CNT = NVL(CONSECUTIVE_FAIL_CNT, 0) + 1,
               LAST_UPD_DT          = SYSDATE
         WHERE EXTRACT_NAME = p_extract_name
        RETURNING CONSECUTIVE_FAIL_CNT INTO l_fail_cnt;

        WWI_AUDIT.PKG_DATA_QUALITY.log_reject(p_extract_name, 'EXTRACT_CONTROL',
                                              TO_CHAR(p_run_id), 'EXTRACT_FAILED',
                                              p_message, 'F');

        /* three strikes and the extract takes itself out of the nightly run
           so the rest of the batch is not held up                          */
        IF l_fail_cnt >= 3 THEN
            UPDATE WWI_AUDIT.EXTRACT_CONTROL
               SET ENABLED_FLAG = 'N',
                   LAST_UPD_DT  = SYSDATE
             WHERE EXTRACT_NAME = p_extract_name;
        END IF;
    END fail_extract;

    PROCEDURE mark_changes_extracted
    (
        p_schema_name IN  WWI_AUDIT.CHANGE_LOG.SRC_SCHEMA_NAME%TYPE,
        p_object_name IN  WWI_AUDIT.CHANGE_LOG.SRC_OBJECT_NAME%TYPE,
        p_thru_dt     IN  DATE,
        p_marked_cnt  OUT PLS_INTEGER
    )
    IS
        CURSOR c_changes IS
            SELECT CHANGE_LOG_ID
              FROM WWI_AUDIT.CHANGE_LOG
             WHERE SRC_SCHEMA_NAME = p_schema_name
               AND SRC_OBJECT_NAME = p_object_name
               AND NVL(EXTRACTED_FLAG, 'N') = 'N'
               AND CHANGE_DT <= p_thru_dt
             ORDER BY CHANGE_LOG_ID;

        TYPE t_id_tab IS TABLE OF WWI_AUDIT.CHANGE_LOG.CHANGE_LOG_ID%TYPE;
        l_ids t_id_tab;
    BEGIN
        p_marked_cnt := 0;

        OPEN c_changes;
        LOOP
            FETCH c_changes BULK COLLECT INTO l_ids LIMIT c_bulk_limit;
            EXIT WHEN l_ids.COUNT = 0;

            FORALL i IN 1 .. l_ids.COUNT
                UPDATE WWI_AUDIT.CHANGE_LOG
                   SET EXTRACTED_FLAG = 'Y',
                       EXTRACTED_DT   = SYSDATE
                 WHERE CHANGE_LOG_ID = l_ids(i);

            p_marked_cnt := p_marked_cnt + l_ids.COUNT;
            COMMIT;

            EXIT WHEN c_changes%NOTFOUND;
        END LOOP;
        CLOSE c_changes;
    EXCEPTION
        WHEN OTHERS THEN
            IF c_changes%ISOPEN THEN
                CLOSE c_changes;
            END IF;
            RAISE;
    END mark_changes_extracted;

    PROCEDURE purge_change_log
    (
        p_retain_days IN  PLS_INTEGER DEFAULT 90,
        p_purged_cnt  OUT PLS_INTEGER
    )
    IS
        l_cutoff_dt DATE;
        l_batch     PLS_INTEGER;
    BEGIN
        l_cutoff_dt  := TRUNC(SYSDATE) - p_retain_days;
        p_purged_cnt := 0;

        LOOP
            DELETE FROM WWI_AUDIT.CHANGE_LOG
             WHERE CHANGE_DT < l_cutoff_dt
               AND NVL(EXTRACTED_FLAG, 'N') = 'Y'
               AND ROWNUM <= c_bulk_limit;

            l_batch      := SQL%ROWCOUNT;
            p_purged_cnt := p_purged_cnt + l_batch;
            COMMIT;

            EXIT WHEN l_batch = 0;
        END LOOP;

        INSERT INTO WWI_AUDIT.PURGE_LOG
            (PURGE_LOG_ID, SRC_SCHEMA_NAME, SRC_OBJECT_NAME, PURGE_DT,
             CUTOFF_DT, ROW_COUNT, PURGED_BY)
        VALUES
            (WWI_AUDIT.SEQ_PURGE_LOG.NEXTVAL, 'WWI_AUDIT', 'CHANGE_LOG', SYSDATE,
             l_cutoff_dt, p_purged_cnt, USER);
        COMMIT;
    END purge_change_log;

END PKG_EXTRACT_CONTROL;
/
