/* ============================================================================
 * Object      : WWI_AUDIT.V_EXTRACT_WATERMARK (view)
 * Schema      : WWI_AUDIT
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_AUDIT.EXTRACT_CONTROL, WWI_AUDIT.CHANGE_LOG,
 *               WWI_AUDIT.INTERFACE_ERROR
 * Called by   : SSIS Oracle extract packages (pre-execute watermark read),
 *               WWI_AUDIT.PKG_EXTRACT_CONTROL
 * Notes       : The SSIS side reads LAST_EXTRACT_VALUE_TXT as a string and
 *               casts it itself, because some extracts watermark on a date
 *               and some on a surrogate key. EXTRACT_CONTROL keeps both, so
 *               the view coalesces them into the one text column the packages
 *               expect.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_AUDIT.V_EXTRACT_WATERMARK AS
SELECT ec.EXTRACT_CONTROL_ID,
       ec.EXTRACT_NAME,
       ec.SOURCE_SCHEMA_NAME                                  AS SRC_SCHEMA_NAME,
       ec.SOURCE_OBJECT_NAME                                  AS SRC_OBJECT_NAME,
       ec.LOAD_TYPE_CD                                        AS WATERMARK_TYPE_CD,
       COALESCE(TO_CHAR(ec.LAST_WATERMARK_TS, 'YYYY-MM-DD HH24:MI:SS.FF6'),
                TO_CHAR(ec.LAST_WATERMARK_ID))                AS LAST_EXTRACT_VALUE_TXT,
       CAST(ec.LAST_EXTRACT_END_TS AS DATE)                   AS LAST_EXTRACT_DT,
       ec.LAST_ROW_COUNT,
       ec.LAST_STATUS_CD,
       ec.ENABLED_FLG                                         AS ENABLED_FLAG,
       CASE
           WHEN NVL(ec.ENABLED_FLG, 'N') = 'N'                             THEN 'DISABLED'
           WHEN ec.LAST_EXTRACT_END_TS IS NULL                             THEN 'NEVER_RUN'
           WHEN ec.LAST_STATUS_CD = 'FAILED'                               THEN 'FAILED'
           WHEN ec.LAST_EXTRACT_END_TS < SYSDATE - NVL(ec.SLA_MINUTES, 1560) / 1440
                                                                           THEN 'SLA_BREACH'
           ELSE 'OK'
       END                                                    AS EXTRACT_HEALTH_CD,
       chg.PENDING_CHANGE_COUNT,
       chg.OLDEST_PENDING_DT,
       NVL(err.OPEN_ERROR_COUNT, 0)                           AS OPEN_ERROR_COUNT
  FROM WWI_AUDIT.EXTRACT_CONTROL ec
  LEFT OUTER JOIN (
        SELECT c.SCHEMA_NAME,
               c.TABLE_NAME,
               COUNT(*)             AS PENDING_CHANGE_COUNT,
               MIN(c.CHANGE_TS)     AS OLDEST_PENDING_DT
          FROM WWI_AUDIT.CHANGE_LOG c
         WHERE NVL(c.EXTRACTED_FLG, 'N') = 'N'
         GROUP BY c.SCHEMA_NAME, c.TABLE_NAME
       ) chg
    ON chg.SCHEMA_NAME = ec.SOURCE_SCHEMA_NAME
   AND chg.TABLE_NAME = ec.SOURCE_OBJECT_NAME
  LEFT OUTER JOIN (
        SELECT e.INTERFACE_NAME, COUNT(*) AS OPEN_ERROR_COUNT
          FROM WWI_AUDIT.INTERFACE_ERROR e
         WHERE e.RESOLVED_TS IS NULL
         GROUP BY e.INTERFACE_NAME
       ) err
    ON err.INTERFACE_NAME = ec.EXTRACT_NAME
/
