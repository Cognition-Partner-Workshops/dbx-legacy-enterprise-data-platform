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
 *               and some on a surrogate key.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_AUDIT.V_EXTRACT_WATERMARK AS
SELECT ec.EXTRACT_CONTROL_ID,
       ec.EXTRACT_NAME,
       ec.SRC_SCHEMA_NAME,
       ec.SRC_OBJECT_NAME,
       ec.WATERMARK_TYPE_CD,
       ec.LAST_EXTRACT_VALUE_TXT,
       ec.LAST_EXTRACT_DT,
       ec.LAST_ROW_COUNT,
       ec.LAST_STATUS_CD,
       ec.ENABLED_FLAG,
       CASE
           WHEN NVL(ec.ENABLED_FLAG, 'N') = 'N'                            THEN 'DISABLED'
           WHEN ec.LAST_EXTRACT_DT IS NULL                                 THEN 'NEVER_RUN'
           WHEN ec.LAST_STATUS_CD = 'FAILED'                               THEN 'FAILED'
           WHEN ec.LAST_EXTRACT_DT < SYSDATE - NVL(ec.SLA_HOURS_NUM, 26) / 24
                                                                           THEN 'SLA_BREACH'
           ELSE 'OK'
       END                                                    AS EXTRACT_HEALTH_CD,
       chg.PENDING_CHANGE_COUNT,
       chg.OLDEST_PENDING_DT,
       NVL(err.OPEN_ERROR_COUNT, 0)                           AS OPEN_ERROR_COUNT
  FROM WWI_AUDIT.EXTRACT_CONTROL ec
  LEFT OUTER JOIN (
        SELECT c.SRC_SCHEMA_NAME,
               c.SRC_OBJECT_NAME,
               COUNT(*)             AS PENDING_CHANGE_COUNT,
               MIN(c.CHANGE_DT)     AS OLDEST_PENDING_DT
          FROM WWI_AUDIT.CHANGE_LOG c
         WHERE NVL(c.EXTRACTED_FLAG, 'N') = 'N'
         GROUP BY c.SRC_SCHEMA_NAME, c.SRC_OBJECT_NAME
       ) chg
    ON chg.SRC_SCHEMA_NAME = ec.SRC_SCHEMA_NAME
   AND chg.SRC_OBJECT_NAME = ec.SRC_OBJECT_NAME
  LEFT OUTER JOIN (
        SELECT e.EXTRACT_NAME, COUNT(*) AS OPEN_ERROR_COUNT
          FROM WWI_AUDIT.INTERFACE_ERROR e
         WHERE e.RESOLVED_DT IS NULL
         GROUP BY e.EXTRACT_NAME
       ) err
    ON err.EXTRACT_NAME = ec.EXTRACT_NAME
/
