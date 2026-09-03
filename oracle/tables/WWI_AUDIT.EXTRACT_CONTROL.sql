/* =====================================================================
 * Object       : TABLE WWI_AUDIT.EXTRACT_CONTROL
 * Schema       : WWI_AUDIT (Oracle ERP - WWIGERP)
 * Deploy order : 81
 * Depends on   : WWI_AUDIT.CHANGE_LOG
 * Called by    : Downstream extract jobs; pairs with the SQL Server etl.BatchWatermark framework
 *
 * Source-side extract bookkeeping: the high-water mark each downstream extract
 * has taken from each object, held on the ERP side as well as in the SQL
 * Server control framework. The two are updated independently and are the
 * first thing to disagree after a failed load.
 *
 * The watermark is stored both as a timestamp and as a numeric id because some
 * extracts are date-driven and some are id-driven, and one is both.
 * ===================================================================== */

CREATE SEQUENCE WWI_AUDIT.SEQ_EXTRACT_CONTROL
    START WITH 10001 INCREMENT BY 1 NOCACHE NOCYCLE
/

CREATE TABLE WWI_AUDIT.EXTRACT_CONTROL
(
    EXTRACT_CONTROL_ID      NUMBER(12)      NOT NULL,
    EXTRACT_NAME            VARCHAR2(60)    NOT NULL,
    SOURCE_SCHEMA_NAME      VARCHAR2(30)    NOT NULL,
    SOURCE_OBJECT_NAME      VARCHAR2(60)    NOT NULL,
    CONSUMER_SYSTEM_CD      VARCHAR2(20)    NOT NULL,
    LOAD_TYPE_CD            VARCHAR2(10)    NOT NULL,
    WATERMARK_COLUMN_NAME   VARCHAR2(30),
    LAST_WATERMARK_TS       TIMESTAMP(6),
    LAST_WATERMARK_ID       NUMBER(15),
    LAST_EXTRACT_START_TS   TIMESTAMP(6),
    LAST_EXTRACT_END_TS     TIMESTAMP(6),
    LAST_ROW_COUNT          NUMBER(12),
    LAST_STATUS_CD          VARCHAR2(8)     DEFAULT 'NEW' NOT NULL,
    LAST_ERROR_TXT          VARCHAR2(2000),
    CONSECUTIVE_FAILURE_CNT NUMBER(4)       DEFAULT 0 NOT NULL,
    ENABLED_FLG             VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    LOCK_FLG                VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    LOCKED_BY_TXT           VARCHAR2(60),
    LOCKED_TS               TIMESTAMP(6),
    EXPECTED_FREQUENCY_CD   VARCHAR2(8),
    SLA_MINUTES             NUMBER(6),
    REGION_CD               VARCHAR2(4),
    NOTES_TXT               VARCHAR2(1000),
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_EXTRACT_CONTROL PRIMARY KEY (EXTRACT_CONTROL_ID) USING INDEX TABLESPACE WWI_AUDIT_DATA,
    CONSTRAINT UK_EXTRACT_CONTROL UNIQUE (EXTRACT_NAME, CONSUMER_SYSTEM_CD)
        USING INDEX TABLESPACE WWI_AUDIT_DATA,
    CONSTRAINT CK_EXTRACT_LOAD_TYPE CHECK (
        LOAD_TYPE_CD IN ('FULL', 'INCR', 'CDC', 'SNAPSHOT')),
    CONSTRAINT CK_EXTRACT_STATUS CHECK (
        LAST_STATUS_CD IN ('NEW', 'RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED')),
    CONSTRAINT CK_EXTRACT_FLAGS CHECK (ENABLED_FLG IN ('Y', 'N') AND LOCK_FLG IN ('Y', 'N')),
    CONSTRAINT CK_EXTRACT_WATERMARK CHECK (
        LOAD_TYPE_CD <> 'INCR' OR WATERMARK_COLUMN_NAME IS NOT NULL)
)
TABLESPACE WWI_AUDIT_DATA
/

CREATE INDEX WWI_AUDIT.IX_EXTRACT_CONTROL_OBJECT
    ON WWI_AUDIT.EXTRACT_CONTROL (SOURCE_SCHEMA_NAME, SOURCE_OBJECT_NAME)
    TABLESPACE WWI_AUDIT_DATA
/

CREATE INDEX WWI_AUDIT.IX_EXTRACT_CONTROL_STATUS
    ON WWI_AUDIT.EXTRACT_CONTROL (LAST_STATUS_CD, ENABLED_FLG) TABLESPACE WWI_AUDIT_DATA
/

COMMENT ON COLUMN WWI_AUDIT.EXTRACT_CONTROL.LOCK_FLG IS
    'Advisory lock. A job killed mid-run leaves this Y and blocks the next run until cleared by hand.'
/
