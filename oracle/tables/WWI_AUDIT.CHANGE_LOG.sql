/* =====================================================================
 * Object       : TABLE WWI_AUDIT.CHANGE_LOG
 * Schema       : WWI_AUDIT (Oracle ERP - WWIGERP)
 * Deploy order : 80 (first table in WWI_AUDIT)
 * Depends on   : oracle/ddl/02_create_schemas.sql
 * Called by    : Row-level audit triggers on WWI_MDM and WWI_FIN, CDC-style extracts
 *
 * Row-level change capture written by per-table triggers. Old and new values
 * are held as text for a single column per row, so a ten-column update writes
 * ten rows. Only the tables someone remembered to put a trigger on are
 * represented, and the trigger set differs between the NA and EU instances
 * because the EU instance was cloned before the 2016 trigger rollout.
 *
 * Range partitioned on CHANGE_TS with a monthly interval so partitions can be
 * dropped by the purge job.
 * ===================================================================== */

CREATE SEQUENCE WWI_AUDIT.SEQ_CHANGE_LOG
    START WITH 50000001 INCREMENT BY 1 CACHE 1000 NOCYCLE
/

CREATE TABLE WWI_AUDIT.CHANGE_LOG
(
    CHANGE_LOG_ID           NUMBER(15)      NOT NULL,
    CHANGE_TS               TIMESTAMP(6)    DEFAULT SYSTIMESTAMP NOT NULL,
    SCHEMA_NAME             VARCHAR2(30)    NOT NULL,
    TABLE_NAME              VARCHAR2(30)    NOT NULL,
    COLUMN_NAME             VARCHAR2(30),
    OPERATION_CD            VARCHAR2(1)     NOT NULL,
    PK_VALUE_TXT            VARCHAR2(120)   NOT NULL,
    OLD_VALUE_TXT           VARCHAR2(2000),
    NEW_VALUE_TXT           VARCHAR2(2000),
    CHANGED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    OS_USER_TXT             VARCHAR2(60),
    TERMINAL_TXT            VARCHAR2(60),
    PROGRAM_TXT             VARCHAR2(80),
    SESSION_ID              NUMBER(12),
    TRANSACTION_ID_TXT      VARCHAR2(30),
    REGION_CD               VARCHAR2(4),
    REASON_TXT              VARCHAR2(400),
    EXTRACTED_FLG           VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    EXTRACTED_TS            TIMESTAMP(6),
    RETENTION_UNTIL_DT      DATE,
    CONSTRAINT CK_CHANGE_LOG_OP CHECK (OPERATION_CD IN ('I', 'U', 'D')),
    CONSTRAINT CK_CHANGE_LOG_EXTRACTED CHECK (EXTRACTED_FLG IN ('Y', 'N'))
)
PARTITION BY RANGE (CHANGE_TS)
INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(
    PARTITION CHANGE_LOG_INIT VALUES LESS THAN (TIMESTAMP '2023-01-01 00:00:00')
        TABLESPACE WWI_AUDIT_DATA
)
/

ALTER TABLE WWI_AUDIT.CHANGE_LOG ADD CONSTRAINT PK_CHANGE_LOG
    PRIMARY KEY (CHANGE_LOG_ID) USING INDEX TABLESPACE WWI_AUDIT_DATA
/

CREATE INDEX WWI_AUDIT.IX_CHANGE_LOG_TABLE
    ON WWI_AUDIT.CHANGE_LOG (TABLE_NAME, CHANGE_TS) LOCAL TABLESPACE WWI_AUDIT_DATA
/

CREATE INDEX WWI_AUDIT.IX_CHANGE_LOG_PK
    ON WWI_AUDIT.CHANGE_LOG (TABLE_NAME, PK_VALUE_TXT) TABLESPACE WWI_AUDIT_DATA
/

CREATE INDEX WWI_AUDIT.IX_CHANGE_LOG_UNEXTRACTED
    ON WWI_AUDIT.CHANGE_LOG (EXTRACTED_FLG, CHANGE_TS) TABLESPACE WWI_AUDIT_DATA
/

COMMENT ON COLUMN WWI_AUDIT.CHANGE_LOG.PK_VALUE_TXT IS
    'Primary key rendered as text. Composite keys are pipe-delimited by the trigger.'
/
