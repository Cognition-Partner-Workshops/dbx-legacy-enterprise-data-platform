/* =====================================================================
 * Object       : TABLE WWI_AUDIT.PURGE_LOG
 * Schema       : WWI_AUDIT (Oracle ERP - WWIGERP)
 * Deploy order : 83
 * Depends on   : WWI_AUDIT.CHANGE_LOG
 * Called by    : Retention purge job, data-protection reporting
 *
 * What the retention purge job removed, when, and under which policy. The
 * three regions purge on different rules - NA on a seven-year tax retention,
 * EU on GDPR erasure plus a six-year statutory floor, APAC on a five-year
 * local rule - and a row here is the only remaining evidence that the purged
 * data ever existed.
 * ===================================================================== */

CREATE SEQUENCE WWI_AUDIT.SEQ_PURGE_LOG
    START WITH 100001 INCREMENT BY 1 NOCACHE NOCYCLE
/

CREATE TABLE WWI_AUDIT.PURGE_LOG
(
    PURGE_LOG_ID            NUMBER(12)      NOT NULL,
    PURGE_RUN_TS            TIMESTAMP(6)    DEFAULT SYSTIMESTAMP NOT NULL,
    POLICY_CD               VARCHAR2(20)    NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    SCHEMA_NAME             VARCHAR2(30)    NOT NULL,
    TABLE_NAME              VARCHAR2(30)    NOT NULL,
    PARTITION_NAME          VARCHAR2(30),
    PURGE_METHOD_CD         VARCHAR2(10)    NOT NULL,
    CUTOFF_DT               DATE            NOT NULL,
    ROWS_EVALUATED_CNT      NUMBER(12),
    ROWS_PURGED_CNT         NUMBER(12)      DEFAULT 0 NOT NULL,
    ROWS_ANONYMISED_CNT     NUMBER(12)      DEFAULT 0 NOT NULL,
    ROWS_RETAINED_CNT       NUMBER(12)      DEFAULT 0 NOT NULL,
    RETAIN_REASON_TXT       VARCHAR2(400),
    LEGAL_HOLD_FLG          VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    ERASURE_REQUEST_REF     VARCHAR2(40),
    ARCHIVE_FILE_REF        VARCHAR2(200),
    DURATION_SECONDS        NUMBER(9),
    RUN_STATUS_CD           VARCHAR2(8)     DEFAULT 'SUCCESS' NOT NULL,
    ERROR_TXT               VARCHAR2(2000),
    RUN_BY                  VARCHAR2(30)    DEFAULT USER NOT NULL,
    CONSTRAINT PK_PURGE_LOG PRIMARY KEY (PURGE_LOG_ID) USING INDEX TABLESPACE WWI_AUDIT_DATA,
    CONSTRAINT CK_PURGE_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC')),
    CONSTRAINT CK_PURGE_METHOD CHECK (
        PURGE_METHOD_CD IN ('DELETE', 'DROPPART', 'ANONYM', 'ARCHIVE')),
    CONSTRAINT CK_PURGE_STATUS CHECK (RUN_STATUS_CD IN ('SUCCESS', 'PARTIAL', 'FAILED')),
    CONSTRAINT CK_PURGE_LEGAL_HOLD CHECK (LEGAL_HOLD_FLG IN ('Y', 'N')),
    CONSTRAINT CK_PURGE_ERASURE CHECK (
        POLICY_CD <> 'EU_GDPR_ERASURE' OR ERASURE_REQUEST_REF IS NOT NULL)
)
TABLESPACE WWI_AUDIT_DATA
/

CREATE INDEX WWI_AUDIT.IX_PURGE_LOG_TABLE
    ON WWI_AUDIT.PURGE_LOG (SCHEMA_NAME, TABLE_NAME, PURGE_RUN_TS) TABLESPACE WWI_AUDIT_DATA
/

CREATE INDEX WWI_AUDIT.IX_PURGE_LOG_POLICY
    ON WWI_AUDIT.PURGE_LOG (POLICY_CD, PURGE_RUN_TS) TABLESPACE WWI_AUDIT_DATA
/
