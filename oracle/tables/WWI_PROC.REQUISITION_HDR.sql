/* =====================================================================
 * Object       : TABLE WWI_PROC.REQUISITION_HDR
 * Schema       : WWI_PROC (Oracle ERP - WWIGERP)
 * Deploy order : 40 (first table in WWI_PROC)
 * Depends on   : WWI_MDM.PRODUCT_MASTER, WWI_FIN.COST_CENTER, oracle/ddl/02_create_schemas.sql
 * Called by    : PKG_PURCHASE_ORDER, requisition approval workflow
 *
 * Internal purchase requisition. Approval is a fixed three-step chain held in
 * columns rather than a workflow table, because the 2001 implementation
 * predates the workflow module the ERP later shipped. A requisition that skips
 * straight to APPROVER_3 (over the regional limit) leaves the first two columns
 * null, so "unapproved" cannot be detected by null-checking alone.
 * ===================================================================== */

CREATE SEQUENCE WWI_PROC.SEQ_REQUISITION_HDR
    START WITH 700001 INCREMENT BY 1 NOCACHE NOCYCLE ORDER
/

CREATE TABLE WWI_PROC.REQUISITION_HDR
(
    REQ_ID                  NUMBER(12)      NOT NULL,
    REQ_NBR                 VARCHAR2(16)    NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    REQUESTOR_CD            VARCHAR2(8)     NOT NULL,
    REQUEST_DT              DATE            DEFAULT SYSDATE NOT NULL,
    NEED_BY_DT              DATE,
    COST_CENTER_CD          VARCHAR2(10)    NOT NULL,
    PROJECT_CD              VARCHAR2(12),
    REQ_STATUS_CD           VARCHAR2(4)     DEFAULT 'DRFT' NOT NULL,
    URGENCY_CD              VARCHAR2(3)     DEFAULT 'STD' NOT NULL,
    ESTIMATED_AMT           NUMBER(15,5)    DEFAULT 0 NOT NULL,
    ESTIMATED_CURR_CD       VARCHAR2(3)     NOT NULL,
    APPROVER_1_CD           VARCHAR2(8),
    APPROVER_1_DT           DATE,
    APPROVER_2_CD           VARCHAR2(8),
    APPROVER_2_DT           DATE,
    APPROVER_3_CD           VARCHAR2(8),
    APPROVER_3_DT           DATE,
    REJECTION_REASON_CD     VARCHAR2(4),
    JUSTIFICATION_TXT       VARCHAR2(2000),
    SOURCED_FLG             VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    CANCELLED_FLG           VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_REQUISITION_HDR PRIMARY KEY (REQ_ID) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_REQUISITION_NBR UNIQUE (REQ_NBR) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_REQ_HDR_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC')),
    CONSTRAINT CK_REQ_HDR_STATUS CHECK (
        REQ_STATUS_CD IN ('DRFT', 'SUBM', 'APPR', 'REJT', 'CONV', 'CANC')),
    CONSTRAINT CK_REQ_HDR_URGENCY CHECK (URGENCY_CD IN ('STD', 'EXP', 'EMG')),
    CONSTRAINT CK_REQ_HDR_FLAGS CHECK (SOURCED_FLG IN ('Y', 'N') AND CANCELLED_FLG IN ('Y', 'N')),
    CONSTRAINT CK_REQ_HDR_REJECT CHECK (
        REQ_STATUS_CD <> 'REJT' OR REJECTION_REASON_CD IS NOT NULL)
)
TABLESPACE WWI_DATA
PCTFREE 10
/

CREATE INDEX WWI_PROC.IX_REQ_HDR_STATUS
    ON WWI_PROC.REQUISITION_HDR (REQ_STATUS_CD, REGION_CD, REQUEST_DT) TABLESPACE WWI_IDX
/

CREATE INDEX WWI_PROC.IX_REQ_HDR_COST_CENTER
    ON WWI_PROC.REQUISITION_HDR (COST_CENTER_CD, REQUEST_DT) TABLESPACE WWI_IDX
/

COMMENT ON COLUMN WWI_PROC.REQUISITION_HDR.URGENCY_CD IS
    'EMG bypasses approver 1 and 2 in NA only; EU and APAC still require both.'
/
