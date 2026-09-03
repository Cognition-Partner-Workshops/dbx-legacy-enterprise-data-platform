/* =====================================================================
 * Object       : TABLE WWI_FIN.GL_ACCOUNT
 * Schema       : WWI_FIN (Oracle ERP - WWIGERP)
 * Deploy order : 60 (first table in WWI_FIN)
 * Depends on   : oracle/ddl/02_create_schemas.sql
 * Called by    : PKG_GL_POST, PKG_AP_INVOICE, V_GL_ACCOUNT_HIERARCHY
 *
 * Chart of accounts. The natural key is a six-segment concatenated string kept
 * in ACCOUNT_CD as well as split into the six segment columns; the two are
 * maintained independently by two different interfaces and disagree on a small
 * number of legacy rows.
 *
 * NA uses the 2003 segment layout (company-cost centre-account-product-
 * intercompany-future), EU adopted a statutory account number in SEGMENT_6
 * during the 2011 IFRS project, APAC still fills SEGMENT_6 with spaces.
 * ===================================================================== */

CREATE SEQUENCE WWI_FIN.SEQ_GL_ACCOUNT
    START WITH 10001 INCREMENT BY 1 NOCACHE NOCYCLE
/

CREATE TABLE WWI_FIN.GL_ACCOUNT
(
    GL_ACCOUNT_ID           NUMBER(12)      NOT NULL,
    ACCOUNT_CD              VARCHAR2(30)    NOT NULL,
    SEGMENT_1_CD            VARCHAR2(4)     NOT NULL,
    SEGMENT_2_CD            VARCHAR2(6),
    SEGMENT_3_CD            VARCHAR2(8)     NOT NULL,
    SEGMENT_4_CD            VARCHAR2(6),
    SEGMENT_5_CD            VARCHAR2(4),
    SEGMENT_6_CD            VARCHAR2(8),
    ACCOUNT_NAME            VARCHAR2(120)   NOT NULL,
    ACCOUNT_TYPE_CD         VARCHAR2(4)     NOT NULL,
    ACCOUNT_CLASS_CD        VARCHAR2(6),
    NORMAL_BALANCE_CD       VARCHAR2(1)     NOT NULL,
    PARENT_ACCOUNT_CD       VARCHAR2(30),
    ROLLUP_LEVEL_NBR        NUMBER(2)       DEFAULT 1 NOT NULL,
    POSTING_ALLOWED_FLG     VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    RECONCILIATION_FLG      VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    INTERCOMPANY_FLG        VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    STATUTORY_ACCOUNT_CD    VARCHAR2(12),
    IFRS_MAPPING_CD         VARCHAR2(12),
    US_GAAP_MAPPING_CD      VARCHAR2(12),
    CURRENCY_RESTRICT_CD    VARCHAR2(3),
    EFFECTIVE_FROM_DT       DATE            DEFAULT SYSDATE NOT NULL,
    EFFECTIVE_TO_DT         DATE,
    ACCOUNT_STATUS_CD       VARCHAR2(4)     DEFAULT 'ACTV' NOT NULL,
    OLD_ACCOUNT_CD          VARCHAR2(20),
    DELETED_FLG             VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_GL_ACCOUNT PRIMARY KEY (GL_ACCOUNT_ID) USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT UK_GL_ACCOUNT_CD UNIQUE (ACCOUNT_CD) USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT CK_GL_ACCOUNT_TYPE CHECK (
        ACCOUNT_TYPE_CD IN ('ASST', 'LIAB', 'EQTY', 'REV', 'EXP', 'STAT')),
    CONSTRAINT CK_GL_ACCOUNT_BALANCE CHECK (NORMAL_BALANCE_CD IN ('D', 'C')),
    CONSTRAINT CK_GL_ACCOUNT_STATUS CHECK (ACCOUNT_STATUS_CD IN ('ACTV', 'INAC', 'CLSD')),
    CONSTRAINT CK_GL_ACCOUNT_FLAGS CHECK (
        POSTING_ALLOWED_FLG IN ('Y', 'N') AND RECONCILIATION_FLG IN ('Y', 'N')
        AND INTERCOMPANY_FLG IN ('Y', 'N') AND DELETED_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_FIN_DATA
PCTFREE 5
/

CREATE INDEX WWI_FIN.IX_GL_ACCOUNT_PARENT
    ON WWI_FIN.GL_ACCOUNT (PARENT_ACCOUNT_CD, ROLLUP_LEVEL_NBR) TABLESPACE WWI_FIN_IDX
/

CREATE INDEX WWI_FIN.IX_GL_ACCOUNT_SEGMENTS
    ON WWI_FIN.GL_ACCOUNT (SEGMENT_1_CD, SEGMENT_3_CD) TABLESPACE WWI_FIN_IDX
/

CREATE INDEX WWI_FIN.IX_GL_ACCOUNT_OLD_CD
    ON WWI_FIN.GL_ACCOUNT (UPPER(TRIM(OLD_ACCOUNT_CD))) TABLESPACE WWI_FIN_IDX
/

COMMENT ON COLUMN WWI_FIN.GL_ACCOUNT.SEGMENT_5_CD IS
    'Originally the intercompany segment. Since 2014 NA reuses it as a legal-entity code.'
/
