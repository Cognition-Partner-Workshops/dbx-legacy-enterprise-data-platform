/* =====================================================================
 * Object       : TABLE WWI_FIN.COST_CENTER
 * Schema       : WWI_FIN (Oracle ERP - WWIGERP)
 * Deploy order : 61
 * Depends on   : WWI_FIN.GL_ACCOUNT
 * Called by    : PKG_GL_POST, PKG_AP_INVOICE, cost allocation batch
 *
 * Cost centres, with their own hierarchy and a manager code that is not a
 * foreign key to anything because HR lives in a separate system. Closed cost
 * centres are still posted to by the allocation batch, which is why
 * CLOSED_DT is advisory rather than enforced.
 * ===================================================================== */

CREATE SEQUENCE WWI_FIN.SEQ_COST_CENTER
    START WITH 5001 INCREMENT BY 1 NOCACHE NOCYCLE
/

CREATE TABLE WWI_FIN.COST_CENTER
(
    COST_CENTER_ID          NUMBER(12)      NOT NULL,
    COST_CENTER_CD          VARCHAR2(10)    NOT NULL,
    COST_CENTER_NAME        VARCHAR2(120)   NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    COUNTRY_CD              VARCHAR2(2),
    LEGAL_ENTITY_CD         VARCHAR2(6)     NOT NULL,
    PARENT_COST_CENTER_CD   VARCHAR2(10),
    HIERARCHY_LEVEL_NBR     NUMBER(2)       DEFAULT 1 NOT NULL,
    MANAGER_CD              VARCHAR2(8),
    MANAGER_NAME_TXT        VARCHAR2(120),
    DEFAULT_GL_ACCOUNT_CD   VARCHAR2(30),
    FUNCTIONAL_CURR_CD      VARCHAR2(3)     NOT NULL,
    BUDGET_OWNER_CD         VARCHAR2(8),
    ANNUAL_BUDGET_AMT       NUMBER(15,5),
    BUDGET_FY_CD            VARCHAR2(6),
    APPROVAL_LIMIT_AMT      NUMBER(15,5),
    ALLOCATION_BASIS_CD     VARCHAR2(6),
    SITE_CD                 VARCHAR2(8),
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    OPENED_DT               DATE,
    CLOSED_DT               DATE,
    LEGACY_DEPT_CD          VARCHAR2(6),
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_COST_CENTER PRIMARY KEY (COST_CENTER_ID) USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT UK_COST_CENTER_CD UNIQUE (COST_CENTER_CD) USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT CK_COST_CENTER_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC')),
    CONSTRAINT CK_COST_CENTER_ACTIVE CHECK (ACTIVE_FLG IN ('Y', 'N')),
    CONSTRAINT CK_COST_CENTER_DATES CHECK (CLOSED_DT IS NULL OR OPENED_DT IS NULL OR CLOSED_DT >= OPENED_DT)
)
TABLESPACE WWI_FIN_DATA
/

CREATE INDEX WWI_FIN.IX_COST_CENTER_PARENT
    ON WWI_FIN.COST_CENTER (PARENT_COST_CENTER_CD) TABLESPACE WWI_FIN_IDX
/

CREATE INDEX WWI_FIN.IX_COST_CENTER_ENTITY
    ON WWI_FIN.COST_CENTER (LEGAL_ENTITY_CD, ACTIVE_FLG) TABLESPACE WWI_FIN_IDX
/
