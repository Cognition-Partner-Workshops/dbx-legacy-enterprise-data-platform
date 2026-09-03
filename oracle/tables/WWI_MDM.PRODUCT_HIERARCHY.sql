/* =====================================================================
 * Object       : TABLE WWI_MDM.PRODUCT_HIERARCHY
 * Schema       : WWI_MDM (Oracle ERP - WWIGERP)
 * Deploy order : 34
 * Depends on   : WWI_MDM.PRODUCT_MASTER, WWI_MDM.PRODUCT_CATEGORY
 * Called by    : PKG_PRODUCT_MASTER, V_PRODUCT_HIERARCHY_FLAT
 *
 * Alternate product groupings that do not fit the category tree: the reporting
 * hierarchy finance uses, the planning hierarchy supply chain uses, and the
 * web merchandising hierarchy. An item may sit in all three at once and the
 * levels are not aligned across them.
 * ===================================================================== */

CREATE SEQUENCE WWI_MDM.SEQ_PRODUCT_HIERARCHY
    START WITH 20001 INCREMENT BY 1 CACHE 20 NOCYCLE
/

CREATE TABLE WWI_MDM.PRODUCT_HIERARCHY
(
    PRODUCT_HIER_ID         NUMBER(12)      NOT NULL,
    HIER_TYPE_CD            VARCHAR2(6)     NOT NULL,
    PRODUCT_ID              NUMBER(12)      NOT NULL,
    LEVEL_1_CD              VARCHAR2(12)    NOT NULL,
    LEVEL_1_NAME            VARCHAR2(80),
    LEVEL_2_CD              VARCHAR2(12),
    LEVEL_2_NAME            VARCHAR2(80),
    LEVEL_3_CD              VARCHAR2(12),
    LEVEL_3_NAME            VARCHAR2(80),
    LEVEL_4_CD              VARCHAR2(12),
    LEVEL_4_NAME            VARCHAR2(80),
    PLANNER_CD              VARCHAR2(8),
    BUYER_CD                VARCHAR2(8),
    EFFECTIVE_DT            DATE            DEFAULT SYSDATE NOT NULL,
    END_DT                  DATE,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_PRODUCT_HIERARCHY PRIMARY KEY (PRODUCT_HIER_ID) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_PRODUCT_HIER UNIQUE (HIER_TYPE_CD, PRODUCT_ID, EFFECTIVE_DT)
        USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_PRODUCT_HIER_TYPE CHECK (HIER_TYPE_CD IN ('FIN', 'PLAN', 'WEB'))
)
TABLESPACE WWI_DATA
/

ALTER TABLE WWI_MDM.PRODUCT_HIERARCHY ADD CONSTRAINT FK_PRODUCT_HIER_PRODUCT
    FOREIGN KEY (PRODUCT_ID) REFERENCES WWI_MDM.PRODUCT_MASTER (PRODUCT_ID)
/

CREATE INDEX WWI_MDM.IX_PRODUCT_HIER_LEVELS
    ON WWI_MDM.PRODUCT_HIERARCHY (HIER_TYPE_CD, LEVEL_1_CD, LEVEL_2_CD, LEVEL_3_CD)
    TABLESPACE WWI_IDX
/
