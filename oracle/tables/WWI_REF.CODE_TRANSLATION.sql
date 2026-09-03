/* =====================================================================
 * Object       : TABLE WWI_REF.CODE_TRANSLATION
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 34
 * Depends on   : WWI_REF.SOURCE_SYSTEM_REF, WWI_REF.STATUS_CODE_REF
 * Called by    : Every inbound interface, extract views, PKG_RECEIPTS tolerances
 *
 * Generic cross-reference between external code sets and the ERP's own codes,
 * and - by accretion - the place operational parameters ended up too. The
 * receipt tolerance percentages, the AP match tolerance and a handful of
 * regional switches are all stored here as TARGET_VALUE_TXT because adding a
 * configuration table required a change board and adding a row did not.
 * ===================================================================== */

CREATE SEQUENCE WWI_REF.SEQ_CODE_TRANSLATION
    START WITH 40001 INCREMENT BY 1 CACHE 20 NOCYCLE
/

CREATE TABLE WWI_REF.CODE_TRANSLATION
(
    TRANSLATION_ID          NUMBER(12)      NOT NULL,
    CODE_SET_CD             VARCHAR2(20)    NOT NULL,
    SOURCE_SYS_CD           VARCHAR2(12)    NOT NULL,
    SOURCE_VALUE_TXT        VARCHAR2(60)    NOT NULL,
    TARGET_VALUE_TXT        VARCHAR2(200)   NOT NULL,
    REGION_CD               VARCHAR2(4)     DEFAULT 'ALL' NOT NULL,
    ENTITY_CD               VARCHAR2(20),
    VALUE_TYPE_CD           VARCHAR2(8)     DEFAULT 'CODE' NOT NULL,
    DESCRIPTION_TXT         VARCHAR2(400),
    EFFECTIVE_FROM_DT       DATE            DEFAULT SYSDATE NOT NULL,
    EFFECTIVE_TO_DT         DATE,
    PRIORITY_NBR            NUMBER(3)       DEFAULT 100 NOT NULL,
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_CODE_TRANSLATION PRIMARY KEY (TRANSLATION_ID) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_CODE_TRANSLATION UNIQUE (CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, REGION_CD, EFFECTIVE_FROM_DT)
        USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_CODE_XLAT_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC', 'ALL')),
    CONSTRAINT CK_CODE_XLAT_TYPE CHECK (VALUE_TYPE_CD IN ('CODE', 'NUMBER', 'FLAG', 'TEXT', 'DATE')),
    CONSTRAINT CK_CODE_XLAT_ACTIVE CHECK (ACTIVE_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_REF_DATA
/

ALTER TABLE WWI_REF.CODE_TRANSLATION ADD CONSTRAINT FK_CODE_XLAT_SOURCE_SYS
    FOREIGN KEY (SOURCE_SYS_CD) REFERENCES WWI_REF.SOURCE_SYSTEM_REF (SOURCE_SYS_CD)
/

CREATE INDEX WWI_REF.IX_CODE_XLAT_LOOKUP
    ON WWI_REF.CODE_TRANSLATION (CODE_SET_CD, UPPER(SOURCE_VALUE_TXT)) TABLESPACE WWI_IDX
/

CREATE INDEX WWI_REF.IX_CODE_XLAT_ENTITY
    ON WWI_REF.CODE_TRANSLATION (ENTITY_CD, REGION_CD, ACTIVE_FLG) TABLESPACE WWI_IDX
/
