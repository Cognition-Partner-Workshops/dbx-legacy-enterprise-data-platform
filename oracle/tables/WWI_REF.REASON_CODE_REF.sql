/* =====================================================================
 * Object       : TABLE WWI_REF.REASON_CODE_REF
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 31
 * Depends on   : WWI_REF.STATUS_CODE_REF
 * Called by    : Returns, holds, cancellations, journal reversals
 *
 * Reason codes by entity. The NA, EU and APAC teams maintain their own sets
 * under the same entity codes, so a reason code is only unique per region and
 * the tables that store one without a region cannot resolve it reliably.
 * ===================================================================== */

CREATE TABLE WWI_REF.REASON_CODE_REF
(
    ENTITY_CD               VARCHAR2(20)    NOT NULL,
    REASON_CD               VARCHAR2(8)     NOT NULL,
    REGION_CD               VARCHAR2(4)     DEFAULT 'ALL' NOT NULL,
    REASON_NAME             VARCHAR2(80)    NOT NULL,
    REASON_DESC             VARCHAR2(400),
    REASON_CATEGORY_CD      VARCHAR2(10),
    REQUIRES_COMMENT_FLG    VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    REQUIRES_APPROVAL_FLG   VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    APPROVAL_LEVEL_NBR      NUMBER(2),
    FINANCIAL_IMPACT_FLG    VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    SUPPLIER_FAULT_FLG      VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    KPI_EXCLUDE_FLG         VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    DISPLAY_SEQ_NBR         NUMBER(4),
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_REASON_CODE_REF PRIMARY KEY (ENTITY_CD, REASON_CD, REGION_CD)
        USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_REASON_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC', 'ALL')),
    CONSTRAINT CK_REASON_FLAGS CHECK (
        REQUIRES_COMMENT_FLG IN ('Y', 'N') AND REQUIRES_APPROVAL_FLG IN ('Y', 'N')
        AND FINANCIAL_IMPACT_FLG IN ('Y', 'N') AND SUPPLIER_FAULT_FLG IN ('Y', 'N')
        AND KPI_EXCLUDE_FLG IN ('Y', 'N') AND ACTIVE_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_REF_DATA
/

CREATE INDEX WWI_REF.IX_REASON_CATEGORY
    ON WWI_REF.REASON_CODE_REF (REASON_CATEGORY_CD, ACTIVE_FLG) TABLESPACE WWI_IDX
/
