/* =====================================================================
 * Object       : TABLE WWI_REF.INCOTERM_REF
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 28
 * Depends on   : oracle/ddl/02_create_schemas.sql
 * Called by    : PKG_PURCHASE_ORDER, landed-cost calculation
 *
 * Incoterms. Both the 2010 and 2020 revisions are present and active at the
 * same time because open contracts still cite the 2010 terms; the revision is
 * part of the natural key but not of the code column that POs actually store,
 * so a PO with 'DAT' is only unambiguous by its order date.
 * ===================================================================== */

CREATE TABLE WWI_REF.INCOTERM_REF
(
    INCOTERM_CD             VARCHAR2(3)     NOT NULL,
    REVISION_YEAR_NBR       NUMBER(4)       NOT NULL,
    INCOTERM_NAME           VARCHAR2(80)    NOT NULL,
    DESCRIPTION_TXT         VARCHAR2(400),
    TRANSPORT_MODE_CD       VARCHAR2(6)     NOT NULL,
    FREIGHT_PAID_BY_CD      VARCHAR2(6)     NOT NULL,
    INSURANCE_PAID_BY_CD    VARCHAR2(6),
    RISK_TRANSFER_POINT_TXT VARCHAR2(200),
    EXPORT_CLEARANCE_BY_CD  VARCHAR2(6),
    IMPORT_CLEARANCE_BY_CD  VARCHAR2(6),
    LANDED_COST_INCLUDE_FLG VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    SUPERSEDED_BY_CD        VARCHAR2(3),
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_INCOTERM_REF PRIMARY KEY (INCOTERM_CD, REVISION_YEAR_NBR)
        USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_INCOTERM_MODE CHECK (TRANSPORT_MODE_CD IN ('ANY', 'SEA', 'AIR', 'ROAD')),
    CONSTRAINT CK_INCOTERM_FREIGHT CHECK (FREIGHT_PAID_BY_CD IN ('BUYER', 'SELLER', 'SPLIT')),
    CONSTRAINT CK_INCOTERM_FLAGS CHECK (
        ACTIVE_FLG IN ('Y', 'N') AND LANDED_COST_INCLUDE_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_REF_DATA
/
