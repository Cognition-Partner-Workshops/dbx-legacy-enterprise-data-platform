/* =====================================================================
 * Object       : TABLE WWI_REF.UOM_REF
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 26
 * Depends on   : oracle/ddl/02_create_schemas.sql
 * Called by    : PKG_PURCHASE_ORDER, PKG_RECEIPTS, product costing
 *
 * Units of measure with conversion to a base unit within a UOM class. Cross
 * class conversion (weight to volume) is not supported and the two places that
 * need it hard-code a factor. The imperial/metric split is a real source of
 * divergence: NA purchases in EA/CS/LB, EU and APAC in EA/CS/KG.
 * ===================================================================== */

CREATE TABLE WWI_REF.UOM_REF
(
    UOM_CD                  VARCHAR2(4)     NOT NULL,
    UOM_NAME                VARCHAR2(60)    NOT NULL,
    UOM_CLASS_CD            VARCHAR2(8)     NOT NULL,
    BASE_UOM_CD             VARCHAR2(4)     NOT NULL,
    CONVERSION_FACTOR       NUMBER(18,8)    DEFAULT 1 NOT NULL,
    MEASUREMENT_SYSTEM_CD   VARCHAR2(8)     DEFAULT 'BOTH' NOT NULL,
    PRECISION_DIGITS        NUMBER(1)       DEFAULT 2 NOT NULL,
    ISO_UOM_CD              VARCHAR2(4),
    LEGACY_UOM_CD           VARCHAR2(4),
    PURCHASING_ALLOWED_FLG  VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    INVENTORY_ALLOWED_FLG   VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_UOM_REF PRIMARY KEY (UOM_CD) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_UOM_CLASS CHECK (
        UOM_CLASS_CD IN ('COUNT', 'WEIGHT', 'VOLUME', 'LENGTH', 'TIME', 'AREA')),
    CONSTRAINT CK_UOM_SYSTEM CHECK (MEASUREMENT_SYSTEM_CD IN ('METRIC', 'IMPER', 'BOTH')),
    CONSTRAINT CK_UOM_FACTOR CHECK (CONVERSION_FACTOR > 0),
    CONSTRAINT CK_UOM_FLAGS CHECK (
        PURCHASING_ALLOWED_FLG IN ('Y', 'N') AND INVENTORY_ALLOWED_FLG IN ('Y', 'N')
        AND ACTIVE_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_REF_DATA
/

CREATE INDEX WWI_REF.IX_UOM_CLASS
    ON WWI_REF.UOM_REF (UOM_CLASS_CD, BASE_UOM_CD) TABLESPACE WWI_IDX
/
