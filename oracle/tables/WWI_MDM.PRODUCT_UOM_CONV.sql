/* =====================================================================
 * Object       : TABLE WWI_MDM.PRODUCT_UOM_CONV
 * Schema       : WWI_MDM (Oracle ERP - WWIGERP)
 * Deploy order : 35
 * Depends on   : WWI_MDM.PRODUCT_MASTER, WWI_REF.UOM_REF
 * Called by    : PKG_PRODUCT_MASTER (FN_CONVERT_UOM path), receiving and invoicing
 *
 * Item-specific unit-of-measure conversions. A global conversion set exists in
 * WWI_REF.UOM_REF, but item overrides are common (a 'CASE' is not the same
 * number of eaches for every item). Conversions are stored one-directional and
 * the reverse is calculated, which loses precision on some cases-to-eaches
 * factors.
 * ===================================================================== */

CREATE SEQUENCE WWI_MDM.SEQ_PRODUCT_UOM_CONV
    START WITH 30001 INCREMENT BY 1 CACHE 20 NOCYCLE
/

CREATE TABLE WWI_MDM.PRODUCT_UOM_CONV
(
    PRODUCT_UOM_CONV_ID     NUMBER(12)      NOT NULL,
    PRODUCT_ID              NUMBER(12)      NOT NULL,
    FROM_UOM_CD             VARCHAR2(4)     NOT NULL,
    TO_UOM_CD               VARCHAR2(4)     NOT NULL,
    CONV_FACTOR             NUMBER(18,8)    NOT NULL,
    ROUNDING_RULE_CD        VARCHAR2(4)     DEFAULT 'HALF' NOT NULL,
    DECIMAL_PRECISION_NBR   NUMBER(1)       DEFAULT 3 NOT NULL,
    PRIMARY_CONV_FLG        VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    EFFECTIVE_DT            DATE            DEFAULT SYSDATE NOT NULL,
    END_DT                  DATE,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_PRODUCT_UOM_CONV PRIMARY KEY (PRODUCT_UOM_CONV_ID) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_PRODUCT_UOM_CONV UNIQUE (PRODUCT_ID, FROM_UOM_CD, TO_UOM_CD, EFFECTIVE_DT)
        USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_PRODUCT_UOM_FACTOR CHECK (CONV_FACTOR > 0),
    CONSTRAINT CK_PRODUCT_UOM_DIFF CHECK (FROM_UOM_CD <> TO_UOM_CD),
    CONSTRAINT CK_PRODUCT_UOM_ROUND CHECK (ROUNDING_RULE_CD IN ('HALF', 'UP', 'DOWN', 'TRUNC')),
    CONSTRAINT CK_PRODUCT_UOM_PRIMARY CHECK (PRIMARY_CONV_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_DATA
/

ALTER TABLE WWI_MDM.PRODUCT_UOM_CONV ADD CONSTRAINT FK_PRODUCT_UOM_PRODUCT
    FOREIGN KEY (PRODUCT_ID) REFERENCES WWI_MDM.PRODUCT_MASTER (PRODUCT_ID)
/

CREATE INDEX WWI_MDM.IX_PRODUCT_UOM_LOOKUP
    ON WWI_MDM.PRODUCT_UOM_CONV (PRODUCT_ID, FROM_UOM_CD) TABLESPACE WWI_IDX
/
