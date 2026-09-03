/* =====================================================================
 * Object       : TABLE WWI_REF.COUNTRY_REF
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 21
 * Depends on   : WWI_REF.REGION_REF
 * Called by    : Address standardisation, PKG_TAX_CALC, geography extract
 *
 * Country reference. Both ISO alpha-2 and alpha-3 are carried, plus the
 * three-digit legacy numeric code the 1998 mainframe used, because two feeder
 * interfaces still send it. EU membership and EU VAT-area membership are
 * separate flags because they are not the same set.
 * ===================================================================== */

CREATE TABLE WWI_REF.COUNTRY_REF
(
    COUNTRY_CD              VARCHAR2(2)     NOT NULL,
    COUNTRY_CD_3            VARCHAR2(3)     NOT NULL,
    COUNTRY_NUM_CD          VARCHAR2(3),
    LEGACY_MAINFRAME_CD     VARCHAR2(3),
    COUNTRY_NAME            VARCHAR2(80)    NOT NULL,
    OFFICIAL_NAME           VARCHAR2(160),
    REGION_CD               VARCHAR2(4)     NOT NULL,
    SUB_REGION_TXT          VARCHAR2(60),
    DEFAULT_CURR_CD         VARCHAR2(3)     NOT NULL,
    DEFAULT_LANGUAGE_CD     VARCHAR2(5),
    EU_MEMBER_FLG           VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    EU_VAT_AREA_FLG         VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    VAT_NBR_FORMAT_TXT      VARCHAR2(60),
    POSTAL_FORMAT_TXT       VARCHAR2(60),
    POSTAL_REQUIRED_FLG     VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    STATE_PROV_REQUIRED_FLG VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    PHONE_PREFIX_CD         VARCHAR2(6),
    SANCTIONED_FLG          VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    TRADE_BLOC_CD           VARCHAR2(8),
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_COUNTRY_REF PRIMARY KEY (COUNTRY_CD) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_COUNTRY_CD_3 UNIQUE (COUNTRY_CD_3) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_COUNTRY_FLAGS CHECK (
        EU_MEMBER_FLG IN ('Y', 'N') AND EU_VAT_AREA_FLG IN ('Y', 'N')
        AND POSTAL_REQUIRED_FLG IN ('Y', 'N') AND STATE_PROV_REQUIRED_FLG IN ('Y', 'N')
        AND SANCTIONED_FLG IN ('Y', 'N') AND ACTIVE_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_REF_DATA
/

ALTER TABLE WWI_REF.COUNTRY_REF ADD CONSTRAINT FK_COUNTRY_REGION
    FOREIGN KEY (REGION_CD) REFERENCES WWI_REF.REGION_REF (REGION_CD)
/

CREATE INDEX WWI_REF.IX_COUNTRY_REGION
    ON WWI_REF.COUNTRY_REF (REGION_CD, ACTIVE_FLG) TABLESPACE WWI_IDX
/

CREATE INDEX WWI_REF.IX_COUNTRY_NAME
    ON WWI_REF.COUNTRY_REF (UPPER(COUNTRY_NAME)) TABLESPACE WWI_IDX
/
