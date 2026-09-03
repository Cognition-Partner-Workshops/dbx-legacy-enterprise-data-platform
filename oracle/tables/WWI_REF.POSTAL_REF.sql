/* =====================================================================
 * Object       : TABLE WWI_REF.POSTAL_REF
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 25
 * Depends on   : WWI_REF.CITY_REF, WWI_REF.COUNTRY_REF
 * Called by    : Address standardisation, tax jurisdiction assignment
 *
 * Postal code reference. The three regions store fundamentally different
 * things here: NA five-digit ZIP plus optional ZIP+4 with a county for sales
 * tax, EU alphanumeric codes of varying length per member state, APAC codes
 * that in JP map to a chome-level district. POSTAL_CD_NORM holds the
 * whitespace- and case-normalised form used for joins.
 * ===================================================================== */

CREATE SEQUENCE WWI_REF.SEQ_POSTAL_REF
    START WITH 500001 INCREMENT BY 1 CACHE 200 NOCYCLE
/

CREATE TABLE WWI_REF.POSTAL_REF
(
    POSTAL_ID               NUMBER(12)      NOT NULL,
    POSTAL_CD               VARCHAR2(12)    NOT NULL,
    POSTAL_CD_NORM          VARCHAR2(12)    NOT NULL,
    COUNTRY_CD              VARCHAR2(2)     NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    CITY_ID                 NUMBER(12),
    CITY_NAME_TXT           VARCHAR2(80),
    STATE_PROV_CD           VARCHAR2(6),
    COUNTY_TXT              VARCHAR2(60),
    DISTRICT_TXT            VARCHAR2(80),
    NA_ZIP4_FROM            VARCHAR2(4),
    NA_ZIP4_TO              VARCHAR2(4),
    NA_COUNTY_FIPS_CD       VARCHAR2(5),
    EU_NUTS_CD              VARCHAR2(8),
    APAC_DISTRICT_CD        VARCHAR2(12),
    TAX_JURISDICTION_CD     VARCHAR2(20),
    DELIVERY_POINT_CNT      NUMBER(8),
    PO_BOX_ONLY_FLG         VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    LAST_VERIFIED_DT        DATE,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_POSTAL_REF PRIMARY KEY (POSTAL_ID) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_POSTAL_REF UNIQUE (COUNTRY_CD, POSTAL_CD_NORM) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_POSTAL_FLAGS CHECK (PO_BOX_ONLY_FLG IN ('Y', 'N') AND ACTIVE_FLG IN ('Y', 'N')),
    CONSTRAINT CK_POSTAL_NA_ZIP CHECK (
        REGION_CD <> 'NA' OR LENGTH(POSTAL_CD_NORM) = 5)
)
TABLESPACE WWI_REF_DATA
/

ALTER TABLE WWI_REF.POSTAL_REF ADD CONSTRAINT FK_POSTAL_CITY
    FOREIGN KEY (CITY_ID) REFERENCES WWI_REF.CITY_REF (CITY_ID)
/

CREATE INDEX WWI_REF.IX_POSTAL_JURISDICTION
    ON WWI_REF.POSTAL_REF (TAX_JURISDICTION_CD) TABLESPACE WWI_IDX
/
