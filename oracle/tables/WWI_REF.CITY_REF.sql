/* =====================================================================
 * Object       : TABLE WWI_REF.CITY_REF
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 24
 * Depends on   : WWI_REF.COUNTRY_REF
 * Called by    : Address standardisation, V_GEOGRAPHY_EXTRACT
 *
 * City reference used to standardise the denormalised address blocks in
 * WWI_MDM. Match quality is poor for APAC because the source list is
 * Latin-script only and the ERP stores local-script city names for JP and KR
 * addresses.
 * ===================================================================== */

CREATE SEQUENCE WWI_REF.SEQ_CITY_REF
    START WITH 100001 INCREMENT BY 1 CACHE 100 NOCYCLE
/

CREATE TABLE WWI_REF.CITY_REF
(
    CITY_ID                 NUMBER(12)      NOT NULL,
    CITY_NAME               VARCHAR2(80)    NOT NULL,
    CITY_NAME_LOCAL         VARCHAR2(120),
    CITY_NAME_UPPER         VARCHAR2(80)    NOT NULL,
    STATE_PROV_CD           VARCHAR2(6),
    STATE_PROV_NAME         VARCHAR2(80),
    PREFECTURE_TXT          VARCHAR2(60),
    COUNTY_TXT              VARCHAR2(60),
    COUNTRY_CD              VARCHAR2(2)     NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    LATITUDE                NUMBER(9,6),
    LONGITUDE               NUMBER(9,6),
    POPULATION_CNT          NUMBER(10),
    TIMEZONE_TXT            VARCHAR2(40),
    METRO_AREA_CD           VARCHAR2(10),
    TAX_JURISDICTION_CD     VARCHAR2(12),
    ALIAS_OF_CITY_ID        NUMBER(12),
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_CITY_REF PRIMARY KEY (CITY_ID) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_CITY_REF UNIQUE (COUNTRY_CD, STATE_PROV_CD, CITY_NAME_UPPER)
        USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_CITY_ACTIVE CHECK (ACTIVE_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_REF_DATA
/

ALTER TABLE WWI_REF.CITY_REF ADD CONSTRAINT FK_CITY_COUNTRY
    FOREIGN KEY (COUNTRY_CD) REFERENCES WWI_REF.COUNTRY_REF (COUNTRY_CD)
/

CREATE INDEX WWI_REF.IX_CITY_NAME_MATCH
    ON WWI_REF.CITY_REF (UPPER(TRIM(CITY_NAME)), COUNTRY_CD) TABLESPACE WWI_IDX
/
