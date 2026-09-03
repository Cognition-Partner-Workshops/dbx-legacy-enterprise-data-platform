/* =====================================================================
 * Object       : TABLE WWI_MDM.CUST_ADDRESS
 * Schema       : WWI_MDM (Oracle ERP - WWIGERP)
 * Deploy order : 21
 * Depends on   : WWI_MDM.CUST_MASTER, WWI_REF.COUNTRY_REF, WWI_REF.POSTAL_REF
 * Called by    : PKG_CUSTOMER_MASTER, V_CUSTOMER_ADDRESS_CURRENT, SSIS EXT_ORA_CustomerAddress
 *
 * Denormalised address block, one row per address type per validity window.
 * The three regions never agreed on a postal model: NA uses STATE_PROV_CD plus
 * ZIP5/ZIP4, EU uses POSTAL_CD with a country-specific format and a separate
 * COUNTY_TXT, APAC uses PREFECTURE_TXT and frequently leaves POSTAL_CD null.
 * ADDR_LINE_4 was added for Japan and is used for building/floor everywhere
 * else. Effective-dated rows are closed by setting VALID_TO_DT; there is no
 * constraint preventing overlapping windows and there are known overlaps.
 * ===================================================================== */

CREATE SEQUENCE WWI_MDM.SEQ_CUST_ADDRESS
    START WITH 500001 INCREMENT BY 1 CACHE 20 NOCYCLE
/

CREATE TABLE WWI_MDM.CUST_ADDRESS
(
    CUST_ADDR_ID            NUMBER(12)      NOT NULL,
    CUST_ID                 NUMBER(12)      NOT NULL,
    ADDR_TYPE_CD            VARCHAR2(4)     NOT NULL,
    ADDR_SEQ_NBR            NUMBER(3)       DEFAULT 1 NOT NULL,
    ADDR_LINE_1             VARCHAR2(80)    NOT NULL,
    ADDR_LINE_2             VARCHAR2(80),
    ADDR_LINE_3             VARCHAR2(80),
    ADDR_LINE_4             VARCHAR2(80),
    CITY_TXT                VARCHAR2(60),
    COUNTY_TXT              VARCHAR2(60),
    STATE_PROV_CD           VARCHAR2(6),
    PREFECTURE_TXT          VARCHAR2(60),
    POSTAL_CD               VARCHAR2(12),
    POSTAL_CD_NORM          VARCHAR2(12),
    ZIP4_CD                 VARCHAR2(4),
    COUNTRY_CD              VARCHAR2(2)     NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    GEO_LAT                 NUMBER(9,6),
    GEO_LON                 NUMBER(9,6),
    ADDR_VERIFIED_FLG       VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    ADDR_VERIFIED_DT        DATE,
    ADDR_VERIFY_VENDOR_CD   VARCHAR2(8),
    PRIMARY_FLG             VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    VALID_FROM_DT           DATE            DEFAULT SYSDATE NOT NULL,
    VALID_TO_DT             DATE,
    DELETED_FLG             VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_CUST_ADDRESS PRIMARY KEY (CUST_ADDR_ID) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_CUST_ADDRESS_SEQ UNIQUE (CUST_ID, ADDR_TYPE_CD, ADDR_SEQ_NBR, VALID_FROM_DT)
        USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_CUST_ADDRESS_TYPE CHECK (ADDR_TYPE_CD IN ('BILL', 'SHIP', 'STMT', 'LEGL', 'DLVR')),
    CONSTRAINT CK_CUST_ADDRESS_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC')),
    CONSTRAINT CK_CUST_ADDRESS_FLAGS CHECK (
        ADDR_VERIFIED_FLG IN ('Y', 'N') AND PRIMARY_FLG IN ('Y', 'N') AND DELETED_FLG IN ('Y', 'N')),
    CONSTRAINT CK_CUST_ADDRESS_NA_STATE CHECK (
        REGION_CD <> 'NA' OR STATE_PROV_CD IS NOT NULL)
)
TABLESPACE WWI_DATA
PCTFREE 10
/

ALTER TABLE WWI_MDM.CUST_ADDRESS ADD CONSTRAINT FK_CUST_ADDRESS_CUST
    FOREIGN KEY (CUST_ID) REFERENCES WWI_MDM.CUST_MASTER (CUST_ID)
/

CREATE INDEX WWI_MDM.IX_CUST_ADDRESS_CUST
    ON WWI_MDM.CUST_ADDRESS (CUST_ID, ADDR_TYPE_CD) TABLESPACE WWI_IDX
/

CREATE INDEX WWI_MDM.IX_CUST_ADDRESS_POSTAL
    ON WWI_MDM.CUST_ADDRESS (COUNTRY_CD, POSTAL_CD_NORM) TABLESPACE WWI_IDX
/

CREATE INDEX WWI_MDM.IX_CUST_ADDRESS_CURRENT
    ON WWI_MDM.CUST_ADDRESS (CUST_ID, NVL(VALID_TO_DT, TO_DATE('31-DEC-4712', 'DD-MON-YYYY')))
    TABLESPACE WWI_IDX
/

COMMENT ON COLUMN WWI_MDM.CUST_ADDRESS.POSTAL_CD_NORM IS
    'Region-specific normalisation: NA = ZIP5, EU = uppercase no-space, APAC = digits only.'
/
COMMENT ON COLUMN WWI_MDM.CUST_ADDRESS.ADDR_LINE_4 IS
    'Added for Japanese addresses; used for building/floor in the other regions.'
/
