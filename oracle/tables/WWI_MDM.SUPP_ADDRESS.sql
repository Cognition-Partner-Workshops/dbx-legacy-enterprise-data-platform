/* =====================================================================
 * Object       : TABLE WWI_MDM.SUPP_ADDRESS
 * Schema       : WWI_MDM (Oracle ERP - WWIGERP)
 * Deploy order : 28
 * Depends on   : WWI_MDM.SUPP_MASTER, WWI_REF.COUNTRY_REF
 * Called by    : PKG_SUPPLIER_MASTER, SSIS EXT_ORA_SupplierMaster
 *
 * Supplier sites. ORDER_FROM and REMIT_TO are different rows, and AP matches
 * an invoice to the REMIT_TO site while procurement raises the PO against the
 * ORDER_FROM site; when the two disagree the invoice goes on a site hold.
 * SITE_CD is the value printed on the PO and is unique per supplier only -
 * it repeats across suppliers.
 * ===================================================================== */

CREATE SEQUENCE WWI_MDM.SEQ_SUPP_ADDRESS
    START WITH 950001 INCREMENT BY 1 CACHE 20 NOCYCLE
/

CREATE TABLE WWI_MDM.SUPP_ADDRESS
(
    SUPP_ADDR_ID            NUMBER(12)      NOT NULL,
    SUPP_ID                 NUMBER(12)      NOT NULL,
    SITE_CD                 VARCHAR2(10)    NOT NULL,
    SITE_TYPE_CD            VARCHAR2(6)     NOT NULL,
    SITE_NAME               VARCHAR2(80),
    ADDR_LINE_1             VARCHAR2(80)    NOT NULL,
    ADDR_LINE_2             VARCHAR2(80),
    ADDR_LINE_3             VARCHAR2(80),
    CITY_TXT                VARCHAR2(60),
    STATE_PROV_CD           VARCHAR2(6),
    POSTAL_CD               VARCHAR2(12),
    COUNTRY_CD              VARCHAR2(2)     NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    TIME_ZONE_TXT           VARCHAR2(40),
    INCOTERM_CD             VARCHAR2(3),
    SHIP_VIA_CD             VARCHAR2(8),
    CARRIER_ACCOUNT_NBR     VARCHAR2(24),
    CUSTOMS_BROKER_TXT      VARCHAR2(120),
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    PRIMARY_FLG             VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_SUPP_ADDRESS PRIMARY KEY (SUPP_ADDR_ID) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_SUPP_ADDRESS_SITE UNIQUE (SUPP_ID, SITE_CD) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_SUPP_ADDRESS_TYPE CHECK (
        SITE_TYPE_CD IN ('ORDER', 'REMIT', 'RETURN', 'LEGAL', 'PICKUP')),
    CONSTRAINT CK_SUPP_ADDRESS_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC')),
    CONSTRAINT CK_SUPP_ADDRESS_FLAGS CHECK (ACTIVE_FLG IN ('Y', 'N') AND PRIMARY_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_DATA
/

ALTER TABLE WWI_MDM.SUPP_ADDRESS ADD CONSTRAINT FK_SUPP_ADDRESS_SUPP
    FOREIGN KEY (SUPP_ID) REFERENCES WWI_MDM.SUPP_MASTER (SUPP_ID)
/

CREATE INDEX WWI_MDM.IX_SUPP_ADDRESS_TYPE
    ON WWI_MDM.SUPP_ADDRESS (SUPP_ID, SITE_TYPE_CD, ACTIVE_FLG) TABLESPACE WWI_IDX
/

CREATE INDEX WWI_MDM.IX_SUPP_ADDRESS_COUNTRY
    ON WWI_MDM.SUPP_ADDRESS (COUNTRY_CD, POSTAL_CD) TABLESPACE WWI_IDX
/
