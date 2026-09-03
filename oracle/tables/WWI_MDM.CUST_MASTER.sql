/* =====================================================================
 * Object       : TABLE WWI_MDM.CUST_MASTER
 * Schema       : WWI_MDM (Oracle ERP - WWIGERP)
 * Deploy order : 20 (first table in WWI_MDM)
 * Depends on   : oracle/ddl/03_create_schemas.sql; WWI_REF.COUNTRY_REF, WWI_REF.CURRENCY_CODE
 * Called by    : PKG_CUSTOMER_MASTER, V_CUSTOMER_EXTRACT, SSIS EXT_ORA_CustomerMaster
 *
 * Customer master. The oldest table in the estate: the original 1998 columns
 * are the un-prefixed ones, the 2004 CRM merge added the SEGMENT/SOURCE
 * columns, and the 2018 privacy programme bolted on the consent block rather
 * than build a separate table.
 *
 * CUST_NBR is the printed business key ('C-NA-0001234'); LEGACY_CUST_CD is the
 * six-character code from the pre-1998 system that finance still quotes.
 * ACCT_MANAGER_CD has held a team code rather than a person since 2015 - the
 * name was never changed. SPECIAL_INSTR_TXT is free text that the EU billing
 * process parses for the token 'PO-REQUIRED'.
 * ===================================================================== */

CREATE SEQUENCE WWI_MDM.SEQ_CUST_MASTER
    START WITH 100001 INCREMENT BY 1 NOCACHE NOCYCLE ORDER
/

CREATE TABLE WWI_MDM.CUST_MASTER
(
    CUST_ID                 NUMBER(12)      NOT NULL,
    CUST_NBR                VARCHAR2(14)    NOT NULL,
    LEGACY_CUST_CD          VARCHAR2(6),
    CUST_NAME               VARCHAR2(120)   NOT NULL,
    CUST_NAME_ALT           VARCHAR2(120),
    TRADING_NAME            VARCHAR2(120),
    REGION_CD               VARCHAR2(4)     NOT NULL,
    COUNTRY_CD              VARCHAR2(2)     NOT NULL,
    CUST_TYPE_CD            VARCHAR2(4)     NOT NULL,
    CUST_STATUS_CD          VARCHAR2(2)     DEFAULT 'AC' NOT NULL,
    BUYING_GROUP_CD         VARCHAR2(10),
    PRICE_LIST_CD           VARCHAR2(10),
    ACCT_MANAGER_CD         VARCHAR2(8),
    PRIMARY_CURR_CD         VARCHAR2(3)     NOT NULL,
    PAYMENT_TERMS_CD        VARCHAR2(8),
    TAX_REG_NBR             VARCHAR2(24),
    VAT_REG_NBR             VARCHAR2(24),
    GST_REG_NBR             VARCHAR2(24),
    TAX_EXEMPT_FLG          VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    TAX_EXEMPT_CERT_NBR     VARCHAR2(30),
    EDI_ENABLED_FLG         VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    EDI_PARTNER_ID          VARCHAR2(20),
    CREDIT_HOLD_FLG         VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    ON_STOP_REASON_CD       VARCHAR2(4),
    FIRST_ORDER_DT          DATE,
    LAST_ORDER_DT           DATE,
    CONSENT_MARKETING_FLG   VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    CONSENT_CAPTURED_DT     DATE,
    CONSENT_SOURCE_CD       VARCHAR2(8),
    RETENTION_UNTIL_DT      DATE,
    SPECIAL_INSTR_TXT       VARCHAR2(2000),
    MISC_FLAG_1             VARCHAR2(1),
    MISC_FLAG_2             VARCHAR2(1),
    DELETED_FLG             VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_CUST_MASTER PRIMARY KEY (CUST_ID) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_CUST_MASTER_NBR UNIQUE (CUST_NBR) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_CUST_MASTER_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC')),
    CONSTRAINT CK_CUST_MASTER_STATUS CHECK (CUST_STATUS_CD IN ('AC', 'IN', 'PD', 'CL', 'MG')),
    CONSTRAINT CK_CUST_MASTER_TYPE CHECK (CUST_TYPE_CD IN ('RETL', 'WHSL', 'DIST', 'INTC', 'GOVT')),
    CONSTRAINT CK_CUST_MASTER_FLAGS CHECK (
        TAX_EXEMPT_FLG IN ('Y', 'N') AND EDI_ENABLED_FLG IN ('Y', 'N')
        AND CREDIT_HOLD_FLG IN ('Y', 'N') AND CONSENT_MARKETING_FLG IN ('Y', 'N')
        AND DELETED_FLG IN ('Y', 'N')),
    CONSTRAINT CK_CUST_MASTER_EXEMPT CHECK (
        TAX_EXEMPT_FLG = 'N' OR TAX_EXEMPT_CERT_NBR IS NOT NULL)
)
TABLESPACE WWI_DATA
PCTFREE 15
STORAGE (INITIAL 64K NEXT 1M PCTINCREASE 0)
/

CREATE INDEX WWI_MDM.IX_CUST_MASTER_NAME_UPPER
    ON WWI_MDM.CUST_MASTER (UPPER(CUST_NAME)) TABLESPACE WWI_IDX
/

CREATE INDEX WWI_MDM.IX_CUST_MASTER_REGION_STATUS
    ON WWI_MDM.CUST_MASTER (REGION_CD, CUST_STATUS_CD, DELETED_FLG) TABLESPACE WWI_IDX
/

CREATE INDEX WWI_MDM.IX_CUST_MASTER_UPDATED
    ON WWI_MDM.CUST_MASTER (UPDATED_DT) TABLESPACE WWI_IDX
/

CREATE INDEX WWI_MDM.IX_CUST_MASTER_LEGACY
    ON WWI_MDM.CUST_MASTER (LEGACY_CUST_CD) TABLESPACE WWI_IDX
/

COMMENT ON TABLE WWI_MDM.CUST_MASTER IS
    'Customer master. Incremental extract source, keyed on UPDATED_DT.'
/
COMMENT ON COLUMN WWI_MDM.CUST_MASTER.ACCT_MANAGER_CD IS
    'Holds a sales TEAM code since 2015 despite the column name.'
/
COMMENT ON COLUMN WWI_MDM.CUST_MASTER.MISC_FLAG_1 IS
    'Overloaded: ''X'' = do not dunning-letter, ''P'' = paper invoice only, NULL = neither.'
/
COMMENT ON COLUMN WWI_MDM.CUST_MASTER.SPECIAL_INSTR_TXT IS
    'Free text parsed by the EU billing job for the token PO-REQUIRED.'
/
