/* =====================================================================
 * Object       : TABLE WWI_REF.REGION_REF
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 20 (first table in WWI_REF)
 * Depends on   : oracle/ddl/03_create_schemas.sql
 * Called by    : Every regional branch in PKG_TAX_CALC and the extract views
 *
 * The three operating regions plus the two retired ones that history still
 * references. Reporting and fiscal calendars differ per region and are
 * described here rather than derived.
 * ===================================================================== */

CREATE TABLE WWI_REF.REGION_REF
(
    REGION_CD               VARCHAR2(4)     NOT NULL,
    REGION_NAME             VARCHAR2(60)    NOT NULL,
    REPORTING_CURR_CD       VARCHAR2(3)     NOT NULL,
    FISCAL_YEAR_START_MONTH NUMBER(2)       NOT NULL,
    FISCAL_CALENDAR_CD      VARCHAR2(10)    NOT NULL,
    TAX_REGIME_CD           VARCHAR2(6)     NOT NULL,
    ADDRESS_FORMAT_CD       VARCHAR2(10)    NOT NULL,
    POSTAL_FORMAT_CD        VARCHAR2(10)    NOT NULL,
    DATE_FORMAT_MASK        VARCHAR2(20)    NOT NULL,
    DEFAULT_LANGUAGE_CD     VARCHAR2(5)     NOT NULL,
    CONSENT_REGIME_CD       VARCHAR2(10)    NOT NULL,
    RETENTION_MONTHS        NUMBER(4)       NOT NULL,
    TIMEZONE_TXT            VARCHAR2(40),
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    RETIRED_DT              DATE,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_REGION_REF PRIMARY KEY (REGION_CD) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_REGION_TAX_REGIME CHECK (TAX_REGIME_CD IN ('SALES', 'VAT', 'GST', 'MIXED')),
    CONSTRAINT CK_REGION_FISCAL_MONTH CHECK (FISCAL_YEAR_START_MONTH BETWEEN 1 AND 12),
    CONSTRAINT CK_REGION_ACTIVE CHECK (ACTIVE_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_REF_DATA
/

COMMENT ON COLUMN WWI_REF.REGION_REF.CONSENT_REGIME_CD IS
    'CAN_SPAM for NA, GDPR for EU, APPI_PDPA for APAC. Drives retention and consent columns in WWI_MDM.'
/
