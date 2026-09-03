/* =====================================================================
 * Object       : TABLE WWI_REF.PAYMENT_METHOD_REF
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 29
 * Depends on   : WWI_REF.COUNTRY_REF
 * Called by    : PKG_AP_PAYMENT, payment file generation
 *
 * Payment methods available per country, with the file format the treasury
 * interface produces. NA cheque and NACHA, EU SEPA pain.001, APAC a
 * bank-proprietary fixed-width format per country - the format code is what
 * the payment run branches on.
 * ===================================================================== */

CREATE TABLE WWI_REF.PAYMENT_METHOD_REF
(
    PAYMENT_METHOD_CD       VARCHAR2(6)     NOT NULL,
    COUNTRY_CD              VARCHAR2(2)     NOT NULL,
    METHOD_NAME             VARCHAR2(80)    NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    FILE_FORMAT_CD          VARCHAR2(12)    NOT NULL,
    SETTLEMENT_DAYS         NUMBER(3)       DEFAULT 0 NOT NULL,
    CUT_OFF_TIME_TXT        VARCHAR2(8),
    MIN_AMT                 NUMBER(15,5),
    MAX_AMT                 NUMBER(15,5),
    METHOD_CURR_CD          VARCHAR2(3),
    BANK_CHARGE_AMT         NUMBER(15,5)    DEFAULT 0 NOT NULL,
    REQUIRES_IBAN_FLG       VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    REQUIRES_ROUTING_FLG    VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    REQUIRES_MANDATE_FLG    VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    REMITTANCE_ADVICE_FLG   VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    EFFECTIVE_FROM_DT       DATE            DEFAULT SYSDATE NOT NULL,
    EFFECTIVE_TO_DT         DATE,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_PAYMENT_METHOD_REF PRIMARY KEY (PAYMENT_METHOD_CD, COUNTRY_CD)
        USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_PAY_METHOD_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC')),
    CONSTRAINT CK_PAY_METHOD_FLAGS CHECK (
        REQUIRES_IBAN_FLG IN ('Y', 'N') AND REQUIRES_ROUTING_FLG IN ('Y', 'N')
        AND REQUIRES_MANDATE_FLG IN ('Y', 'N') AND REMITTANCE_ADVICE_FLG IN ('Y', 'N')
        AND ACTIVE_FLG IN ('Y', 'N')),
    CONSTRAINT CK_PAY_METHOD_AMT CHECK (MAX_AMT IS NULL OR MIN_AMT IS NULL OR MAX_AMT >= MIN_AMT)
)
TABLESPACE WWI_REF_DATA
/

ALTER TABLE WWI_REF.PAYMENT_METHOD_REF ADD CONSTRAINT FK_PAY_METHOD_COUNTRY
    FOREIGN KEY (COUNTRY_CD) REFERENCES WWI_REF.COUNTRY_REF (COUNTRY_CD)
/

CREATE INDEX WWI_REF.IX_PAY_METHOD_REGION
    ON WWI_REF.PAYMENT_METHOD_REF (REGION_CD, ACTIVE_FLG) TABLESPACE WWI_IDX
/
