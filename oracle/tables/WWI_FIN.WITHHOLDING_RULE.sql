/* =====================================================================
 * Object       : TABLE WWI_FIN.WITHHOLDING_RULE
 * Schema       : WWI_FIN (Oracle ERP - WWIGERP)
 * Deploy order : 66
 * Depends on   : WWI_FIN.TAX_JURISDICTION, WWI_MDM.SUPP_MASTER
 * Called by    : PKG_AP_PAYMENT (withholding deduction), statutory reporting extracts
 *
 * Withholding tax rules applied at payment time. The three regions use this
 * table for genuinely different obligations - NA 1099/backup withholding, EU
 * cross-border royalty and interest withholding under treaty, APAC domestic
 * contractor withholding - and only the NA rows populate the 1099 box columns.
 * ===================================================================== */

CREATE SEQUENCE WWI_FIN.SEQ_WITHHOLDING_RULE
    START WITH 1201 INCREMENT BY 1 NOCACHE NOCYCLE
/

CREATE TABLE WWI_FIN.WITHHOLDING_RULE
(
    WHT_RULE_ID             NUMBER(12)      NOT NULL,
    WHT_RULE_CD             VARCHAR2(12)    NOT NULL,
    RULE_DESC               VARCHAR2(200)   NOT NULL,
    REGION_CD               VARCHAR2(4)     NOT NULL,
    COUNTRY_CD              VARCHAR2(2)     NOT NULL,
    JURISDICTION_CD         VARCHAR2(20),
    SUPPLIER_TYPE_CD        VARCHAR2(6),
    INCOME_TYPE_CD          VARCHAR2(8)     NOT NULL,
    WHT_RATE_PCT            NUMBER(7,5)     NOT NULL,
    TREATY_RATE_PCT         NUMBER(7,5),
    TREATY_COUNTRY_CD       VARCHAR2(2),
    THRESHOLD_AMT           NUMBER(15,5),
    THRESHOLD_CURR_CD       VARCHAR2(3),
    THRESHOLD_BASIS_CD      VARCHAR2(10),
    CERTIFICATE_REQ_FLG     VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    NA_1099_BOX_CD          VARCHAR2(4),
    NA_1099_FORM_CD         VARCHAR2(10),
    EU_TREATY_ARTICLE_TXT   VARCHAR2(60),
    APAC_FORM_CD            VARCHAR2(12),
    WHT_LIABILITY_ACCT_CD   VARCHAR2(30),
    EFFECTIVE_FROM_DT       DATE            NOT NULL,
    EFFECTIVE_TO_DT         DATE,
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_WITHHOLDING_RULE PRIMARY KEY (WHT_RULE_ID) USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT UK_WHT_RULE_CD UNIQUE (WHT_RULE_CD, EFFECTIVE_FROM_DT) USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT CK_WHT_REGION CHECK (REGION_CD IN ('NA', 'EU', 'APAC')),
    CONSTRAINT CK_WHT_RATE CHECK (WHT_RATE_PCT BETWEEN 0 AND 100),
    CONSTRAINT CK_WHT_NA_BOX CHECK (REGION_CD = 'NA' OR NA_1099_BOX_CD IS NULL),
    CONSTRAINT CK_WHT_FLAGS CHECK (ACTIVE_FLG IN ('Y', 'N') AND CERTIFICATE_REQ_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_FIN_DATA
/

CREATE INDEX WWI_FIN.IX_WHT_RULE_COUNTRY
    ON WWI_FIN.WITHHOLDING_RULE (COUNTRY_CD, INCOME_TYPE_CD, ACTIVE_FLG) TABLESPACE WWI_FIN_IDX
/
