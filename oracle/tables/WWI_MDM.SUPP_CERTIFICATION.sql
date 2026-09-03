/* =====================================================================
 * Object       : TABLE WWI_MDM.SUPP_CERTIFICATION
 * Schema       : WWI_MDM (Oracle ERP - WWIGERP)
 * Deploy order : 31
 * Depends on   : WWI_MDM.SUPP_MASTER
 * Called by    : PKG_SUPPLIER_MASTER (certification expiry check), supplier scorecard
 *
 * Quality, compliance and insurance certificates held for a supplier. Expiry
 * handling is manual: the nightly job only warns, it never blocks a PO, so
 * expired certificates are common in the data. DOC_REF_TXT points at a file
 * share path recorded as free text.
 * ===================================================================== */

CREATE SEQUENCE WWI_MDM.SEQ_SUPP_CERTIFICATION
    START WITH 980001 INCREMENT BY 1 CACHE 10 NOCYCLE
/

CREATE TABLE WWI_MDM.SUPP_CERTIFICATION
(
    SUPP_CERT_ID            NUMBER(12)      NOT NULL,
    SUPP_ID                 NUMBER(12)      NOT NULL,
    CERT_TYPE_CD            VARCHAR2(10)    NOT NULL,
    CERT_NBR                VARCHAR2(40),
    ISSUING_BODY_TXT        VARCHAR2(120),
    SCOPE_TXT               VARCHAR2(400),
    ISSUE_DT                DATE            NOT NULL,
    EXPIRY_DT               DATE,
    RENEWAL_REMINDER_DT     DATE,
    CERT_STATUS_CD          VARCHAR2(4)     DEFAULT 'VAL' NOT NULL,
    COVERAGE_AMT            NUMBER(15,5),
    COVERAGE_CURR_CD        VARCHAR2(3),
    DOC_REF_TXT             VARCHAR2(400),
    VERIFIED_BY_CD          VARCHAR2(8),
    VERIFIED_DT             DATE,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_SUPP_CERTIFICATION PRIMARY KEY (SUPP_CERT_ID) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_SUPP_CERT UNIQUE (SUPP_ID, CERT_TYPE_CD, ISSUE_DT) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_SUPP_CERT_TYPE CHECK (
        CERT_TYPE_CD IN ('ISO9001', 'ISO14001', 'HACCP', 'INSUR', 'GDPR', 'MODSLAV', 'FSC')),
    CONSTRAINT CK_SUPP_CERT_STATUS CHECK (CERT_STATUS_CD IN ('VAL', 'EXPD', 'SUSP', 'PEND')),
    CONSTRAINT CK_SUPP_CERT_DATES CHECK (EXPIRY_DT IS NULL OR EXPIRY_DT > ISSUE_DT)
)
TABLESPACE WWI_DATA
/

ALTER TABLE WWI_MDM.SUPP_CERTIFICATION ADD CONSTRAINT FK_SUPP_CERT_SUPP
    FOREIGN KEY (SUPP_ID) REFERENCES WWI_MDM.SUPP_MASTER (SUPP_ID)
/

CREATE INDEX WWI_MDM.IX_SUPP_CERT_EXPIRY
    ON WWI_MDM.SUPP_CERTIFICATION (EXPIRY_DT, CERT_STATUS_CD) TABLESPACE WWI_IDX
/
