/* =====================================================================
 * Object       : TABLE WWI_MDM.PARTY_XREF
 * Schema       : WWI_MDM (Oracle ERP - WWIGERP)
 * Deploy order : 37
 * Depends on   : WWI_MDM.CUST_MASTER, WWI_MDM.SUPP_MASTER, WWI_REF.SOURCE_SYSTEM_REF
 * Called by    : PKG_CUSTOMER_MASTER, PKG_SUPPLIER_MASTER, C360 matching packages
 *
 * Cross-reference from a source system's key to the ERP party. This is how the
 * WideWorldImporters OLTP customer id, the CRM account id and the legacy
 * mainframe code all resolve to one CUST_ID or SUPP_ID. PARTY_TYPE_CD decides
 * which of the two id columns is populated - neither is a foreign key, because
 * the mainframe rows point at parties that were purged years ago.
 * ===================================================================== */

CREATE SEQUENCE WWI_MDM.SEQ_PARTY_XREF
    START WITH 1000001 INCREMENT BY 1 CACHE 100 NOCYCLE
/

CREATE TABLE WWI_MDM.PARTY_XREF
(
    PARTY_XREF_ID           NUMBER(12)      NOT NULL,
    PARTY_TYPE_CD           VARCHAR2(4)     NOT NULL,
    CUST_ID                 NUMBER(12),
    SUPP_ID                 NUMBER(12),
    SOURCE_SYS_CD           VARCHAR2(12)    NOT NULL,
    SOURCE_KEY_TXT          VARCHAR2(60)    NOT NULL,
    SOURCE_KEY_TYPE_CD      VARCHAR2(8)     DEFAULT 'ID' NOT NULL,
    MATCH_METHOD_CD         VARCHAR2(6)     DEFAULT 'MAN' NOT NULL,
    MATCH_SCORE             NUMBER(5,2),
    MATCH_RUN_ID            NUMBER(12),
    REVIEW_REQUIRED_FLG     VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    REVIEWED_BY_CD          VARCHAR2(8),
    REVIEWED_DT             DATE,
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_PARTY_XREF PRIMARY KEY (PARTY_XREF_ID) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT UK_PARTY_XREF_SOURCE UNIQUE (SOURCE_SYS_CD, SOURCE_KEY_TYPE_CD, SOURCE_KEY_TXT)
        USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_PARTY_XREF_TYPE CHECK (PARTY_TYPE_CD IN ('CUST', 'SUPP')),
    CONSTRAINT CK_PARTY_XREF_TARGET CHECK (
        (PARTY_TYPE_CD = 'CUST' AND CUST_ID IS NOT NULL AND SUPP_ID IS NULL)
     OR (PARTY_TYPE_CD = 'SUPP' AND SUPP_ID IS NOT NULL AND CUST_ID IS NULL)),
    CONSTRAINT CK_PARTY_XREF_METHOD CHECK (MATCH_METHOD_CD IN ('MAN', 'EXACT', 'FUZZY', 'MIGR')),
    CONSTRAINT CK_PARTY_XREF_FLAGS CHECK (
        REVIEW_REQUIRED_FLG IN ('Y', 'N') AND ACTIVE_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_DATA
/

CREATE INDEX WWI_MDM.IX_PARTY_XREF_CUST
    ON WWI_MDM.PARTY_XREF (CUST_ID, SOURCE_SYS_CD) TABLESPACE WWI_IDX
/

CREATE INDEX WWI_MDM.IX_PARTY_XREF_SUPP
    ON WWI_MDM.PARTY_XREF (SUPP_ID, SOURCE_SYS_CD) TABLESPACE WWI_IDX
/

COMMENT ON TABLE WWI_MDM.PARTY_XREF IS
    'No foreign keys by design: historical rows reference purged parties.'
/
