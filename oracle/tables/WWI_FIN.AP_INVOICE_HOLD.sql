/* =====================================================================
 * Object       : TABLE WWI_FIN.AP_INVOICE_HOLD
 * Schema       : WWI_FIN (Oracle ERP - WWIGERP)
 * Deploy order : 72
 * Depends on   : WWI_FIN.AP_INVOICE_HDR
 * Called by    : PKG_AP_INVOICE (validation), AP exception reporting
 *
 * Holds placed on an invoice by validation or by a user. Holds are never
 * deleted, only released, and the release can itself be reversed by placing
 * the same hold code again - so the current state is the latest row per
 * invoice and hold code, not the presence of a row.
 * ===================================================================== */

CREATE SEQUENCE WWI_FIN.SEQ_AP_INVOICE_HOLD
    START WITH 800001 INCREMENT BY 1 CACHE 50 NOCYCLE
/

CREATE TABLE WWI_FIN.AP_INVOICE_HOLD
(
    HOLD_ID                 NUMBER(12)      NOT NULL,
    INVOICE_ID              NUMBER(12)      NOT NULL,
    INVOICE_LINE_ID         NUMBER(12),
    HOLD_CODE_CD            VARCHAR2(8)     NOT NULL,
    HOLD_REASON_TXT         VARCHAR2(400),
    HOLD_SOURCE_CD          VARCHAR2(6)     DEFAULT 'SYS' NOT NULL,
    PLACED_BY_CD            VARCHAR2(8),
    PLACED_DT               DATE            DEFAULT SYSDATE NOT NULL,
    RELEASED_FLG            VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    RELEASE_CODE_CD         VARCHAR2(8),
    RELEASED_BY_CD          VARCHAR2(8),
    RELEASED_DT             DATE,
    RELEASE_NOTES_TXT       VARCHAR2(1000),
    BLOCKS_PAYMENT_FLG      VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    BLOCKS_POSTING_FLG      VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    AGE_DAYS                NUMBER(6),
    ESCALATED_FLG           VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_AP_INVOICE_HOLD PRIMARY KEY (HOLD_ID) USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT CK_AP_HOLD_SOURCE CHECK (HOLD_SOURCE_CD IN ('SYS', 'USER', 'MATCH', 'TAX')),
    CONSTRAINT CK_AP_HOLD_FLAGS CHECK (
        RELEASED_FLG IN ('Y', 'N') AND BLOCKS_PAYMENT_FLG IN ('Y', 'N')
        AND BLOCKS_POSTING_FLG IN ('Y', 'N') AND ESCALATED_FLG IN ('Y', 'N')),
    CONSTRAINT CK_AP_HOLD_RELEASE CHECK (
        RELEASED_FLG = 'N' OR (RELEASE_CODE_CD IS NOT NULL AND RELEASED_DT IS NOT NULL))
)
TABLESPACE WWI_FIN_DATA
PCTFREE 20
/

ALTER TABLE WWI_FIN.AP_INVOICE_HOLD ADD CONSTRAINT FK_AP_HOLD_INVOICE
    FOREIGN KEY (INVOICE_ID) REFERENCES WWI_FIN.AP_INVOICE_HDR (INVOICE_ID)
/

CREATE INDEX WWI_FIN.IX_AP_HOLD_OPEN
    ON WWI_FIN.AP_INVOICE_HOLD (INVOICE_ID, RELEASED_FLG) TABLESPACE WWI_FIN_IDX
/

CREATE INDEX WWI_FIN.IX_AP_HOLD_CODE
    ON WWI_FIN.AP_INVOICE_HOLD (HOLD_CODE_CD, PLACED_DT) TABLESPACE WWI_FIN_IDX
/
