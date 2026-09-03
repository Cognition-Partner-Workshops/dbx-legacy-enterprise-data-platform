/* =====================================================================
 * Object       : TABLE WWI_FIN.AP_PAYMENT_APPLY
 * Schema       : WWI_FIN (Oracle ERP - WWIGERP)
 * Deploy order : 74
 * Depends on   : WWI_FIN.AP_PAYMENT, WWI_FIN.AP_INVOICE_HDR
 * Called by    : PKG_AP_PAYMENT (application), AP reconciliation
 *
 * Application of a payment to an invoice, many-to-many. Partial applications
 * and credit-memo offsets both live here; a credit memo applied to an invoice
 * produces a zero-amount payment row with two application rows, which is why
 * payment counts and application counts never agree.
 * ===================================================================== */

CREATE SEQUENCE WWI_FIN.SEQ_AP_PAYMENT_APPLY
    START WITH 9000001 INCREMENT BY 1 CACHE 200 NOCYCLE
/

CREATE TABLE WWI_FIN.AP_PAYMENT_APPLY
(
    APPLY_ID                NUMBER(12)      NOT NULL,
    PAYMENT_ID              NUMBER(12)      NOT NULL,
    INVOICE_ID              NUMBER(12)      NOT NULL,
    APPLY_SEQ_NBR           NUMBER(4)       DEFAULT 1 NOT NULL,
    APPLIED_AMT             NUMBER(15,5)    NOT NULL,
    APPLIED_CURR_CD         VARCHAR2(3)     NOT NULL,
    DISCOUNT_AMT            NUMBER(15,5)    DEFAULT 0 NOT NULL,
    WITHHELD_AMT            NUMBER(15,5)    DEFAULT 0 NOT NULL,
    FX_GAIN_LOSS_AMT        NUMBER(15,5)    DEFAULT 0 NOT NULL,
    APPLY_DT                DATE            DEFAULT SYSDATE NOT NULL,
    GL_DATE                 DATE,
    APPLY_TYPE_CD           VARCHAR2(6)     DEFAULT 'PAY' NOT NULL,
    REVERSED_FLG            VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    REVERSAL_DT             DATE,
    REVERSAL_REASON_CD      VARCHAR2(4),
    POSTED_FLG              VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    JOURNAL_ID              NUMBER(12),
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_AP_PAYMENT_APPLY PRIMARY KEY (APPLY_ID) USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT UK_AP_APPLY UNIQUE (PAYMENT_ID, INVOICE_ID, APPLY_SEQ_NBR)
        USING INDEX TABLESPACE WWI_FIN_IDX,
    CONSTRAINT CK_AP_APPLY_TYPE CHECK (APPLY_TYPE_CD IN ('PAY', 'CRED', 'PREP', 'ADJ')),
    CONSTRAINT CK_AP_APPLY_FLAGS CHECK (REVERSED_FLG IN ('Y', 'N') AND POSTED_FLG IN ('Y', 'N')),
    CONSTRAINT CK_AP_APPLY_REVERSAL CHECK (
        REVERSED_FLG = 'N' OR REVERSAL_DT IS NOT NULL)
)
TABLESPACE WWI_FIN_DATA
/

ALTER TABLE WWI_FIN.AP_PAYMENT_APPLY ADD CONSTRAINT FK_AP_APPLY_PAYMENT
    FOREIGN KEY (PAYMENT_ID) REFERENCES WWI_FIN.AP_PAYMENT (PAYMENT_ID)
/

ALTER TABLE WWI_FIN.AP_PAYMENT_APPLY ADD CONSTRAINT FK_AP_APPLY_INVOICE
    FOREIGN KEY (INVOICE_ID) REFERENCES WWI_FIN.AP_INVOICE_HDR (INVOICE_ID)
/

CREATE INDEX WWI_FIN.IX_AP_APPLY_INVOICE
    ON WWI_FIN.AP_PAYMENT_APPLY (INVOICE_ID, REVERSED_FLG) TABLESPACE WWI_FIN_IDX
/
