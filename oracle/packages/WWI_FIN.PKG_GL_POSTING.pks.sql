/* ============================================================================
 * Object      : WWI_FIN.PKG_GL_POSTING (package specification)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.GL_JOURNAL_HDR, WWI_FIN.GL_JOURNAL_LINE,
 *               WWI_FIN.GL_ACCOUNT, WWI_FIN.GL_PERIOD_STATUS
 * Called by   : WWI_FIN.PKG_AP_INVOICE, WWI_FIN.PKG_AP_PAYMENT,
 *               WWI_FIN.PRC_RUN_PERIOD_CLOSE, WWI_FIN.PRC_ACCRUE_UNINVOICED_RECEIPTS
 * History     : 1998 created; 2003 accrual/reversal pairs; 2011 region-aware
 *               period control after the EU close moved off the US calendar.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_FIN.PKG_GL_POSTING AS

    e_period_not_open  EXCEPTION;
    e_journal_unbalanced EXCEPTION;
    e_journal_posted   EXCEPTION;
    e_account_invalid  EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_period_not_open,     -20121);
    PRAGMA EXCEPTION_INIT(e_journal_unbalanced,  -20122);
    PRAGMA EXCEPTION_INIT(e_journal_posted,      -20123);
    PRAGMA EXCEPTION_INIT(e_account_invalid,     -20124);

    c_bulk_limit CONSTANT PLS_INTEGER := 1000;

    FUNCTION period_status
    (
        p_period_cd IN WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE,
        p_region_cd IN VARCHAR2
    ) RETURN VARCHAR2;

    FUNCTION create_journal_header
    (
        p_source_cd   IN WWI_FIN.GL_JOURNAL_HDR.JOURNAL_SOURCE_CD%TYPE,
        p_category_cd IN WWI_FIN.GL_JOURNAL_HDR.JOURNAL_CATEGORY_CD%TYPE,
        p_region_cd   IN VARCHAR2,
        p_gl_date     IN DATE,
        p_accrual     IN VARCHAR2 DEFAULT 'N'
    ) RETURN WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE;

    PROCEDURE add_journal_line
    (
        p_journal_id     IN WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE,
        p_account_cd     IN WWI_FIN.GL_ACCOUNT.ACCOUNT_CD%TYPE,
        p_cost_center_id IN WWI_FIN.GL_JOURNAL_LINE.COST_CENTER_ID%TYPE,
        p_currency_cd    IN WWI_FIN.GL_JOURNAL_LINE.CURRENCY_CD%TYPE,
        p_debit_amt      IN NUMBER,
        p_credit_amt     IN NUMBER,
        p_line_desc      IN WWI_FIN.GL_JOURNAL_LINE.LINE_DESC%TYPE DEFAULT NULL,
        p_src_doc_type   IN WWI_FIN.GL_JOURNAL_LINE.SRC_DOC_TYPE_CD%TYPE DEFAULT NULL,
        p_src_doc_id     IN WWI_FIN.GL_JOURNAL_LINE.SRC_DOC_ID%TYPE DEFAULT NULL
    );

    PROCEDURE create_invoice_journal
    (
        p_invoice_id IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE
    );

    PROCEDURE post_journal
    (
        p_journal_id IN WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE
    );

    PROCEDURE post_pending_journals
    (
        p_region_cd IN  VARCHAR2,
        p_period_cd IN  WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE,
        p_posted    OUT PLS_INTEGER,
        p_failed    OUT PLS_INTEGER
    );

    PROCEDURE reverse_document_journal
    (
        p_doc_type_cd IN WWI_FIN.GL_JOURNAL_LINE.SRC_DOC_TYPE_CD%TYPE,
        p_doc_id      IN WWI_FIN.GL_JOURNAL_LINE.SRC_DOC_ID%TYPE,
        p_reason_txt  IN VARCHAR2
    );

    PROCEDURE reverse_accruals
    (
        p_region_cd IN  VARCHAR2,
        p_period_cd IN  WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE,
        p_reversed  OUT PLS_INTEGER
    );

END PKG_GL_POSTING;
/
