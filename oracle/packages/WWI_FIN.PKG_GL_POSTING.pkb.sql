/* ============================================================================
 * Object      : WWI_FIN.PKG_GL_POSTING (package body)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_FIN.PKG_GL_POSTING, WWI_FIN.GL_JOURNAL_HDR,
 *               WWI_FIN.GL_JOURNAL_LINE, WWI_FIN.GL_ACCOUNT,
 *               WWI_FIN.GL_PERIOD_STATUS, WWI_FIN.AP_INVOICE_HDR,
 *               WWI_FIN.AP_INVOICE_LINE, WWI_FIN.FN_CONVERT_AMOUNT,
 *               WWI_REF.FN_FISCAL_PERIOD, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_FIN.PKG_GL_POSTING AS

    c_ap_liability_acct CONSTANT VARCHAR2(20) := '2100';
    c_ap_accrual_acct   CONSTANT VARCHAR2(20) := '2110';
    c_vat_input_acct    CONSTANT VARCHAR2(20) := '1350';
    c_sales_tax_acct    CONSTANT VARCHAR2(20) := '1355';
    c_gst_input_acct    CONSTANT VARCHAR2(20) := '1357';
    c_suspense_acct     CONSTANT VARCHAR2(20) := '9999';

    FUNCTION period_status
    (
        p_period_cd IN WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE,
        p_region_cd IN VARCHAR2
    ) RETURN VARCHAR2
    IS
        l_status WWI_FIN.GL_PERIOD_STATUS.GL_STATUS_CD%TYPE;
    BEGIN
        SELECT GL_STATUS_CD
          INTO l_status
          FROM WWI_FIN.GL_PERIOD_STATUS
         WHERE PERIOD_CD = p_period_cd
           AND REGION_CD = p_region_cd;

        RETURN l_status;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 'N';   /* not defined - treated as never opened */
    END period_status;

    FUNCTION resolve_account
    (
        p_account_cd IN WWI_FIN.GL_ACCOUNT.ACCOUNT_CD%TYPE
    ) RETURN WWI_FIN.GL_ACCOUNT.GL_ACCOUNT_ID%TYPE
    IS
        l_id WWI_FIN.GL_ACCOUNT.GL_ACCOUNT_ID%TYPE;
    BEGIN
        SELECT GL_ACCOUNT_ID
          INTO l_id
          FROM WWI_FIN.GL_ACCOUNT
         WHERE ACCOUNT_CD = p_account_cd
           AND NVL(POSTING_ALLOWED_FLG, 'Y') = 'Y';

        RETURN l_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20124,
                'PKG_GL_POSTING.resolve_account: account ' || p_account_cd
                || ' is missing or inactive');
    END resolve_account;

    FUNCTION create_journal_header
    (
        p_source_cd   IN WWI_FIN.GL_JOURNAL_HDR.JOURNAL_SOURCE_CD%TYPE,
        p_category_cd IN WWI_FIN.GL_JOURNAL_HDR.JOURNAL_CATEGORY_CD%TYPE,
        p_region_cd   IN VARCHAR2,
        p_gl_date     IN DATE,
        p_accrual     IN VARCHAR2 DEFAULT 'N'
    ) RETURN WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE
    IS
        l_journal_id WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE;
        l_period_cd  WWI_FIN.GL_JOURNAL_HDR.PERIOD_CD%TYPE;
    BEGIN
        l_period_cd := WWI_REF.FN_FISCAL_PERIOD(p_gl_date, p_region_cd);

        IF period_status(l_period_cd, p_region_cd) <> 'O' THEN
            RAISE_APPLICATION_ERROR(-20121,
                'PKG_GL_POSTING.create_journal_header: period ' || l_period_cd
                || ' is not open for ' || p_region_cd);
        END IF;

        l_journal_id := WWI_FIN.SEQ_GL_JOURNAL_HDR.NEXTVAL;

        INSERT INTO WWI_FIN.GL_JOURNAL_HDR
            (JOURNAL_ID, JOURNAL_NBR, JOURNAL_SOURCE_CD, JOURNAL_CATEGORY_CD,
             REGION_CD, PERIOD_CD, ACCOUNTING_DT, POSTING_STATUS_CD, ACCRUAL_FLG,
             REVERSAL_FLG, CREATED_DT, CREATED_BY, UPDATED_DT)
        VALUES
            (l_journal_id,
             p_source_cd || '-' || TO_CHAR(l_journal_id),
             p_source_cd, p_category_cd, p_region_cd, l_period_cd,
             TRUNC(p_gl_date), 'N', NVL(p_accrual, 'N'), 'N',
             SYSDATE, USER, SYSDATE);

        RETURN l_journal_id;
    END create_journal_header;

    PROCEDURE add_journal_line
    (
        p_journal_id     IN WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE,
        p_account_cd     IN WWI_FIN.GL_ACCOUNT.ACCOUNT_CD%TYPE,
        p_cost_center_id IN WWI_FIN.GL_JOURNAL_LINE.COST_CENTER_CD%TYPE,
        p_currency_cd    IN WWI_FIN.GL_JOURNAL_LINE.ENTERED_CURR_CD%TYPE,
        p_debit_amt      IN NUMBER,
        p_credit_amt     IN NUMBER,
        p_line_desc      IN WWI_FIN.GL_JOURNAL_LINE.LINE_DESC%TYPE DEFAULT NULL,
        p_src_doc_type   IN WWI_FIN.GL_JOURNAL_LINE.SOURCE_DOC_TYPE_CD%TYPE DEFAULT NULL,
        p_src_doc_id     IN WWI_FIN.GL_JOURNAL_LINE.SOURCE_DOC_ID%TYPE DEFAULT NULL
    )
    IS
        l_line_num   PLS_INTEGER;
        l_account_id WWI_FIN.GL_ACCOUNT.GL_ACCOUNT_ID%TYPE;
        l_gl_date    WWI_FIN.GL_JOURNAL_HDR.ACCOUNTING_DT%TYPE;
        l_posted     WWI_FIN.GL_JOURNAL_HDR.POSTING_STATUS_CD%TYPE;
    BEGIN
        SELECT ACCOUNTING_DT, NVL(POSTING_STATUS_CD, 'N')
          INTO l_gl_date, l_posted
          FROM WWI_FIN.GL_JOURNAL_HDR
         WHERE JOURNAL_ID = p_journal_id;

        IF l_posted = 'Y' THEN
            RAISE_APPLICATION_ERROR(-20123,
                'PKG_GL_POSTING.add_journal_line: journal ' || p_journal_id
                || ' is already posted');
        END IF;

        BEGIN
            l_account_id := resolve_account(p_account_cd);
        EXCEPTION
            WHEN OTHERS THEN
                /* unresolvable accounts go to suspense - finance clears it
                   manually every month, and has since 2002                  */
                l_account_id := resolve_account(c_suspense_acct);
                WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_GL_POSTING.add_journal_line',
                                                     p_account_cd,
                                                     'posted to suspense: ' || SQLERRM);
        END;

        SELECT NVL(MAX(LINE_NBR), 0) + 10
          INTO l_line_num
          FROM WWI_FIN.GL_JOURNAL_LINE
         WHERE JOURNAL_ID = p_journal_id;

        INSERT INTO WWI_FIN.GL_JOURNAL_LINE
            (JOURNAL_LINE_ID, JOURNAL_ID, LINE_NBR, ACCOUNT_CD, COST_CENTER_CD,
             ENTERED_CURR_CD, ENTERED_DEBIT_AMT, ENTERED_CREDIT_AMT, ACCOUNTED_DEBIT_AMT, ACCOUNTED_CREDIT_AMT,
             LINE_DESC, SOURCE_DOC_TYPE_CD, SOURCE_DOC_ID, CREATED_DT)
        VALUES
            (WWI_FIN.SEQ_GL_JOURNAL_LINE.NEXTVAL, p_journal_id, l_line_num,
             l_account_id, p_cost_center_id, p_currency_cd,
             NVL(p_debit_amt, 0), NVL(p_credit_amt, 0),
             WWI_FIN.FN_CONVERT_AMOUNT(NVL(p_debit_amt, 0), p_currency_cd, 'USD',
                                       l_gl_date, 'CORP'),
             WWI_FIN.FN_CONVERT_AMOUNT(NVL(p_credit_amt, 0), p_currency_cd, 'USD',
                                       l_gl_date, 'CORP'),
             p_line_desc, p_src_doc_type, p_src_doc_id, SYSDATE);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20123,
                'PKG_GL_POSTING.add_journal_line: journal ' || p_journal_id || ' not found');
    END add_journal_line;

    PROCEDURE create_invoice_journal
    (
        p_invoice_id IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE
    )
    IS
        l_hdr        WWI_FIN.AP_INVOICE_HDR%ROWTYPE;
        l_journal_id WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE;
        l_tax_acct   VARCHAR2(20);
    BEGIN
        SELECT *
          INTO l_hdr
          FROM WWI_FIN.AP_INVOICE_HDR
         WHERE INVOICE_ID = p_invoice_id;

        l_journal_id := create_journal_header('AP', 'INVOICE', l_hdr.REGION_CD,
                                              NVL(l_hdr.GL_DATE, l_hdr.INVOICE_DT));

        FOR r IN (SELECT il.LINE_AMT, il.ACCOUNT_CD, il.COST_CENTER_CD, il.LINE_NBR
                    FROM WWI_FIN.AP_INVOICE_LINE il
                   WHERE il.INVOICE_ID = p_invoice_id
                   ORDER BY il.LINE_NBR) LOOP

            add_journal_line(l_journal_id,
                             NVL(r.ACCOUNT_CD, c_suspense_acct),
                             r.COST_CENTER_CD,
                             l_hdr.INVOICE_CURR_CD,
                             r.LINE_AMT, 0,
                             'AP invoice ' || l_hdr.INVOICE_NBR || ' line ' || r.LINE_NBR,
                             'AP_INV', p_invoice_id);
        END LOOP;

        /* the tax account depends on the regional tax regime */
        l_tax_acct := CASE l_hdr.REGION_CD
                          WHEN 'EU'   THEN c_vat_input_acct
                          WHEN 'APAC' THEN c_gst_input_acct
                          ELSE c_sales_tax_acct
                      END;

        IF NVL(l_hdr.TAX_AMT, 0) <> 0
           AND NVL(l_hdr.EU_SELF_BILLING_FLG, 'N') = 'N' THEN
            add_journal_line(l_journal_id, l_tax_acct, NULL, l_hdr.INVOICE_CURR_CD,
                             l_hdr.TAX_AMT, 0,
                             'Input tax on ' || l_hdr.INVOICE_NBR, 'AP_INV', p_invoice_id);
        ELSIF NVL(l_hdr.EU_SELF_BILLING_FLG, 'N') = 'Y' THEN
            /* EU reverse charge books input and output tax simultaneously */
            add_journal_line(l_journal_id, c_vat_input_acct, NULL, l_hdr.INVOICE_CURR_CD,
                             NVL(l_hdr.TAX_AMT, 0), 0,
                             'Reverse charge input VAT', 'AP_INV', p_invoice_id);
            add_journal_line(l_journal_id, '2350', NULL, l_hdr.INVOICE_CURR_CD,
                             0, NVL(l_hdr.TAX_AMT, 0),
                             'Reverse charge output VAT', 'AP_INV', p_invoice_id);
        END IF;

        add_journal_line(l_journal_id, c_ap_liability_acct, NULL, l_hdr.INVOICE_CURR_CD,
                         0, NVL(l_hdr.GROSS_AMT, 0),
                         'AP liability ' || l_hdr.INVOICE_NBR, 'AP_INV', p_invoice_id);

        post_journal(l_journal_id);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20123,
                'PKG_GL_POSTING.create_invoice_journal: invoice ' || p_invoice_id
                || ' not found');
    END create_invoice_journal;

    PROCEDURE post_journal
    (
        p_journal_id IN WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE
    )
    IS
        l_debit   NUMBER;
        l_credit  NUMBER;
        l_posted  WWI_FIN.GL_JOURNAL_HDR.POSTING_STATUS_CD%TYPE;
        l_period  WWI_FIN.GL_JOURNAL_HDR.PERIOD_CD%TYPE;
        l_region  WWI_FIN.GL_JOURNAL_HDR.REGION_CD%TYPE;
    BEGIN
        SELECT NVL(POSTING_STATUS_CD, 'N'), PERIOD_CD, REGION_CD
          INTO l_posted, l_period, l_region
          FROM WWI_FIN.GL_JOURNAL_HDR
         WHERE JOURNAL_ID = p_journal_id
           FOR UPDATE;

        IF l_posted = 'Y' THEN
            RAISE_APPLICATION_ERROR(-20123,
                'PKG_GL_POSTING.post_journal: journal already posted');
        END IF;

        IF period_status(l_period, l_region) <> 'O' THEN
            RAISE_APPLICATION_ERROR(-20121,
                'PKG_GL_POSTING.post_journal: period ' || l_period || ' not open');
        END IF;

        SELECT NVL(SUM(ACCOUNTED_DEBIT_AMT), 0), NVL(SUM(ACCOUNTED_CREDIT_AMT), 0)
          INTO l_debit, l_credit
          FROM WWI_FIN.GL_JOURNAL_LINE
         WHERE JOURNAL_ID = p_journal_id;

        IF ABS(l_debit - l_credit) > 0.02 THEN
            RAISE_APPLICATION_ERROR(-20122,
                'PKG_GL_POSTING.post_journal: journal ' || p_journal_id
                || ' out of balance by ' || TO_CHAR(ROUND(l_debit - l_credit, 2)));
        END IF;

        UPDATE WWI_FIN.GL_JOURNAL_HDR
           SET POSTING_STATUS_CD = 'Y',
               POSTED_DT   = SYSDATE,
               POSTED_BY_CD   = USER,
               UPDATED_DT = SYSDATE
         WHERE JOURNAL_ID = p_journal_id;
    END post_journal;

    PROCEDURE post_pending_journals
    (
        p_region_cd IN  VARCHAR2,
        p_period_cd IN  WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE,
        p_posted    OUT PLS_INTEGER,
        p_failed    OUT PLS_INTEGER
    )
    IS
        CURSOR c_pending IS
            SELECT JOURNAL_ID
              FROM WWI_FIN.GL_JOURNAL_HDR
             WHERE NVL(POSTING_STATUS_CD, 'N') = 'N'
               AND REGION_CD = p_region_cd
               AND PERIOD_CD = p_period_cd
             ORDER BY JOURNAL_ID;

        TYPE t_id_tab IS TABLE OF WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE;
        l_ids t_id_tab;
    BEGIN
        p_posted := 0;
        p_failed := 0;

        OPEN c_pending;
        LOOP
            FETCH c_pending BULK COLLECT INTO l_ids LIMIT c_bulk_limit;
            EXIT WHEN l_ids.COUNT = 0;

            FOR i IN 1 .. l_ids.COUNT LOOP
                BEGIN
                    post_journal(l_ids(i));
                    p_posted := p_posted + 1;
                    COMMIT;
                EXCEPTION
                    WHEN OTHERS THEN
                        ROLLBACK;
                        p_failed := p_failed + 1;
                        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_GL_POSTING.post_pending_journals',
                                                             TO_CHAR(l_ids(i)), SQLERRM);
                END;
            END LOOP;

            EXIT WHEN c_pending%NOTFOUND;
        END LOOP;
        CLOSE c_pending;
    EXCEPTION
        WHEN OTHERS THEN
            IF c_pending%ISOPEN THEN
                CLOSE c_pending;
            END IF;
            RAISE;
    END post_pending_journals;

    PROCEDURE reverse_document_journal
    (
        p_doc_type_cd IN WWI_FIN.GL_JOURNAL_LINE.SOURCE_DOC_TYPE_CD%TYPE,
        p_doc_id      IN WWI_FIN.GL_JOURNAL_LINE.SOURCE_DOC_ID%TYPE,
        p_reason_txt  IN VARCHAR2
    )
    IS
        l_orig_id    WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE;
        l_region_cd  WWI_FIN.GL_JOURNAL_HDR.REGION_CD%TYPE;
        l_journal_id WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE;
    BEGIN
        SELECT MAX(jh.JOURNAL_ID), MAX(jh.REGION_CD)
          INTO l_orig_id, l_region_cd
          FROM WWI_FIN.GL_JOURNAL_HDR jh
         WHERE EXISTS (SELECT 1
                         FROM WWI_FIN.GL_JOURNAL_LINE jl
                        WHERE jl.JOURNAL_ID      = jh.JOURNAL_ID
                          AND jl.SOURCE_DOC_TYPE_CD = p_doc_type_cd
                          AND jl.SOURCE_DOC_ID      = p_doc_id)
           AND NVL(jh.REVERSAL_FLG, 'N') = 'N';

        IF l_orig_id IS NULL THEN
            RETURN;
        END IF;

        l_journal_id := create_journal_header('AP', 'REVERSAL', l_region_cd, TRUNC(SYSDATE));

        FOR r IN (SELECT jl.ACCOUNT_CD, jl.COST_CENTER_CD, jl.ENTERED_CURR_CD,
                         jl.ENTERED_DEBIT_AMT, jl.ENTERED_CREDIT_AMT
                    FROM WWI_FIN.GL_JOURNAL_LINE jl
                   WHERE jl.JOURNAL_ID = l_orig_id
                   ORDER BY jl.LINE_NBR) LOOP

            add_journal_line(l_journal_id, r.ACCOUNT_CD, r.COST_CENTER_CD,
                             r.ENTERED_CURR_CD, r.ENTERED_CREDIT_AMT, r.ENTERED_DEBIT_AMT,
                             SUBSTR(p_reason_txt, 1, 200), p_doc_type_cd, p_doc_id);
        END LOOP;

        UPDATE WWI_FIN.GL_JOURNAL_HDR
           SET REVERSAL_FLG       = 'Y',
               REVERSAL_OF_JOURNAL_ID = l_orig_id,
               UPDATED_DT         = SYSDATE
         WHERE JOURNAL_ID = l_journal_id;

        post_journal(l_journal_id);
    EXCEPTION
        WHEN OTHERS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_GL_POSTING.reverse_document_journal',
                                                 p_doc_type_cd || ':' || p_doc_id, SQLERRM);
            RAISE;
    END reverse_document_journal;

    PROCEDURE reverse_accruals
    (
        p_region_cd IN  VARCHAR2,
        p_period_cd IN  WWI_FIN.GL_PERIOD_STATUS.PERIOD_CD%TYPE,
        p_reversed  OUT PLS_INTEGER
    )
    IS
        l_journal_id WWI_FIN.GL_JOURNAL_HDR.JOURNAL_ID%TYPE;
    BEGIN
        p_reversed := 0;

        FOR a IN (SELECT jh.JOURNAL_ID
                    FROM WWI_FIN.GL_JOURNAL_HDR jh
                   WHERE jh.ACCRUAL_FLG = 'Y'
                     AND NVL(jh.POSTING_STATUS_CD, 'N') = 'Y'
                     AND jh.PERIOD_CD = p_period_cd
                     AND jh.REGION_CD = p_region_cd
                     AND NOT EXISTS (SELECT 1
                                       FROM WWI_FIN.GL_JOURNAL_HDR r
                                      WHERE r.REVERSAL_OF_JOURNAL_ID = jh.JOURNAL_ID)
                   ORDER BY jh.JOURNAL_ID) LOOP

            l_journal_id := create_journal_header('AP', 'ACCRUAL_REV', p_region_cd,
                                                  TRUNC(SYSDATE));

            FOR r IN (SELECT jl.ACCOUNT_CD, jl.COST_CENTER_CD, jl.ENTERED_CURR_CD,
                             jl.ENTERED_DEBIT_AMT, jl.ENTERED_CREDIT_AMT
                        FROM WWI_FIN.GL_JOURNAL_LINE jl
                       WHERE jl.JOURNAL_ID = a.JOURNAL_ID) LOOP

                add_journal_line(l_journal_id, r.ACCOUNT_CD, r.COST_CENTER_CD,
                                 r.ENTERED_CURR_CD, r.ENTERED_CREDIT_AMT, r.ENTERED_DEBIT_AMT,
                                 'Accrual reversal for period ' || p_period_cd,
                                 'ACCRUAL', a.JOURNAL_ID);
            END LOOP;

            UPDATE WWI_FIN.GL_JOURNAL_HDR
               SET REVERSAL_FLG       = 'Y',
                   REVERSAL_OF_JOURNAL_ID = a.JOURNAL_ID,
                   UPDATED_DT         = SYSDATE
             WHERE JOURNAL_ID = l_journal_id;

            post_journal(l_journal_id);
            p_reversed := p_reversed + 1;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_GL_POSTING.reverse_accruals',
                                                 p_region_cd || ':' || p_period_cd, SQLERRM);
            RAISE;
    END reverse_accruals;

END PKG_GL_POSTING;
/
