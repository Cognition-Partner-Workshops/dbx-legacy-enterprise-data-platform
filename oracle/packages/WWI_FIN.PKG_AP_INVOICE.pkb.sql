/* ============================================================================
 * Object      : WWI_FIN.PKG_AP_INVOICE (package body)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_FIN.PKG_AP_INVOICE, WWI_FIN.AP_INVOICE_HDR,
 *               WWI_FIN.AP_INVOICE_LINE, WWI_FIN.AP_INVOICE_HOLD,
 *               WWI_FIN.GL_PERIOD_STATUS, WWI_FIN.TAX_RATE,
 *               WWI_PROC.PURCHASE_ORDER_HDR, WWI_PROC.PURCHASE_ORDER_LINE,
 *               WWI_PROC.PO_RECEIPT_LINE, WWI_FIN.FN_DUE_DATE,
 *               WWI_FIN.FN_TAX_AMOUNT, WWI_REF.FN_FISCAL_PERIOD,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_FIN.PKG_AP_INVOICE AS

    /* tolerance defaults. The table WWI_FIN.MATCH_TOLERANCE was supposed to
       replace these in 2012 but APAC never populated it, so the constants
       are still the effective values for that region.                      */
    c_qty_tol_pct_na      CONSTANT NUMBER := 2;
    c_qty_tol_pct_eu      CONSTANT NUMBER := 1;
    c_qty_tol_pct_apac    CONSTANT NUMBER := 5;
    c_price_tol_pct_na    CONSTANT NUMBER := 5;
    c_price_tol_pct_eu    CONSTANT NUMBER := 2;
    c_price_tol_pct_apac  CONSTANT NUMBER := 7.5;
    c_amt_tol_abs         CONSTANT NUMBER := 25;

    FUNCTION region_of_invoice
    (
        p_invoice_id IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE
    ) RETURN VARCHAR2
    IS
        l_region_cd WWI_FIN.AP_INVOICE_HDR.REGION_CD%TYPE;
    BEGIN
        SELECT REGION_CD
          INTO l_region_cd
          FROM WWI_FIN.AP_INVOICE_HDR
         WHERE INVOICE_ID = p_invoice_id;

        RETURN l_region_cd;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20101,
                'PKG_AP_INVOICE.region_of_invoice: invoice ' || p_invoice_id || ' not found');
    END region_of_invoice;

    FUNCTION is_duplicate
    (
        p_supp_id     IN WWI_FIN.AP_INVOICE_HDR.SUPP_ID%TYPE,
        p_invoice_num IN WWI_FIN.AP_INVOICE_HDR.INVOICE_NUM%TYPE,
        p_invoice_id  IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE DEFAULT NULL
    ) RETURN BOOLEAN
    IS
        l_cnt PLS_INTEGER;
    BEGIN
        /* the duplicate rule ignores punctuation because the OCR feed and the
           EDI feed disagree about dashes in supplier invoice numbers         */
        SELECT COUNT(*)
          INTO l_cnt
          FROM WWI_FIN.AP_INVOICE_HDR h
         WHERE h.SUPP_ID = p_supp_id
           AND UPPER(REGEXP_REPLACE(h.INVOICE_NUM, '[^A-Za-z0-9]', ''))
               = UPPER(REGEXP_REPLACE(p_invoice_num, '[^A-Za-z0-9]', ''))
           AND h.STATUS_CD <> 'CN'
           AND (p_invoice_id IS NULL OR h.INVOICE_ID <> p_invoice_id);

        RETURN l_cnt > 0;
    EXCEPTION
        WHEN OTHERS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_AP_INVOICE.is_duplicate',
                                                 p_invoice_num, SQLERRM);
            RAISE;
    END is_duplicate;

    FUNCTION match_line
    (
        p_invoice_line_id IN WWI_FIN.AP_INVOICE_LINE.INVOICE_LINE_ID%TYPE,
        p_match_type_cd   IN VARCHAR2 DEFAULT '3WAY'
    ) RETURN t_match_result
    IS
        l_res           t_match_result;
        l_region_cd     WWI_FIN.AP_INVOICE_HDR.REGION_CD%TYPE;
        l_inv_qty       WWI_FIN.AP_INVOICE_LINE.QTY%TYPE;
        l_inv_price     WWI_FIN.AP_INVOICE_LINE.UNIT_PRICE_AMT%TYPE;
        l_inv_amt       WWI_FIN.AP_INVOICE_LINE.LINE_AMT%TYPE;
        l_po_qty        WWI_PROC.PURCHASE_ORDER_LINE.ORDER_QTY%TYPE;
        l_po_price      WWI_PROC.PURCHASE_ORDER_LINE.UNIT_PRICE_AMT%TYPE;
        l_recv_qty      NUMBER := 0;
        l_qty_tol_pct   NUMBER;
        l_price_tol_pct NUMBER;
    BEGIN
        SELECT il.QTY, il.UNIT_PRICE_AMT, il.LINE_AMT, il.PO_LINE_ID, ih.REGION_CD
          INTO l_inv_qty, l_inv_price, l_inv_amt, l_res.po_line_id, l_region_cd
          FROM WWI_FIN.AP_INVOICE_LINE il
          JOIN WWI_FIN.AP_INVOICE_HDR ih
            ON ih.INVOICE_ID = il.INVOICE_ID
         WHERE il.INVOICE_LINE_ID = p_invoice_line_id;

        IF l_res.po_line_id IS NULL THEN
            l_res.match_status_cd := 'NONPO';
            l_res.hold_cd         := NULL;
            RETURN l_res;
        END IF;

        SELECT pl.ORDER_QTY, pl.UNIT_PRICE_AMT
          INTO l_po_qty, l_po_price
          FROM WWI_PROC.PURCHASE_ORDER_LINE pl
         WHERE pl.PO_LINE_ID = l_res.po_line_id;

        IF p_match_type_cd = '3WAY' THEN
            SELECT NVL(SUM(NVL(rl.ACCEPTED_QTY, rl.RECEIVED_QTY)), 0)
              INTO l_recv_qty
              FROM WWI_PROC.PO_RECEIPT_LINE rl
             WHERE rl.PO_LINE_ID = l_res.po_line_id
               AND NVL(rl.INSPECTION_STATUS_CD, 'ACC') <> 'REJ';
        ELSE
            l_recv_qty := l_inv_qty;
        END IF;

        IF l_region_cd = 'EU' THEN
            l_qty_tol_pct   := c_qty_tol_pct_eu;
            l_price_tol_pct := c_price_tol_pct_eu;
        ELSIF l_region_cd = 'APAC' THEN
            l_qty_tol_pct   := c_qty_tol_pct_apac;
            l_price_tol_pct := c_price_tol_pct_apac;
        ELSE
            l_qty_tol_pct   := c_qty_tol_pct_na;
            l_price_tol_pct := c_price_tol_pct_na;
        END IF;

        l_res.qty_variance := CASE
                                  WHEN NVL(l_recv_qty, 0) = 0 THEN 100
                                  ELSE ROUND((l_inv_qty - l_recv_qty) / l_recv_qty * 100, 4)
                              END;

        l_res.price_variance := CASE
                                    WHEN NVL(l_po_price, 0) = 0 THEN 0
                                    ELSE ROUND((l_inv_price - l_po_price) / l_po_price * 100, 4)
                                END;

        l_res.amount_variance := ROUND(l_inv_amt - (l_recv_qty * NVL(l_po_price, 0)), 2);

        IF ABS(l_res.qty_variance) > l_qty_tol_pct THEN
            l_res.match_status_cd := 'FAIL';
            l_res.hold_cd         := CASE WHEN l_inv_qty > l_recv_qty
                                          THEN 'QTY_REC' ELSE 'QTY_ORD' END;
        ELSIF ABS(l_res.price_variance) > l_price_tol_pct THEN
            l_res.match_status_cd := 'FAIL';
            l_res.hold_cd         := 'PRICE';
        ELSIF ABS(l_res.amount_variance) > c_amt_tol_abs
              AND l_region_cd <> 'APAC' THEN
            /* APAC never enforced the absolute band - rounding on JPY made it
               fire on almost every line                                     */
            l_res.match_status_cd := 'FAIL';
            l_res.hold_cd         := 'AMOUNT';
        ELSE
            l_res.match_status_cd := 'PASS';
            l_res.hold_cd         := NULL;
        END IF;

        RETURN l_res;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20104,
                'PKG_AP_INVOICE.match_line: invoice line ' || p_invoice_line_id
                || ' has no matching PO line');
    END match_line;

    PROCEDURE apply_hold
    (
        p_invoice_id IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_hold_cd    IN WWI_FIN.AP_INVOICE_HOLD.HOLD_CD%TYPE,
        p_hold_desc  IN WWI_FIN.AP_INVOICE_HOLD.HOLD_DESC%TYPE DEFAULT NULL
    )
    IS
        l_exists PLS_INTEGER;
    BEGIN
        SELECT COUNT(*)
          INTO l_exists
          FROM WWI_FIN.AP_INVOICE_HOLD
         WHERE INVOICE_ID = p_invoice_id
           AND HOLD_CD    = p_hold_cd
           AND RELEASED_DT IS NULL;

        IF l_exists = 0 THEN
            INSERT INTO WWI_FIN.AP_INVOICE_HOLD
                (INVOICE_ID, HOLD_CD, HOLD_DESC, ACTIVE_FLAG, CREATED_DT, CREATED_BY)
            VALUES
                (p_invoice_id, p_hold_cd,
                 NVL(p_hold_desc, 'Applied by PKG_AP_INVOICE'),
                 'Y', SYSDATE, USER);
        END IF;

        UPDATE WWI_FIN.AP_INVOICE_HDR
           SET STATUS_CD   = 'HO',
               LAST_UPD_DT = SYSDATE,
               LAST_UPD_BY = USER
         WHERE INVOICE_ID = p_invoice_id
           AND STATUS_CD IN ('EN', 'VA');
    EXCEPTION
        WHEN OTHERS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_AP_INVOICE.apply_hold',
                                                 TO_CHAR(p_invoice_id), SQLERRM);
            RAISE;
    END apply_hold;

    PROCEDURE release_hold
    (
        p_invoice_id  IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_hold_cd     IN WWI_FIN.AP_INVOICE_HOLD.HOLD_CD%TYPE,
        p_released_by IN WWI_FIN.AP_INVOICE_HOLD.RELEASED_BY%TYPE
    )
    IS
        l_open_holds PLS_INTEGER;
    BEGIN
        UPDATE WWI_FIN.AP_INVOICE_HOLD
           SET RELEASED_DT = SYSDATE,
               RELEASED_BY = p_released_by,
               ACTIVE_FLAG = 'N'
         WHERE INVOICE_ID = p_invoice_id
           AND HOLD_CD    = p_hold_cd
           AND RELEASED_DT IS NULL;

        SELECT COUNT(*)
          INTO l_open_holds
          FROM WWI_FIN.AP_INVOICE_HOLD
         WHERE INVOICE_ID = p_invoice_id
           AND RELEASED_DT IS NULL;

        IF l_open_holds = 0 THEN
            UPDATE WWI_FIN.AP_INVOICE_HDR
               SET STATUS_CD   = 'VA',
                   LAST_UPD_DT = SYSDATE,
                   LAST_UPD_BY = p_released_by
             WHERE INVOICE_ID = p_invoice_id
               AND STATUS_CD  = 'HO';
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_AP_INVOICE.release_hold',
                                                 TO_CHAR(p_invoice_id), SQLERRM);
            RAISE;
    END release_hold;

    PROCEDURE validate_invoice
    (
        p_invoice_id  IN  WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_hold_count  OUT PLS_INTEGER,
        p_status_cd   OUT WWI_FIN.AP_INVOICE_HDR.STATUS_CD%TYPE
    )
    IS
        CURSOR c_lines (cp_invoice_id WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE) IS
            SELECT il.INVOICE_LINE_ID, il.LINE_AMT, il.TAX_CD, il.PO_LINE_ID
              FROM WWI_FIN.AP_INVOICE_LINE il
             WHERE il.INVOICE_ID = cp_invoice_id
             ORDER BY il.LINE_NUM;

        TYPE t_line_tab IS TABLE OF c_lines%ROWTYPE INDEX BY PLS_INTEGER;
        l_lines        t_line_tab;

        l_hdr          WWI_FIN.AP_INVOICE_HDR%ROWTYPE;
        l_match        t_match_result;
        l_period_stat  WWI_FIN.GL_PERIOD_STATUS.STATUS_CD%TYPE;
        l_line_total   NUMBER := 0;
        l_calc_tax     NUMBER := 0;
        l_due_dt       DATE;
    BEGIN
        p_hold_count := 0;

        SELECT *
          INTO l_hdr
          FROM WWI_FIN.AP_INVOICE_HDR
         WHERE INVOICE_ID = p_invoice_id;

        IF l_hdr.STATUS_CD NOT IN ('EN', 'VA', 'HO') THEN
            RAISE_APPLICATION_ERROR(-20105,
                'PKG_AP_INVOICE.validate_invoice: invoice ' || p_invoice_id
                || ' is in status ' || l_hdr.STATUS_CD);
        END IF;

        IF is_duplicate(l_hdr.SUPP_ID, l_hdr.INVOICE_NUM, l_hdr.INVOICE_ID) THEN
            apply_hold(p_invoice_id, 'DUP', 'Duplicate supplier invoice number');
            p_hold_count := p_hold_count + 1;
        END IF;

        BEGIN
            SELECT ps.STATUS_CD
              INTO l_period_stat
              FROM WWI_FIN.GL_PERIOD_STATUS ps
             WHERE ps.PERIOD_CD = NVL(l_hdr.PERIOD_CD,
                                      WWI_REF.FN_FISCAL_PERIOD(l_hdr.GL_DATE, l_hdr.REGION_CD))
               AND ps.REGION_CD = l_hdr.REGION_CD;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                l_period_stat := 'UNKNOWN';
        END;

        IF l_period_stat IN ('C', 'P') THEN
            apply_hold(p_invoice_id, 'PERIOD', 'GL period ' || l_hdr.PERIOD_CD || ' not open');
            p_hold_count := p_hold_count + 1;
        END IF;

        OPEN c_lines(p_invoice_id);
        LOOP
            FETCH c_lines BULK COLLECT INTO l_lines LIMIT c_bulk_limit;
            EXIT WHEN l_lines.COUNT = 0;

            FOR i IN 1 .. l_lines.COUNT LOOP
                l_line_total := l_line_total + NVL(l_lines(i).LINE_AMT, 0);

                l_calc_tax := l_calc_tax
                              + WWI_FIN.FN_TAX_AMOUNT(NVL(l_lines(i).LINE_AMT, 0),
                                                      l_hdr.REGION_CD,
                                                      l_lines(i).TAX_CD,
                                                      NULL,
                                                      l_hdr.INVOICE_DT,
                                                      NVL(l_hdr.REVERSE_CHARGE_FLAG, 'N'));

                IF l_lines(i).PO_LINE_ID IS NOT NULL THEN
                    l_match := match_line(l_lines(i).INVOICE_LINE_ID,
                                          NVL(l_hdr.MATCH_TYPE_CD, '3WAY'));

                    UPDATE WWI_FIN.AP_INVOICE_LINE
                       SET MATCH_STATUS_CD  = l_match.match_status_cd,
                           QTY_VARIANCE_PCT = l_match.qty_variance,
                           PRICE_VARIANCE_PCT = l_match.price_variance,
                           LAST_UPD_DT      = SYSDATE
                     WHERE INVOICE_LINE_ID = l_lines(i).INVOICE_LINE_ID;

                    IF l_match.match_status_cd = 'FAIL' THEN
                        apply_hold(p_invoice_id, l_match.hold_cd,
                                   'Match failure on line ' || l_lines(i).INVOICE_LINE_ID);
                        p_hold_count := p_hold_count + 1;
                    END IF;
                END IF;
            END LOOP;

            EXIT WHEN c_lines%NOTFOUND;
        END LOOP;
        CLOSE c_lines;

        IF ABS(NVL(l_hdr.INVOICE_AMT, 0)
               - (l_line_total + NVL(l_hdr.TAX_AMT, 0))) > 0.01 THEN
            apply_hold(p_invoice_id, 'DIST_VAR',
                       'Header amount does not equal lines plus tax');
            p_hold_count := p_hold_count + 1;
        END IF;

        /* EU reverse-charge invoices carry zero tax on the document but the
           calculated tax is still posted, so the variance test is skipped   */
        IF NVL(l_hdr.REVERSE_CHARGE_FLAG, 'N') = 'N'
           AND ABS(NVL(l_hdr.TAX_AMT, 0) - l_calc_tax) > 1 THEN
            apply_hold(p_invoice_id, 'TAX_VAR',
                       'Tax variance ' || TO_CHAR(ROUND(NVL(l_hdr.TAX_AMT, 0) - l_calc_tax, 2)));
            p_hold_count := p_hold_count + 1;
        END IF;

        l_due_dt := WWI_FIN.FN_DUE_DATE(NVL(l_hdr.INVOICE_DT, l_hdr.RECEIVED_DT),
                                        l_hdr.PAYMENT_TERMS_CD,
                                        l_hdr.REGION_CD);

        IF p_hold_count = 0 THEN
            p_status_cd := 'VA';
        ELSE
            p_status_cd := 'HO';
        END IF;

        UPDATE WWI_FIN.AP_INVOICE_HDR
           SET STATUS_CD      = p_status_cd,
               DUE_DT         = l_due_dt,
               VALIDATED_DT   = SYSDATE,
               CALC_TAX_AMT   = l_calc_tax,
               LAST_UPD_DT    = SYSDATE,
               LAST_UPD_BY    = USER
         WHERE INVOICE_ID = p_invoice_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20101,
                'PKG_AP_INVOICE.validate_invoice: invoice ' || p_invoice_id || ' not found');
        WHEN OTHERS THEN
            IF c_lines%ISOPEN THEN
                CLOSE c_lines;
            END IF;
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_AP_INVOICE.validate_invoice',
                                                 TO_CHAR(p_invoice_id), SQLERRM);
            RAISE;
    END validate_invoice;

    PROCEDURE approve_invoice
    (
        p_invoice_id  IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_approved_by IN WWI_FIN.AP_INVOICE_HDR.APPROVED_BY%TYPE
    )
    IS
        l_status     WWI_FIN.AP_INVOICE_HDR.STATUS_CD%TYPE;
        l_holds      PLS_INTEGER;
        l_region_cd  WWI_FIN.AP_INVOICE_HDR.REGION_CD%TYPE;
        l_amount     WWI_FIN.AP_INVOICE_HDR.INVOICE_AMT%TYPE;
    BEGIN
        SELECT STATUS_CD, REGION_CD, INVOICE_AMT
          INTO l_status, l_region_cd, l_amount
          FROM WWI_FIN.AP_INVOICE_HDR
         WHERE INVOICE_ID = p_invoice_id
           FOR UPDATE;

        SELECT COUNT(*)
          INTO l_holds
          FROM WWI_FIN.AP_INVOICE_HOLD
         WHERE INVOICE_ID = p_invoice_id
           AND RELEASED_DT IS NULL;

        IF l_holds > 0 THEN
            RAISE_APPLICATION_ERROR(-20105,
                'PKG_AP_INVOICE.approve_invoice: ' || l_holds || ' open hold(s)');
        END IF;

        IF l_status <> 'VA' THEN
            RAISE_APPLICATION_ERROR(-20105,
                'PKG_AP_INVOICE.approve_invoice: invoice not validated (status '
                || l_status || ')');
        END IF;

        /* the EU entity requires a second approver over 50k; NA and APAC
           inherited the limit from the 2003 delegation-of-authority sheet   */
        IF l_region_cd = 'EU' AND l_amount > 50000 THEN
            INSERT INTO WWI_FIN.AP_INVOICE_HOLD
                (INVOICE_ID, HOLD_CD, HOLD_DESC, ACTIVE_FLAG, CREATED_DT, CREATED_BY)
            VALUES
                (p_invoice_id, 'SECOND_APPR', 'Second approver required (EU DOA)',
                 'Y', SYSDATE, p_approved_by);
            RETURN;
        END IF;

        UPDATE WWI_FIN.AP_INVOICE_HDR
           SET STATUS_CD   = 'AP',
               APPROVED_BY = p_approved_by,
               APPROVED_DT = SYSDATE,
               LAST_UPD_DT = SYSDATE,
               LAST_UPD_BY = p_approved_by
         WHERE INVOICE_ID = p_invoice_id;

        WWI_FIN.PKG_GL_POSTING.create_invoice_journal(p_invoice_id);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20101,
                'PKG_AP_INVOICE.approve_invoice: invoice ' || p_invoice_id || ' not found');
    END approve_invoice;

    PROCEDURE cancel_invoice
    (
        p_invoice_id   IN WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE,
        p_reason_cd    IN VARCHAR2,
        p_cancelled_by IN WWI_FIN.AP_INVOICE_HDR.LAST_UPD_BY%TYPE
    )
    IS
        l_paid_amt WWI_FIN.AP_INVOICE_HDR.PAID_AMT%TYPE;
        l_status   WWI_FIN.AP_INVOICE_HDR.STATUS_CD%TYPE;
    BEGIN
        SELECT NVL(PAID_AMT, 0), STATUS_CD
          INTO l_paid_amt, l_status
          FROM WWI_FIN.AP_INVOICE_HDR
         WHERE INVOICE_ID = p_invoice_id
           FOR UPDATE;

        IF l_paid_amt > 0 THEN
            RAISE_APPLICATION_ERROR(-20105,
                'PKG_AP_INVOICE.cancel_invoice: invoice has payments applied');
        END IF;

        UPDATE WWI_FIN.AP_INVOICE_HOLD
           SET RELEASED_DT = SYSDATE,
               RELEASED_BY = p_cancelled_by,
               ACTIVE_FLAG = 'N'
         WHERE INVOICE_ID  = p_invoice_id
           AND RELEASED_DT IS NULL;

        UPDATE WWI_FIN.AP_INVOICE_HDR
           SET STATUS_CD        = 'CN',
               CANCEL_REASON_CD = p_reason_cd,
               LAST_UPD_DT      = SYSDATE,
               LAST_UPD_BY      = p_cancelled_by
         WHERE INVOICE_ID = p_invoice_id;

        IF l_status = 'AP' THEN
            WWI_FIN.PKG_GL_POSTING.reverse_document_journal('AP_INV', p_invoice_id,
                                                            'Invoice cancelled');
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20101,
                'PKG_AP_INVOICE.cancel_invoice: invoice ' || p_invoice_id || ' not found');
    END cancel_invoice;

    PROCEDURE validate_batch
    (
        p_region_cd     IN  VARCHAR2 DEFAULT NULL,
        p_max_rows      IN  PLS_INTEGER DEFAULT 100000,
        p_validated_cnt OUT PLS_INTEGER,
        p_held_cnt      OUT PLS_INTEGER
    )
    IS
        CURSOR c_pending IS
            SELECT h.INVOICE_ID
              FROM WWI_FIN.AP_INVOICE_HDR h
             WHERE h.STATUS_CD = 'EN'
               AND (p_region_cd IS NULL OR h.REGION_CD = p_region_cd)
             ORDER BY h.RECEIVED_DT, h.INVOICE_ID;

        TYPE t_id_tab IS TABLE OF WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE;
        l_ids       t_id_tab;
        l_holds     PLS_INTEGER;
        l_status    WWI_FIN.AP_INVOICE_HDR.STATUS_CD%TYPE;
        l_processed PLS_INTEGER := 0;
    BEGIN
        p_validated_cnt := 0;
        p_held_cnt      := 0;

        OPEN c_pending;
        LOOP
            FETCH c_pending BULK COLLECT INTO l_ids LIMIT c_bulk_limit;
            EXIT WHEN l_ids.COUNT = 0;

            FOR i IN 1 .. l_ids.COUNT LOOP
                BEGIN
                    validate_invoice(l_ids(i), l_holds, l_status);

                    IF l_status = 'VA' THEN
                        p_validated_cnt := p_validated_cnt + 1;
                    ELSE
                        p_held_cnt := p_held_cnt + 1;
                    END IF;

                    COMMIT;
                EXCEPTION
                    WHEN OTHERS THEN
                        ROLLBACK;
                        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_AP_INVOICE.validate_batch',
                                                             TO_CHAR(l_ids(i)), SQLERRM);
                END;

                l_processed := l_processed + 1;
            END LOOP;

            EXIT WHEN l_processed >= p_max_rows;
            EXIT WHEN c_pending%NOTFOUND;
        END LOOP;
        CLOSE c_pending;
    EXCEPTION
        WHEN OTHERS THEN
            IF c_pending%ISOPEN THEN
                CLOSE c_pending;
            END IF;
            RAISE;
    END validate_batch;

END PKG_AP_INVOICE;
/
