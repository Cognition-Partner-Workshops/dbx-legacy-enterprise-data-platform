/* ============================================================================
 * Object      : WWI_PROC.PKG_PURCHASE_ORDER (package body)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_PROC.PKG_PURCHASE_ORDER, WWI_PROC.PURCHASE_ORDER_HDR,
 *               WWI_PROC.PURCHASE_ORDER_LINE, WWI_PROC.PO_CHANGE_ORDER,
 *               WWI_PROC.VENDOR_CONTRACT_LINE, WWI_MDM.PKG_SUPPLIER_MASTER,
 *               WWI_MDM.PRODUCT_MASTER, WWI_PROC.FN_PO_OPEN_QTY,
 *               WWI_FIN.FN_CONVERT_AMOUNT, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_PROC.PKG_PURCHASE_ORDER AS

    FUNCTION approval_level
    (
        p_region_cd   IN VARCHAR2,
        p_amount      IN NUMBER,
        p_currency_cd IN VARCHAR2 DEFAULT 'USD'
    ) RETURN VARCHAR2
    IS
        l_usd_amt NUMBER;
    BEGIN
        l_usd_amt := WWI_FIN.FN_CONVERT_AMOUNT(NVL(p_amount, 0), p_currency_cd, 'USD',
                                               TRUNC(SYSDATE), 'CORP');

        /* three separate delegation-of-authority sheets, never reconciled */
        IF p_region_cd = 'EU' THEN
            RETURN CASE
                       WHEN l_usd_amt <  5000  THEN 'BUYER'
                       WHEN l_usd_amt <  50000 THEN 'PROC_MGR'
                       WHEN l_usd_amt < 250000 THEN 'FIN_DIR'
                       ELSE 'BOARD'
                   END;
        ELSIF p_region_cd = 'APAC' THEN
            RETURN CASE
                       WHEN l_usd_amt <  2000  THEN 'BUYER'
                       WHEN l_usd_amt <  20000 THEN 'COUNTRY_MGR'
                       WHEN l_usd_amt < 100000 THEN 'REGION_MGR'
                       ELSE 'BOARD'
                   END;
        END IF;

        RETURN CASE
                   WHEN l_usd_amt <  10000  THEN 'BUYER'
                   WHEN l_usd_amt <  100000 THEN 'PROC_MGR'
                   ELSE 'VP'
               END;
    END approval_level;

    FUNCTION contract_price
    (
        p_contract_id IN WWI_PROC.VENDOR_CONTRACT_LINE.CONTRACT_ID%TYPE,
        p_product_id  IN WWI_PROC.VENDOR_CONTRACT_LINE.PRODUCT_ID%TYPE,
        p_order_dt    IN DATE DEFAULT TRUNC(SYSDATE)
    ) RETURN NUMBER
    IS
        l_price NUMBER;
    BEGIN
        SELECT CONTRACT_PRICE
          INTO l_price
          FROM WWI_PROC.VENDOR_CONTRACT_LINE
         WHERE CONTRACT_ID = p_contract_id
           AND PRODUCT_ID  = p_product_id
           AND TRUNC(p_order_dt) BETWEEN PRICE_EFFECTIVE_DT
                                     AND NVL(PRICE_END_DT, DATE '4712-12-31')
           AND ROWNUM = 1;

        RETURN l_price;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
    END contract_price;

    PROCEDURE create_po
    (
        p_supp_id     IN  WWI_PROC.PURCHASE_ORDER_HDR.SUPP_ID%TYPE,
        p_region_cd   IN  WWI_PROC.PURCHASE_ORDER_HDR.REGION_CD%TYPE,
        p_currency_cd IN  WWI_PROC.PURCHASE_ORDER_HDR.ORDER_CURR_CD%TYPE,
        p_buyer_id    IN  WWI_PROC.PURCHASE_ORDER_HDR.BUYER_CD%TYPE,
        p_contract_id IN  WWI_PROC.PURCHASE_ORDER_HDR.CONTRACT_ID%TYPE DEFAULT NULL,
        p_po_id       OUT WWI_PROC.PURCHASE_ORDER_HDR.PO_ID%TYPE
    )
    IS
        l_supplier_state VARCHAR2(20);
    BEGIN
        l_supplier_state := WWI_MDM.PKG_SUPPLIER_MASTER.is_approved_for_po(p_supp_id,
                                                                           p_region_cd, 0);

        IF l_supplier_state <> 'APPROVED' THEN
            RAISE_APPLICATION_ERROR(-20303,
                'PKG_PURCHASE_ORDER.create_po: supplier ' || p_supp_id
                || ' is not approved (' || l_supplier_state || ')');
        END IF;

        p_po_id := WWI_PROC.SEQ_PURCHASE_ORDER_HDR.NEXTVAL;

        INSERT INTO WWI_PROC.PURCHASE_ORDER_HDR
            (PO_ID, PO_NBR, SUPP_ID, REGION_CD, ORDER_CURR_CD, PO_STATUS_CD,
             ORDER_DT, BUYER_CD, CONTRACT_ID, TOTAL_AMT, TAX_AMT,
             PAYMENT_TERMS_CD, INCOTERM_CD, CREATED_DT, CREATED_BY, UPDATED_DT)
        VALUES
            (p_po_id,
             /* PO numbers carry a regional prefix; the DW still parses it */
             CASE p_region_cd WHEN 'EU' THEN 'EU' WHEN 'APAC' THEN 'AP' ELSE 'US' END
             || TO_CHAR(p_po_id),
             p_supp_id, p_region_cd, p_currency_cd, 'DR',
             TRUNC(SYSDATE), p_buyer_id, p_contract_id, 0, 0,
             CASE p_region_cd WHEN 'EU' THEN 'NET30EOM'
                              WHEN 'APAC' THEN 'NET60' ELSE 'NET30' END,
             CASE p_region_cd WHEN 'EU' THEN 'DAP' ELSE 'FOB' END,
             SYSDATE, USER, SYSDATE);
    END create_po;

    PROCEDURE add_po_line
    (
        p_po_id       IN  WWI_PROC.PURCHASE_ORDER_LINE.PO_ID%TYPE,
        p_product_id  IN  WWI_PROC.PURCHASE_ORDER_LINE.PRODUCT_ID%TYPE,
        p_order_qty   IN  WWI_PROC.PURCHASE_ORDER_LINE.ORDER_QTY%TYPE,
        p_uom_cd      IN  WWI_PROC.PURCHASE_ORDER_LINE.UOM_CD%TYPE,
        p_unit_price  IN  WWI_PROC.PURCHASE_ORDER_LINE.UNIT_PRICE%TYPE DEFAULT NULL,
        p_need_by_dt  IN  DATE DEFAULT NULL,
        p_po_line_id  OUT WWI_PROC.PURCHASE_ORDER_LINE.PO_LINE_ID%TYPE
    )
    IS
        l_hdr        WWI_PROC.PURCHASE_ORDER_HDR%ROWTYPE;
        l_price      NUMBER;
        l_line_num   PLS_INTEGER;
        l_lead_days  WWI_PROC.SUPPLIER_SCORECARD.AVG_LEAD_TIME_DAYS%TYPE;
        l_tax_cd     VARCHAR2(20);
    BEGIN
        SELECT * INTO l_hdr
          FROM WWI_PROC.PURCHASE_ORDER_HDR
         WHERE PO_ID = p_po_id
           FOR UPDATE;

        IF l_hdr.PO_STATUS_CD NOT IN ('DR', 'AP') THEN
            RAISE_APPLICATION_ERROR(-20302,
                'PKG_PURCHASE_ORDER.add_po_line: cannot add lines to a PO in status '
                || l_hdr.PO_STATUS_CD);
        END IF;

        SELECT NVL(LEAD_TIME_DAYS, 14)
          INTO l_lead_days
          FROM WWI_MDM.SUPP_MASTER
         WHERE SUPP_ID = l_hdr.SUPP_ID;

        l_price := NVL(p_unit_price,
                       contract_price(l_hdr.CONTRACT_ID, p_product_id, l_hdr.ORDER_DT));

        IF l_price IS NULL THEN
            SELECT UNIT_COST_STD
              INTO l_price
              FROM WWI_MDM.PRODUCT_MASTER
             WHERE PRODUCT_ID = p_product_id;
        END IF;

        /* the tax code is decided by region at line entry and never revisited,
           even if the PO is approved months later                            */
        l_tax_cd := CASE l_hdr.REGION_CD
                        WHEN 'EU'   THEN 'VAT_STD'
                        WHEN 'APAC' THEN 'GST_STD'
                        ELSE 'ST_STD'
                    END;

        SELECT NVL(MAX(LINE_NBR), 0) + 1
          INTO l_line_num
          FROM WWI_PROC.PURCHASE_ORDER_LINE
         WHERE PO_ID = p_po_id;

        p_po_line_id := WWI_PROC.SEQ_PURCHASE_ORDER_LINE.NEXTVAL;

        INSERT INTO WWI_PROC.PURCHASE_ORDER_LINE
            (PO_LINE_ID, PO_ID, LINE_NBR, PRODUCT_ID, ORDER_QTY, RECEIVED_QTY,
             BILLED_QTY, CANCELLED_QTY, UOM_CD, UNIT_PRICE, LINE_AMT,
             TAX_CODE_CD, NEED_BY_DT, LINE_STATUS_CD, CREATED_DT, UPDATED_DT)
        VALUES
            (p_po_line_id, p_po_id, l_line_num, p_product_id, p_order_qty, 0,
             0, 0, p_uom_cd, l_price, ROUND(p_order_qty * l_price, 2),
             l_tax_cd, NVL(p_need_by_dt, TRUNC(SYSDATE) + l_lead_days),
             'OP', SYSDATE, SYSDATE);

        UPDATE WWI_PROC.PURCHASE_ORDER_HDR
           SET TOTAL_AMT   = NVL(TOTAL_AMT, 0) + ROUND(p_order_qty * l_price, 2),
               PROMISED_DT = GREATEST(NVL(PROMISED_DT, TRUNC(SYSDATE)),
                                      NVL(p_need_by_dt, TRUNC(SYSDATE) + l_lead_days)),
               UPDATED_DT = SYSDATE
         WHERE PO_ID = p_po_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20301,
                'PKG_PURCHASE_ORDER.add_po_line: PO or product not found');
    END add_po_line;

    PROCEDURE approve_po
    (
        p_po_id       IN WWI_PROC.PURCHASE_ORDER_HDR.PO_ID%TYPE,
        p_approved_by IN WWI_PROC.PURCHASE_ORDER_HDR.UPDATED_BY%TYPE
    )
    IS
        l_hdr       WWI_PROC.PURCHASE_ORDER_HDR%ROWTYPE;
        l_level     VARCHAR2(20);
        l_supplier  VARCHAR2(20);
        l_line_cnt  PLS_INTEGER;
    BEGIN
        SELECT * INTO l_hdr
          FROM WWI_PROC.PURCHASE_ORDER_HDR
         WHERE PO_ID = p_po_id
           FOR UPDATE;

        IF l_hdr.PO_STATUS_CD <> 'DR' THEN
            RAISE_APPLICATION_ERROR(-20302,
                'PKG_PURCHASE_ORDER.approve_po: PO ' || p_po_id
                || ' is in status ' || l_hdr.PO_STATUS_CD);
        END IF;

        SELECT COUNT(*)
          INTO l_line_cnt
          FROM WWI_PROC.PURCHASE_ORDER_LINE
         WHERE PO_ID = p_po_id;

        IF l_line_cnt = 0 THEN
            RAISE_APPLICATION_ERROR(-20302,
                'PKG_PURCHASE_ORDER.approve_po: PO ' || p_po_id || ' has no lines');
        END IF;

        l_supplier := WWI_MDM.PKG_SUPPLIER_MASTER.is_approved_for_po(l_hdr.SUPP_ID,
                                                                     l_hdr.REGION_CD,
                                                                     l_hdr.TOTAL_AMT);
        IF l_supplier <> 'APPROVED' THEN
            RAISE_APPLICATION_ERROR(-20303,
                'PKG_PURCHASE_ORDER.approve_po: supplier check returned ' || l_supplier);
        END IF;

        l_level := approval_level(l_hdr.REGION_CD, l_hdr.TOTAL_AMT, l_hdr.ORDER_CURR_CD);

        UPDATE WWI_PROC.PURCHASE_ORDER_HDR
           SET PO_STATUS_CD          = 'AP',
               UPDATED_BY        = p_approved_by,
               APPROVAL_STATUS_CD  = l_level,
               UPDATED_DT        = SYSDATE
         WHERE PO_ID = p_po_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20301,
                'PKG_PURCHASE_ORDER.approve_po: PO ' || p_po_id || ' not found');
    END approve_po;

    PROCEDURE apply_change_order
    (
        p_po_line_id  IN WWI_PROC.PURCHASE_ORDER_LINE.PO_LINE_ID%TYPE,
        p_new_qty     IN WWI_PROC.PURCHASE_ORDER_LINE.ORDER_QTY%TYPE,
        p_new_price   IN WWI_PROC.PURCHASE_ORDER_LINE.UNIT_PRICE%TYPE,
        p_reason_cd   IN WWI_PROC.PO_CHANGE_ORDER.CHANGE_REASON_CD%TYPE,
        p_changed_by  IN VARCHAR2
    )
    IS
        l_line      WWI_PROC.PURCHASE_ORDER_LINE%ROWTYPE;
        l_received  NUMBER;
        l_old_amt   NUMBER;
        l_new_amt   NUMBER;
        l_region_cd WWI_PROC.PURCHASE_ORDER_HDR.REGION_CD%TYPE;
    BEGIN
        SELECT * INTO l_line
          FROM WWI_PROC.PURCHASE_ORDER_LINE
         WHERE PO_LINE_ID = p_po_line_id
           FOR UPDATE;

        IF NVL(l_line.LINE_STATUS_CD, 'OP') = 'C' THEN
            RAISE_APPLICATION_ERROR(-20304,
                'PKG_PURCHASE_ORDER.apply_change_order: line ' || p_po_line_id
                || ' is closed');
        END IF;

        SELECT REGION_CD INTO l_region_cd
          FROM WWI_PROC.PURCHASE_ORDER_HDR WHERE PO_ID = l_line.PO_ID;

        l_received := NVL(l_line.RECEIVED_QTY, 0);

        IF p_new_qty < l_received THEN
            RAISE_APPLICATION_ERROR(-20302,
                'PKG_PURCHASE_ORDER.apply_change_order: new quantity is below '
                || 'the received quantity ' || l_received);
        END IF;

        l_old_amt := NVL(l_line.LINE_AMT, 0);
        l_new_amt := ROUND(p_new_qty * NVL(p_new_price, l_line.UNIT_PRICE), 2);

        INSERT INTO WWI_PROC.PO_CHANGE_ORDER
            (CHANGE_ID, PO_ID, PO_LINE_ID, REQUESTED_DT, CHANGE_TYPE_CD,
             CHANGE_REASON_CD, OLD_VALUE_TXT, NEW_VALUE_TXT,
             QTY_DELTA, AMOUNT_DELTA, CHANGE_STATUS_CD, REQUESTED_BY_CD)
        VALUES
            (WWI_PROC.SEQ_PO_CHANGE_ORDER.NEXTVAL, l_line.PO_ID, p_po_line_id, SYSDATE,
             'QTYPRICE', p_reason_cd,
             'qty=' || l_line.ORDER_QTY || '; price=' || l_line.UNIT_PRICE,
             'qty=' || p_new_qty || '; price='
                    || NVL(p_new_price, l_line.UNIT_PRICE),
             p_new_qty - NVL(l_line.ORDER_QTY, 0), l_new_amt - l_old_amt,
             /* EU change orders above 10% need re-approval; elsewhere the
                change is auto-approved on entry                            */
             CASE WHEN l_region_cd = 'EU'
                       AND ABS(l_new_amt - l_old_amt) > l_old_amt * 0.10
                  THEN 'PENDING' ELSE 'APPROVED' END,
             p_changed_by);

        UPDATE WWI_PROC.PURCHASE_ORDER_LINE
           SET ORDER_QTY      = p_new_qty,
               UNIT_PRICE = NVL(p_new_price, UNIT_PRICE),
               LINE_AMT       = l_new_amt,
               UPDATED_DT    = SYSDATE
         WHERE PO_LINE_ID = p_po_line_id;

        UPDATE WWI_PROC.PURCHASE_ORDER_HDR
           SET TOTAL_AMT   = NVL(TOTAL_AMT, 0) - l_old_amt + l_new_amt,
               UPDATED_DT = SYSDATE
         WHERE PO_ID = l_line.PO_ID;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20301,
                'PKG_PURCHASE_ORDER.apply_change_order: line ' || p_po_line_id
                || ' not found');
    END apply_change_order;

    PROCEDURE close_stale_pos
    (
        p_region_cd  IN  VARCHAR2,
        p_stale_days IN  PLS_INTEGER DEFAULT 180,
        p_closed_cnt OUT PLS_INTEGER
    )
    IS
        CURSOR c_stale IS
            SELECT h.PO_ID, l.PO_LINE_ID
              FROM WWI_PROC.PURCHASE_ORDER_HDR h
              JOIN WWI_PROC.PURCHASE_ORDER_LINE l
                ON l.PO_ID = h.PO_ID
             WHERE h.REGION_CD = p_region_cd
               AND h.PO_STATUS_CD IN ('AP', 'OP')
               AND NVL(l.LINE_STATUS_CD, 'OP') <> 'CL'
               AND h.ORDER_DT < TRUNC(SYSDATE) - p_stale_days
               AND WWI_PROC.FN_PO_OPEN_QTY(l.PO_LINE_ID) <= 0
             ORDER BY h.PO_ID, l.PO_LINE_ID;

        TYPE t_row_tab IS TABLE OF c_stale%ROWTYPE INDEX BY PLS_INTEGER;
        l_rows t_row_tab;
    BEGIN
        p_closed_cnt := 0;

        OPEN c_stale;
        LOOP
            FETCH c_stale BULK COLLECT INTO l_rows LIMIT c_bulk_limit;
            EXIT WHEN l_rows.COUNT = 0;

            FOR i IN 1 .. l_rows.COUNT LOOP
                UPDATE WWI_PROC.PURCHASE_ORDER_LINE
                   SET LINE_STATUS_CD  = 'CL',
                       CLOSE_REASON_CD = 'STALE',
                       UPDATED_DT = SYSDATE
                 WHERE PO_LINE_ID = l_rows(i).PO_LINE_ID;

                p_closed_cnt := p_closed_cnt + 1;
            END LOOP;

            COMMIT;
            EXIT WHEN c_stale%NOTFOUND;
        END LOOP;
        CLOSE c_stale;

        UPDATE WWI_PROC.PURCHASE_ORDER_HDR h
           SET h.PO_STATUS_CD   = 'CL',
               h.UPDATED_DT = SYSDATE
         WHERE h.REGION_CD = p_region_cd
           AND h.PO_STATUS_CD IN ('AP', 'OP')
           AND NOT EXISTS (SELECT 1
                             FROM WWI_PROC.PURCHASE_ORDER_LINE l
                            WHERE l.PO_ID = h.PO_ID
                              AND NVL(l.LINE_STATUS_CD, 'OP') <> 'CL');
    EXCEPTION
        WHEN OTHERS THEN
            IF c_stale%ISOPEN THEN
                CLOSE c_stale;
            END IF;
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_PURCHASE_ORDER.close_stale_pos',
                                                 p_region_cd, SQLERRM);
            RAISE;
    END close_stale_pos;

END PKG_PURCHASE_ORDER;
/
