/* ============================================================================
 * Object      : WWI_PROC.PRC_RELEASE_REQUISITIONS (procedure)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.REQUISITION_HDR, WWI_PROC.REQUISITION_LINE,
 *               WWI_PROC.PKG_PURCHASE_ORDER, WWI_PROC.VENDOR_CONTRACT,
 *               WWI_MDM.PKG_SUPPLIER_MASTER, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : DBMS_JOB 'ERP_REQ_RELEASE' (every two hours in business hours)
 * Notes       : Turns approved requisition lines into purchase orders, one
 *               PO per supplier and currency. Requisition lines without a
 *               contract are only auto-released in NA; the other regions
 *               route them to a buyer.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_PROC.PRC_RELEASE_REQUISITIONS
(
    p_region_cd  IN  VARCHAR2,
    p_po_cnt     OUT PLS_INTEGER,
    p_line_cnt   OUT PLS_INTEGER,
    p_held_cnt   OUT PLS_INTEGER
)
IS
    CURSOR c_lines IS
        SELECT rl.REQ_LINE_ID,
               rh.REQ_ID,
               rh.REQUESTOR_CD,
               rh.APPROVER_1_CD        AS BUYER_ID,
               rl.SUGGESTED_SUPP_ID    AS SUPP_ID,
               rl.PRODUCT_ID,
               rl.REQ_QTY,
               rl.UOM_CD,
               rl.NEED_BY_DT,
               vc.CONTRACT_ID,
               rh.ESTIMATED_CURR_CD    AS CURRENCY_CD
          FROM WWI_PROC.REQUISITION_LINE rl
          JOIN WWI_PROC.REQUISITION_HDR rh
            ON rh.REQ_ID = rl.REQ_ID
          LEFT JOIN WWI_PROC.VENDOR_CONTRACT vc
            ON vc.CONTRACT_NBR = rl.CONTRACT_NBR
         WHERE rh.REGION_CD = p_region_cd
           AND rh.REQ_STATUS_CD = 'APPROVED'
           AND rl.CONVERTED_PO_ID IS NULL
           AND rl.SUGGESTED_SUPP_ID IS NOT NULL
         ORDER BY rl.SUGGESTED_SUPP_ID, rh.ESTIMATED_CURR_CD, rl.REQ_LINE_ID;

    l_prev_supp   WWI_PROC.REQUISITION_LINE.SUGGESTED_SUPP_ID%TYPE := -1;
    l_prev_ccy    WWI_PROC.REQUISITION_HDR.ESTIMATED_CURR_CD%TYPE := '~';
    l_po_id       WWI_PROC.PURCHASE_ORDER_HDR.PO_ID%TYPE;
    l_po_line_id  WWI_PROC.PURCHASE_ORDER_LINE.PO_LINE_ID%TYPE;
    l_price       NUMBER;
    l_approved    VARCHAR2(20);
BEGIN
    p_po_cnt   := 0;
    p_line_cnt := 0;
    p_held_cnt := 0;

    FOR rec IN c_lines LOOP
        IF rec.CONTRACT_ID IS NULL AND p_region_cd <> 'NA' THEN
            /* EU and APAC will not raise an off contract PO automatically */
            p_held_cnt := p_held_cnt + 1;
            WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL,
                'WWI_PROC.REQUISITION_LINE', TO_CHAR(rec.REQ_LINE_ID),
                'NO_CONTRACT', 'off contract line routed to buyer', 'W');
            CONTINUE;
        END IF;

        l_approved := WWI_MDM.PKG_SUPPLIER_MASTER.is_approved_for_po(rec.SUPP_ID,
                                                                     p_region_cd);

        IF l_approved <> 'APPROVED' THEN
            p_held_cnt := p_held_cnt + 1;
            WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL,
                'WWI_PROC.REQUISITION_LINE', TO_CHAR(rec.REQ_LINE_ID),
                'SUPPLIER_NOT_APPROVED',
                'supplier ' || rec.SUPP_ID || ' not approved in ' || p_region_cd,
                'E');
            CONTINUE;
        END IF;

        IF rec.SUPP_ID <> l_prev_supp OR rec.CURRENCY_CD <> l_prev_ccy THEN
            WWI_PROC.PKG_PURCHASE_ORDER.create_po(
                p_supp_id     => rec.SUPP_ID,
                p_region_cd   => p_region_cd,
                p_currency_cd => rec.CURRENCY_CD,
                p_buyer_id    => rec.BUYER_ID,
                p_contract_id => rec.CONTRACT_ID,
                p_po_id       => l_po_id);

            l_prev_supp := rec.SUPP_ID;
            l_prev_ccy  := rec.CURRENCY_CD;
            p_po_cnt    := p_po_cnt + 1;
        END IF;

        l_price := NULL;

        IF rec.CONTRACT_ID IS NOT NULL THEN
            l_price := WWI_PROC.PKG_PURCHASE_ORDER.contract_price(rec.CONTRACT_ID,
                                                                  rec.PRODUCT_ID,
                                                                  TRUNC(SYSDATE));
        END IF;

        BEGIN
            WWI_PROC.PKG_PURCHASE_ORDER.add_po_line(
                p_po_id      => l_po_id,
                p_product_id => rec.PRODUCT_ID,
                p_order_qty  => rec.REQ_QTY,
                p_uom_cd     => rec.UOM_CD,
                p_unit_price => l_price,
                p_need_by_dt => rec.NEED_BY_DT,
                p_po_line_id => l_po_line_id);

            UPDATE WWI_PROC.REQUISITION_LINE
               SET LINE_STATUS_CD   = 'C',
                   CONVERTED_PO_ID  = l_po_id,
                   CONVERTED_DT     = SYSDATE,
                   UPDATED_DT   = SYSDATE
             WHERE REQ_LINE_ID = rec.REQ_LINE_ID;

            p_line_cnt := p_line_cnt + 1;
            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                p_held_cnt := p_held_cnt + 1;
                WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL,
                    'WWI_PROC.REQUISITION_LINE', TO_CHAR(rec.REQ_LINE_ID),
                    'RELEASE_FAILED', SQLERRM, 'E');
                l_prev_supp := -1;
        END;
    END LOOP;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RELEASE_REQUISITIONS',
                                             p_region_cd, SQLERRM);
        RAISE;
END PRC_RELEASE_REQUISITIONS;
/
