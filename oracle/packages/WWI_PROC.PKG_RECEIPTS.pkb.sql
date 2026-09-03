/* ============================================================================
 * Object      : WWI_PROC.PKG_RECEIPTS (package body)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_PROC.PKG_RECEIPTS, WWI_PROC.PO_RECEIPT_HDR,
 *               WWI_PROC.PO_RECEIPT_LINE, WWI_PROC.GOODS_RETURN_LINE,
 *               WWI_PROC.PURCHASE_ORDER_LINE, WWI_PROC.PURCHASE_ORDER_HDR,
 *               WWI_MDM.PKG_PRODUCT_MASTER, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_PROC.PKG_RECEIPTS AS

    FUNCTION over_receipt_tolerance_pct
    (
        p_region_cd IN VARCHAR2
    ) RETURN NUMBER
    IS
    BEGIN
        /* NA warehouses were always allowed to take an overage; EU refuses
           anything over the ordered quantity; APAC uses a wide band because
           of the case-pack rounding on inbound container loads.            */
        RETURN CASE p_region_cd
                   WHEN 'EU'   THEN 0
                   WHEN 'APAC' THEN 10
                   ELSE 5
               END;
    END over_receipt_tolerance_pct;

    PROCEDURE create_receipt
    (
        p_po_id        IN  WWI_PROC.PURCHASE_ORDER_HDR.PO_ID%TYPE,
        p_warehouse_cd IN  WWI_PROC.PO_RECEIPT_HDR.WAREHOUSE_CD%TYPE,
        p_packing_slip IN  WWI_PROC.PO_RECEIPT_HDR.PACKING_SLIP_NBR%TYPE,
        p_received_by  IN  WWI_PROC.PO_RECEIPT_HDR.RECEIVED_BY_CD%TYPE,
        p_receipt_id   OUT WWI_PROC.PO_RECEIPT_HDR.RECEIPT_ID%TYPE
    )
    IS
        l_supp_id   WWI_PROC.PURCHASE_ORDER_HDR.SUPP_ID%TYPE;
        l_status    WWI_PROC.PURCHASE_ORDER_HDR.PO_STATUS_CD%TYPE;
        l_dup_cnt   PLS_INTEGER;
    BEGIN
        SELECT SUPP_ID, PO_STATUS_CD
          INTO l_supp_id, l_status
          FROM WWI_PROC.PURCHASE_ORDER_HDR
         WHERE PO_ID = p_po_id;

        IF l_status NOT IN ('AP', 'OP') THEN
            RAISE_APPLICATION_ERROR(-20313,
                'PKG_RECEIPTS.create_receipt: PO ' || p_po_id
                || ' is not open for receipt (status ' || l_status || ')');
        END IF;

        SELECT COUNT(*)
          INTO l_dup_cnt
          FROM WWI_PROC.PO_RECEIPT_HDR
         WHERE SUPP_ID          = l_supp_id
           AND PACKING_SLIP_NBR = p_packing_slip
           AND RECEIPT_STATUS_CD <> 'CANCELLED';

        IF l_dup_cnt > 0 THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_RECEIPTS.create_receipt',
                                                 p_packing_slip,
                                                 'packing slip already received on PO '
                                                 || p_po_id);
        END IF;

        p_receipt_id := WWI_PROC.SEQ_PO_RECEIPT_HDR.NEXTVAL;

        INSERT INTO WWI_PROC.PO_RECEIPT_HDR
            (RECEIPT_ID, RECEIPT_NBR, SUPP_ID, WAREHOUSE_CD, RECEIPT_DT,
             PACKING_SLIP_NBR, RECEIVED_BY_CD, RECEIPT_STATUS_CD, CREATED_DT, UPDATED_DT)
        VALUES
            (p_receipt_id, 'RCV' || TO_CHAR(p_receipt_id), l_supp_id,
             p_warehouse_cd, TRUNC(SYSDATE), p_packing_slip, p_received_by,
             'OPEN', SYSDATE, SYSDATE);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20311,
                'PKG_RECEIPTS.create_receipt: PO ' || p_po_id || ' not found');
    END create_receipt;

    PROCEDURE receive_line
    (
        p_receipt_id      IN  WWI_PROC.PO_RECEIPT_LINE.RECEIPT_ID%TYPE,
        p_po_line_id      IN  WWI_PROC.PO_RECEIPT_LINE.PO_LINE_ID%TYPE,
        p_received_qty    IN  WWI_PROC.PO_RECEIPT_LINE.RECEIVED_QTY%TYPE,
        p_uom_cd          IN  WWI_PROC.PO_RECEIPT_LINE.UOM_CD%TYPE,
        p_receipt_line_id OUT WWI_PROC.PO_RECEIPT_LINE.RECEIPT_LINE_ID%TYPE
    )
    IS
        l_line       WWI_PROC.PURCHASE_ORDER_LINE%ROWTYPE;
        l_region_cd  WWI_PROC.PURCHASE_ORDER_HDR.REGION_CD%TYPE;
        l_base_qty   NUMBER;
        l_open_qty   NUMBER;
        l_tolerance  NUMBER;
        l_inspect    VARCHAR2(10);
        l_supp_id    WWI_PROC.PURCHASE_ORDER_HDR.SUPP_ID%TYPE;
        l_prior_rcpt PLS_INTEGER;
    BEGIN
        SELECT * INTO l_line
          FROM WWI_PROC.PURCHASE_ORDER_LINE
         WHERE PO_LINE_ID = p_po_line_id
           FOR UPDATE;

        IF NVL(l_line.LINE_STATUS_CD, 'O') = 'C' THEN
            RAISE_APPLICATION_ERROR(-20313,
                'PKG_RECEIPTS.receive_line: PO line ' || p_po_line_id || ' is closed');
        END IF;

        SELECT REGION_CD, SUPP_ID INTO l_region_cd, l_supp_id
          FROM WWI_PROC.PURCHASE_ORDER_HDR WHERE PO_ID = l_line.PO_ID;

        l_base_qty := WWI_MDM.PKG_PRODUCT_MASTER.convert_uom(l_line.PRODUCT_ID,
                                                              p_received_qty,
                                                              p_uom_cd,
                                                              l_line.UOM_CD);

        l_open_qty  := WWI_PROC.FN_PO_OPEN_QTY(p_po_line_id);
        l_tolerance := over_receipt_tolerance_pct(l_region_cd);

        IF l_base_qty > l_open_qty * (1 + l_tolerance / 100) + 0.0001 THEN
            RAISE_APPLICATION_ERROR(-20312,
                'PKG_RECEIPTS.receive_line: receipt of ' || l_base_qty
                || ' exceeds open quantity ' || l_open_qty
                || ' beyond the ' || l_tolerance || '% tolerance');
        END IF;

        /* EU receipts always go through inspection; NA only inspects if the
           product is flagged; APAC inspects the first three receipts from a
           supplier and then trusts them.                                    */
        SELECT COUNT(*)
          INTO l_prior_rcpt
          FROM WWI_PROC.PO_RECEIPT_HDR rh
         WHERE rh.SUPP_ID = l_supp_id;

        l_inspect := CASE
                         WHEN l_region_cd = 'EU' THEN 'PENDING'
                         WHEN l_region_cd = 'APAC' AND l_prior_rcpt <= 3 THEN 'PENDING'
                         ELSE 'ACC'
                     END;

        p_receipt_line_id := WWI_PROC.SEQ_PO_RECEIPT_LINE.NEXTVAL;

        INSERT INTO WWI_PROC.PO_RECEIPT_LINE
            (RECEIPT_LINE_ID, RECEIPT_ID, PO_LINE_ID, PRODUCT_ID, RECEIVED_QTY,
             ACCEPTED_QTY, REJECTED_QTY, UOM_CD, RECEIPT_DT, INSPECTION_RESULT_CD,
             CREATED_DT, UPDATED_DT)
        VALUES
            (p_receipt_line_id, p_receipt_id, p_po_line_id, l_line.PRODUCT_ID,
             l_base_qty,
             CASE WHEN l_inspect = 'ACC' THEN l_base_qty ELSE NULL END,
             0, l_line.UOM_CD, TRUNC(SYSDATE), l_inspect, SYSDATE, SYSDATE);

        IF l_inspect = 'ACC' THEN
            UPDATE WWI_PROC.PURCHASE_ORDER_LINE
               SET RECEIVED_QTY = NVL(RECEIVED_QTY, 0) + l_base_qty,
                   LINE_STATUS_CD = CASE
                                      WHEN NVL(RECEIVED_QTY, 0) + l_base_qty
                                           >= ORDER_QTY - NVL(CANCELLED_QTY, 0)
                                          THEN 'RC'
                                      ELSE 'PR'
                                  END,
                   UPDATED_DT  = SYSDATE
             WHERE PO_LINE_ID = p_po_line_id;
        END IF;

        UPDATE WWI_PROC.PURCHASE_ORDER_HDR
           SET PO_STATUS_CD   = 'OP',
               UPDATED_DT = SYSDATE
         WHERE PO_ID = l_line.PO_ID
           AND PO_STATUS_CD = 'AP';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20311,
                'PKG_RECEIPTS.receive_line: PO line ' || p_po_line_id || ' not found');
    END receive_line;

    PROCEDURE record_inspection
    (
        p_receipt_line_id IN WWI_PROC.PO_RECEIPT_LINE.RECEIPT_LINE_ID%TYPE,
        p_accepted_qty    IN WWI_PROC.PO_RECEIPT_LINE.ACCEPTED_QTY%TYPE,
        p_rejected_qty    IN WWI_PROC.PO_RECEIPT_LINE.REJECTED_QTY%TYPE,
        p_reject_reason   IN WWI_PROC.PO_RECEIPT_LINE.REJECT_REASON_CD%TYPE DEFAULT NULL,
        p_inspector       IN VARCHAR2
    )
    IS
        l_rl WWI_PROC.PO_RECEIPT_LINE%ROWTYPE;
    BEGIN
        SELECT * INTO l_rl
          FROM WWI_PROC.PO_RECEIPT_LINE
         WHERE RECEIPT_LINE_ID = p_receipt_line_id
           FOR UPDATE;

        IF NVL(p_accepted_qty, 0) + NVL(p_rejected_qty, 0)
           > NVL(l_rl.RECEIVED_QTY, 0) + 0.0001 THEN
            RAISE_APPLICATION_ERROR(-20312,
                'PKG_RECEIPTS.record_inspection: accepted plus rejected exceeds received');
        END IF;

        UPDATE WWI_PROC.PO_RECEIPT_LINE
           SET ACCEPTED_QTY         = p_accepted_qty,
               REJECTED_QTY         = p_rejected_qty,
               REJECT_REASON_CD     = p_reject_reason,
               INSPECTION_RESULT_CD = CASE
                                          WHEN NVL(p_rejected_qty, 0) = 0 THEN 'ACC'
                                          WHEN NVL(p_accepted_qty, 0) = 0 THEN 'REJ'
                                          ELSE 'PART'
                                      END,
               UPDATED_DT           = SYSDATE
         WHERE RECEIPT_LINE_ID = p_receipt_line_id;

        /* inspection identity is held once per receipt, not per line */
        UPDATE WWI_PROC.PO_RECEIPT_HDR
           SET INSPECTED_BY_CD = p_inspector,
               INSPECTED_DT    = SYSDATE,
               UPDATED_DT      = SYSDATE
         WHERE RECEIPT_ID = l_rl.RECEIPT_ID;

        UPDATE WWI_PROC.PURCHASE_ORDER_LINE
           SET RECEIVED_QTY = NVL(RECEIVED_QTY, 0) + NVL(p_accepted_qty, 0),
               UPDATED_DT  = SYSDATE
         WHERE PO_LINE_ID = l_rl.PO_LINE_ID;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20311,
                'PKG_RECEIPTS.record_inspection: receipt line ' || p_receipt_line_id
                || ' not found');
    END record_inspection;

    PROCEDURE return_goods
    (
        p_receipt_line_id IN  WWI_PROC.GOODS_RETURN_LINE.RECEIPT_LINE_ID%TYPE,
        p_return_qty      IN  WWI_PROC.GOODS_RETURN_LINE.RETURN_QTY%TYPE,
        p_reason_cd       IN  WWI_PROC.GOODS_RETURN_LINE.DEFECT_CODE_CD%TYPE,
        p_returned_by     IN  VARCHAR2,
        p_return_line_id  OUT WWI_PROC.GOODS_RETURN_LINE.RETURN_LINE_ID%TYPE
    )
    IS
        l_accepted   NUMBER;
        l_returned   NUMBER;
        l_po_line_id WWI_PROC.PO_RECEIPT_LINE.PO_LINE_ID%TYPE;
    BEGIN
        SELECT NVL(ACCEPTED_QTY, RECEIVED_QTY), PO_LINE_ID
          INTO l_accepted, l_po_line_id
          FROM WWI_PROC.PO_RECEIPT_LINE
         WHERE RECEIPT_LINE_ID = p_receipt_line_id;

        SELECT NVL(SUM(RETURN_QTY), 0)
          INTO l_returned
          FROM WWI_PROC.GOODS_RETURN_LINE
         WHERE RECEIPT_LINE_ID = p_receipt_line_id;

        IF l_returned + p_return_qty > l_accepted + 0.0001 THEN
            RAISE_APPLICATION_ERROR(-20314,
                'PKG_RECEIPTS.return_goods: total returns would exceed the accepted '
                || 'quantity ' || l_accepted);
        END IF;

        p_return_line_id := WWI_PROC.SEQ_GOODS_RETURN_LINE.NEXTVAL;

        INSERT INTO WWI_PROC.GOODS_RETURN_LINE
            (RETURN_LINE_ID, RECEIPT_LINE_ID, PO_LINE_ID, RETURN_QTY, DEFECT_CODE_CD,
             CREDIT_RECEIVED_FLG, CREATED_BY, CREATED_DT)
        VALUES
            (p_return_line_id, p_receipt_line_id, l_po_line_id, p_return_qty, p_reason_cd,
             'N', p_returned_by, SYSDATE);

        UPDATE WWI_PROC.PURCHASE_ORDER_LINE
           SET RECEIVED_QTY = GREATEST(NVL(RECEIVED_QTY, 0) - p_return_qty, 0),
               UPDATED_DT  = SYSDATE
         WHERE PO_LINE_ID = l_po_line_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20311,
                'PKG_RECEIPTS.return_goods: receipt line ' || p_receipt_line_id
                || ' not found');
    END return_goods;

END PKG_RECEIPTS;
/
