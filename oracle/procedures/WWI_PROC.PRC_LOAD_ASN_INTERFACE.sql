/* ============================================================================
 * Object      : WWI_PROC.PRC_LOAD_ASN_INTERFACE (procedure)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.PKG_RECEIPTS, WWI_PROC.PURCHASE_ORDER_HDR,
 *               WWI_PROC.PURCHASE_ORDER_LINE, WWI_REF.SOURCE_SYSTEM_REF,
 *               WWI_REF.PKG_CODE_TRANSLATION, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : DBMS_JOB 'ERP_EDI_ASN' (every 15 minutes)
 * Notes       : EDI 856 advance shipping notices arriving over the EDI
 *               gateway link. Supplier item codes are translated through the
 *               ITEM_XREF code set; an untranslated code is passed through,
 *               which is how the 2016 mis-receipt happened.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_PROC.PRC_LOAD_ASN_INTERFACE
(
    p_region_cd    IN  VARCHAR2,
    p_receipt_cnt  OUT PLS_INTEGER,
    p_line_cnt     OUT PLS_INTEGER,
    p_rejected_cnt OUT PLS_INTEGER
)
IS
    TYPE t_ref IS REF CURSOR;
    TYPE t_asn_rec IS RECORD (
        asn_num       VARCHAR2(40),
        po_num        WWI_PROC.PURCHASE_ORDER_HDR.PO_NBR%TYPE,
        supp_item_cd  VARCHAR2(40),
        ship_qty      NUMBER,
        uom_cd        WWI_PROC.PO_RECEIPT_LINE.UOM_CD%TYPE,
        warehouse_cd  WWI_PROC.PO_RECEIPT_HDR.WAREHOUSE_CD%TYPE,
        ship_dt       DATE
    );
    TYPE t_asn_tab IS TABLE OF t_asn_rec;

    l_link_name  WWI_REF.SOURCE_SYSTEM_REF.CONNECTION_PARAM_NAME%TYPE;
    l_sql        VARCHAR2(4000);
    l_cur        t_ref;
    l_rows       t_asn_tab;
    l_po_id      WWI_PROC.PURCHASE_ORDER_HDR.PO_ID%TYPE;
    l_po_line_id WWI_PROC.PURCHASE_ORDER_LINE.PO_LINE_ID%TYPE;
    l_product_cd VARCHAR2(40);
    l_receipt_id WWI_PROC.PO_RECEIPT_HDR.RECEIPT_ID%TYPE;
    l_rl_id      WWI_PROC.PO_RECEIPT_LINE.RECEIPT_LINE_ID%TYPE;
    l_prev_asn   VARCHAR2(40) := '~';
BEGIN
    p_receipt_cnt  := 0;
    p_line_cnt     := 0;
    p_rejected_cnt := 0;

    SELECT CONNECTION_PARAM_NAME
      INTO l_link_name
      FROM WWI_REF.SOURCE_SYSTEM_REF
     WHERE SOURCE_SYS_CD = 'EDI';

    l_sql := 'SELECT asn_num, po_num, supp_item_cd, ship_qty, uom_cd, '
          || 'warehouse_cd, ship_dt FROM asn_inbound@' || l_link_name || ' '
          || 'WHERE processed_flag = ''N'' AND region_cd = :r '
          || 'ORDER BY asn_num, line_num';

    OPEN l_cur FOR l_sql USING p_region_cd;
    LOOP
        FETCH l_cur BULK COLLECT INTO l_rows LIMIT 100;
        EXIT WHEN l_rows.COUNT = 0;

        FOR i IN 1 .. l_rows.COUNT LOOP
            BEGIN
                SELECT PO_ID
                  INTO l_po_id
                  FROM WWI_PROC.PURCHASE_ORDER_HDR
                 WHERE PO_NBR = l_rows(i).po_num;

                l_product_cd :=
                    WWI_REF.PKG_CODE_TRANSLATION.translate('ITEM_XREF',
                                                           l_rows(i).supp_item_cd,
                                                           'EDI', p_region_cd);

                SELECT pl.PO_LINE_ID
                  INTO l_po_line_id
                  FROM WWI_PROC.PURCHASE_ORDER_LINE pl
                  JOIN WWI_MDM.PRODUCT_MASTER pm
                    ON pm.PRODUCT_ID = pl.PRODUCT_ID
                 WHERE pl.PO_ID = l_po_id
                   AND pm.ITEM_NBR = l_product_cd
                   AND NVL(pl.LINE_STATUS_CD, 'OP') NOT IN ('CL', 'CA')
                   AND ROWNUM = 1;

                IF l_rows(i).asn_num <> l_prev_asn THEN
                    WWI_PROC.PKG_RECEIPTS.create_receipt(
                        p_po_id        => l_po_id,
                        p_warehouse_cd => l_rows(i).warehouse_cd,
                        p_packing_slip => l_rows(i).asn_num,
                        p_received_by  => 'EDI',
                        p_receipt_id   => l_receipt_id);

                    l_prev_asn    := l_rows(i).asn_num;
                    p_receipt_cnt := p_receipt_cnt + 1;
                END IF;

                WWI_PROC.PKG_RECEIPTS.receive_line(
                    p_receipt_id      => l_receipt_id,
                    p_po_line_id      => l_po_line_id,
                    p_received_qty    => l_rows(i).ship_qty,
                    p_uom_cd          => l_rows(i).uom_cd,
                    p_receipt_line_id => l_rl_id);

                /* NA books the ASN as inspected on arrival; EU and APAC leave
                   it pending for the goods inspection step                  */
                IF p_region_cd = 'NA' THEN
                    WWI_PROC.PKG_RECEIPTS.record_inspection(
                        p_receipt_line_id => l_rl_id,
                        p_accepted_qty    => l_rows(i).ship_qty,
                        p_rejected_qty    => 0,
                        p_reject_reason   => NULL,
                        p_inspector       => 'EDI');
                END IF;

                p_line_cnt := p_line_cnt + 1;
                COMMIT;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    ROLLBACK;
                    p_rejected_cnt := p_rejected_cnt + 1;
                    l_prev_asn := '~';
                    WWI_AUDIT.PKG_DATA_QUALITY.log_reject('EDI', 'asn_inbound',
                        l_rows(i).asn_num, 'NO_MATCHING_PO_LINE',
                        'po ' || l_rows(i).po_num || ' item '
                        || l_rows(i).supp_item_cd, 'E');
                WHEN OTHERS THEN
                    ROLLBACK;
                    p_rejected_cnt := p_rejected_cnt + 1;
                    l_prev_asn := '~';
                    WWI_AUDIT.PKG_DATA_QUALITY.log_reject('EDI', 'asn_inbound',
                        l_rows(i).asn_num, 'ASN_FAILED', SQLERRM, 'E');
            END;
        END LOOP;
    END LOOP;
    CLOSE l_cur;
EXCEPTION
    WHEN OTHERS THEN
        IF l_cur%ISOPEN THEN
            CLOSE l_cur;
        END IF;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_LOAD_ASN_INTERFACE',
                                             p_region_cd, SQLERRM);
        RAISE;
END PRC_LOAD_ASN_INTERFACE;
/
