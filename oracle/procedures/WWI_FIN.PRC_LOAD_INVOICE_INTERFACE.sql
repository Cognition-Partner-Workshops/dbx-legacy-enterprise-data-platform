/* ============================================================================
 * Object      : WWI_FIN.PRC_LOAD_INVOICE_INTERFACE (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.AP_INVOICE_HDR, WWI_FIN.AP_INVOICE_LINE,
 *               WWI_FIN.PKG_AP_INVOICE, WWI_FIN.PKG_TAX, WWI_FIN.FN_DUE_DATE,
 *               WWI_MDM.SUPP_MASTER, WWI_AUDIT.PKG_DATA_QUALITY,
 *               remote invoice scanning feed over database link INVSCAN_LINK
 * Called by   : WWI_FIN.PRC_RUN_NIGHTLY_AP
 * History     : The scanning vendor changed twice; the 2007 feed still sends
 *               region NULL, so it is defaulted from the supplier.
 * Notes       : Dynamic SQL is used because the link name differs per
 *               environment and is held in WWI_REF.SOURCE_SYSTEM_REF.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_FIN.PRC_LOAD_INVOICE_INTERFACE
(
    p_region_cd    IN  VARCHAR2,
    p_loaded_cnt   OUT PLS_INTEGER,
    p_rejected_cnt OUT PLS_INTEGER
)
IS
    TYPE t_iface_rec IS RECORD (
        supp_num      WWI_MDM.SUPP_MASTER.SUPP_NUM%TYPE,
        invoice_num   WWI_FIN.AP_INVOICE_HDR.INVOICE_NUM%TYPE,
        invoice_dt    DATE,
        currency_cd   WWI_FIN.AP_INVOICE_HDR.CURRENCY_CD%TYPE,
        gross_amt     NUMBER,
        po_num        WWI_PROC.PURCHASE_ORDER_HDR.PO_NUM%TYPE,
        terms_cd      WWI_FIN.PAYMENT_TERMS.PAYMENT_TERMS_CD%TYPE
    );
    TYPE t_iface_tab IS TABLE OF t_iface_rec;
    TYPE t_ref IS REF CURSOR;

    l_link_name  WWI_REF.SOURCE_SYSTEM_REF.DB_LINK_NAME%TYPE;
    l_sql        VARCHAR2(4000);
    l_cur        t_ref;
    l_rows       t_iface_tab;
    l_supp_id    WWI_MDM.SUPP_MASTER.SUPP_ID%TYPE;
    l_region_cd  WWI_MDM.SUPP_MASTER.REGION_CD%TYPE;
    l_invoice_id WWI_FIN.AP_INVOICE_HDR.INVOICE_ID%TYPE;
    l_tax_amt    NUMBER;
    l_terms_cd   WWI_FIN.PAYMENT_TERMS.PAYMENT_TERMS_CD%TYPE;
BEGIN
    p_loaded_cnt   := 0;
    p_rejected_cnt := 0;

    SELECT DB_LINK_NAME
      INTO l_link_name
      FROM WWI_REF.SOURCE_SYSTEM_REF
     WHERE SRC_SYSTEM_CD = 'INVSCAN';

    l_sql := 'SELECT supp_num, invoice_num, invoice_dt, currency_cd, '
          || 'gross_amt, po_num, terms_cd '
          || 'FROM invoice_staging@' || l_link_name || ' '
          || 'WHERE processed_flag = ''N'' '
          || 'AND (region_cd = :r OR region_cd IS NULL) '
          || 'ORDER BY invoice_dt, invoice_num';

    OPEN l_cur FOR l_sql USING p_region_cd;
    LOOP
        FETCH l_cur BULK COLLECT INTO l_rows LIMIT 200;
        EXIT WHEN l_rows.COUNT = 0;

        FOR i IN 1 .. l_rows.COUNT LOOP
            BEGIN
                SELECT SUPP_ID, REGION_CD
                  INTO l_supp_id, l_region_cd
                  FROM WWI_MDM.SUPP_MASTER
                 WHERE SUPP_NUM = l_rows(i).supp_num;

                IF WWI_FIN.PKG_AP_INVOICE.is_duplicate(l_supp_id,
                                                       l_rows(i).invoice_num) THEN
                    RAISE_APPLICATION_ERROR(-20103,
                        'duplicate invoice ' || l_rows(i).invoice_num);
                END IF;

                l_terms_cd := NVL(l_rows(i).terms_cd,
                                  CASE NVL(l_region_cd, p_region_cd)
                                      WHEN 'EU'   THEN 'N30'
                                      WHEN 'APAC' THEN 'N60'
                                      ELSE 'N45'
                                  END);

                l_invoice_id := WWI_FIN.SEQ_AP_INVOICE.NEXTVAL;

                INSERT INTO WWI_FIN.AP_INVOICE_HDR
                    (INVOICE_ID, INVOICE_NUM, SUPP_ID, REGION_CD, INVOICE_DT,
                     CURRENCY_CD, GROSS_AMT, TAX_AMT, PAYMENT_TERMS_CD, DUE_DT,
                     STATUS_CD, SRC_SYSTEM_CD, CREATED_DT, CREATED_BY)
                VALUES
                    (l_invoice_id, l_rows(i).invoice_num, l_supp_id,
                     NVL(l_region_cd, p_region_cd), l_rows(i).invoice_dt,
                     l_rows(i).currency_cd, l_rows(i).gross_amt, 0, l_terms_cd,
                     WWI_FIN.FN_DUE_DATE(l_rows(i).invoice_dt, l_terms_cd,
                                         NVL(l_region_cd, p_region_cd)),
                     'EN', 'INVSCAN', SYSDATE, USER);

                /* the feed sends a gross amount only; tax is re-derived here
                   so EU reverse charge invoices land with zero tax          */
                l_tax_amt := WWI_FIN.FN_TAX_AMOUNT(
                                 p_line_amt  => l_rows(i).gross_amt,
                                 p_region_cd => NVL(l_region_cd, p_region_cd),
                                 p_tax_cd    => CASE NVL(l_region_cd, p_region_cd)
                                                    WHEN 'EU'   THEN 'VAT_STD'
                                                    WHEN 'APAC' THEN 'GST_STD'
                                                    ELSE 'ST_STD'
                                                END,
                                 p_tax_dt    => l_rows(i).invoice_dt);

                UPDATE WWI_FIN.AP_INVOICE_HDR
                   SET TAX_AMT = l_tax_amt,
                       NET_AMT = l_rows(i).gross_amt - l_tax_amt
                 WHERE INVOICE_ID = l_invoice_id;

                p_loaded_cnt := p_loaded_cnt + 1;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    p_rejected_cnt := p_rejected_cnt + 1;
                    WWI_AUDIT.PKG_DATA_QUALITY.log_reject('INVSCAN',
                        'invoice_staging', l_rows(i).invoice_num,
                        'UNKNOWN_SUPPLIER',
                        'supplier ' || l_rows(i).supp_num || ' not in the master',
                        'E');
                WHEN OTHERS THEN
                    p_rejected_cnt := p_rejected_cnt + 1;
                    WWI_AUDIT.PKG_DATA_QUALITY.log_reject('INVSCAN',
                        'invoice_staging', l_rows(i).invoice_num,
                        'LOAD_FAILED', SQLERRM, 'E');
            END;
        END LOOP;

        COMMIT;
    END LOOP;
    CLOSE l_cur;
EXCEPTION
    WHEN OTHERS THEN
        IF l_cur%ISOPEN THEN
            CLOSE l_cur;
        END IF;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_LOAD_INVOICE_INTERFACE',
                                             p_region_cd, SQLERRM);
        RAISE;
END PRC_LOAD_INVOICE_INTERFACE;
/
