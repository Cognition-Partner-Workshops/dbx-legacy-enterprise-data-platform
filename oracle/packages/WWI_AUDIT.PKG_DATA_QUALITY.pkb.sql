/* ============================================================================
 * Object      : WWI_AUDIT.PKG_DATA_QUALITY (package body)
 * Schema      : WWI_AUDIT
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_AUDIT.PKG_DATA_QUALITY, WWI_AUDIT.INTERFACE_ERROR,
 *               WWI_MDM.CUST_MASTER, WWI_MDM.SUPP_MASTER,
 *               WWI_FIN.AP_INVOICE_HDR, WWI_PROC.PURCHASE_ORDER_LINE
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_AUDIT.PKG_DATA_QUALITY AS

    PROCEDURE log_error
    (
        p_source_txt IN WWI_AUDIT.INTERFACE_ERROR.SRC_OBJECT_NAME%TYPE,
        p_key_txt    IN WWI_AUDIT.INTERFACE_ERROR.SRC_KEY_TXT%TYPE,
        p_message    IN WWI_AUDIT.INTERFACE_ERROR.ERROR_MSG_TXT%TYPE
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO WWI_AUDIT.INTERFACE_ERROR
            (INTERFACE_ERROR_ID, EXTRACT_NAME, SRC_OBJECT_NAME, SRC_KEY_TXT,
             RULE_CD, SEVERITY_CD, ERROR_MSG_TXT, ERROR_DT, RESOLVED_DT, CREATED_BY)
        VALUES
            (WWI_AUDIT.SEQ_INTERFACE_ERROR.NEXTVAL, NULL, p_source_txt, p_key_txt,
             'RUNTIME', 'E', SUBSTR(p_message, 1, 2000), SYSDATE, NULL, USER);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            /* the error sink must never take the caller down with it */
            ROLLBACK;
    END log_error;

    PROCEDURE log_reject
    (
        p_extract_name IN WWI_AUDIT.INTERFACE_ERROR.EXTRACT_NAME%TYPE,
        p_source_txt   IN WWI_AUDIT.INTERFACE_ERROR.SRC_OBJECT_NAME%TYPE,
        p_key_txt      IN WWI_AUDIT.INTERFACE_ERROR.SRC_KEY_TXT%TYPE,
        p_rule_cd      IN WWI_AUDIT.INTERFACE_ERROR.RULE_CD%TYPE,
        p_message      IN WWI_AUDIT.INTERFACE_ERROR.ERROR_MSG_TXT%TYPE,
        p_severity_cd  IN WWI_AUDIT.INTERFACE_ERROR.SEVERITY_CD%TYPE DEFAULT 'W'
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO WWI_AUDIT.INTERFACE_ERROR
            (INTERFACE_ERROR_ID, EXTRACT_NAME, SRC_OBJECT_NAME, SRC_KEY_TXT,
             RULE_CD, SEVERITY_CD, ERROR_MSG_TXT, ERROR_DT, RESOLVED_DT, CREATED_BY)
        VALUES
            (WWI_AUDIT.SEQ_INTERFACE_ERROR.NEXTVAL, p_extract_name, p_source_txt,
             p_key_txt, p_rule_cd, p_severity_cd, SUBSTR(p_message, 1, 2000),
             SYSDATE, NULL, USER);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
    END log_reject;

    PROCEDURE resolve_errors
    (
        p_extract_name IN  WWI_AUDIT.INTERFACE_ERROR.EXTRACT_NAME%TYPE,
        p_resolved_by  IN  VARCHAR2,
        p_resolved_cnt OUT PLS_INTEGER
    )
    IS
    BEGIN
        UPDATE WWI_AUDIT.INTERFACE_ERROR
           SET RESOLVED_DT = SYSDATE,
               RESOLVED_BY = p_resolved_by
         WHERE EXTRACT_NAME = p_extract_name
           AND RESOLVED_DT IS NULL
           AND SEVERITY_CD <> 'F';

        p_resolved_cnt := SQL%ROWCOUNT;
    END resolve_errors;

    PROCEDURE run_orphan_checks
    (
        p_region_cd IN  VARCHAR2 DEFAULT NULL,
        p_issue_cnt OUT PLS_INTEGER
    )
    IS
        l_cnt PLS_INTEGER;
    BEGIN
        p_issue_cnt := 0;

        /* invoices whose supplier no longer exists - the supplier purge in
           2013 did not cascade                                            */
        FOR rec IN (SELECT i.INVOICE_ID, i.SUPP_ID
                      FROM WWI_FIN.AP_INVOICE_HDR i
                     WHERE (p_region_cd IS NULL OR i.REGION_CD = p_region_cd)
                       AND NOT EXISTS (SELECT 1
                                         FROM WWI_MDM.SUPP_MASTER s
                                        WHERE s.SUPP_ID = i.SUPP_ID)) LOOP
            log_reject(NULL, 'WWI_FIN.AP_INVOICE_HDR', TO_CHAR(rec.INVOICE_ID),
                       'ORPHAN_SUPPLIER',
                       'supplier ' || rec.SUPP_ID || ' does not exist', 'E');
            p_issue_cnt := p_issue_cnt + 1;
        END LOOP;

        /* PO lines pointing at products that were physically deleted */
        SELECT COUNT(*)
          INTO l_cnt
          FROM WWI_PROC.PURCHASE_ORDER_LINE l
         WHERE NOT EXISTS (SELECT 1
                             FROM WWI_MDM.PRODUCT_MASTER p
                            WHERE p.PRODUCT_ID = l.PRODUCT_ID);

        IF l_cnt > 0 THEN
            log_reject(NULL, 'WWI_PROC.PURCHASE_ORDER_LINE', NULL,
                       'ORPHAN_PRODUCT',
                       l_cnt || ' PO line(s) reference a missing product', 'E');
            p_issue_cnt := p_issue_cnt + l_cnt;
        END IF;

        /* customers with no current billing address; EU treats this as a
           blocking issue because the invoice cannot be issued            */
        FOR rec IN (SELECT c.CUST_ID, c.REGION_CD
                      FROM WWI_MDM.CUST_MASTER c
                     WHERE c.STATUS_CD = 'A'
                       AND (p_region_cd IS NULL OR c.REGION_CD = p_region_cd)
                       AND NOT EXISTS (SELECT 1
                                         FROM WWI_MDM.CUST_ADDRESS a
                                        WHERE a.CUST_ID = c.CUST_ID
                                          AND a.ADDRESS_TYPE_CD = 'BILL'
                                          AND NVL(a.CURRENT_FLAG, 'Y') = 'Y')) LOOP
            log_reject(NULL, 'WWI_MDM.CUST_MASTER', TO_CHAR(rec.CUST_ID),
                       'NO_BILL_ADDRESS', 'active customer without a billing address',
                       CASE WHEN rec.REGION_CD = 'EU' THEN 'F' ELSE 'W' END);
            p_issue_cnt := p_issue_cnt + 1;
        END LOOP;
    END run_orphan_checks;

    PROCEDURE assert_reject_rate
    (
        p_extract_name IN WWI_AUDIT.INTERFACE_ERROR.EXTRACT_NAME%TYPE,
        p_row_count    IN NUMBER,
        p_max_pct      IN NUMBER DEFAULT 2
    )
    IS
        l_rejects PLS_INTEGER;
        l_pct     NUMBER;
    BEGIN
        IF NVL(p_row_count, 0) = 0 THEN
            RETURN;
        END IF;

        SELECT COUNT(*)
          INTO l_rejects
          FROM WWI_AUDIT.INTERFACE_ERROR
         WHERE EXTRACT_NAME = p_extract_name
           AND RESOLVED_DT IS NULL
           AND ERROR_DT >= TRUNC(SYSDATE);

        l_pct := ROUND(l_rejects * 100 / p_row_count, 2);

        IF l_pct > p_max_pct THEN
            RAISE_APPLICATION_ERROR(-20501,
                'PKG_DATA_QUALITY.assert_reject_rate: ' || p_extract_name
                || ' rejected ' || l_pct || '% of ' || p_row_count
                || ' rows, threshold is ' || p_max_pct || '%');
        END IF;
    END assert_reject_rate;

END PKG_DATA_QUALITY;
/
