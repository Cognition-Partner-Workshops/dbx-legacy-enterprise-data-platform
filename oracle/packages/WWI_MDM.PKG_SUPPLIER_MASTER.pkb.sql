/* ============================================================================
 * Object      : WWI_MDM.PKG_SUPPLIER_MASTER (package body)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_MDM.PKG_SUPPLIER_MASTER, WWI_MDM.SUPP_MASTER,
 *               WWI_MDM.SUPP_BANK_ACCOUNT, WWI_MDM.SUPP_CERTIFICATION,
 *               WWI_FIN.PKG_TAX, WWI_MDM.FN_NORMALIZE_NAME,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_MDM.PKG_SUPPLIER_MASTER AS

    FUNCTION certification_status
    (
        p_supp_id IN WWI_MDM.SUPP_MASTER.SUPP_ID%TYPE,
        p_cert_cd IN WWI_MDM.SUPP_CERTIFICATION.CERT_CD%TYPE
    ) RETURN VARCHAR2
    IS
        l_expiry_dt WWI_MDM.SUPP_CERTIFICATION.EXPIRY_DT%TYPE;
    BEGIN
        SELECT MAX(EXPIRY_DT)
          INTO l_expiry_dt
          FROM WWI_MDM.SUPP_CERTIFICATION
         WHERE SUPP_ID = p_supp_id
           AND CERT_CD = p_cert_cd
           AND NVL(REVOKED_FLAG, 'N') = 'N';

        IF l_expiry_dt IS NULL THEN
            RETURN 'MISSING';
        ELSIF l_expiry_dt < TRUNC(SYSDATE) THEN
            RETURN 'EXPIRED';
        ELSIF l_expiry_dt < TRUNC(SYSDATE) + 60 THEN
            RETURN 'EXPIRING';
        END IF;

        RETURN 'VALID';
    END certification_status;

    FUNCTION is_approved_for_po
    (
        p_supp_id   IN WWI_MDM.SUPP_MASTER.SUPP_ID%TYPE,
        p_region_cd IN VARCHAR2,
        p_amount    IN NUMBER DEFAULT 0
    ) RETURN VARCHAR2
    IS
        l_status    WWI_MDM.SUPP_MASTER.STATUS_CD%TYPE;
        l_blocked   WWI_MDM.SUPP_MASTER.BLOCK_REASON_CD%TYPE;
        l_tax_reg   WWI_MDM.SUPP_MASTER.TAX_REG_NUM%TYPE;
        l_country   WWI_MDM.SUPP_MASTER.COUNTRY_CD%TYPE;
    BEGIN
        SELECT STATUS_CD, BLOCK_REASON_CD, TAX_REG_NUM, COUNTRY_CD
          INTO l_status, l_blocked, l_tax_reg, l_country
          FROM WWI_MDM.SUPP_MASTER
         WHERE SUPP_ID = p_supp_id;

        IF l_blocked IS NOT NULL THEN
            RETURN 'BLOCKED';
        END IF;

        IF l_status <> 'A' THEN
            RETURN 'INACTIVE';
        END IF;

        /* each region gates PO creation differently */
        IF p_region_cd = 'EU' THEN
            IF WWI_FIN.PKG_TAX.validate_vat_id(l_tax_reg, l_country) = 'N' THEN
                RETURN 'VAT_INVALID';
            END IF;
            IF p_amount > 25000 AND certification_status(p_supp_id, 'ISO9001') <> 'VALID' THEN
                RETURN 'CERT_REQUIRED';
            END IF;
        ELSIF p_region_cd = 'APAC' THEN
            IF certification_status(p_supp_id, 'LOCALREG') IN ('MISSING', 'EXPIRED') THEN
                RETURN 'CERT_REQUIRED';
            END IF;
        ELSE
            /* NA only wants a tax id over the 1099 reporting threshold */
            IF p_amount > 600 AND l_tax_reg IS NULL THEN
                RETURN 'TAXID_REQUIRED';
            END IF;
        END IF;

        RETURN 'APPROVED';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20211,
                'PKG_SUPPLIER_MASTER.is_approved_for_po: supplier ' || p_supp_id
                || ' not found');
    END is_approved_for_po;

    PROCEDURE onboard_supplier
    (
        p_supp_num    IN  WWI_MDM.SUPP_MASTER.SUPP_NUM%TYPE,
        p_supp_name   IN  WWI_MDM.SUPP_MASTER.SUPP_NAME%TYPE,
        p_region_cd   IN  WWI_MDM.SUPP_MASTER.REGION_CD%TYPE,
        p_country_cd  IN  WWI_MDM.SUPP_MASTER.COUNTRY_CD%TYPE,
        p_tax_reg_num IN  WWI_MDM.SUPP_MASTER.TAX_REG_NUM%TYPE,
        p_supp_id     OUT WWI_MDM.SUPP_MASTER.SUPP_ID%TYPE
    )
    IS
        l_dup_cnt PLS_INTEGER;
    BEGIN
        SELECT COUNT(*)
          INTO l_dup_cnt
          FROM WWI_MDM.SUPP_MASTER
         WHERE SUPP_NAME_NORM = WWI_MDM.FN_NORMALIZE_NAME(p_supp_name, p_region_cd)
           AND COUNTRY_CD     = p_country_cd
           AND STATUS_CD     <> 'T';

        p_supp_id := WWI_MDM.SEQ_SUPP.NEXTVAL;

        INSERT INTO WWI_MDM.SUPP_MASTER
            (SUPP_ID, SUPP_NUM, SUPP_NAME, SUPP_NAME_NORM, REGION_CD, COUNTRY_CD,
             TAX_REG_NUM, STATUS_CD, WITHHOLDING_EXEMPT_FLAG, ONBOARDED_DT,
             CREATED_DT, CREATED_BY, LAST_UPD_DT, LAST_UPD_BY)
        VALUES
            (p_supp_id, p_supp_num, p_supp_name,
             WWI_MDM.FN_NORMALIZE_NAME(p_supp_name, p_region_cd),
             p_region_cd, p_country_cd, p_tax_reg_num,
             /* EU suppliers start pending until the VAT id is checked */
             CASE WHEN p_region_cd = 'EU' THEN 'P' ELSE 'A' END,
             'N', TRUNC(SYSDATE), SYSDATE, USER, SYSDATE, USER);

        IF l_dup_cnt > 0 THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_SUPPLIER_MASTER.onboard_supplier',
                                                 p_supp_num,
                                                 'possible duplicate supplier name in '
                                                 || p_country_cd);
        END IF;
    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
            RAISE_APPLICATION_ERROR(-20211,
                'PKG_SUPPLIER_MASTER.onboard_supplier: supplier number '
                || p_supp_num || ' already exists');
    END onboard_supplier;

    PROCEDURE register_bank_account
    (
        p_supp_id      IN  WWI_MDM.SUPP_BANK_ACCOUNT.SUPP_ID%TYPE,
        p_bank_name    IN  WWI_MDM.SUPP_BANK_ACCOUNT.BANK_NAME%TYPE,
        p_acct_num     IN  VARCHAR2,
        p_iban         IN  VARCHAR2 DEFAULT NULL,
        p_swift_cd     IN  WWI_MDM.SUPP_BANK_ACCOUNT.SWIFT_CD%TYPE DEFAULT NULL,
        p_currency_cd  IN  WWI_MDM.SUPP_BANK_ACCOUNT.CURRENCY_CD%TYPE,
        p_bank_acct_id OUT WWI_MDM.SUPP_BANK_ACCOUNT.BANK_ACCT_ID%TYPE
    )
    IS
        l_region_cd WWI_MDM.SUPP_MASTER.REGION_CD%TYPE;
        l_masked    VARCHAR2(60);
        l_iban_mask VARCHAR2(60);
    BEGIN
        SELECT REGION_CD
          INTO l_region_cd
          FROM WWI_MDM.SUPP_MASTER
         WHERE SUPP_ID = p_supp_id;

        IF l_region_cd = 'EU' AND p_iban IS NULL THEN
            RAISE_APPLICATION_ERROR(-20212,
                'PKG_SUPPLIER_MASTER.register_bank_account: IBAN is mandatory in EU');
        END IF;

        /* only the masked forms are stored in this table; the full details
           live in the treasury vault and are keyed by BANK_ACCT_ID          */
        l_masked    := RPAD('*', GREATEST(LENGTH(p_acct_num) - 4, 0), '*')
                       || SUBSTR(p_acct_num, -4);
        l_iban_mask := CASE WHEN p_iban IS NULL THEN NULL
                            ELSE SUBSTR(p_iban, 1, 4) || RPAD('*', 12, '*')
                                 || SUBSTR(p_iban, -4)
                       END;

        p_bank_acct_id := WWI_MDM.SEQ_SUPP_BANK.NEXTVAL;

        INSERT INTO WWI_MDM.SUPP_BANK_ACCOUNT
            (BANK_ACCT_ID, SUPP_ID, BANK_NAME, ACCT_NUM_MASKED, IBAN_MASKED,
             SWIFT_CD, CURRENCY_CD, ACTIVE_FLAG, VERIFIED_FLAG,
             CREATED_DT, LAST_UPD_DT, LAST_UPD_BY)
        VALUES
            (p_bank_acct_id, p_supp_id, p_bank_name, l_masked, l_iban_mask,
             p_swift_cd, p_currency_cd, 'Y', 'N', SYSDATE, SYSDATE, USER);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20211,
                'PKG_SUPPLIER_MASTER.register_bank_account: supplier not found');
    END register_bank_account;

    PROCEDURE block_supplier
    (
        p_supp_id    IN WWI_MDM.SUPP_MASTER.SUPP_ID%TYPE,
        p_reason_cd  IN WWI_MDM.SUPP_MASTER.BLOCK_REASON_CD%TYPE,
        p_blocked_by IN VARCHAR2
    )
    IS
        l_open_po PLS_INTEGER;
    BEGIN
        SELECT COUNT(*)
          INTO l_open_po
          FROM WWI_PROC.PURCHASE_ORDER_HDR
         WHERE SUPP_ID = p_supp_id
           AND STATUS_CD IN ('AP', 'OP');

        UPDATE WWI_MDM.SUPP_MASTER
           SET BLOCK_REASON_CD = p_reason_cd,
               BLOCKED_DT      = SYSDATE,
               STATUS_CD       = 'B',
               LAST_UPD_DT     = SYSDATE,
               LAST_UPD_BY     = p_blocked_by
         WHERE SUPP_ID = p_supp_id;

        IF SQL%ROWCOUNT = 0 THEN
            RAISE_APPLICATION_ERROR(-20211,
                'PKG_SUPPLIER_MASTER.block_supplier: supplier ' || p_supp_id
                || ' not found');
        END IF;

        IF l_open_po > 0 THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_SUPPLIER_MASTER.block_supplier',
                                                 TO_CHAR(p_supp_id),
                                                 l_open_po || ' open PO(s) left on a '
                                                 || 'blocked supplier');
        END IF;
    END block_supplier;

    PROCEDURE expire_certifications
    (
        p_expired_cnt OUT PLS_INTEGER
    )
    IS
    BEGIN
        UPDATE WWI_MDM.SUPP_CERTIFICATION
           SET STATUS_CD   = 'EXPIRED',
               LAST_UPD_DT = SYSDATE,
               LAST_UPD_BY = USER
         WHERE EXPIRY_DT < TRUNC(SYSDATE)
           AND NVL(STATUS_CD, 'ACTIVE') <> 'EXPIRED'
           AND NVL(REVOKED_FLAG, 'N') = 'N';

        p_expired_cnt := SQL%ROWCOUNT;

        /* APAC suppliers lose approval as soon as the local registration
           lapses; the other regions only get a warning on the scorecard   */
        UPDATE WWI_MDM.SUPP_MASTER s
           SET s.STATUS_CD   = 'P',
               s.LAST_UPD_DT = SYSDATE
         WHERE s.REGION_CD = 'APAC'
           AND s.STATUS_CD = 'A'
           AND EXISTS (SELECT 1
                         FROM WWI_MDM.SUPP_CERTIFICATION c
                        WHERE c.SUPP_ID = s.SUPP_ID
                          AND c.CERT_CD = 'LOCALREG'
                          AND c.EXPIRY_DT < TRUNC(SYSDATE));
    EXCEPTION
        WHEN OTHERS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_SUPPLIER_MASTER.expire_certifications',
                                                 NULL, SQLERRM);
            RAISE;
    END expire_certifications;

END PKG_SUPPLIER_MASTER;
/
