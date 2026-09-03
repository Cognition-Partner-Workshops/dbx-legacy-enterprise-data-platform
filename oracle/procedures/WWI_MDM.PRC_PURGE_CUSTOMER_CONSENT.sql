/* ============================================================================
 * Object      : WWI_MDM.PRC_PURGE_CUSTOMER_CONSENT (procedure)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.PKG_CUSTOMER_MASTER, WWI_MDM.CUST_MASTER,
 *               WWI_AUDIT.PURGE_LOG, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : DBMS_JOB 'MDM_CONSENT_PURGE' (EU nightly, NA and APAC monthly)
 * Notes       : Retention differs by region: EU 24 months after the last
 *               transaction, APAC 60, NA 84. EU also withdraws consent when
 *               it has not been re-confirmed in 24 months, which the other
 *               regions do not do at all.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_MDM.PRC_PURGE_CUSTOMER_CONSENT
(
    p_region_cd     IN  VARCHAR2,
    p_dry_run       IN  VARCHAR2 DEFAULT 'Y',
    p_withdrawn_cnt OUT PLS_INTEGER,
    p_purged_cnt    OUT PLS_INTEGER
)
IS
    l_consent_months PLS_INTEGER := 24;
BEGIN
    p_withdrawn_cnt := 0;
    p_purged_cnt    := 0;

    IF p_region_cd = 'EU' THEN
        UPDATE WWI_MDM.CUST_MASTER
           SET CONSENT_MARKETING_FLG = 'N',
               CONSENT_SOURCE_CD    = 'WITHDRAWN',
               CONSENT_CAPTURED_DT  = TRUNC(SYSDATE),
               UPDATED_DT       = SYSDATE,
               UPDATED_BY       = USER
         WHERE REGION_CD = 'EU'
           AND NVL(CONSENT_MARKETING_FLG, 'N') = 'Y'
           AND NVL(CONSENT_CAPTURED_DT, CREATED_DT)
               < ADD_MONTHS(TRUNC(SYSDATE), -l_consent_months);

        p_withdrawn_cnt := SQL%ROWCOUNT;

        IF NVL(p_dry_run, 'Y') = 'Y' THEN
            ROLLBACK;
        ELSE
            COMMIT;
        END IF;
    END IF;

    WWI_MDM.PKG_CUSTOMER_MASTER.purge_expired_customers(p_region_cd, p_dry_run,
                                                        p_purged_cnt);

    IF NVL(p_dry_run, 'Y') <> 'Y' THEN
        INSERT INTO WWI_AUDIT.PURGE_LOG
            (PURGE_LOG_ID, SCHEMA_NAME, TABLE_NAME, PURGE_RUN_TS, CUTOFF_DT,
             ROWS_PURGED_CNT, RUN_BY)
        VALUES
            (WWI_AUDIT.SEQ_PURGE_LOG.NEXTVAL, 'WWI_MDM', 'CUST_MASTER', SYSDATE,
             ADD_MONTHS(TRUNC(SYSDATE),
                        -CASE p_region_cd
                             WHEN 'EU'   THEN 24
                             WHEN 'APAC' THEN 60
                             ELSE 84
                         END),
             p_purged_cnt, USER);
        COMMIT;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_PURGE_CUSTOMER_CONSENT',
                                             p_region_cd, SQLERRM);
        RAISE;
END PRC_PURGE_CUSTOMER_CONSENT;
/
