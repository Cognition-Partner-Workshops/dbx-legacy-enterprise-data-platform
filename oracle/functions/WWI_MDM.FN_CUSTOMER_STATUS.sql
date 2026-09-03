/* ============================================================================
 * Object      : WWI_MDM.FN_CUSTOMER_STATUS (function)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.CUST_MASTER, WWI_MDM.CUST_CREDIT_PROFILE,
 *               WWI_MDM.CUST_ADDRESS, WWI_MDM.CUST_CONTACT
 * Called by   : WWI_MDM.V_CUSTOMER_EXTRACT, WWI_MDM.PKG_CUSTOMER_MASTER,
 *               WWI_MDM.PRC_PURGE_CUSTOMER_CONSENT
 * History     : 2001 original; 2009 dormancy window made region specific;
 *               2018 consent withdrawal folded into the same return code set.
 * Notes       : Returns the *derived* status the warehouse consumes. The stored
 *               CUST_MASTER.STATUS_CD is only one of the inputs; downstream
 *               reporting has always trusted this function instead.
 * ========================================================================= */

CREATE OR REPLACE FUNCTION WWI_MDM.FN_CUSTOMER_STATUS
(
    p_cust_id   IN WWI_MDM.CUST_MASTER.CUST_ID%TYPE,
    p_as_of_dt  IN DATE DEFAULT SYSDATE
)
RETURN VARCHAR2
IS
    l_stored_status   WWI_MDM.CUST_MASTER.STATUS_CD%TYPE;
    l_region_cd       WWI_MDM.CUST_MASTER.REGION_CD%TYPE;
    l_merged_into     WWI_MDM.CUST_MASTER.MERGED_INTO_CUST_ID%TYPE;
    l_consent_flag    WWI_MDM.CUST_MASTER.CONSENT_FLAG%TYPE;
    l_last_upd_dt     WWI_MDM.CUST_MASTER.LAST_UPD_DT%TYPE;
    l_hold_flag       WWI_MDM.CUST_CREDIT_PROFILE.HOLD_FLAG%TYPE;
    l_dbt_num         WWI_MDM.CUST_CREDIT_PROFILE.DAYS_BEYOND_TERMS_NUM%TYPE;
    l_last_activity   DATE;
    l_dormant_days    PLS_INTEGER;
BEGIN
    SELECT c.STATUS_CD, c.REGION_CD, c.MERGED_INTO_CUST_ID, c.CONSENT_FLAG, c.LAST_UPD_DT
      INTO l_stored_status, l_region_cd, l_merged_into, l_consent_flag, l_last_upd_dt
      FROM WWI_MDM.CUST_MASTER c
     WHERE c.CUST_ID = p_cust_id;

    IF l_merged_into IS NOT NULL THEN
        RETURN 'MERGED';
    END IF;

    IF l_stored_status = 'X' THEN
        RETURN 'CLOSED';
    END IF;

    BEGIN
        SELECT cp.HOLD_FLAG, cp.DAYS_BEYOND_TERMS_NUM
          INTO l_hold_flag, l_dbt_num
          FROM WWI_MDM.CUST_CREDIT_PROFILE cp
         WHERE cp.CUST_ID = p_cust_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_hold_flag := 'N';
            l_dbt_num   := 0;
    END;

    /* EU withdrew-consent customers are reported as suppressed even while the
       commercial relationship is open; NA and APAC are not suppressed. */
    IF UPPER(l_region_cd) = 'EU' AND NVL(l_consent_flag, 'N') = 'N' THEN
        RETURN 'SUPPRESSED';
    END IF;

    IF NVL(l_hold_flag, 'N') = 'Y' OR NVL(l_dbt_num, 0) > 45 THEN
        RETURN 'HOLD';
    END IF;

    /* dormancy window: NA 18 months, EU 24 months (retention rules), APAC 12 */
    l_dormant_days :=
        CASE UPPER(l_region_cd)
            WHEN 'EU'   THEN 730
            WHEN 'APAC' THEN 365
            ELSE 545
        END;

    /* "Activity" has meant touch-date on any owned child record since the 2009
       rewrite; the ERP holds no AR ledger, so there is nothing better. */
    SELECT GREATEST(NVL(MAX(a.LAST_UPD_DT), l_last_upd_dt),
                    NVL(MAX(k.LAST_UPD_DT), l_last_upd_dt),
                    l_last_upd_dt)
      INTO l_last_activity
      FROM WWI_MDM.CUST_ADDRESS a
      FULL OUTER JOIN WWI_MDM.CUST_CONTACT k
        ON k.CUST_ID = a.CUST_ID
     WHERE NVL(a.CUST_ID, k.CUST_ID) = p_cust_id;

    IF l_last_activity IS NULL OR l_last_activity < p_as_of_dt - l_dormant_days THEN
        RETURN 'DORMANT';
    END IF;

    RETURN 'ACTIVE';
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 'UNKNOWN';
    WHEN OTHERS THEN
        RETURN 'UNKNOWN';
END FN_CUSTOMER_STATUS;
/
