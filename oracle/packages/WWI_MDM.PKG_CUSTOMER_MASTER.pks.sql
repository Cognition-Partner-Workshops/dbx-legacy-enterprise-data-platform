/* ============================================================================
 * Object      : WWI_MDM.PKG_CUSTOMER_MASTER (package specification)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.CUST_MASTER, WWI_MDM.CUST_ADDRESS, WWI_MDM.CUST_CONTACT,
 *               WWI_MDM.CUST_CREDIT_PROFILE, WWI_MDM.MDM_MERGE_HISTORY
 * Called by   : the customer maintenance form, the CRM inbound interface
 *               (WWI_MDM.PRC_LOAD_CUSTOMER_INTERFACE) and the stewardship
 *               merge screen.
 * History     : 2002 created; 2007 hand-rolled SCD2 on the address table;
 *               2011 GDPR-driven consent columns added for EU only, then
 *               retro-fitted to APAC in 2018 with different semantics.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_MDM.PKG_CUSTOMER_MASTER AS

    e_customer_not_found EXCEPTION;
    e_merge_not_allowed  EXCEPTION;
    e_consent_required   EXCEPTION;
    e_duplicate_customer EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_customer_not_found, -20201);
    PRAGMA EXCEPTION_INIT(e_merge_not_allowed,  -20202);
    PRAGMA EXCEPTION_INIT(e_consent_required,   -20203);
    PRAGMA EXCEPTION_INIT(e_duplicate_customer, -20204);

    c_bulk_limit CONSTANT PLS_INTEGER := 500;

    FUNCTION match_score
    (
        p_cust_id_a IN WWI_MDM.CUST_MASTER.CUST_ID%TYPE,
        p_cust_id_b IN WWI_MDM.CUST_MASTER.CUST_ID%TYPE
    ) RETURN NUMBER;

    FUNCTION retention_expiry_dt
    (
        p_cust_id IN WWI_MDM.CUST_MASTER.CUST_ID%TYPE
    ) RETURN DATE;

    PROCEDURE upsert_customer
    (
        p_cust_num   IN  WWI_MDM.CUST_MASTER.CUST_NBR%TYPE,
        p_cust_name  IN  WWI_MDM.CUST_MASTER.CUST_NAME%TYPE,
        p_region_cd  IN  WWI_MDM.CUST_MASTER.REGION_CD%TYPE,
        p_country_cd IN  WWI_MDM.CUST_MASTER.COUNTRY_CD%TYPE,
        p_segment_cd IN  WWI_MDM.CUST_SEGMENT_ASSIGN.SEGMENT_CD%TYPE DEFAULT NULL,
        p_src_system IN  WWI_MDM.CUST_MASTER.SOURCE_SYS%TYPE DEFAULT 'CRM',
        p_cust_id    OUT WWI_MDM.CUST_MASTER.CUST_ID%TYPE,
        p_action_cd  OUT VARCHAR2
    );

    PROCEDURE apply_address_change
    (
        p_cust_id      IN WWI_MDM.CUST_ADDRESS.CUST_ID%TYPE,
        p_address_type IN WWI_MDM.CUST_ADDRESS.ADDR_TYPE_CD%TYPE,
        p_line1_txt    IN WWI_MDM.CUST_ADDRESS.ADDR_LINE_1%TYPE,
        p_line2_txt    IN WWI_MDM.CUST_ADDRESS.ADDR_LINE_2%TYPE,
        p_city_name    IN WWI_MDM.CUST_ADDRESS.CITY_TXT%TYPE,
        p_state_cd     IN WWI_MDM.CUST_ADDRESS.STATE_PROV_CD%TYPE,
        p_postal_cd    IN WWI_MDM.CUST_ADDRESS.POSTAL_CD%TYPE,
        p_country_cd   IN WWI_MDM.CUST_ADDRESS.COUNTRY_CD%TYPE,
        p_eff_dt       IN DATE DEFAULT TRUNC(SYSDATE)
    );

    PROCEDURE set_credit_limit
    (
        p_cust_id     IN WWI_MDM.CUST_CREDIT_PROFILE.CUST_ID%TYPE,
        p_limit_amt   IN WWI_MDM.CUST_CREDIT_PROFILE.CREDIT_LIMIT_AMT%TYPE,
        p_currency_cd IN WWI_MDM.CUST_CREDIT_PROFILE.CREDIT_LIMIT_CURR_CD%TYPE,
        p_approved_by IN VARCHAR2
    );

    PROCEDURE merge_customer
    (
        p_surviving_id IN WWI_MDM.CUST_MASTER.CUST_ID%TYPE,
        p_merged_id    IN WWI_MDM.CUST_MASTER.CUST_ID%TYPE,
        p_merged_by    IN VARCHAR2,
        p_reason_txt   IN VARCHAR2 DEFAULT NULL
    );

    PROCEDURE purge_expired_customers
    (
        p_region_cd  IN  VARCHAR2,
        p_dry_run    IN  VARCHAR2 DEFAULT 'Y',
        p_purged_cnt OUT PLS_INTEGER
    );

END PKG_CUSTOMER_MASTER;
/
