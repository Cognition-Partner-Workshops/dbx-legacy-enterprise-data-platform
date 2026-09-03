/* ============================================================================
 * Object      : WWI_MDM.PKG_CUSTOMER_MASTER (package body)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_MDM.PKG_CUSTOMER_MASTER, WWI_MDM.CUST_MASTER,
 *               WWI_MDM.CUST_ADDRESS, WWI_MDM.CUST_CONTACT,
 *               WWI_MDM.CUST_CREDIT_PROFILE, WWI_MDM.MDM_MERGE_HISTORY,
 *               WWI_MDM.FN_NORMALIZE_NAME, WWI_MDM.FN_CUSTOMER_STATUS,
 *               WWI_REF.COUNTRY_REF, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_MDM.PKG_CUSTOMER_MASTER AS

    FUNCTION match_score
    (
        p_cust_id_a IN WWI_MDM.CUST_MASTER.CUST_ID%TYPE,
        p_cust_id_b IN WWI_MDM.CUST_MASTER.CUST_ID%TYPE
    ) RETURN NUMBER
    IS
        l_a      WWI_MDM.CUST_MASTER%ROWTYPE;
        l_b      WWI_MDM.CUST_MASTER%ROWTYPE;
        l_score  NUMBER := 0;
        l_pa     WWI_MDM.CUST_ADDRESS.POSTAL_CD%TYPE;
        l_pb     WWI_MDM.CUST_ADDRESS.POSTAL_CD%TYPE;
    BEGIN
        SELECT * INTO l_a FROM WWI_MDM.CUST_MASTER WHERE CUST_ID = p_cust_id_a;
        SELECT * INTO l_b FROM WWI_MDM.CUST_MASTER WHERE CUST_ID = p_cust_id_b;

        IF WWI_MDM.FN_NORMALIZE_NAME(l_a.CUST_NAME, l_a.REGION_CD)
           = WWI_MDM.FN_NORMALIZE_NAME(l_b.CUST_NAME, l_b.REGION_CD) THEN
            l_score := l_score + 50;
        ELSIF UTL_MATCH.JARO_WINKLER_SIMILARITY(UPPER(l_a.CUST_NAME),
                                                UPPER(l_b.CUST_NAME)) > 90 THEN
            l_score := l_score + 30;
        END IF;

        IF l_a.COUNTRY_CD = l_b.COUNTRY_CD THEN
            l_score := l_score + 10;
        END IF;

        IF l_a.TAX_REG_NUM IS NOT NULL AND l_a.TAX_REG_NUM = l_b.TAX_REG_NUM THEN
            l_score := l_score + 30;
        END IF;

        BEGIN
            SELECT MAX(POSTAL_CD) INTO l_pa
              FROM WWI_MDM.CUST_ADDRESS
             WHERE CUST_ID = p_cust_id_a AND ADDRESS_TYPE_CD = 'BILL'
               AND NVL(CURRENT_FLAG, 'Y') = 'Y';

            SELECT MAX(POSTAL_CD) INTO l_pb
              FROM WWI_MDM.CUST_ADDRESS
             WHERE CUST_ID = p_cust_id_b AND ADDRESS_TYPE_CD = 'BILL'
               AND NVL(CURRENT_FLAG, 'Y') = 'Y';

            IF l_pa IS NOT NULL AND l_pa = l_pb THEN
                l_score := l_score + 10;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                NULL;
        END;

        RETURN LEAST(l_score, 100);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20201,
                'PKG_CUSTOMER_MASTER.match_score: customer not found');
    END match_score;

    FUNCTION retention_expiry_dt
    (
        p_cust_id IN WWI_MDM.CUST_MASTER.CUST_ID%TYPE
    ) RETURN DATE
    IS
        l_region_cd  WWI_MDM.CUST_MASTER.REGION_CD%TYPE;
        l_closed_dt  WWI_MDM.CUST_MASTER.CLOSED_DT%TYPE;
        l_months     PLS_INTEGER;
    BEGIN
        SELECT REGION_CD, CLOSED_DT
          INTO l_region_cd, l_closed_dt
          FROM WWI_MDM.CUST_MASTER
         WHERE CUST_ID = p_cust_id;

        IF l_closed_dt IS NULL THEN
            RETURN NULL;
        END IF;

        /* retention is regional policy, not one global rule */
        l_months := CASE l_region_cd
                        WHEN 'EU'   THEN 24     /* GDPR minimisation */
                        WHEN 'APAC' THEN 60     /* local tax record keeping */
                        ELSE 84                 /* NA: 7 years, IRS default */
                    END;

        RETURN ADD_MONTHS(TRUNC(l_closed_dt), l_months);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
    END retention_expiry_dt;

    PROCEDURE upsert_customer
    (
        p_cust_num   IN  WWI_MDM.CUST_MASTER.CUST_NUM%TYPE,
        p_cust_name  IN  WWI_MDM.CUST_MASTER.CUST_NAME%TYPE,
        p_region_cd  IN  WWI_MDM.CUST_MASTER.REGION_CD%TYPE,
        p_country_cd IN  WWI_MDM.CUST_MASTER.COUNTRY_CD%TYPE,
        p_segment_cd IN  WWI_MDM.CUST_MASTER.SEGMENT_CD%TYPE DEFAULT NULL,
        p_src_system IN  WWI_MDM.CUST_MASTER.SRC_SYSTEM_CD%TYPE DEFAULT 'CRM',
        p_cust_id    OUT WWI_MDM.CUST_MASTER.CUST_ID%TYPE,
        p_action_cd  OUT VARCHAR2
    )
    IS
        l_existing_id WWI_MDM.CUST_MASTER.CUST_ID%TYPE;
    BEGIN
        BEGIN
            SELECT CUST_ID
              INTO l_existing_id
              FROM WWI_MDM.CUST_MASTER
             WHERE CUST_NUM = p_cust_num;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                l_existing_id := NULL;
        END;

        IF l_existing_id IS NULL THEN
            p_cust_id   := WWI_MDM.SEQ_CUST.NEXTVAL;
            p_action_cd := 'INSERT';

            INSERT INTO WWI_MDM.CUST_MASTER
                (CUST_ID, CUST_NUM, CUST_NAME, CUST_NAME_NORM, REGION_CD, COUNTRY_CD,
                 SEGMENT_CD, STATUS_CD, CONSENT_FLAG, SRC_SYSTEM_CD,
                 CREATED_DT, CREATED_BY, LAST_UPD_DT, LAST_UPD_BY)
            VALUES
                (p_cust_id, p_cust_num, p_cust_name,
                 WWI_MDM.FN_NORMALIZE_NAME(p_cust_name, p_region_cd),
                 p_region_cd, p_country_cd, p_segment_cd, 'A',
                 CASE WHEN p_region_cd = 'EU' THEN 'N' ELSE 'Y' END,
                 p_src_system, SYSDATE, USER, SYSDATE, USER);
        ELSE
            p_cust_id   := l_existing_id;
            p_action_cd := 'UPDATE';

            UPDATE WWI_MDM.CUST_MASTER
               SET CUST_NAME      = p_cust_name,
                   CUST_NAME_NORM = WWI_MDM.FN_NORMALIZE_NAME(p_cust_name, p_region_cd),
                   REGION_CD      = p_region_cd,
                   COUNTRY_CD     = p_country_cd,
                   SEGMENT_CD     = NVL(p_segment_cd, SEGMENT_CD),
                   LAST_UPD_DT    = SYSDATE,
                   LAST_UPD_BY    = USER
             WHERE CUST_ID = l_existing_id;
        END IF;
    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
            RAISE_APPLICATION_ERROR(-20204,
                'PKG_CUSTOMER_MASTER.upsert_customer: duplicate customer number '
                || p_cust_num);
    END upsert_customer;

    PROCEDURE apply_address_change
    (
        p_cust_id      IN WWI_MDM.CUST_ADDRESS.CUST_ID%TYPE,
        p_address_type IN WWI_MDM.CUST_ADDRESS.ADDRESS_TYPE_CD%TYPE,
        p_line1_txt    IN WWI_MDM.CUST_ADDRESS.ADDRESS_LINE1_TXT%TYPE,
        p_line2_txt    IN WWI_MDM.CUST_ADDRESS.ADDRESS_LINE2_TXT%TYPE,
        p_city_name    IN WWI_MDM.CUST_ADDRESS.CITY_NAME%TYPE,
        p_state_cd     IN WWI_MDM.CUST_ADDRESS.STATE_PROVINCE_CD%TYPE,
        p_postal_cd    IN WWI_MDM.CUST_ADDRESS.POSTAL_CD%TYPE,
        p_country_cd   IN WWI_MDM.CUST_ADDRESS.COUNTRY_CD%TYPE,
        p_eff_dt       IN DATE DEFAULT TRUNC(SYSDATE)
    )
    IS
        l_current   WWI_MDM.CUST_ADDRESS%ROWTYPE;
        l_changed   BOOLEAN := FALSE;
        l_postal_cd WWI_MDM.CUST_ADDRESS.POSTAL_CD%TYPE;
        l_region_cd WWI_MDM.CUST_MASTER.REGION_CD%TYPE;
    BEGIN
        SELECT REGION_CD INTO l_region_cd
          FROM WWI_MDM.CUST_MASTER WHERE CUST_ID = p_cust_id;

        /* postal standardisation is done on write for EU and APAC but only on
           read for NA, which is why the NA rows still hold whatever the sales
           rep typed in                                                      */
        l_postal_cd := CASE l_region_cd
                           WHEN 'EU'   THEN UPPER(REGEXP_REPLACE(p_postal_cd,
                                                                 '[[:space:]]+', ' '))
                           WHEN 'APAC' THEN UPPER(REPLACE(p_postal_cd, ' ', ''))
                           ELSE p_postal_cd
                       END;

        BEGIN
            SELECT *
              INTO l_current
              FROM WWI_MDM.CUST_ADDRESS
             WHERE CUST_ID         = p_cust_id
               AND ADDRESS_TYPE_CD = p_address_type
               AND NVL(CURRENT_FLAG, 'Y') = 'Y'
               AND ROWNUM = 1;

            l_changed := NVL(l_current.ADDRESS_LINE1_TXT, '~') <> NVL(p_line1_txt, '~')
                      OR NVL(l_current.CITY_NAME, '~')         <> NVL(p_city_name, '~')
                      OR NVL(l_current.POSTAL_CD, '~')         <> NVL(l_postal_cd, '~')
                      OR NVL(l_current.COUNTRY_CD, '~')        <> NVL(p_country_cd, '~');
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                l_current.ADDRESS_ID := NULL;
                l_changed            := TRUE;
        END;

        IF NOT l_changed THEN
            RETURN;
        END IF;

        /* hand-rolled type 2: close the old row, open a new one */
        IF l_current.ADDRESS_ID IS NOT NULL THEN
            UPDATE WWI_MDM.CUST_ADDRESS
               SET CURRENT_FLAG = 'N',
                   VALID_TO_DT  = p_eff_dt - 1,
                   LAST_UPD_DT  = SYSDATE,
                   LAST_UPD_BY  = USER
             WHERE ADDRESS_ID = l_current.ADDRESS_ID;
        END IF;

        INSERT INTO WWI_MDM.CUST_ADDRESS
            (ADDRESS_ID, CUST_ID, ADDRESS_TYPE_CD, ADDRESS_LINE1_TXT, ADDRESS_LINE2_TXT,
             CITY_NAME, STATE_PROVINCE_CD, POSTAL_CD, COUNTRY_CD,
             CURRENT_FLAG, VALID_FROM_DT, VALID_TO_DT, CREATED_DT, LAST_UPD_DT, LAST_UPD_BY)
        VALUES
            (WWI_MDM.SEQ_CUST_ADDRESS.NEXTVAL, p_cust_id, p_address_type,
             p_line1_txt, p_line2_txt, p_city_name, p_state_cd, l_postal_cd, p_country_cd,
             'Y', p_eff_dt, DATE '4712-12-31', SYSDATE, SYSDATE, USER);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20201,
                'PKG_CUSTOMER_MASTER.apply_address_change: customer '
                || p_cust_id || ' not found');
    END apply_address_change;

    PROCEDURE set_credit_limit
    (
        p_cust_id     IN WWI_MDM.CUST_CREDIT_PROFILE.CUST_ID%TYPE,
        p_limit_amt   IN WWI_MDM.CUST_CREDIT_PROFILE.CREDIT_LIMIT_AMT%TYPE,
        p_currency_cd IN WWI_MDM.CUST_CREDIT_PROFILE.CURRENCY_CD%TYPE,
        p_approved_by IN VARCHAR2
    )
    IS
        l_rows PLS_INTEGER;
    BEGIN
        UPDATE WWI_MDM.CUST_CREDIT_PROFILE
           SET CREDIT_LIMIT_AMT = p_limit_amt,
               CURRENCY_CD      = p_currency_cd,
               LIMIT_APPROVED_BY = p_approved_by,
               LIMIT_APPROVED_DT = SYSDATE,
               LAST_UPD_DT      = SYSDATE,
               LAST_UPD_BY      = p_approved_by
         WHERE CUST_ID = p_cust_id;

        l_rows := SQL%ROWCOUNT;

        IF l_rows = 0 THEN
            INSERT INTO WWI_MDM.CUST_CREDIT_PROFILE
                (CUST_ID, CREDIT_LIMIT_AMT, CURRENCY_CD, CREDIT_HOLD_FLAG,
                 LIMIT_APPROVED_BY, LIMIT_APPROVED_DT, CREATED_DT, LAST_UPD_DT, LAST_UPD_BY)
            VALUES
                (p_cust_id, p_limit_amt, p_currency_cd, 'N',
                 p_approved_by, SYSDATE, SYSDATE, SYSDATE, p_approved_by);
        END IF;
    END set_credit_limit;

    PROCEDURE merge_customer
    (
        p_surviving_id IN WWI_MDM.CUST_MASTER.CUST_ID%TYPE,
        p_merged_id    IN WWI_MDM.CUST_MASTER.CUST_ID%TYPE,
        p_merged_by    IN VARCHAR2,
        p_reason_txt   IN VARCHAR2 DEFAULT NULL
    )
    IS
        l_score       NUMBER;
        l_surv_status WWI_MDM.CUST_MASTER.STATUS_CD%TYPE;
        l_merge_status WWI_MDM.CUST_MASTER.STATUS_CD%TYPE;
        l_moved_addr  PLS_INTEGER := 0;
        l_moved_cont  PLS_INTEGER := 0;
    BEGIN
        IF p_surviving_id = p_merged_id THEN
            RAISE_APPLICATION_ERROR(-20202,
                'PKG_CUSTOMER_MASTER.merge_customer: cannot merge a customer into itself');
        END IF;

        SELECT STATUS_CD INTO l_surv_status
          FROM WWI_MDM.CUST_MASTER WHERE CUST_ID = p_surviving_id FOR UPDATE;
        SELECT STATUS_CD INTO l_merge_status
          FROM WWI_MDM.CUST_MASTER WHERE CUST_ID = p_merged_id FOR UPDATE;

        IF l_surv_status = 'M' THEN
            RAISE_APPLICATION_ERROR(-20202,
                'PKG_CUSTOMER_MASTER.merge_customer: survivor is itself a merged record');
        END IF;

        l_score := match_score(p_surviving_id, p_merged_id);

        IF l_score < 60 THEN
            RAISE_APPLICATION_ERROR(-20202,
                'PKG_CUSTOMER_MASTER.merge_customer: match score ' || l_score
                || ' below the stewardship threshold of 60');
        END IF;

        UPDATE WWI_MDM.CUST_ADDRESS
           SET CUST_ID      = p_surviving_id,
               CURRENT_FLAG = 'N',
               VALID_TO_DT  = TRUNC(SYSDATE),
               LAST_UPD_DT  = SYSDATE,
               LAST_UPD_BY  = p_merged_by
         WHERE CUST_ID = p_merged_id;
        l_moved_addr := SQL%ROWCOUNT;

        UPDATE WWI_MDM.CUST_CONTACT
           SET CUST_ID     = p_surviving_id,
               LAST_UPD_DT = SYSDATE,
               LAST_UPD_BY = p_merged_by
         WHERE CUST_ID = p_merged_id;
        l_moved_cont := SQL%ROWCOUNT;

        UPDATE WWI_MDM.CUST_MASTER
           SET STATUS_CD         = 'M',
               MERGED_INTO_ID    = p_surviving_id,
               MERGED_DT         = SYSDATE,
               LAST_UPD_DT       = SYSDATE,
               LAST_UPD_BY       = p_merged_by
         WHERE CUST_ID = p_merged_id;

        INSERT INTO WWI_MDM.MDM_MERGE_HISTORY
            (MERGE_LOG_ID, SURVIVING_CUST_ID, MERGED_CUST_ID, MATCH_SCORE,
             ADDRESS_MOVED_CNT, CONTACT_MOVED_CNT, REASON_TXT, MERGED_BY, MERGED_DT)
        VALUES
            (WWI_MDM.SEQ_MDM_MERGE_HISTORY.NEXTVAL, p_surviving_id, p_merged_id, l_score,
             l_moved_addr, l_moved_cont, p_reason_txt, p_merged_by, SYSDATE);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20201,
                'PKG_CUSTOMER_MASTER.merge_customer: customer not found');
        WHEN OTHERS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_CUSTOMER_MASTER.merge_customer',
                                                 p_surviving_id || '<-' || p_merged_id,
                                                 SQLERRM);
            RAISE;
    END merge_customer;

    PROCEDURE purge_expired_customers
    (
        p_region_cd  IN  VARCHAR2,
        p_dry_run    IN  VARCHAR2 DEFAULT 'Y',
        p_purged_cnt OUT PLS_INTEGER
    )
    IS
        CURSOR c_expired IS
            SELECT c.CUST_ID
              FROM WWI_MDM.CUST_MASTER c
             WHERE c.REGION_CD = p_region_cd
               AND c.STATUS_CD IN ('C', 'M')
               AND retention_expiry_dt(c.CUST_ID) < TRUNC(SYSDATE)
             ORDER BY c.CUST_ID;

        TYPE t_id_tab IS TABLE OF WWI_MDM.CUST_MASTER.CUST_ID%TYPE;
        l_ids t_id_tab;
    BEGIN
        p_purged_cnt := 0;

        OPEN c_expired;
        LOOP
            FETCH c_expired BULK COLLECT INTO l_ids LIMIT c_bulk_limit;
            EXIT WHEN l_ids.COUNT = 0;

            FOR i IN 1 .. l_ids.COUNT LOOP
                IF NVL(p_dry_run, 'Y') = 'N' THEN
                    /* EU deletes the personal attributes outright; the other
                       regions only anonymise the contact rows                */
                    IF p_region_cd = 'EU' THEN
                        DELETE FROM WWI_MDM.CUST_CONTACT WHERE CUST_ID = l_ids(i);

                        UPDATE WWI_MDM.CUST_MASTER
                           SET CUST_NAME      = 'PURGED-' || l_ids(i),
                               CUST_NAME_NORM = NULL,
                               TAX_REG_NUM    = NULL,
                               PURGED_FLAG    = 'Y',
                               PURGED_DT      = SYSDATE,
                               LAST_UPD_DT    = SYSDATE
                         WHERE CUST_ID = l_ids(i);
                    ELSE
                        UPDATE WWI_MDM.CUST_CONTACT
                           SET EMAIL_TXT  = NULL,
                               PHONE_TXT  = NULL,
                               LAST_UPD_DT = SYSDATE
                         WHERE CUST_ID = l_ids(i);

                        UPDATE WWI_MDM.CUST_MASTER
                           SET PURGED_FLAG = 'Y',
                               PURGED_DT   = SYSDATE,
                               LAST_UPD_DT = SYSDATE
                         WHERE CUST_ID = l_ids(i);
                    END IF;
                END IF;

                p_purged_cnt := p_purged_cnt + 1;
            END LOOP;

            IF NVL(p_dry_run, 'Y') = 'N' THEN
                COMMIT;
            END IF;

            EXIT WHEN c_expired%NOTFOUND;
        END LOOP;
        CLOSE c_expired;
    EXCEPTION
        WHEN OTHERS THEN
            IF c_expired%ISOPEN THEN
                CLOSE c_expired;
            END IF;
            RAISE;
    END purge_expired_customers;

END PKG_CUSTOMER_MASTER;
/
