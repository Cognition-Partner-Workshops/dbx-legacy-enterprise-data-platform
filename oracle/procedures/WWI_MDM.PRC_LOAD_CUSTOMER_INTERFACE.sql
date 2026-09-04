/* ============================================================================
 * Object      : WWI_MDM.PRC_LOAD_CUSTOMER_INTERFACE (procedure)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.PKG_CUSTOMER_MASTER, WWI_MDM.PARTY_XREF,
 *               WWI_MDM.CUST_MASTER, WWI_AUDIT.CHANGE_LOG,
 *               WWI_AUDIT.PKG_DATA_QUALITY, WWI_REF.SOURCE_SYSTEM_REF
 * Called by   : DBMS_JOB 'ERP_CRM_CUSTOMER_FEED' (hourly)
 * Notes       : Row at a time by design - the CRM feed sends corrections out
 *               of order and a set based load kept applying the older row
 *               last. Reads the CRM extract over a database link whose name
 *               is held in WWI_REF.SOURCE_SYSTEM_REF.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_MDM.PRC_LOAD_CUSTOMER_INTERFACE
(
    p_src_system_cd IN  WWI_REF.SOURCE_SYSTEM_REF.SOURCE_SYS_CD%TYPE DEFAULT 'CRM',
    p_max_rows      IN  PLS_INTEGER DEFAULT 20000,
    p_applied_cnt   OUT PLS_INTEGER,
    p_rejected_cnt  OUT PLS_INTEGER
)
IS
    TYPE t_ref IS REF CURSOR;
    TYPE t_feed_rec IS RECORD (
        ext_party_id WWI_MDM.PARTY_XREF.SOURCE_KEY_TXT%TYPE,
        cust_num     WWI_MDM.CUST_MASTER.CUST_NBR%TYPE,
        cust_name    WWI_MDM.CUST_MASTER.CUST_NAME%TYPE,
        region_cd    WWI_MDM.CUST_MASTER.REGION_CD%TYPE,
        country_cd   WWI_MDM.CUST_MASTER.COUNTRY_CD%TYPE,
        segment_cd   WWI_MDM.CUST_SEGMENT_ASSIGN.SEGMENT_CD%TYPE,
        line1_txt    WWI_MDM.CUST_ADDRESS.ADDR_LINE_1%TYPE,
        line2_txt    WWI_MDM.CUST_ADDRESS.ADDR_LINE_2%TYPE,
        city_name    WWI_MDM.CUST_ADDRESS.CITY_TXT%TYPE,
        state_cd     WWI_MDM.CUST_ADDRESS.STATE_PROV_CD%TYPE,
        postal_cd    WWI_MDM.CUST_ADDRESS.POSTAL_CD%TYPE,
        changed_dt   DATE
    );
    TYPE t_feed_tab IS TABLE OF t_feed_rec;

    l_link_name WWI_REF.SOURCE_SYSTEM_REF.CONNECTION_PARAM_NAME%TYPE;
    l_sql       VARCHAR2(4000);
    l_cur       t_ref;
    l_rows      t_feed_tab;
    l_cust_id   WWI_MDM.CUST_MASTER.CUST_ID%TYPE;
    l_action_cd VARCHAR2(10);
    l_xref_cnt  PLS_INTEGER;
BEGIN
    p_applied_cnt  := 0;
    p_rejected_cnt := 0;

    SELECT CONNECTION_PARAM_NAME
      INTO l_link_name
      FROM WWI_REF.SOURCE_SYSTEM_REF
     WHERE SOURCE_SYS_CD = p_src_system_cd;

    l_sql := 'SELECT ext_party_id, cust_num, cust_name, region_cd, country_cd, '
          || 'segment_cd, addr_line1, addr_line2, city, state_cd, postal_cd, '
          || 'changed_dt FROM customer_feed@' || l_link_name || ' '
          || 'WHERE applied_flag = ''N'' AND ROWNUM <= :n '
          || 'ORDER BY changed_dt, ext_party_id';

    OPEN l_cur FOR l_sql USING p_max_rows;
    LOOP
        FETCH l_cur BULK COLLECT INTO l_rows LIMIT 100;
        EXIT WHEN l_rows.COUNT = 0;

        FOR i IN 1 .. l_rows.COUNT LOOP
            BEGIN
                IF l_rows(i).cust_name IS NULL OR l_rows(i).country_cd IS NULL THEN
                    RAISE_APPLICATION_ERROR(-20611,
                        'name and country are mandatory');
                END IF;

                WWI_MDM.PKG_CUSTOMER_MASTER.upsert_customer(
                    p_cust_num   => l_rows(i).cust_num,
                    p_cust_name  => l_rows(i).cust_name,
                    p_region_cd  => l_rows(i).region_cd,
                    p_country_cd => l_rows(i).country_cd,
                    p_segment_cd => l_rows(i).segment_cd,
                    p_src_system => p_src_system_cd,
                    p_cust_id    => l_cust_id,
                    p_action_cd  => l_action_cd);

                IF l_rows(i).line1_txt IS NOT NULL THEN
                    WWI_MDM.PKG_CUSTOMER_MASTER.apply_address_change(
                        p_cust_id      => l_cust_id,
                        p_address_type => 'BILL',
                        p_line1_txt    => l_rows(i).line1_txt,
                        p_line2_txt    => l_rows(i).line2_txt,
                        p_city_name    => l_rows(i).city_name,
                        p_state_cd     => l_rows(i).state_cd,
                        p_postal_cd    => l_rows(i).postal_cd,
                        p_country_cd   => l_rows(i).country_cd,
                        p_eff_dt       => TRUNC(NVL(l_rows(i).changed_dt, SYSDATE)));
                END IF;

                SELECT COUNT(*)
                  INTO l_xref_cnt
                  FROM WWI_MDM.PARTY_XREF
                 WHERE SOURCE_SYS_CD = p_src_system_cd
                   AND SOURCE_KEY_TXT  = l_rows(i).ext_party_id;

                IF l_xref_cnt = 0 THEN
                    INSERT INTO WWI_MDM.PARTY_XREF
                        (PARTY_XREF_ID, PARTY_TYPE_CD, CUST_ID, SOURCE_SYS_CD,
                         SOURCE_KEY_TXT, ACTIVE_FLG, CREATED_DT, CREATED_BY)
                    VALUES
                        (WWI_MDM.SEQ_PARTY_XREF.NEXTVAL, 'CUST', l_cust_id,
                         p_src_system_cd, l_rows(i).ext_party_id, 'Y', SYSDATE,
                         USER);
                END IF;

                INSERT INTO WWI_AUDIT.CHANGE_LOG
                    (CHANGE_LOG_ID, SCHEMA_NAME, TABLE_NAME, PK_VALUE_TXT,
                     OPERATION_CD, CHANGE_TS, NEW_VALUE_TXT, EXTRACTED_FLG,
                     CHANGED_BY)
                VALUES
                    (WWI_AUDIT.SEQ_CHANGE_LOG.NEXTVAL, 'WWI_MDM', 'CUST_MASTER',
                     TO_CHAR(l_cust_id), l_action_cd, SYSDATE,
                     p_src_system_cd || ' feed ' || l_rows(i).ext_party_id, 'N',
                     USER);

                p_applied_cnt := p_applied_cnt + 1;
                COMMIT;
            EXCEPTION
                WHEN OTHERS THEN
                    ROLLBACK;
                    p_rejected_cnt := p_rejected_cnt + 1;
                    WWI_AUDIT.PKG_DATA_QUALITY.log_reject(p_src_system_cd,
                        'customer_feed', l_rows(i).ext_party_id, 'APPLY_FAILED',
                        SQLERRM, 'E');
            END;
        END LOOP;
    END LOOP;
    CLOSE l_cur;
EXCEPTION
    WHEN OTHERS THEN
        IF l_cur%ISOPEN THEN
            CLOSE l_cur;
        END IF;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_LOAD_CUSTOMER_INTERFACE',
                                             p_src_system_cd, SQLERRM);
        RAISE;
END PRC_LOAD_CUSTOMER_INTERFACE;
/
