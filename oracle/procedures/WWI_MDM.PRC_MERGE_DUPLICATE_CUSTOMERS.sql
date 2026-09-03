/* ============================================================================
 * Object      : WWI_MDM.PRC_MERGE_DUPLICATE_CUSTOMERS (procedure)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.PKG_CUSTOMER_MASTER, WWI_MDM.CUST_MASTER,
 *               WWI_MDM.FN_NORMALIZE_NAME, WWI_MDM.MDM_MERGE_HISTORY,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : DBMS_JOB 'MDM_DEDUPE_WEEKLY', data stewards on demand
 * Notes       : Candidate pairs are found on the normalised name within a
 *               country, then scored. Only high scores merge automatically;
 *               the rest are written out as steward tasks. The survivor is
 *               the older record, which is how the 2006 clean-up was run and
 *               downstream keys now depend on it.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_MDM.PRC_MERGE_DUPLICATE_CUSTOMERS
(
    p_region_cd    IN  VARCHAR2,
    p_auto_score   IN  NUMBER DEFAULT NULL,
    p_dry_run      IN  VARCHAR2 DEFAULT 'Y',
    p_merged_cnt   OUT PLS_INTEGER,
    p_review_cnt   OUT PLS_INTEGER
)
IS
    CURSOR c_pairs IS
        SELECT a.CUST_ID AS SURVIVOR_ID,
               b.CUST_ID AS DUP_ID,
               a.CUST_NAME,
               a.COUNTRY_CD
          FROM WWI_MDM.CUST_MASTER a
          JOIN WWI_MDM.CUST_MASTER b
            ON b.COUNTRY_CD = a.COUNTRY_CD
           AND b.CUST_ID    > a.CUST_ID
           AND WWI_MDM.FN_NORMALIZE_NAME(b.CUST_NAME)
             = WWI_MDM.FN_NORMALIZE_NAME(a.CUST_NAME)
         WHERE a.REGION_CD = p_region_cd
           AND b.REGION_CD = p_region_cd
           AND a.CUST_STATUS_CD <> 'M'
           AND b.CUST_STATUS_CD <> 'M'
         ORDER BY a.CUST_ID, b.CUST_ID;

    l_score     NUMBER;
    l_threshold NUMBER;
BEGIN
    p_merged_cnt := 0;
    p_review_cnt := 0;

    /* EU stewards insisted on a near certain match before an automatic
       merge; APAC sits in the middle because of transliterated names   */
    l_threshold := NVL(p_auto_score,
                       CASE p_region_cd
                           WHEN 'EU'   THEN 95
                           WHEN 'APAC' THEN 90
                           ELSE 80
                       END);

    FOR pair IN c_pairs LOOP
        l_score := WWI_MDM.PKG_CUSTOMER_MASTER.match_score(pair.SURVIVOR_ID,
                                                           pair.DUP_ID);

        IF l_score >= l_threshold THEN
            IF NVL(p_dry_run, 'Y') = 'Y' THEN
                p_merged_cnt := p_merged_cnt + 1;
                CONTINUE;
            END IF;

            BEGIN
                WWI_MDM.PKG_CUSTOMER_MASTER.merge_customer(
                    p_surviving_id => pair.SURVIVOR_ID,
                    p_merged_id    => pair.DUP_ID,
                    p_merged_by    => 'MDM_DEDUPE_WEEKLY',
                    p_reason_txt   => 'auto merge score ' || l_score);

                p_merged_cnt := p_merged_cnt + 1;
                COMMIT;
            EXCEPTION
                WHEN OTHERS THEN
                    ROLLBACK;
                    WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL,
                        'WWI_MDM.CUST_MASTER',
                        pair.SURVIVOR_ID || '/' || pair.DUP_ID, 'MERGE_FAILED',
                        SQLERRM, 'E');
            END;
        ELSIF l_score >= 60 THEN
            p_review_cnt := p_review_cnt + 1;
            WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL, 'WWI_MDM.CUST_MASTER',
                pair.SURVIVOR_ID || '/' || pair.DUP_ID, 'MERGE_REVIEW',
                'score ' || l_score || ' below auto threshold ' || l_threshold,
                'W');
        END IF;
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_MERGE_DUPLICATE_CUSTOMERS',
                                             p_region_cd, SQLERRM);
        RAISE;
END PRC_MERGE_DUPLICATE_CUSTOMERS;
/
