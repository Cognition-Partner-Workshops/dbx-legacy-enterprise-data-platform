/* ============================================================================
 * Object      : WWI_FIN.PRC_RUN_DUNNING (procedure)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.AP_AGING_SNAPSHOT, WWI_MDM.SUPP_MASTER,
 *               WWI_MDM.SUPP_CONTACT, WWI_AUDIT.CHANGE_LOG,
 *               WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : DBMS_JOB 'ERP_DUNNING_WEEKLY' (Monday 06:00 per region)
 * History     : Written for NA in 2001. EU consent handling was added in 2018
 *               and APAC's manual-review rule in 2013; the three paths never
 *               got refactored together.
 * Notes       : This chases supplier debit balances - credit notes and
 *               overpayments the supplier owes back. AP was the only module
 *               with an aging engine in 2001, so the receivables team was
 *               given a dunning run on top of it and it never moved. The
 *               letter itself is produced downstream from the rows written
 *               to WWI_AUDIT.CHANGE_LOG with object name DUNNING.
 * ========================================================================= */

CREATE OR REPLACE PROCEDURE WWI_FIN.PRC_RUN_DUNNING
(
    p_region_cd   IN  VARCHAR2,
    p_as_of_dt    IN  DATE DEFAULT TRUNC(SYSDATE),
    p_letter_cnt  OUT PLS_INTEGER,
    p_skipped_cnt OUT PLS_INTEGER
)
IS
    CURSOR c_overdue IS
        SELECT sm.SUPP_ID,
               sm.SUPP_NAME,
               sm.CONSENT_FLAG,
               sm.STATUS_CD,
               MAX(s.DAYS_PAST_DUE)      AS MAX_DAYS_PAST_DUE,
               SUM(-s.BASE_OPEN_AMT)     AS TOTAL_OPEN_AMT,
               COUNT(*)                  AS OPEN_ITEM_CNT
          FROM WWI_FIN.AP_AGING_SNAPSHOT s
          JOIN WWI_MDM.SUPP_MASTER sm
            ON sm.SUPP_ID = s.SUPP_ID
         WHERE s.REGION_CD   = p_region_cd
           AND s.SNAPSHOT_DT = (SELECT MAX(s2.SNAPSHOT_DT)
                                  FROM WWI_FIN.AP_AGING_SNAPSHOT s2
                                 WHERE s2.REGION_CD = p_region_cd
                                   AND s2.SNAPSHOT_DT <= p_as_of_dt)
           AND s.DAYS_PAST_DUE > 0
           AND s.BASE_OPEN_AMT < 0
         GROUP BY sm.SUPP_ID, sm.SUPP_NAME, sm.CONSENT_FLAG, sm.STATUS_CD;

    l_level_cd   VARCHAR2(10);
    l_min_days   PLS_INTEGER;
    l_min_amt    NUMBER;
    l_email_txt  WWI_MDM.SUPP_CONTACT.EMAIL_TXT%TYPE;
BEGIN
    p_letter_cnt  := 0;
    p_skipped_cnt := 0;

    /* thresholds diverge: EU is legally cautious, APAC is relationship led */
    l_min_days := CASE p_region_cd WHEN 'EU' THEN 14 WHEN 'APAC' THEN 30 ELSE 7 END;
    l_min_amt  := CASE p_region_cd WHEN 'EU' THEN 250 WHEN 'APAC' THEN 1000 ELSE 100 END;

    FOR rec IN c_overdue LOOP
        IF rec.MAX_DAYS_PAST_DUE < l_min_days
           OR NVL(rec.TOTAL_OPEN_AMT, 0) < l_min_amt
           OR rec.STATUS_CD <> 'A' THEN
            p_skipped_cnt := p_skipped_cnt + 1;
            CONTINUE;
        END IF;

        IF p_region_cd = 'EU' AND NVL(rec.CONSENT_FLAG, 'N') <> 'Y' THEN
            /* no marketing or reminder contact without recorded consent */
            p_skipped_cnt := p_skipped_cnt + 1;
            WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL, 'WWI_MDM.SUPP_MASTER',
                TO_CHAR(rec.SUPP_ID), 'NO_CONSENT',
                'dunning suppressed, consent flag is '
                || NVL(rec.CONSENT_FLAG, 'NULL'), 'W');
            CONTINUE;
        END IF;

        IF p_region_cd = 'EU' THEN
            l_level_cd := CASE
                              WHEN rec.MAX_DAYS_PAST_DUE > 60 THEN 'LEGAL'
                              WHEN rec.MAX_DAYS_PAST_DUE > 30 THEN 'D2'
                              ELSE 'D1'
                          END;
        ELSIF p_region_cd = 'APAC' THEN
            /* APAC never auto-escalates past the second reminder; anything
               worse goes to the country manager as a review task          */
            l_level_cd := CASE
                              WHEN rec.MAX_DAYS_PAST_DUE > 90 THEN 'REVIEW'
                              WHEN rec.MAX_DAYS_PAST_DUE > 45 THEN 'D2'
                              ELSE 'D1'
                          END;
        ELSE
            l_level_cd := CASE
                              WHEN rec.MAX_DAYS_PAST_DUE > 90 THEN 'COLLECT'
                              WHEN rec.MAX_DAYS_PAST_DUE > 45 THEN 'D3'
                              WHEN rec.MAX_DAYS_PAST_DUE > 21 THEN 'D2'
                              ELSE 'D1'
                          END;
        END IF;

        BEGIN
            SELECT MIN(ct.EMAIL_TXT)
              INTO l_email_txt
              FROM WWI_MDM.SUPP_CONTACT ct
             WHERE ct.SUPP_ID = rec.SUPP_ID
               AND ct.CONTACT_TYPE_CD = 'AR'
               AND NVL(ct.ACTIVE_FLAG, 'Y') = 'Y';
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                l_email_txt := NULL;
        END;

        IF l_email_txt IS NULL AND l_level_cd IN ('D1', 'D2') THEN
            p_skipped_cnt := p_skipped_cnt + 1;
            WWI_AUDIT.PKG_DATA_QUALITY.log_reject(NULL, 'WWI_MDM.SUPP_CONTACT',
                TO_CHAR(rec.SUPP_ID), 'NO_AR_CONTACT',
                'no active AR contact for dunning', 'W');
            CONTINUE;
        END IF;

        INSERT INTO WWI_AUDIT.CHANGE_LOG
            (CHANGE_LOG_ID, SRC_SCHEMA_NAME, SRC_OBJECT_NAME, SRC_KEY_TXT,
             CHANGE_TYPE_CD, CHANGE_DT, CHANGE_DETAIL_TXT, EXTRACTED_FLAG,
             CHANGED_BY)
        VALUES
            (WWI_AUDIT.SEQ_CHANGE_LOG.NEXTVAL, 'WWI_FIN', 'DUNNING',
             TO_CHAR(rec.SUPP_ID), l_level_cd, SYSDATE,
             p_region_cd || ' ' || rec.OPEN_ITEM_CNT || ' item(s) '
             || ROUND(rec.TOTAL_OPEN_AMT, 2) || ' max '
             || rec.MAX_DAYS_PAST_DUE || ' days late; contact '
             || NVL(l_email_txt, 'POST'), 'N', USER);

        /* only NA lets the job put a payment hold on the supplier; EU and
           APAC credit control insist on a human decision                 */
        IF p_region_cd = 'NA' AND l_level_cd = 'COLLECT' THEN
            UPDATE WWI_MDM.SUPP_MASTER
               SET PAYMENT_HOLD_FLAG = 'Y',
                   LAST_UPD_DT       = SYSDATE,
                   LAST_UPD_BY       = USER
             WHERE SUPP_ID = rec.SUPP_ID;
        END IF;

        p_letter_cnt := p_letter_cnt + 1;

        IF MOD(p_letter_cnt, 200) = 0 THEN
            COMMIT;
        END IF;
    END LOOP;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        WWI_AUDIT.PKG_DATA_QUALITY.log_error('PRC_RUN_DUNNING', p_region_cd,
                                             SQLERRM);
        RAISE;
END PRC_RUN_DUNNING;
/
