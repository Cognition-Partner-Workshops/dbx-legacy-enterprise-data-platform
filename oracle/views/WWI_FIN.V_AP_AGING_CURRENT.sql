/* ============================================================================
 * Object      : WWI_FIN.V_AP_AGING_CURRENT (view)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.AP_AGING_SNAPSHOT, WWI_MDM.SUPP_MASTER,
 *               WWI_FIN.FN_AGING_BUCKET
 * Called by   : SSIS EXT_ORA_ApAging, AP management reporting
 * Notes       : Reads the latest snapshot produced by
 *               WWI_FIN.PRC_BUILD_AGING_SNAPSHOT. It does NOT recompute aging
 *               live - the snapshot is the audited number.
 *
 *               The snapshot is held per supplier, region and snapshot date,
 *               with the balance already split into buckets; there is no
 *               invoice grain on it. Invoice-level aging is a join of
 *               V_AP_INVOICE_EXTRACT and the aging function, not this view.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_FIN.V_AP_AGING_CURRENT AS
SELECT a.SNAPSHOT_ID,
       a.SNAPSHOT_DT,
       a.PERIOD_CD,
       a.SUPP_ID,
       s.SUPP_NBR                                                 AS SUPP_NUM,
       s.SUPP_NAME,
       a.REGION_CD,
       a.LEGAL_ENTITY_CD,
       a.BALANCE_CURR_CD                                          AS CURRENCY_CD,
       a.OPEN_INVOICE_CNT,
       a.OLDEST_INVOICE_DT,
       a.AVG_DAYS_OUTSTANDING,
       a.CURRENT_AMT,
       a.BUCKET_1_AMT,
       a.BUCKET_2_AMT,
       a.BUCKET_3_AMT,
       a.BUCKET_4_AMT,
       a.TOTAL_OUTSTANDING_AMT                                    AS OUTSTANDING_AMT,
       a.REPORTING_AMT_USD                                        AS OUTSTANDING_BASE_AMT,
       a.DISPUTED_AMT,
       a.ON_HOLD_AMT,
       a.BUCKET_DEFINITION_CD,
       WWI_FIN.FN_AGING_BUCKET(a.AVG_DAYS_OUTSTANDING, a.REGION_CD) AS AGING_BUCKET_CD,
       CASE WHEN NVL(a.ON_HOLD_AMT, 0) > 0  THEN 'Y' ELSE 'N' END AS HOLD_FLAG,
       CASE WHEN NVL(a.DISPUTED_AMT, 0) > 0 THEN 'Y' ELSE 'N' END AS DISPUTE_FLAG,
       CASE
           WHEN NVL(a.DISPUTED_AMT, 0) > 0        THEN 'EXCLUDE'
           WHEN NVL(a.ON_HOLD_AMT, 0) > 0         THEN 'HOLD'
           WHEN NVL(a.AVG_DAYS_OUTSTANDING, 0) > 90 THEN 'ESCALATE'
           ELSE 'NORMAL'
       END                                                        AS COLLECTION_ACTION_CD
  FROM WWI_FIN.AP_AGING_SNAPSHOT a
  JOIN WWI_MDM.SUPP_MASTER s
    ON s.SUPP_ID = a.SUPP_ID
 WHERE a.SNAPSHOT_DT = (SELECT MAX(a2.SNAPSHOT_DT)
                          FROM WWI_FIN.AP_AGING_SNAPSHOT a2
                         WHERE a2.REGION_CD = a.REGION_CD)
/
