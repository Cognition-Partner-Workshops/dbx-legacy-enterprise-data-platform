/* ============================================================================
 * Object      : WWI_FIN.V_AP_AGING_CURRENT (view)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.AP_AGING_SNAPSHOT, WWI_MDM.SUPP_MASTER,
 *               WWI_FIN.FN_AGING_BUCKET
 * Called by   : SSIS EXT_ORA_ApAgingSnapshot, AP management reporting
 * Notes       : Reads the latest snapshot produced by
 *               WWI_FIN.PRC_BUILD_AGING_SNAPSHOT. It does NOT recompute aging
 *               live - the snapshot is the audited number.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_FIN.V_AP_AGING_CURRENT AS
SELECT a.SNAPSHOT_ID,
       a.SNAPSHOT_DT,
       a.SUPP_ID,
       s.SUPP_NUM,
       s.SUPP_NAME,
       a.REGION_CD,
       a.CURRENCY_CD,
       a.INVOICE_ID,
       a.INVOICE_NUM,
       a.INVOICE_DT,
       a.DUE_DT,
       a.DAYS_PAST_DUE,
       a.OUTSTANDING_AMT,
       a.OUTSTANDING_BASE_AMT,
       NVL(a.AGING_BUCKET_CD,
           WWI_FIN.FN_AGING_BUCKET(a.DAYS_PAST_DUE, a.REGION_CD)) AS AGING_BUCKET_CD,
       a.HOLD_FLAG,
       a.DISPUTE_FLAG,
       CASE
           WHEN NVL(a.DISPUTE_FLAG, 'N') = 'Y'    THEN 'EXCLUDE'
           WHEN NVL(a.HOLD_FLAG, 'N') = 'Y'       THEN 'HOLD'
           WHEN a.DAYS_PAST_DUE > 90              THEN 'ESCALATE'
           ELSE 'NORMAL'
       END                                                        AS COLLECTION_ACTION_CD
  FROM WWI_FIN.AP_AGING_SNAPSHOT a
  JOIN WWI_MDM.SUPP_MASTER s
    ON s.SUPP_ID = a.SUPP_ID
 WHERE a.SNAPSHOT_DT = (SELECT MAX(a2.SNAPSHOT_DT)
                          FROM WWI_FIN.AP_AGING_SNAPSHOT a2
                         WHERE a2.REGION_CD = a.REGION_CD)
/
