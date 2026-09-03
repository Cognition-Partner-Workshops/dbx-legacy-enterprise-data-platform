/* ============================================================================
 * Object      : WWI_PROC.V_SUPPLIER_SCORECARD_CURRENT (view)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.SUPPLIER_SCORECARD, WWI_MDM.SUPP_MASTER,
 *               WWI_PROC.PO_RECEIPT_LINE, WWI_PROC.PURCHASE_ORDER_LINE,
 *               WWI_PROC.PURCHASE_ORDER_HDR, WWI_PROC.VENDOR_CONTRACT
 * Called by   : SSIS PRC_Load_SupplierScorecard, procurement reporting
 * Warning     : Expensive. A DISTINCT over a four-table join is used to find
 *               the suppliers with recent activity, then joined back to the
 *               latest scorecard row. The DISTINCT has been there since 2007
 *               to hide a fan-out nobody has ever traced.
 * Notes       : Reads WWI_MDM; the SELECT grants live in
 *               oracle/ddl/05_grant_privileges.sql.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_PROC.V_SUPPLIER_SCORECARD_CURRENT AS
SELECT s.SUPP_ID,
       s.SUPP_NBR                                        AS SUPP_NUM,
       s.SUPP_NAME,
       s.REGION_CD,
       s.SUPP_STATUS_CD                                  AS SUPPLIER_STATUS_CD,
       sc.SCORE_PERIOD_CD                                AS PERIOD_CD,
       sc.PERIOD_START_DT,
       sc.PERIOD_END_DT,
       sc.OTIF_PCT,
       sc.QUALITY_REJECT_PCT,
       sc.PRICE_VARIANCE_PCT,
       sc.INVOICE_ACCURACY_PCT,
       sc.OVERALL_SCORE,
       sc.SCORE_BAND_CD                                  AS RATING_CD,
       sc.TREND_CD,
       sc.CALCULATED_DT                                  AS CALC_DT,
       NVL(act.ACTIVE_PO_COUNT, 0)                       AS ACTIVE_PO_COUNT,
       /* the regional thresholds were never harmonised */
       CASE s.REGION_CD
           WHEN 'EU'   THEN CASE WHEN sc.OVERALL_SCORE >= 85 THEN 'PREFERRED'
                                 WHEN sc.OVERALL_SCORE >= 65 THEN 'APPROVED'
                                 ELSE 'REVIEW' END
           WHEN 'APAC' THEN CASE WHEN sc.OVERALL_SCORE >= 80 THEN 'PREFERRED'
                                 WHEN sc.OVERALL_SCORE >= 60 THEN 'APPROVED'
                                 ELSE 'REVIEW' END
           ELSE CASE WHEN sc.OVERALL_SCORE >= 90 THEN 'PREFERRED'
                     WHEN sc.OVERALL_SCORE >= 70 THEN 'APPROVED'
                     ELSE 'REVIEW' END
       END                                               AS TIER_CD
  FROM WWI_MDM.SUPP_MASTER s
  JOIN (
        SELECT sc1.*
          FROM WWI_PROC.SUPPLIER_SCORECARD sc1
         WHERE sc1.SCORE_PERIOD_CD = (SELECT MAX(sc2.SCORE_PERIOD_CD)
                                        FROM WWI_PROC.SUPPLIER_SCORECARD sc2
                                       WHERE sc2.SUPP_ID = sc1.SUPP_ID)
       ) sc
    ON sc.SUPP_ID = s.SUPP_ID
  LEFT OUTER JOIN (
        SELECT act_inner.SUPP_ID, COUNT(*) AS ACTIVE_PO_COUNT
          FROM (
                SELECT DISTINCT h.SUPP_ID, h.PO_ID
                  FROM WWI_PROC.PURCHASE_ORDER_HDR h
                  JOIN WWI_PROC.PURCHASE_ORDER_LINE l
                    ON l.PO_ID = h.PO_ID
                  LEFT OUTER JOIN WWI_PROC.PO_RECEIPT_LINE r
                    ON r.PO_LINE_ID = l.PO_LINE_ID
                  LEFT OUTER JOIN WWI_PROC.VENDOR_CONTRACT v
                    ON v.SUPP_ID = h.SUPP_ID
                 WHERE h.ORDER_DT >= ADD_MONTHS(TRUNC(SYSDATE), -12)
                   AND h.PO_STATUS_CD IN ('OPEN', 'PART', 'RECV')
               ) act_inner
         GROUP BY act_inner.SUPP_ID
       ) act
    ON act.SUPP_ID = s.SUPP_ID
 WHERE s.SUPP_STATUS_CD <> 'IN'
   AND NVL(s.DELETED_FLG, 'N') = 'N'
/
