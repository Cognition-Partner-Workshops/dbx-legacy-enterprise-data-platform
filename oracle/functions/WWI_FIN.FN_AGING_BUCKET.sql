/* ============================================================================
 * Object      : WWI_FIN.FN_AGING_BUCKET (function)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : none (pure arithmetic)
 * Called by   : WWI_FIN.PRC_LOAD_AP_AGING, WWI_FIN.V_AP_AGING_CURRENT,
 *               WWI_FIN.PKG_AP_PAYMENT, WWI_FIN.PRC_RUN_DUNNING
 * History     : 1996 NA buckets; 2005 EU buckets; 2011 APAC buckets.
 * Notes       : Three bucket sets that do not line up. Warehouse reporting
 *               re-buckets NA-style on top of this, which is why the aging
 *               fact and the ERP aging snapshot never agree exactly.
 * ========================================================================= */

CREATE OR REPLACE FUNCTION WWI_FIN.FN_AGING_BUCKET
(
    p_days_past_due IN NUMBER,
    p_region_cd     IN VARCHAR2 DEFAULT 'NA'
)
RETURN VARCHAR2 DETERMINISTIC
IS
    l_days NUMBER := NVL(p_days_past_due, 0);
BEGIN
    IF l_days <= 0 THEN
        RETURN CASE UPPER(p_region_cd) WHEN 'EU' THEN 'NOT_DUE' ELSE 'CURRENT' END;
    END IF;

    IF UPPER(p_region_cd) = 'EU' THEN
        RETURN CASE
                   WHEN l_days <= 30  THEN 'D01_30'
                   WHEN l_days <= 60  THEN 'D31_60'
                   WHEN l_days <= 90  THEN 'D61_90'
                   ELSE 'D90_PLUS'
               END;
    ELSIF UPPER(p_region_cd) = 'APAC' THEN
        RETURN CASE
                   WHEN l_days <= 15 THEN 'D01_15'
                   WHEN l_days <= 30 THEN 'D16_30'
                   WHEN l_days <= 45 THEN 'D31_45'
                   WHEN l_days <= 60 THEN 'D46_60'
                   ELSE 'D60_PLUS'
               END;
    END IF;

    RETURN CASE
               WHEN l_days <= 30  THEN 'B1_1_30'
               WHEN l_days <= 60  THEN 'B2_31_60'
               WHEN l_days <= 90  THEN 'B3_61_90'
               WHEN l_days <= 120 THEN 'B4_91_120'
               ELSE 'B5_120_PLUS'
           END;
END FN_AGING_BUCKET;
/
