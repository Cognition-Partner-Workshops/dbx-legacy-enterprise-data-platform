/* ============================================================================
 * Object      : WWI_MDM.FN_PRODUCT_ACTIVE_FLAG (function)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.PRODUCT_MASTER, WWI_MDM.PRODUCT_SUBSTITUTE,
 *               WWI_PROC.PURCHASE_ORDER_LINE
 * Called by   : WWI_MDM.V_PRODUCT_EXTRACT, WWI_MDM.PKG_PRODUCT_MASTER,
 *               WWI_PROC.PKG_PURCHASE_ORDER
 * History     : 2003 original; 2013 region parameter added when APAC started
 *               running its own discontinuation calendar.
 * Notes       : A product that is discontinued but still has open purchase
 *               order quantity is reported as 'R' (run-out), not 'N'. The
 *               warehouse maps 'R' to active, which is a known divergence
 *               between the ERP and the dimension.
 * ========================================================================= */

CREATE OR REPLACE FUNCTION WWI_MDM.FN_PRODUCT_ACTIVE_FLAG
(
    p_product_id IN WWI_MDM.PRODUCT_MASTER.PRODUCT_ID%TYPE,
    p_region_cd  IN VARCHAR2 DEFAULT 'NA',
    p_as_of_dt   IN DATE     DEFAULT SYSDATE
)
RETURN VARCHAR2
IS
    l_status_cd        WWI_MDM.PRODUCT_MASTER.LIFECYCLE_STATUS_CD%TYPE;
    l_discontinued_dt  WWI_MDM.PRODUCT_MASTER.DISCONTINUED_DT%TYPE;
    l_hazmat_flag      WWI_MDM.PRODUCT_MASTER.HAZMAT_FLG%TYPE;
    l_open_qty         NUMBER := 0;
    l_sub_count        PLS_INTEGER := 0;
    l_grace_days       PLS_INTEGER;
BEGIN
    SELECT p.LIFECYCLE_STATUS_CD, p.DISCONTINUED_DT, p.HAZMAT_FLG
      INTO l_status_cd, l_discontinued_dt, l_hazmat_flag
      FROM WWI_MDM.PRODUCT_MASTER p
     WHERE p.PRODUCT_ID = p_product_id;

    /* EU hazardous goods lose sellability on the discontinuation date itself;
       everyone else gets a run-out window. */
    l_grace_days :=
        CASE
            WHEN UPPER(p_region_cd) = 'EU' AND NVL(l_hazmat_flag, 'N') = 'Y' THEN 0
            WHEN UPPER(p_region_cd) = 'EU'   THEN 90
            WHEN UPPER(p_region_cd) = 'APAC' THEN 45
            ELSE 180
        END;

    IF l_status_cd = 'OBSL' AND l_discontinued_dt IS NULL THEN
        RETURN 'N';
    END IF;

    IF l_discontinued_dt IS NOT NULL AND p_as_of_dt > l_discontinued_dt + l_grace_days THEN
        RETURN 'N';
    END IF;

    IF l_discontinued_dt IS NOT NULL THEN
        SELECT NVL(SUM(GREATEST(pl.ORDER_QTY - NVL(pl.RECEIVED_QTY, 0)
                                - NVL(pl.CANCELLED_QTY, 0), 0)), 0)
          INTO l_open_qty
          FROM WWI_PROC.PURCHASE_ORDER_LINE pl
         WHERE pl.PRODUCT_ID = p_product_id
           AND pl.LINE_STATUS_CD NOT IN ('CLSD', 'CANC');

        SELECT COUNT(*)
          INTO l_sub_count
          FROM WWI_MDM.PRODUCT_SUBSTITUTE s
         WHERE s.PRODUCT_ID = p_product_id
           AND p_as_of_dt BETWEEN s.EFFECTIVE_DT
                              AND NVL(s.END_DT, DATE '4712-12-31')
           AND CASE UPPER(p_region_cd)
                   WHEN 'EU'   THEN s.AVAILABLE_EU_FLG
                   WHEN 'APAC' THEN s.AVAILABLE_APAC_FLG
                   ELSE             s.AVAILABLE_NA_FLG
               END = 'Y';

        IF l_open_qty > 0 OR l_sub_count = 0 THEN
            RETURN 'R';
        END IF;
        RETURN 'N';
    END IF;

    RETURN CASE WHEN l_status_cd IN ('ACT', 'NEW') THEN 'Y' ELSE 'N' END;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 'N';
END FN_PRODUCT_ACTIVE_FLAG;
/
