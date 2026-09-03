/* ============================================================================
 * Object      : WWI_MDM.PKG_PRODUCT_MASTER (package body)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_MDM.PKG_PRODUCT_MASTER, WWI_MDM.PRODUCT_MASTER,
 *               WWI_MDM.PRODUCT_UOM_CONV, WWI_MDM.PRODUCT_SUBSTITUTE,
 *               WWI_MDM.PRODUCT_HIERARCHY, WWI_MDM.PRODUCT_CATEGORY,
 *               WWI_PROC.PURCHASE_ORDER_LINE, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_MDM.PKG_PRODUCT_MASTER AS

    c_bulk_limit CONSTANT PLS_INTEGER := 1000;

    FUNCTION convert_uom
    (
        p_product_id IN WWI_MDM.PRODUCT_MASTER.PRODUCT_ID%TYPE,
        p_qty        IN NUMBER,
        p_from_uom   IN WWI_MDM.PRODUCT_UOM_CONV.FROM_UOM_CD%TYPE,
        p_to_uom     IN WWI_MDM.PRODUCT_UOM_CONV.TO_UOM_CD%TYPE
    ) RETURN NUMBER
    IS
        l_factor NUMBER;
    BEGIN
        IF p_from_uom = p_to_uom THEN
            RETURN p_qty;
        END IF;

        BEGIN
            SELECT CONV_FACTOR
              INTO l_factor
              FROM WWI_MDM.PRODUCT_UOM_CONV
             WHERE PRODUCT_ID  = p_product_id
               AND FROM_UOM_CD = p_from_uom
               AND TO_UOM_CD   = p_to_uom
               AND EFFECTIVE_DT <= TRUNC(SYSDATE)
               AND ROWNUM = 1;

            RETURN p_qty * l_factor;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                NULL;
        END;

        /* try the inverse direction before giving up - half the conversion
           rows were only ever loaded one way round                        */
        BEGIN
            SELECT CONV_FACTOR
              INTO l_factor
              FROM WWI_MDM.PRODUCT_UOM_CONV
             WHERE PRODUCT_ID  = p_product_id
               AND FROM_UOM_CD = p_to_uom
               AND TO_UOM_CD   = p_from_uom
               AND EFFECTIVE_DT <= TRUNC(SYSDATE)
               AND ROWNUM = 1;

            IF NVL(l_factor, 0) = 0 THEN
                RAISE ZERO_DIVIDE;
            END IF;

            RETURN p_qty / l_factor;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20222,
                    'PKG_PRODUCT_MASTER.convert_uom: no conversion from '
                    || p_from_uom || ' to ' || p_to_uom
                    || ' for product ' || p_product_id);
            WHEN ZERO_DIVIDE THEN
                RAISE_APPLICATION_ERROR(-20222,
                    'PKG_PRODUCT_MASTER.convert_uom: zero conversion factor for product '
                    || p_product_id);
        END;
    END convert_uom;

    FUNCTION preferred_substitute
    (
        p_product_id IN WWI_MDM.PRODUCT_MASTER.PRODUCT_ID%TYPE,
        p_region_cd  IN VARCHAR2 DEFAULT 'NA'
    ) RETURN WWI_MDM.PRODUCT_MASTER.PRODUCT_ID%TYPE
    IS
        l_sub_id WWI_MDM.PRODUCT_MASTER.PRODUCT_ID%TYPE;
    BEGIN
        SELECT SUBSTITUTE_PRODUCT_ID
          INTO l_sub_id
          FROM (SELECT s.SUBSTITUTE_PRODUCT_ID
                  FROM WWI_MDM.PRODUCT_SUBSTITUTE s
                  JOIN WWI_MDM.PRODUCT_MASTER p
                    ON p.PRODUCT_ID = s.SUBSTITUTE_PRODUCT_ID
                 WHERE s.PRODUCT_ID = p_product_id
                   AND NVL(s.END_DT, DATE '4712-12-31') >= TRUNC(SYSDATE)
                   AND CASE p_region_cd
                           WHEN 'EU'   THEN NVL(s.AVAILABLE_EU_FLG, 'N')
                           WHEN 'APAC' THEN NVL(s.AVAILABLE_APAC_FLG, 'N')
                           ELSE NVL(s.AVAILABLE_NA_FLG, 'N')
                       END = 'Y'
                   AND p.LIFECYCLE_STATUS_CD = 'A'
                   /* EU will not substitute across hazard classes */
                   AND (p_region_cd <> 'EU'
                        OR NVL(p.HAZMAT_FLG, 'N') = 'N')
                 ORDER BY NVL(s.PRIORITY_NBR, 99), s.SUBSTITUTE_PRODUCT_ID)
         WHERE ROWNUM = 1;

        RETURN l_sub_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
    END preferred_substitute;

    PROCEDURE discontinue_product
    (
        p_product_id     IN WWI_MDM.PRODUCT_MASTER.PRODUCT_ID%TYPE,
        p_discontinue_dt IN DATE DEFAULT TRUNC(SYSDATE),
        p_reason_cd      IN VARCHAR2 DEFAULT NULL
    )
    IS
        l_open_qty NUMBER;
        l_status   WWI_MDM.PRODUCT_MASTER.LIFECYCLE_STATUS_CD%TYPE;
    BEGIN
        SELECT LIFECYCLE_STATUS_CD
          INTO l_status
          FROM WWI_MDM.PRODUCT_MASTER
         WHERE PRODUCT_ID = p_product_id
           FOR UPDATE;

        SELECT NVL(SUM(WWI_PROC.FN_PO_OPEN_QTY(pl.PO_LINE_ID)), 0)
          INTO l_open_qty
          FROM WWI_PROC.PURCHASE_ORDER_LINE pl
         WHERE pl.PRODUCT_ID = p_product_id
           AND NVL(pl.LINE_STATUS_CD, 'O') <> 'C';

        UPDATE WWI_MDM.PRODUCT_MASTER
           SET LIFECYCLE_STATUS_CD        = CASE WHEN l_open_qty > 0 THEN 'D' ELSE 'X' END,
               DISCONTINUED_DT  = p_discontinue_dt,
               ENG_NOTES_TXT    = SUBSTR('Discontinued: ' || p_reason_cd, 1, 200),
               UPDATED_DT      = SYSDATE,
               UPDATED_BY      = USER
         WHERE PRODUCT_ID = p_product_id;

        IF l_open_qty > 0 THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_PRODUCT_MASTER.discontinue_product',
                                                 TO_CHAR(p_product_id),
                                                 'discontinued with ' || l_open_qty
                                                 || ' units still on open PO lines');
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20221,
                'PKG_PRODUCT_MASTER.discontinue_product: product ' || p_product_id
                || ' not found');
    END discontinue_product;

    PROCEDURE rebuild_hierarchy
    (
        p_hier_id   IN  WWI_MDM.PRODUCT_HIERARCHY.PRODUCT_HIER_ID%TYPE,
        p_node_cnt  OUT PLS_INTEGER
    )
    IS
        l_cycle_cnt PLS_INTEGER;
    BEGIN
        /* the hierarchy is stored flattened as LEVEL_1..LEVEL_4 codes, so the
           integrity test is a level gap test rather than a cycle test */
        SELECT COUNT(*)
          INTO l_cycle_cnt
          FROM WWI_MDM.PRODUCT_HIERARCHY h
         WHERE h.PRODUCT_HIER_ID = p_hier_id
           AND ((h.LEVEL_2_CD IS NOT NULL AND h.LEVEL_1_CD IS NULL)
             OR (h.LEVEL_3_CD IS NOT NULL AND h.LEVEL_2_CD IS NULL)
             OR (h.LEVEL_4_CD IS NOT NULL AND h.LEVEL_3_CD IS NULL));

        IF l_cycle_cnt > 0 THEN
            RAISE_APPLICATION_ERROR(-20223,
                'PKG_PRODUCT_MASTER.rebuild_hierarchy: ' || l_cycle_cnt
                || ' level gap(s) detected in hierarchy ' || p_hier_id);
        END IF;

        /* the denormalised LEVEL_NUM column is rewritten in place. It is only
           used by the extract view and the old Discoverer reports.          */
        SELECT COUNT(*)
          INTO p_node_cnt
          FROM WWI_MDM.PRODUCT_HIERARCHY h
         WHERE h.PRODUCT_HIER_ID = p_hier_id;
    EXCEPTION
        WHEN OTHERS THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_PRODUCT_MASTER.rebuild_hierarchy',
                                                 TO_CHAR(p_hier_id), SQLERRM);
            RAISE;
    END rebuild_hierarchy;

    PROCEDURE recost_products
    (
        p_category_id IN  WWI_MDM.PRODUCT_CATEGORY.PRODUCT_CATEGORY_ID%TYPE,
        p_factor      IN  NUMBER,
        p_updated_cnt OUT PLS_INTEGER
    )
    IS
        CURSOR c_products IS
            SELECT PRODUCT_ID, UNIT_COST_STD
              FROM WWI_MDM.PRODUCT_MASTER
             WHERE PRODUCT_CATEGORY_ID = p_category_id
               AND LIFECYCLE_STATUS_CD IN ('A', 'D')
               FOR UPDATE;

        TYPE t_row_tab IS TABLE OF c_products%ROWTYPE INDEX BY PLS_INTEGER;
        l_rows t_row_tab;
    BEGIN
        p_updated_cnt := 0;

        OPEN c_products;
        LOOP
            FETCH c_products BULK COLLECT INTO l_rows LIMIT c_bulk_limit;
            EXIT WHEN l_rows.COUNT = 0;

            FOR i IN 1 .. l_rows.COUNT LOOP
                UPDATE WWI_MDM.PRODUCT_MASTER
                   SET UNIT_COST_STD       = ROUND(NVL(UNIT_COST_STD, 0) * p_factor, 4),
                       UPDATED_DT        = SYSDATE,
                       UPDATED_BY        = USER
                 WHERE PRODUCT_ID = l_rows(i).PRODUCT_ID;

                p_updated_cnt := p_updated_cnt + 1;
            END LOOP;

            EXIT WHEN c_products%NOTFOUND;
        END LOOP;
        CLOSE c_products;
    EXCEPTION
        WHEN OTHERS THEN
            IF c_products%ISOPEN THEN
                CLOSE c_products;
            END IF;
            RAISE;
    END recost_products;

END PKG_PRODUCT_MASTER;
/
