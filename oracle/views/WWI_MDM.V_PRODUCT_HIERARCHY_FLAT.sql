/* ============================================================================
 * Object      : WWI_MDM.V_PRODUCT_HIERARCHY_FLAT (view)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.PRODUCT_HIERARCHY, WWI_MDM.PRODUCT_MASTER
 * Called by   : SSIS EXT_ORA_ProductHierarchy (full refresh)
 * History     : 2006 original CONNECT BY flattener; 2011 widened from three to
 *               five levels by adding two more SUBSTR/REGEXP pairs.
 * Notes       : PRODUCT_HIERARCHY stores the assignment already flattened -
 *               one row per product per hierarchy type, with four fixed level
 *               columns - so the extract projects those columns instead of
 *               walking a parent/child tree. LEVEL5_CD is retained in the
 *               extract contract and is always NULL against this table.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_MDM.V_PRODUCT_HIERARCHY_FLAT AS
SELECT h.PRODUCT_HIER_ID                                    AS HIER_ID,
       h.HIER_TYPE_CD,
       h.PRODUCT_ID,
       pm.ITEM_NBR                                          AS PRODUCT_NUM,
       pm.ITEM_DESC_SHORT                                   AS PRODUCT_NAME,
       h.LEVEL_1_CD                                         AS LEVEL1_CD,
       h.LEVEL_1_NAME                                       AS LEVEL1_NAME,
       h.LEVEL_2_CD                                         AS LEVEL2_CD,
       h.LEVEL_2_NAME                                       AS LEVEL2_NAME,
       h.LEVEL_3_CD                                         AS LEVEL3_CD,
       h.LEVEL_3_NAME                                       AS LEVEL3_NAME,
       h.LEVEL_4_CD                                         AS LEVEL4_CD,
       h.LEVEL_4_NAME                                       AS LEVEL4_NAME,
       CAST(NULL AS VARCHAR2(20))                           AS LEVEL5_CD,
       COALESCE(h.LEVEL_4_CD, h.LEVEL_3_CD, h.LEVEL_2_CD, h.LEVEL_1_CD)
                                                            AS NODE_CD,
       COALESCE(h.LEVEL_4_NAME, h.LEVEL_3_NAME, h.LEVEL_2_NAME, h.LEVEL_1_NAME)
                                                            AS NODE_NAME,
       h.LEVEL_1_CD                                         AS ROOT_NODE_CD,
       CASE
           WHEN h.LEVEL_4_CD IS NOT NULL THEN 4
           WHEN h.LEVEL_3_CD IS NOT NULL THEN 3
           WHEN h.LEVEL_2_CD IS NOT NULL THEN 2
           ELSE 1
       END                                                  AS DEPTH_NUM,
       h.LEVEL_1_CD
           || CASE WHEN h.LEVEL_2_CD IS NOT NULL THEN '|' || h.LEVEL_2_CD END
           || CASE WHEN h.LEVEL_3_CD IS NOT NULL THEN '|' || h.LEVEL_3_CD END
           || CASE WHEN h.LEVEL_4_CD IS NOT NULL THEN '|' || h.LEVEL_4_CD END
                                                            AS NODE_PATH,
       CASE WHEN h.PRODUCT_ID IS NULL THEN 'N' ELSE 'Y' END AS LEAF_FLAG,
       h.PLANNER_CD,
       h.BUYER_CD,
       h.EFFECTIVE_DT,
       h.END_DT
  FROM WWI_MDM.PRODUCT_HIERARCHY h
  LEFT OUTER JOIN WWI_MDM.PRODUCT_MASTER pm
    ON pm.PRODUCT_ID = h.PRODUCT_ID
 WHERE h.EFFECTIVE_DT <= TRUNC(SYSDATE)
   AND NVL(h.END_DT, DATE '4712-12-31') >= TRUNC(SYSDATE)
/
