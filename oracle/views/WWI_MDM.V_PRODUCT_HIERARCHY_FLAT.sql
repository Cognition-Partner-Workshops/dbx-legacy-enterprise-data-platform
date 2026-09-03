/* ============================================================================
 * Object      : WWI_MDM.V_PRODUCT_HIERARCHY_FLAT (view)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.PRODUCT_HIERARCHY, WWI_MDM.PRODUCT_MASTER
 * Called by   : SSIS EXT_ORA_ProductHierarchy (full refresh)
 * History     : 2006 original CONNECT BY flattener; 2011 widened from three to
 *               five levels by adding two more SUBSTR/REGEXP pairs.
 * Warning     : Expensive. The hierarchy is walked twice - once to build the
 *               path and once to count descendants - and the path is then
 *               shredded with REGEXP_SUBSTR. A rewrite has been proposed
 *               every year since 2013.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_MDM.V_PRODUCT_HIERARCHY_FLAT AS
WITH walked AS (
    SELECT h.HIER_ID,
           h.NODE_ID,
           h.NODE_CD,
           h.NODE_NAME,
           h.PARENT_NODE_ID,
           h.LEVEL_NUM,
           h.PRODUCT_ID,
           LEVEL                                              AS DEPTH_NUM,
           SYS_CONNECT_BY_PATH(h.NODE_CD, '|')                AS NODE_PATH,
           CONNECT_BY_ISLEAF                                  AS LEAF_FLAG,
           CONNECT_BY_ROOT h.NODE_CD                          AS ROOT_NODE_CD
      FROM WWI_MDM.PRODUCT_HIERARCHY h
     START WITH h.PARENT_NODE_ID IS NULL
   CONNECT BY PRIOR h.NODE_ID = h.PARENT_NODE_ID
)
SELECT w.HIER_ID,
       w.NODE_ID,
       w.NODE_CD,
       w.NODE_NAME,
       w.PARENT_NODE_ID,
       w.DEPTH_NUM,
       w.ROOT_NODE_CD,
       REGEXP_SUBSTR(w.NODE_PATH, '[^|]+', 1, 1)   AS LEVEL1_CD,
       REGEXP_SUBSTR(w.NODE_PATH, '[^|]+', 1, 2)   AS LEVEL2_CD,
       REGEXP_SUBSTR(w.NODE_PATH, '[^|]+', 1, 3)   AS LEVEL3_CD,
       REGEXP_SUBSTR(w.NODE_PATH, '[^|]+', 1, 4)   AS LEVEL4_CD,
       REGEXP_SUBSTR(w.NODE_PATH, '[^|]+', 1, 5)   AS LEVEL5_CD,
       w.NODE_PATH,
       w.LEAF_FLAG,
       w.PRODUCT_ID,
       pm.PRODUCT_NUM,
       pm.PRODUCT_NAME,
       (SELECT COUNT(*)
          FROM WWI_MDM.PRODUCT_HIERARCHY d
         START WITH d.PARENT_NODE_ID = w.NODE_ID
       CONNECT BY PRIOR d.NODE_ID = d.PARENT_NODE_ID)  AS DESCENDANT_COUNT
  FROM walked w
  LEFT OUTER JOIN WWI_MDM.PRODUCT_MASTER pm
    ON pm.PRODUCT_ID = w.PRODUCT_ID
/
