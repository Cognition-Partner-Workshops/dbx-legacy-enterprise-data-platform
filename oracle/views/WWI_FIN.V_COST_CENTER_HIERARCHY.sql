/* ============================================================================
 * Object      : WWI_FIN.V_COST_CENTER_HIERARCHY (view)
 * Schema      : WWI_FIN
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_FIN.COST_CENTER, WWI_FIN.COST_ALLOCATION_RULE
 * Called by   : SSIS EXT_ORA_CostCenter, WWI_FIN.PRC_RUN_COST_ALLOCATION
 * Notes       : The rollup path is built with CONNECT BY. Cost centres that
 *               point at a parent in another region are left in place - the
 *               1990s org structure allowed it and nobody has cleaned it up.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_FIN.V_COST_CENTER_HIERARCHY AS
SELECT cc.COST_CENTER_ID,
       cc.COST_CENTER_CD,
       cc.COST_CENTER_NAME,
       cc.PARENT_COST_CENTER_ID,
       parent.COST_CENTER_CD                              AS PARENT_COST_CENTER_CD,
       cc.REGION_CD,
       parent.REGION_CD                                   AS PARENT_REGION_CD,
       CASE
           WHEN cc.PARENT_COST_CENTER_ID IS NULL              THEN 'ROOT'
           WHEN parent.REGION_CD <> cc.REGION_CD              THEN 'CROSS_REGION'
           ELSE 'NORMAL'
       END                                                AS ROLLUP_ANOMALY_CD,
       cc.OWNER_EMP_ID,
       cc.ACTIVE_FLAG,
       cc.EFF_FROM_DT,
       cc.EFF_TO_DT,
       lvl.DEPTH_NUM,
       lvl.ROLLUP_PATH,
       lvl.ROOT_COST_CENTER_CD,
       NVL(rules.ALLOC_RULE_COUNT, 0)                     AS ALLOC_RULE_COUNT
  FROM WWI_FIN.COST_CENTER cc
  LEFT OUTER JOIN WWI_FIN.COST_CENTER parent
    ON parent.COST_CENTER_ID = cc.PARENT_COST_CENTER_ID
  LEFT OUTER JOIN (
        SELECT c.COST_CENTER_ID,
               LEVEL                                          AS DEPTH_NUM,
               SYS_CONNECT_BY_PATH(c.COST_CENTER_CD, '/')     AS ROLLUP_PATH,
               CONNECT_BY_ROOT c.COST_CENTER_CD               AS ROOT_COST_CENTER_CD
          FROM WWI_FIN.COST_CENTER c
         START WITH c.PARENT_COST_CENTER_ID IS NULL
       CONNECT BY NOCYCLE PRIOR c.COST_CENTER_ID = c.PARENT_COST_CENTER_ID
       ) lvl
    ON lvl.COST_CENTER_ID = cc.COST_CENTER_ID
  LEFT OUTER JOIN (
        SELECT r.SRC_COST_CENTER_ID, COUNT(*) AS ALLOC_RULE_COUNT
          FROM WWI_FIN.COST_ALLOCATION_RULE r
         WHERE NVL(r.ACTIVE_FLAG, 'N') = 'Y'
         GROUP BY r.SRC_COST_CENTER_ID
       ) rules
    ON rules.SRC_COST_CENTER_ID = cc.COST_CENTER_ID
/
