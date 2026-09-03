/* ============================================================================
 * Object      : WWI_MDM.V_PRODUCT_EXTRACT (view)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.PRODUCT_MASTER, WWI_MDM.PRODUCT_CATEGORY,
 *               WWI_MDM.PRODUCT_UOM_CONV, WWI_MDM.PRODUCT_SUBSTITUTE,
 *               WWI_REF.UOM_REF, WWI_MDM.FN_PRODUCT_ACTIVE_FLAG,
 *               WWI_REF.FN_TRANSLATE_CODE
 * Called by   : SSIS EXT_ORA_ProductMaster
 * History     : 2003 original; 2009 category rollup denormalised; 2013 region
 *               parameter threaded into the active flag.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_MDM.V_PRODUCT_EXTRACT AS
SELECT p.PRODUCT_ID,
       p.PRODUCT_NUM,
       p.PRODUCT_NAME,
       p.PRODUCT_DESC,
       p.CATEGORY_ID,
       cat.CATEGORY_CD,
       cat.CATEGORY_NAME,
       parent_cat.CATEGORY_CD                                AS PARENT_CATEGORY_CD,
       parent_cat.CATEGORY_NAME                              AS PARENT_CATEGORY_NAME,
       p.BASE_UOM_CD,
       u.UOM_NAME                                            AS BASE_UOM_NAME,
       u.UOM_CLASS_CD,
       p.STATUS_CD,
       WWI_MDM.FN_PRODUCT_ACTIVE_FLAG(p.PRODUCT_ID, 'NA')    AS ACTIVE_FLAG_NA,
       WWI_MDM.FN_PRODUCT_ACTIVE_FLAG(p.PRODUCT_ID, 'EU')    AS ACTIVE_FLAG_EU,
       WWI_MDM.FN_PRODUCT_ACTIVE_FLAG(p.PRODUCT_ID, 'APAC')  AS ACTIVE_FLAG_APAC,
       WWI_REF.FN_TRANSLATE_CODE('PRODUCT_STATUS', p.STATUS_CD) AS DW_STATUS_CD,
       p.HAZMAT_FLAG,
       p.CHILLER_FLAG,
       p.LEAD_TIME_DAYS,
       p.STD_COST_AMT,
       p.COST_CURRENCY_CD,
       p.DISCONTINUED_DT,
       conv.CASE_CONV_FACTOR,
       conv.PALLET_CONV_FACTOR,
       sub.SUBSTITUTE_COUNT,
       p.CREATED_DT,
       p.LAST_UPD_DT
  FROM WWI_MDM.PRODUCT_MASTER p
  LEFT OUTER JOIN WWI_MDM.PRODUCT_CATEGORY cat
    ON cat.CATEGORY_ID = p.CATEGORY_ID
  LEFT OUTER JOIN WWI_MDM.PRODUCT_CATEGORY parent_cat
    ON parent_cat.CATEGORY_ID = cat.PARENT_CATEGORY_ID
  LEFT OUTER JOIN WWI_REF.UOM_REF u
    ON u.UOM_CD = p.BASE_UOM_CD
  LEFT OUTER JOIN (
        SELECT c.PRODUCT_ID,
               MAX(CASE WHEN c.FROM_UOM_CD = 'CS'  THEN c.CONV_FACTOR_NUM END) AS CASE_CONV_FACTOR,
               MAX(CASE WHEN c.FROM_UOM_CD = 'PAL' THEN c.CONV_FACTOR_NUM END) AS PALLET_CONV_FACTOR
          FROM WWI_MDM.PRODUCT_UOM_CONV c
         WHERE c.TO_UOM_CD = 'EA'
           AND c.EFF_FROM_DT <= TRUNC(SYSDATE)
         GROUP BY c.PRODUCT_ID
       ) conv
    ON conv.PRODUCT_ID = p.PRODUCT_ID
  LEFT OUTER JOIN (
        SELECT s.PRODUCT_ID, COUNT(*) AS SUBSTITUTE_COUNT
          FROM WWI_MDM.PRODUCT_SUBSTITUTE s
         WHERE NVL(s.ACTIVE_FLAG, 'N') = 'Y'
         GROUP BY s.PRODUCT_ID
       ) sub
    ON sub.PRODUCT_ID = p.PRODUCT_ID
/
