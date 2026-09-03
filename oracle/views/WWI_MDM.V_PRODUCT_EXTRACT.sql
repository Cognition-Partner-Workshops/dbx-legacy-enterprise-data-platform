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
 * Notes       : PRODUCT_SUBSTITUTE and PRODUCT_UOM_CONV carry no active flag;
 *               currency of a row is its EFFECTIVE_DT/END_DT window.
 *               Reads WWI_REF; the SELECT grants live in
 *               oracle/ddl/05_grant_privileges.sql.
 * ========================================================================= */

CREATE OR REPLACE VIEW WWI_MDM.V_PRODUCT_EXTRACT AS
SELECT p.PRODUCT_ID,
       p.ITEM_NBR                                            AS PRODUCT_NUM,
       p.ITEM_DESC_SHORT                                     AS PRODUCT_NAME,
       p.ITEM_DESC                                           AS PRODUCT_DESC,
       p.PRODUCT_CATEGORY_ID                                 AS CATEGORY_ID,
       cat.CATEGORY_CD,
       cat.CATEGORY_NAME,
       parent_cat.CATEGORY_CD                                AS PARENT_CATEGORY_CD,
       parent_cat.CATEGORY_NAME                              AS PARENT_CATEGORY_NAME,
       p.PRIMARY_UOM_CD                                      AS BASE_UOM_CD,
       u.UOM_NAME                                            AS BASE_UOM_NAME,
       u.UOM_CLASS_CD,
       p.ITEM_TYPE_CD,
       p.LIFECYCLE_STATUS_CD                                 AS STATUS_CD,
       WWI_MDM.FN_PRODUCT_ACTIVE_FLAG(p.PRODUCT_ID, 'NA')    AS ACTIVE_FLAG_NA,
       WWI_MDM.FN_PRODUCT_ACTIVE_FLAG(p.PRODUCT_ID, 'EU')    AS ACTIVE_FLAG_EU,
       WWI_MDM.FN_PRODUCT_ACTIVE_FLAG(p.PRODUCT_ID, 'APAC')  AS ACTIVE_FLAG_APAC,
       WWI_REF.FN_TRANSLATE_CODE('PRODUCT_STATUS', p.LIFECYCLE_STATUS_CD) AS DW_STATUS_CD,
       p.HAZMAT_FLG                                          AS HAZMAT_FLAG,
       p.CHILLER_FLG                                         AS CHILLER_FLAG,
       p.SHELF_LIFE_DAYS,
       p.UNIT_COST_STD                                       AS STD_COST_AMT,
       p.COST_CURR_CD                                        AS COST_CURRENCY_CD,
       p.LIST_PRICE_AMT,
       p.LIST_PRICE_CURR_CD,
       p.DISCONTINUED_DT,
       conv.CASE_CONV_FACTOR,
       conv.PALLET_CONV_FACTOR,
       NVL(sub.SUBSTITUTE_COUNT, 0)                          AS SUBSTITUTE_COUNT,
       p.CREATED_DT,
       NVL(p.UPDATED_DT, p.CREATED_DT)                       AS LAST_UPD_DT
  FROM WWI_MDM.PRODUCT_MASTER p
  LEFT OUTER JOIN WWI_MDM.PRODUCT_CATEGORY cat
    ON cat.PRODUCT_CATEGORY_ID = p.PRODUCT_CATEGORY_ID
  LEFT OUTER JOIN WWI_MDM.PRODUCT_CATEGORY parent_cat
    ON parent_cat.PRODUCT_CATEGORY_ID = cat.PARENT_CATEGORY_ID
  LEFT OUTER JOIN WWI_REF.UOM_REF u
    ON u.UOM_CD = p.PRIMARY_UOM_CD
  LEFT OUTER JOIN (
        SELECT c.PRODUCT_ID,
               MAX(CASE WHEN c.FROM_UOM_CD = 'CS'  THEN c.CONV_FACTOR END) AS CASE_CONV_FACTOR,
               MAX(CASE WHEN c.FROM_UOM_CD = 'PAL' THEN c.CONV_FACTOR END) AS PALLET_CONV_FACTOR
          FROM WWI_MDM.PRODUCT_UOM_CONV c
         WHERE c.TO_UOM_CD = 'EA'
           AND c.EFFECTIVE_DT <= TRUNC(SYSDATE)
           AND NVL(c.END_DT, DATE '4712-12-31') >= TRUNC(SYSDATE)
         GROUP BY c.PRODUCT_ID
       ) conv
    ON conv.PRODUCT_ID = p.PRODUCT_ID
  LEFT OUTER JOIN (
        SELECT s.PRODUCT_ID, COUNT(*) AS SUBSTITUTE_COUNT
          FROM WWI_MDM.PRODUCT_SUBSTITUTE s
         WHERE s.EFFECTIVE_DT <= TRUNC(SYSDATE)
           AND NVL(s.END_DT, DATE '4712-12-31') >= TRUNC(SYSDATE)
         GROUP BY s.PRODUCT_ID
       ) sub
    ON sub.PRODUCT_ID = p.PRODUCT_ID
 WHERE NVL(p.DELETED_FLG, 'N') = 'N'
/
