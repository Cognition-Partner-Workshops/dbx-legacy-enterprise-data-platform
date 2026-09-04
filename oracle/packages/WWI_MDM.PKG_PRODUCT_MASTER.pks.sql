/* ============================================================================
 * Object      : WWI_MDM.PKG_PRODUCT_MASTER (package specification)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_MDM.PRODUCT_MASTER, WWI_MDM.PRODUCT_CATEGORY,
 *               WWI_MDM.PRODUCT_HIERARCHY, WWI_MDM.PRODUCT_UOM_CONV,
 *               WWI_MDM.PRODUCT_SUBSTITUTE
 * Called by   : the item maintenance form, the PIM inbound interface and
 *               WWI_PROC.PKG_PURCHASE_ORDER (UOM conversion on PO lines).
 * History     : 2001 created; 2005 UOM conversion moved in from the forms
 *               layer; 2014 hierarchy rebuild added for the DW.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_MDM.PKG_PRODUCT_MASTER AS

    e_product_not_found EXCEPTION;
    e_uom_not_convertible EXCEPTION;
    e_hierarchy_cycle   EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_product_not_found,   -20221);
    PRAGMA EXCEPTION_INIT(e_uom_not_convertible, -20222);
    PRAGMA EXCEPTION_INIT(e_hierarchy_cycle,     -20223);

    FUNCTION convert_uom
    (
        p_product_id IN WWI_MDM.PRODUCT_MASTER.PRODUCT_ID%TYPE,
        p_qty        IN NUMBER,
        p_from_uom   IN WWI_MDM.PRODUCT_UOM_CONV.FROM_UOM_CD%TYPE,
        p_to_uom     IN WWI_MDM.PRODUCT_UOM_CONV.TO_UOM_CD%TYPE
    ) RETURN NUMBER;

    FUNCTION preferred_substitute
    (
        p_product_id IN WWI_MDM.PRODUCT_MASTER.PRODUCT_ID%TYPE,
        p_region_cd  IN VARCHAR2 DEFAULT 'NA'
    ) RETURN WWI_MDM.PRODUCT_MASTER.PRODUCT_ID%TYPE;

    PROCEDURE discontinue_product
    (
        p_product_id     IN WWI_MDM.PRODUCT_MASTER.PRODUCT_ID%TYPE,
        p_discontinue_dt IN DATE DEFAULT TRUNC(SYSDATE),
        p_reason_cd      IN VARCHAR2 DEFAULT NULL
    );

    PROCEDURE rebuild_hierarchy
    (
        p_hier_id   IN  WWI_MDM.PRODUCT_HIERARCHY.PRODUCT_HIER_ID%TYPE,
        p_node_cnt  OUT PLS_INTEGER
    );

    PROCEDURE recost_products
    (
        p_category_id IN  WWI_MDM.PRODUCT_CATEGORY.PRODUCT_CATEGORY_ID%TYPE,
        p_factor      IN  NUMBER,
        p_updated_cnt OUT PLS_INTEGER
    );

END PKG_PRODUCT_MASTER;
/
