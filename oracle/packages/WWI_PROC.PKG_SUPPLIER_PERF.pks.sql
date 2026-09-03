/* ============================================================================
 * Object      : WWI_PROC.PKG_SUPPLIER_PERF (package specification)
 * Schema      : WWI_PROC
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_PROC.SUPPLIER_SCORECARD, WWI_PROC.PO_RECEIPT_LINE,
 *               WWI_PROC.PO_RECEIPT_HDR, WWI_PROC.PURCHASE_ORDER_LINE,
 *               WWI_PROC.GOODS_RETURN_LINE
 * Called by   : the monthly scorecard job WWI_PROC.PRC_BUILD_SUPPLIER_SCORECARD
 *               and the sourcing workbench.
 * History     : 2009 created as a spreadsheet replacement; the weightings
 *               have been argued over ever since and now differ per region.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_PROC.PKG_SUPPLIER_PERF AS

    e_no_activity      EXCEPTION;
    e_period_invalid   EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_no_activity,    -20321);
    PRAGMA EXCEPTION_INIT(e_period_invalid, -20322);

    c_bulk_limit CONSTANT PLS_INTEGER := 200;

    FUNCTION on_time_pct
    (
        p_supp_id   IN WWI_PROC.SUPPLIER_SCORECARD.SUPP_ID%TYPE,
        p_from_dt   IN DATE,
        p_to_dt     IN DATE,
        p_region_cd IN VARCHAR2
    ) RETURN NUMBER;

    FUNCTION quality_pct
    (
        p_supp_id IN WWI_PROC.SUPPLIER_SCORECARD.SUPP_ID%TYPE,
        p_from_dt IN DATE,
        p_to_dt   IN DATE
    ) RETURN NUMBER;

    FUNCTION composite_score
    (
        p_region_cd   IN VARCHAR2,
        p_on_time_pct IN NUMBER,
        p_quality_pct IN NUMBER,
        p_price_var   IN NUMBER
    ) RETURN NUMBER;

    PROCEDURE build_scorecards
    (
        p_period_cd  IN  WWI_PROC.SUPPLIER_SCORECARD.PERIOD_CD%TYPE,
        p_region_cd  IN  VARCHAR2,
        p_built_cnt  OUT PLS_INTEGER
    );

END PKG_SUPPLIER_PERF;
/
