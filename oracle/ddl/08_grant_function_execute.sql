/* =====================================================================
 * Object       : Cross-schema EXECUTE grants on the WWIGERP scalar functions
 * Schema       : n/a (owner-issued object grants)
 * Deploy order : 08  - after oracle/functions/* exist and before the views,
 *                procedures and packages that call them
 * Depends on   : oracle/functions/*
 * Called by    : DBA deployment runbook (oracle/ddl/README.md)
 *
 * A view, procedure or package body calling a scalar function owned by
 * another schema resolves that call with direct privileges only, so a role
 * grant leaves the object INVALID with ORA-00942. These grants cannot live
 * in 05_grant_privileges.sql because that script runs before the functions
 * are created.
 *
 * One entry per (callee, caller schema) pair backing a static dependency in
 * oracle/views, oracle/procedures and oracle/packages.
 * ===================================================================== */

/* WWI_REF.FN_FISCAL_PERIOD: WWI_AUDIT.PRC_PREPARE_INVOICE_EXTRACT,
   WWI_FIN AP/GL procedures and views, WWI_PROC PO extract/scorecard */
GRANT EXECUTE ON WWI_REF.FN_FISCAL_PERIOD      TO WWI_AUDIT
/
GRANT EXECUTE ON WWI_REF.FN_FISCAL_PERIOD      TO WWI_FIN
/
GRANT EXECUTE ON WWI_REF.FN_FISCAL_PERIOD      TO WWI_PROC
/

/* WWI_REF.FN_TRANSLATE_CODE: WWI_MDM customer/product extracts,
   WWI_PROC.V_PURCHASE_ORDER_EXTRACT */
GRANT EXECUTE ON WWI_REF.FN_TRANSLATE_CODE     TO WWI_MDM
/
GRANT EXECUTE ON WWI_REF.FN_TRANSLATE_CODE     TO WWI_PROC
/

/* WWI_FIN.FN_CONVERT_AMOUNT: WWI_PROC PO views and package,
   WWI_REF.PKG_FX */
GRANT EXECUTE ON WWI_FIN.FN_CONVERT_AMOUNT     TO WWI_PROC
/
GRANT EXECUTE ON WWI_FIN.FN_CONVERT_AMOUNT     TO WWI_REF
/

/* WWI_FIN.FN_DUE_DATE: WWI_REF.V_PAYMENT_TERMS_EXTRACT */
GRANT EXECUTE ON WWI_FIN.FN_DUE_DATE           TO WWI_REF
/

/* WWI_PROC.FN_PO_OPEN_QTY: WWI_MDM.PKG_PRODUCT_MASTER */
GRANT EXECUTE ON WWI_PROC.FN_PO_OPEN_QTY       TO WWI_MDM
/
