/* =====================================================================
 * Object       : Cross-schema EXECUTE grants on the WWIGERP packages
 * Schema       : n/a (owner-issued object grants)
 * Deploy order : 09  - after the package specifications exist and before the
 *                package bodies and procedures that call them
 * Depends on   : oracle/packages/*.pks.sql
 * Called by    : DBA deployment runbook (oracle/ddl/README.md)
 *
 * A package body or procedure calling a package owned by another schema
 * resolves that call with direct privileges only, so a role grant leaves the
 * caller INVALID with PLS-00201. These grants cannot live in
 * 08_grant_function_execute.sql because that script runs before the package
 * specifications are created.
 *
 * One entry per (callee package, caller schema) pair backing a static
 * dependency in oracle/packages and oracle/procedures.
 * ===================================================================== */

/* WWI_AUDIT.PKG_DATA_QUALITY: reject and error logging from every module */
GRANT EXECUTE ON WWI_AUDIT.PKG_DATA_QUALITY    TO WWI_FIN
/
GRANT EXECUTE ON WWI_AUDIT.PKG_DATA_QUALITY    TO WWI_MDM
/
GRANT EXECUTE ON WWI_AUDIT.PKG_DATA_QUALITY    TO WWI_PROC
/
GRANT EXECUTE ON WWI_AUDIT.PKG_DATA_QUALITY    TO WWI_REF
/

/* WWI_AUDIT.PKG_EXTRACT_CONTROL: watermark handling in the AP extracts */
GRANT EXECUTE ON WWI_AUDIT.PKG_EXTRACT_CONTROL TO WWI_FIN
/

/* WWI_FIN.PKG_TAX: customer tax defaulting in WWI_MDM */
GRANT EXECUTE ON WWI_FIN.PKG_TAX               TO WWI_MDM
/

/* WWI_MDM master-data packages called by procurement and finance */
GRANT EXECUTE ON WWI_MDM.PKG_PRODUCT_MASTER    TO WWI_PROC
/
GRANT EXECUTE ON WWI_MDM.PKG_SUPPLIER_MASTER   TO WWI_FIN
/
GRANT EXECUTE ON WWI_MDM.PKG_SUPPLIER_MASTER   TO WWI_PROC
/

/* WWI_REF reference packages called across the estate */
GRANT EXECUTE ON WWI_REF.PKG_CODE_TRANSLATION  TO WWI_AUDIT
/
GRANT EXECUTE ON WWI_REF.PKG_CODE_TRANSLATION  TO WWI_PROC
/
GRANT EXECUTE ON WWI_REF.PKG_FX                TO WWI_AUDIT
/
GRANT EXECUTE ON WWI_REF.PKG_FX                TO WWI_FIN
/

/* WWI_AUDIT.V_EXTRACT_WATERMARK: watermark anchors in the AP aging load */
GRANT SELECT ON WWI_AUDIT.V_EXTRACT_WATERMARK  TO WWI_FIN
/
GRANT SELECT ON WWI_AUDIT.V_EXTRACT_WATERMARK  TO WWI_ETL_EXTRACT_ROLE
/
