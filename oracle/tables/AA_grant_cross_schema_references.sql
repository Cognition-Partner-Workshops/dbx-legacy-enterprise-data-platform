/* =====================================================================
 * Object       : Cross-schema REFERENCES grants on the WWI_MDM masters
 * Schema       : WWI_MDM (grantor), WWI_PROC and WWI_FIN (grantees)
 * Deploy order : 39 - after every WWI_MDM table (20-38) and before the
 *                WWI_PROC (40-54) and WWI_FIN (60-75) tables
 * Depends on   : oracle/tables/WWI_MDM.SUPP_MASTER.sql,
 *                oracle/tables/WWI_MDM.PRODUCT_MASTER.sql
 * Called by    : deployment only
 *
 * WWI_PROC and WWI_FIN carry foreign keys onto the two MDM master tables.
 * Oracle checks the REFERENCES privilege against the owner of the child
 * table, not against the account running the deployment, so without these
 * grants those foreign keys fail with ORA-00942 even for a DBA. The grants
 * have to be object level and therefore cannot live in oracle/ddl, which
 * runs before any table exists.
 * ===================================================================== */

GRANT REFERENCES ON WWI_MDM.SUPP_MASTER    TO WWI_PROC
/
GRANT REFERENCES ON WWI_MDM.PRODUCT_MASTER TO WWI_PROC
/
GRANT REFERENCES ON WWI_MDM.SUPP_MASTER    TO WWI_FIN
/
