/* =====================================================================
 * Object       : DIRECTORY objects used by the ERP's file interfaces
 * Schema       : n/a (SYS/DBA level)
 * Deploy order : 06  - after 04_create_roles.sql
 * Depends on   : oracle/ddl/04_create_roles.sql
 * Called by    : DBA deployment runbook (oracle/ddl/README.md)
 *
 * Three of these directories are for interfaces that pre-date the SSIS
 * estate and still run: the bank statement drop (WWI_BANK_IN), the legacy
 * supplier catalogue load (WWI_CATALOG_IN) and the nightly flat-file spool
 * the finance team refuses to give up (WWI_FIN_OUT). Paths come from the
 * substitution variable so no environment path is committed.
 * ===================================================================== */

DEFINE ORACLE_IFACE_DIR = '/u03/interfaces/WWIGERP'

CREATE OR REPLACE DIRECTORY WWI_BANK_IN     AS '&&ORACLE_IFACE_DIR/bank_in'
/
CREATE OR REPLACE DIRECTORY WWI_CATALOG_IN  AS '&&ORACLE_IFACE_DIR/catalog_in'
/
CREATE OR REPLACE DIRECTORY WWI_FIN_OUT     AS '&&ORACLE_IFACE_DIR/fin_out'
/
CREATE OR REPLACE DIRECTORY WWI_DATAPUMP    AS '&&ORACLE_IFACE_DIR/datapump'
/

GRANT READ ON DIRECTORY WWI_BANK_IN     TO WWI_FIN
/
GRANT READ, WRITE ON DIRECTORY WWI_CATALOG_IN TO WWI_MDM
/
GRANT READ, WRITE ON DIRECTORY WWI_FIN_OUT  TO WWI_FIN
/
GRANT READ ON DIRECTORY WWI_FIN_OUT     TO WWI_ETL_EXTRACT_ROLE
/
GRANT READ, WRITE ON DIRECTORY WWI_DATAPUMP TO WWI_MDM, WWI_PROC, WWI_FIN, WWI_REF, WWI_AUDIT
/

UNDEFINE ORACLE_IFACE_DIR
