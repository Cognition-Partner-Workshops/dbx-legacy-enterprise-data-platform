/* =====================================================================
 * Object       : ROLES WWI_ERP_OWNER_ROLE, WWI_ERP_APP_ROLE,
 *                WWI_ERP_READONLY_ROLE, WWI_ETL_EXTRACT_ROLE,
 *                WWI_FIN_SENSITIVE_ROLE
 * Schema       : n/a (SYS/DBA level)
 * Deploy order : 04  - after 02_create_schemas.sql
 * Depends on   : oracle/ddl/02_create_schemas.sql
 * Called by    : DBA deployment runbook; grants applied in
 *                oracle/ddl/05_grant_privileges.sql
 *
 * Role granularity is historical. WWI_ERP_READONLY_ROLE predates the ETL
 * estate and is still granted to a handful of finance analysts directly;
 * WWI_ETL_EXTRACT_ROLE was added in 2011 when the SSIS extracts replaced the
 * old nightly flat-file drop and is deliberately narrower.
 * ===================================================================== */

CREATE ROLE WWI_ERP_OWNER_ROLE
/

CREATE ROLE WWI_ERP_APP_ROLE
/

CREATE ROLE WWI_ERP_READONLY_ROLE
/

CREATE ROLE WWI_ETL_EXTRACT_ROLE
/

/* Bank details and withholding rules sit behind this role. Granted to the
   extract account only for the masked supplier view. */
CREATE ROLE WWI_FIN_SENSITIVE_ROLE
/

GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE,
      CREATE PROCEDURE, CREATE TRIGGER, CREATE SYNONYM, CREATE TYPE
    TO WWI_ERP_OWNER_ROLE
/

GRANT CREATE SESSION TO WWI_ERP_APP_ROLE
/

GRANT CREATE SESSION TO WWI_ERP_READONLY_ROLE
/

GRANT CREATE SESSION TO WWI_ETL_EXTRACT_ROLE
/

GRANT WWI_ERP_OWNER_ROLE TO WWI_MDM, WWI_PROC, WWI_FIN, WWI_REF, WWI_AUDIT
/

GRANT WWI_ETL_EXTRACT_ROLE TO WWI_EXTRACT
/
