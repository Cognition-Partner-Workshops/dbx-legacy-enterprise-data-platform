/* =====================================================================
 * Object       : SCHEMA (user) creation for WWI_MDM, WWI_PROC, WWI_FIN,
 *                WWI_REF, WWI_AUDIT and the read-only extract account
 *                WWI_EXTRACT used by the downstream SSIS estate
 * Schema       : n/a (SYS/DBA level)
 * Deploy order : 02  - after 01_create_tablespaces.sql
 * Depends on   : oracle/ddl/01_create_tablespaces.sql
 * Called by    : DBA deployment runbook (oracle/ddl/README.md)
 *
 * No credential values appear in this repository. Each account is created
 * with a SQL*Plus substitution variable that the deploying DBA supplies from
 * the environment (ORACLE_USER / ORACLE_PASSWORD family). The extract account
 * WWI_EXTRACT is the only account the ETL estate connects with.
 * ===================================================================== */

CREATE USER WWI_MDM
    IDENTIFIED BY "&&WWI_MDM_SECRET"
    DEFAULT TABLESPACE WWI_DATA
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON WWI_DATA
    QUOTA UNLIMITED ON WWI_IDX
/

CREATE USER WWI_PROC
    IDENTIFIED BY "&&WWI_PROC_SECRET"
    DEFAULT TABLESPACE WWI_DATA
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON WWI_DATA
    QUOTA UNLIMITED ON WWI_IDX
    QUOTA UNLIMITED ON WWI_HIST_DATA
    QUOTA UNLIMITED ON WWI_HIST_IDX
/

CREATE USER WWI_FIN
    IDENTIFIED BY "&&WWI_FIN_SECRET"
    DEFAULT TABLESPACE WWI_FIN_DATA
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON WWI_FIN_DATA
    QUOTA UNLIMITED ON WWI_FIN_IDX
    QUOTA UNLIMITED ON WWI_HIST_DATA
    QUOTA UNLIMITED ON WWI_HIST_IDX
/

CREATE USER WWI_REF
    IDENTIFIED BY "&&WWI_REF_SECRET"
    DEFAULT TABLESPACE WWI_REF_DATA
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON WWI_REF_DATA
    QUOTA 512M ON WWI_HIST_DATA
/

CREATE USER WWI_AUDIT
    IDENTIFIED BY "&&WWI_AUDIT_SECRET"
    DEFAULT TABLESPACE WWI_AUDIT_DATA
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON WWI_AUDIT_DATA
/

/* Downstream extract account. Owns no objects; reads through the synonyms
   created in 06_create_synonyms.sql. Profile limits its session count so a
   runaway extract cannot starve the ERP. */
CREATE USER WWI_EXTRACT
    IDENTIFIED BY "&&WWI_EXTRACT_SECRET"
    DEFAULT TABLESPACE WWI_REF_DATA
    TEMPORARY TABLESPACE TEMP
    QUOTA 0 ON WWI_REF_DATA
    PROFILE WWI_ETL_PROFILE
/

ALTER USER WWI_MDM   ACCOUNT UNLOCK
/
ALTER USER WWI_PROC  ACCOUNT UNLOCK
/
ALTER USER WWI_FIN   ACCOUNT UNLOCK
/
ALTER USER WWI_REF   ACCOUNT UNLOCK
/
ALTER USER WWI_AUDIT ACCOUNT UNLOCK
/
ALTER USER WWI_EXTRACT ACCOUNT UNLOCK
/
