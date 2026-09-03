/* =====================================================================
 * Object       : TABLESPACES for the WWIGERP Oracle ERP instance
 * Schema       : n/a (SYS/DBA level)
 * Deploy order : 01  - first script of the Oracle deployment
 * Depends on   : nothing
 * Called by    : DBA deployment runbook (oracle/ddl/README.md)
 *
 * The estate has accreted tablespaces since the original 1998 install: the
 * old WWI_DATA / WWI_IDX pair is still where master data lives, the finance
 * team got their own tablespaces in the 2007 AP re-platform, and the
 * partitioned procurement history landed in WWI_HIST_* in 2014. Nothing was
 * ever consolidated.
 *
 * Data file paths come from the substitution variable &&ORACLE_DATA_DIR so
 * that no environment path is hard-coded here.
 * ===================================================================== */

DEFINE ORACLE_DATA_DIR = '/u02/oradata/WWIGERP'

/* --- 1998 original install ---------------------------------------- */
CREATE TABLESPACE WWI_DATA
    DATAFILE '&&ORACLE_DATA_DIR/wwi_data_01.dbf' SIZE 512M
    AUTOEXTEND ON NEXT 128M MAXSIZE 16G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO
/

CREATE TABLESPACE WWI_IDX
    DATAFILE '&&ORACLE_DATA_DIR/wwi_idx_01.dbf' SIZE 256M
    AUTOEXTEND ON NEXT 64M MAXSIZE 8G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO
/

/* --- 2007 AP re-platform ------------------------------------------ */
CREATE TABLESPACE WWI_FIN_DATA
    DATAFILE '&&ORACLE_DATA_DIR/wwi_fin_data_01.dbf' SIZE 1024M
    AUTOEXTEND ON NEXT 256M MAXSIZE 32G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO
/

CREATE TABLESPACE WWI_FIN_IDX
    DATAFILE '&&ORACLE_DATA_DIR/wwi_fin_idx_01.dbf' SIZE 512M
    AUTOEXTEND ON NEXT 128M MAXSIZE 16G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
/

/* --- 2014 partitioned history ------------------------------------- */
CREATE TABLESPACE WWI_HIST_DATA
    DATAFILE '&&ORACLE_DATA_DIR/wwi_hist_data_01.dbf' SIZE 2048M
    AUTOEXTEND ON NEXT 512M MAXSIZE 64G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO
/

CREATE TABLESPACE WWI_HIST_IDX
    DATAFILE '&&ORACLE_DATA_DIR/wwi_hist_idx_01.dbf' SIZE 1024M
    AUTOEXTEND ON NEXT 256M MAXSIZE 32G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
/

/* --- reference and audit ------------------------------------------ */
CREATE TABLESPACE WWI_REF_DATA
    DATAFILE '&&ORACLE_DATA_DIR/wwi_ref_data_01.dbf' SIZE 128M
    AUTOEXTEND ON NEXT 32M MAXSIZE 4G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
/

CREATE TABLESPACE WWI_AUDIT_DATA
    DATAFILE '&&ORACLE_DATA_DIR/wwi_audit_data_01.dbf' SIZE 512M
    AUTOEXTEND ON NEXT 128M MAXSIZE 24G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
/

UNDEFINE ORACLE_DATA_DIR
