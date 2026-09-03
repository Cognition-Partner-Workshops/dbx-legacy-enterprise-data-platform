/* =====================================================================
 * Object       : PROFILE WWI_ETL_PROFILE, PROFILE WWI_APP_PROFILE
 * Schema       : n/a (SYS/DBA level)
 * Deploy order : 03  - referenced by 02_create_schemas.sql, so on a clean
 *                build this script is run first; it is numbered 03 because
 *                the 1998 runbook listed it here and the order was never
 *                corrected.
 * Depends on   : nothing
 * Called by    : DBA deployment runbook (oracle/ddl/README.md)
 *
 * The nightly extract window is 22:00-05:00 local. WWI_ETL_PROFILE caps the
 * damage a stuck extract session can do to the ERP; WWI_APP_PROFILE is the
 * profile the ERP application accounts have used since the 2007 upgrade.
 * ===================================================================== */

CREATE PROFILE WWI_ETL_PROFILE LIMIT
    SESSIONS_PER_USER          12
    CPU_PER_SESSION            UNLIMITED
    CPU_PER_CALL               600000          /* 100 minutes, hundredths of a second */
    CONNECT_TIME               480             /* minutes */
    IDLE_TIME                  30
    LOGICAL_READS_PER_SESSION  UNLIMITED
    PRIVATE_SGA                UNLIMITED
    FAILED_LOGIN_ATTEMPTS      5
    PASSWORD_LIFE_TIME         365
    PASSWORD_REUSE_TIME        1800
    PASSWORD_GRACE_TIME        14
/

CREATE PROFILE WWI_APP_PROFILE LIMIT
    SESSIONS_PER_USER          200
    CPU_PER_CALL               UNLIMITED
    CONNECT_TIME               UNLIMITED
    IDLE_TIME                  120
    FAILED_LOGIN_ATTEMPTS      10
    PASSWORD_LIFE_TIME         UNLIMITED       /* set in 2003, never revisited */
    PASSWORD_GRACE_TIME        UNLIMITED
/
