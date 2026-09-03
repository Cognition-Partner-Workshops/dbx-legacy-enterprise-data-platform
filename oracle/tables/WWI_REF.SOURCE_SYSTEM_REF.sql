/* =====================================================================
 * Object       : TABLE WWI_REF.SOURCE_SYSTEM_REF
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 33
 * Depends on   : oracle/ddl/02_create_schemas.sql
 * Called by    : SOURCE_SYS columns across the estate, ETL control framework
 *
 * The systems that write into this ERP. The values here are what the SOURCE_SYS
 * column on every table is supposed to contain; three of the listed systems
 * were decommissioned years ago but their rows remain because history still
 * carries their code.
 * ===================================================================== */

CREATE TABLE WWI_REF.SOURCE_SYSTEM_REF
(
    SOURCE_SYS_CD           VARCHAR2(12)    NOT NULL,
    SYSTEM_NAME             VARCHAR2(80)    NOT NULL,
    SYSTEM_TYPE_CD          VARCHAR2(10)    NOT NULL,
    VENDOR_TXT              VARCHAR2(80),
    OWNING_TEAM_TXT         VARCHAR2(80),
    REGION_CD               VARCHAR2(4),
    INTERFACE_MODE_CD       VARCHAR2(8),
    INTERFACE_FREQUENCY_CD  VARCHAR2(8),
    CONNECTION_PARAM_NAME   VARCHAR2(40),
    TIMEZONE_TXT            VARCHAR2(40),
    COMMISSIONED_DT         DATE,
    DECOMMISSIONED_DT       DATE,
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    TRUSTED_SOURCE_FLG      VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    NOTES_TXT               VARCHAR2(1000),
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_SOURCE_SYSTEM_REF PRIMARY KEY (SOURCE_SYS_CD) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_SOURCE_SYS_TYPE CHECK (
        SYSTEM_TYPE_CD IN ('ERP', 'WMS', 'CRM', 'MAINFRM', 'FEED', 'MANUAL', 'FILE')),
    CONSTRAINT CK_SOURCE_SYS_FLAGS CHECK (
        ACTIVE_FLG IN ('Y', 'N') AND TRUSTED_SOURCE_FLG IN ('Y', 'N')),
    CONSTRAINT CK_SOURCE_SYS_DECOMM CHECK (
        ACTIVE_FLG = 'Y' OR DECOMMISSIONED_DT IS NOT NULL)
)
TABLESPACE WWI_REF_DATA
/

COMMENT ON COLUMN WWI_REF.SOURCE_SYSTEM_REF.CONNECTION_PARAM_NAME IS
    'Name of the deployment parameter holding the connection details. No credential values here.'
/
