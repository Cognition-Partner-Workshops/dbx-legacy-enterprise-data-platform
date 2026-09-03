/* =====================================================================
 * Object       : TABLE WWI_REF.STATUS_CODE_REF
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 30
 * Depends on   : oracle/ddl/03_create_schemas.sql
 * Called by    : Every status column in WWI_PROC and WWI_FIN, extract decoding
 *
 * Central decode table for the status codes scattered across the estate. It is
 * keyed by entity plus code, and the same literal code means different things
 * for different entities ('OPEN' on a PO, a receipt and a GL period are three
 * different states). Nothing enforces that a status column's value exists
 * here; the check constraints on the tables and the contents of this table
 * have drifted.
 * ===================================================================== */

CREATE TABLE WWI_REF.STATUS_CODE_REF
(
    ENTITY_CD               VARCHAR2(20)    NOT NULL,
    STATUS_CD               VARCHAR2(8)     NOT NULL,
    STATUS_NAME             VARCHAR2(80)    NOT NULL,
    STATUS_DESC             VARCHAR2(400),
    STATUS_GROUP_CD         VARCHAR2(10),
    DISPLAY_SEQ_NBR         NUMBER(4)       DEFAULT 1 NOT NULL,
    IS_TERMINAL_FLG         VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    IS_ERROR_FLG            VARCHAR2(1)     DEFAULT 'N' NOT NULL,
    ALLOWS_UPDATE_FLG       VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    NEXT_STATUS_LIST_TXT    VARCHAR2(200),
    REGION_CD               VARCHAR2(4),
    LEGACY_STATUS_CD        VARCHAR2(4),
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_STATUS_CODE_REF PRIMARY KEY (ENTITY_CD, STATUS_CD) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_STATUS_FLAGS CHECK (
        IS_TERMINAL_FLG IN ('Y', 'N') AND IS_ERROR_FLG IN ('Y', 'N')
        AND ALLOWS_UPDATE_FLG IN ('Y', 'N') AND ACTIVE_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_REF_DATA
/

CREATE INDEX WWI_REF.IX_STATUS_GROUP
    ON WWI_REF.STATUS_CODE_REF (STATUS_GROUP_CD, DISPLAY_SEQ_NBR) TABLESPACE WWI_IDX
/

COMMENT ON COLUMN WWI_REF.STATUS_CODE_REF.NEXT_STATUS_LIST_TXT IS
    'Comma-separated allowed successor codes. Parsed at run time; not enforced by the database.'
/
