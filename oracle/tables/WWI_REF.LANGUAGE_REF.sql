/* =====================================================================
 * Object       : TABLE WWI_REF.LANGUAGE_REF
 * Schema       : WWI_REF (Oracle ERP - WWIGERP)
 * Deploy order : 27
 * Depends on   : oracle/ddl/02_create_schemas.sql
 * Called by    : Customer communication preferences, document generation
 *
 * Languages used for supplier and customer correspondence. The code is the
 * five-character locale form, but three legacy interfaces send the two-letter
 * form, which is why LEGACY_LANG_CD is not unique.
 * ===================================================================== */

CREATE TABLE WWI_REF.LANGUAGE_REF
(
    LANGUAGE_CD             VARCHAR2(5)     NOT NULL,
    LANGUAGE_NAME           VARCHAR2(60)    NOT NULL,
    NATIVE_NAME             VARCHAR2(80),
    LEGACY_LANG_CD          VARCHAR2(2),
    ISO_639_1_CD            VARCHAR2(2),
    CHARACTER_SET_CD        VARCHAR2(20),
    WRITING_DIRECTION_CD    VARCHAR2(3)     DEFAULT 'LTR' NOT NULL,
    DEFAULT_REGION_CD       VARCHAR2(4),
    DOCUMENT_TEMPLATE_CD    VARCHAR2(10),
    TRANSLATION_STATUS_CD   VARCHAR2(6)     DEFAULT 'FULL' NOT NULL,
    ACTIVE_FLG              VARCHAR2(1)     DEFAULT 'Y' NOT NULL,
    SOURCE_SYS              VARCHAR2(12)    DEFAULT 'ORA_ERP' NOT NULL,
    CREATED_BY              VARCHAR2(30)    DEFAULT USER NOT NULL,
    CREATED_DT              DATE            DEFAULT SYSDATE NOT NULL,
    UPDATED_BY              VARCHAR2(30),
    UPDATED_DT              DATE,
    CONSTRAINT PK_LANGUAGE_REF PRIMARY KEY (LANGUAGE_CD) USING INDEX TABLESPACE WWI_IDX,
    CONSTRAINT CK_LANGUAGE_DIRECTION CHECK (WRITING_DIRECTION_CD IN ('LTR', 'RTL')),
    CONSTRAINT CK_LANGUAGE_TRANSLATION CHECK (
        TRANSLATION_STATUS_CD IN ('FULL', 'PART', 'NONE')),
    CONSTRAINT CK_LANGUAGE_ACTIVE CHECK (ACTIVE_FLG IN ('Y', 'N'))
)
TABLESPACE WWI_REF_DATA
/
