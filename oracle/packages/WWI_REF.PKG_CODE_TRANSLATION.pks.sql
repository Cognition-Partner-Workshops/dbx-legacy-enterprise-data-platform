/* ============================================================================
 * Object      : WWI_REF.PKG_CODE_TRANSLATION (package specification)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.CODE_TRANSLATION, WWI_REF.SOURCE_SYSTEM_REF, WWI_REF.STATUS_CODE_REF
 * Called by   : WWI_REF.FN_TRANSLATE_CODE, the extract views in WWI_MDM and
 *               WWI_PROC, and the SSIS Oracle extracts through those views.
 * History     : 1998 codes were three characters and system specific; every
 *               acquisition since added its own code set instead of mapping
 *               onto the existing one, so the cross-reference table is the
 *               only thing holding the estate together.
 * ========================================================================= */

CREATE OR REPLACE PACKAGE WWI_REF.PKG_CODE_TRANSLATION AS

    e_code_set_unknown EXCEPTION;
    e_no_mapping       EXCEPTION;
    e_ambiguous_mapping EXCEPTION;

    PRAGMA EXCEPTION_INIT(e_code_set_unknown,  -20411);
    PRAGMA EXCEPTION_INIT(e_no_mapping,        -20412);
    PRAGMA EXCEPTION_INIT(e_ambiguous_mapping, -20413);

    FUNCTION translate
    (
        p_code_set_cd IN WWI_REF.CODE_TRANSLATION.CODE_SET_CD%TYPE,
        p_src_code    IN WWI_REF.CODE_TRANSLATION.SOURCE_VALUE_TXT%TYPE,
        p_src_system  IN WWI_REF.CODE_TRANSLATION.SOURCE_SYS_CD%TYPE DEFAULT 'ERP',
        p_region_cd   IN VARCHAR2 DEFAULT NULL
    ) RETURN WWI_REF.CODE_TRANSLATION.TARGET_VALUE_TXT%TYPE;

    FUNCTION reverse_translate
    (
        p_code_set_cd IN WWI_REF.CODE_TRANSLATION.CODE_SET_CD%TYPE,
        p_tgt_code    IN WWI_REF.CODE_TRANSLATION.TARGET_VALUE_TXT%TYPE,
        p_src_system  IN WWI_REF.CODE_TRANSLATION.SOURCE_SYS_CD%TYPE DEFAULT 'ERP'
    ) RETURN WWI_REF.CODE_TRANSLATION.SOURCE_VALUE_TXT%TYPE;

    PROCEDURE register_mapping
    (
        p_code_set_cd IN WWI_REF.CODE_TRANSLATION.CODE_SET_CD%TYPE,
        p_src_system  IN WWI_REF.CODE_TRANSLATION.SOURCE_SYS_CD%TYPE,
        p_src_code    IN WWI_REF.CODE_TRANSLATION.SOURCE_VALUE_TXT%TYPE,
        p_tgt_code    IN WWI_REF.CODE_TRANSLATION.TARGET_VALUE_TXT%TYPE,
        p_region_cd   IN WWI_REF.CODE_TRANSLATION.REGION_CD%TYPE DEFAULT NULL
    );

    PROCEDURE report_unmapped
    (
        p_code_set_cd  IN  WWI_REF.CODE_TRANSLATION.CODE_SET_CD%TYPE,
        p_unmapped_cnt OUT PLS_INTEGER
    );

END PKG_CODE_TRANSLATION;
/
