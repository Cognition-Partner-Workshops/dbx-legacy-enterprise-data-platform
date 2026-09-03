/* ============================================================================
 * Object      : WWI_REF.PKG_CODE_TRANSLATION (package body)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : spec WWI_REF.PKG_CODE_TRANSLATION, WWI_REF.CODE_TRANSLATION,
 *               WWI_REF.STATUS_CODE_REF, WWI_AUDIT.PKG_DATA_QUALITY
 * Called by   : see specification header
 * ========================================================================= */

CREATE OR REPLACE PACKAGE BODY WWI_REF.PKG_CODE_TRANSLATION AS

    /* small session cache: the extract views call translate() per row and the
       cross-reference table has never been indexed on REGION_CD             */
    TYPE t_cache IS TABLE OF WWI_REF.CODE_TRANSLATION.TGT_CODE%TYPE INDEX BY VARCHAR2(200);
    g_cache t_cache;

    FUNCTION cache_key
    (
        p_code_set_cd IN VARCHAR2,
        p_src_system  IN VARCHAR2,
        p_src_code    IN VARCHAR2,
        p_region_cd   IN VARCHAR2
    ) RETURN VARCHAR2
    IS
    BEGIN
        RETURN p_code_set_cd || '|' || p_src_system || '|' || p_src_code
               || '|' || NVL(p_region_cd, '*');
    END cache_key;

    FUNCTION translate
    (
        p_code_set_cd IN WWI_REF.CODE_TRANSLATION.CODE_SET_CD%TYPE,
        p_src_code    IN WWI_REF.CODE_TRANSLATION.SRC_CODE%TYPE,
        p_src_system  IN WWI_REF.CODE_TRANSLATION.SRC_SYSTEM_CD%TYPE DEFAULT 'ERP',
        p_region_cd   IN VARCHAR2 DEFAULT NULL
    ) RETURN WWI_REF.CODE_TRANSLATION.TGT_CODE%TYPE
    IS
        l_key      VARCHAR2(200);
        l_tgt      WWI_REF.CODE_TRANSLATION.TGT_CODE%TYPE;
        l_hit_cnt  PLS_INTEGER;
    BEGIN
        l_key := cache_key(p_code_set_cd, p_src_system, p_src_code, p_region_cd);

        IF g_cache.EXISTS(l_key) THEN
            RETURN g_cache(l_key);
        END IF;

        SELECT COUNT(*)
          INTO l_hit_cnt
          FROM WWI_REF.CODE_TRANSLATION x
         WHERE x.CODE_SET_CD   = p_code_set_cd
           AND x.SRC_SYSTEM_CD = p_src_system
           AND x.SRC_CODE      = p_src_code
           AND NVL(x.REGION_CD, NVL(p_region_cd, '*')) = NVL(p_region_cd, '*')
           AND NVL(x.ACTIVE_FLAG, 'Y') = 'Y';

        IF l_hit_cnt = 0 THEN
            /* legacy behaviour: an unmapped code is passed through unchanged
               rather than failing the extract                              */
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_CODE_TRANSLATION.translate',
                                                 p_code_set_cd || ':' || p_src_code,
                                                 'no mapping, code passed through');
            g_cache(l_key) := p_src_code;
            RETURN p_src_code;
        END IF;

        SELECT TGT_CODE
          INTO l_tgt
          FROM (SELECT x.TGT_CODE
                  FROM WWI_REF.CODE_TRANSLATION x
                 WHERE x.CODE_SET_CD   = p_code_set_cd
                   AND x.SRC_SYSTEM_CD = p_src_system
                   AND x.SRC_CODE      = p_src_code
                   AND NVL(x.REGION_CD, NVL(p_region_cd, '*')) = NVL(p_region_cd, '*')
                   AND NVL(x.ACTIVE_FLAG, 'Y') = 'Y'
                 /* region specific rows win over the generic row */
                 ORDER BY NVL2(x.REGION_CD, 0, 1), x.EFF_FROM_DT DESC)
         WHERE ROWNUM = 1;

        IF l_hit_cnt > 1 THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_CODE_TRANSLATION.translate',
                                                 p_code_set_cd || ':' || p_src_code,
                                                 l_hit_cnt || ' active mappings, '
                                                 || 'first by precedence used');
        END IF;

        g_cache(l_key) := l_tgt;
        RETURN l_tgt;
    END translate;

    FUNCTION reverse_translate
    (
        p_code_set_cd IN WWI_REF.CODE_TRANSLATION.CODE_SET_CD%TYPE,
        p_tgt_code    IN WWI_REF.CODE_TRANSLATION.TGT_CODE%TYPE,
        p_src_system  IN WWI_REF.CODE_TRANSLATION.SRC_SYSTEM_CD%TYPE DEFAULT 'ERP'
    ) RETURN WWI_REF.CODE_TRANSLATION.SRC_CODE%TYPE
    IS
        l_src WWI_REF.CODE_TRANSLATION.SRC_CODE%TYPE;
    BEGIN
        SELECT MIN(x.SRC_CODE)
          INTO l_src
          FROM WWI_REF.CODE_TRANSLATION x
         WHERE x.CODE_SET_CD   = p_code_set_cd
           AND x.SRC_SYSTEM_CD = p_src_system
           AND x.TGT_CODE      = p_tgt_code
           AND NVL(x.ACTIVE_FLAG, 'Y') = 'Y';

        IF l_src IS NULL THEN
            RAISE_APPLICATION_ERROR(-20412,
                'PKG_CODE_TRANSLATION.reverse_translate: no source code for '
                || p_code_set_cd || ':' || p_tgt_code);
        END IF;

        RETURN l_src;
    END reverse_translate;

    PROCEDURE register_mapping
    (
        p_code_set_cd IN WWI_REF.CODE_TRANSLATION.CODE_SET_CD%TYPE,
        p_src_system  IN WWI_REF.CODE_TRANSLATION.SRC_SYSTEM_CD%TYPE,
        p_src_code    IN WWI_REF.CODE_TRANSLATION.SRC_CODE%TYPE,
        p_tgt_code    IN WWI_REF.CODE_TRANSLATION.TGT_CODE%TYPE,
        p_region_cd   IN WWI_REF.CODE_TRANSLATION.REGION_CD%TYPE DEFAULT NULL
    )
    IS
        l_set_cnt PLS_INTEGER;
    BEGIN
        SELECT COUNT(*)
          INTO l_set_cnt
          FROM WWI_REF.STATUS_CODE_REF
         WHERE CODE_SET_CD = p_code_set_cd;

        IF l_set_cnt = 0 THEN
            RAISE_APPLICATION_ERROR(-20411,
                'PKG_CODE_TRANSLATION.register_mapping: unknown code set '
                || p_code_set_cd);
        END IF;

        UPDATE WWI_REF.CODE_TRANSLATION
           SET ACTIVE_FLAG = 'N',
               EFF_TO_DT   = TRUNC(SYSDATE) - 1,
               LAST_UPD_DT = SYSDATE
         WHERE CODE_SET_CD   = p_code_set_cd
           AND SRC_SYSTEM_CD = p_src_system
           AND SRC_CODE      = p_src_code
           AND NVL(REGION_CD, '*') = NVL(p_region_cd, '*')
           AND NVL(ACTIVE_FLAG, 'Y') = 'Y';

        INSERT INTO WWI_REF.CODE_TRANSLATION
            (CODE_TRANSLATION_ID, CODE_SET_CD, SRC_SYSTEM_CD, SRC_CODE, TGT_CODE,
             REGION_CD, ACTIVE_FLAG, EFF_FROM_DT, EFF_TO_DT, CREATED_DT, LAST_UPD_DT)
        VALUES
            (WWI_REF.SEQ_CODE_TRANSLATION.NEXTVAL, p_code_set_cd, p_src_system, p_src_code,
             p_tgt_code, p_region_cd, 'Y', TRUNC(SYSDATE), NULL, SYSDATE, SYSDATE);

        /* the cache is per session, so drop it whenever mappings change */
        g_cache.DELETE;
    END register_mapping;

    PROCEDURE report_unmapped
    (
        p_code_set_cd  IN  WWI_REF.CODE_TRANSLATION.CODE_SET_CD%TYPE,
        p_unmapped_cnt OUT PLS_INTEGER
    )
    IS
    BEGIN
        SELECT COUNT(DISTINCT s.STATUS_CD)
          INTO p_unmapped_cnt
          FROM WWI_REF.STATUS_CODE_REF s
         WHERE s.CODE_SET_CD = p_code_set_cd
           AND NOT EXISTS (SELECT 1
                             FROM WWI_REF.CODE_TRANSLATION x
                            WHERE x.CODE_SET_CD = s.CODE_SET_CD
                              AND x.SRC_CODE    = s.STATUS_CD
                              AND NVL(x.ACTIVE_FLAG, 'Y') = 'Y');

        IF p_unmapped_cnt > 0 THEN
            WWI_AUDIT.PKG_DATA_QUALITY.log_error('PKG_CODE_TRANSLATION.report_unmapped',
                                                 p_code_set_cd,
                                                 p_unmapped_cnt
                                                 || ' status code(s) without a mapping');
        END IF;
    END report_unmapped;

END PKG_CODE_TRANSLATION;
/
