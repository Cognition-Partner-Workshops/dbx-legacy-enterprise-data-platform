/* ============================================================================
 * Object      : WWI_REF.FN_TRANSLATE_CODE (function)
 * Schema      : WWI_REF
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.CODE_TRANSLATION, WWI_REF.SOURCE_SYSTEM_REF
 * Called by   : WWI_REF.PKG_CODE_TRANSLATION, WWI_MDM.PKG_CUSTOMER_MASTER,
 *               WWI_PROC.PKG_PURCHASE_ORDER, WWI_REF.V_GEOGRAPHY_EXTRACT,
 *               every EXT_ORA_* extract view that emits a downstream code
 * History     : 2002 original; 2008 region qualifier; 2015 effective dating.
 * Notes       : Falls back through region-specific row -> global row ->
 *               pass-through of the source code. Callers rely on the
 *               pass-through: an untranslated code must not become NULL.
 * ========================================================================= */

CREATE OR REPLACE FUNCTION WWI_REF.FN_TRANSLATE_CODE
(
    p_code_set_cd  IN WWI_REF.CODE_TRANSLATION.CODE_SET_CD%TYPE,
    p_src_code     IN WWI_REF.CODE_TRANSLATION.SRC_CODE%TYPE,
    p_src_system   IN WWI_REF.CODE_TRANSLATION.SRC_SYSTEM_CD%TYPE DEFAULT 'ORAERP',
    p_tgt_system   IN WWI_REF.CODE_TRANSLATION.TGT_SYSTEM_CD%TYPE DEFAULT 'WWIDW',
    p_region_cd    IN VARCHAR2 DEFAULT NULL,
    p_as_of_dt     IN DATE     DEFAULT SYSDATE
)
RETURN VARCHAR2
IS
    l_tgt_code WWI_REF.CODE_TRANSLATION.TGT_CODE%TYPE;
BEGIN
    IF p_src_code IS NULL THEN
        RETURN NULL;
    END IF;

    IF p_region_cd IS NOT NULL THEN
        BEGIN
            SELECT t.TGT_CODE
              INTO l_tgt_code
              FROM WWI_REF.CODE_TRANSLATION t
             WHERE t.CODE_SET_CD   = p_code_set_cd
               AND t.SRC_SYSTEM_CD = p_src_system
               AND t.TGT_SYSTEM_CD = p_tgt_system
               AND t.SRC_CODE      = p_src_code
               AND t.REGION_CD     = p_region_cd
               AND NVL(t.ACTIVE_FLAG, 'Y') = 'Y'
               AND TRUNC(p_as_of_dt) BETWEEN NVL(t.EFF_FROM_DT, DATE '1900-01-01')
                                         AND NVL(t.EFF_TO_DT, DATE '4712-12-31')
               AND ROWNUM = 1;
            RETURN l_tgt_code;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                NULL;
        END;
    END IF;

    BEGIN
        SELECT t.TGT_CODE
          INTO l_tgt_code
          FROM WWI_REF.CODE_TRANSLATION t
         WHERE t.CODE_SET_CD   = p_code_set_cd
           AND t.SRC_SYSTEM_CD = p_src_system
           AND t.TGT_SYSTEM_CD = p_tgt_system
           AND t.SRC_CODE      = p_src_code
           AND t.REGION_CD IS NULL
           AND NVL(t.ACTIVE_FLAG, 'Y') = 'Y'
           AND TRUNC(p_as_of_dt) BETWEEN NVL(t.EFF_FROM_DT, DATE '1900-01-01')
                                     AND NVL(t.EFF_TO_DT, DATE '4712-12-31')
           AND ROWNUM = 1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_tgt_code := p_src_code;
    END;

    RETURN l_tgt_code;
END FN_TRANSLATE_CODE;
/
