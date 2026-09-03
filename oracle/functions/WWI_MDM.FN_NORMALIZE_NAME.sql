/* ============================================================================
 * Object      : WWI_MDM.FN_NORMALIZE_NAME (function)
 * Schema      : WWI_MDM
 * Database    : WWIGERP (Oracle ERP)
 * Depends on  : WWI_REF.COUNTRY_REF, WWI_REF.CODE_TRANSLATION
 * Called by   : WWI_MDM.PKG_CUSTOMER_MASTER, WWI_MDM.PKG_SUPPLIER_MASTER,
 *               WWI_MDM.PRC_MERGE_DUPLICATE_CUSTOMERS,
 *               WWI_MDM.V_CUSTOMER_EXTRACT, WWI_MDM.V_SUPPLIER_EXTRACT
 * History     : 1998 original (NA only), 2004 EU legal forms added,
 *               2011 APAC legal forms added, 2016 diacritic folding added.
 * Notes       : Match key generator for party de-duplication. The suffix lists
 *               are region specific and have never been consolidated.
 * ========================================================================= */

CREATE OR REPLACE FUNCTION WWI_MDM.FN_NORMALIZE_NAME
(
    p_name        IN VARCHAR2,
    p_region_cd   IN VARCHAR2 DEFAULT 'NA',
    p_strip_legal IN VARCHAR2 DEFAULT 'Y'
)
RETURN VARCHAR2 DETERMINISTIC
IS
    c_na_suffix    CONSTANT VARCHAR2(300) :=
        ',INC,INCORPORATED,CORP,CORPORATION,CO,COMPANY,LLC,LLP,LP,LTD,'
        || 'HOLDINGS,ENTERPRISES,GROUP,DBA,';
    c_eu_suffix    CONSTANT VARCHAR2(300) :=
        ',GMBH,AG,KG,GMBHCOKG,SARL,SAS,SA,SPA,SRL,BV,NV,AB,AS,OY,APS,'
        || 'PLC,LTD,LIMITED,SP ZOO,DOO,';
    c_apac_suffix  CONSTANT VARCHAR2(300) :=
        ',PTE LTD,PTY LTD,SDN BHD,BHD,KK,KABUSHIKI KAISHA,YK,CO LTD,'
        || 'LIMITED,PVT LTD,PRIVATE LIMITED,';

    l_work         VARCHAR2(4000);
    l_token        VARCHAR2(200);
    l_suffix_list  VARCHAR2(300);
    l_pos          PLS_INTEGER;
BEGIN
    IF p_name IS NULL THEN
        RETURN NULL;
    END IF;

    /* 2016: fold accents so that the umlaut and the transliterated spelling of
       the same German name collide. The German expansion below is applied
       before the generic fold, on purpose. */
    l_work := UPPER(TRIM(p_name));
    l_work := REPLACE(l_work, UNISTR('\00C4'), 'AE');
    l_work := REPLACE(l_work, UNISTR('\00D6'), 'OE');
    l_work := REPLACE(l_work, UNISTR('\00DC'), 'UE');
    l_work := REPLACE(l_work, UNISTR('\1E9E'), 'SS');
    l_work := REPLACE(l_work, UNISTR('\00DF'), 'SS');
    l_work := CONVERT(l_work, 'US7ASCII');

    /* punctuation out, whitespace collapsed */
    l_work := TRANSLATE(l_work, '.,;:''"()[]{}/\&+*#!?@', '                     ');
    l_work := REGEXP_REPLACE(l_work, '[[:space:]]+', ' ');
    l_work := TRIM(l_work);

    l_work := REPLACE(l_work, ' AND ', ' ');
    l_work := REPLACE(l_work, ' THE ', ' ');

    IF NVL(p_strip_legal, 'Y') = 'Y' THEN
        l_suffix_list :=
            CASE UPPER(p_region_cd)
                WHEN 'EU'   THEN c_eu_suffix
                WHEN 'APAC' THEN c_apac_suffix
                ELSE c_na_suffix
            END;

        /* Strip up to three trailing legal-form tokens. Deliberately naive -
           it is the behaviour every downstream match key has been built on. */
        FOR i IN 1 .. 3 LOOP
            l_pos := INSTR(l_work, ' ', -1);
            EXIT WHEN l_pos = 0;
            l_token := SUBSTR(l_work, l_pos + 1);
            IF INSTR(l_suffix_list, ',' || l_token || ',') > 0 THEN
                l_work := TRIM(SUBSTR(l_work, 1, l_pos - 1));
            ELSE
                EXIT;
            END IF;
        END LOOP;

        /* two-word APAC forms are checked separately, the loop above is
           single token only */
        IF UPPER(p_region_cd) = 'APAC' THEN
            FOR i IN 1 .. 2 LOOP
                IF REGEXP_LIKE(l_work, '(PTE LTD|PTY LTD|SDN BHD|CO LTD|PVT LTD|PRIVATE LIMITED)$') THEN
                    l_work := TRIM(REGEXP_REPLACE(l_work,
                        '(PTE LTD|PTY LTD|SDN BHD|CO LTD|PVT LTD|PRIVATE LIMITED)$', ''));
                ELSE
                    EXIT;
                END IF;
            END LOOP;
        END IF;
    END IF;

    RETURN SUBSTR(l_work, 1, 200);
EXCEPTION
    WHEN VALUE_ERROR THEN
        /* never fail a load because of a pathological name */
        RETURN SUBSTR(UPPER(TRIM(p_name)), 1, 200);
END FN_NORMALIZE_NAME;
/
