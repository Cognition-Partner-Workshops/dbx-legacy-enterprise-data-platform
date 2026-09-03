/* =====================================================================
 * Object       : Seed data - WWI_REF.CODE_TRANSLATION (operational parameters)
 * Schema       : WWI_REF / WWI_AUDIT / WWI_FIN (Oracle ERP - WWIGERP)
 * Deploy order : 202
 * Depends on   : oracle/seed/02_code_translation_external_codes.sql
 * Called by    : run once per environment, after the reference content
 *
 * Operational parameters that live in the code translation table because
 * creating a configuration table required a change board and inserting a row
 * did not.
 *
 * The receipt over-delivery tolerance, the AP three-way match price and
 * quantity tolerances, the aging bucket boundaries and several regional
 * switches are all here as text in TARGET_VALUE_TXT, parsed by the PL/SQL that
 * reads them. Two of them differ per region, which is the only reason the
 * REGION_CD column on this table is populated at all.
 *
 * CODE_SET_CD 'PARAM' is the marker that a row is configuration rather than a
 * code mapping. Nothing enforces that distinction.
 * ===================================================================== */

SET DEFINE OFF

INSERT ALL
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2001, 'PARAM', 'ORA_ERP', 'RECEIPT_OVER_TOLERANCE_PCT', '5',
            'NA', 'PO_RECEIPT', 'NUMBER', 'Over-delivery accepted without a change order.',
            TO_DATE('2004-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2002, 'PARAM', 'ORA_ERP', 'RECEIPT_OVER_TOLERANCE_PCT', '2',
            'EU', 'PO_RECEIPT', 'NUMBER', 'EU buyers hold suppliers to a tighter tolerance.',
            TO_DATE('2011-03-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2003, 'PARAM', 'ORA_ERP', 'RECEIPT_OVER_TOLERANCE_PCT', '10',
            'APAC', 'PO_RECEIPT', 'NUMBER', 'APAC allows a wider tolerance for sea freight lots.',
            TO_DATE('2009-04-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2004, 'PARAM', 'ORA_ERP', 'AP_MATCH_PRICE_TOLERANCE_PCT', '2',
            'ALL', 'AP_INVOICE', 'NUMBER', 'Unit price variance allowed by the three-way match.',
            TO_DATE('2004-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2005, 'PARAM', 'ORA_ERP', 'AP_MATCH_QTY_TOLERANCE_PCT', '1',
            'ALL', 'AP_INVOICE', 'NUMBER', 'Quantity variance allowed by the three-way match.',
            TO_DATE('2004-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2006, 'PARAM', 'ORA_ERP', 'AP_MATCH_AMT_TOLERANCE_ABS', '25',
            'ALL', 'AP_INVOICE', 'NUMBER', 'Absolute variance floor, always read as USD regardless of currency.',
            TO_DATE('2006-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2007, 'PARAM', 'ORA_ERP', 'AP_AGING_BUCKETS', '0,30,60,90,120',
            'NA', 'AP_AGING', 'TEXT', 'Bucket boundaries in days past due.',
            TO_DATE('2005-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2008, 'PARAM', 'ORA_ERP', 'AP_AGING_BUCKETS', '0,30,60,90',
            'EU', 'AP_AGING', 'TEXT', 'EU reports four buckets; the fifth column is left null.',
            TO_DATE('2005-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2009, 'PARAM', 'ORA_ERP', 'DUPLICATE_INVOICE_WINDOW_DAYS', '365',
            'ALL', 'AP_INVOICE', 'NUMBER', 'Window searched for a duplicate supplier invoice number.',
            TO_DATE('2009-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2010, 'PARAM', 'ORA_ERP', 'FX_FALLBACK_RATE_TYPE', 'CORP',
            'NA', NULL, 'CODE', 'Rate type used when the requested type has no row for the date.',
            TO_DATE('2005-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2011, 'PARAM', 'ORA_ERP', 'FX_FALLBACK_RATE_TYPE', 'ECB',
            'EU', NULL, 'CODE', 'Rate type used when the requested type has no row for the date.',
            TO_DATE('2005-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2012, 'PARAM', 'ORA_ERP', 'FX_MAX_LOOKBACK_DAYS', '7',
            'APAC', NULL, 'NUMBER', 'How far back the APAC lookup walks when a weekend has no rate row.',
            TO_DATE('2009-04-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2013, 'PARAM', 'ORA_ERP', 'PO_APPROVAL_LIMIT_DEFAULT_AMT', '25000',
            'ALL', 'PURCHASE_ORDER', 'NUMBER', 'Used when the cost centre has no approval limit set.',
            TO_DATE('2004-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2014, 'PARAM', 'ORA_ERP', 'CONSENT_REQUIRED_FLG', 'Y',
            'EU', 'CUST_MASTER', 'FLAG', 'Marketing contact blocked without recorded consent.',
            TO_DATE('2018-05-25', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2015, 'PARAM', 'ORA_ERP', 'CONSENT_REQUIRED_FLG', 'N',
            'NA', 'CUST_MASTER', 'FLAG', 'NA operates on opt-out, so consent is not required to contact.',
            TO_DATE('2018-05-25', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2016, 'PARAM', 'ORA_ERP', 'RETENTION_PURGE_ENABLED_FLG', 'Y',
            'EU', NULL, 'FLAG', 'Only the EU purge job is enabled; NA and APAC are run by hand.',
            TO_DATE('2019-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2017, 'PARAM', 'ORA_ERP', 'EXTRACT_BATCH_SIZE', '50000',
            'ALL', NULL, 'NUMBER', 'Rows per fetch used by the extract cursors.',
            TO_DATE('2012-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2018, 'PARAM', 'ORA_ERP', 'INTERFACE_RETRY_LIMIT', '99',
            'ALL', NULL, 'NUMBER', 'Effectively unlimited since the backoff logic was removed.',
            TO_DATE('2015-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2019, 'PARAM', 'ORA_ERP', 'SUSPENSE_ACCOUNT_CD', '0000-9999-000-000',
            'ALL', 'GL_JOURNAL', 'CODE', 'Where unmapped postings land.', TO_DATE('1998-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2020, 'PARAM', 'ORA_ERP', 'SCORECARD_PERIOD_BASIS', 'QUARTER',
            'NA', 'SUPPLIER_SCORECARD', 'CODE', 'NA scores quarterly, EU monthly, APAC half-yearly.',
            TO_DATE('2010-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2021, 'PARAM', 'ORA_ERP', 'SCORECARD_PERIOD_BASIS', 'MONTH',
            'EU', 'SUPPLIER_SCORECARD', 'CODE', 'NA scores quarterly, EU monthly, APAC half-yearly.',
            TO_DATE('2010-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
    INTO WWI_REF.CODE_TRANSLATION
        (TRANSLATION_ID, CODE_SET_CD, SOURCE_SYS_CD, SOURCE_VALUE_TXT, TARGET_VALUE_TXT,
         REGION_CD, ENTITY_CD, VALUE_TYPE_CD, DESCRIPTION_TXT, EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT,
         PRIORITY_NBR, ACTIVE_FLG)
    VALUES (2022, 'PARAM', 'ORA_ERP', 'SCORECARD_PERIOD_BASIS', 'HALFYR',
            'APAC', 'SUPPLIER_SCORECARD', 'CODE', 'NA scores quarterly, EU monthly, APAC half-yearly.',
            TO_DATE('2010-01-01', 'YYYY-MM-DD'), NULL,
            100, 'Y')
SELECT * FROM DUAL
/

COMMIT
/

SET DEFINE ON
