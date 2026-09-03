/* =====================================================================
 * Object       : Seed data - WWI_FIN.COST_ALLOCATION_RULE
 * Schema       : WWI_REF / WWI_AUDIT / WWI_FIN (Oracle ERP - WWIGERP)
 * Deploy order : 204 (last seed script)
 * Depends on   : oracle/reference/10_chart_of_accounts_and_cost_centers.sql, oracle/tables/WWI_FIN.COST_ALLOCATION_RULE.sql
 * Called by    : run once per environment, after the reference content
 *
 * The month-end allocation rule sets, one per region, run in sequence order.
 *
 * The driver rules carry a SQL fragment in DRIVER_SQL_TXT which the month-end
 * package concatenates into a dynamic statement. The fragments reference
 * objects by name with no schema qualification and rely on the synonyms, and
 * the NA headcount driver still selects from a cost centre code that closed in
 * 2021 - the rule returns no rows and the allocation silently under-allocates.
 *
 * Percentages within a rule set are not constrained to sum to 100. The EU set
 * sums to 100, the NA set sums to 95 and the remainder stays in the source
 * cost centre.
 * ===================================================================== */

SET DEFINE OFF

INSERT ALL
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1001, 'NA_MONTH', 10, 'Allocate Chicago DC cost to NA procurement', 'NA',
            'NA-WH-CHI', '0000-63%', 'NA-PROC', '0000-6300-000-000',
            'PCT', 15.0000, NULL, NULL, NULL, NULL,
            TO_DATE('2015-01-01', 'YYYY-MM-DD'), NULL, 'N', 'Y',
            NULL, 'Fixed percentage agreed in 2015 and never revisited.')
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1002, 'NA_MONTH', 20, 'Allocate Chicago DC cost to Dallas by headcount', 'NA',
            'NA-WH-CHI', '0000-63%', 'NA-WH-DAL', '0000-6300-000-000',
            'DRIVER', NULL, 'HEADCT',
            'SELECT SUM(HEADCOUNT_NBR) FROM HR_HEADCOUNT_SNAPSHOT WHERE COST_CENTER_CD = ''NA-WH-DAL''',
            NULL, NULL,
            TO_DATE('2015-01-01', 'YYYY-MM-DD'), NULL, 'N', 'Y',
            NULL, 'Driver fragment is concatenated into dynamic SQL by the month-end package.')
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1003, 'NA_MONTH', 30, 'Allocate corporate overhead to Toronto', 'NA',
            'NA-CORP', '0000-63%', 'CA-WH-TOR', '0000-6300-000-000',
            'PCT', 8.0000, NULL, NULL, NULL, NULL,
            TO_DATE('2016-01-01', 'YYYY-MM-DD'), NULL, 'N', 'Y',
            NULL, 'Allocated in USD then translated at the corporate rate.')
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1004, 'NA_MONTH', 40, 'Recharge intercompany services to EU', 'NA',
            'NA-CORP', '0000-69%', 'EU-CORP', '0000-6900-000-000',
            'FIXED', NULL, NULL, NULL, 42000.00000, 'USD',
            TO_DATE('2019-01-01', 'YYYY-MM-DD'), NULL, 'Y', 'Y',
            NULL, 'Reversed the following period and re-raised, so the balance never settles.')
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1005, 'EU_MONTH', 10, 'Allocate Rotterdam hub cost across depots', 'EU',
            'EU-WH-RTM', '0000-63%', 'EU-WH-MUC', '0000-6300-000-000',
            'PCT', 35.0000, NULL, NULL, NULL, NULL,
            TO_DATE('2014-01-01', 'YYYY-MM-DD'), NULL, 'N', 'Y',
            NULL, NULL)
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1006, 'EU_MONTH', 20, 'Allocate Rotterdam hub cost to procurement', 'EU',
            'EU-WH-RTM', '0000-63%', 'EU-PROC', '0000-6300-000-000',
            'PCT', 65.0000, NULL, NULL, NULL, NULL,
            TO_DATE('2014-01-01', 'YYYY-MM-DD'), NULL, 'N', 'Y',
            NULL, 'With rule 10 this set sums to exactly 100.')
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1007, 'EU_MONTH', 30, 'Allocate Dublin depot residual after closure', 'EU',
            'EU-WH-DUB', '0000-63%', 'EU-WH-RTM', '0000-6300-000-000',
            'PCT', 100.0000, NULL, NULL, NULL, NULL,
            TO_DATE('2021-10-01', 'YYYY-MM-DD'), NULL, 'N', 'Y',
            NULL, 'Source cost centre is closed; the rule stays active for trailing accruals.')
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1008, 'EU_MONTH', 40, 'Distribute EU corporate cost evenly across depots', 'EU',
            'EU-CORP', '0000-63%', 'EU-WH-MUC', '0000-6300-000-000',
            'EVEN', NULL, NULL, NULL, NULL, NULL,
            TO_DATE('2017-01-01', 'YYYY-MM-DD'), NULL, 'N', 'Y',
            NULL, 'Even split counts only the target rows present in this rule set.')
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1009, 'AP_MONTH', 10, 'Allocate APAC corporate cost to Sydney', 'APAC',
            'AP-CORP', '0000-63%', 'AP-WH-SYD', '0000-6300-000-000',
            'PCT', 45.0000, NULL, NULL, NULL, NULL,
            TO_DATE('2013-04-01', 'YYYY-MM-DD'), NULL, 'N', 'Y',
            NULL, 'Fiscal year runs April to March, so period 1 is April.')
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1010, 'AP_MONTH', 20, 'Allocate APAC corporate cost to Osaka by floor area', 'APAC',
            'AP-CORP', '0000-63%', 'AP-WH-OSA', '0000-6300-000-000',
            'DRIVER', NULL, 'SQM',
            'SELECT SUM(FLOOR_AREA_SQM) FROM SITE_MASTER WHERE SITE_CD = ''OSA-DC1''',
            NULL, NULL,
            TO_DATE('2013-04-01', 'YYYY-MM-DD'), NULL, 'N', 'Y',
            NULL, 'Result is in square metres; the NA equivalent driver returns square feet.')
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1011, 'AP_MONTH', 30, 'Step-down allocation Sydney to Auckland', 'APAC',
            'AP-WH-SYD', '0000-63%', 'AP-WH-AKL', '0000-6300-000-000',
            'STEP', NULL, NULL, NULL, NULL, NULL,
            TO_DATE('2015-04-01', 'YYYY-MM-DD'), NULL, 'N', 'Y',
            NULL, 'Step-down order is the rule sequence; there is no reciprocal step.')
    INTO WWI_FIN.COST_ALLOCATION_RULE
        (ALLOC_RULE_ID, RULE_SET_CD, RULE_SEQ_NBR, RULE_NAME, REGION_CD,
         SOURCE_COST_CENTER_CD, SOURCE_ACCOUNT_MASK, TARGET_COST_CENTER_CD, TARGET_ACCOUNT_CD,
         ALLOCATION_METHOD_CD, ALLOCATION_PCT, DRIVER_CD, DRIVER_SQL_TXT, FIXED_AMT, FIXED_CURR_CD,
         EFFECTIVE_FROM_DT, EFFECTIVE_TO_DT, REVERSE_NEXT_PERIOD_FLG, ACTIVE_FLG,
         LAST_RUN_PERIOD_CD, NOTES_TXT)
    VALUES (1012, 'AP_MONTH', 40, 'Legacy Singapore recharge (inactive)', 'APAC',
            'AP-CORP', '0000-69%', 'AP-PROC', '0000-6900-000-000',
            'FIXED', NULL, NULL, NULL, 15000.00000, 'SGD',
            TO_DATE('2011-04-01', 'YYYY-MM-DD'), TO_DATE('2018-03-31', 'YYYY-MM-DD'), 'N', 'N',
            '2018-12', 'Left in place so the historic runs can be reproduced.')
SELECT * FROM DUAL
/

COMMIT
/

SET DEFINE ON
