/* =====================================================================
 * Object       : Seed data - WWI_REF.SOURCE_SYSTEM_REF
 * Schema       : WWI_REF / WWI_AUDIT / WWI_FIN (Oracle ERP - WWIGERP)
 * Deploy order : 200 (first seed script)
 * Depends on   : oracle/tables/WWI_REF.SOURCE_SYSTEM_REF.sql
 * Called by    : run once per environment, after the reference content
 *
 * The systems that write into the ERP, and the values every SOURCE_SYS column
 * in the estate is supposed to contain.
 *
 * Three of these are decommissioned and remain because history carries their
 * code: the AS/400 order feed, the acquired NA distributor system, and the
 * first-generation EU warehouse system. Their rows are the only place the
 * codes are still explained.
 *
 * CONNECTION_PARAM_NAME names the deployment parameter that holds the
 * connection details for each feed. No connection strings or credential
 * values are recorded here or anywhere else in the schema.
 * ===================================================================== */

SET DEFINE OFF

INSERT ALL
    INTO WWI_REF.SOURCE_SYSTEM_REF
        (SOURCE_SYS_CD, SYSTEM_NAME, SYSTEM_TYPE_CD, VENDOR_TXT, OWNING_TEAM_TXT, REGION_CD,
         INTERFACE_MODE_CD, INTERFACE_FREQUENCY_CD, CONNECTION_PARAM_NAME, TIMEZONE_TXT,
         COMMISSIONED_DT, DECOMMISSIONED_DT, ACTIVE_FLG, TRUSTED_SOURCE_FLG, NOTES_TXT)
    VALUES ('ORA_ERP', 'Oracle ERP (this instance)', 'ERP', 'Oracle', 'ERP Applications', NULL,
            'DIRECT', 'RT', 'ORACLE_SERVICE', 'UTC',
            TO_DATE('1998-01-01', 'YYYY-MM-DD'), NULL, 'Y', 'Y',
            'System of record for master data, procurement and finance.')
    INTO WWI_REF.SOURCE_SYSTEM_REF
        (SOURCE_SYS_CD, SYSTEM_NAME, SYSTEM_TYPE_CD, VENDOR_TXT, OWNING_TEAM_TXT, REGION_CD,
         INTERFACE_MODE_CD, INTERFACE_FREQUENCY_CD, CONNECTION_PARAM_NAME, TIMEZONE_TXT,
         COMMISSIONED_DT, DECOMMISSIONED_DT, ACTIVE_FLG, TRUSTED_SOURCE_FLG, NOTES_TXT)
    VALUES ('WMS_NA', 'North America warehouse management', 'WMS', 'Manhattan', 'NA Logistics IT', 'NA',
            'FILE', 'H', 'WMS_NA_LANDING_DIR', 'America/Chicago',
            TO_DATE('2004-06-01', 'YYYY-MM-DD'), NULL, 'Y', 'Y',
            'Receipt confirmations arrive as fixed-width files every hour.')
    INTO WWI_REF.SOURCE_SYSTEM_REF
        (SOURCE_SYS_CD, SYSTEM_NAME, SYSTEM_TYPE_CD, VENDOR_TXT, OWNING_TEAM_TXT, REGION_CD,
         INTERFACE_MODE_CD, INTERFACE_FREQUENCY_CD, CONNECTION_PARAM_NAME, TIMEZONE_TXT,
         COMMISSIONED_DT, DECOMMISSIONED_DT, ACTIVE_FLG, TRUSTED_SOURCE_FLG, NOTES_TXT)
    VALUES ('WMS_EU', 'Europe warehouse management', 'WMS', 'Blue Yonder', 'EMEA Logistics IT', 'EU',
            'DBLINK', 'RT', 'WMS_EU_DBLINK_NAME', 'Europe/Amsterdam',
            TO_DATE('2011-03-01', 'YYYY-MM-DD'), NULL, 'Y', 'Y',
            'Reads through a database link; the link definition is deployed separately.')
    INTO WWI_REF.SOURCE_SYSTEM_REF
        (SOURCE_SYS_CD, SYSTEM_NAME, SYSTEM_TYPE_CD, VENDOR_TXT, OWNING_TEAM_TXT, REGION_CD,
         INTERFACE_MODE_CD, INTERFACE_FREQUENCY_CD, CONNECTION_PARAM_NAME, TIMEZONE_TXT,
         COMMISSIONED_DT, DECOMMISSIONED_DT, ACTIVE_FLG, TRUSTED_SOURCE_FLG, NOTES_TXT)
    VALUES ('WMS_AP', 'Asia Pacific warehouse management', 'WMS', 'In-house', 'APAC IT', 'APAC',
            'FILE', 'D', 'WMS_AP_LANDING_DIR', 'Asia/Singapore',
            TO_DATE('2009-04-01', 'YYYY-MM-DD'), NULL, 'Y', 'N',
            'Daily CSV drop. Quantities occasionally arrive in the wrong UOM.')
    INTO WWI_REF.SOURCE_SYSTEM_REF
        (SOURCE_SYS_CD, SYSTEM_NAME, SYSTEM_TYPE_CD, VENDOR_TXT, OWNING_TEAM_TXT, REGION_CD,
         INTERFACE_MODE_CD, INTERFACE_FREQUENCY_CD, CONNECTION_PARAM_NAME, TIMEZONE_TXT,
         COMMISSIONED_DT, DECOMMISSIONED_DT, ACTIVE_FLG, TRUSTED_SOURCE_FLG, NOTES_TXT)
    VALUES ('CRM_GLOBAL', 'Global CRM', 'CRM', 'Siebel', 'Commercial IT', NULL,
            'API', 'RT', 'CRM_ENDPOINT_PARAM', 'UTC',
            TO_DATE('2004-01-01', 'YYYY-MM-DD'), NULL, 'Y', 'Y',
            'Customer master merge source since the 2004 CRM programme.')
    INTO WWI_REF.SOURCE_SYSTEM_REF
        (SOURCE_SYS_CD, SYSTEM_NAME, SYSTEM_TYPE_CD, VENDOR_TXT, OWNING_TEAM_TXT, REGION_CD,
         INTERFACE_MODE_CD, INTERFACE_FREQUENCY_CD, CONNECTION_PARAM_NAME, TIMEZONE_TXT,
         COMMISSIONED_DT, DECOMMISSIONED_DT, ACTIVE_FLG, TRUSTED_SOURCE_FLG, NOTES_TXT)
    VALUES ('TREASURY', 'Treasury FX feed', 'FEED', 'Reuters', 'Corporate Treasury', NULL,
            'FILE', 'D', 'FX_FEED_LANDING_DIR', 'UTC',
            TO_DATE('2002-01-01', 'YYYY-MM-DD'), NULL, 'Y', 'Y',
            'Loads WWI_REF.FX_RATE_DAILY. Three regional variants share this code.')
    INTO WWI_REF.SOURCE_SYSTEM_REF
        (SOURCE_SYS_CD, SYSTEM_NAME, SYSTEM_TYPE_CD, VENDOR_TXT, OWNING_TEAM_TXT, REGION_CD,
         INTERFACE_MODE_CD, INTERFACE_FREQUENCY_CD, CONNECTION_PARAM_NAME, TIMEZONE_TXT,
         COMMISSIONED_DT, DECOMMISSIONED_DT, ACTIVE_FLG, TRUSTED_SOURCE_FLG, NOTES_TXT)
    VALUES ('EDI_GW', 'EDI gateway', 'FEED', 'Sterling', 'Integration Team', NULL,
            'API', 'RT', 'EDI_GATEWAY_PARAM', 'UTC',
            TO_DATE('2007-09-01', 'YYYY-MM-DD'), NULL, 'Y', 'Y',
            'Inbound ASN and invoice documents from trading partners.')
    INTO WWI_REF.SOURCE_SYSTEM_REF
        (SOURCE_SYS_CD, SYSTEM_NAME, SYSTEM_TYPE_CD, VENDOR_TXT, OWNING_TEAM_TXT, REGION_CD,
         INTERFACE_MODE_CD, INTERFACE_FREQUENCY_CD, CONNECTION_PARAM_NAME, TIMEZONE_TXT,
         COMMISSIONED_DT, DECOMMISSIONED_DT, ACTIVE_FLG, TRUSTED_SOURCE_FLG, NOTES_TXT)
    VALUES ('MANUAL', 'Manual entry', 'MANUAL', NULL, 'Business users', NULL,
            'DIRECT', 'RT', NULL, NULL,
            TO_DATE('1998-01-01', 'YYYY-MM-DD'), NULL, 'Y', 'N',
            'Keyed directly into the ERP forms. Highest correction rate of any source.')
    INTO WWI_REF.SOURCE_SYSTEM_REF
        (SOURCE_SYS_CD, SYSTEM_NAME, SYSTEM_TYPE_CD, VENDOR_TXT, OWNING_TEAM_TXT, REGION_CD,
         INTERFACE_MODE_CD, INTERFACE_FREQUENCY_CD, CONNECTION_PARAM_NAME, TIMEZONE_TXT,
         COMMISSIONED_DT, DECOMMISSIONED_DT, ACTIVE_FLG, TRUSTED_SOURCE_FLG, NOTES_TXT)
    VALUES ('MF_AS400', 'AS/400 order entry (decommissioned)', 'MAINFRM', 'IBM', 'Retired', 'NA',
            'FILE', 'D', NULL, 'America/Chicago',
            TO_DATE('1992-01-01', 'YYYY-MM-DD'), TO_DATE('2003-12-31', 'YYYY-MM-DD'), 'N', 'Y',
            'Origin of LEGACY_CUST_CD and the three-digit country codes.')
    INTO WWI_REF.SOURCE_SYSTEM_REF
        (SOURCE_SYS_CD, SYSTEM_NAME, SYSTEM_TYPE_CD, VENDOR_TXT, OWNING_TEAM_TXT, REGION_CD,
         INTERFACE_MODE_CD, INTERFACE_FREQUENCY_CD, CONNECTION_PARAM_NAME, TIMEZONE_TXT,
         COMMISSIONED_DT, DECOMMISSIONED_DT, ACTIVE_FLG, TRUSTED_SOURCE_FLG, NOTES_TXT)
    VALUES ('ACQ_NADIST', 'Acquired NA distributor system (decommissioned)', 'ERP', 'Epicor', 'Retired', 'NA',
            'FILE', 'W', NULL, 'America/Chicago',
            TO_DATE('2008-05-01', 'YYYY-MM-DD'), TO_DATE('2013-06-30', 'YYYY-MM-DD'), 'N', 'N',
            'Source of the duplicate customer records the merge history table records.')
    INTO WWI_REF.SOURCE_SYSTEM_REF
        (SOURCE_SYS_CD, SYSTEM_NAME, SYSTEM_TYPE_CD, VENDOR_TXT, OWNING_TEAM_TXT, REGION_CD,
         INTERFACE_MODE_CD, INTERFACE_FREQUENCY_CD, CONNECTION_PARAM_NAME, TIMEZONE_TXT,
         COMMISSIONED_DT, DECOMMISSIONED_DT, ACTIVE_FLG, TRUSTED_SOURCE_FLG, NOTES_TXT)
    VALUES ('WMS_EU_V1', 'First generation EU warehouse system (decommissioned)', 'WMS', 'In-house', 'Retired', 'EU',
            'FILE', 'D', NULL, 'Europe/London',
            TO_DATE('2000-06-01', 'YYYY-MM-DD'), TO_DATE('2011-02-28', 'YYYY-MM-DD'), 'N', 'N',
            'Receipts loaded under this code have no lot or container detail.')
SELECT * FROM DUAL
/

COMMIT
/

SET DEFINE ON
