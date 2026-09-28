"""Package specifications for the 22 EXT_ORA_* packages of WWI_Extract_Oracle.

GENERATED from ssis/01_oracle_extract/generate_oracle_extracts.py + the emitted .dtsx
(see docs/migration/01_oracle_extract-package-mapping.md). The Oracle SQL is the
legacy OLE DB source query verbatim; ``?`` placeholders are bound from the
watermark window in the order given by ``bindOrder``. Derived columns carry both
the SSIS expression (legacyExpr) and its Spark SQL translation (sparkExpr).
"""
from oracle_extract.model import (
    ConditionalSplit, DerivedColumn, ExtractSpec, Lookup, RowOwnership, SourceColumn, SourceQuery,
)

PROJECT_NAME = "WWI_Extract_Oracle"
STEP_NAME = "Extract Oracle"

EXT_ORA_CUSTOMERMASTER = ExtractSpec(
    packageName='EXT_ORA_CustomerMaster',
    description='Incremental customer master extract (LAST_UPDATE_DT watermark with lookback), denormalised over CUST_MASTER, CUST_CLASSIFICATION and CUST_CREDIT_PROFILE, plus a delete-detection pass over WWI_AUDIT.CHANGE_LOG.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleCustomerMaster',
    loadMode='append',
    ownership=RowOwnership('RecordKind IS NULL', ('RecordKind',), defaultOwner=True),
    watermarkObject='WWI_MDM.CUST_MASTER',
    watermarkType='Timestamp',
    postSteps=('captureInsertCount',),
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'DataFlowTask:Extract Customer Master', 'DataFlowTask:Detect Deleted Customers', 'ExecuteSql:Capture Insert Count', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA CUST_MASTER',
            oracleSql="""
SELECT  c.CUST_ID,
        c.CUST_NBR,
        c.CUST_NAME,
        WWI_MDM.FN_NORMALIZE_NAME(c.CUST_NAME)          AS CUST_NAME_NORM,
        c.LEGAL_ENTITY_CD,
        c.REGION_CD,
        c.COUNTRY_CD,
        WWI_MDM.FN_CUSTOMER_STATUS(c.CUST_ID)           AS CUST_STATUS_CD,
        cl.CLASSIFICATION_CD,
        cl.BUYING_GROUP_CD,
        cl.PRICE_LIST_CD,
        cp.CREDIT_LIMIT_AMT,
        cp.CREDIT_RATING_CD,
        cp.PAYMENT_TERMS_CD,
        c.CURRENCY_CD,
        c.TAX_REGISTRATION_NBR,
        CASE
            WHEN c.REGION_CD = 'EU'
                 AND c.CONSENT_CAPTURED_DT < ADD_MONTHS(SYSDATE, -24) THEN NULL
            ELSE c.MARKETING_CONSENT_FLG
        END                                             AS MARKETING_CONSENT_FLG,
        c.CONSENT_CAPTURED_DT,
        c.FIRST_ORDER_DT,
        c.LAST_UPDATE_DT,
        c.LAST_UPDATE_USER
FROM    WWI_MDM.CUST_MASTER c
        LEFT OUTER JOIN WWI_MDM.CUST_CLASSIFICATION cl
            ON cl.CUST_ID = c.CUST_ID
           AND cl.EFFECTIVE_TO_DT IS NULL
        LEFT OUTER JOIN WWI_MDM.CUST_CREDIT_PROFILE cp
            ON cp.CUST_ID = c.CUST_ID
           AND cp.PROFILE_STATUS_CD = 'CURR'
WHERE   c.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   c.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   c.MERGE_TARGET_CUST_ID IS NULL
ORDER BY c.CUST_ID
""",
            bindOrder=('from', 'to'),
            columns=(
                SourceColumn('CUST_ID', 'decimal(12,0)'),
                SourceColumn('CUST_NBR', 'string'),
                SourceColumn('CUST_NAME', 'string'),
                SourceColumn('CUST_NAME_NORM', 'string'),
                SourceColumn('LEGAL_ENTITY_CD', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('COUNTRY_CD', 'string'),
                SourceColumn('CUST_STATUS_CD', 'string'),
                SourceColumn('CLASSIFICATION_CD', 'string'),
                SourceColumn('BUYING_GROUP_CD', 'string'),
                SourceColumn('PRICE_LIST_CD', 'string'),
                SourceColumn('CREDIT_LIMIT_AMT', 'decimal(18,2)'),
                SourceColumn('CREDIT_RATING_CD', 'string'),
                SourceColumn('PAYMENT_TERMS_CD', 'string'),
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('TAX_REGISTRATION_NBR', 'string'),
                SourceColumn('MARKETING_CONSENT_FLG', 'string'),
                SourceColumn('CONSENT_CAPTURED_DT', 'timestamp'),
                SourceColumn('FIRST_ORDER_DT', 'timestamp'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
                SourceColumn('LAST_UPDATE_USER', 'string'),
            ),
            fileName='WWI_MDM/CUST_MASTER',
            windowColumn='LAST_UPDATE_DT',
            timeoutSeconds=3600,
            partitionColumn='CUST_ID',
            derived=(
                DerivedColumn('DeleteFlag', "'N'", 'string', legacyExpr='"N"'),
            ),
            split=ConditionalSplit(name='Route Unusable Customers', caseName='Valid', matchExpr="CUST_NBR IS NOT NULL AND trim(CUST_NBR) <> '' AND CUST_NAME IS NOT NULL", defaultName='Unusable', defaultIsReject=True, rejectReasonCode='CUST_UNUSABLE', legacyErrTable='err.RejectedCustomer'),
            legacyErrTable='err.RejectedCustomer',
        ),
        SourceQuery(
            name='ORA CHANGE_LOG Customer Deletes',
            oracleSql="""
SELECT  TO_NUMBER(cg.PRIMARY_KEY_VALUE)  AS CUST_ID,
        cg.SECONDARY_KEY_VALUE           AS CUST_NBR,
        cg.CHANGE_DT                     AS LAST_UPDATE_DT,
        cg.CHANGED_BY                    AS LAST_UPDATE_USER
FROM    WWI_AUDIT.CHANGE_LOG cg
WHERE   cg.TABLE_NAME = 'CUST_MASTER'
  AND   cg.OPERATION_CD = 'D'
  AND   cg.CHANGE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   cg.CHANGE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
""",
            bindOrder=('from', 'to'),
            columns=(
                SourceColumn('CUST_ID', 'decimal(12,0)'),
                SourceColumn('CUST_NBR', 'string'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
                SourceColumn('LAST_UPDATE_USER', 'string'),
            ),
            fileName='WWI_AUDIT/CHANGE_LOG__CUST_MASTER_DELETES',
            windowColumn='LAST_UPDATE_DT',
            timeoutSeconds=900,
            derived=(
                DerivedColumn('DeleteFlag', "'Y'", 'string', legacyExpr='"Y"'),
            ),
            rowCountVariable='RowsDeleted',
        ),
    ),
)

EXT_ORA_CUSTOMERADDRESS = ExtractSpec(
    packageName='EXT_ORA_CustomerAddress',
    description='Incremental customer address extract from V_CUSTOMER_ADDRESS_CURRENT with region-specific postal standardisation (ZIP+4, UK/DE postcode casing, APAC prefecture handling) and a geography lookup against raw.OracleGeography.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleCustomerAddress',
    loadMode='append',
    watermarkObject='WWI_MDM.CUST_ADDRESS',
    watermarkType='Timestamp',
    dependsOn=('EXT_ORA_Geography',),
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'DataFlowTask:Extract Customer Addresses', 'ExecuteSql:Log Unmatched Geography', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA V_CUSTOMER_ADDRESS_CURRENT',
            oracleSql="""
SELECT  a.ADDRESS_ID,
        a.CUST_ID,
        a.ADDRESS_TYPE_CD,
        a.ADDRESS_LINE_1,
        a.ADDRESS_LINE_2,
        a.CITY_NAME,
        a.STATE_PROVINCE_CD,
        CASE a.REGION_CD
            WHEN 'NA'   THEN REGEXP_REPLACE(a.POSTAL_CD, '^([0-9]{5})([0-9]{4})$', '\\1-\\2')
            WHEN 'EU'   THEN UPPER(REPLACE(a.POSTAL_CD, ' ', ''))
            WHEN 'APAC' THEN TRIM(a.POSTAL_CD)
            ELSE a.POSTAL_CD
        END                                     AS POSTAL_CD,
        a.COUNTRY_CD,
        a.REGION_CD,
        a.VALIDATION_STATUS_CD,
        a.EFFECTIVE_FROM_DT,
        a.LAST_UPDATE_DT
FROM    WWI_MDM.V_CUSTOMER_ADDRESS_CURRENT a
WHERE   a.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   a.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   a.ADDRESS_TYPE_CD IN ('BILL', 'SHIP', 'STMT')
""",
            bindOrder=('from', 'to'),
            columns=(
                SourceColumn('ADDRESS_ID', 'decimal(12,0)'),
                SourceColumn('CUST_ID', 'decimal(12,0)'),
                SourceColumn('ADDRESS_TYPE_CD', 'string'),
                SourceColumn('ADDRESS_LINE_1', 'string'),
                SourceColumn('ADDRESS_LINE_2', 'string'),
                SourceColumn('CITY_NAME', 'string'),
                SourceColumn('STATE_PROVINCE_CD', 'string'),
                SourceColumn('POSTAL_CD', 'string'),
                SourceColumn('COUNTRY_CD', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('VALIDATION_STATUS_CD', 'string'),
                SourceColumn('EFFECTIVE_FROM_DT', 'timestamp'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_MDM/CUST_ADDRESS',
            windowColumn='LAST_UPDATE_DT',
            timeoutSeconds=1800,
            partitionColumn='CUST_ADDRESS_ID',
            derived=(
                DerivedColumn('AddressLine1Std', 'upper(trim(ADDRESS_LINE_1))', 'string', legacyExpr='UPPER(TRIM(ADDRESS_LINE_1))'),
                DerivedColumn('CityNameStd', 'upper(trim(CITY_NAME))', 'string', legacyExpr='UPPER(TRIM(CITY_NAME))'),
                DerivedColumn('PostalCdStd', "CASE WHEN REGION_CD = 'EU' THEN upper(replace(POSTAL_CD, ' ', '')) ELSE trim(POSTAL_CD) END", 'string', legacyExpr='REGION_CD == "EU" ? UPPER(REPLACE(POSTAL_CD, " ", "")) : TRIM(POSTAL_CD)'),
            ),
            lookup=Lookup(name='Lookup Geography Key', legacyTable='raw.OracleGeography', joinColumns=(('COUNTRY_CD', 'COUNTRY_CD'), ('PostalCdStd', 'PostalCode')), outputColumns=(('GeographyKey', 'GeographyKey', 'int'),), rejectReasonCode='GEO_NOMATCH', rejectReason='Postal code did not resolve to a geography key.', legacyErrTable='err.RejectedLookupFailure', rejectObjectName='WWI_MDM.CUST_ADDRESS'),
            legacyErrTable='err.RejectedLookupFailure',
        ),
    ),
)

EXT_ORA_SUPPLIERMASTER = ExtractSpec(
    packageName='EXT_ORA_SupplierMaster',
    description='Incremental supplier master extract joined to V_SUPPLIER_BANK_MASKED and SUPP_CERTIFICATION, filtered source-side to suppliers transacted in the last seven years (retention rule) and to non-merged parties.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleSupplierMaster',
    loadMode='append',
    watermarkObject='WWI_MDM.SUPP_MASTER',
    watermarkType='Timestamp',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'DataFlowTask:Extract Supplier Master', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA SUPP_MASTER',
            oracleSql="""
SELECT  s.SUPP_ID,
        s.SUPP_NBR,
        s.SUPP_NAME,
        s.SUPP_STATUS_CD,
        s.SUPP_TYPE_CD,
        s.REGION_CD,
        s.COUNTRY_CD,
        s.CURRENCY_CD,
        s.PAYMENT_TERMS_CD,
        s.PAYMENT_METHOD_CD,
        b.BANK_ACCOUNT_MASKED,
        b.BANK_COUNTRY_CD,
        s.TAX_ID_MASKED,
        s.WITHHOLDING_RULE_CD,
        s.DIVERSITY_CLASSIFICATION_CD,
        cert.QUALITY_CERT_CD,
        cert.CERT_EXPIRY_DT,
        s.LAST_TRANSACTION_DT,
        s.LAST_UPDATE_DT
FROM    WWI_MDM.SUPP_MASTER s
        LEFT OUTER JOIN WWI_MDM.V_SUPPLIER_BANK_MASKED b
            ON b.SUPP_ID = s.SUPP_ID
           AND b.PRIMARY_FLG = 'Y'
        LEFT OUTER JOIN (
            SELECT SUPP_ID,
                   MAX(QUALITY_CERT_CD) KEEP (DENSE_RANK LAST ORDER BY CERT_EXPIRY_DT) AS QUALITY_CERT_CD,
                   MAX(CERT_EXPIRY_DT)                                                 AS CERT_EXPIRY_DT
            FROM   WWI_MDM.SUPP_CERTIFICATION
            GROUP BY SUPP_ID
        ) cert ON cert.SUPP_ID = s.SUPP_ID
WHERE   s.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   s.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   (s.LAST_TRANSACTION_DT >= ADD_MONTHS(SYSDATE, -84) OR s.SUPP_STATUS_CD = 'ACTV')
  AND   s.MERGE_TARGET_SUPP_ID IS NULL
""",
            bindOrder=('from', 'to'),
            columns=(
                SourceColumn('SUPP_ID', 'decimal(12,0)'),
                SourceColumn('SUPP_NBR', 'string'),
                SourceColumn('SUPP_NAME', 'string'),
                SourceColumn('SUPP_STATUS_CD', 'string'),
                SourceColumn('SUPP_TYPE_CD', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('COUNTRY_CD', 'string'),
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('PAYMENT_TERMS_CD', 'string'),
                SourceColumn('PAYMENT_METHOD_CD', 'string'),
                SourceColumn('BANK_ACCOUNT_MASKED', 'string'),
                SourceColumn('BANK_COUNTRY_CD', 'string'),
                SourceColumn('TAX_ID_MASKED', 'string'),
                SourceColumn('WITHHOLDING_RULE_CD', 'string'),
                SourceColumn('DIVERSITY_CLASSIFICATION_CD', 'string'),
                SourceColumn('QUALITY_CERT_CD', 'string'),
                SourceColumn('CERT_EXPIRY_DT', 'timestamp'),
                SourceColumn('LAST_TRANSACTION_DT', 'timestamp'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_MDM/SUPP_MASTER',
            windowColumn='LAST_UPDATE_DT',
            timeoutSeconds=2400,
            partitionColumn='SUPP_ID',
            derived=(
                DerivedColumn('CertificationExpiredFlag', "CASE WHEN CERT_EXPIRY_DT IS NULL THEN 'U' WHEN CERT_EXPIRY_DT < current_timestamp() THEN 'Y' ELSE 'N' END", 'string', legacyExpr='ISNULL(CERT_EXPIRY_DT) ? "U" : (CERT_EXPIRY_DT < GETDATE() ? "Y" : "N")'),
                DerivedColumn('WithholdingApplies', "CASE WHEN REGION_CD = 'NA' AND WITHHOLDING_RULE_CD IS NOT NULL THEN 'Y' ELSE 'N' END", 'string', legacyExpr='REGION_CD == "NA" && !ISNULL(WITHHOLDING_RULE_CD) ? "Y" : "N"'),
            ),
            legacyErrTable='err.RejectedSupplier',
        ),
    ),
)

EXT_ORA_PRODUCTMASTER = ExtractSpec(
    packageName='EXT_ORA_ProductMaster',
    description='Incremental product master extract joined to PRODUCT_CATEGORY and PRODUCT_UOM_CONV, converting Oracle NUMBER to the staging decimal scale and detecting obsoletions recorded in WWI_AUDIT.CHANGE_LOG.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleProductMaster',
    loadMode='append',
    ownership=RowOwnership('RecordKind IS NULL', ('RecordKind',), defaultOwner=True),
    watermarkObject='WWI_MDM.PRODUCT_MASTER',
    watermarkType='Timestamp',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'DataFlowTask:Extract Product Master', 'DataFlowTask:Detect Obsoleted Products', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA PRODUCT_MASTER',
            oracleSql="""
SELECT  p.PRODUCT_ID,
        p.PRODUCT_CD,
        p.PRODUCT_DESC,
        c.CATEGORY_CD,
        c.CATEGORY_DESC,
        p.BRAND_CD,
        WWI_MDM.FN_PRODUCT_ACTIVE_FLAG(p.PRODUCT_ID)    AS PRODUCT_STATUS_CD,
        p.BASE_UOM_CD,
        u.SELL_UOM_CD,
        u.CONVERSION_FACTOR                             AS SELL_TO_BASE_FACTOR,
        p.STANDARD_COST_AMT,
        p.LIST_PRICE_AMT,
        p.COST_CURRENCY_CD,
        p.HAZMAT_CLASS_CD,
        p.CHILLER_FLG,
        p.SHELF_LIFE_DAYS,
        p.LAST_UPDATE_DT
FROM    WWI_MDM.PRODUCT_MASTER p
        INNER JOIN WWI_MDM.PRODUCT_CATEGORY c
            ON c.CATEGORY_CD = p.CATEGORY_CD
        LEFT OUTER JOIN WWI_MDM.PRODUCT_UOM_CONV u
            ON u.PRODUCT_ID = p.PRODUCT_ID
           AND u.DEFAULT_SELL_FLG = 'Y'
WHERE   p.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   p.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
""",
            bindOrder=('from', 'to'),
            columns=(
                SourceColumn('PRODUCT_ID', 'decimal(12,0)'),
                SourceColumn('PRODUCT_CD', 'string'),
                SourceColumn('PRODUCT_DESC', 'string'),
                SourceColumn('CATEGORY_CD', 'string'),
                SourceColumn('CATEGORY_DESC', 'string'),
                SourceColumn('BRAND_CD', 'string'),
                SourceColumn('PRODUCT_STATUS_CD', 'string'),
                SourceColumn('BASE_UOM_CD', 'string'),
                SourceColumn('SELL_UOM_CD', 'string'),
                SourceColumn('SELL_TO_BASE_FACTOR', 'decimal(18,6)'),
                SourceColumn('STANDARD_COST_AMT', 'decimal(18,4)'),
                SourceColumn('LIST_PRICE_AMT', 'decimal(18,4)'),
                SourceColumn('COST_CURRENCY_CD', 'string'),
                SourceColumn('HAZMAT_CLASS_CD', 'string'),
                SourceColumn('CHILLER_FLG', 'string'),
                SourceColumn('SHELF_LIFE_DAYS', 'int'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_MDM/PRODUCT_MASTER',
            windowColumn='LAST_UPDATE_DT',
            timeoutSeconds=1800,
            partitionColumn='PRODUCT_ID',
            derived=(
                DerivedColumn('StandardCostAmount', 'cast(STANDARD_COST_AMT as decimal(18,2))', 'decimal(18,2)', legacyExpr='Data Conversion STANDARD_COST_AMT -> DT_NUMERIC(18,2)'),
                DerivedColumn('ListPriceAmount', 'cast(LIST_PRICE_AMT as decimal(18,2))', 'decimal(18,2)', legacyExpr='Data Conversion LIST_PRICE_AMT -> DT_NUMERIC(18,2)'),
                DerivedColumn('SellToBaseFactor', 'cast(SELL_TO_BASE_FACTOR as decimal(18,6))', 'decimal(18,6)', legacyExpr='Data Conversion SELL_TO_BASE_FACTOR -> DT_NUMERIC(18,6)'),
                DerivedColumn('HandlingClass', "CASE WHEN CHILLER_FLG = 'Y' THEN 'CHILL' WHEN HAZMAT_CLASS_CD IS NULL THEN 'AMB' ELSE 'HAZ' END", 'string', legacyExpr='CHILLER_FLG == "Y" ? "CHILL" : (ISNULL(HAZMAT_CLASS_CD) ? "AMB" : "HAZ")'),
                DerivedColumn('DeleteFlag', "'N'", 'string', legacyExpr='"N"'),
            ),
            legacyErrTable='err.RejectedProduct',
        ),
        SourceQuery(
            name='ORA CHANGE_LOG Product Deletes',
            oracleSql="""
SELECT  TO_NUMBER(cg.PRIMARY_KEY_VALUE) AS PRODUCT_ID,
        cg.SECONDARY_KEY_VALUE          AS PRODUCT_CD,
        cg.CHANGE_DT                    AS LAST_UPDATE_DT
FROM    WWI_AUDIT.CHANGE_LOG cg
WHERE   cg.TABLE_NAME = 'PRODUCT_MASTER'
  AND   cg.OPERATION_CD IN ('D', 'O')
  AND   cg.CHANGE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
""",
            bindOrder=('from',),
            columns=(
                SourceColumn('PRODUCT_ID', 'decimal(12,0)'),
                SourceColumn('PRODUCT_CD', 'string'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_AUDIT/CHANGE_LOG__PRODUCT_MASTER_DELETES',
            windowColumn='LAST_UPDATE_DT',
            windowUpperOpen=False,
            upperUnbounded=True,
            timeoutSeconds=600,
            derived=(
                DerivedColumn('DeleteFlag', "'Y'", 'string', legacyExpr='"Y"'),
            ),
            rowCountVariable='RowsDeleted',
        ),
    ),
)

EXT_ORA_PRODUCTHIERARCHY = ExtractSpec(
    packageName='EXT_ORA_ProductHierarchy',
    description='Full truncate-and-load of the flattened product hierarchy. The source query walks PRODUCT_HIERARCHY with CONNECT BY, so the extract is small but expensive and runs on the weekly reference cadence.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleProductMaster',
    loadMode='delete_scope',
    ownership=RowOwnership("RecordKind = 'HIERARCHY'", ('RecordKind',)),
    scopePredicate="RecordKind = 'HIERARCHY'",
    constantColumns=(('RecordKind', 'HIERARCHY'),),
    dependsOn=('EXT_ORA_ProductMaster',),
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Delete Hierarchy Rows', 'DataFlowTask:Load Product Hierarchy', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA PRODUCT_HIERARCHY',
            oracleSql="""
SELECT  h.HIERARCHY_NODE_ID,
        h.PARENT_NODE_ID,
        h.HIERARCHY_CD,
        h.NODE_CD,
        h.NODE_DESC,
        LEVEL                                                   AS NODE_LEVEL,
        SYS_CONNECT_BY_PATH(h.NODE_CD, '/')                     AS NODE_PATH,
        REGEXP_SUBSTR(SYS_CONNECT_BY_PATH(h.NODE_CD, '/'), '[^/]+', 1, 1) AS LEVEL1_CD,
        REGEXP_SUBSTR(SYS_CONNECT_BY_PATH(h.NODE_CD, '/'), '[^/]+', 1, 2) AS LEVEL2_CD,
        REGEXP_SUBSTR(SYS_CONNECT_BY_PATH(h.NODE_CD, '/'), '[^/]+', 1, 3) AS LEVEL3_CD,
        CASE WHEN CONNECT_BY_ISLEAF = 1 THEN 'Y' ELSE 'N' END   AS LEAF_FLG,
        h.LAST_UPDATE_DT
FROM    WWI_MDM.PRODUCT_HIERARCHY h
WHERE   h.HIERARCHY_CD = 'MERCH'
START WITH h.PARENT_NODE_ID IS NULL
CONNECT BY PRIOR h.HIERARCHY_NODE_ID = h.PARENT_NODE_ID
ORDER SIBLINGS BY h.NODE_CD
""",
            bindOrder=(),
            columns=(
                SourceColumn('HIERARCHY_NODE_ID', 'decimal(12,0)'),
                SourceColumn('PARENT_NODE_ID', 'decimal(12,0)'),
                SourceColumn('HIERARCHY_CD', 'string'),
                SourceColumn('NODE_CD', 'string'),
                SourceColumn('NODE_DESC', 'string'),
                SourceColumn('NODE_LEVEL', 'int'),
                SourceColumn('NODE_PATH', 'string'),
                SourceColumn('LEVEL1_CD', 'string'),
                SourceColumn('LEVEL2_CD', 'string'),
                SourceColumn('LEVEL3_CD', 'string'),
                SourceColumn('LEAF_FLG', 'string'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_MDM/PRODUCT_HIERARCHY',
            timeoutSeconds=3600,
        ),
    ),
)

EXT_ORA_PURCHASEORDERHDR = ExtractSpec(
    packageName='EXT_ORA_PurchaseOrderHdr',
    description='Incremental purchase order header extract from V_PURCHASE_ORDER_EXTRACT. Status and org predicates are pushed into the Oracle query; cancelled orders are split out and landed with a cancellation reason for the reconciliation report.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OraclePurchaseOrderHdr',
    loadMode='append',
    watermarkObject='WWI_PROC.PURCHASE_ORDER_HDR',
    watermarkType='Timestamp',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'DataFlowTask:Extract Purchase Orders', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA V_PURCHASE_ORDER_EXTRACT',
            oracleSql="""
SELECT  h.PO_HDR_ID,
        h.PO_NBR,
        h.SUPP_ID,
        h.PURCH_ORG_CD,
        h.REGION_CD,
        h.PO_TYPE_CD,
        h.PO_STATUS_CD,
        h.APPROVAL_STATUS_CD,
        h.BUYER_CD,
        h.CURRENCY_CD,
        h.PO_TOTAL_AMT,
        WWI_FIN.FN_CONVERT_AMOUNT(h.PO_TOTAL_AMT, h.CURRENCY_CD, 'USD', h.ORDER_DT) AS PO_TOTAL_BASE_AMT,
        h.INCOTERM_CD,
        h.PAYMENT_TERMS_CD,
        h.CANCEL_REASON_CD,
        h.ORDER_DT,
        h.PROMISED_DT,
        h.LAST_UPDATE_DT
FROM    WWI_PROC.V_PURCHASE_ORDER_EXTRACT h
WHERE   h.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   h.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   h.PO_STATUS_CD IN ('OPEN', 'PART', 'CLSD', 'CANC')
  AND   h.APPROVAL_STATUS_CD <> 'DRFT'
  AND   h.PURCH_ORG_CD NOT IN ('TEST', 'TRNG')
""",
            bindOrder=('from', 'to'),
            columns=(
                SourceColumn('PO_HDR_ID', 'decimal(12,0)'),
                SourceColumn('PO_NBR', 'string'),
                SourceColumn('SUPP_ID', 'decimal(12,0)'),
                SourceColumn('PURCH_ORG_CD', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('PO_TYPE_CD', 'string'),
                SourceColumn('PO_STATUS_CD', 'string'),
                SourceColumn('APPROVAL_STATUS_CD', 'string'),
                SourceColumn('BUYER_CD', 'string'),
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('PO_TOTAL_AMT', 'decimal(18,2)'),
                SourceColumn('PO_TOTAL_BASE_AMT', 'decimal(18,2)'),
                SourceColumn('INCOTERM_CD', 'string'),
                SourceColumn('PAYMENT_TERMS_CD', 'string'),
                SourceColumn('CANCEL_REASON_CD', 'string'),
                SourceColumn('ORDER_DT', 'timestamp'),
                SourceColumn('PROMISED_DT', 'timestamp'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_PROC/PURCHASE_ORDER_HDR',
            windowColumn='LAST_UPDATE_DT',
            timeoutSeconds=2400,
            partitionColumn='PO_HDR_ID',
            derived=(
                DerivedColumn('OrderAgeDays', 'datediff(current_date(), to_date(ORDER_DT))', 'int', legacyExpr='DATEDIFF("dd", ORDER_DT, GETDATE())'),
                DerivedColumn('LateFlag', "CASE WHEN PROMISED_DT < current_timestamp() AND PO_STATUS_CD <> 'CLSD' THEN 'Y' ELSE 'N' END", 'string', legacyExpr='PROMISED_DT < GETDATE() && PO_STATUS_CD != "CLSD" ? "Y" : "N"'),
            ),
            split=ConditionalSplit(name='Split Cancelled Orders', caseName='Active', matchExpr="PO_STATUS_CD <> 'CANC'", defaultName='Cancelled', defaultIsReject=False),
        ),
    ),
)

EXT_ORA_PURCHASEORDERLINE = ExtractSpec(
    packageName='EXT_ORA_PurchaseOrderLine',
    description='Numeric-key incremental purchase order line extract. The watermark is the highest PO_LINE_ID landed in raw.OraclePurchaseOrderLine; the new maximum is read from the source before the data flow so a mid-run insert cannot be lost.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OraclePurchaseOrderLine',
    loadMode='append',
    watermarkObject='WWI_PROC.PURCHASE_ORDER_LINE',
    watermarkType='NumericKey',
    numericUpperBoundSql='SELECT NVL(MAX(PO_LINE_ID), 0) AS MAX_KEY FROM WWI_PROC.PURCHASE_ORDER_LINE',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'ExecuteSql:Read Source Max Key', 'DataFlowTask:Extract Purchase Order Lines', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA V_PO_LINE_EXTRACT',
            oracleSql="""
SELECT  l.PO_LINE_ID,
        l.PO_HDR_ID,
        l.LINE_NBR,
        l.PRODUCT_ID,
        l.PRODUCT_CD,
        l.LINE_STATUS_CD,
        l.ORDER_QTY,
        l.RECEIVED_QTY,
        WWI_PROC.FN_PO_OPEN_QTY(l.PO_LINE_ID) AS OPEN_QTY,
        l.UOM_CD,
        l.UNIT_PRICE_AMT,
        l.ORDER_QTY * l.UNIT_PRICE_AMT        AS EXTENDED_AMT,
        l.CURRENCY_CD,
        l.COST_CENTER_CD,
        l.GL_ACCOUNT_CD,
        l.NEED_BY_DT,
        l.CREATED_DT
FROM    WWI_PROC.V_PO_LINE_EXTRACT l
WHERE   l.PO_LINE_ID > ?
  AND   l.PO_LINE_ID <= ?
ORDER BY l.PO_LINE_ID
""",
            bindOrder=('from', 'to'),
            columns=(
                SourceColumn('PO_LINE_ID', 'bigint'),
                SourceColumn('PO_HDR_ID', 'decimal(12,0)'),
                SourceColumn('LINE_NBR', 'int'),
                SourceColumn('PRODUCT_ID', 'decimal(12,0)'),
                SourceColumn('PRODUCT_CD', 'string'),
                SourceColumn('LINE_STATUS_CD', 'string'),
                SourceColumn('ORDER_QTY', 'decimal(18,4)'),
                SourceColumn('RECEIVED_QTY', 'decimal(18,4)'),
                SourceColumn('OPEN_QTY', 'decimal(18,4)'),
                SourceColumn('UOM_CD', 'string'),
                SourceColumn('UNIT_PRICE_AMT', 'decimal(18,4)'),
                SourceColumn('EXTENDED_AMT', 'decimal(18,2)'),
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('COST_CENTER_CD', 'string'),
                SourceColumn('GL_ACCOUNT_CD', 'string'),
                SourceColumn('NEED_BY_DT', 'timestamp'),
                SourceColumn('CREATED_DT', 'timestamp'),
            ),
            fileName='WWI_PROC/PURCHASE_ORDER_LINE',
            windowColumn='PO_LINE_ID',
            windowLowerInclusive=False,
            windowUpperOpen=False,
            timeoutSeconds=3600,
            partitionColumn='PO_LINE_ID',
            derived=(
                DerivedColumn('ReceiptCompletePct', 'CASE WHEN ORDER_QTY = 0 THEN cast(0 as decimal(9,4)) ELSE cast(RECEIVED_QTY / ORDER_QTY as decimal(9,4)) END', 'decimal(9,4)', legacyExpr='ORDER_QTY == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(RECEIVED_QTY / ORDER_QTY)'),
            ),
            legacyErrTable='err.RejectedConstraintViolation',
        ),
    ),
)

EXT_ORA_RECEIPTLINE = ExtractSpec(
    packageName='EXT_ORA_ReceiptLine',
    description='Numeric-key incremental receipt line extract joined to PO_RECEIPT_HDR and the PO line, carrying the price/quantity variance percentage used by the supplier scorecard.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleReceiptLine',
    loadMode='append',
    watermarkObject='WWI_PROC.PO_RECEIPT_LINE',
    watermarkType='NumericKey',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'DataFlowTask:Extract Receipt Lines', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA PO_RECEIPT_LINE',
            oracleSql="""
SELECT  rl.RECEIPT_LINE_ID,
        rh.RECEIPT_HDR_ID,
        rh.RECEIPT_NBR,
        rl.PO_LINE_ID,
        ph.PO_NBR,
        rh.SUPP_ID,
        rl.PRODUCT_ID,
        rh.WAREHOUSE_CD,
        rl.RECEIVED_QTY,
        rl.ACCEPTED_QTY,
        rl.RECEIVED_QTY - rl.ACCEPTED_QTY                       AS REJECTED_QTY,
        rl.UOM_CD,
        rl.UNIT_COST_AMT,
        WWI_PROC.FN_RECEIPT_VARIANCE_PCT(rl.RECEIPT_LINE_ID)    AS VARIANCE_PCT,
        rl.INSPECTION_STATUS_CD,
        rl.REJECT_REASON_CD,
        rh.RECEIPT_DT
FROM    WWI_PROC.PO_RECEIPT_LINE rl
        INNER JOIN WWI_PROC.PO_RECEIPT_HDR rh
            ON rh.RECEIPT_HDR_ID = rl.RECEIPT_HDR_ID
        INNER JOIN WWI_PROC.PURCHASE_ORDER_LINE pl
            ON pl.PO_LINE_ID = rl.PO_LINE_ID
        INNER JOIN WWI_PROC.PURCHASE_ORDER_HDR ph
            ON ph.PO_HDR_ID = pl.PO_HDR_ID
WHERE   rl.RECEIPT_LINE_ID > ?
  AND   rh.RECEIPT_STATUS_CD <> 'VOID'
ORDER BY rl.RECEIPT_LINE_ID
""",
            bindOrder=('from',),
            columns=(
                SourceColumn('RECEIPT_LINE_ID', 'bigint'),
                SourceColumn('RECEIPT_HDR_ID', 'decimal(12,0)'),
                SourceColumn('RECEIPT_NBR', 'string'),
                SourceColumn('PO_LINE_ID', 'bigint'),
                SourceColumn('PO_NBR', 'string'),
                SourceColumn('SUPP_ID', 'decimal(12,0)'),
                SourceColumn('PRODUCT_ID', 'decimal(12,0)'),
                SourceColumn('WAREHOUSE_CD', 'string'),
                SourceColumn('RECEIVED_QTY', 'decimal(18,4)'),
                SourceColumn('ACCEPTED_QTY', 'decimal(18,4)'),
                SourceColumn('REJECTED_QTY', 'decimal(18,4)'),
                SourceColumn('UOM_CD', 'string'),
                SourceColumn('UNIT_COST_AMT', 'decimal(18,4)'),
                SourceColumn('VARIANCE_PCT', 'decimal(9,4)'),
                SourceColumn('INSPECTION_STATUS_CD', 'string'),
                SourceColumn('REJECT_REASON_CD', 'string'),
                SourceColumn('RECEIPT_DT', 'timestamp'),
            ),
            fileName='WWI_PROC/PO_RECEIPT_LINE',
            windowColumn='RECEIPT_LINE_ID',
            windowLowerInclusive=False,
            upperUnbounded=True,
            timeoutSeconds=3600,
            partitionColumn='RECEIPT_LINE_ID',
            derived=(
                DerivedColumn('VarianceBand', "CASE WHEN abs(VARIANCE_PCT) <= 0.01 THEN 'OK' WHEN abs(VARIANCE_PCT) <= 0.05 THEN 'WARN' ELSE 'EXCP' END", 'string', legacyExpr='ABS(VARIANCE_PCT) <= 0.01 ? "OK" : (ABS(VARIANCE_PCT) <= 0.05 ? "WARN" : "EXCP")'),
            ),
            split=ConditionalSplit(name='Route Inspection Failures', caseName='Accepted', matchExpr="INSPECTION_STATUS_CD <> 'FAIL'", defaultName='Failed Inspection', defaultIsReject=False),
        ),
    ),
)

EXT_ORA_VENDORCONTRACT = ExtractSpec(
    packageName='EXT_ORA_VendorContract',
    description='Full truncate-and-load of vendor contracts with contract line commitments aggregated in the source query. Contracts are few and change rarely, so the package reloads the whole set rather than tracking a watermark.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleVendorContract',
    loadMode='truncate',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Truncate raw_OracleVendorContract', 'DataFlowTask:Load Vendor Contracts', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA VENDOR_CONTRACT',
            oracleSql="""
SELECT  c.CONTRACT_ID,
        c.CONTRACT_NBR,
        c.SUPP_ID,
        c.CONTRACT_TYPE_CD,
        c.CONTRACT_STATUS_CD,
        c.REGION_CD,
        c.CURRENCY_CD,
        c.COMMITTED_AMT,
        NVL(cl.CONSUMED_AMT, 0)  AS CONSUMED_AMT,
        NVL(cl.LINE_COUNT, 0)    AS LINE_COUNT,
        c.PRICE_PROTECTION_FLG,
        c.AUTO_RENEW_FLG,
        c.NOTICE_PERIOD_DAYS,
        c.EFFECTIVE_FROM_DT,
        c.EFFECTIVE_TO_DT,
        c.LAST_UPDATE_DT
FROM    WWI_PROC.VENDOR_CONTRACT c
        LEFT OUTER JOIN (
            SELECT CONTRACT_ID,
                   SUM(CONSUMED_AMT) AS CONSUMED_AMT,
                   COUNT(*)          AS LINE_COUNT
            FROM   WWI_PROC.VENDOR_CONTRACT_LINE
            GROUP BY CONTRACT_ID
        ) cl ON cl.CONTRACT_ID = c.CONTRACT_ID
WHERE   c.CONTRACT_STATUS_CD <> 'DELT'
""",
            bindOrder=(),
            columns=(
                SourceColumn('CONTRACT_ID', 'decimal(12,0)'),
                SourceColumn('CONTRACT_NBR', 'string'),
                SourceColumn('SUPP_ID', 'decimal(12,0)'),
                SourceColumn('CONTRACT_TYPE_CD', 'string'),
                SourceColumn('CONTRACT_STATUS_CD', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('COMMITTED_AMT', 'decimal(18,2)'),
                SourceColumn('CONSUMED_AMT', 'decimal(18,2)'),
                SourceColumn('LINE_COUNT', 'int'),
                SourceColumn('PRICE_PROTECTION_FLG', 'string'),
                SourceColumn('AUTO_RENEW_FLG', 'string'),
                SourceColumn('NOTICE_PERIOD_DAYS', 'int'),
                SourceColumn('EFFECTIVE_FROM_DT', 'timestamp'),
                SourceColumn('EFFECTIVE_TO_DT', 'timestamp'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_PROC/VENDOR_CONTRACT',
            timeoutSeconds=900,
            derived=(
                DerivedColumn('UtilisationPct', 'CASE WHEN COMMITTED_AMT = 0 THEN cast(0 as decimal(9,4)) ELSE cast(CONSUMED_AMT / COMMITTED_AMT as decimal(9,4)) END', 'decimal(9,4)', legacyExpr='COMMITTED_AMT == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(CONSUMED_AMT / COMMITTED_AMT)'),
                DerivedColumn('RenewalDueFlag', "CASE WHEN AUTO_RENEW_FLG = 'N' AND datediff(to_date(EFFECTIVE_TO_DT), current_date()) <= NOTICE_PERIOD_DAYS THEN 'Y' ELSE 'N' END", 'string', legacyExpr='AUTO_RENEW_FLG == "N" && DATEDIFF("dd", GETDATE(), EFFECTIVE_TO_DT) <= NOTICE_PERIOD_DAYS ? "Y" : "N"'),
            ),
        ),
    ),
)

EXT_ORA_APINVOICEHDR = ExtractSpec(
    packageName='EXT_ORA_ApInvoiceHdr',
    description='Incremental AP invoice header extract. Regional tax treatment diverges in the source query: NA invoices carry state and local sales tax, EU invoices carry recoverable and non-recoverable VAT with a registration number, APAC invoices carry GST. Invoices sitting on an unresolved hold are excluded.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleApInvoiceHdr',
    loadMode='append',
    ownership=RowOwnership('RecordKind IS NULL', ('RecordKind',), defaultOwner=True),
    watermarkObject='WWI_FIN.AP_INVOICE_HDR',
    watermarkType='Timestamp',
    postSteps=('countSuppressedHolds',),
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'DataFlowTask:Extract AP Invoice Headers', 'ExecuteSql:Count Suppressed Holds', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA V_AP_INVOICE_EXTRACT',
            oracleSql="""
SELECT  i.AP_INVOICE_ID,
        i.INVOICE_NBR,
        i.SUPP_ID,
        i.LEGAL_ENTITY_CD,
        i.REGION_CD,
        i.INVOICE_TYPE_CD,
        i.INVOICE_STATUS_CD,
        i.CURRENCY_CD,
        i.INVOICE_AMT,
        WWI_FIN.FN_TAX_AMOUNT(i.AP_INVOICE_ID)  AS TAX_AMT,
        CASE i.REGION_CD
            WHEN 'EU'   THEN i.RECOVERABLE_VAT_AMT
            ELSE 0
        END                                     AS RECOVERABLE_TAX_AMT,
        CASE i.REGION_CD
            WHEN 'EU'   THEN i.NON_RECOVERABLE_VAT_AMT
            WHEN 'NA'   THEN i.SALES_TAX_AMT
            WHEN 'APAC' THEN i.GST_AMT
            ELSE 0
        END                                     AS NON_RECOVERABLE_TAX_AMT,
        CASE i.REGION_CD
            WHEN 'NA'   THEN 'SALESTAX'
            WHEN 'EU'   THEN 'VAT'
            WHEN 'APAC' THEN 'GST'
            ELSE 'NONE'
        END                                     AS TAX_TREATMENT_CD,
        i.TAX_REGISTRATION_NBR,
        WWI_FIN.FN_CONVERT_AMOUNT(i.INVOICE_AMT, i.CURRENCY_CD, 'USD', i.GL_DATE) AS BASE_AMT,
        i.PAYMENT_TERMS_CD,
        i.INVOICE_DT,
        WWI_FIN.FN_DUE_DATE(i.INVOICE_DT, i.PAYMENT_TERMS_CD) AS DUE_DT,
        i.GL_DATE,
        i.LAST_UPDATE_DT
FROM    WWI_FIN.V_AP_INVOICE_EXTRACT i
WHERE   i.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   i.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   NOT EXISTS (
            SELECT 1
            FROM   WWI_FIN.AP_INVOICE_HOLD h
            WHERE  h.AP_INVOICE_ID = i.AP_INVOICE_ID
              AND  h.RELEASE_DT IS NULL
        )
""",
            bindOrder=('from', 'to'),
            columns=(
                SourceColumn('AP_INVOICE_ID', 'decimal(12,0)'),
                SourceColumn('INVOICE_NBR', 'string'),
                SourceColumn('SUPP_ID', 'decimal(12,0)'),
                SourceColumn('LEGAL_ENTITY_CD', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('INVOICE_TYPE_CD', 'string'),
                SourceColumn('INVOICE_STATUS_CD', 'string'),
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('INVOICE_AMT', 'decimal(18,2)'),
                SourceColumn('TAX_AMT', 'decimal(18,2)'),
                SourceColumn('RECOVERABLE_TAX_AMT', 'decimal(18,2)'),
                SourceColumn('NON_RECOVERABLE_TAX_AMT', 'decimal(18,2)'),
                SourceColumn('TAX_TREATMENT_CD', 'string'),
                SourceColumn('TAX_REGISTRATION_NBR', 'string'),
                SourceColumn('BASE_AMT', 'decimal(18,2)'),
                SourceColumn('PAYMENT_TERMS_CD', 'string'),
                SourceColumn('INVOICE_DT', 'timestamp'),
                SourceColumn('DUE_DT', 'timestamp'),
                SourceColumn('GL_DATE', 'timestamp'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_FIN/AP_INVOICE_HDR',
            windowColumn='LAST_UPDATE_DT',
            timeoutSeconds=3600,
            partitionColumn='AP_INVOICE_ID',
            derived=(
                DerivedColumn('TaxRatePct', 'CASE WHEN INVOICE_AMT = 0 THEN cast(0 as decimal(9,4)) ELSE cast(TAX_AMT / INVOICE_AMT as decimal(9,4)) END', 'decimal(9,4)', legacyExpr='INVOICE_AMT == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(TAX_AMT / INVOICE_AMT)'),
                DerivedColumn('VatRecoverableFlag', "CASE WHEN TAX_TREATMENT_CD = 'VAT' AND RECOVERABLE_TAX_AMT > 0 THEN 'Y' ELSE 'N' END", 'string', legacyExpr='TAX_TREATMENT_CD == "VAT" && RECOVERABLE_TAX_AMT > 0 ? "Y" : "N"'),
            ),
            legacyErrTable='err.RejectedConstraintViolation',
        ),
    ),
)

EXT_ORA_APINVOICELINE = ExtractSpec(
    packageName='EXT_ORA_ApInvoiceLine',
    description='Numeric-key incremental AP invoice distribution extract. Cost centres are looked up against raw.OracleCostCenter and unmatched distributions are sent to err.RejectedInvoiceLine rather than defaulted to a suspense account.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleApInvoiceLine',
    loadMode='append',
    watermarkObject='WWI_FIN.AP_INVOICE_LINE',
    watermarkType='NumericKey',
    dependsOn=('EXT_ORA_CostCenter',),
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'DataFlowTask:Extract AP Invoice Lines', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA AP_INVOICE_LINE',
            oracleSql="""
SELECT  l.AP_INVOICE_LINE_ID,
        l.AP_INVOICE_ID,
        l.LINE_NBR,
        l.DISTRIBUTION_TYPE_CD,
        l.COST_CENTER_CD,
        l.GL_ACCOUNT_CD,
        l.PROJECT_CD,
        l.PO_LINE_ID,
        l.LINE_AMT,
        l.LINE_TAX_AMT,
        h.CURRENCY_CD,
        l.EXPENSE_CATEGORY_CD,
        l.ACCRUAL_FLG,
        h.GL_DATE
FROM    WWI_FIN.AP_INVOICE_LINE l
        INNER JOIN WWI_FIN.AP_INVOICE_HDR h
            ON h.AP_INVOICE_ID = l.AP_INVOICE_ID
WHERE   l.AP_INVOICE_LINE_ID > ?
  AND   l.AP_INVOICE_LINE_ID <= ?
  AND   h.INVOICE_STATUS_CD <> 'CANC'
ORDER BY l.AP_INVOICE_LINE_ID
""",
            bindOrder=('from', 'to'),
            columns=(
                SourceColumn('AP_INVOICE_LINE_ID', 'bigint'),
                SourceColumn('AP_INVOICE_ID', 'decimal(12,0)'),
                SourceColumn('LINE_NBR', 'int'),
                SourceColumn('DISTRIBUTION_TYPE_CD', 'string'),
                SourceColumn('COST_CENTER_CD', 'string'),
                SourceColumn('GL_ACCOUNT_CD', 'string'),
                SourceColumn('PROJECT_CD', 'string'),
                SourceColumn('PO_LINE_ID', 'bigint'),
                SourceColumn('LINE_AMT', 'decimal(18,2)'),
                SourceColumn('LINE_TAX_AMT', 'decimal(18,2)'),
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('EXPENSE_CATEGORY_CD', 'string'),
                SourceColumn('ACCRUAL_FLG', 'string'),
                SourceColumn('GL_DATE', 'timestamp'),
            ),
            fileName='WWI_FIN/AP_INVOICE_LINE',
            windowColumn='AP_INVOICE_LINE_ID',
            windowLowerInclusive=False,
            windowUpperOpen=False,
            timeoutSeconds=3600,
            partitionColumn='AP_INVOICE_LINE_ID',
            derived=(
                DerivedColumn('AccrualReversalFlag', "CASE WHEN ACCRUAL_FLG = 'Y' AND LINE_AMT < 0 THEN 'Y' ELSE 'N' END", 'string', legacyExpr='ACCRUAL_FLG == "Y" && LINE_AMT < 0 ? "Y" : "N"'),
            ),
            lookup=Lookup(name='Lookup Cost Center', legacyTable='raw.OracleCostCenter', joinColumns=(('COST_CENTER_CD', 'COST_CENTER_CD'),), outputColumns=(('CostCenterKey', 'CostCenterKey', 'int'), ('REGION_CD', 'OwningRegionCode', 'string')), filterExpr='IsActive = 1', rejectReasonCode='CC_NOMATCH', rejectReason='Cost center is not an active cost center.', legacyErrTable='err.RejectedInvoiceLine', rejectObjectName='WWI_FIN.AP_INVOICE_LINE'),
            legacyErrTable='err.RejectedInvoiceLine',
        ),
    ),
)

EXT_ORA_APPAYMENT = ExtractSpec(
    packageName='EXT_ORA_ApPayment',
    description='Incremental AP payment extract from V_AP_PAYMENT_EXTRACT. Realised FX gain and loss is computed in the source query against the payment-date rate, and void payments are landed with a reversal flag so downstream nets them off.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleApPayment',
    loadMode='append',
    ownership=RowOwnership('PAYMENT_APPLY_ID IS NULL', ('PAYMENT_APPLY_ID',), defaultOwner=True),
    watermarkObject='WWI_FIN.AP_PAYMENT',
    watermarkType='Timestamp',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'DataFlowTask:Extract AP Payments', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA V_AP_PAYMENT_EXTRACT',
            oracleSql="""
SELECT  p.AP_PAYMENT_ID,
        p.PAYMENT_NBR,
        p.SUPP_ID,
        p.PAYMENT_METHOD_CD,
        p.PAYMENT_STATUS_CD,
        p.BANK_ACCOUNT_CD,
        p.CURRENCY_CD,
        p.PAYMENT_AMT,
        WWI_FIN.FN_CONVERT_AMOUNT(p.PAYMENT_AMT, p.CURRENCY_CD, 'USD', p.PAYMENT_DT) AS PAYMENT_BASE_AMT,
        fx.RATE                                              AS FX_RATE,
        WWI_FIN.FN_CONVERT_AMOUNT(p.PAYMENT_AMT, p.CURRENCY_CD, 'USD', p.PAYMENT_DT)
            - WWI_FIN.FN_CONVERT_AMOUNT(p.PAYMENT_AMT, p.CURRENCY_CD, 'USD', p.INVOICE_GL_DATE)
                                                             AS FX_GAIN_LOSS_AMT,
        p.REGION_CD,
        p.VOID_FLG,
        p.PAYMENT_DT,
        p.CLEARED_DT,
        p.LAST_UPDATE_DT
FROM    WWI_FIN.V_AP_PAYMENT_EXTRACT p
        LEFT OUTER JOIN WWI_REF.FX_RATE_DAILY fx
            ON fx.FROM_CURRENCY_CD = p.CURRENCY_CD
           AND fx.TO_CURRENCY_CD = 'USD'
           AND fx.RATE_DT = TRUNC(p.PAYMENT_DT)
           AND fx.RATE_TYPE_CD = 'SPOT'
WHERE   p.LAST_UPDATE_DT >= TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
  AND   p.LAST_UPDATE_DT <  TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
""",
            bindOrder=('from', 'to'),
            columns=(
                SourceColumn('AP_PAYMENT_ID', 'decimal(12,0)'),
                SourceColumn('PAYMENT_NBR', 'string'),
                SourceColumn('SUPP_ID', 'decimal(12,0)'),
                SourceColumn('PAYMENT_METHOD_CD', 'string'),
                SourceColumn('PAYMENT_STATUS_CD', 'string'),
                SourceColumn('BANK_ACCOUNT_CD', 'string'),
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('PAYMENT_AMT', 'decimal(18,2)'),
                SourceColumn('PAYMENT_BASE_AMT', 'decimal(18,2)'),
                SourceColumn('FX_RATE', 'decimal(18,8)'),
                SourceColumn('FX_GAIN_LOSS_AMT', 'decimal(18,2)'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('VOID_FLG', 'string'),
                SourceColumn('PAYMENT_DT', 'timestamp'),
                SourceColumn('CLEARED_DT', 'timestamp'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_FIN/AP_PAYMENT',
            windowColumn='LAST_UPDATE_DT',
            timeoutSeconds=2400,
            partitionColumn='AP_PAYMENT_ID',
            derived=(
                DerivedColumn('ReversalFlag', "CASE WHEN VOID_FLG = 'Y' OR PAYMENT_AMT < 0 THEN 'Y' ELSE 'N' END", 'string', legacyExpr='VOID_FLG == "Y" || PAYMENT_AMT < 0 ? "Y" : "N"'),
                DerivedColumn('DaysToClear', 'CASE WHEN CLEARED_DT IS NULL THEN -1 ELSE datediff(to_date(CLEARED_DT), to_date(PAYMENT_DT)) END', 'int', legacyExpr='ISNULL(CLEARED_DT) ? -1 : DATEDIFF("dd", PAYMENT_DT, CLEARED_DT)'),
            ),
            split=ConditionalSplit(name='Split Void Payments', caseName='Settled', matchExpr="VOID_FLG <> 'Y'", defaultName='Voided', defaultIsReject=True, rejectReasonCode='PAY_VOID', legacyErrTable='err.RejectedPayment'),
            legacyErrTable='err.RejectedPayment',
        ),
    ),
)

EXT_ORA_APPAYMENTAPPLY = ExtractSpec(
    packageName='EXT_ORA_ApPaymentApply',
    description='Numeric-key incremental extract of payment applications, denormalised over AP_PAYMENT_APPLY, AP_PAYMENT and AP_INVOICE_HDR so the settlement, the discount taken and the withheld amount arrive on one row.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleApPayment',
    loadMode='append',
    ownership=RowOwnership('PAYMENT_APPLY_ID IS NOT NULL', ('PAYMENT_APPLY_ID',)),
    watermarkObject='WWI_FIN.AP_PAYMENT_APPLY',
    watermarkType='NumericKey',
    dependsOn=('EXT_ORA_ApPayment',),
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'DataFlowTask:Extract Payment Applications', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA AP_PAYMENT_APPLY',
            oracleSql="""
SELECT  a.PAYMENT_APPLY_ID,
        a.AP_PAYMENT_ID,
        a.AP_INVOICE_ID,
        p.PAYMENT_NBR,
        i.INVOICE_NBR,
        i.SUPP_ID,
        a.APPLIED_AMT,
        a.DISCOUNT_TAKEN_AMT,
        NVL(w.WITHHELD_AMT, 0)  AS WITHHELD_AMT,
        w.WITHHOLDING_RULE_CD,
        p.CURRENCY_CD,
        a.APPLIED_DT,
        i.INVOICE_DT
FROM    WWI_FIN.AP_PAYMENT_APPLY a
        INNER JOIN WWI_FIN.AP_PAYMENT p
            ON p.AP_PAYMENT_ID = a.AP_PAYMENT_ID
        INNER JOIN WWI_FIN.AP_INVOICE_HDR i
            ON i.AP_INVOICE_ID = a.AP_INVOICE_ID
        LEFT OUTER JOIN WWI_FIN.WITHHOLDING_RULE w
            ON w.WITHHOLDING_RULE_CD = i.WITHHOLDING_RULE_CD
WHERE   a.PAYMENT_APPLY_ID > ?
ORDER BY a.PAYMENT_APPLY_ID
""",
            bindOrder=('from',),
            columns=(
                SourceColumn('PAYMENT_APPLY_ID', 'bigint'),
                SourceColumn('AP_PAYMENT_ID', 'decimal(12,0)'),
                SourceColumn('AP_INVOICE_ID', 'decimal(12,0)'),
                SourceColumn('PAYMENT_NBR', 'string'),
                SourceColumn('INVOICE_NBR', 'string'),
                SourceColumn('SUPP_ID', 'decimal(12,0)'),
                SourceColumn('APPLIED_AMT', 'decimal(18,2)'),
                SourceColumn('DISCOUNT_TAKEN_AMT', 'decimal(18,2)'),
                SourceColumn('WITHHELD_AMT', 'decimal(18,2)'),
                SourceColumn('WITHHOLDING_RULE_CD', 'string'),
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('APPLIED_DT', 'timestamp'),
                SourceColumn('INVOICE_DT', 'timestamp'),
            ),
            fileName='WWI_FIN/AP_PAYMENT_APPLY',
            windowColumn='PAYMENT_APPLY_ID',
            windowLowerInclusive=False,
            upperUnbounded=True,
            timeoutSeconds=1800,
            partitionColumn='PAYMENT_APPLY_ID',
            derived=(
                DerivedColumn('SettlementDays', 'datediff(to_date(APPLIED_DT), to_date(INVOICE_DT))', 'int', legacyExpr='DATEDIFF("dd", INVOICE_DT, APPLIED_DT)'),
                DerivedColumn('DiscountTakenFlag', "CASE WHEN DISCOUNT_TAKEN_AMT > 0 THEN 'Y' ELSE 'N' END", 'string', legacyExpr='DISCOUNT_TAKEN_AMT > 0 ? "Y" : "N"'),
            ),
            legacyErrTable='err.RejectedPayment',
        ),
    ),
)

EXT_ORA_APAGING = ExtractSpec(
    packageName='EXT_ORA_ApAging',
    description='Full reload of the AP aging snapshot. The ERP recomputes the buckets nightly, so the package clears the current as-of date and reloads it; the aging bucket boundaries come from the ERP function rather than being reimplemented here.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleApInvoiceHdr',
    loadMode='delete_snapshot',
    ownership=RowOwnership("RecordKind = 'AGING'", ('RecordKind',)),
    scopePredicate="RecordKind = 'AGING'",
    constantColumns=(('RecordKind', 'AGING'),),
    dependsOn=('EXT_ORA_ApInvoiceHdr',),
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Clear Current Snapshot', 'DataFlowTask:Load AP Aging Snapshot', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA V_AP_AGING_CURRENT',
            oracleSql="""
SELECT  a.AP_INVOICE_ID,
        a.INVOICE_NBR,
        a.SUPP_ID,
        a.REGION_CD,
        a.CURRENCY_CD,
        a.OUTSTANDING_AMT,
        WWI_FIN.FN_CONVERT_AMOUNT(a.OUTSTANDING_AMT, a.CURRENCY_CD, 'USD', a.SNAPSHOT_DT) AS OUTSTANDING_BASE_AMT,
        TRUNC(a.SNAPSHOT_DT) - TRUNC(a.DUE_DT)          AS DAYS_PAST_DUE,
        WWI_FIN.FN_AGING_BUCKET(a.DUE_DT, a.SNAPSHOT_DT) AS AGING_BUCKET_CD,
        a.DISPUTE_FLG,
        a.DUE_DT,
        a.SNAPSHOT_DT
FROM    WWI_FIN.V_AP_AGING_CURRENT a
WHERE   a.OUTSTANDING_AMT <> 0
""",
            bindOrder=(),
            columns=(
                SourceColumn('AP_INVOICE_ID', 'decimal(12,0)'),
                SourceColumn('INVOICE_NBR', 'string'),
                SourceColumn('SUPP_ID', 'decimal(12,0)'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('OUTSTANDING_AMT', 'decimal(18,2)'),
                SourceColumn('OUTSTANDING_BASE_AMT', 'decimal(18,2)'),
                SourceColumn('DAYS_PAST_DUE', 'int'),
                SourceColumn('AGING_BUCKET_CD', 'string'),
                SourceColumn('DISPUTE_FLG', 'string'),
                SourceColumn('DUE_DT', 'timestamp'),
                SourceColumn('SNAPSHOT_DT', 'timestamp'),
            ),
            fileName='WWI_FIN/AP_AGING_SNAPSHOT',
            timeoutSeconds=2400,
        ),
    ),
)

EXT_ORA_GLJOURNALLINE = ExtractSpec(
    packageName='EXT_ORA_GlJournalLine',
    description='Date-window GL journal line extract. The window comes from etl.usp_GetWatermark (DateWindow style) and is re-runnable for any bounded accounting-date range; only periods that the ledger reports as open or recently closed are pulled, and the fiscal period is resolved per region.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleGlJournalLine',
    loadMode='delete_window',
    watermarkObject='WWI_FIN.GL_JOURNAL_LINE',
    watermarkType='DateWindow',
    windowDeleteColumn='ACCOUNTING_DT',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'ExecuteSql:Clear Target Window', 'DataFlowTask:Extract GL Journal Lines', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA GL_JOURNAL_LINE',
            oracleSql="""
SELECT  l.GL_JOURNAL_LINE_ID,
        h.GL_JOURNAL_HDR_ID,
        h.JOURNAL_NBR,
        h.JOURNAL_SOURCE_CD,
        h.JOURNAL_CATEGORY_CD,
        h.LEDGER_CD,
        h.REGION_CD,
        l.GL_ACCOUNT_CD,
        l.COST_CENTER_CD,
        l.PRODUCT_LINE_CD,
        l.ENTERED_DR_AMT,
        l.ENTERED_CR_AMT,
        l.ACCOUNTED_DR_AMT,
        l.ACCOUNTED_CR_AMT,
        h.CURRENCY_CD,
        WWI_REF.FN_FISCAL_PERIOD(h.ACCOUNTING_DT, h.REGION_CD) AS FISCAL_PERIOD_CD,
        ps.PERIOD_STATUS_CD,
        h.ACCOUNTING_DT,
        h.POSTED_DT
FROM    WWI_FIN.GL_JOURNAL_LINE l
        INNER JOIN WWI_FIN.GL_JOURNAL_HDR h
            ON h.GL_JOURNAL_HDR_ID = l.GL_JOURNAL_HDR_ID
        INNER JOIN WWI_FIN.GL_PERIOD_STATUS ps
            ON ps.LEDGER_CD = h.LEDGER_CD
           AND ps.FISCAL_PERIOD_CD = WWI_REF.FN_FISCAL_PERIOD(h.ACCOUNTING_DT, h.REGION_CD)
WHERE   h.ACCOUNTING_DT >= TO_DATE(?, 'YYYY-MM-DD')
  AND   h.ACCOUNTING_DT <  TO_DATE(?, 'YYYY-MM-DD')
  AND   h.POSTING_STATUS_CD = 'P'
  AND   ps.PERIOD_STATUS_CD IN ('O', 'C')
ORDER BY h.ACCOUNTING_DT, l.GL_JOURNAL_LINE_ID
""",
            bindOrder=('from', 'to'),
            columns=(
                SourceColumn('GL_JOURNAL_LINE_ID', 'bigint'),
                SourceColumn('GL_JOURNAL_HDR_ID', 'decimal(12,0)'),
                SourceColumn('JOURNAL_NBR', 'string'),
                SourceColumn('JOURNAL_SOURCE_CD', 'string'),
                SourceColumn('JOURNAL_CATEGORY_CD', 'string'),
                SourceColumn('LEDGER_CD', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('GL_ACCOUNT_CD', 'string'),
                SourceColumn('COST_CENTER_CD', 'string'),
                SourceColumn('PRODUCT_LINE_CD', 'string'),
                SourceColumn('ENTERED_DR_AMT', 'decimal(18,2)'),
                SourceColumn('ENTERED_CR_AMT', 'decimal(18,2)'),
                SourceColumn('ACCOUNTED_DR_AMT', 'decimal(18,2)'),
                SourceColumn('ACCOUNTED_CR_AMT', 'decimal(18,2)'),
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('FISCAL_PERIOD_CD', 'string'),
                SourceColumn('PERIOD_STATUS_CD', 'string'),
                SourceColumn('ACCOUNTING_DT', 'timestamp'),
                SourceColumn('POSTED_DT', 'timestamp'),
            ),
            fileName='WWI_FIN/GL_JOURNAL_LINE',
            windowColumn='ACCOUNTING_DT',
            timeoutSeconds=7200,
            partitionColumn='GL_JOURNAL_LINE_ID',
            derived=(
                DerivedColumn('NetAmount', 'cast(ACCOUNTED_DR_AMT - ACCOUNTED_CR_AMT as decimal(18,2))', 'decimal(18,2)', legacyExpr='ACCOUNTED_DR_AMT - ACCOUNTED_CR_AMT'),
                DerivedColumn('FiscalCalendarCd', "CASE WHEN REGION_CD = 'NA' THEN '445' WHEN REGION_CD = 'EU' THEN 'CAL' ELSE 'APR_MAR' END", 'string', legacyExpr='REGION_CD == "NA" ? "445" : (REGION_CD == "EU" ? "CAL" : "APR_MAR")'),
            ),
            legacyErrTable='err.RejectedConstraintViolation',
        ),
    ),
)

EXT_ORA_COSTCENTER = ExtractSpec(
    packageName='EXT_ORA_CostCenter',
    description='Full truncate-and-load of the cost centre hierarchy from V_COST_CENTER_HIERARCHY, including the allocation rule that applies to each centre. Small dimension, reloaded whole every night.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleCostCenter',
    loadMode='truncate',
    postSteps=('assignCostCenterSurrogateKeys',),
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Truncate raw_OracleCostCenter', 'DataFlowTask:Load Cost Centers', 'ExecuteSql:Assign Surrogate Keys', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA V_COST_CENTER_HIERARCHY',
            oracleSql="""
SELECT  cc.COST_CENTER_CD,
        cc.COST_CENTER_NAME,
        cc.PARENT_COST_CENTER_CD,
        cc.HIERARCHY_LEVEL,
        cc.COMPANY_CD,
        cc.REGION_CD,
        cc.MANAGER_EMPLOYEE_CD,
        cc.FUNCTION_CD,
        ar.ALLOCATION_RULE_CD,
        ar.ALLOCATION_PCT,
        cc.ACTIVE_FLG,
        cc.EFFECTIVE_FROM_DT,
        cc.EFFECTIVE_TO_DT
FROM    WWI_FIN.V_COST_CENTER_HIERARCHY cc
        LEFT OUTER JOIN WWI_FIN.COST_ALLOCATION_RULE ar
            ON ar.COST_CENTER_CD = cc.COST_CENTER_CD
           AND SYSDATE BETWEEN ar.EFFECTIVE_FROM_DT AND NVL(ar.EFFECTIVE_TO_DT, DATE '4712-12-31')
""",
            bindOrder=(),
            columns=(
                SourceColumn('COST_CENTER_CD', 'string'),
                SourceColumn('COST_CENTER_NAME', 'string'),
                SourceColumn('PARENT_COST_CENTER_CD', 'string'),
                SourceColumn('HIERARCHY_LEVEL', 'int'),
                SourceColumn('COMPANY_CD', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('MANAGER_EMPLOYEE_CD', 'string'),
                SourceColumn('FUNCTION_CD', 'string'),
                SourceColumn('ALLOCATION_RULE_CD', 'string'),
                SourceColumn('ALLOCATION_PCT', 'decimal(9,4)'),
                SourceColumn('ACTIVE_FLG', 'string'),
                SourceColumn('EFFECTIVE_FROM_DT', 'timestamp'),
                SourceColumn('EFFECTIVE_TO_DT', 'timestamp'),
            ),
            fileName='WWI_FIN/COST_CENTER',
            timeoutSeconds=600,
            derived=(
                DerivedColumn('IsActive', "CASE WHEN ACTIVE_FLG = 'Y' THEN 1 ELSE 0 END", 'int', legacyExpr='ACTIVE_FLG == "Y" ? (DT_I4)1 : (DT_I4)0'),
                DerivedColumn('CostCenterKey', '0', 'int', legacyExpr='(DT_I4)0'),
            ),
        ),
    ),
)

EXT_ORA_TAXRATE = ExtractSpec(
    packageName='EXT_ORA_TaxRate',
    description='Reference refresh of tax rates on the weekly reference cadence. The source query unions the three regimes the estate actually runs - NA state/county sales tax, EU VAT with reduced and zero rates, APAC GST - because they live in TAX_RATE with different jurisdiction granularity.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleTaxRate',
    loadMode='truncate',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Truncate raw_OracleTaxRate', 'DataFlowTask:Load Tax Rates', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA TAX_RATE',
            oracleSql="""
SELECT  t.TAX_RATE_CD,
        'SALESTAX'                  AS TAX_REGIME_CD,
        j.JURISDICTION_CD,
        j.JURISDICTION_LEVEL_CD,
        j.COUNTRY_CD,
        'NA'                        AS REGION_CD,
        t.RATE_PCT,
        t.RATE_TYPE_CD,
        'N'                         AS RECOVERABLE_FLG,
        t.REPORTING_CATEGORY_CD,
        t.EFFECTIVE_FROM_DT,
        t.EFFECTIVE_TO_DT
FROM    WWI_FIN.TAX_RATE t
        INNER JOIN WWI_FIN.TAX_JURISDICTION j
            ON j.JURISDICTION_CD = t.JURISDICTION_CD
WHERE   j.COUNTRY_CD IN ('USA', 'CAN', 'MEX')
UNION ALL
SELECT  t.TAX_RATE_CD,
        'VAT'                       AS TAX_REGIME_CD,
        j.JURISDICTION_CD,
        'COUNTRY'                   AS JURISDICTION_LEVEL_CD,
        j.COUNTRY_CD,
        'EU'                        AS REGION_CD,
        t.RATE_PCT,
        t.RATE_TYPE_CD,
        t.RECOVERABLE_FLG,
        t.REPORTING_CATEGORY_CD,
        t.EFFECTIVE_FROM_DT,
        t.EFFECTIVE_TO_DT
FROM    WWI_FIN.TAX_RATE t
        INNER JOIN WWI_FIN.TAX_JURISDICTION j
            ON j.JURISDICTION_CD = t.JURISDICTION_CD
WHERE   j.TAX_REGIME_CD = 'VAT'
UNION ALL
SELECT  t.TAX_RATE_CD,
        'GST'                       AS TAX_REGIME_CD,
        j.JURISDICTION_CD,
        'COUNTRY'                   AS JURISDICTION_LEVEL_CD,
        j.COUNTRY_CD,
        'APAC'                      AS REGION_CD,
        t.RATE_PCT,
        t.RATE_TYPE_CD,
        'Y'                         AS RECOVERABLE_FLG,
        t.REPORTING_CATEGORY_CD,
        t.EFFECTIVE_FROM_DT,
        t.EFFECTIVE_TO_DT
FROM    WWI_FIN.TAX_RATE t
        INNER JOIN WWI_FIN.TAX_JURISDICTION j
            ON j.JURISDICTION_CD = t.JURISDICTION_CD
WHERE   j.TAX_REGIME_CD = 'GST'
""",
            bindOrder=(),
            columns=(
                SourceColumn('TAX_RATE_CD', 'string'),
                SourceColumn('TAX_REGIME_CD', 'string'),
                SourceColumn('JURISDICTION_CD', 'string'),
                SourceColumn('JURISDICTION_LEVEL_CD', 'string'),
                SourceColumn('COUNTRY_CD', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('RATE_PCT', 'decimal(9,5)'),
                SourceColumn('RATE_TYPE_CD', 'string'),
                SourceColumn('RECOVERABLE_FLG', 'string'),
                SourceColumn('REPORTING_CATEGORY_CD', 'string'),
                SourceColumn('EFFECTIVE_FROM_DT', 'timestamp'),
                SourceColumn('EFFECTIVE_TO_DT', 'timestamp'),
            ),
            fileName='WWI_FIN/TAX_RATE',
            timeoutSeconds=600,
            derived=(
                DerivedColumn('RateBasisPoints', 'cast(RATE_PCT * 10000 as int)', 'int', legacyExpr='(DT_I4)(RATE_PCT * 10000)'),
                DerivedColumn('CurrentFlag', "CASE WHEN EFFECTIVE_TO_DT IS NULL OR EFFECTIVE_TO_DT >= current_timestamp() THEN 'Y' ELSE 'N' END", 'string', legacyExpr='ISNULL(EFFECTIVE_TO_DT) || EFFECTIVE_TO_DT >= GETDATE() ? "Y" : "N"'),
            ),
        ),
    ),
)

EXT_ORA_PAYMENTTERMS = ExtractSpec(
    packageName='EXT_ORA_PaymentTerms',
    description='Reference refresh of payment terms from V_PAYMENT_TERMS_EXTRACT, including the discount ladder (2/10 net 30 style) that the AP matching rules read.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OraclePaymentTerms',
    loadMode='truncate',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Truncate raw_OraclePaymentTerms', 'DataFlowTask:Load Payment Terms', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA V_PAYMENT_TERMS_EXTRACT',
            oracleSql="""
SELECT  pt.PAYMENT_TERMS_CD,
        pt.PAYMENT_TERMS_DESC,
        pt.NET_DAYS,
        pt.DISCOUNT_DAYS,
        pt.DISCOUNT_PCT,
        pt.DUE_DATE_BASIS_CD,
        pt.PRORATE_FLG,
        pt.REGION_CD,
        pt.ACTIVE_FLG,
        pt.LAST_UPDATE_DT
FROM    WWI_FIN.V_PAYMENT_TERMS_EXTRACT pt
ORDER BY pt.PAYMENT_TERMS_CD
""",
            bindOrder=(),
            columns=(
                SourceColumn('PAYMENT_TERMS_CD', 'string'),
                SourceColumn('PAYMENT_TERMS_DESC', 'string'),
                SourceColumn('NET_DAYS', 'int'),
                SourceColumn('DISCOUNT_DAYS', 'int'),
                SourceColumn('DISCOUNT_PCT', 'decimal(9,4)'),
                SourceColumn('DUE_DATE_BASIS_CD', 'string'),
                SourceColumn('PRORATE_FLG', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('ACTIVE_FLG', 'string'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_FIN/PAYMENT_TERMS',
            timeoutSeconds=300,
            derived=(
                DerivedColumn('EarlyPayIncentiveFlag', "CASE WHEN DISCOUNT_PCT > 0 AND DISCOUNT_DAYS > 0 THEN 'Y' ELSE 'N' END", 'string', legacyExpr='DISCOUNT_PCT > 0 && DISCOUNT_DAYS > 0 ? "Y" : "N"'),
                DerivedColumn('EffectiveAnnualisedPct', 'CASE WHEN DISCOUNT_DAYS = NET_DAYS THEN cast(0 as decimal(9,4)) ELSE cast(DISCOUNT_PCT * 365 / (NET_DAYS - DISCOUNT_DAYS) as decimal(9,4)) END', 'decimal(9,4)', legacyExpr='DISCOUNT_DAYS == NET_DAYS ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(DISCOUNT_PCT * 365 / (NET_DAYS - DISCOUNT_DAYS))'),
            ),
        ),
    ),
)

EXT_ORA_CURRENCY = ExtractSpec(
    packageName='EXT_ORA_Currency',
    description='Reference refresh of the currency master from V_CURRENCY_EXTRACT. Carries minor-unit precision and the regional reporting currency each ledger rolls up to (USD for NA, EUR for EU, SGD for APAC).',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleCurrency',
    loadMode='truncate',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Truncate raw_OracleCurrency', 'DataFlowTask:Load Currencies', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA V_CURRENCY_EXTRACT',
            oracleSql="""
SELECT  c.CURRENCY_CD,
        c.CURRENCY_NAME,
        c.CURRENCY_SYMBOL,
        c.MINOR_UNIT_DIGITS,
        c.ISO_NUMERIC_CD,
        c.ACTIVE_FLG,
        CASE c.PRIMARY_REGION_CD
            WHEN 'NA'   THEN 'USD'
            WHEN 'EU'   THEN 'EUR'
            WHEN 'APAC' THEN 'SGD'
            ELSE 'USD'
        END                     AS REPORTING_CURRENCY_CD,
        c.PRIMARY_REGION_CD     AS REGION_CD,
        c.LAST_UPDATE_DT
FROM    WWI_REF.V_CURRENCY_EXTRACT c
""",
            bindOrder=(),
            columns=(
                SourceColumn('CURRENCY_CD', 'string'),
                SourceColumn('CURRENCY_NAME', 'string'),
                SourceColumn('CURRENCY_SYMBOL', 'string'),
                SourceColumn('MINOR_UNIT_DIGITS', 'int'),
                SourceColumn('ISO_NUMERIC_CD', 'string'),
                SourceColumn('ACTIVE_FLG', 'string'),
                SourceColumn('REPORTING_CURRENCY_CD', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_REF/CURRENCY_CODE',
            timeoutSeconds=300,
        ),
    ),
)

EXT_ORA_FXRATEDAILY = ExtractSpec(
    packageName='EXT_ORA_FxRateDaily',
    description='Date-window FX rate extract. Spot, corporate and period-average rates are pulled for the requested rate-date window; EUR and SGD cross rates are triangulated through USD because the ERP only publishes USD pairs for the minor currencies.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleFxRate',
    loadMode='delete_window',
    watermarkObject='WWI_REF.FX_RATE_DAILY',
    watermarkType='DateWindow',
    windowDeleteColumn='RATE_DT',
    postSteps=('countMissingRateDays',),
    dependsOn=('EXT_ORA_Currency',),
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Get Watermark', 'ExecuteSql:Clear Rate Window', 'DataFlowTask:Extract FX Rates', 'ExecuteSql:Count Missing Rate Days', 'ExecuteSql:Set Watermark', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA FX_RATE_DAILY',
            oracleSql="""
SELECT  fx.RATE_DT,
        fx.FROM_CURRENCY_CD,
        fx.TO_CURRENCY_CD,
        fx.RATE_TYPE_CD,
        fx.RATE,
        CASE WHEN fx.RATE = 0 THEN 0 ELSE 1 / fx.RATE END   AS INVERSE_RATE,
        fx.RATE_SOURCE_CD,
        'GLOBAL'                                            AS REGION_CD,
        fx.LAST_UPDATE_DT
FROM    WWI_REF.FX_RATE_DAILY fx
WHERE   fx.RATE_DT >= TO_DATE(?, 'YYYY-MM-DD')
  AND   fx.RATE_DT <  TO_DATE(?, 'YYYY-MM-DD')
  AND   fx.RATE_TYPE_CD IN ('SPOT', 'CORP', 'AVG')
UNION ALL
SELECT  usd_from.RATE_DT,
        usd_from.FROM_CURRENCY_CD,
        usd_to.FROM_CURRENCY_CD                             AS TO_CURRENCY_CD,
        usd_from.RATE_TYPE_CD,
        usd_from.RATE / NULLIF(usd_to.RATE, 0)              AS RATE,
        usd_to.RATE / NULLIF(usd_from.RATE, 0)              AS INVERSE_RATE,
        'TRIANG'                                            AS RATE_SOURCE_CD,
        CASE usd_to.FROM_CURRENCY_CD
            WHEN 'EUR' THEN 'EU'
            WHEN 'SGD' THEN 'APAC'
            ELSE 'GLOBAL'
        END                                                 AS REGION_CD,
        usd_from.LAST_UPDATE_DT
FROM    WWI_REF.FX_RATE_DAILY usd_from
        INNER JOIN WWI_REF.FX_RATE_DAILY usd_to
            ON usd_to.RATE_DT = usd_from.RATE_DT
           AND usd_to.RATE_TYPE_CD = usd_from.RATE_TYPE_CD
           AND usd_to.TO_CURRENCY_CD = 'USD'
           AND usd_to.FROM_CURRENCY_CD IN ('EUR', 'SGD')
WHERE   usd_from.TO_CURRENCY_CD = 'USD'
  AND   usd_from.RATE_DT >= TO_DATE(?, 'YYYY-MM-DD')
  AND   usd_from.RATE_DT <  TO_DATE(?, 'YYYY-MM-DD')
  AND   usd_from.FROM_CURRENCY_CD <> usd_to.FROM_CURRENCY_CD
""",
            bindOrder=('from', 'to', 'from', 'to'),
            columns=(
                SourceColumn('RATE_DT', 'timestamp'),
                SourceColumn('FROM_CURRENCY_CD', 'string'),
                SourceColumn('TO_CURRENCY_CD', 'string'),
                SourceColumn('RATE_TYPE_CD', 'string'),
                SourceColumn('RATE', 'decimal(18,8)'),
                SourceColumn('INVERSE_RATE', 'decimal(18,8)'),
                SourceColumn('RATE_SOURCE_CD', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_REF/FX_RATE_DAILY',
            windowColumn='RATE_DT',
            timeoutSeconds=1200,
            derived=(
                DerivedColumn('RatePairCd', "concat(FROM_CURRENCY_CD, '/', TO_CURRENCY_CD)", 'string', legacyExpr='FROM_CURRENCY_CD + "/" + TO_CURRENCY_CD'),
            ),
            legacyErrTable='err.RejectedConstraintViolation',
        ),
    ),
)

EXT_ORA_GEOGRAPHY = ExtractSpec(
    packageName='EXT_ORA_Geography',
    description='Full truncate-and-load of the geography reference from V_GEOGRAPHY_EXTRACT. Postal formatting differs by region (ZIP+4 in NA, outward/inward split in the UK, prefecture and postal district in APAC) and is normalised here so every downstream address lookup uses one shape.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleGeography',
    loadMode='truncate',
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Truncate raw_OracleGeography', 'DataFlowTask:Load Geography', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA V_GEOGRAPHY_EXTRACT',
            oracleSql="""
SELECT  g.GEOGRAPHY_KEY                    AS GeographyKey,
        g.COUNTRY_CD,
        g.COUNTRY_NAME,
        g.REGION_CD,
        g.SUBREGION_CD,
        g.STATE_PROVINCE_CD,
        g.STATE_PROVINCE_NAME,
        g.CITY_NAME,
        CASE g.REGION_CD
            WHEN 'NA'   THEN REGEXP_REPLACE(g.POSTAL_CD, '^([0-9]{5})([0-9]{4})$', '\\1-\\2')
            WHEN 'EU'   THEN UPPER(REPLACE(g.POSTAL_CD, ' ', ''))
            ELSE UPPER(TRIM(g.POSTAL_CD))
        END                                 AS POSTAL_CD,
        CASE g.REGION_CD
            WHEN 'NA'   THEN 'ZIP5_PLUS4'
            WHEN 'EU'   THEN 'ALPHANUM'
            WHEN 'APAC' THEN 'NUMERIC6'
            ELSE 'FREEFORM'
        END                                 AS POSTAL_FORMAT_CD,
        g.TIME_ZONE_CD,
        g.LATITUDE,
        g.LONGITUDE
FROM    WWI_REF.V_GEOGRAPHY_EXTRACT g
WHERE   g.ACTIVE_FLG = 'Y'
""",
            bindOrder=(),
            columns=(
                SourceColumn('GeographyKey', 'int'),
                SourceColumn('COUNTRY_CD', 'string'),
                SourceColumn('COUNTRY_NAME', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('SUBREGION_CD', 'string'),
                SourceColumn('STATE_PROVINCE_CD', 'string'),
                SourceColumn('STATE_PROVINCE_NAME', 'string'),
                SourceColumn('CITY_NAME', 'string'),
                SourceColumn('POSTAL_CD', 'string'),
                SourceColumn('POSTAL_FORMAT_CD', 'string'),
                SourceColumn('TIME_ZONE_CD', 'string'),
                SourceColumn('LATITUDE', 'decimal(9,6)'),
                SourceColumn('LONGITUDE', 'decimal(9,6)'),
            ),
            fileName='WWI_REF/GEOGRAPHY',
            timeoutSeconds=1800,
            derived=(
                DerivedColumn('PostalCode', 'upper(trim(POSTAL_CD))', 'string', legacyExpr='UPPER(TRIM(POSTAL_CD))'),
                DerivedColumn('CountryCode', 'upper(COUNTRY_CD)', 'string', legacyExpr='UPPER(COUNTRY_CD)'),
            ),
            legacyErrTable='err.RejectedConstraintViolation',
        ),
    ),
)

EXT_ORA_CODETRANSLATION = ExtractSpec(
    packageName='EXT_ORA_CodeTranslation',
    description='Reference refresh of WWI_REF.CODE_TRANSLATION - the cryptic legacy code map (status, reason, incoterm, payment method) that every downstream package joins to. Region-specific code sets are kept distinct rather than merged.',
    sourceSystemCode='ORA_ERP',
    legacyTargetTable='raw.OracleCustomerMaster',
    loadMode='delete_scope',
    ownership=RowOwnership("RecordKind = 'CODEXREF'", ('RecordKind',)),
    scopePredicate="RecordKind = 'CODEXREF'",
    constantColumns=(('RecordKind', 'CODEXREF'),),
    dependsOn=('EXT_ORA_CustomerMaster',),
    legacyTasks=('Expression:Init Batch Variables', 'ExecuteSql:Log Package Start', 'ExecuteSql:Delete Code Rows', 'DataFlowTask:Load Code Translations', 'ExecuteSql:Log Row Counts', 'ExecuteSql:Log Package Success'),
    sources=(
        SourceQuery(
            name='ORA CODE_TRANSLATION',
            oracleSql="""
SELECT  ct.CODE_SET_CD,
        ct.SOURCE_SYSTEM_CD,
        ct.SOURCE_CODE,
        WWI_REF.FN_TRANSLATE_CODE(ct.CODE_SET_CD, ct.SOURCE_CODE, ct.REGION_CD) AS TARGET_CODE,
        ct.CODE_DESC,
        ct.REGION_CD,
        ct.SORT_ORDER,
        ct.ACTIVE_FLG,
        ct.EFFECTIVE_FROM_DT,
        ct.LAST_UPDATE_DT
FROM    WWI_REF.CODE_TRANSLATION ct
WHERE   ct.CODE_SET_CD IN ('CUST_STATUS', 'PO_STATUS', 'REASON', 'INCOTERM', 'PAY_METHOD')
ORDER BY ct.CODE_SET_CD, ct.REGION_CD, ct.SORT_ORDER
""",
            bindOrder=(),
            columns=(
                SourceColumn('CODE_SET_CD', 'string'),
                SourceColumn('SOURCE_SYSTEM_CD', 'string'),
                SourceColumn('SOURCE_CODE', 'string'),
                SourceColumn('TARGET_CODE', 'string'),
                SourceColumn('CODE_DESC', 'string'),
                SourceColumn('REGION_CD', 'string'),
                SourceColumn('SORT_ORDER', 'int'),
                SourceColumn('ACTIVE_FLG', 'string'),
                SourceColumn('EFFECTIVE_FROM_DT', 'timestamp'),
                SourceColumn('LAST_UPDATE_DT', 'timestamp'),
            ),
            fileName='WWI_REF/CODE_TRANSLATION',
            timeoutSeconds=300,
        ),
    ),
)

PACKAGES = {
    'EXT_ORA_CustomerMaster': EXT_ORA_CUSTOMERMASTER,
    'EXT_ORA_CustomerAddress': EXT_ORA_CUSTOMERADDRESS,
    'EXT_ORA_SupplierMaster': EXT_ORA_SUPPLIERMASTER,
    'EXT_ORA_ProductMaster': EXT_ORA_PRODUCTMASTER,
    'EXT_ORA_ProductHierarchy': EXT_ORA_PRODUCTHIERARCHY,
    'EXT_ORA_PurchaseOrderHdr': EXT_ORA_PURCHASEORDERHDR,
    'EXT_ORA_PurchaseOrderLine': EXT_ORA_PURCHASEORDERLINE,
    'EXT_ORA_ReceiptLine': EXT_ORA_RECEIPTLINE,
    'EXT_ORA_VendorContract': EXT_ORA_VENDORCONTRACT,
    'EXT_ORA_ApInvoiceHdr': EXT_ORA_APINVOICEHDR,
    'EXT_ORA_ApInvoiceLine': EXT_ORA_APINVOICELINE,
    'EXT_ORA_ApPayment': EXT_ORA_APPAYMENT,
    'EXT_ORA_ApPaymentApply': EXT_ORA_APPAYMENTAPPLY,
    'EXT_ORA_ApAging': EXT_ORA_APAGING,
    'EXT_ORA_GlJournalLine': EXT_ORA_GLJOURNALLINE,
    'EXT_ORA_CostCenter': EXT_ORA_COSTCENTER,
    'EXT_ORA_TaxRate': EXT_ORA_TAXRATE,
    'EXT_ORA_PaymentTerms': EXT_ORA_PAYMENTTERMS,
    'EXT_ORA_Currency': EXT_ORA_CURRENCY,
    'EXT_ORA_FxRateDaily': EXT_ORA_FXRATEDAILY,
    'EXT_ORA_Geography': EXT_ORA_GEOGRAPHY,
    'EXT_ORA_CodeTranslation': EXT_ORA_CODETRANSLATION,
}

PACKAGE_ORDER = tuple(PACKAGES.keys())


def getSpec(packageName):
    return PACKAGES[packageName]
