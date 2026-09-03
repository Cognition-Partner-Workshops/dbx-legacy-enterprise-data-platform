/*
    raw.Oracle* landing tables (WWIGERP extracts)

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Deploy order  : 10
    Depends on    : sqlserver/control/01_schemas.sql
    Called by     : the EXT_ORA_* extract packages (OLE DB fast-load destinations)
                    and stg.usp_TruncateAndReload_* / stg.usp_AppendIncremental_*

    Landing shape only. Column names are the Oracle source column names, types
    are permissive (NVARCHAR for everything the source can emit badly) so that a
    bad character set, an overflowed NUMBER or a '00-JAN-00' date lands instead
    of failing the extract. Typing, trimming and validation happen on the way
    into stg.*, and anything that will not type is routed to err.*.

    No primary keys, no foreign keys, no check constraints: raw accepts whatever
    the source sent. The clustered index exists for load and for the
    BatchId-scoped reads the staging procedures do, not for integrity.
*/

IF OBJECT_ID(N'raw.OracleCustomerMaster', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleCustomerMaster
    (
        CUST_ID                 NVARCHAR(50)    NULL,
        CUST_NUMBER             NVARCHAR(50)    NULL,
        CUST_NAME               NVARCHAR(400)   NULL,
        CUST_LEGAL_NAME         NVARCHAR(400)   NULL,
        PARENT_CUST_ID          NVARCHAR(50)    NULL,
        CUST_TYPE_CD            NVARCHAR(20)    NULL,   -- ERP code set, not the OLTP one
        CUST_STATUS_CD          NVARCHAR(20)    NULL,   -- A / I / H / P / X
        CREDIT_LIMIT_AMT        NVARCHAR(50)    NULL,
        CREDIT_RATING_CD        NVARCHAR(10)    NULL,
        PAYMENT_TERMS_CD        NVARCHAR(20)    NULL,
        CURRENCY_CD             NVARCHAR(10)    NULL,
        TAX_REGISTRATION_NUM    NVARCHAR(50)    NULL,   -- VAT/GST number, NULL for most NA rows
        REGION_CD               NVARCHAR(10)    NULL,
        LEDGER_CD               NVARCHAR(20)    NULL,   -- NA01 / EU01 / EU02 / AP01
        SALES_REP_ID            NVARCHAR(50)    NULL,
        BUYING_GROUP_NAME       NVARCHAR(200)   NULL,
        MARKETING_CONSENT_FLG   NVARCHAR(5)     NULL,   -- Y/N/NULL; EU rows carry a date too
        CONSENT_DT              NVARCHAR(40)    NULL,
        RETENTION_CLASS_CD      NVARCHAR(20)    NULL,
        ACCOUNT_OPENED_DT       NVARCHAR(40)    NULL,
        LAST_ACTIVITY_DT        NVARCHAR(40)    NULL,
        CREATED_BY              NVARCHAR(100)   NULL,
        CREATED_DT              NVARCHAR(40)    NULL,
        LAST_UPDATE_BY          NVARCHAR(100)   NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleCustomerMaster_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleCustomerMaster_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleCustomerMaster_Batch
        ON raw.OracleCustomerMaster (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleCustomerAddress', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleCustomerAddress
    (
        ADDRESS_ID              NVARCHAR(50)    NULL,
        CUST_ID                 NVARCHAR(50)    NULL,
        ADDRESS_USAGE_CD        NVARCHAR(20)    NULL,   -- BILL / SHIP / STMT / LEGAL
        ADDRESS_LINE_1          NVARCHAR(400)   NULL,
        ADDRESS_LINE_2          NVARCHAR(400)   NULL,
        ADDRESS_LINE_3          NVARCHAR(400)   NULL,   -- APAC rows use line 3 as district
        CITY_NAME               NVARCHAR(200)   NULL,
        STATE_PROVINCE_CD       NVARCHAR(50)    NULL,
        POSTAL_CD               NVARCHAR(40)    NULL,   -- unformatted; NA 5 or 9 digit, EU alpha, APAC mixed
        COUNTRY_CD              NVARCHAR(10)    NULL,   -- ISO2 in newer rows, ISO3 in rows created before 2011
        PREFECTURE_NAME         NVARCHAR(200)   NULL,   -- JP only
        DELIVERY_INSTRUCTIONS   NVARCHAR(1000)  NULL,
        GEOCODE_LAT             NVARCHAR(50)    NULL,
        GEOCODE_LON             NVARCHAR(50)    NULL,
        PRIMARY_FLG             NVARCHAR(5)     NULL,
        VALID_FROM_DT           NVARCHAR(40)    NULL,
        VALID_TO_DT             NVARCHAR(40)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleCustomerAddress_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleCustomerAddress_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleCustomerAddress_Batch
        ON raw.OracleCustomerAddress (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleSupplierMaster', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleSupplierMaster
    (
        SUPP_ID                 NVARCHAR(50)    NULL,
        SUPP_NUMBER             NVARCHAR(50)    NULL,
        SUPP_NAME               NVARCHAR(400)   NULL,
        SUPP_SHORT_NAME         NVARCHAR(100)   NULL,
        SUPP_STATUS_CD          NVARCHAR(20)    NULL,
        SUPP_CATEGORY_CD        NVARCHAR(20)    NULL,
        DUNS_NUMBER             NVARCHAR(30)    NULL,
        TAX_ID_NUM              NVARCHAR(50)    NULL,
        WITHHOLDING_CD          NVARCHAR(20)    NULL,   -- NA 1099 handling only
        VAT_REGISTRATION_NUM    NVARCHAR(50)    NULL,   -- EU only
        GST_REGISTRATION_NUM    NVARCHAR(50)    NULL,   -- APAC only
        PAYMENT_TERMS_CD        NVARCHAR(20)    NULL,
        PAYMENT_METHOD_CD       NVARCHAR(20)    NULL,   -- CHK / ACH / SEPA / WIRE / BPAY
        CURRENCY_CD             NVARCHAR(10)    NULL,
        DEFAULT_INCOTERM_CD     NVARCHAR(10)    NULL,
        LEAD_TIME_DAYS          NVARCHAR(20)    NULL,
        MIN_ORDER_AMT           NVARCHAR(50)    NULL,
        SCORECARD_RATING        NVARCHAR(20)    NULL,
        DIVERSITY_CLASS_CD      NVARCHAR(20)    NULL,   -- NA supplier-diversity programme
        REGION_CD               NVARCHAR(10)    NULL,
        LEDGER_CD               NVARCHAR(20)    NULL,
        ON_HOLD_FLG             NVARCHAR(5)     NULL,
        HOLD_REASON_CD          NVARCHAR(20)    NULL,
        CREATED_DT              NVARCHAR(40)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleSupplierMaster_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleSupplierMaster_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleSupplierMaster_Batch
        ON raw.OracleSupplierMaster (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleProductMaster', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleProductMaster
    (
        PRODUCT_ID              NVARCHAR(50)    NULL,
        PRODUCT_CD              NVARCHAR(50)    NULL,
        PRODUCT_DESC            NVARCHAR(1000)  NULL,
        PRODUCT_LONG_DESC       NVARCHAR(MAX)   NULL,
        CATEGORY_CD             NVARCHAR(30)    NULL,
        SUB_CATEGORY_CD         NVARCHAR(30)    NULL,
        BRAND_CD                NVARCHAR(30)    NULL,
        BASE_UOM_CD             NVARCHAR(10)    NULL,   -- EA / CS / KG / LB / L / GAL
        SELL_UOM_CD             NVARCHAR(10)    NULL,
        UOM_CONVERSION_FACTOR   NVARCHAR(50)    NULL,
        STANDARD_COST_AMT       NVARCHAR(50)    NULL,
        STANDARD_COST_CURR_CD   NVARCHAR(10)    NULL,
        LIST_PRICE_AMT          NVARCHAR(50)    NULL,
        TAX_CLASS_CD            NVARCHAR(20)    NULL,   -- drives sales tax / VAT / GST class
        HAZMAT_CLASS_CD         NVARCHAR(20)    NULL,
        TEMPERATURE_CLASS_CD    NVARCHAR(20)    NULL,   -- ambient / chiller / frozen
        SHELF_LIFE_DAYS         NVARCHAR(20)    NULL,
        COUNTRY_OF_ORIGIN_CD    NVARCHAR(10)    NULL,
        HS_TARIFF_CD            NVARCHAR(30)    NULL,
        PRIMARY_SUPP_ID         NVARCHAR(50)    NULL,
        LIFECYCLE_STATUS_CD     NVARCHAR(20)    NULL,   -- NPI / ACT / DSC / OBS
        DISCONTINUED_DT         NVARCHAR(40)    NULL,
        WWI_STOCK_ITEM_ID       NVARCHAR(50)    NULL,   -- populated by MDM for ~80% of rows only
        CREATED_DT              NVARCHAR(40)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleProductMaster_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleProductMaster_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleProductMaster_Batch
        ON raw.OracleProductMaster (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OraclePurchaseOrderHdr', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OraclePurchaseOrderHdr
    (
        PO_HDR_ID               NVARCHAR(50)    NULL,
        PO_NUMBER               NVARCHAR(50)    NULL,
        PO_REVISION_NUM         NVARCHAR(20)    NULL,
        SUPP_ID                 NVARCHAR(50)    NULL,
        SUPP_SITE_ID            NVARCHAR(50)    NULL,
        BUYER_ID                NVARCHAR(50)    NULL,
        PO_STATUS_CD            NVARCHAR(20)    NULL,
        PO_TYPE_CD              NVARCHAR(20)    NULL,   -- STD / BLKT / RELEASE / CONSIGN
        ORDER_DT                NVARCHAR(40)    NULL,
        NEED_BY_DT              NVARCHAR(40)    NULL,
        PROMISED_DT             NVARCHAR(40)    NULL,
        CURRENCY_CD             NVARCHAR(10)    NULL,
        FX_RATE                 NVARCHAR(50)    NULL,   -- rate captured at PO time, not reload
        FX_RATE_TYPE_CD         NVARCHAR(20)    NULL,
        PO_TOTAL_AMT            NVARCHAR(50)    NULL,
        TAX_TOTAL_AMT           NVARCHAR(50)    NULL,
        FREIGHT_AMT             NVARCHAR(50)    NULL,
        INCOTERM_CD             NVARCHAR(10)    NULL,
        SHIP_TO_SITE_CD         NVARCHAR(30)    NULL,
        COST_CENTER_CD          NVARCHAR(30)    NULL,
        CONTRACT_ID             NVARCHAR(50)    NULL,
        LEDGER_CD               NVARCHAR(20)    NULL,
        REGION_CD               NVARCHAR(10)    NULL,
        APPROVED_BY             NVARCHAR(100)   NULL,
        APPROVED_DT             NVARCHAR(40)    NULL,
        CANCELLED_FLG           NVARCHAR(5)     NULL,
        CANCEL_REASON_CD        NVARCHAR(20)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOraclePurchaseOrderHdr_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOraclePurchaseOrderHdr_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOraclePurchaseOrderHdr_Batch
        ON raw.OraclePurchaseOrderHdr (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OraclePurchaseOrderLine', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OraclePurchaseOrderLine
    (
        PO_LINE_ID              NVARCHAR(50)    NULL,
        PO_HDR_ID               NVARCHAR(50)    NULL,
        PO_NUMBER               NVARCHAR(50)    NULL,
        LINE_NUM                NVARCHAR(20)    NULL,
        SHIPMENT_NUM            NVARCHAR(20)    NULL,
        PRODUCT_ID              NVARCHAR(50)    NULL,
        SUPP_ITEM_CD            NVARCHAR(50)    NULL,
        LINE_DESC               NVARCHAR(1000)  NULL,
        ORDER_QTY               NVARCHAR(50)    NULL,
        ORDER_UOM_CD            NVARCHAR(10)    NULL,
        UNIT_PRICE_AMT          NVARCHAR(50)    NULL,
        EXTENDED_AMT            NVARCHAR(50)    NULL,
        DISCOUNT_PCT            NVARCHAR(50)    NULL,
        TAX_CD                  NVARCHAR(20)    NULL,
        TAX_AMT                 NVARCHAR(50)    NULL,
        RECEIVED_QTY            NVARCHAR(50)    NULL,
        BILLED_QTY              NVARCHAR(50)    NULL,
        CANCELLED_QTY           NVARCHAR(50)    NULL,
        LINE_STATUS_CD          NVARCHAR(20)    NULL,
        NEED_BY_DT              NVARCHAR(40)    NULL,
        CLOSED_DT               NVARCHAR(40)    NULL,
        COST_CENTER_CD          NVARCHAR(30)    NULL,
        GL_ACCOUNT_CD           NVARCHAR(40)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOraclePurchaseOrderLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOraclePurchaseOrderLine_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOraclePurchaseOrderLine_Batch
        ON raw.OraclePurchaseOrderLine (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleReceiptLine', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleReceiptLine
    (
        RECEIPT_LINE_ID         NVARCHAR(50)    NULL,
        RECEIPT_HDR_ID          NVARCHAR(50)    NULL,
        RECEIPT_NUM             NVARCHAR(50)    NULL,
        PO_LINE_ID              NVARCHAR(50)    NULL,
        PO_NUMBER               NVARCHAR(50)    NULL,
        PRODUCT_ID              NVARCHAR(50)    NULL,
        RECEIVED_QTY            NVARCHAR(50)    NULL,
        RECEIVED_UOM_CD         NVARCHAR(10)    NULL,
        ACCEPTED_QTY            NVARCHAR(50)    NULL,
        REJECTED_QTY            NVARCHAR(50)    NULL,
        REJECT_REASON_CD        NVARCHAR(20)    NULL,
        LOT_NUMBER              NVARCHAR(50)    NULL,
        RECEIPT_DT              NVARCHAR(40)    NULL,
        INSPECTION_DT           NVARCHAR(40)    NULL,
        INSPECTION_RESULT_CD    NVARCHAR(20)    NULL,
        WAREHOUSE_CD            NVARCHAR(30)    NULL,
        BIN_CD                  NVARCHAR(30)    NULL,
        RECEIVER_ID             NVARCHAR(50)    NULL,
        LANDED_COST_AMT         NVARCHAR(50)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleReceiptLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleReceiptLine_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleReceiptLine_Batch
        ON raw.OracleReceiptLine (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleApInvoiceHdr', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleApInvoiceHdr
    (
        AP_INVOICE_ID           NVARCHAR(50)    NULL,
        INVOICE_NUM             NVARCHAR(50)    NULL,
        SUPP_ID                 NVARCHAR(50)    NULL,
        INVOICE_TYPE_CD         NVARCHAR(20)    NULL,   -- STD / CRDT / DEBIT / PREPAY / EXPENSE
        INVOICE_DT              NVARCHAR(40)    NULL,
        GL_DATE                 NVARCHAR(40)    NULL,
        DUE_DT                  NVARCHAR(40)    NULL,
        PAYMENT_TERMS_CD        NVARCHAR(20)    NULL,
        CURRENCY_CD             NVARCHAR(10)    NULL,
        FX_RATE                 NVARCHAR(50)    NULL,
        INVOICE_AMT             NVARCHAR(50)    NULL,
        TAX_AMT                 NVARCHAR(50)    NULL,
        WITHHOLDING_AMT         NVARCHAR(50)    NULL,   -- NA only
        VAT_RECOVERABLE_AMT     NVARCHAR(50)    NULL,   -- EU only
        GST_INPUT_CREDIT_AMT    NVARCHAR(50)    NULL,   -- APAC only
        AMOUNT_PAID             NVARCHAR(50)    NULL,
        INVOICE_STATUS_CD       NVARCHAR(20)    NULL,
        HOLD_FLG                NVARCHAR(5)     NULL,
        HOLD_REASON_CD          NVARCHAR(20)    NULL,
        MATCH_TYPE_CD           NVARCHAR(20)    NULL,   -- 2WAY / 3WAY / 4WAY / NONE
        PO_NUMBER               NVARCHAR(50)    NULL,
        LEDGER_CD               NVARCHAR(20)    NULL,
        REGION_CD               NVARCHAR(10)    NULL,
        PERIOD_NAME             NVARCHAR(20)    NULL,   -- ERP fiscal period label, e.g. FY24-P07
        CREATED_DT              NVARCHAR(40)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleApInvoiceHdr_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleApInvoiceHdr_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleApInvoiceHdr_Batch
        ON raw.OracleApInvoiceHdr (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleApInvoiceLine', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleApInvoiceLine
    (
        AP_INVOICE_LINE_ID      NVARCHAR(50)    NULL,
        AP_INVOICE_ID           NVARCHAR(50)    NULL,
        INVOICE_NUM             NVARCHAR(50)    NULL,
        LINE_NUM                NVARCHAR(20)    NULL,
        LINE_TYPE_CD            NVARCHAR(20)    NULL,   -- ITEM / FREIGHT / TAX / MISC / RETAINAGE
        PO_LINE_ID              NVARCHAR(50)    NULL,
        RECEIPT_LINE_ID         NVARCHAR(50)    NULL,
        PRODUCT_ID              NVARCHAR(50)    NULL,
        LINE_DESC               NVARCHAR(1000)  NULL,
        QUANTITY                NVARCHAR(50)    NULL,
        UOM_CD                  NVARCHAR(10)    NULL,
        UNIT_PRICE_AMT          NVARCHAR(50)    NULL,
        LINE_AMT                NVARCHAR(50)    NULL,
        TAX_CD                  NVARCHAR(20)    NULL,
        TAX_AMT                 NVARCHAR(50)    NULL,
        TAX_JURISDICTION_CD     NVARCHAR(30)    NULL,
        GL_ACCOUNT_CD           NVARCHAR(40)    NULL,
        COST_CENTER_CD          NVARCHAR(30)    NULL,
        PROJECT_CD              NVARCHAR(30)    NULL,
        ACCRUAL_FLG             NVARCHAR(5)     NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleApInvoiceLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleApInvoiceLine_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleApInvoiceLine_Batch
        ON raw.OracleApInvoiceLine (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleApPayment', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleApPayment
    (
        PAYMENT_ID              NVARCHAR(50)    NULL,
        PAYMENT_NUM             NVARCHAR(50)    NULL,
        SUPP_ID                 NVARCHAR(50)    NULL,
        PAYMENT_METHOD_CD       NVARCHAR(20)    NULL,   -- CHK / ACH / SEPA / WIRE / BPAY / BACS
        PAYMENT_DT              NVARCHAR(40)    NULL,
        CLEARED_DT              NVARCHAR(40)    NULL,
        VOID_DT                 NVARCHAR(40)    NULL,
        PAYMENT_STATUS_CD       NVARCHAR(20)    NULL,
        CURRENCY_CD             NVARCHAR(10)    NULL,
        FX_RATE                 NVARCHAR(50)    NULL,
        PAYMENT_AMT             NVARCHAR(50)    NULL,
        DISCOUNT_TAKEN_AMT      NVARCHAR(50)    NULL,
        BANK_ACCOUNT_REF        NVARCHAR(50)    NULL,   -- internal account reference, never the number
        REMITTANCE_REF          NVARCHAR(100)   NULL,
        APPLIED_INVOICE_NUMS    NVARCHAR(MAX)   NULL,   -- pipe-delimited list; exploded during matching
        LEDGER_CD               NVARCHAR(20)    NULL,
        REGION_CD               NVARCHAR(10)    NULL,
        CREATED_DT              NVARCHAR(40)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleApPayment_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleApPayment_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleApPayment_Batch
        ON raw.OracleApPayment (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleGlJournalLine', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleGlJournalLine
    (
        JOURNAL_LINE_ID         NVARCHAR(50)    NULL,
        JOURNAL_HDR_ID          NVARCHAR(50)    NULL,
        JOURNAL_NAME            NVARCHAR(200)   NULL,
        JOURNAL_SOURCE_CD       NVARCHAR(20)    NULL,   -- AP / AR / INV / MANUAL / INTERFACE
        JOURNAL_CATEGORY_CD     NVARCHAR(20)    NULL,
        LINE_NUM                NVARCHAR(20)    NULL,
        LEDGER_CD               NVARCHAR(20)    NULL,
        PERIOD_NAME             NVARCHAR(20)    NULL,
        EFFECTIVE_DT            NVARCHAR(40)    NULL,
        GL_ACCOUNT_CD           NVARCHAR(40)    NULL,   -- concatenated segment string, dash separated
        COST_CENTER_CD          NVARCHAR(30)    NULL,
        PROJECT_CD              NVARCHAR(30)    NULL,
        INTERCOMPANY_CD         NVARCHAR(20)    NULL,
        ENTERED_DR_AMT          NVARCHAR(50)    NULL,
        ENTERED_CR_AMT          NVARCHAR(50)    NULL,
        ACCOUNTED_DR_AMT        NVARCHAR(50)    NULL,
        ACCOUNTED_CR_AMT        NVARCHAR(50)    NULL,
        CURRENCY_CD             NVARCHAR(10)    NULL,
        FX_RATE                 NVARCHAR(50)    NULL,
        STATISTICAL_AMT         NVARCHAR(50)    NULL,
        LINE_DESC               NVARCHAR(1000)  NULL,
        REVERSAL_FLG            NVARCHAR(5)     NULL,
        POSTED_FLG              NVARCHAR(5)     NULL,
        POSTED_DT               NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleGlJournalLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleGlJournalLine_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleGlJournalLine_Batch
        ON raw.OracleGlJournalLine (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleCostCenter', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleCostCenter
    (
        COST_CENTER_CD          NVARCHAR(30)    NULL,
        COST_CENTER_NAME        NVARCHAR(200)   NULL,
        PARENT_COST_CENTER_CD   NVARCHAR(30)    NULL,
        HIERARCHY_LEVEL_NUM     NVARCHAR(10)    NULL,
        COMPANY_CD              NVARCHAR(20)    NULL,
        LEDGER_CD               NVARCHAR(20)    NULL,
        REGION_CD               NVARCHAR(10)    NULL,
        MANAGER_EMPLOYEE_ID     NVARCHAR(50)    NULL,
        FUNCTIONAL_AREA_CD      NVARCHAR(20)    NULL,
        ACTIVE_FLG              NVARCHAR(5)     NULL,
        EFFECTIVE_FROM_DT       NVARCHAR(40)    NULL,
        EFFECTIVE_TO_DT         NVARCHAR(40)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleCostCenter_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleCostCenter_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleCostCenter_Batch
        ON raw.OracleCostCenter (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleCurrency', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleCurrency
    (
        CURRENCY_CD             NVARCHAR(10)    NULL,
        CURRENCY_NAME           NVARCHAR(100)   NULL,
        CURRENCY_SYMBOL         NVARCHAR(10)    NULL,
        MINOR_UNIT_DIGITS       NVARCHAR(10)    NULL,
        ISO_NUMERIC_CD          NVARCHAR(10)    NULL,
        ACTIVE_FLG              NVARCHAR(5)     NULL,
        EURO_LEGACY_FLG         NVARCHAR(5)     NULL,   -- DEM/FRF/ITL rows still present since 1999
        LEGACY_FIXED_RATE       NVARCHAR(50)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleCurrency_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleCurrency_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleCurrency_Batch
        ON raw.OracleCurrency (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleFxRate', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleFxRate
    (
        FROM_CURRENCY_CD        NVARCHAR(10)    NULL,
        TO_CURRENCY_CD          NVARCHAR(10)    NULL,
        RATE_DT                 NVARCHAR(40)    NULL,
        RATE_TYPE_CD            NVARCHAR(20)    NULL,   -- SPOT / CORPORATE / MONTH_END / BUDGET
        CONVERSION_RATE         NVARCHAR(50)    NULL,
        INVERSE_RATE            NVARCHAR(50)    NULL,
        RATE_SOURCE_CD          NVARCHAR(20)    NULL,
        LEDGER_CD               NVARCHAR(20)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleFxRate_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleFxRate_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleFxRate_Batch
        ON raw.OracleFxRate (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleTaxRate', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleTaxRate
    (
        TAX_RATE_ID             NVARCHAR(50)    NULL,
        TAX_CD                  NVARCHAR(20)    NULL,
        TAX_REGIME_CD           NVARCHAR(20)    NULL,   -- SALESTAX / VAT / GST / CONSUMPTION
        TAX_JURISDICTION_CD     NVARCHAR(30)    NULL,
        COUNTRY_CD              NVARCHAR(10)    NULL,
        STATE_PROVINCE_CD       NVARCHAR(50)    NULL,
        TAX_CLASS_CD            NVARCHAR(20)    NULL,
        RATE_PCT                NVARCHAR(50)    NULL,
        COMPOUND_FLG            NVARCHAR(5)     NULL,   -- CA GST+PST compounding
        RECOVERABLE_PCT         NVARCHAR(50)    NULL,   -- EU input VAT recovery
        REVERSE_CHARGE_FLG      NVARCHAR(5)     NULL,   -- EU cross-border
        EFFECTIVE_FROM_DT       NVARCHAR(40)    NULL,
        EFFECTIVE_TO_DT         NVARCHAR(40)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleTaxRate_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleTaxRate_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleTaxRate_Batch
        ON raw.OracleTaxRate (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OraclePaymentTerms', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OraclePaymentTerms
    (
        PAYMENT_TERMS_CD        NVARCHAR(20)    NULL,
        PAYMENT_TERMS_NAME      NVARCHAR(100)   NULL,
        NET_DAYS                NVARCHAR(20)    NULL,
        DISCOUNT_DAYS           NVARCHAR(20)    NULL,
        DISCOUNT_PCT            NVARCHAR(50)    NULL,
        DAY_OF_MONTH_DUE        NVARCHAR(20)    NULL,   -- EU end-of-month terms
        INSTALMENT_COUNT        NVARCHAR(20)    NULL,
        CALCULATION_BASIS_CD    NVARCHAR(20)    NULL,   -- INVOICE_DT / GL_DATE / RECEIPT_DT
        REGION_CD               NVARCHAR(10)    NULL,
        ACTIVE_FLG              NVARCHAR(5)     NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOraclePaymentTerms_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOraclePaymentTerms_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOraclePaymentTerms_Batch
        ON raw.OraclePaymentTerms (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleGeography', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleGeography
    (
        GEOGRAPHY_ID            NVARCHAR(50)    NULL,
        COUNTRY_CD              NVARCHAR(10)    NULL,
        COUNTRY_NAME            NVARCHAR(200)   NULL,
        ISO3_CD                 NVARCHAR(10)    NULL,
        REGION_CD               NVARCHAR(10)    NULL,
        SUB_REGION_NAME         NVARCHAR(200)   NULL,
        STATE_PROVINCE_CD       NVARCHAR(50)    NULL,
        STATE_PROVINCE_NAME     NVARCHAR(200)   NULL,
        CITY_NAME               NVARCHAR(200)   NULL,
        POSTAL_CD               NVARCHAR(40)    NULL,
        POSTAL_FORMAT_MASK      NVARCHAR(50)    NULL,   -- NNNNN, ANA NAN, NNN-NNNN ...
        TIMEZONE_NAME           NVARCHAR(50)    NULL,
        CURRENCY_CD             NVARCHAR(10)    NULL,
        TAX_JURISDICTION_CD     NVARCHAR(30)    NULL,
        POPULATION_NUM          NVARCHAR(30)    NULL,
        LATITUDE                NVARCHAR(50)    NULL,
        LONGITUDE               NVARCHAR(50)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleGeography_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleGeography_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleGeography_Batch
        ON raw.OracleGeography (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.OracleVendorContract', N'U') IS NULL
BEGIN
    CREATE TABLE raw.OracleVendorContract
    (
        CONTRACT_ID             NVARCHAR(50)    NULL,
        CONTRACT_NUM            NVARCHAR(50)    NULL,
        SUPP_ID                 NVARCHAR(50)    NULL,
        CONTRACT_TYPE_CD        NVARCHAR(20)    NULL,   -- BLANKET / RATE / VOLUME / REBATE
        CONTRACT_STATUS_CD      NVARCHAR(20)    NULL,
        START_DT                NVARCHAR(40)    NULL,
        END_DT                  NVARCHAR(40)    NULL,
        AUTO_RENEW_FLG          NVARCHAR(5)     NULL,
        NOTICE_PERIOD_DAYS      NVARCHAR(20)    NULL,
        COMMITTED_AMT           NVARCHAR(50)    NULL,
        CONSUMED_AMT            NVARCHAR(50)    NULL,
        CURRENCY_CD             NVARCHAR(10)    NULL,
        REBATE_TIER_JSON        NVARCHAR(MAX)   NULL,   -- free-text tier grid, parsed downstream
        GOVERNING_LAW_CD        NVARCHAR(20)    NULL,
        REGION_CD               NVARCHAR(10)    NULL,
        SIGNED_DT               NVARCHAR(40)    NULL,
        LAST_UPDATE_DT          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawOracleVendorContract_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawOracleVendorContract_Source DEFAULT (N'ORA_ERP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawOracleVendorContract_Batch
        ON raw.OracleVendorContract (BatchId, SourceRowNumber);
END;
GO
