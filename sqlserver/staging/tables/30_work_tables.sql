/*
    work.* scratch and intermediate tables

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Deploy order  : 30
    Depends on    : 20_stg_tables_master.sql, 21_stg_tables_finance.sql,
                    22_stg_tables_sales.sql
    Written by    : work.usp_BuildProductCrosswalk, work.usp_BuildInventoryPositionDaily,
                    work.usp_MatchPaymentsToInvoices, work.usp_QueueLateArrivingDimensions,
                    stg.usp_DeduplicateCustomer, stg.usp_DeduplicateOrderLine

    These are not truncated at the start of every run. The multi-step loads leave
    their working sets behind on purpose so that a failed batch can be inspected
    the next morning, and the housekeeping job removes anything older than the
    retention window in etl.Configuration. Rows are always keyed by BatchId.
*/

IF OBJECT_ID(N'work.CustomerDedup', N'U') IS NULL
BEGIN
    CREATE TABLE work.CustomerDedup
    (
        WorkRowId               BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        DuplicateGroupId        BIGINT          NOT NULL,
        CandidateCustomerBusinessKey NVARCHAR(100) NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SourceCustomerId        NVARCHAR(50)    NULL,
        MatchKeyName            NVARCHAR(200)   NULL,       -- upper-cased, punctuation and suffix stripped
        MatchKeyPostal          NVARCHAR(20)    NULL,
        MatchKeyTaxNumber       NVARCHAR(50)    NULL,
        MatchRuleCode           NVARCHAR(30)    NOT NULL,   -- EXACT_TAXNUM / NAME_POSTAL / NAME_FUZZY
        MatchScore              DECIMAL(5,2)    NULL,
        SurvivorshipScore       DECIMAL(9,4)    NULL,       -- completeness + recency + source rank
        SourceRank              SMALLINT        NULL,       -- ORA_ERP beats WWI_OLTP beats WWI_WEB
        AttributeCompleteness   SMALLINT        NULL,
        SourceModifiedDate      DATETIME2(3)    NULL,
        IsSelectedSurvivor      BIT             NOT NULL CONSTRAINT DF_workCustomerDedup_Survivor DEFAULT (0),
        LosesToBusinessKey      NVARCHAR(100)   NULL,
        DecisionNote            NVARCHAR(400)   NULL,
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_workCustomerDedup_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_workCustomerDedup PRIMARY KEY CLUSTERED (WorkRowId)
    );

    CREATE INDEX IX_workCustomerDedup_Group ON work.CustomerDedup (BatchId, DuplicateGroupId);
END;
GO

IF OBJECT_ID(N'work.CustomerAddressStandardized', N'U') IS NULL
BEGIN
    CREATE TABLE work.CustomerAddressStandardized
    (
        WorkRowId               BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        AddressBusinessKey      NVARCHAR(120)   NOT NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        RuleSetCode             NVARCHAR(30)    NOT NULL,   -- NA_USPS / EU_COUNTRY / APAC_LOCAL
        InputAddressLine1       NVARCHAR(200)   NULL,
        InputCityName           NVARCHAR(100)   NULL,
        InputPostalCode         NVARCHAR(40)    NULL,
        InputCountryCode        NVARCHAR(10)    NULL,
        OutputAddressLine1      NVARCHAR(200)   NULL,
        OutputCityName          NVARCHAR(100)   NULL,
        OutputStateProvinceCode NVARCHAR(20)    NULL,
        OutputPostalCode        NVARCHAR(20)    NULL,
        OutputCountryCode       NCHAR(2)        NULL,
        PostalMaskApplied       NVARCHAR(30)    NULL,
        StandardizationStatusCode NVARCHAR(20)  NOT NULL,   -- CLEAN / CORRECTED / UNPARSED
        ChangedComponentList    NVARCHAR(200)   NULL,
        GeographyBusinessKey    NVARCHAR(120)   NULL,
        GeographyMatchLevel     NVARCHAR(20)    NULL,
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_workCustomerAddressStd_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_workCustomerAddressStandardized PRIMARY KEY CLUSTERED (WorkRowId)
    );

    CREATE INDEX IX_workCustomerAddressStd_Batch ON work.CustomerAddressStandardized (BatchId, StandardizationStatusCode);
END;
GO

IF OBJECT_ID(N'work.SupplierDedup', N'U') IS NULL
BEGIN
    CREATE TABLE work.SupplierDedup
    (
        WorkRowId               BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        DuplicateGroupId        BIGINT          NOT NULL,
        CandidateSupplierBusinessKey NVARCHAR(100) NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        MatchKeyName            NVARCHAR(200)   NULL,
        MatchKeyTaxIdentifier   NVARCHAR(50)    NULL,
        MatchKeyDuns            NVARCHAR(20)    NULL,
        MatchRuleCode           NVARCHAR(30)    NOT NULL,   -- DUNS / TAXID / NAME_COUNTRY
        HasOpenPurchaseOrders   BIT             NULL,       -- a supplier with open POs never loses a merge
        HasOpenInvoices         BIT             NULL,
        SurvivorshipScore       DECIMAL(9,4)    NULL,
        IsSelectedSurvivor      BIT             NOT NULL CONSTRAINT DF_workSupplierDedup_Survivor DEFAULT (0),
        LosesToBusinessKey      NVARCHAR(100)   NULL,
        DecisionNote            NVARCHAR(400)   NULL,
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_workSupplierDedup_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_workSupplierDedup PRIMARY KEY CLUSTERED (WorkRowId)
    );
END;
GO

IF OBJECT_ID(N'work.ProductCrosswalk', N'U') IS NULL
BEGIN
    CREATE TABLE work.ProductCrosswalk
    (
        WorkRowId               BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        ErpProductCode          NVARCHAR(50)    NULL,
        ErpProductBusinessKey   NVARCHAR(100)   NULL,
        OltpStockItemId         INT             NULL,
        StockItemBusinessKey    NVARCHAR(100)   NULL,
        PartnerProductCode      NVARCHAR(60)    NULL,
        Barcode                 NVARCHAR(30)    NULL,
        MatchMethodCode         NVARCHAR(20)    NOT NULL,   -- BARCODE / MANUAL_XREF / NAME / UNMATCHED
        MatchConfidence         DECIMAL(5,2)    NULL,
        NormalizedName          NVARCHAR(200)   NULL,
        NameTokenOverlapPercent DECIMAL(5,2)    NULL,
        IsAmbiguous             BIT             NOT NULL CONSTRAINT DF_workProductCrosswalk_Ambiguous DEFAULT (0),
        CandidateCount          INT             NULL,
        ResolvedFlag            BIT             NOT NULL CONSTRAINT DF_workProductCrosswalk_Resolved DEFAULT (0),
        ReviewedByName          NVARCHAR(100)   NULL,       -- populated by the manual xref maintenance screen
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_workProductCrosswalk_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_workProductCrosswalk PRIMARY KEY CLUSTERED (WorkRowId)
    );

    CREATE INDEX IX_workProductCrosswalk_Erp ON work.ProductCrosswalk (BatchId, ErpProductCode);
    CREATE INDEX IX_workProductCrosswalk_Barcode ON work.ProductCrosswalk (Barcode) WHERE Barcode IS NOT NULL;
END;
GO

IF OBJECT_ID(N'work.OrderLineEnriched', N'U') IS NULL
BEGIN
    CREATE TABLE work.OrderLineEnriched
    (
        WorkRowId               BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        OrderLineBusinessKey    NVARCHAR(120)   NOT NULL,
        OrderBusinessKey        NVARCHAR(100)   NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        StockItemBusinessKey    NVARCHAR(100)   NULL,
        SalespersonBusinessKey  NVARCHAR(100)   NULL,
        PromotionBusinessKey    NVARCHAR(100)   NULL,
        GeographyBusinessKey    NVARCHAR(120)   NULL,
        RegionCode              NVARCHAR(10)    NULL,
        OrderDate               DATE            NULL,
        OrderedQuantity         DECIMAL(18,4)   NULL,
        NetLineAmount           DECIMAL(19,4)   NULL,
        NetLineAmountUsd        DECIMAL(19,4)   NULL,
        TaxRegimeCode           NVARCHAR(20)    NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        LookupFailureList       NVARCHAR(400)   NULL,       -- comma separated dimension names that missed
        LookupFailureCount      SMALLINT        NOT NULL CONSTRAINT DF_workOrderLineEnriched_Failures DEFAULT (0),
        IsReadyForFact          BIT             NOT NULL CONSTRAINT DF_workOrderLineEnriched_Ready DEFAULT (0),
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_workOrderLineEnriched_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_workOrderLineEnriched PRIMARY KEY CLUSTERED (WorkRowId)
    );

    CREATE INDEX IX_workOrderLineEnriched_Ready ON work.OrderLineEnriched (BatchId, IsReadyForFact);
END;
GO

IF OBJECT_ID(N'work.SaleLineEnriched', N'U') IS NULL
BEGIN
    CREATE TABLE work.SaleLineEnriched
    (
        WorkRowId               BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        SaleLineBusinessKey     NVARCHAR(120)   NOT NULL,
        SaleBusinessKey         NVARCHAR(100)   NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        BillToCustomerBusinessKey NVARCHAR(100) NULL,
        StockItemBusinessKey    NVARCHAR(100)   NULL,
        SalespersonBusinessKey  NVARCHAR(100)   NULL,
        GeographyBusinessKey    NVARCHAR(120)   NULL,
        InvoiceDate             DATE            NULL,
        RegionCode              NVARCHAR(10)    NULL,
        Quantity                DECIMAL(18,4)   NULL,
        NetLineAmount           DECIMAL(19,4)   NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        GrossLineAmount         DECIMAL(19,4)   NULL,
        NetLineAmountUsd        DECIMAL(19,4)   NULL,
        LineProfitAmount        DECIMAL(19,4)   NULL,
        CommissionAmount        DECIMAL(19,4)   NULL,
        MarginPercent           DECIMAL(9,4)    NULL,
        MarginOutlierFlag       BIT             NULL,       -- outside the tolerance in etl.Configuration
        LookupFailureList       NVARCHAR(400)   NULL,
        IsReadyForFact          BIT             NOT NULL CONSTRAINT DF_workSaleLineEnriched_Ready DEFAULT (0),
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_workSaleLineEnriched_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_workSaleLineEnriched PRIMARY KEY CLUSTERED (WorkRowId)
    );

    CREATE INDEX IX_workSaleLineEnriched_Ready ON work.SaleLineEnriched (BatchId, IsReadyForFact);
END;
GO

IF OBJECT_ID(N'work.PurchaseLineEnriched', N'U') IS NULL
BEGIN
    CREATE TABLE work.PurchaseLineEnriched
    (
        WorkRowId               BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        PurchaseOrderLineBusinessKey NVARCHAR(120) NOT NULL,
        PurchaseOrderBusinessKey NVARCHAR(100)  NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        ProductBusinessKey      NVARCHAR(100)   NULL,
        CostCenterCode          NVARCHAR(30)    NULL,
        ContractBusinessKey     NVARCHAR(100)   NULL,
        LedgerCode              NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        OrderDate               DATE            NULL,
        OrderQuantityBaseUom    DECIMAL(18,4)   NULL,
        ExtendedAmountUsd       DECIMAL(19,4)   NULL,
        RecoverableTaxAmount    DECIMAL(19,4)   NULL,
        ReceiptedQuantity       DECIMAL(18,4)   NULL,
        InvoicedQuantity        DECIMAL(18,4)   NULL,
        ThreeWayMatchStatusCode NVARCHAR(20)    NULL,
        PriceVariancePercent    DECIMAL(9,4)    NULL,
        ContractComplianceFlag  BIT             NULL,       -- off-contract spend lands here for reporting
        LookupFailureList       NVARCHAR(400)   NULL,
        IsReadyForFact          BIT             NOT NULL CONSTRAINT DF_workPurchaseLineEnriched_Ready DEFAULT (0),
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_workPurchaseLineEnriched_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_workPurchaseLineEnriched PRIMARY KEY CLUSTERED (WorkRowId)
    );
END;
GO

IF OBJECT_ID(N'work.PaymentMatched', N'U') IS NULL
BEGIN
    CREATE TABLE work.PaymentMatched
    (
        WorkRowId               BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        PaymentBusinessKey      NVARCHAR(100)   NOT NULL,
        ApInvoiceBusinessKey    NVARCHAR(100)   NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        MatchPassNumber         SMALLINT        NOT NULL,   -- 1 remittance ref, 2 exact amount, 3 residual
        MatchRuleCode           NVARCHAR(30)    NOT NULL,
        AppliedAmount           DECIMAL(19,4)   NULL,
        AppliedAmountUsd        DECIMAL(19,4)   NULL,
        DiscountTakenAmount     DECIMAL(19,4)   NULL,
        FxDifferenceUsd         DECIMAL(19,4)   NULL,
        ResidualAmount          DECIMAL(19,4)   NULL,
        WithinToleranceFlag     BIT             NULL,
        MatchConfidence         DECIMAL(5,2)    NULL,
        IsFinalAllocation       BIT             NOT NULL CONSTRAINT DF_workPaymentMatched_Final DEFAULT (0),
        UnmatchedReasonCode     NVARCHAR(30)    NULL,
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_workPaymentMatched_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_workPaymentMatched PRIMARY KEY CLUSTERED (WorkRowId)
    );

    CREATE INDEX IX_workPaymentMatched_Payment ON work.PaymentMatched (BatchId, PaymentBusinessKey);
END;
GO

IF OBJECT_ID(N'work.InventoryPositionDaily', N'U') IS NULL
BEGIN
    CREATE TABLE work.InventoryPositionDaily
    (
        WorkRowId               BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        PositionDate            DATE            NOT NULL,
        StockItemBusinessKey    NVARCHAR(100)   NOT NULL,
        WarehouseCode           NVARCHAR(30)    NOT NULL,
        OpeningQuantity         DECIMAL(18,4)   NULL,
        ReceiptQuantity         DECIMAL(18,4)   NULL,
        IssueQuantity           DECIMAL(18,4)   NULL,
        AdjustmentQuantity      DECIMAL(18,4)   NULL,
        TransferInQuantity      DECIMAL(18,4)   NULL,
        TransferOutQuantity     DECIMAL(18,4)   NULL,
        ClosingQuantity         DECIMAL(18,4)   NULL,
        ClosingValueUsd         DECIMAL(19,4)   NULL,
        AverageUnitCostUsd      DECIMAL(19,6)   NULL,
        DaysOfCoverEstimate     DECIMAL(9,2)    NULL,
        NegativeBalanceFlag     BIT             NOT NULL CONSTRAINT DF_workInventoryPositionDaily_Negative DEFAULT (0),
        RollForwardBrokenFlag   BIT             NOT NULL CONSTRAINT DF_workInventoryPositionDaily_Broken DEFAULT (0),
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_workInventoryPositionDaily_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_workInventoryPositionDaily PRIMARY KEY CLUSTERED (WorkRowId)
    );

    CREATE INDEX IX_workInventoryPositionDaily_Key
        ON work.InventoryPositionDaily (PositionDate, StockItemBusinessKey, WarehouseCode);
END;
GO

IF OBJECT_ID(N'work.CurrencyConversionScratch', N'U') IS NULL
BEGIN
    CREATE TABLE work.CurrencyConversionScratch
    (
        WorkRowId               BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        TargetObjectName        NVARCHAR(200)   NOT NULL,   -- the stg table the amount belongs to
        TargetBusinessKey       NVARCHAR(140)   NOT NULL,
        AmountColumnName        NVARCHAR(100)   NOT NULL,
        FromCurrencyCode        NCHAR(3)        NULL,
        ToCurrencyCode          NCHAR(3)        NULL,
        RateTypeCode            NVARCHAR(20)    NULL,
        RequestedRateDate       DATE            NULL,
        AppliedRateDate         DATE            NULL,       -- falls back to the most recent prior rate
        ConversionRate          DECIMAL(19,8)   NULL,
        SourceAmount            DECIMAL(19,4)   NULL,
        ConvertedAmount         DECIMAL(19,4)   NULL,
        RateResolutionCode      NVARCHAR(20)    NULL,       -- EXACT / PRIOR_DAY / OVERRIDE / MISSING
        FallbackDaysUsed        SMALLINT        NULL,
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_workCurrencyConversionScratch_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_workCurrencyConversionScratch PRIMARY KEY CLUSTERED (WorkRowId)
    );

    CREATE INDEX IX_workCurrencyConversionScratch_Batch
        ON work.CurrencyConversionScratch (BatchId, TargetObjectName);
END;
GO

IF OBJECT_ID(N'work.LateArrivingDimensionQueue', N'U') IS NULL
BEGIN
    CREATE TABLE work.LateArrivingDimensionQueue
    (
        QueueRowId              BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        DimensionName           NVARCHAR(100)   NOT NULL,   -- Customer / Supplier / StockItem / Geography
        MissingBusinessKey      NVARCHAR(140)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NULL,
        FirstSeenObjectName     NVARCHAR(200)   NULL,       -- the fact staging table that needed it
        FirstSeenAtUtc          DATETIME2(3)    NOT NULL CONSTRAINT DF_workLateArrivingDimQueue_FirstSeen DEFAULT (SYSUTCDATETIME()),
        OccurrenceCount         INT             NOT NULL CONSTRAINT DF_workLateArrivingDimQueue_Count DEFAULT (1),
        InferredAttributesJson  NVARCHAR(MAX)   NULL,       -- what the fact row knew, for the stub row
        StubCreatedFlag         BIT             NOT NULL CONSTRAINT DF_workLateArrivingDimQueue_Stub DEFAULT (0),
        StubCreatedAtUtc        DATETIME2(3)    NULL,
        ResolvedFlag            BIT             NOT NULL CONSTRAINT DF_workLateArrivingDimQueue_Resolved DEFAULT (0),
        ResolvedAtUtc           DATETIME2(3)    NULL,
        ResolutionNote          NVARCHAR(400)   NULL,
        CONSTRAINT PK_workLateArrivingDimensionQueue PRIMARY KEY CLUSTERED (QueueRowId)
    );

    CREATE INDEX IX_workLateArrivingDimQueue_Open
        ON work.LateArrivingDimensionQueue (DimensionName, ResolvedFlag) INCLUDE (MissingBusinessKey);
END;
GO

IF OBJECT_ID(N'work.FactRekeyQueue', N'U') IS NULL
BEGIN
    CREATE TABLE work.FactRekeyQueue
    (
        QueueRowId              BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        FactObjectName          NVARCHAR(200)   NOT NULL,
        FactBusinessKey         NVARCHAR(140)   NOT NULL,
        DimensionName           NVARCHAR(100)   NOT NULL,
        CurrentSurrogateKey     BIGINT          NULL,       -- usually the -1 unknown member
        CorrectedSurrogateKey   BIGINT          NULL,
        RekeyReasonCode         NVARCHAR(30)    NOT NULL,   -- LATE_DIM / SCD2_BACKDATE / MERGE_SURVIVOR
        EffectiveDate           DATE            NULL,
        RekeyPriority           SMALLINT        NOT NULL CONSTRAINT DF_workFactRekeyQueue_Priority DEFAULT (5),
        AppliedFlag             BIT             NOT NULL CONSTRAINT DF_workFactRekeyQueue_Applied DEFAULT (0),
        AppliedAtUtc            DATETIME2(3)    NULL,
        AttemptCount            SMALLINT        NOT NULL CONSTRAINT DF_workFactRekeyQueue_Attempts DEFAULT (0),
        LastErrorText           NVARCHAR(1000)  NULL,
        CreatedAtUtc            DATETIME2(3)    NOT NULL CONSTRAINT DF_workFactRekeyQueue_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_workFactRekeyQueue PRIMARY KEY CLUSTERED (QueueRowId)
    );

    CREATE INDEX IX_workFactRekeyQueue_Pending ON work.FactRekeyQueue (AppliedFlag, RekeyPriority, FactObjectName);
END;
GO
