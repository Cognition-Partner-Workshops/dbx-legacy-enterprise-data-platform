/*
    stg.* conformed procurement, payables and finance tables

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Deploy order  : 21
    Depends on    : 10_raw_tables_oracle.sql, 50_ref_tables.sql
    Loaded by     : stg.usp_AppendIncremental_Payment, stg.usp_TranslateSourceCodes,
                    stg.usp_ConvertCurrencyAmounts, work.usp_MatchPaymentsToInvoices
    Read by       : stg.vw_PurchaseLineReadyForFact, stg.vw_PaymentReadyForFact

    Monetary columns come in threes: the transaction amount in its own currency,
    the ledger amount in the ledger currency, and the USD reporting amount that
    the warehouse measures are built on. The FX rate and rate date used are kept
    on the row so a restatement can be explained without re-deriving it.
*/

IF OBJECT_ID(N'stg.PurchaseOrder', N'U') IS NULL
BEGIN
    CREATE TABLE stg.PurchaseOrder
    (
        StagingPurchaseOrderId  BIGINT          NOT NULL IDENTITY(1,1),
        PurchaseOrderBusinessKey NVARCHAR(100)  NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SourcePurchaseOrderId   NVARCHAR(50)    NOT NULL,
        PurchaseOrderNumber     NVARCHAR(50)    NOT NULL,
        RevisionNumber          SMALLINT        NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        BuyerEmployeeKey        NVARCHAR(100)   NULL,
        PurchaseOrderTypeCode   NVARCHAR(20)    NULL,
        PurchaseOrderStatusCode NVARCHAR(20)    NOT NULL,
        OrderDate               DATE            NULL,
        NeedByDate              DATE            NULL,
        PromisedDate            DATE            NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        TransactionFxRate       DECIMAL(19,8)   NULL,
        FxRateTypeCode          NVARCHAR(20)    NULL,
        OrderTotalAmount        DECIMAL(19,4)   NULL,
        OrderTotalAmountUsd     DECIMAL(19,4)   NULL,
        TaxTotalAmount          DECIMAL(19,4)   NULL,
        FreightAmount           DECIMAL(19,4)   NULL,
        IncotermCode            NVARCHAR(10)    NULL,
        ShipToSiteCode          NVARCHAR(30)    NULL,
        CostCenterCode          NVARCHAR(30)    NULL,
        ContractBusinessKey     NVARCHAR(100)   NULL,
        LedgerCode              NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        FiscalPeriodLabel       NVARCHAR(20)    NULL,       -- derived per regional fiscal calendar
        ApprovedByName          NVARCHAR(100)   NULL,
        ApprovedDate            DATE            NULL,
        IsCancelled             BIT             NOT NULL CONSTRAINT DF_stgPurchaseOrder_Cancelled DEFAULT (0),
        CancelReasonCode        NVARCHAR(20)    NULL,
        SourceModifiedDate      DATETIME2(3)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgPurchaseOrder_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgPurchaseOrder_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgPurchaseOrder PRIMARY KEY CLUSTERED (StagingPurchaseOrderId)
    );

    CREATE INDEX IX_stgPurchaseOrder_Number ON stg.PurchaseOrder (PurchaseOrderNumber, RevisionNumber);
END;
GO

IF OBJECT_ID(N'stg.PurchaseOrderLine', N'U') IS NULL
BEGIN
    CREATE TABLE stg.PurchaseOrderLine
    (
        StagingPurchaseOrderLineId BIGINT       NOT NULL IDENTITY(1,1),
        PurchaseOrderLineBusinessKey NVARCHAR(120) NOT NULL,
        PurchaseOrderBusinessKey NVARCHAR(100)  NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        LineNumber              INT             NULL,
        ShipmentNumber          INT             NULL,
        ProductBusinessKey      NVARCHAR(100)   NULL,
        SupplierItemCode        NVARCHAR(50)    NULL,
        LineDescription         NVARCHAR(500)   NULL,
        OrderQuantity           DECIMAL(18,4)   NULL,
        OrderUomCode            NVARCHAR(10)    NULL,
        OrderQuantityBaseUom    DECIMAL(18,4)   NULL,       -- converted through ref.UomConversion
        UnitPriceAmount         DECIMAL(19,4)   NULL,
        ExtendedAmount          DECIMAL(19,4)   NULL,
        ExtendedAmountUsd       DECIMAL(19,4)   NULL,
        DiscountPercent         DECIMAL(9,4)    NULL,
        TaxCode                 NVARCHAR(20)    NULL,
        TaxRegimeCode           NVARCHAR(20)    NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        RecoverableTaxAmount    DECIMAL(19,4)   NULL,       -- EU input VAT; zero for NA sales tax
        ReceivedQuantity        DECIMAL(18,4)   NULL,
        BilledQuantity          DECIMAL(18,4)   NULL,
        CancelledQuantity       DECIMAL(18,4)   NULL,
        OpenQuantity            AS (ISNULL(OrderQuantity, 0) - ISNULL(ReceivedQuantity, 0) - ISNULL(CancelledQuantity, 0)),
        LineStatusCode          NVARCHAR(20)    NULL,
        NeedByDate              DATE            NULL,
        ClosedDate              DATE            NULL,
        CostCenterCode          NVARCHAR(30)    NULL,
        GlAccountCode           NVARCHAR(40)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgPurchaseOrderLine_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgPurchaseOrderLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgPurchaseOrderLine PRIMARY KEY CLUSTERED (StagingPurchaseOrderLineId)
    );

    CREATE INDEX IX_stgPurchaseOrderLine_Header ON stg.PurchaseOrderLine (PurchaseOrderBusinessKey, LineNumber);
END;
GO

IF OBJECT_ID(N'stg.Receipt', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Receipt
    (
        StagingReceiptId        BIGINT          NOT NULL IDENTITY(1,1),
        ReceiptLineBusinessKey  NVARCHAR(120)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        ReceiptNumber           NVARCHAR(50)    NULL,
        PurchaseOrderLineBusinessKey NVARCHAR(120) NULL,
        PurchaseOrderNumber     NVARCHAR(50)    NULL,
        ProductBusinessKey      NVARCHAR(100)   NULL,
        ReceivedQuantity        DECIMAL(18,4)   NULL,
        ReceivedUomCode         NVARCHAR(10)    NULL,
        ReceivedQuantityBaseUom DECIMAL(18,4)   NULL,
        AcceptedQuantity        DECIMAL(18,4)   NULL,
        RejectedQuantity        DECIMAL(18,4)   NULL,
        RejectReasonCode        NVARCHAR(20)    NULL,
        LotNumber               NVARCHAR(50)    NULL,
        ReceiptDate             DATE            NULL,
        ReceiptDateTimeUtc      DATETIME2(3)    NULL,
        InspectionDate          DATE            NULL,
        InspectionResultCode    NVARCHAR(20)    NULL,
        WarehouseCode           NVARCHAR(30)    NULL,
        BinCode                 NVARCHAR(30)    NULL,
        ReceiverEmployeeKey     NVARCHAR(100)   NULL,
        LandedCostAmount        DECIMAL(19,4)   NULL,
        LandedCostAmountUsd     DECIMAL(19,4)   NULL,
        DaysLateVsPromised      INT             NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgReceipt_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgReceipt_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgReceipt PRIMARY KEY CLUSTERED (StagingReceiptId)
    );
END;
GO

IF OBJECT_ID(N'stg.ApInvoice', N'U') IS NULL
BEGIN
    CREATE TABLE stg.ApInvoice
    (
        StagingApInvoiceId      BIGINT          NOT NULL IDENTITY(1,1),
        ApInvoiceBusinessKey    NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        InvoiceNumber           NVARCHAR(50)    NOT NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        InvoiceTypeCode         NVARCHAR(20)    NULL,
        InvoiceDate             DATE            NULL,
        GlDate                  DATE            NULL,
        DueDate                 DATE            NULL,
        DiscountDueDate         DATE            NULL,       -- derived from stg.PaymentTerms per region
        PaymentTermsCode        NVARCHAR(20)    NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        TransactionFxRate       DECIMAL(19,8)   NULL,
        FxRateDate              DATE            NULL,
        InvoiceAmount           DECIMAL(19,4)   NULL,
        InvoiceAmountUsd        DECIMAL(19,4)   NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        WithholdingAmount       DECIMAL(19,4)   NULL,       -- NA
        VatRecoverableAmount    DECIMAL(19,4)   NULL,       -- EU
        GstInputCreditAmount    DECIMAL(19,4)   NULL,       -- APAC
        AmountPaid              DECIMAL(19,4)   NULL,
        OpenAmount              AS (ISNULL(InvoiceAmount, 0) - ISNULL(AmountPaid, 0)),
        InvoiceStatusCode       NVARCHAR(20)    NULL,
        IsOnHold                BIT             NOT NULL CONSTRAINT DF_stgApInvoice_Hold DEFAULT (0),
        HoldReasonCode          NVARCHAR(20)    NULL,
        MatchTypeCode           NVARCHAR(20)    NULL,
        PurchaseOrderNumber     NVARCHAR(50)    NULL,
        LedgerCode              NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        FiscalPeriodLabel       NVARCHAR(20)    NULL,
        AgeBucketCode           NVARCHAR(20)    NULL,       -- 0-30 / 31-60 / 61-90 / 90+
        SourceModifiedDate      DATETIME2(3)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgApInvoice_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgApInvoice_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgApInvoice PRIMARY KEY CLUSTERED (StagingApInvoiceId)
    );

    CREATE INDEX IX_stgApInvoice_Supplier ON stg.ApInvoice (SupplierBusinessKey, InvoiceDate);
END;
GO

IF OBJECT_ID(N'stg.ApInvoiceLine', N'U') IS NULL
BEGIN
    CREATE TABLE stg.ApInvoiceLine
    (
        StagingApInvoiceLineId  BIGINT          NOT NULL IDENTITY(1,1),
        ApInvoiceLineBusinessKey NVARCHAR(120)  NOT NULL,
        ApInvoiceBusinessKey    NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        LineNumber              INT             NULL,
        LineTypeCode            NVARCHAR(20)    NULL,
        PurchaseOrderLineBusinessKey NVARCHAR(120) NULL,
        ReceiptLineBusinessKey  NVARCHAR(120)   NULL,
        ProductBusinessKey      NVARCHAR(100)   NULL,
        LineDescription         NVARCHAR(500)   NULL,
        Quantity                DECIMAL(18,4)   NULL,
        UomCode                 NVARCHAR(10)    NULL,
        UnitPriceAmount         DECIMAL(19,4)   NULL,
        LineAmount              DECIMAL(19,4)   NULL,
        LineAmountUsd           DECIMAL(19,4)   NULL,
        TaxCode                 NVARCHAR(20)    NULL,
        TaxRatePercent          DECIMAL(9,4)    NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        TaxJurisdictionCode     NVARCHAR(30)    NULL,
        ReverseChargeFlag       BIT             NULL,       -- EU cross-border acquisitions
        GlAccountCode           NVARCHAR(40)    NULL,
        CostCenterCode          NVARCHAR(30)    NULL,
        ProjectCode             NVARCHAR(30)    NULL,
        IsAccrual               BIT             NOT NULL CONSTRAINT DF_stgApInvoiceLine_Accrual DEFAULT (0),
        ThreeWayMatchStatusCode NVARCHAR(20)    NULL,       -- MATCHED / QTY_VARIANCE / PRICE_VARIANCE / NO_PO
        MatchVarianceAmount     DECIMAL(19,4)   NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgApInvoiceLine_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgApInvoiceLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgApInvoiceLine PRIMARY KEY CLUSTERED (StagingApInvoiceLineId)
    );
END;
GO

IF OBJECT_ID(N'stg.Payment', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Payment
    (
        StagingPaymentId        BIGINT          NOT NULL IDENTITY(1,1),
        PaymentBusinessKey      NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        PaymentNumber           NVARCHAR(50)    NOT NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        PaymentMethodCode       NVARCHAR(20)    NULL,
        PaymentDate             DATE            NULL,
        ClearedDate             DATE            NULL,
        VoidDate                DATE            NULL,
        PaymentStatusCode       NVARCHAR(20)    NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        TransactionFxRate       DECIMAL(19,8)   NULL,
        PaymentAmount           DECIMAL(19,4)   NULL,
        PaymentAmountUsd        DECIMAL(19,4)   NULL,
        DiscountTakenAmount     DECIMAL(19,4)   NULL,
        RealizedFxGainLossUsd   DECIMAL(19,4)   NULL,       -- rate at payment vs rate at invoice
        BankAccountReference    NVARCHAR(50)    NULL,       -- internal reference only, never an account number
        RemittanceReference     NVARCHAR(100)   NULL,
        AppliedInvoiceCount     INT             NULL,
        UnappliedAmount         DECIMAL(19,4)   NULL,
        MatchStatusCode         NVARCHAR(20)    NULL,       -- set by work.usp_MatchPaymentsToInvoices
        LedgerCode              NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        SourceModifiedDate      DATETIME2(3)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgPayment_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgPayment_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgPayment PRIMARY KEY CLUSTERED (StagingPaymentId)
    );

    CREATE INDEX IX_stgPayment_Supplier ON stg.Payment (SupplierBusinessKey, PaymentDate);
END;
GO

IF OBJECT_ID(N'stg.GlJournalLine', N'U') IS NULL
BEGIN
    CREATE TABLE stg.GlJournalLine
    (
        StagingGlJournalLineId  BIGINT          NOT NULL IDENTITY(1,1),
        GlJournalLineBusinessKey NVARCHAR(120)  NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        JournalName             NVARCHAR(200)   NULL,
        JournalSourceCode       NVARCHAR(20)    NULL,
        JournalCategoryCode     NVARCHAR(20)    NULL,
        LineNumber              INT             NULL,
        LedgerCode              NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        FiscalPeriodLabel       NVARCHAR(20)    NULL,
        FiscalYear              SMALLINT        NULL,
        FiscalPeriodNumber      TINYINT         NULL,
        EffectiveDate           DATE            NULL,
        GlAccountCode           NVARCHAR(40)    NULL,
        GlAccountSegment1       NVARCHAR(10)    NULL,       -- company
        GlAccountSegment2       NVARCHAR(10)    NULL,       -- cost centre
        GlAccountSegment3       NVARCHAR(10)    NULL,       -- natural account
        GlAccountSegment4       NVARCHAR(10)    NULL,       -- product line
        CostCenterCode          NVARCHAR(30)    NULL,
        ProjectCode             NVARCHAR(30)    NULL,
        IntercompanyCode        NVARCHAR(20)    NULL,
        EnteredDebitAmount      DECIMAL(19,4)   NULL,
        EnteredCreditAmount     DECIMAL(19,4)   NULL,
        AccountedDebitAmount    DECIMAL(19,4)   NULL,
        AccountedCreditAmount   DECIMAL(19,4)   NULL,
        NetAmountUsd            DECIMAL(19,4)   NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        TransactionFxRate       DECIMAL(19,8)   NULL,
        StatisticalAmount       DECIMAL(19,4)   NULL,
        LineDescription         NVARCHAR(500)   NULL,
        IsReversal              BIT             NOT NULL CONSTRAINT DF_stgGlJournalLine_Reversal DEFAULT (0),
        IsPosted                BIT             NOT NULL CONSTRAINT DF_stgGlJournalLine_Posted DEFAULT (0),
        PostedDate              DATE            NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgGlJournalLine_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgGlJournalLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgGlJournalLine PRIMARY KEY CLUSTERED (StagingGlJournalLineId)
    );

    CREATE INDEX IX_stgGlJournalLine_Period ON stg.GlJournalLine (LedgerCode, FiscalPeriodLabel);
END;
GO

IF OBJECT_ID(N'stg.VendorContract', N'U') IS NULL
BEGIN
    CREATE TABLE stg.VendorContract
    (
        StagingVendorContractId BIGINT          NOT NULL IDENTITY(1,1),
        ContractBusinessKey     NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        ContractNumber          NVARCHAR(50)    NOT NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        ContractTypeCode        NVARCHAR(20)    NULL,
        ContractStatusCode      NVARCHAR(20)    NULL,
        StartDate               DATE            NULL,
        EndDate                 DATE            NULL,
        AutoRenewFlag           BIT             NULL,
        NoticePeriodDays        SMALLINT        NULL,
        CommittedAmount         DECIMAL(19,4)   NULL,
        CommittedAmountUsd      DECIMAL(19,4)   NULL,
        ConsumedAmount          DECIMAL(19,4)   NULL,
        ConsumedPercent         DECIMAL(9,4)    NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        RebateTierCount         SMALLINT        NULL,       -- parsed out of the source tier grid
        TopRebatePercent        DECIMAL(9,4)    NULL,
        GoverningLawCode        NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        SignedDate              DATE            NULL,
        ExpiringWithin90DaysFlag BIT            NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgVendorContract_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgVendorContract_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgVendorContract PRIMARY KEY CLUSTERED (StagingVendorContractId)
    );
END;
GO

IF OBJECT_ID(N'stg.Currency', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Currency
    (
        StagingCurrencyId       BIGINT          NOT NULL IDENTITY(1,1),
        CurrencyCode            NCHAR(3)        NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        CurrencyName            NVARCHAR(100)   NULL,
        CurrencySymbol          NVARCHAR(10)    NULL,
        MinorUnitDigits         TINYINT         NULL,
        IsoNumericCode          NVARCHAR(5)     NULL,
        IsActive                BIT             NOT NULL CONSTRAINT DF_stgCurrency_IsActive DEFAULT (1),
        IsEuroLegacy            BIT             NOT NULL CONSTRAINT DF_stgCurrency_EuroLegacy DEFAULT (0),
        EuroFixedRate           DECIMAL(19,8)   NULL,       -- irrevocable conversion rate for legacy rows
        IsReportingCurrency     BIT             NOT NULL CONSTRAINT DF_stgCurrency_Reporting DEFAULT (0),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgCurrency_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgCurrency PRIMARY KEY CLUSTERED (StagingCurrencyId)
    );
END;
GO

IF OBJECT_ID(N'stg.FxRate', N'U') IS NULL
BEGIN
    CREATE TABLE stg.FxRate
    (
        StagingFxRateId         BIGINT          NOT NULL IDENTITY(1,1),
        FxRateBusinessKey       NVARCHAR(120)   NOT NULL,   -- from|to|date|type
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        FromCurrencyCode        NCHAR(3)        NOT NULL,
        ToCurrencyCode          NCHAR(3)        NOT NULL,
        RateDate                DATE            NOT NULL,
        RateTypeCode            NVARCHAR(20)    NOT NULL,
        ConversionRate          DECIMAL(19,8)   NOT NULL,
        InverseRate             DECIMAL(19,8)   NULL,
        RateSourceCode          NVARCHAR(20)    NULL,
        IsTreasuryOverride      BIT             NOT NULL CONSTRAINT DF_stgFxRate_Override DEFAULT (0),
        OverrideReason          NVARCHAR(400)   NULL,
        PriorDayRate            DECIMAL(19,8)   NULL,
        DayOverDayMovePercent   DECIMAL(9,4)    NULL,
        ExceedsToleranceFlag    BIT             NULL,       -- compared with FxRateTolerancePercent
        LedgerCode              NVARCHAR(20)    NULL,
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgFxRate_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgFxRate PRIMARY KEY CLUSTERED (StagingFxRateId)
    );

    CREATE INDEX IX_stgFxRate_Lookup ON stg.FxRate (FromCurrencyCode, ToCurrencyCode, RateDate, RateTypeCode);
END;
GO

IF OBJECT_ID(N'stg.TaxRate', N'U') IS NULL
BEGIN
    CREATE TABLE stg.TaxRate
    (
        StagingTaxRateId        BIGINT          NOT NULL IDENTITY(1,1),
        TaxRateBusinessKey      NVARCHAR(120)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        TaxCode                 NVARCHAR(20)    NOT NULL,
        TaxRegimeCode           NVARCHAR(20)    NOT NULL,   -- SALESTAX (NA) / VAT (EU) / GST (APAC)
        TaxJurisdictionCode     NVARCHAR(30)    NULL,
        CountryCode             NCHAR(2)        NULL,
        StateProvinceCode       NVARCHAR(20)    NULL,
        TaxClassCode            NVARCHAR(20)    NULL,
        RatePercent             DECIMAL(9,4)    NOT NULL,
        IsCompound              BIT             NOT NULL CONSTRAINT DF_stgTaxRate_Compound DEFAULT (0),
        CompoundOnTaxCode       NVARCHAR(20)    NULL,
        RecoverablePercent      DECIMAL(9,4)    NULL,
        ReverseChargeFlag       BIT             NULL,
        EffectiveFromDate       DATE            NULL,
        EffectiveToDate         DATE            NULL,
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgTaxRate_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgTaxRate PRIMARY KEY CLUSTERED (StagingTaxRateId)
    );

    CREATE INDEX IX_stgTaxRate_Lookup ON stg.TaxRate (TaxCode, EffectiveFromDate);
END;
GO

IF OBJECT_ID(N'stg.PaymentTerms', N'U') IS NULL
BEGIN
    CREATE TABLE stg.PaymentTerms
    (
        StagingPaymentTermsId   BIGINT          NOT NULL IDENTITY(1,1),
        PaymentTermsCode        NVARCHAR(20)    NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        PaymentTermsName        NVARCHAR(100)   NULL,
        NetDays                 SMALLINT        NULL,
        DiscountDays            SMALLINT        NULL,
        DiscountPercent         DECIMAL(9,4)    NULL,
        DayOfMonthDue           TINYINT         NULL,       -- EU end-of-month and 10th-of-month terms
        InstalmentCount         TINYINT         NULL,
        CalculationBasisCode    NVARCHAR(20)    NULL,       -- INVOICE_DATE / GL_DATE / RECEIPT_DATE
        RegionCode              NVARCHAR(10)    NULL,
        IsActive                BIT             NOT NULL CONSTRAINT DF_stgPaymentTerms_IsActive DEFAULT (1),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgPaymentTerms_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgPaymentTerms PRIMARY KEY CLUSTERED (StagingPaymentTermsId)
    );
END;
GO
