/*
    stg.* conformed sales, fulfilment and channel tables

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Deploy order  : 22
    Depends on    : 11_raw_tables_sqlserver.sql, 12_raw_tables_file.sql,
                    50_ref_tables.sql
    Loaded by     : stg.usp_AppendIncremental_OrderLine / _SaleLine / _StockMovement /
                    _Shipment, stg.usp_ConvertCurrencyAmounts
    Read by       : stg.vw_OrderLineReadyForFact, stg.vw_SaleLineReadyForFact,
                    stg.vw_ShipmentReadyForFact

    Sale/SaleLine are the invoiced view of the business (OLTP invoices plus the
    partner file feed); Order/OrderLine are the demand view. They are deliberately
    separate tables with different grains, different reject rules and different
    watermarks, because the warehouse loads them from different packages.

    Tax is regionalised at this layer, not in the warehouse: NA lines carry a
    sales-tax amount on top of a tax-exclusive price, EU lines carry a
    VAT-inclusive gross that is decomposed here, and APAC lines carry GST with
    the line-level rounding rule the local statutory reports expect.
*/

IF OBJECT_ID(N'stg.[Order]', N'U') IS NULL
BEGIN
    CREATE TABLE stg.[Order]
    (
        StagingOrderId          BIGINT          NOT NULL IDENTITY(1,1),
        OrderBusinessKey        NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SourceOrderId           NVARCHAR(50)    NOT NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        SalespersonBusinessKey  NVARCHAR(100)   NULL,
        ContactPersonKey        NVARCHAR(100)   NULL,
        BackorderOrderBusinessKey NVARCHAR(100) NULL,
        OrderDate               DATE            NULL,
        OrderDateTimeUtc        DATETIME2(3)    NULL,
        ExpectedDeliveryDate    DATE            NULL,
        CustomerPurchaseOrderNumber NVARCHAR(50) NULL,
        IsUndersupplyBackordered BIT            NOT NULL CONSTRAINT DF_stgOrder_Backordered DEFAULT (0),
        SalesChannelCode        NVARCHAR(20)    NULL,
        SalesTerritoryCode      NVARCHAR(20)    NULL,
        PromotionBusinessKey    NVARCHAR(100)   NULL,
        OrderStatusCode         NVARCHAR(20)    NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        RegionCode              NVARCHAR(10)    NULL,
        FiscalPeriodLabel       NVARCHAR(20)    NULL,
        LineCount               INT             NULL,
        OrderGrossAmount        DECIMAL(19,4)   NULL,
        OrderGrossAmountUsd     DECIMAL(19,4)   NULL,
        DeliveryInstructionsText NVARCHAR(1000) NULL,
        SourceModifiedDate      DATETIME2(3)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgOrder_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgOrder_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgOrder PRIMARY KEY CLUSTERED (StagingOrderId)
    );

    CREATE INDEX IX_stgOrder_Customer ON stg.[Order] (CustomerBusinessKey, OrderDate);
END;
GO

IF OBJECT_ID(N'stg.OrderLine', N'U') IS NULL
BEGIN
    CREATE TABLE stg.OrderLine
    (
        StagingOrderLineId      BIGINT          NOT NULL IDENTITY(1,1),
        OrderLineBusinessKey    NVARCHAR(120)   NOT NULL,
        OrderBusinessKey        NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        LineNumber              INT             NULL,
        StockItemBusinessKey    NVARCHAR(100)   NULL,
        ProductBusinessKey      NVARCHAR(100)   NULL,
        LineDescription         NVARCHAR(200)   NULL,
        PackageTypeCode         NVARCHAR(30)    NULL,
        OrderedQuantity         DECIMAL(18,4)   NULL,
        PickedQuantity          DECIMAL(18,4)   NULL,
        OutstandingQuantity     AS (ISNULL(OrderedQuantity, 0) - ISNULL(PickedQuantity, 0)),
        UnitPriceAmount         DECIMAL(19,4)   NULL,
        LineDiscountAmount      DECIMAL(19,4)   NULL,
        LineDiscountPercent     DECIMAL(9,4)    NULL,
        NetLineAmount           DECIMAL(19,4)   NULL,
        TaxRegimeCode           NVARCHAR(20)    NULL,
        TaxRatePercent          DECIMAL(9,4)    NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        GrossLineAmount         DECIMAL(19,4)   NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        NetLineAmountUsd        DECIMAL(19,4)   NULL,
        PromotionBusinessKey    NVARCHAR(100)   NULL,
        PickingCompletedWhenUtc DATETIME2(3)    NULL,
        LineStatusCode          NVARCHAR(20)    NULL,
        OrderDate               DATE            NULL,       -- denormalised for the fact partition key
        DuplicateGroupId        BIGINT          NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgOrderLine_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgOrderLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgOrderLine PRIMARY KEY CLUSTERED (StagingOrderLineId)
    );

    CREATE INDEX IX_stgOrderLine_Order ON stg.OrderLine (OrderBusinessKey, LineNumber);
    CREATE INDEX IX_stgOrderLine_Batch ON stg.OrderLine (BatchId) INCLUDE (DqStatusCode);
END;
GO

IF OBJECT_ID(N'stg.Sale', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Sale
    (
        StagingSaleId           BIGINT          NOT NULL IDENTITY(1,1),
        SaleBusinessKey         NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SourceInvoiceId         NVARCHAR(50)    NOT NULL,
        OrderBusinessKey        NVARCHAR(100)   NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        BillToCustomerBusinessKey NVARCHAR(100) NULL,
        SalespersonBusinessKey  NVARCHAR(100)   NULL,
        InvoiceDate             DATE            NULL,
        InvoiceDateTimeUtc      DATETIME2(3)    NULL,
        IsCreditNote            BIT             NOT NULL CONSTRAINT DF_stgSale_CreditNote DEFAULT (0),
        CreditNoteReasonText    NVARCHAR(500)   NULL,
        DeliveryMethodCode      NVARCHAR(30)    NULL,
        DeliveryRunCode         NVARCHAR(20)    NULL,
        RunPosition             INT             NULL,
        TotalDryItems           INT             NULL,
        TotalChillerItems       INT             NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        TransactionFxRate       DECIMAL(19,8)   NULL,
        SaleNetAmount           DECIMAL(19,4)   NULL,
        SaleTaxAmount           DECIMAL(19,4)   NULL,
        SaleGrossAmount         DECIMAL(19,4)   NULL,
        SaleNetAmountUsd        DECIMAL(19,4)   NULL,
        RegionCode              NVARCHAR(10)    NULL,
        FiscalPeriodLabel       NVARCHAR(20)    NULL,
        ConfirmedDeliveryUtc    DATETIME2(3)    NULL,
        ConfirmedReceivedByName NVARCHAR(200)   NULL,
        SourceModifiedDate      DATETIME2(3)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgSale_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgSale_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgSale PRIMARY KEY CLUSTERED (StagingSaleId)
    );

    CREATE INDEX IX_stgSale_InvoiceDate ON stg.Sale (InvoiceDate, RegionCode);
END;
GO

IF OBJECT_ID(N'stg.SaleLine', N'U') IS NULL
BEGIN
    CREATE TABLE stg.SaleLine
    (
        StagingSaleLineId       BIGINT          NOT NULL IDENTITY(1,1),
        SaleLineBusinessKey     NVARCHAR(120)   NOT NULL,
        SaleBusinessKey         NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        LineNumber              INT             NULL,
        StockItemBusinessKey    NVARCHAR(100)   NULL,
        ProductBusinessKey      NVARCHAR(100)   NULL,
        LineDescription         NVARCHAR(200)   NULL,
        Quantity                DECIMAL(18,4)   NULL,
        UomCode                 NVARCHAR(10)    NULL,
        QuantityBaseUom         DECIMAL(18,4)   NULL,
        UnitPriceAmount         DECIMAL(19,4)   NULL,
        NetLineAmount           DECIMAL(19,4)   NULL,
        TaxRegimeCode           NVARCHAR(20)    NULL,
        TaxRatePercent          DECIMAL(9,4)    NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        TaxRoundingRuleCode     NVARCHAR(20)    NULL,       -- LINE (APAC) / INVOICE (EU) / JURISDICTION (NA)
        GrossLineAmount         DECIMAL(19,4)   NULL,
        LineProfitAmount        DECIMAL(19,4)   NULL,
        CommissionRatePercent   DECIMAL(9,4)    NULL,
        CommissionAmount        DECIMAL(19,4)   NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        NetLineAmountUsd        DECIMAL(19,4)   NULL,
        GrossLineAmountUsd      DECIMAL(19,4)   NULL,
        FxRateDate              DATE            NULL,
        PromotionBusinessKey    NVARCHAR(100)   NULL,
        InvoiceDate             DATE            NULL,
        RegionCode              NVARCHAR(10)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgSaleLine_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgSaleLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgSaleLine PRIMARY KEY CLUSTERED (StagingSaleLineId)
    );

    CREATE INDEX IX_stgSaleLine_Sale ON stg.SaleLine (SaleBusinessKey, LineNumber);
    CREATE INDEX IX_stgSaleLine_Item ON stg.SaleLine (StockItemBusinessKey, InvoiceDate);
END;
GO

IF OBJECT_ID(N'stg.StockMovement', N'U') IS NULL
BEGIN
    CREATE TABLE stg.StockMovement
    (
        StagingStockMovementId  BIGINT          NOT NULL IDENTITY(1,1),
        StockMovementBusinessKey NVARCHAR(120)  NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        StockItemBusinessKey    NVARCHAR(100)   NULL,
        MovementTypeCode        NVARCHAR(20)    NOT NULL,   -- RECEIPT / ISSUE / ADJUST / TRANSFER / RETURN
        MovementReasonCode      NVARCHAR(20)    NULL,
        MovementDirection       SMALLINT        NULL,       -- +1 inbound, -1 outbound
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        SaleBusinessKey         NVARCHAR(100)   NULL,
        PurchaseOrderBusinessKey NVARCHAR(100)  NULL,
        WarehouseCode           NVARCHAR(30)    NULL,
        BinCode                 NVARCHAR(30)    NULL,
        MovementDate            DATE            NULL,
        MovementDateTimeUtc     DATETIME2(3)    NULL,
        Quantity                DECIMAL(18,4)   NULL,
        UnitCostAmount          DECIMAL(19,4)   NULL,
        ExtendedCostAmountUsd   DECIMAL(19,4)   NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgStockMovement_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgStockMovement_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgStockMovement PRIMARY KEY CLUSTERED (StagingStockMovementId)
    );

    CREATE INDEX IX_stgStockMovement_Item ON stg.StockMovement (StockItemBusinessKey, MovementDate, WarehouseCode);
END;
GO

IF OBJECT_ID(N'stg.Shipment', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Shipment
    (
        StagingShipmentId       BIGINT          NOT NULL IDENTITY(1,1),
        ShipmentBusinessKey     NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        ShipmentReference       NVARCHAR(50)    NULL,
        SaleBusinessKey         NVARCHAR(100)   NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        CarrierCode             NVARCHAR(30)    NULL,
        ServiceLevelCode        NVARCHAR(30)    NULL,
        DeliveryRouteCode       NVARCHAR(30)    NULL,
        ShipFromWarehouseCode   NVARCHAR(30)    NULL,
        ShipToCountryCode       NCHAR(2)        NULL,
        ShipToPostalCodeStandardized NVARCHAR(20) NULL,
        ShipToGeographyBusinessKey NVARCHAR(120) NULL,
        ShippedDate             DATE            NULL,
        ShippedDateTimeUtc      DATETIME2(3)    NULL,
        PromisedDeliveryUtc     DATETIME2(3)    NULL,
        DeliveredDateTimeUtc    DATETIME2(3)    NULL,
        DeliveryLatencyHours    DECIMAL(9,2)    NULL,
        OnTimeDeliveryFlag      BIT             NULL,
        TotalWeightKg           DECIMAL(18,3)   NULL,       -- LB values from NA depots converted here
        TotalVolumeM3           DECIMAL(18,4)   NULL,
        FreightChargeAmount     DECIMAL(19,4)   NULL,
        FreightCurrencyCode     NCHAR(3)        NULL,
        FreightChargeAmountUsd  DECIMAL(19,4)   NULL,
        CustomsDeclarationRef   NVARCHAR(50)    NULL,
        CustomsRequiredFlag     BIT             NULL,       -- intra-EU movements are exempt
        ShipmentStatusCode      NVARCHAR(20)    NULL,
        LastScanEventCode       NVARCHAR(20)    NULL,       -- from the carrier scan file feed
        LastScanUtc             DATETIME2(3)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgShipment_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgShipment_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgShipment PRIMARY KEY CLUSTERED (StagingShipmentId)
    );

    CREATE INDEX IX_stgShipment_Sale ON stg.Shipment (SaleBusinessKey);
END;
GO

IF OBJECT_ID(N'stg.ShipmentLine', N'U') IS NULL
BEGIN
    CREATE TABLE stg.ShipmentLine
    (
        StagingShipmentLineId   BIGINT          NOT NULL IDENTITY(1,1),
        ShipmentLineBusinessKey NVARCHAR(120)   NOT NULL,
        ShipmentBusinessKey     NVARCHAR(100)   NOT NULL,
        SaleLineBusinessKey     NVARCHAR(120)   NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        StockItemBusinessKey    NVARCHAR(100)   NULL,
        PackageTypeCode         NVARCHAR(30)    NULL,
        ShippedQuantity         DECIMAL(18,4)   NULL,
        WeightKg                DECIMAL(18,3)   NULL,
        SerialNumberCount       INT             NULL,       -- the serial list itself stays in raw
        TemperatureAtLoadC      DECIMAL(9,2)    NULL,
        ColdChainBreachFlag     BIT             NULL,
        LineStatusCode          NVARCHAR(20)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgShipmentLine_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgShipmentLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgShipmentLine PRIMARY KEY CLUSTERED (StagingShipmentLineId)
    );
END;
GO

IF OBJECT_ID(N'stg.[Return]', N'U') IS NULL
BEGIN
    CREATE TABLE stg.[Return]
    (
        StagingReturnId         BIGINT          NOT NULL IDENTITY(1,1),
        ReturnLineBusinessKey   NVARCHAR(120)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        RmaNumber               NVARCHAR(50)    NULL,
        SaleLineBusinessKey     NVARCHAR(120)   NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        StockItemBusinessKey    NVARCHAR(100)   NULL,
        ReturnReasonCode        NVARCHAR(20)    NULL,
        ReturnReasonGroupCode   NVARCHAR(20)    NULL,       -- conformed grouping over the source reasons
        ReturnedQuantity        DECIMAL(18,4)   NULL,
        RestockedQuantity       DECIMAL(18,4)   NULL,
        ScrappedQuantity        DECIMAL(18,4)   NULL,
        InspectionResultCode    NVARCHAR(20)    NULL,
        RestockingFeeAmount     DECIMAL(19,4)   NULL,       -- EU distance selling waives this within 14 days
        RefundAmount            DECIMAL(19,4)   NULL,
        RefundAmountUsd         DECIMAL(19,4)   NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        ReturnedDate            DATE            NULL,
        ProcessedDate           DATE            NULL,
        DaysSinceSale           INT             NULL,
        WithinStatutoryWindowFlag BIT           NULL,
        RegionCode              NVARCHAR(10)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgReturn_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgReturn_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgReturn PRIMARY KEY CLUSTERED (StagingReturnId)
    );
END;
GO

IF OBJECT_ID(N'stg.CreditNote', N'U') IS NULL
BEGIN
    CREATE TABLE stg.CreditNote
    (
        StagingCreditNoteId     BIGINT          NOT NULL IDENTITY(1,1),
        CreditNoteBusinessKey   NVARCHAR(100)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        CreditNoteNumber        NVARCHAR(50)    NOT NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        OriginalSaleBusinessKey NVARCHAR(100)   NULL,
        RmaNumber               NVARCHAR(50)    NULL,
        CreditReasonCode        NVARCHAR(20)    NULL,
        CreditNoteDate          DATE            NULL,
        NetAmount               DECIMAL(19,4)   NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        GrossAmount             DECIMAL(19,4)   NULL,
        NetAmountUsd            DECIMAL(19,4)   NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        AppliedToSaleBusinessKey NVARCHAR(100)  NULL,
        ApprovedByName          NVARCHAR(100)   NULL,
        CreditStatusCode        NVARCHAR(20)    NULL,
        VatCreditNoteRequiredFlag BIT           NULL,       -- EU statutory credit-note numbering
        RegionCode              NVARCHAR(10)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgCreditNote_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgCreditNote_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgCreditNote PRIMARY KEY CLUSTERED (StagingCreditNoteId)
    );
END;
GO

IF OBJECT_ID(N'stg.LoyaltyLedger', N'U') IS NULL
BEGIN
    CREATE TABLE stg.LoyaltyLedger
    (
        StagingLoyaltyLedgerId  BIGINT          NOT NULL IDENTITY(1,1),
        LoyaltyLedgerBusinessKey NVARCHAR(120)  NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        LoyaltyMemberBusinessKey NVARCHAR(100)  NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        ProgramCode             NVARCHAR(30)    NULL,
        TierCode                NVARCHAR(20)    NULL,
        EntryTypeCode           NVARCHAR(20)    NULL,
        PointsDelta             DECIMAL(18,2)   NULL,
        PointsBalanceAfter      DECIMAL(18,2)   NULL,
        SaleBusinessKey         NVARCHAR(100)   NULL,
        RedemptionReference     NVARCHAR(50)    NULL,
        EntryDate               DATE            NULL,
        EntryDateTimeUtc        DATETIME2(3)    NULL,
        ExpiryDate              DATE            NULL,
        ExpiryRuleCode          NVARCHAR(20)    NULL,       -- NA 24m rolling, EU 12m calendar, APAC 36m
        PointsValueUsd          DECIMAL(19,4)   NULL,
        RegionCode              NVARCHAR(10)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgLoyaltyLedger_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgLoyaltyLedger_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgLoyaltyLedger PRIMARY KEY CLUSTERED (StagingLoyaltyLedgerId)
    );
END;
GO

IF OBJECT_ID(N'stg.WebSession', N'U') IS NULL
BEGIN
    CREATE TABLE stg.WebSession
    (
        StagingWebSessionId     BIGINT          NOT NULL IDENTITY(1,1),
        WebSessionBusinessKey   NVARCHAR(120)   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        VisitorKeyHashed        NVARCHAR(64)    NULL,       -- hashed at staging; the raw key is dropped for EU
        SessionStartedUtc       DATETIME2(3)    NULL,
        SessionEndedUtc         DATETIME2(3)    NULL,
        SessionDurationSeconds  INT             NULL,
        LandingPagePath         NVARCHAR(400)   NULL,       -- query string stripped
        ReferrerDomain          NVARCHAR(200)   NULL,
        CampaignCode            NVARCHAR(50)    NULL,
        DeviceCategoryCode      NVARCHAR(20)    NULL,
        BrowserFamily           NVARCHAR(50)    NULL,
        CountryCode             NCHAR(2)        NULL,
        RegionCode              NVARCHAR(10)    NULL,
        PageViewCount           INT             NULL,
        CartCreatedFlag         BIT             NULL,
        OrderPlacedFlag         BIT             NULL,
        OrderBusinessKey        NVARCHAR(100)   NULL,
        AnalyticsConsentFlag    BIT             NULL,       -- EU rows without consent are aggregated only
        SuppressedForConsentFlag BIT            NOT NULL CONSTRAINT DF_stgWebSession_Suppressed DEFAULT (0),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgWebSession_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgWebSession_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgWebSession PRIMARY KEY CLUSTERED (StagingWebSessionId)
    );
END;
GO

IF OBJECT_ID(N'stg.PartnerSale', N'U') IS NULL
BEGIN
    CREATE TABLE stg.PartnerSale
    (
        StagingPartnerSaleId    BIGINT          NOT NULL IDENTITY(1,1),
        PartnerSaleBusinessKey  NVARCHAR(140)   NOT NULL,   -- partner|outlet|reference
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        PartnerCode             NVARCHAR(30)    NOT NULL,
        PartnerOutletCode       NVARCHAR(30)    NULL,
        ReportingPeriodCode     NVARCHAR(10)    NULL,       -- normalised to YYYYMM from both feed shapes
        TransactionReference    NVARCHAR(60)    NULL,
        TransactionDate         DATE            NULL,
        PartnerProductCode      NVARCHAR(60)    NULL,
        StockItemBusinessKey    NVARCHAR(100)   NULL,       -- resolved by barcode, then by name similarity
        ProductMatchMethodCode  NVARCHAR(20)    NULL,       -- BARCODE / XREF / NAME / UNMATCHED
        Barcode                 NVARCHAR(30)    NULL,
        QuantitySold            DECIMAL(18,4)   NULL,
        UomCode                 NVARCHAR(10)    NULL,
        QuantityBaseUom         DECIMAL(18,4)   NULL,
        GrossAmount             DECIMAL(19,4)   NULL,
        DiscountAmount          DECIMAL(19,4)   NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        NetAmount               DECIMAL(19,4)   NULL,
        NetAmountUsd            DECIMAL(19,4)   NULL,
        TransactionCurrencyCode NCHAR(3)        NULL,
        CountryCode             NCHAR(2)        NULL,
        RegionCode              NVARCHAR(10)    NULL,
        SalesChannelCode        NVARCHAR(20)    NULL,
        FileFormatVersion       NVARCHAR(10)    NULL,
        SourceFileName          NVARCHAR(260)   NULL,
        SourceRowNumber         BIGINT          NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgPartnerSale_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgPartnerSale_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgPartnerSale PRIMARY KEY CLUSTERED (StagingPartnerSaleId)
    );

    CREATE INDEX IX_stgPartnerSale_Period ON stg.PartnerSale (ReportingPeriodCode, PartnerCode);
END;
GO
