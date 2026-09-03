/*
    raw.Sql* landing tables (WideWorldImporters OLTP and ecommerce extracts)

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Deploy order  : 11
    Depends on    : sqlserver/control/01_schemas.sql
    Called by     : the EXT_SQL_* extract packages and the stg.usp_AppendIncremental_*
                    procedures

    The OLTP extracts are incremental by LastEditedWhen / change-tracking version,
    so these tables are appended to inside a batch and cleaned by the maintenance
    packages, not truncated per run. Types stay permissive: the extracts read
    through views that the OLTP team changes without notice, and a widened source
    column must not break the landing.
*/

IF OBJECT_ID(N'raw.SqlOrder', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlOrder
    (
        OrderID                 NVARCHAR(50)    NULL,
        CustomerID              NVARCHAR(50)    NULL,
        SalespersonPersonID     NVARCHAR(50)    NULL,
        PickedByPersonID        NVARCHAR(50)    NULL,
        ContactPersonID         NVARCHAR(50)    NULL,
        BackorderOrderID        NVARCHAR(50)    NULL,
        OrderDate               NVARCHAR(40)    NULL,
        ExpectedDeliveryDate    NVARCHAR(40)    NULL,
        CustomerPurchaseOrderNumber NVARCHAR(50) NULL,
        IsUndersupplyBackordered NVARCHAR(5)    NULL,
        SalesChannelCode        NVARCHAR(20)    NULL,   -- WEB / EDI / PHONE / FIELD / MARKETPLACE
        SalesTerritoryCode      NVARCHAR(20)    NULL,
        PromotionCode           NVARCHAR(30)    NULL,
        OrderStatusCode         NVARCHAR(20)    NULL,
        CurrencyCode            NVARCHAR(10)    NULL,
        Comments                NVARCHAR(MAX)   NULL,
        DeliveryInstructions    NVARCHAR(MAX)   NULL,
        InternalComments        NVARCHAR(MAX)   NULL,
        LastEditedBy            NVARCHAR(50)    NULL,
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlOrder_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlOrder_Source DEFAULT (N'WWI_OLTP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlOrder_Batch ON raw.SqlOrder (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.SqlOrderLine', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlOrderLine
    (
        OrderLineID             NVARCHAR(50)    NULL,
        OrderID                 NVARCHAR(50)    NULL,
        StockItemID             NVARCHAR(50)    NULL,
        Description             NVARCHAR(200)   NULL,
        PackageTypeID           NVARCHAR(50)    NULL,
        Quantity                NVARCHAR(50)    NULL,
        UnitPrice               NVARCHAR(50)    NULL,
        TaxRate                 NVARCHAR(50)    NULL,
        LineDiscountAmount      NVARCHAR(50)    NULL,
        LineDiscountPercent     NVARCHAR(50)    NULL,
        PromotionLineID         NVARCHAR(50)    NULL,
        PickedQuantity          NVARCHAR(50)    NULL,
        PickingCompletedWhen    NVARCHAR(40)    NULL,
        LineStatusCode          NVARCHAR(20)    NULL,
        LastEditedBy            NVARCHAR(50)    NULL,
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlOrderLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlOrderLine_Source DEFAULT (N'WWI_OLTP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlOrderLine_Batch ON raw.SqlOrderLine (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.SqlInvoice', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlInvoice
    (
        InvoiceID               NVARCHAR(50)    NULL,
        CustomerID              NVARCHAR(50)    NULL,
        BillToCustomerID        NVARCHAR(50)    NULL,
        OrderID                 NVARCHAR(50)    NULL,
        DeliveryMethodID        NVARCHAR(50)    NULL,
        ContactPersonID         NVARCHAR(50)    NULL,
        AccountsPersonID        NVARCHAR(50)    NULL,
        SalespersonPersonID     NVARCHAR(50)    NULL,
        PackedByPersonID        NVARCHAR(50)    NULL,
        InvoiceDate             NVARCHAR(40)    NULL,
        CustomerPurchaseOrderNumber NVARCHAR(50) NULL,
        IsCreditNote            NVARCHAR(5)     NULL,
        CreditNoteReason        NVARCHAR(MAX)   NULL,
        TotalDryItems           NVARCHAR(20)    NULL,
        TotalChillerItems       NVARCHAR(20)    NULL,
        DeliveryRun             NVARCHAR(20)    NULL,
        RunPosition             NVARCHAR(20)    NULL,
        CurrencyCode            NVARCHAR(10)    NULL,
        ConfirmedDeliveryTime   NVARCHAR(40)    NULL,
        ConfirmedReceivedBy     NVARCHAR(200)   NULL,
        LastEditedBy            NVARCHAR(50)    NULL,
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlInvoice_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlInvoice_Source DEFAULT (N'WWI_OLTP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlInvoice_Batch ON raw.SqlInvoice (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.SqlInvoiceLine', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlInvoiceLine
    (
        InvoiceLineID           NVARCHAR(50)    NULL,
        InvoiceID               NVARCHAR(50)    NULL,
        StockItemID             NVARCHAR(50)    NULL,
        Description             NVARCHAR(200)   NULL,
        PackageTypeID           NVARCHAR(50)    NULL,
        Quantity                NVARCHAR(50)    NULL,
        UnitPrice               NVARCHAR(50)    NULL,
        TaxRate                 NVARCHAR(50)    NULL,
        TaxAmount               NVARCHAR(50)    NULL,
        LineProfit              NVARCHAR(50)    NULL,
        ExtendedPrice           NVARCHAR(50)    NULL,
        PromotionLineID         NVARCHAR(50)    NULL,
        CommissionRate          NVARCHAR(50)    NULL,
        LastEditedBy            NVARCHAR(50)    NULL,
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlInvoiceLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlInvoiceLine_Source DEFAULT (N'WWI_OLTP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlInvoiceLine_Batch ON raw.SqlInvoiceLine (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.SqlStockItem', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlStockItem
    (
        StockItemID             NVARCHAR(50)    NULL,
        StockItemName           NVARCHAR(200)   NULL,
        SupplierID              NVARCHAR(50)    NULL,
        ColorID                 NVARCHAR(50)    NULL,
        UnitPackageID           NVARCHAR(50)    NULL,
        OuterPackageID          NVARCHAR(50)    NULL,
        Brand                   NVARCHAR(100)   NULL,
        Size                    NVARCHAR(50)    NULL,
        LeadTimeDays            NVARCHAR(20)    NULL,
        QuantityPerOuter        NVARCHAR(20)    NULL,
        IsChillerStock          NVARCHAR(5)     NULL,
        Barcode                 NVARCHAR(50)    NULL,
        TaxRate                 NVARCHAR(50)    NULL,
        UnitPrice               NVARCHAR(50)    NULL,
        RecommendedRetailPrice  NVARCHAR(50)    NULL,
        TypicalWeightPerUnit    NVARCHAR(50)    NULL,
        MarketingComments       NVARCHAR(MAX)   NULL,
        InternalComments        NVARCHAR(MAX)   NULL,
        CustomFields            NVARCHAR(MAX)   NULL,   -- JSON blob; tags parsed downstream
        ErpProductCode          NVARCHAR(50)    NULL,   -- hand-maintained by merchandising, often blank
        LastEditedBy            NVARCHAR(50)    NULL,
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlStockItem_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlStockItem_Source DEFAULT (N'WWI_OLTP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlStockItem_Batch ON raw.SqlStockItem (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.SqlStockMovement', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlStockMovement
    (
        StockItemTransactionID  NVARCHAR(50)    NULL,
        StockItemID             NVARCHAR(50)    NULL,
        TransactionTypeID       NVARCHAR(50)    NULL,
        TransactionTypeName     NVARCHAR(100)   NULL,
        CustomerID              NVARCHAR(50)    NULL,
        InvoiceID               NVARCHAR(50)    NULL,
        SupplierID              NVARCHAR(50)    NULL,
        PurchaseOrderID         NVARCHAR(50)    NULL,
        WarehouseCode           NVARCHAR(30)    NULL,
        BinCode                 NVARCHAR(30)    NULL,
        TransactionOccurredWhen NVARCHAR(40)    NULL,
        Quantity                NVARCHAR(50)    NULL,
        UnitCost                NVARCHAR(50)    NULL,
        MovementReasonCode      NVARCHAR(20)    NULL,
        LastEditedBy            NVARCHAR(50)    NULL,
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlStockMovement_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlStockMovement_Source DEFAULT (N'WWI_OLTP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlStockMovement_Batch ON raw.SqlStockMovement (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.SqlShipment', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlShipment
    (
        ShipmentID              NVARCHAR(50)    NULL,
        ShipmentReference       NVARCHAR(50)    NULL,
        InvoiceID               NVARCHAR(50)    NULL,
        CustomerID              NVARCHAR(50)    NULL,
        CarrierCode             NVARCHAR(30)    NULL,
        ServiceLevelCode        NVARCHAR(30)    NULL,
        DeliveryRouteCode       NVARCHAR(30)    NULL,
        ShippedWhen             NVARCHAR(40)    NULL,
        PromisedDeliveryWhen    NVARCHAR(40)    NULL,
        DeliveredWhen           NVARCHAR(40)    NULL,
        ShipFromWarehouseCode   NVARCHAR(30)    NULL,
        ShipToCountryCode       NVARCHAR(10)    NULL,
        ShipToPostalCode        NVARCHAR(40)    NULL,
        TotalWeightKg           NVARCHAR(50)    NULL,
        TotalVolumeM3           NVARCHAR(50)    NULL,
        FreightChargeAmount     NVARCHAR(50)    NULL,
        FreightCurrencyCode     NVARCHAR(10)    NULL,
        CustomsDeclarationRef   NVARCHAR(50)    NULL,
        ShipmentStatusCode      NVARCHAR(20)    NULL,
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlShipment_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlShipment_Source DEFAULT (N'WWI_OLTP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlShipment_Batch ON raw.SqlShipment (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.SqlShipmentLine', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlShipmentLine
    (
        ShipmentLineID          NVARCHAR(50)    NULL,
        ShipmentID              NVARCHAR(50)    NULL,
        InvoiceLineID           NVARCHAR(50)    NULL,
        StockItemID             NVARCHAR(50)    NULL,
        PackageTypeCode         NVARCHAR(30)    NULL,
        ShippedQuantity         NVARCHAR(50)    NULL,
        WeightKg                NVARCHAR(50)    NULL,
        SerialNumbers           NVARCHAR(MAX)   NULL,   -- comma separated, sometimes newline separated
        TemperatureAtLoadC      NVARCHAR(50)    NULL,   -- chiller lines only
        LineStatusCode          NVARCHAR(20)    NULL,
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlShipmentLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlShipmentLine_Source DEFAULT (N'WWI_OLTP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlShipmentLine_Batch ON raw.SqlShipmentLine (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.SqlReturnLine', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlReturnLine
    (
        ReturnLineID            NVARCHAR(50)    NULL,
        ReturnAuthorizationID   NVARCHAR(50)    NULL,
        RmaNumber               NVARCHAR(50)    NULL,
        InvoiceLineID           NVARCHAR(50)    NULL,
        CustomerID              NVARCHAR(50)    NULL,
        StockItemID             NVARCHAR(50)    NULL,
        ReturnReasonCode        NVARCHAR(20)    NULL,
        ReturnedQuantity        NVARCHAR(50)    NULL,
        RestockedQuantity       NVARCHAR(50)    NULL,
        ScrappedQuantity        NVARCHAR(50)    NULL,
        InspectionResultCode    NVARCHAR(20)    NULL,
        RestockingFeeAmount     NVARCHAR(50)    NULL,
        RefundAmount            NVARCHAR(50)    NULL,
        CurrencyCode            NVARCHAR(10)    NULL,
        ReturnedWhen            NVARCHAR(40)    NULL,
        ProcessedWhen           NVARCHAR(40)    NULL,
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlReturnLine_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlReturnLine_Source DEFAULT (N'WWI_OLTP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlReturnLine_Batch ON raw.SqlReturnLine (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.SqlCreditNote', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlCreditNote
    (
        CreditNoteID            NVARCHAR(50)    NULL,
        CreditNoteNumber        NVARCHAR(50)    NULL,
        CustomerID              NVARCHAR(50)    NULL,
        OriginalInvoiceID       NVARCHAR(50)    NULL,
        ReturnAuthorizationID   NVARCHAR(50)    NULL,
        CreditReasonCode        NVARCHAR(20)    NULL,
        CreditNoteDate          NVARCHAR(40)    NULL,
        NetAmount               NVARCHAR(50)    NULL,
        TaxAmount               NVARCHAR(50)    NULL,
        GrossAmount             NVARCHAR(50)    NULL,
        CurrencyCode            NVARCHAR(10)    NULL,
        AppliedToInvoiceID      NVARCHAR(50)    NULL,
        ApprovedBy              NVARCHAR(100)   NULL,
        CreditStatusCode        NVARCHAR(20)    NULL,
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlCreditNote_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlCreditNote_Source DEFAULT (N'WWI_OLTP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlCreditNote_Batch ON raw.SqlCreditNote (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.SqlLoyaltyLedger', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlLoyaltyLedger
    (
        LoyaltyLedgerID         NVARCHAR(50)    NULL,
        LoyaltyMemberID         NVARCHAR(50)    NULL,
        CustomerID              NVARCHAR(50)    NULL,
        ProgramCode             NVARCHAR(30)    NULL,
        TierCode                NVARCHAR(20)    NULL,
        EntryTypeCode           NVARCHAR(20)    NULL,   -- EARN / BURN / EXPIRE / ADJUST / TRANSFER
        PointsDelta             NVARCHAR(50)    NULL,
        PointsBalanceAfter      NVARCHAR(50)    NULL,
        SourceInvoiceID         NVARCHAR(50)    NULL,
        RedemptionReference     NVARCHAR(50)    NULL,
        EntryWhen               NVARCHAR(40)    NULL,
        ExpiryDate              NVARCHAR(40)    NULL,
        RegionCode              NVARCHAR(10)    NULL,   -- points expiry rules differ by region
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlLoyaltyLedger_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlLoyaltyLedger_Source DEFAULT (N'WWI_OLTP'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlLoyaltyLedger_Batch ON raw.SqlLoyaltyLedger (BatchId, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'raw.SqlWebSession', N'U') IS NULL
BEGIN
    CREATE TABLE raw.SqlWebSession
    (
        WebSessionID            NVARCHAR(64)    NULL,
        CustomerID              NVARCHAR(50)    NULL,
        AnonymousVisitorKey     NVARCHAR(64)    NULL,
        SessionStartedWhen      NVARCHAR(40)    NULL,
        SessionEndedWhen        NVARCHAR(40)    NULL,
        LandingPageUrl          NVARCHAR(1000)  NULL,
        ReferrerUrl             NVARCHAR(1000)  NULL,
        CampaignCode            NVARCHAR(50)    NULL,
        DeviceCategory          NVARCHAR(30)    NULL,
        BrowserFamily           NVARCHAR(50)    NULL,
        CountryCode             NVARCHAR(10)    NULL,
        PageViewCount           NVARCHAR(20)    NULL,
        CartCreatedFlag         NVARCHAR(5)     NULL,
        OrderPlacedFlag         NVARCHAR(5)     NULL,
        OrderID                 NVARCHAR(50)    NULL,
        ConsentCategories       NVARCHAR(200)   NULL,   -- EU cookie consent string; empty for NA
        LastEditedWhen          NVARCHAR(40)    NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_rawSqlWebSession_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        SourceSystemCode        NVARCHAR(20)    NOT NULL CONSTRAINT DF_rawSqlWebSession_Source DEFAULT (N'WWI_WEB'),
        SourceRowNumber         BIGINT          NULL
    );

    CREATE CLUSTERED INDEX CIX_rawSqlWebSession_Batch ON raw.SqlWebSession (BatchId, SourceRowNumber);
END;
GO
