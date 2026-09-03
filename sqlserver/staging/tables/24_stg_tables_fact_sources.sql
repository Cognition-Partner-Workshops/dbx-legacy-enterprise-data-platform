/*
    stg.* conformed fact-source tables

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Deploy order  : 24
    Depends on    : 20_stg_tables_master.sql, 21_stg_tables_finance.sql,
                    22_stg_tables_sales.sql, 30_work_tables.sql, 50_ref_tables.sql
    Loaded by     : the stg.usp_Conform*ForFact procedures
    Read by       : FACT_Load_CustomerTransaction, FACT_Load_DailyInventorySnapshot,
                    FACT_Load_DailySalesSnapshot, FACT_Load_GLPosting,
                    FACT_Load_LoyaltyPoints, FACT_Load_Movement,
                    FACT_Load_OrderFulfilment, FACT_Load_Purchase,
                    FACT_Load_PurchaseReceipt, FACT_Load_StockHolding,
                    FACT_Load_SupplierPayment, FACT_Load_SupplierTransaction,
                    FACT_Load_Transaction

    The fact packages were written against a narrower, flatter shape than the
    line-level staging tables carry: numeric business keys, one currency column
    called TransactionCurrency rather than TransactionCurrencyCode, and a single
    LastModifiedAt column that the incremental window is driven from. Rather
    than rewrite thirteen packages, the staging layer publishes that shape here
    and the Conform procedures project the typed tables into it. Nothing is
    re-derived from raw at this level except the loyalty ledger, which the OLTP
    lands directly.

    Two grains are snapshots rather than transactions - stg.StockHolding and
    stg.DailyInventorySnapshot are keyed by PositionDate, stg.DailySalesSnapshot
    by SnapshotDate - and their packages select on that date instead of on a
    watermark, so the whole day is deleted and rebuilt by the load.

    The numeric *BusinessKey columns hold the trailing numeric segment of the
    staging business key. Where the source key is not numeric (partner and file
    feeds, mostly) the conform procedures reject the row rather than hash it
    into a number the warehouse could never trace back.
*/

IF OBJECT_ID(N'stg.CustomerTransaction', N'U') IS NULL
BEGIN
    CREATE TABLE stg.CustomerTransaction
    (
        StagingCustomerTransactionId BIGINT     NOT NULL IDENTITY(1,1),
        CustomerTransactionBusinessKey BIGINT   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        TransactionTypeCode     NVARCHAR(20)    NULL,       -- INV / CRN / PMT / ADJ
        InvoiceNumber           NVARCHAR(50)    NULL,
        TransactionDate         DATE            NULL,
        DueDate                 DATE            NULL,       -- NA net-30, EU net-30, APAC net-60
        TransactionAmount       DECIMAL(19,4)   NULL,
        OutstandingBalance      DECIMAL(19,4)   NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        TransactionCurrency     NCHAR(3)        NULL,
        TransactionAmountUsd    DECIMAL(19,4)   NULL,
        AccountingPeriodCode    NVARCHAR(10)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        LastModifiedAt          DATETIME2(3)    NOT NULL CONSTRAINT DF_stgCustomerTransaction_Modified DEFAULT (SYSUTCDATETIME()),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgCustomerTransaction_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgCustomerTransaction_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgCustomerTransaction PRIMARY KEY CLUSTERED (StagingCustomerTransactionId)
    );

    CREATE INDEX IX_stgCustomerTransaction_Window ON stg.CustomerTransaction (LastModifiedAt) INCLUDE (RegionCode);
    CREATE INDEX IX_stgCustomerTransaction_Customer ON stg.CustomerTransaction (CustomerBusinessKey, TransactionDate);
END;
GO

IF OBJECT_ID(N'stg.SupplierTransaction', N'U') IS NULL
BEGIN
    CREATE TABLE stg.SupplierTransaction
    (
        StagingSupplierTransactionId BIGINT     NOT NULL IDENTITY(1,1),
        SupplierTransactionBusinessKey BIGINT   NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        TransactionTypeCode     NVARCHAR(20)    NULL,       -- INV / CRN / PMT / ACCR
        SupplierInvoiceNumber   NVARCHAR(50)    NULL,
        TransactionDate         DATE            NULL,
        DueDate                 DATE            NULL,
        TransactionAmount       DECIMAL(19,4)   NULL,
        OutstandingBalance      DECIMAL(19,4)   NULL,
        TransactionCurrency     NCHAR(3)        NULL,
        TransactionAmountUsd    DECIMAL(19,4)   NULL,
        IsAccrual               BIT             NOT NULL CONSTRAINT DF_stgSupplierTransaction_Accrual DEFAULT (0),
        AccountingPeriodCode    NVARCHAR(10)    NULL,       -- fiscal calendar differs by region
        LedgerCode              NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        LastModifiedAt          DATETIME2(3)    NOT NULL CONSTRAINT DF_stgSupplierTransaction_Modified DEFAULT (SYSUTCDATETIME()),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgSupplierTransaction_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgSupplierTransaction_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgSupplierTransaction PRIMARY KEY CLUSTERED (StagingSupplierTransactionId)
    );

    CREATE INDEX IX_stgSupplierTransaction_Window ON stg.SupplierTransaction (LastModifiedAt) INCLUDE (LedgerCode);
    CREATE INDEX IX_stgSupplierTransaction_Supplier ON stg.SupplierTransaction (SupplierBusinessKey, TransactionDate);
END;
GO

IF OBJECT_ID(N'stg.Transaction', N'U') IS NULL
BEGIN
    CREATE TABLE stg.[Transaction]
    (
        StagingTransactionId    BIGINT          NOT NULL IDENTITY(1,1),
        TransactionBusinessKey  BIGINT          NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        TransactionTypeCode     NVARCHAR(20)    NULL,
        PartyBusinessKey        NVARCHAR(100)   NULL,       -- customer or supplier, per PartyTypeCode
        PartyTypeCode           NVARCHAR(10)    NULL,       -- CUST / SUPP
        TransactionDate         DATE            NULL,
        AmountExcludingTax      DECIMAL(19,4)   NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        TransactionAmount       DECIMAL(19,4)   NULL,
        TransactionCurrency     NCHAR(3)        NULL,
        TransactionAmountUsd    DECIMAL(19,4)   NULL,
        SourceDocumentNumber    NVARCHAR(50)    NULL,
        AccountingPeriodCode    NVARCHAR(10)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        LastModifiedAt          DATETIME2(3)    NOT NULL CONSTRAINT DF_stgTransaction_Modified DEFAULT (SYSUTCDATETIME()),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgTransaction_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgTransaction_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgTransaction PRIMARY KEY CLUSTERED (StagingTransactionId)
    );

    CREATE INDEX IX_stgTransaction_Window ON stg.[Transaction] (LastModifiedAt) INCLUDE (PartyTypeCode);
    CREATE INDEX IX_stgTransaction_Party ON stg.[Transaction] (PartyTypeCode, PartyBusinessKey, TransactionDate);
END;
GO

IF OBJECT_ID(N'stg.GLPosting', N'U') IS NULL
BEGIN
    CREATE TABLE stg.GLPosting
    (
        StagingGlPostingId      BIGINT          NOT NULL IDENTITY(1,1),
        GlPostingBusinessKey    BIGINT          NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        JournalBatchNumber      NVARCHAR(50)    NULL,
        JournalLineNumber       INT             NULL,
        GlAccountCode           NVARCHAR(40)    NULL,
        CostCentreCode          NVARCHAR(30)    NULL,       -- spelled the British way by the fact package
        LegalEntityCode         NVARCHAR(20)    NULL,       -- first segment of the Oracle account string
        PostingDate             DATE            NULL,
        AccountingPeriodCode    NVARCHAR(10)    NULL,
        DebitAmount             DECIMAL(19,4)   NULL,
        CreditAmount            DECIMAL(19,4)   NULL,
        TransactionCurrency     NCHAR(3)        NULL,
        NetAmountUsd            DECIMAL(19,4)   NULL,
        JournalSourceCode       NVARCHAR(20)    NULL,       -- AP / AR / INV / MANUAL / INTERFACE
        IsPosted                BIT             NOT NULL CONSTRAINT DF_stgGLPosting_Posted DEFAULT (0),
        RegionCode              NVARCHAR(10)    NULL,
        LastModifiedAt          DATETIME2(3)    NOT NULL CONSTRAINT DF_stgGLPosting_Modified DEFAULT (SYSUTCDATETIME()),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgGLPosting_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgGLPosting_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgGLPosting PRIMARY KEY CLUSTERED (StagingGlPostingId)
    );

    CREATE INDEX IX_stgGLPosting_Window ON stg.GLPosting (LastModifiedAt);
    CREATE INDEX IX_stgGLPosting_Batch ON stg.GLPosting (JournalBatchNumber, JournalLineNumber);
END;
GO

IF OBJECT_ID(N'stg.LoyaltyPoints', N'U') IS NULL
BEGIN
    CREATE TABLE stg.LoyaltyPoints
    (
        StagingLoyaltyPointsId  BIGINT          NOT NULL IDENTITY(1,1),
        LoyaltyEventBusinessKey BIGINT          NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        LoyaltyMemberId         NVARCHAR(50)    NULL,
        LoyaltyProgramCode      NVARCHAR(30)    NULL,
        TierCode                NVARCHAR(20)    NULL,
        EventTypeCode           NVARCHAR(20)    NULL,       -- EARN / REDEEM / EXPIRE / ADJUST
        EventDate               DATE            NULL,
        PointsQuantity          INT             NULL,       -- signed; redemptions are negative
        PointsBalanceAfter      INT             NULL,
        QualifyingSpendAmount   DECIMAL(19,4)   NULL,
        TransactionCurrency     NCHAR(3)        NULL,
        QualifyingSpendAmountUsd DECIMAL(19,4)  NULL,
        SourceInvoiceNumber     NVARCHAR(50)    NULL,
        RedemptionReference     NVARCHAR(50)    NULL,
        ExpiryDate              DATE            NULL,       -- NA 24m rolling, EU 12m calendar, APAC 36m
        ExpiryRuleCode          NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        LastModifiedAt          DATETIME2(3)    NOT NULL CONSTRAINT DF_stgLoyaltyPoints_Modified DEFAULT (SYSUTCDATETIME()),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgLoyaltyPoints_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgLoyaltyPoints_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgLoyaltyPoints PRIMARY KEY CLUSTERED (StagingLoyaltyPointsId)
    );

    CREATE INDEX IX_stgLoyaltyPoints_Window ON stg.LoyaltyPoints (LastModifiedAt);
    CREATE INDEX IX_stgLoyaltyPoints_Customer ON stg.LoyaltyPoints (CustomerBusinessKey, EventDate);
END;
GO

IF OBJECT_ID(N'stg.Movement', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Movement
    (
        StagingMovementId       BIGINT          NOT NULL IDENTITY(1,1),
        MovementBusinessKey     BIGINT          NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        StockItemBusinessKey    BIGINT          NULL,
        MovementTypeCode        NVARCHAR(20)    NULL,       -- RECEIPT / ISSUE / ADJUST / TRANSFER / RETURN
        MovementDate            DATE            NULL,
        QuantityMoved           DECIMAL(18,4)   NULL,       -- signed by MovementDirection
        MovementValueAmount     DECIMAL(19,4)   NULL,
        FromLocationCode        NVARCHAR(30)    NULL,
        ToLocationCode          NVARCHAR(30)    NULL,
        ReasonCode              NVARCHAR(20)    NULL,
        ReversesMovementKey     BIGINT          NULL,       -- set for the ADJUST pairs the WMS writes
        RegionCode              NVARCHAR(10)    NULL,
        LastModifiedAt          DATETIME2(3)    NOT NULL CONSTRAINT DF_stgMovement_Modified DEFAULT (SYSUTCDATETIME()),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgMovement_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgMovement_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgMovement PRIMARY KEY CLUSTERED (StagingMovementId)
    );

    CREATE INDEX IX_stgMovement_Window ON stg.Movement (LastModifiedAt);
    CREATE INDEX IX_stgMovement_Item ON stg.Movement (StockItemBusinessKey, MovementDate);
END;
GO

IF OBJECT_ID(N'stg.OrderFulfilment', N'U') IS NULL
BEGIN
    CREATE TABLE stg.OrderFulfilment
    (
        StagingOrderFulfilmentId BIGINT         NOT NULL IDENTITY(1,1),
        OrderNumber             NVARCHAR(50)    NOT NULL,   -- the accumulating snapshot grain
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        OrderBusinessKey        NVARCHAR(100)   NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        OrderedAt               DATE            NULL,
        AllocatedAt             DATE            NULL,
        PickedAt                DATE            NULL,
        InvoicedAt              DATE            NULL,
        CashReceivedAt          DATE            NULL,
        OrderNetAmount          DECIMAL(19,4)   NULL,
        InvoicedAmount          DECIMAL(19,4)   NULL,
        CashReceivedAmount      DECIMAL(19,4)   NULL,
        TransactionCurrency     NCHAR(3)        NULL,
        OrderNetAmountUsd       DECIMAL(19,4)   NULL,
        DaysOrderToInvoice      INT             NULL,
        DaysInvoiceToCash       INT             NULL,
        FulfilmentStatusCode    NVARCHAR(20)    NULL,       -- OPEN / PICKED / INVOICED / SETTLED
        RegionCode              NVARCHAR(10)    NULL,
        LastModifiedAt          DATETIME2(3)    NOT NULL CONSTRAINT DF_stgOrderFulfilment_Modified DEFAULT (SYSUTCDATETIME()),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgOrderFulfilment_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgOrderFulfilment_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgOrderFulfilment PRIMARY KEY CLUSTERED (StagingOrderFulfilmentId)
    );

    CREATE INDEX IX_stgOrderFulfilment_Window ON stg.OrderFulfilment (LastModifiedAt);
    CREATE INDEX IX_stgOrderFulfilment_Order ON stg.OrderFulfilment (OrderNumber, BatchId);
END;
GO

IF OBJECT_ID(N'stg.Purchase', N'U') IS NULL
BEGIN
    CREATE TABLE stg.Purchase
    (
        StagingPurchaseId       BIGINT          NOT NULL IDENTITY(1,1),
        PurchaseOrderLineBusinessKey BIGINT     NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        PurchaseOrderNumber     NVARCHAR(50)    NULL,
        PurchaseOrderLineNumber INT             NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        StockItemBusinessKey    BIGINT          NULL,
        OrderPlacedDate         DATE            NULL,
        ExpectedReceiptDate     DATE            NULL,
        QuantityOrdered         DECIMAL(18,4)   NULL,
        UnitCostAmount          DECIMAL(19,4)   NULL,
        FreightAmount           DECIMAL(19,4)   NULL,       -- header freight apportioned by line value
        TransactionCurrency     NCHAR(3)        NULL,
        ExtendedAmountUsd       DECIMAL(19,4)   NULL,
        RecoverableTaxAmount    DECIMAL(19,4)   NULL,       -- EU input VAT, APAC GST credit, zero in NA
        SupplierRegionCode      NVARCHAR(10)    NULL,
        BuyerCode               NVARCHAR(20)    NULL,
        ThreeWayMatchStatusCode NVARCHAR(20)    NULL,
        LastModifiedAt          DATETIME2(3)    NOT NULL CONSTRAINT DF_stgPurchase_Modified DEFAULT (SYSUTCDATETIME()),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgPurchase_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgPurchase_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgPurchase PRIMARY KEY CLUSTERED (StagingPurchaseId)
    );

    CREATE INDEX IX_stgPurchase_Window ON stg.Purchase (LastModifiedAt);
    CREATE INDEX IX_stgPurchase_Order ON stg.Purchase (PurchaseOrderNumber, PurchaseOrderLineNumber);
END;
GO

IF OBJECT_ID(N'stg.PurchaseReceipt', N'U') IS NULL
BEGIN
    CREATE TABLE stg.PurchaseReceipt
    (
        StagingPurchaseReceiptId BIGINT         NOT NULL IDENTITY(1,1),
        ReceiptBusinessKey      BIGINT          NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        ReceiptNumber           NVARCHAR(50)    NULL,
        PurchaseOrderNumber     NVARCHAR(50)    NULL,
        PurchaseOrderLineNumber INT             NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        StockItemBusinessKey    BIGINT          NULL,
        OrderRaisedDate         DATE            NULL,
        GoodsReceivedDate       DATE            NULL,
        InvoiceReceivedDate     DATE            NULL,
        PaymentSettledDate      DATE            NULL,
        QuantityOrdered         DECIMAL(18,4)   NULL,
        QuantityReceived        DECIMAL(18,4)   NULL,
        ReceivedCostAmount      DECIMAL(19,4)   NULL,
        InvoicedAmount          DECIMAL(19,4)   NULL,
        TransactionCurrency     NCHAR(3)        NULL,
        ReceivedCostAmountUsd   DECIMAL(19,4)   NULL,
        DaysOrderToReceipt      INT             NULL,
        DaysReceiptToInvoice    INT             NULL,
        LastModifiedAt          DATETIME2(3)    NOT NULL CONSTRAINT DF_stgPurchaseReceipt_Modified DEFAULT (SYSUTCDATETIME()),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgPurchaseReceipt_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgPurchaseReceipt_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgPurchaseReceipt PRIMARY KEY CLUSTERED (StagingPurchaseReceiptId)
    );

    CREATE INDEX IX_stgPurchaseReceipt_Window ON stg.PurchaseReceipt (LastModifiedAt);
    CREATE INDEX IX_stgPurchaseReceipt_Order ON stg.PurchaseReceipt (PurchaseOrderNumber, PurchaseOrderLineNumber);
END;
GO

IF OBJECT_ID(N'stg.SupplierPayment', N'U') IS NULL
BEGIN
    CREATE TABLE stg.SupplierPayment
    (
        StagingSupplierPaymentId BIGINT         NOT NULL IDENTITY(1,1),
        SupplierPaymentBusinessKey BIGINT       NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        PaymentReference        NVARCHAR(50)    NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        SupplierInvoiceNumber   NVARCHAR(50)    NULL,
        InvoiceDate             DATE            NULL,
        SettlementDate          DATE            NULL,
        InvoiceAmount           DECIMAL(19,4)   NULL,
        SettledAmount           DECIMAL(19,4)   NULL,
        SettlementDiscountAmount DECIMAL(19,4)  NULL,
        TransactionCurrency     NCHAR(3)        NULL,
        SettledAmountUsd        DECIMAL(19,4)   NULL,
        RealizedFxGainLossUsd   DECIMAL(19,4)   NULL,
        PaymentRunCode          NVARCHAR(30)    NULL,       -- NA check run, EU SEPA file, APAC BPAY batch
        PaymentMethodCode       NVARCHAR(20)    NULL,
        MatchStatusCode         NVARCHAR(20)    NULL,
        RegionCode              NVARCHAR(10)    NULL,
        LastModifiedAt          DATETIME2(3)    NOT NULL CONSTRAINT DF_stgSupplierPayment_Modified DEFAULT (SYSUTCDATETIME()),
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgSupplierPayment_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgSupplierPayment_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgSupplierPayment PRIMARY KEY CLUSTERED (StagingSupplierPaymentId)
    );

    CREATE INDEX IX_stgSupplierPayment_Window ON stg.SupplierPayment (LastModifiedAt);
    CREATE INDEX IX_stgSupplierPayment_Supplier ON stg.SupplierPayment (SupplierBusinessKey, SettlementDate);
END;
GO

IF OBJECT_ID(N'stg.StockHolding', N'U') IS NULL
BEGIN
    CREATE TABLE stg.StockHolding
    (
        StagingStockHoldingId   BIGINT          NOT NULL IDENTITY(1,1),
        StockItemBusinessKey    BIGINT          NOT NULL,
        WarehouseCode           NVARCHAR(30)    NOT NULL,
        PositionDate            DATE            NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        QuantityOnHand          DECIMAL(18,4)   NULL,
        QuantityAllocated       DECIMAL(18,4)   NULL,
        QuantityOnOrder         DECIMAL(18,4)   NULL,
        UnitCostAmount          DECIMAL(19,4)   NULL,
        StockValueAmountUsd     DECIMAL(19,4)   NULL,
        ReorderLevel            DECIMAL(18,4)   NULL,
        TargetStockLevel        DECIMAL(18,4)   NULL,
        LastStocktakeDate       DATE            NULL,
        NegativeBalanceFlag     BIT             NOT NULL CONSTRAINT DF_stgStockHolding_Negative DEFAULT (0),
        RegionCode              NVARCHAR(10)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgStockHolding_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgStockHolding_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgStockHolding PRIMARY KEY CLUSTERED (StagingStockHoldingId)
    );

    CREATE INDEX IX_stgStockHolding_Position
        ON stg.StockHolding (PositionDate, StockItemBusinessKey, WarehouseCode);
END;
GO

IF OBJECT_ID(N'stg.DailyInventorySnapshot', N'U') IS NULL
BEGIN
    CREATE TABLE stg.DailyInventorySnapshot
    (
        StagingDailyInventoryId BIGINT          NOT NULL IDENTITY(1,1),
        StockItemBusinessKey    BIGINT          NOT NULL,
        WarehouseCode           NVARCHAR(30)    NOT NULL,
        PositionDate            DATE            NOT NULL,
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        QuantityOnHand          DECIMAL(18,4)   NULL,
        StockValueAmount        DECIMAL(19,4)   NULL,
        DaysOfCover             INT             NULL,       -- capped at 999 for items with no offtake
        QuantityAgedOver90Days  DECIMAL(18,4)   NULL,
        ObsolescenceProvisionAmount DECIMAL(19,4) NULL,     -- provision ladder differs by region
        ProvisionRuleCode       NVARCHAR(20)    NULL,       -- NA_LIFO / EU_IAS2 / APAC_LOCAL
        RegionCode              NVARCHAR(10)    NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgDailyInventory_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgDailyInventory_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgDailyInventorySnapshot PRIMARY KEY CLUSTERED (StagingDailyInventoryId)
    );

    CREATE INDEX IX_stgDailyInventorySnapshot_Position
        ON stg.DailyInventorySnapshot (PositionDate, StockItemBusinessKey, WarehouseCode);
END;
GO

IF OBJECT_ID(N'stg.DailySalesSnapshot', N'U') IS NULL
BEGIN
    CREATE TABLE stg.DailySalesSnapshot
    (
        StagingDailySalesId     BIGINT          NOT NULL IDENTITY(1,1),
        SnapshotDate            DATE            NOT NULL,
        CustomerKey             INT             NOT NULL,   -- warehouse surrogate, -1 when unresolved
        StockItemKey            INT             NOT NULL,
        InvoiceDateKey          INT             NOT NULL,   -- yyyymmdd
        SourceSystemCode        NVARCHAR(20)    NOT NULL,
        RegionCode              NVARCHAR(10)    NULL,
        Quantity                DECIMAL(18,4)   NULL,
        GrossAmount             DECIMAL(19,4)   NULL,
        DiscountAmount          DECIMAL(19,4)   NULL,
        NetAmount               DECIMAL(19,4)   NULL,
        TaxAmount               DECIMAL(19,4)   NULL,
        TotalCostAmount         DECIMAL(19,4)   NULL,
        MarginAmount            DECIMAL(19,4)   NULL,
        MarginPercent           DECIMAL(9,4)    NULL,
        InvoiceLineCount        INT             NULL,
        DqStatusCode            NVARCHAR(20)    NOT NULL CONSTRAINT DF_stgDailySales_DqStatus DEFAULT (N'PASS'),
        RowHash                 BINARY(32)      NULL,
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        LoadedAtUtc             DATETIME2(3)    NOT NULL CONSTRAINT DF_stgDailySales_LoadedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_stgDailySalesSnapshot PRIMARY KEY CLUSTERED (StagingDailySalesId)
    );

    CREATE INDEX IX_stgDailySalesSnapshot_Snapshot
        ON stg.DailySalesSnapshot (SnapshotDate, CustomerKey, StockItemKey);
END;
GO
