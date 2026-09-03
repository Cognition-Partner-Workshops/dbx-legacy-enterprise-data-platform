/*
    err.* reject tables

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Deploy order  : 40
    Depends on    : 10_raw_tables_oracle.sql, 11_raw_tables_sqlserver.sql,
                    12_raw_tables_file.sql
    Written by    : the stg.usp_* load procedures and err.usp_LogRejectedRows
    Purged by     : err.usp_PurgeRejectedRows

    Every reject table keeps the original payload as JSON so a rejected row can be
    corrected and replayed without going back to the source system. The columns
    line up with etl.RejectedRecord (RejectReasonCode, RejectReason, RejectStage)
    so err.vw_RejectSummaryByBatch can union them with the control-framework view
    of the same batch.

    Reprocessing status lifecycle: NEW -> UNDER_REVIEW -> CORRECTED -> REPLAYED,
    or NEW -> WONTFIX for the rows the business has agreed to abandon.
*/

IF OBJECT_ID(N'err.RejectedCustomer', N'U') IS NULL
BEGIN
    CREATE TABLE err.RejectedCustomer
    (
        RejectId                BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        SourceSystemCode        NVARCHAR(20)    NULL,
        SourceCustomerId        NVARCHAR(50)    NULL,
        CustomerBusinessKey     NVARCHAR(100)   NULL,
        CustomerName            NVARCHAR(400)   NULL,
        RejectReasonCode        NVARCHAR(50)    NOT NULL,   -- MISSING_NAME / BAD_COUNTRY / DUP_TAXNUM / NO_CONSENT
        RejectReason            NVARCHAR(500)   NULL,
        RejectStage             NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedCustomer_Stage DEFAULT (N'Stage'),
        FailedColumnName        NVARCHAR(100)   NULL,
        FailedValue             NVARCHAR(400)   NULL,
        RegionCode              NVARCHAR(10)    NULL,
        RecordPayload           NVARCHAR(MAX)   NULL,
        ReprocessStatusCode     NVARCHAR(20)    NOT NULL CONSTRAINT DF_errRejectedCustomer_Status DEFAULT (N'NEW'),
        ReprocessAttemptCount   SMALLINT        NOT NULL CONSTRAINT DF_errRejectedCustomer_Attempts DEFAULT (0),
        ReprocessedInBatchId    BIGINT          NULL,
        ReviewedByName          NVARCHAR(100)   NULL,
        ReviewNote              NVARCHAR(1000)  NULL,
        RejectedAtUtc           DATETIME2(3)    NOT NULL CONSTRAINT DF_errRejectedCustomer_At DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_errRejectedCustomer PRIMARY KEY CLUSTERED (RejectId)
    );

    CREATE INDEX IX_errRejectedCustomer_Batch ON err.RejectedCustomer (BatchId, RejectReasonCode);
END;
GO

IF OBJECT_ID(N'err.RejectedSupplier', N'U') IS NULL
BEGIN
    CREATE TABLE err.RejectedSupplier
    (
        RejectId                BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        SourceSystemCode        NVARCHAR(20)    NULL,
        SourceSupplierId        NVARCHAR(50)    NULL,
        SupplierBusinessKey     NVARCHAR(100)   NULL,
        SupplierName            NVARCHAR(400)   NULL,
        TaxIdentifier           NVARCHAR(50)    NULL,
        RejectReasonCode        NVARCHAR(50)    NOT NULL,   -- BAD_TAXID / MISSING_TERMS / UNKNOWN_CURRENCY
        RejectReason            NVARCHAR(500)   NULL,
        RejectStage             NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedSupplier_Stage DEFAULT (N'Stage'),
        FailedColumnName        NVARCHAR(100)   NULL,
        FailedValue             NVARCHAR(400)   NULL,
        RegionCode              NVARCHAR(10)    NULL,
        RecordPayload           NVARCHAR(MAX)   NULL,
        ReprocessStatusCode     NVARCHAR(20)    NOT NULL CONSTRAINT DF_errRejectedSupplier_Status DEFAULT (N'NEW'),
        ReprocessAttemptCount   SMALLINT        NOT NULL CONSTRAINT DF_errRejectedSupplier_Attempts DEFAULT (0),
        ReprocessedInBatchId    BIGINT          NULL,
        RejectedAtUtc           DATETIME2(3)    NOT NULL CONSTRAINT DF_errRejectedSupplier_At DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_errRejectedSupplier PRIMARY KEY CLUSTERED (RejectId)
    );
END;
GO

IF OBJECT_ID(N'err.RejectedProduct', N'U') IS NULL
BEGIN
    CREATE TABLE err.RejectedProduct
    (
        RejectId                BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        SourceSystemCode        NVARCHAR(20)    NULL,
        SourceProductId         NVARCHAR(50)    NULL,
        ProductBusinessKey      NVARCHAR(100)   NULL,
        ProductName             NVARCHAR(400)   NULL,
        Barcode                 NVARCHAR(30)    NULL,
        RejectReasonCode        NVARCHAR(50)    NOT NULL,   -- BAD_UOM / NEGATIVE_COST / UNMATCHED_XREF
        RejectReason            NVARCHAR(500)   NULL,
        RejectStage             NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedProduct_Stage DEFAULT (N'Stage'),
        FailedColumnName        NVARCHAR(100)   NULL,
        FailedValue             NVARCHAR(400)   NULL,
        RecordPayload           NVARCHAR(MAX)   NULL,
        ReprocessStatusCode     NVARCHAR(20)    NOT NULL CONSTRAINT DF_errRejectedProduct_Status DEFAULT (N'NEW'),
        ReprocessAttemptCount   SMALLINT        NOT NULL CONSTRAINT DF_errRejectedProduct_Attempts DEFAULT (0),
        ReprocessedInBatchId    BIGINT          NULL,
        RejectedAtUtc           DATETIME2(3)    NOT NULL CONSTRAINT DF_errRejectedProduct_At DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_errRejectedProduct PRIMARY KEY CLUSTERED (RejectId)
    );
END;
GO

IF OBJECT_ID(N'err.RejectedOrderLine', N'U') IS NULL
BEGIN
    CREATE TABLE err.RejectedOrderLine
    (
        RejectId                BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        SourceSystemCode        NVARCHAR(20)    NULL,
        OrderBusinessKey        NVARCHAR(100)   NULL,
        OrderLineBusinessKey    NVARCHAR(120)   NULL,
        LineNumber              NVARCHAR(20)    NULL,
        StockItemReference      NVARCHAR(60)    NULL,
        OrderedQuantityText     NVARCHAR(50)    NULL,       -- kept as text: the value may be why it rejected
        UnitPriceText           NVARCHAR(50)    NULL,
        RejectReasonCode        NVARCHAR(50)    NOT NULL,   -- BAD_NUMERIC / NEG_QTY / ORPHAN_HEADER / DUP_LINE
        RejectReason            NVARCHAR(500)   NULL,
        RejectStage             NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedOrderLine_Stage DEFAULT (N'Stage'),
        RecordPayload           NVARCHAR(MAX)   NULL,
        ReprocessStatusCode     NVARCHAR(20)    NOT NULL CONSTRAINT DF_errRejectedOrderLine_Status DEFAULT (N'NEW'),
        ReprocessAttemptCount   SMALLINT        NOT NULL CONSTRAINT DF_errRejectedOrderLine_Attempts DEFAULT (0),
        ReprocessedInBatchId    BIGINT          NULL,
        RejectedAtUtc           DATETIME2(3)    NOT NULL CONSTRAINT DF_errRejectedOrderLine_At DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_errRejectedOrderLine PRIMARY KEY CLUSTERED (RejectId)
    );

    CREATE INDEX IX_errRejectedOrderLine_Batch ON err.RejectedOrderLine (BatchId, RejectReasonCode);
END;
GO

IF OBJECT_ID(N'err.RejectedInvoiceLine', N'U') IS NULL
BEGIN
    CREATE TABLE err.RejectedInvoiceLine
    (
        RejectId                BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        SourceSystemCode        NVARCHAR(20)    NULL,
        InvoiceBusinessKey      NVARCHAR(100)   NULL,
        InvoiceLineBusinessKey  NVARCHAR(120)   NULL,
        InvoiceNumber           NVARCHAR(50)    NULL,
        LineNumber              NVARCHAR(20)    NULL,
        TaxCode                 NVARCHAR(20)    NULL,
        LineAmountText          NVARCHAR(50)    NULL,
        RejectReasonCode        NVARCHAR(50)    NOT NULL,   -- TAX_MISMATCH / UNKNOWN_TAXCODE / NO_PO_MATCH
        RejectReason            NVARCHAR(500)   NULL,
        RejectStage             NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedInvoiceLine_Stage DEFAULT (N'Stage'),
        ExpectedTaxAmount       DECIMAL(19,4)   NULL,
        ActualTaxAmount         DECIMAL(19,4)   NULL,
        VarianceAmount          DECIMAL(19,4)   NULL,
        RecordPayload           NVARCHAR(MAX)   NULL,
        ReprocessStatusCode     NVARCHAR(20)    NOT NULL CONSTRAINT DF_errRejectedInvoiceLine_Status DEFAULT (N'NEW'),
        ReprocessAttemptCount   SMALLINT        NOT NULL CONSTRAINT DF_errRejectedInvoiceLine_Attempts DEFAULT (0),
        ReprocessedInBatchId    BIGINT          NULL,
        RejectedAtUtc           DATETIME2(3)    NOT NULL CONSTRAINT DF_errRejectedInvoiceLine_At DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_errRejectedInvoiceLine PRIMARY KEY CLUSTERED (RejectId)
    );
END;
GO

IF OBJECT_ID(N'err.RejectedPayment', N'U') IS NULL
BEGIN
    CREATE TABLE err.RejectedPayment
    (
        RejectId                BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        SourceSystemCode        NVARCHAR(20)    NULL,
        PaymentBusinessKey      NVARCHAR(100)   NULL,
        PaymentNumber           NVARCHAR(50)    NULL,
        SupplierReference       NVARCHAR(60)    NULL,
        PaymentAmountText       NVARCHAR(50)    NULL,
        CurrencyCode            NVARCHAR(10)    NULL,
        RejectReasonCode        NVARCHAR(50)    NOT NULL,   -- NO_FX_RATE / UNKNOWN_SUPPLIER / OVERAPPLIED
        RejectReason            NVARCHAR(500)   NULL,
        RejectStage             NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedPayment_Stage DEFAULT (N'Stage'),
        UnappliedAmount         DECIMAL(19,4)   NULL,
        RecordPayload           NVARCHAR(MAX)   NULL,
        ReprocessStatusCode     NVARCHAR(20)    NOT NULL CONSTRAINT DF_errRejectedPayment_Status DEFAULT (N'NEW'),
        ReprocessAttemptCount   SMALLINT        NOT NULL CONSTRAINT DF_errRejectedPayment_Attempts DEFAULT (0),
        ReprocessedInBatchId    BIGINT          NULL,
        RejectedAtUtc           DATETIME2(3)    NOT NULL CONSTRAINT DF_errRejectedPayment_At DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_errRejectedPayment PRIMARY KEY CLUSTERED (RejectId)
    );
END;
GO

IF OBJECT_ID(N'err.RejectedShipment', N'U') IS NULL
BEGIN
    CREATE TABLE err.RejectedShipment
    (
        RejectId                BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        SourceSystemCode        NVARCHAR(20)    NULL,
        ShipmentBusinessKey     NVARCHAR(100)   NULL,
        ShipmentReference       NVARCHAR(50)    NULL,
        CarrierCode             NVARCHAR(30)    NULL,
        ScanEventCode           NVARCHAR(20)    NULL,
        ScanTimestampText       NVARCHAR(40)    NULL,       -- carrier feeds send at least four formats
        RejectReasonCode        NVARCHAR(50)    NOT NULL,   -- BAD_TIMESTAMP / UNKNOWN_SHIPMENT / OUT_OF_ORDER_SCAN
        RejectReason            NVARCHAR(500)   NULL,
        RejectStage             NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedShipment_Stage DEFAULT (N'Stage'),
        RecordPayload           NVARCHAR(MAX)   NULL,
        ReprocessStatusCode     NVARCHAR(20)    NOT NULL CONSTRAINT DF_errRejectedShipment_Status DEFAULT (N'NEW'),
        ReprocessAttemptCount   SMALLINT        NOT NULL CONSTRAINT DF_errRejectedShipment_Attempts DEFAULT (0),
        ReprocessedInBatchId    BIGINT          NULL,
        RejectedAtUtc           DATETIME2(3)    NOT NULL CONSTRAINT DF_errRejectedShipment_At DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_errRejectedShipment PRIMARY KEY CLUSTERED (RejectId)
    );
END;
GO

IF OBJECT_ID(N'err.RejectedFileRow', N'U') IS NULL
BEGIN
    CREATE TABLE err.RejectedFileRow
    (
        RejectId                BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        SourceSystemCode        NVARCHAR(20)    NULL,
        SourceFileName          NVARCHAR(260)   NULL,
        FileFormatVersion       NVARCHAR(10)    NULL,
        SourceRowNumber         BIGINT          NULL,
        RawRowText              NVARCHAR(MAX)   NULL,       -- the delimited line exactly as received
        ExpectedColumnCount     SMALLINT        NULL,
        ActualColumnCount       SMALLINT        NULL,
        DelimiterUsed           NVARCHAR(10)    NULL,
        DecimalSeparatorUsed    NVARCHAR(5)     NULL,       -- EU partners still send decimal commas
        DateFormatAssumed       NVARCHAR(20)    NULL,
        RejectReasonCode        NVARCHAR(50)    NOT NULL,   -- COLUMN_COUNT / UNPARSEABLE_DATE / BAD_ENCODING
        RejectReason            NVARCHAR(500)   NULL,
        RejectStage             NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedFileRow_Stage DEFAULT (N'Extract'),
        ReprocessStatusCode     NVARCHAR(20)    NOT NULL CONSTRAINT DF_errRejectedFileRow_Status DEFAULT (N'NEW'),
        ReprocessAttemptCount   SMALLINT        NOT NULL CONSTRAINT DF_errRejectedFileRow_Attempts DEFAULT (0),
        ReprocessedInBatchId    BIGINT          NULL,
        RejectedAtUtc           DATETIME2(3)    NOT NULL CONSTRAINT DF_errRejectedFileRow_At DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_errRejectedFileRow PRIMARY KEY CLUSTERED (RejectId)
    );

    CREATE INDEX IX_errRejectedFileRow_File ON err.RejectedFileRow (SourceFileName, SourceRowNumber);
END;
GO

IF OBJECT_ID(N'err.RejectedLookupFailure', N'U') IS NULL
BEGIN
    CREATE TABLE err.RejectedLookupFailure
    (
        RejectId                BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        SourceObjectName        NVARCHAR(200)   NOT NULL,
        SourceBusinessKey       NVARCHAR(140)   NULL,
        LookupName              NVARCHAR(100)   NOT NULL,   -- Currency / TaxCode / Geography / StockItem
        LookupColumnName        NVARCHAR(100)   NULL,
        LookupValue             NVARCHAR(200)   NULL,
        SourceSystemCode        NVARCHAR(20)    NULL,
        RejectReasonCode        NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedLookupFailure_Code DEFAULT (N'LOOKUP_MISS'),
        RejectReason            NVARCHAR(500)   NULL,
        RejectStage             NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedLookupFailure_Stage DEFAULT (N'Transform'),
        RoutedToUnknownMember   BIT             NOT NULL CONSTRAINT DF_errRejectedLookupFailure_Unknown DEFAULT (0),
        QueuedForLateArrival    BIT             NOT NULL CONSTRAINT DF_errRejectedLookupFailure_Queued DEFAULT (0),
        OccurrenceCount         INT             NOT NULL CONSTRAINT DF_errRejectedLookupFailure_Count DEFAULT (1),
        RecordPayload           NVARCHAR(MAX)   NULL,
        ReprocessStatusCode     NVARCHAR(20)    NOT NULL CONSTRAINT DF_errRejectedLookupFailure_Status DEFAULT (N'NEW'),
        RejectedAtUtc           DATETIME2(3)    NOT NULL CONSTRAINT DF_errRejectedLookupFailure_At DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_errRejectedLookupFailure PRIMARY KEY CLUSTERED (RejectId)
    );

    CREATE INDEX IX_errRejectedLookupFailure_Lookup ON err.RejectedLookupFailure (LookupName, LookupValue);
END;
GO

IF OBJECT_ID(N'err.RejectedConstraintViolation', N'U') IS NULL
BEGIN
    CREATE TABLE err.RejectedConstraintViolation
    (
        RejectId                BIGINT          NOT NULL IDENTITY(1,1),
        BatchId                 BIGINT          NOT NULL,
        PackageExecutionId      BIGINT          NULL,
        TargetObjectName        NVARCHAR(200)   NOT NULL,
        ConstraintName          NVARCHAR(200)   NULL,
        ConstraintTypeCode      NVARCHAR(20)    NULL,       -- PK / UQ / CK / FK / NOTNULL
        ViolatingBusinessKey    NVARCHAR(140)   NULL,
        ViolatingColumnName     NVARCHAR(100)   NULL,
        ViolatingValue          NVARCHAR(400)   NULL,
        SqlErrorNumber          INT             NULL,
        SqlErrorMessage         NVARCHAR(2000)  NULL,
        RejectReasonCode        NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedConstraint_Code DEFAULT (N'CONSTRAINT'),
        RejectReason            NVARCHAR(500)   NULL,
        RejectStage             NVARCHAR(50)    NOT NULL CONSTRAINT DF_errRejectedConstraint_Stage DEFAULT (N'Load'),
        RecordPayload           NVARCHAR(MAX)   NULL,
        ReprocessStatusCode     NVARCHAR(20)    NOT NULL CONSTRAINT DF_errRejectedConstraint_Status DEFAULT (N'NEW'),
        RejectedAtUtc           DATETIME2(3)    NOT NULL CONSTRAINT DF_errRejectedConstraint_At DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_errRejectedConstraintViolation PRIMARY KEY CLUSTERED (RejectId)
    );

    CREATE INDEX IX_errRejectedConstraint_Target ON err.RejectedConstraintViolation (TargetObjectName, BatchId);
END;
GO
