/*
    Object        : [Integration].[DimensionKeyRegistry], [Integration].[DimensionLoadAudit],
                    [Integration].[InferredMemberQueue]
    Deploy target : WideWorldImportersDW
    Deploy order  : 2 (after 00_dimension_schemas_and_sequences.sql)
    Depends on    : Dimension / Integration schemas
    Called by     : Integration.usp_AllocateDimensionKeyRange,
                    Integration.usp_InsertInferredMember,
                    Integration.usp_EnrichInferredMembers,
                    every Integration.usp_MigrateStaged*Data procedure

    The key registry is the 2014 addition that let the fact loads allocate a block
    of surrogate keys up front instead of round-tripping to a sequence per row. It
    coexists with the sequences rather than replacing them: dimensions loaded by
    the nightly SCD procedures take keys from their sequence, and dimensions that
    can receive inferred members take theirs from a registry block. The registry
    also records the reserved range so nothing hands out a key inside it.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Integration.DimensionKeyRegistry', N'U') IS NULL
BEGIN
    CREATE TABLE [Integration].[DimensionKeyRegistry]
    (
        [Dimension Name]        NVARCHAR(100)   NOT NULL,
        [Key Column Name]       NVARCHAR(100)   NOT NULL,
        [Allocation Method]     NVARCHAR(20)    NOT NULL,
        [Sequence Name]         SYSNAME         NULL,
        [Reserved Key Low]      INT             NOT NULL CONSTRAINT [DF_DimensionKeyRegistry_ReservedLow] DEFAULT (-9),
        [Reserved Key High]     INT             NOT NULL CONSTRAINT [DF_DimensionKeyRegistry_ReservedHigh] DEFAULT (0),
        [Next Key]              INT             NOT NULL CONSTRAINT [DF_DimensionKeyRegistry_NextKey] DEFAULT (1),
        [Block Size]            INT             NOT NULL CONSTRAINT [DF_DimensionKeyRegistry_BlockSize] DEFAULT (1000),
        [Supports Inferred]     BIT             NOT NULL CONSTRAINT [DF_DimensionKeyRegistry_Inferred] DEFAULT (0),
        [SCD Pattern]           NVARCHAR(20)    NOT NULL,
        [Last Allocated On]     DATETIME2(7)    NULL,
        [Last Allocated By]     NVARCHAR(128)   NULL,
        CONSTRAINT [PK_Integration_DimensionKeyRegistry] PRIMARY KEY CLUSTERED ([Dimension Name] ASC),
        CONSTRAINT [CK_Integration_DimensionKeyRegistry_Method]
            CHECK ([Allocation Method] IN (N'Sequence', N'Identity', N'Registry')),
        CONSTRAINT [CK_Integration_DimensionKeyRegistry_Pattern]
            CHECK ([SCD Pattern] IN (N'SCD1', N'SCD2', N'Hybrid', N'Junk', N'Mini',
                                     N'Static', N'Bridge', N'Outrigger'))
    );
END;
GO

/*
    One row per dimension. Deliberately data, not code: the operations team edits
    this table when a dimension changes pattern, and the load procedures read it
    rather than hard-coding the pattern.
*/
MERGE [Integration].[DimensionKeyRegistry] AS tgt
USING (VALUES
    (N'Dimension.Customer',              N'Customer Key',              N'Sequence', N'CustomerKey',            1, N'Hybrid'),
    (N'Dimension.Customer Category',     N'Customer Category Key',     N'Sequence', N'CustomerCategoryKey',    0, N'SCD1'),
    (N'Dimension.Buying Group',          N'Buying Group Key',          N'Sequence', N'BuyingGroupKey',         0, N'SCD2'),
    (N'Dimension.Customer Segment',      N'Customer Segment Key',      N'Sequence', N'CustomerSegmentKey',     0, N'SCD2'),
    (N'Dimension.Supplier',              N'Supplier Key',              N'Sequence', N'SupplierKey',            1, N'Hybrid'),
    (N'Dimension.Supplier Category',     N'Supplier Category Key',     N'Sequence', N'SupplierCategoryKey',    0, N'SCD1'),
    (N'Dimension.Vendor Contract',       N'Vendor Contract Key',       N'Sequence', N'VendorContractKey',      0, N'SCD2'),
    (N'Dimension.Stock Item',            N'Stock Item Key',            N'Sequence', N'StockItemKey',           1, N'Hybrid'),
    (N'Dimension.Product Category',      N'Product Category Key',      N'Sequence', N'ProductCategoryKey',     0, N'SCD1'),
    (N'Dimension.Product Hierarchy',     N'Product Hierarchy Key',     N'Sequence', N'ProductHierarchyKey',    0, N'SCD1'),
    (N'Dimension.Employee',              N'Employee Key',              N'Sequence', N'EmployeeKey',            0, N'SCD2'),
    (N'Dimension.Salesperson',           N'Salesperson Key',           N'Sequence', N'SalespersonKey',         0, N'SCD2'),
    (N'Dimension.City',                  N'City Key',                  N'Sequence', N'CityKey',                1, N'SCD2'),
    (N'Dimension.Geography',             N'Geography Key',             N'Sequence', N'GeographyKey',           1, N'SCD1'),
    (N'Dimension.Country',               N'Country Key',               N'Sequence', N'CountryKey',             0, N'Outrigger'),
    (N'Dimension.Region',                N'Region Key',                N'Sequence', N'RegionKey',              0, N'Outrigger'),
    (N'Dimension.Sales Territory',       N'Sales Territory Key',       N'Sequence', N'SalesTerritoryKey',      0, N'SCD1'),
    (N'Dimension.Warehouse Site',        N'Warehouse Site Key',        N'Sequence', N'WarehouseSiteKey',       0, N'SCD1'),
    (N'Dimension.Cost Center',           N'Cost Center Key',           N'Sequence', N'CostCenterKey',          0, N'SCD2'),
    (N'Dimension.GL Account',            N'GL Account Key',            N'Sequence', N'GlAccountKey',           0, N'SCD2'),
    (N'Dimension.Payment Method',        N'Payment Method Key',        N'Sequence', N'PaymentMethodKey',       0, N'SCD1'),
    (N'Dimension.Payment Terms',         N'Payment Terms Key',         N'Sequence', N'PaymentTermsKey',        0, N'SCD1'),
    (N'Dimension.Currency',              N'Currency Key',              N'Sequence', N'CurrencyKey',            0, N'SCD1'),
    (N'Dimension.Transaction Type',      N'Transaction Type Key',      N'Sequence', N'TransactionTypeKey',     0, N'SCD1'),
    (N'Dimension.Sales Channel',         N'Sales Channel Key',         N'Sequence', N'SalesChannelKey',        0, N'SCD1'),
    (N'Dimension.Promotion',             N'Promotion Key',             N'Sequence', N'PromotionKey',           0, N'SCD2'),
    (N'Dimension.Carrier',               N'Carrier Key',               N'Sequence', N'CarrierKey',             0, N'SCD1'),
    (N'Dimension.Return Reason',         N'Return Reason Key',         N'Sequence', N'ReturnReasonKey',        0, N'SCD1'),
    (N'Dimension.Loyalty Tier',          N'Loyalty Tier Key',          N'Sequence', N'LoyaltyTierKey',         0, N'SCD1'),
    (N'Dimension.Date',                  N'Date Key',                  N'Identity', NULL,                      0, N'Static'),
    (N'Dimension.Time',                  N'Time Key',                  N'Identity', NULL,                      0, N'Static'),
    (N'Dimension.Order Status Junk',     N'Order Status Junk Key',     N'Sequence', N'OrderStatusJunkKey',     0, N'Junk'),
    (N'Dimension.Customer Demographic',  N'Customer Demographic Key',  N'Sequence', N'CustomerDemographicKey', 0, N'Mini'),
    (N'Dimension.Customer Buying Group Bridge', N'Customer Buying Group Bridge Key', N'Identity', NULL, 0, N'Bridge'),
    (N'Dimension.Employee Territory Bridge',    N'Employee Territory Bridge Key',    N'Identity', NULL, 0, N'Bridge')
) AS src ([Dimension Name], [Key Column Name], [Allocation Method], [Sequence Name],
          [Supports Inferred], [SCD Pattern])
    ON tgt.[Dimension Name] = src.[Dimension Name]
WHEN MATCHED THEN
    UPDATE SET tgt.[Key Column Name]   = src.[Key Column Name],
               tgt.[Allocation Method] = src.[Allocation Method],
               tgt.[Sequence Name]     = src.[Sequence Name],
               tgt.[Supports Inferred] = src.[Supports Inferred],
               tgt.[SCD Pattern]       = src.[SCD Pattern]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([Dimension Name], [Key Column Name], [Allocation Method], [Sequence Name],
            [Reserved Key Low], [Reserved Key High], [Next Key], [Block Size],
            [Supports Inferred], [SCD Pattern])
    VALUES (src.[Dimension Name], src.[Key Column Name], src.[Allocation Method], src.[Sequence Name],
            -9, 0, 1, 1000, src.[Supports Inferred], src.[SCD Pattern]);
GO

IF OBJECT_ID(N'Integration.DimensionLoadAudit', N'U') IS NULL
BEGIN
    CREATE TABLE [Integration].[DimensionLoadAudit]
    (
        [Dimension Load Audit Id] BIGINT        IDENTITY(1, 1) NOT NULL,
        [Batch Id]                BIGINT        NULL,
        [Package Execution Id]    BIGINT        NULL,
        [Dimension Name]          NVARCHAR(100) NOT NULL,
        [Region Code]             NVARCHAR(10)  NULL,
        [Load Pattern]            NVARCHAR(30)  NOT NULL,
        [Source Row Count]        BIGINT        NULL,
        [Type 1 Update Count]     BIGINT        NULL,
        [Type 2 Close Count]      BIGINT        NULL,
        [Type 2 Insert Count]     BIGINT        NULL,
        [New Member Count]        BIGINT        NULL,
        [Inferred Enriched Count] BIGINT        NULL,
        [Reject Count]            BIGINT        NULL,
        [Same Day Change Count]   BIGINT        NULL,
        [Started On]              DATETIME2(7)  NOT NULL CONSTRAINT [DF_DimensionLoadAudit_Started] DEFAULT (SYSDATETIME()),
        [Completed On]            DATETIME2(7)  NULL,
        CONSTRAINT [PK_Integration_DimensionLoadAudit] PRIMARY KEY CLUSTERED ([Dimension Load Audit Id] ASC)
    );

    CREATE NONCLUSTERED INDEX [IX_Integration_DimensionLoadAudit_Dimension]
        ON [Integration].[DimensionLoadAudit] ([Dimension Name] ASC, [Started On] DESC);
END;
GO

/*
    Inferred (late-arriving) member queue. A fact load that cannot resolve a
    business key calls Integration.usp_InsertInferredMember, which writes the stub
    row into the dimension and a request row here. The next dimension load
    enriches the stub in place - the surrogate key never changes, because the
    facts already point at it.
*/
IF OBJECT_ID(N'Integration.InferredMemberQueue', N'U') IS NULL
BEGIN
    CREATE TABLE [Integration].[InferredMemberQueue]
    (
        [Inferred Member Queue Id] BIGINT        IDENTITY(1, 1) NOT NULL,
        [Dimension Name]           NVARCHAR(100) NOT NULL,
        [Business Key]             NVARCHAR(100) NOT NULL,
        [Surrogate Key]            INT           NOT NULL,
        [Source System Code]       NVARCHAR(20)  NULL,
        [Region Code]              NVARCHAR(10)  NULL,
        [Requested By Package]     NVARCHAR(200) NULL,
        [Requested Batch Id]       BIGINT        NULL,
        [Requested On]             DATETIME2(7)  NOT NULL CONSTRAINT [DF_InferredMemberQueue_Requested] DEFAULT (SYSDATETIME()),
        [Enrichment Status]        NVARCHAR(20)  NOT NULL CONSTRAINT [DF_InferredMemberQueue_Status] DEFAULT (N'Pending'),
        [Enriched On]              DATETIME2(7)  NULL,
        [Enrichment Attempts]      INT           NOT NULL CONSTRAINT [DF_InferredMemberQueue_Attempts] DEFAULT (0),
        [Last Attempt Note]        NVARCHAR(500) NULL,
        CONSTRAINT [PK_Integration_InferredMemberQueue] PRIMARY KEY CLUSTERED ([Inferred Member Queue Id] ASC),
        CONSTRAINT [UQ_Integration_InferredMemberQueue_Member]
            UNIQUE ([Dimension Name], [Business Key], [Surrogate Key]),
        CONSTRAINT [CK_Integration_InferredMemberQueue_Status]
            CHECK ([Enrichment Status] IN (N'Pending', N'Enriched', N'Abandoned', N'Rejected'))
    );

    CREATE NONCLUSTERED INDEX [IX_Integration_InferredMemberQueue_Pending]
        ON [Integration].[InferredMemberQueue] ([Dimension Name] ASC, [Enrichment Status] ASC)
        INCLUDE ([Business Key], [Surrogate Key]);
END;
GO
