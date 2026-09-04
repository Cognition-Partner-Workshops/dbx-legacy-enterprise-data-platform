/*
    Object        : [Dimension].[Stock Item]  (hybrid SCD - Type 2 on commercial attributes, Type 1 on descriptive)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.StockItemKey (WideWorldImportersDW baseline),
                    Dimension.Product Category, Dimension.Product Hierarchy,
                    Dimension.Supplier
    Called by     : Integration.usp_MigrateStagedStockItemData,
                    Integration.usp_InsertInferredMember (fact loads see new SKUs
                    before the product master extract does)

    History
      2004  Shipped with the Microsoft sample.
      2008  Merchandising added the hierarchy keys and the pack/size attributes
            when the catalogue grew past the point where [Stock Item] alone
            identified the product.
      2011  Regional listing columns: the same SKU is listed at different prices,
            with different tax categories and different compliance labels in each
            region, so the regional block is per-row and the load procedure writes
            one dimension row per (SKU, region) listing.
      2016  Type 2 mechanics made explicit, matching Dimension.Customer.

    Type 2 attributes : selling price band, cost band, supplier, tax category,
                        hazardous classification, discontinued flag.
    Type 1 attributes : marketing description, search keywords, image URL,
                        unit of measure label, custom fields blob.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Stock Item', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Stock Item]
    (
        [Stock Item Key]        INT             CONSTRAINT [DF_Dimension_Stock_Item_Stock_Item_Key] DEFAULT (NEXT VALUE FOR [Sequences].[StockItemKey]) NOT NULL,
        [WWI Stock Item ID]     INT             NOT NULL,
        [Stock Item]            NVARCHAR(100)   NOT NULL,
        [Color]                 NVARCHAR(20)    NOT NULL,
        [Selling Package]       NVARCHAR(50)    NOT NULL,
        [Buying Package]        NVARCHAR(50)    NOT NULL,
        [Brand]                 NVARCHAR(50)    NOT NULL,
        [Size]                  NVARCHAR(20)    NOT NULL,
        [Lead Time Days]        INT             NOT NULL,
        [Quantity Per Outer]    INT             NOT NULL,
        [Is Chiller Stock]      BIT             NOT NULL,
        [Barcode]               NVARCHAR(50)    NULL,
        [Tax Rate]              DECIMAL(18, 3)  NOT NULL,
        [Unit Price]            DECIMAL(18, 2)  NOT NULL,
        [Recommended Retail Price] DECIMAL(18, 2) NULL,
        [Typical Weight Per Unit]  DECIMAL(18, 3) NOT NULL,
        [Photo]                 VARBINARY(MAX)  NULL,
        [Valid From]            DATETIME2(7)    NOT NULL,
        [Valid To]              DATETIME2(7)    NOT NULL,
        [Lineage Key]           INT             NOT NULL,
        CONSTRAINT [PK_Dimension_Stock_Item] PRIMARY KEY CLUSTERED ([Stock Item Key] ASC)
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Stock_Item_WWIStockItemID]
        ON [Dimension].[Stock Item] ([WWI Stock Item ID] ASC, [Valid From] ASC, [Valid To] ASC);
END;
GO

IF COL_LENGTH(N'Dimension.Stock Item', N'Source System Code') IS NULL
    ALTER TABLE [Dimension].[Stock Item] ADD
        [Source System Code]            NVARCHAR(20)    NULL,
        [Stock Item Code]               NVARCHAR(50)    NULL,
        [Source Product Reference]      NVARCHAR(50)    NULL,
        [Manufacturer Part Number]      NVARCHAR(50)    NULL,
        [Global Trade Item Number]      NVARCHAR(14)    NULL,
        [Product Category Key]          INT             NULL,
        [Product Hierarchy Key]         INT             NULL,
        [Primary Supplier Key]          INT             NULL,
        [Product Status Code]           NVARCHAR(10)    NULL,   -- NEW / ACT / RUN / DSC / OBS
        [Is Discontinued]               BIT             NULL,
        [Discontinued On]               DATE            NULL,
        [Replacement Stock Item Key]    INT             NULL;
GO

IF COL_LENGTH(N'Dimension.Stock Item', N'Unit Of Measure Code') IS NULL
    ALTER TABLE [Dimension].[Stock Item] ADD
        [Unit Of Measure Code]          NVARCHAR(10)    NULL,
        [Unit Of Measure Label]         NVARCHAR(30)    NULL,
        [Pack Size Quantity]            DECIMAL(18, 3)  NULL,
        [Case Quantity]                 INT             NULL,
        [Pallet Quantity]               INT             NULL,
        [Shelf Life Days]               INT             NULL,
        [Storage Class Code]            NVARCHAR(10)    NULL,   -- AMB / CHL / FRZ / HAZ
        [Hazard Class Code]             NVARCHAR(10)    NULL,
        [Is Serialized]                 BIT             NULL,
        [Is Batch Tracked]              BIT             NULL;
GO

/*
    Regional listing block. One dimension row per SKU per region: the same
    [WWI Stock Item ID] therefore has up to three current rows, distinguished by
    [Listing Region Code], and every lookup in the fact loads joins on both.
*/
IF COL_LENGTH(N'Dimension.Stock Item', N'Listing Region Code') IS NULL
    ALTER TABLE [Dimension].[Stock Item] ADD
        [Listing Region Code]           NVARCHAR(10)    NULL,
        [Listing Currency Code]         NVARCHAR(3)     NULL,
        [Listing Unit Price]            DECIMAL(18, 2)  NULL,
        [Listing Price Band]            NVARCHAR(10)    NULL,   -- banded so a cent change does not open a Type 2 row
        [Standard Cost Amount]          DECIMAL(18, 4)  NULL,
        [Standard Cost Band]            NVARCHAR(10)    NULL,
        [NA Sales Tax Category Code]    NVARCHAR(10)    NULL,   -- TPP / FOOD / RX / SVC
        [EU VAT Rate Category]          NVARCHAR(10)    NULL,   -- STD / RED / SUP / ZER
        [EU Intrastat Commodity Code]   NVARCHAR(10)    NULL,
        [APAC GST Treatment Code]       NVARCHAR(10)    NULL,
        [APAC Import Duty Code]         NVARCHAR(10)    NULL,
        [Country Of Origin Code]        NVARCHAR(3)     NULL;
GO

IF COL_LENGTH(N'Dimension.Stock Item', N'Marketing Description') IS NULL
    ALTER TABLE [Dimension].[Stock Item] ADD
        [Marketing Description]         NVARCHAR(500)   NULL,
        [Search Keywords]               NVARCHAR(500)   NULL,
        [Image URL]                     NVARCHAR(256)   NULL,
        [Custom Fields]                 NVARCHAR(MAX)   NULL,   -- JSON kept as text; parsed nowhere in the warehouse
        [Merchandising Notes]           NVARCHAR(MAX)   NULL;
GO

IF COL_LENGTH(N'Dimension.Stock Item', N'Effective From') IS NULL
    ALTER TABLE [Dimension].[Stock Item] ADD
        [Effective From]                DATETIME2(7)    NULL,
        [Effective To]                  DATETIME2(7)    NULL,
        [Effective From Date]           DATE            NULL,
        [Effective Sequence]            SMALLINT        NULL,
        [Is Current Row]                BIT             NULL,
        [Version Number]                INT             NULL,
        [Row Hash Type 2]               VARBINARY(32)   NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Is Inferred Member]            BIT             NULL,
        [Inferred Created On]           DATETIME2(7)    NULL,
        [Enriched On]                   DATETIME2(7)    NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        [Last Load Package Execution Id] BIGINT         NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Dimension_Stock_Item_Current'
                 AND object_id = OBJECT_ID(N'Dimension.Stock Item'))
    CREATE NONCLUSTERED INDEX [IX_Dimension_Stock_Item_Current]
        ON [Dimension].[Stock Item] ([WWI Stock Item ID] ASC, [Listing Region Code] ASC, [Is Current Row] ASC)
        INCLUDE ([Stock Item Key], [Row Hash Type 2], [Version Number]);
GO
