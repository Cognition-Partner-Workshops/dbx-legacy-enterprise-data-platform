/*
    Fact.Order (estate extensions)

    Object        : ALTER of the pre-existing [Fact].[Order] (order line grain)
                    shipped with the WideWorldImporters DW sample.
    Deploy target : WideWorldImportersDW
    Deploy order  : after wwi-dw-ssdt; before Integration.usp_LoadFactOrder.
    Called by     : deployment only. Loaded by Integration.usp_LoadFactOrder.
    Depends on    : Dimension.Sales Channel, Dimension.Promotion,
                    Dimension.Customer Segment (WP05).

    Order lines are the head of the order-to-cash chain, so the accretion here
    is mostly about *state*: how much of the line is still open, why it is
    still open, and which despatch consumed it. The backlog snapshot and the
    Fact.Order Fulfilment accumulating snapshot both read these columns.
*/
SET NOCOUNT ON;
GO

IF COL_LENGTH(N'Fact.Order', N'Order Number') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Order Number] NVARCHAR (20) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Order Line Number') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Order Line Number] INT NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Customer Purchase Order Number') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Customer Purchase Order Number] NVARCHAR (25) NULL;
GO

/* order line state - added with the 2007 backlog reporting project */
IF COL_LENGTH(N'Fact.Order', N'Order Status Code') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Order Status Code] NVARCHAR (4) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Quantity Ordered') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Quantity Ordered] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Quantity Allocated') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Quantity Allocated] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Quantity Despatched') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Quantity Despatched] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Quantity Open') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Quantity Open] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Backorder Reason Code') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Backorder Reason Code] NVARCHAR (6) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Requested Delivery Date Key') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Requested Delivery Date Key] DATE NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Promised Delivery Date Key') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Promised Delivery Date Key] DATE NULL;
GO

/* money - the order is priced in the customer's currency, reported in USD */
IF COL_LENGTH(N'Fact.Order', N'Region Code') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Region Code] NVARCHAR (4) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Transaction Currency Code') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Transaction Currency Code] NCHAR(3) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Currency Key') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Currency Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'FX Rate To Reporting') IS NULL
    ALTER TABLE [Fact].[Order] ADD [FX Rate To Reporting] DECIMAL (19, 9) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Gross Order Amount') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Gross Order Amount] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Line Discount Amount') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Line Discount Amount] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Net Order Amount') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Net Order Amount] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Net Order Amount Reporting') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Net Order Amount Reporting] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Open Value Reporting') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Open Value Reporting] DECIMAL (18, 2) NULL;
GO

/* channel and campaign attribution, 2013 and 2016 */
IF COL_LENGTH(N'Fact.Order', N'Sales Channel Key') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Sales Channel Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Sales Territory Key') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Sales Territory Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Promotion Key') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Promotion Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Customer Segment Key') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Customer Segment Key] INT NULL;
GO

/* load control */
IF COL_LENGTH(N'Fact.Order', N'Natural Key Hash') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Natural Key Hash] BINARY (32) NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Inferred Member Flag') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Inferred Member Flag] BIT NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Batch Id') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Batch Id] BIGINT NULL;
GO
IF COL_LENGTH(N'Fact.Order', N'Load Datetime') IS NULL
    ALTER TABLE [Fact].[Order] ADD [Load Datetime] DATETIME2 (3) NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Fact_Order_Open_Lines'
                 AND object_id = OBJECT_ID(N'Fact.Order'))
    CREATE NONCLUSTERED INDEX [IX_Fact_Order_Open_Lines]
        ON [Fact].[Order] ([Order Status Code] ASC, [Order Date Key] ASC)
        INCLUDE ([Quantity Open], [Open Value Reporting], [Region Code])
        ON [PS_Date] ([Order Date Key]);
GO
