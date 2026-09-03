/*
    Fact.Order Fulfilment

    Object        : [Fact].[Order Fulfilment] - accumulating snapshot fact for
                    the order-to-cash pipeline: order -> pick -> despatch ->
                    deliver -> invoice -> cash.
    Deploy target : WideWorldImportersDW
    Deploy order  : after [Fact].[Order], [Fact].[Sale], [Fact].[Shipment],
                    [Fact].[Payment].
    Called by     : loaded and repeatedly updated by
                    Integration.usp_LoadFactOrderFulfilment.
    Grain         : one sales order line.

    Every milestone date key starts NULL and is set once, by the first run that
    can see the event. Lag measures are recomputed on every update; the row is
    closed when cash is applied in full or the order is cancelled.
    [Open Milestone Count] is maintained so the nightly update can restrict
    itself to rows that still have work outstanding instead of rescanning the
    whole table, which is what the original 2009 version did.

    Regional service targets differ (NA 2 days pick-to-despatch, EU 3 days plus
    a customs step, APAC 5 days sea freight), so the SLA breach flags are
    evaluated against [Service Target Days] captured on the row rather than a
    single global constant.
*/
CREATE TABLE [Fact].[Order Fulfilment] (
    [Order Fulfilment Key]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [Order Date Key]                DATE            NOT NULL,
    [Allocation Date Key]           DATE            NULL,
    [Pick Date Key]                 DATE            NULL,
    [Pack Date Key]                 DATE            NULL,
    [Despatch Date Key]             DATE            NULL,
    [Delivery Date Key]             DATE            NULL,
    [Invoice Date Key]              DATE            NULL,
    [Cash Applied Date Key]         DATE            NULL,
    [Cancellation Date Key]         DATE            NULL,
    [Customer Key]                  INT             NOT NULL,
    [Stock Item Key]                INT             NOT NULL,
    [Salesperson Key]               INT             NULL,
    [Warehouse Site Key]            INT             NULL,
    [Carrier Key]                   INT             NULL,
    [Sales Channel Key]             INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Order Number]                  NVARCHAR (20)   NOT NULL,
    [Order Line Number]             INT             NOT NULL,
    [Despatch Note Number]          NVARCHAR (20)   NULL,
    [Invoice Number]                NVARCHAR (20)   NULL,
    [Receipt Number]                NVARCHAR (20)   NULL,
    [Quantity Ordered]              DECIMAL (18, 4) NULL,
    [Quantity Despatched]           DECIMAL (18, 4) NULL,
    [Quantity Invoiced]             DECIMAL (18, 4) NULL,
    [Order Value Reporting]         DECIMAL (18, 2) NULL,
    [Invoiced Value Reporting]      DECIMAL (18, 2) NULL,
    [Cash Applied Reporting]        DECIMAL (18, 2) NULL,
    [Order To Pick Lag Days]        INT             NULL,
    [Pick To Despatch Lag Days]     INT             NULL,
    [Despatch To Delivery Lag Days] INT             NULL,
    [Delivery To Invoice Lag Days]  INT             NULL,
    [Invoice To Cash Lag Days]      INT             NULL,
    [Order To Cash Cycle Days]      INT             NULL,
    [Service Target Days]           INT             NULL,
    [Pick SLA Breach Flag]          BIT             NULL,
    [Delivery SLA Breach Flag]      BIT             NULL,
    [Perfect Order Flag]            BIT             NULL,
    [Cancelled Flag]                BIT             NULL,
    [Open Milestone Count]          TINYINT         CONSTRAINT [DF_Fact_Order_Fulfilment_Open_Milestone_Count] DEFAULT (6) NOT NULL,
    [Pipeline Status Code]          NVARCHAR (6)    NULL,
    [Cycle Complete Flag]           BIT             NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    [Last Milestone Update]         DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Order_Fulfilment] PRIMARY KEY NONCLUSTERED ([Order Fulfilment Key] ASC, [Order Date Key] ASC) ON [PS_Date] ([Order Date Key]),
    CONSTRAINT [FK_Fact_Order_Fulfilment_Order_Date_Key_Dimension_Date] FOREIGN KEY ([Order Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Order_Fulfilment_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key]),
    CONSTRAINT [FK_Fact_Order_Fulfilment_Stock_Item_Key_Dimension_Stock Item] FOREIGN KEY ([Stock Item Key]) REFERENCES [Dimension].[Stock Item] ([Stock Item Key])
)
ON [PS_Date] ([Order Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Order_Fulfilment_Natural_Key]
    ON [Fact].[Order Fulfilment] ([Order Number] ASC, [Order Line Number] ASC, [Order Date Key] ASC)
    ON [PS_Date] ([Order Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Order_Fulfilment_Open_Pipeline]
    ON [Fact].[Order Fulfilment] ([Open Milestone Count] ASC, [Order Date Key] ASC)
    INCLUDE ([Order Number], [Order Line Number], [Pipeline Status Code], [Region Code])
    ON [PS_Date] ([Order Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Order_Fulfilment_Invoice_Number]
    ON [Fact].[Order Fulfilment] ([Invoice Number] ASC)
    ON [PS_Date] ([Order Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Order-to-cash accumulating snapshot at sales order line grain',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Order Fulfilment';
GO
