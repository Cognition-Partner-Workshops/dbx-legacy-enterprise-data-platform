/*
    Fact.Purchase Receipt

    Object        : [Fact].[Purchase Receipt] - transaction fact, one row per
                    goods-receipt line (PO receipt) posted in the Oracle
                    purchasing system.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Supplier, Dimension.Stock Item,
                    Dimension.Warehouse Site, Dimension.Date (WP05).
    Called by     : loaded by Integration.usp_LoadFactPurchaseReceipt, driven by
                    the FACT_Load_PurchaseReceipt package.
    Grain         : one receipt line (receipt number + receipt line number).

    Receipts are the second milestone of procure-to-pay. The fact keeps both the
    ordered and the received quantity so over/under receipt is measurable
    without joining back to Fact.Purchase, which is how the warehouse team has
    always reported it. Quality-hold quantities are regionally different: EU
    sites quarantine on arrival for chilled goods, APAC sites quarantine only on
    a failed inspection, NA sites do not quarantine at all and post a claim.
*/
CREATE TABLE [Fact].[Purchase Receipt] (
    [Purchase Receipt Key]        BIGINT          IDENTITY (1, 1) NOT NULL,
    [Receipt Date Key]            DATE            NOT NULL,
    [Purchase Order Date Key]     DATE            NULL,
    [Supplier Key]                INT             NOT NULL,
    [Stock Item Key]              INT             NOT NULL,
    [Warehouse Site Key]          INT             NOT NULL,
    [Vendor Contract Key]         INT             NULL,
    [Currency Key]                INT             NULL,
    [Employee Key]                INT             NULL,
    [Region Code]                 NVARCHAR (4)    NOT NULL,
    [Receipt Number]              NVARCHAR (20)   NOT NULL,
    [Receipt Line Number]         INT             NOT NULL,
    [Purchase Order Number]       NVARCHAR (20)   NULL,
    [Purchase Order Line Number]  INT             NULL,
    [Supplier Delivery Note]      NVARCHAR (30)   NULL,
    [Container Reference]         NVARCHAR (20)   NULL,
    [Quantity Ordered Base UOM]   DECIMAL (18, 4) NOT NULL,
    [Quantity Received Base UOM]  DECIMAL (18, 4) NOT NULL,
    [Quantity Rejected Base UOM]  DECIMAL (18, 4) NULL,
    [Quantity On Quality Hold]    DECIMAL (18, 4) NULL,
    [Source UOM Code]             NVARCHAR (10)   NULL,
    [Quantity Source UOM]         DECIMAL (18, 4) NULL,
    [Over Receipt Quantity]       DECIMAL (18, 4) NULL,
    [Transaction Currency Code]   NCHAR (3)       NULL,
    [Unit Cost]                   DECIMAL (18, 4) NULL,
    [Receipt Value]               DECIMAL (18, 2) NULL,
    [FX Rate To Reporting]        DECIMAL (19, 9) NULL,
    [Receipt Value Reporting]     DECIMAL (18, 2) NULL,
    [Freight In Amount]           DECIMAL (18, 2) NULL,
    [Customs Duty Amount]         DECIMAL (18, 2) NULL,
    [Days Late Versus Promise]    INT             NULL,
    [Lead Time Days]              INT             NULL,
    [Price Variance Amount]       DECIMAL (18, 2) NULL,
    [On Time Flag]                BIT             NULL,
    [In Full Flag]                BIT             NULL,
    [Quality Hold Reason Code]    NVARCHAR (6)    NULL,
    [Inspection Result Code]      NVARCHAR (4)    NULL,
    [Natural Key Hash]            BINARY (32)     NULL,
    [Inferred Member Flag]        BIT             NULL,
    [Lineage Key]                 INT             NOT NULL,
    [Batch Id]                    BIGINT          NULL,
    [Load Datetime]               DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Purchase_Receipt] PRIMARY KEY NONCLUSTERED ([Purchase Receipt Key] ASC, [Receipt Date Key] ASC) ON [PS_Date] ([Receipt Date Key]),
    CONSTRAINT [FK_Fact_Purchase_Receipt_Receipt_Date_Key_Dimension_Date] FOREIGN KEY ([Receipt Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Purchase_Receipt_Supplier_Key_Dimension_Supplier] FOREIGN KEY ([Supplier Key]) REFERENCES [Dimension].[Supplier] ([Supplier Key]),
    CONSTRAINT [FK_Fact_Purchase_Receipt_Stock_Item_Key_Dimension_Stock Item] FOREIGN KEY ([Stock Item Key]) REFERENCES [Dimension].[Stock Item] ([Stock Item Key])
)
ON [PS_Date] ([Receipt Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Purchase_Receipt_Natural_Key]
    ON [Fact].[Purchase Receipt] ([Receipt Number] ASC, [Receipt Line Number] ASC, [Receipt Date Key] ASC)
    ON [PS_Date] ([Receipt Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Purchase_Receipt_Supplier_Performance]
    ON [Fact].[Purchase Receipt] ([Supplier Key] ASC, [Receipt Date Key] ASC)
    INCLUDE ([On Time Flag], [In Full Flag], [Receipt Value Reporting], [Days Late Versus Promise])
    ON [PS_Date] ([Receipt Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Purchase_Receipt_Po_Line]
    ON [Fact].[Purchase Receipt] ([Purchase Order Number] ASC, [Purchase Order Line Number] ASC)
    ON [PS_Date] ([Receipt Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Purchase_Receipt]
    ON [Fact].[Purchase Receipt]
    ON [PS_Date] ([Receipt Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Goods receipt line fact (procure-to-pay milestone 3)',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Purchase Receipt';
GO
