/*
    Fact.Sales Margin

    Object        : [Fact].[Sales Margin] - consolidated (derived) fact
                    combining the revenue side from [Fact].[Sale], the cost side
                    from [Fact].[Movement] / standard cost, the credit side from
                    [Fact].[Credit Note] and returns from [Fact].[Return] into a
                    single margin row.
    Deploy target : WideWorldImportersDW
    Deploy order  : after all four contributing facts.
    Called by     : rebuilt by Integration.usp_RefreshAggregateMarginAnalysis
                    with a delete-by-window incremental.
    Grain         : invoice line (invoice number + invoice line number).

    Consolidated fact: nothing here is sourced directly from an operational
    system. It exists because the margin question used to be answered by a
    600-line report query that joined the four facts at run time and timed out
    every month end.

    Cost basis is not consistent across regions, and the fact records which
    basis produced [Cost Of Sale Amount]: NA weighted average, EU FIFO layer,
    APAC standard cost with the purchase-price variance carried separately.
    Group margin is therefore only comparable at [Standard Margin Amount],
    which is why both are stored.
*/
CREATE TABLE [Fact].[Sales Margin] (
    [Sales Margin Key]              BIGINT          IDENTITY (1, 1) NOT NULL,
    [Invoice Date Key]              DATE            NOT NULL,
    [Customer Key]                  INT             NOT NULL,
    [Stock Item Key]                INT             NOT NULL,
    [Product Category Key]          INT             NULL,
    [Salesperson Key]               INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Sales Channel Key]             INT             NULL,
    [Promotion Key]                 INT             NULL,
    [Customer Segment Key]          INT             NULL,
    [Currency Key]                  INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Invoice Number]                NVARCHAR (20)   NOT NULL,
    [Invoice Line Number]           INT             NOT NULL,
    [Order Number]                  NVARCHAR (20)   NULL,
    [Quantity Base UOM]             DECIMAL (18, 4) NULL,
    [Gross Amount]                  DECIMAL (18, 2) NOT NULL,
    [Line Discount Amount]          DECIMAL (18, 2) NULL,
    [Promotion Discount Amount]     DECIMAL (18, 2) NULL,
    [Net Amount]                    DECIMAL (18, 2) NOT NULL,
    [Tax Amount]                    DECIMAL (18, 2) NULL,
    [Freight Recovered Amount]      DECIMAL (18, 2) NULL,
    [Freight Cost Amount]           DECIMAL (18, 2) NULL,
    [Cost Of Sale Amount]           DECIMAL (18, 2) NULL,
    [Cost Basis Code]               NVARCHAR (6)    NULL,
    [Standard Cost Amount]          DECIMAL (18, 2) NULL,
    [Purchase Price Variance]       DECIMAL (18, 2) NULL,
    [Rebate Accrual Amount]         DECIMAL (18, 2) NULL,
    [Credit Note Amount]            DECIMAL (18, 2) NULL,
    [Return Amount]                 DECIMAL (18, 2) NULL,
    [Gross Margin Amount]           DECIMAL (18, 2) NULL,
    [Standard Margin Amount]        DECIMAL (18, 2) NULL,
    [Contribution Margin Amount]    DECIMAL (18, 2) NULL,
    [Margin Percent]                DECIMAL (9, 4)  NULL,
    [Standard Margin Percent]       DECIMAL (9, 4)  NULL,
    [FX Rate To Reporting]          DECIMAL (19, 9) NULL,
    [Net Amount Reporting]          DECIMAL (18, 2) NULL,
    [Gross Margin Reporting]        DECIMAL (18, 2) NULL,
    [Negative Margin Flag]          BIT             NULL,
    [Restated Flag]                 BIT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Sales_Margin] PRIMARY KEY NONCLUSTERED ([Sales Margin Key] ASC, [Invoice Date Key] ASC) ON [PS_Date] ([Invoice Date Key]),
    CONSTRAINT [FK_Fact_Sales_Margin_Invoice_Date_Key_Dimension_Date] FOREIGN KEY ([Invoice Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Sales_Margin_Stock_Item_Key_Dimension_Stock Item] FOREIGN KEY ([Stock Item Key]) REFERENCES [Dimension].[Stock Item] ([Stock Item Key]),
    CONSTRAINT [FK_Fact_Sales_Margin_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key])
)
ON [PS_Date] ([Invoice Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Sales_Margin_Grain]
    ON [Fact].[Sales Margin] ([Invoice Number] ASC, [Invoice Line Number] ASC, [Invoice Date Key] ASC)
    ON [PS_Date] ([Invoice Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Sales_Margin_Category_Period]
    ON [Fact].[Sales Margin] ([Product Category Key] ASC, [Invoice Date Key] ASC)
    INCLUDE ([Net Amount Reporting], [Gross Margin Reporting], [Margin Percent], [Region Code])
    ON [PS_Date] ([Invoice Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Sales_Margin]
    ON [Fact].[Sales Margin]
    ON [PS_Date] ([Invoice Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Consolidated sales and cost margin fact at invoice line grain',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Sales Margin';
GO
