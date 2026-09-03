/*
    Fact.Return

    Object        : [Fact].[Return] - transaction fact, one row per returned
                    line (RMA line) received back into a warehouse.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Return Reason, Dimension.Customer,
                    Dimension.Stock Item, Dimension.Warehouse Site (WP05).
    Called by     : loaded by Integration.usp_LoadFactReturn.
    Grain         : one RMA line.

    Return quantities are negative on purpose: the 2008 build made returns
    negative sales so the old Access reports could union the two. Everything
    downstream now depends on that sign convention, including
    Aggregate.Product Performance.

    Consumer-rights divergence: EU carries a statutory 14-day withdrawal window
    and the reason code set includes WITHDR; APAC uses a shorter, jurisdiction
    dependent window; NA has no statutory window and returns are governed by the
    commercial policy on the customer's contract.
*/
CREATE TABLE [Fact].[Return] (
    [Return Key]                    BIGINT          IDENTITY (1, 1) NOT NULL,
    [Return Date Key]               DATE            NOT NULL,
    [Original Invoice Date Key]     DATE            NULL,
    [Credit Issued Date Key]        DATE            NULL,
    [Customer Key]                  INT             NOT NULL,
    [Stock Item Key]                INT             NOT NULL,
    [Return Reason Key]             INT             NOT NULL,
    [Warehouse Site Key]            INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Salesperson Key]               INT             NULL,
    [Currency Key]                  INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [RMA Number]                    NVARCHAR (20)   NOT NULL,
    [RMA Line Number]               INT             NOT NULL,
    [Original Invoice Number]       NVARCHAR (20)   NULL,
    [Original Invoice Line Number]  INT             NULL,
    [Credit Note Number]            NVARCHAR (20)   NULL,
    [Return Reason Free Text]       NVARCHAR (200)  NULL,
    [Quantity Returned]             DECIMAL (18, 4) NOT NULL,
    [Quantity Restocked]            DECIMAL (18, 4) NULL,
    [Quantity Scrapped]             DECIMAL (18, 4) NULL,
    [Source UOM Code]               NVARCHAR (10)   NULL,
    [Transaction Currency Code]     NCHAR (3)       NULL,
    [Gross Return Amount]           DECIMAL (18, 2) NOT NULL,
    [Restocking Fee Amount]         DECIMAL (18, 2) NULL,
    [Tax Amount]                    DECIMAL (18, 2) NULL,
    [Net Credit Amount]             DECIMAL (18, 2) NOT NULL,
    [FX Rate To Reporting]          DECIMAL (19, 9) NULL,
    [Net Credit Amount Reporting]   DECIMAL (18, 2) NULL,
    [Cost Of Returned Goods]        DECIMAL (18, 2) NULL,
    [Margin Reversed]               DECIMAL (18, 2) NULL,
    [Days Since Invoice]            INT             NULL,
    [Within Statutory Window Flag]  BIT             NULL,
    [Statutory Window Days]         INT             NULL,
    [Faulty Goods Flag]             BIT             NULL,
    [Disposition Code]              NVARCHAR (6)    NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Inferred Member Flag]          BIT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Return] PRIMARY KEY NONCLUSTERED ([Return Key] ASC, [Return Date Key] ASC) ON [PS_Date] ([Return Date Key]),
    CONSTRAINT [FK_Fact_Return_Return_Date_Key_Dimension_Date] FOREIGN KEY ([Return Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Return_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key]),
    CONSTRAINT [FK_Fact_Return_Stock_Item_Key_Dimension_Stock Item] FOREIGN KEY ([Stock Item Key]) REFERENCES [Dimension].[Stock Item] ([Stock Item Key]),
    CONSTRAINT [FK_Fact_Return_Return_Reason_Key_Dimension_Return Reason] FOREIGN KEY ([Return Reason Key]) REFERENCES [Dimension].[Return Reason] ([Return Reason Key])
)
ON [PS_Date] ([Return Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Return_Natural_Key]
    ON [Fact].[Return] ([RMA Number] ASC, [RMA Line Number] ASC, [Return Date Key] ASC)
    ON [PS_Date] ([Return Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Return_Original_Invoice]
    ON [Fact].[Return] ([Original Invoice Number] ASC, [Original Invoice Line Number] ASC)
    ON [PS_Date] ([Return Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Return]
    ON [Fact].[Return]
    ON [PS_Date] ([Return Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Return (RMA) line fact; quantities negative by 2008 sign convention',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Return';
GO
