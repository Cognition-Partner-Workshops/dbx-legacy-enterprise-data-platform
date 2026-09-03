/*
    Fact.Customer Transaction

    Object        : [Fact].[Customer Transaction] - transaction fact, one row per
                    AR ledger entry (invoice, credit note, receipt, adjustment,
                    write-off) raised against a customer.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Customer, Dimension.Transaction Type,
                    Dimension.Payment Method, Dimension.Currency (WP05).
    Called by     : loaded by Integration.usp_LoadFactPayment (the AR load also
                    writes the ledger side) and read by
                    Integration.usp_LoadFactArAgingSnapshot.
    Grain         : one AR ledger line.

    Split out of [Fact].[Transaction] in 2014 when the AP side grew its own
    columns; [Fact].[Transaction] was never retired, so both tables are loaded
    and the finance close reconciles them. That duplication is deliberate and is
    exactly the sort of thing the migration has to untangle.
*/
CREATE TABLE [Fact].[Customer Transaction] (
    [Customer Transaction Key]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [Transaction Date Key]          DATE            NOT NULL,
    [Due Date Key]                  DATE            NULL,
    [Customer Key]                  INT             NOT NULL,
    [Bill To Customer Key]          INT             NULL,
    [Transaction Type Key]          INT             NOT NULL,
    [Payment Method Key]            INT             NULL,
    [Currency Key]                  INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Customer Segment Key]          INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [WWI Customer Transaction ID]   INT             NULL,
    [Invoice Number]                NVARCHAR (20)   NULL,
    [Credit Note Number]            NVARCHAR (20)   NULL,
    [Source System Code]            NVARCHAR (10)   NULL,
    [Transaction Currency Code]     NCHAR (3)       NULL,
    [Amount Excluding Tax]          DECIMAL (18, 2) NOT NULL,
    [Tax Amount]                    DECIMAL (18, 2) NULL,
    [Transaction Amount]            DECIMAL (18, 2) NOT NULL,
    [Outstanding Balance]           DECIMAL (18, 2) NULL,
    [FX Rate To Reporting]          DECIMAL (19, 9) NULL,
    [Transaction Amount Reporting]  DECIMAL (18, 2) NULL,
    [Tax Regime Code]               NVARCHAR (10)   NULL,
    [VAT Reverse Charge Flag]       BIT             NULL,
    [GST Free Flag]                 BIT             NULL,
    [Credit Limit At Transaction]   DECIMAL (18, 2) NULL,
    [Credit Hold Flag]              BIT             NULL,
    [Is Finalized]                  BIT             NULL,
    [Fiscal Year]                   SMALLINT        NULL,
    [Fiscal Period]                 TINYINT         NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Correction Type Code]          NVARCHAR (3)    NULL,
    [Inferred Member Flag]          BIT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Customer_Transaction] PRIMARY KEY NONCLUSTERED ([Customer Transaction Key] ASC, [Transaction Date Key] ASC) ON [PS_Date] ([Transaction Date Key]),
    CONSTRAINT [FK_Fact_Customer_Transaction_Transaction_Date_Key_Dimension_Date] FOREIGN KEY ([Transaction Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Customer_Transaction_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key]),
    CONSTRAINT [FK_Fact_Customer_Transaction_Transaction_Type_Key_Dimension_Transaction Type] FOREIGN KEY ([Transaction Type Key]) REFERENCES [Dimension].[Transaction Type] ([Transaction Type Key])
)
ON [PS_Date] ([Transaction Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Customer_Transaction_Open_Items]
    ON [Fact].[Customer Transaction] ([Customer Key] ASC, [Due Date Key] ASC)
    INCLUDE ([Outstanding Balance], [Transaction Amount Reporting], [Region Code])
    ON [PS_Date] ([Transaction Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Customer_Transaction_Invoice_Number]
    ON [Fact].[Customer Transaction] ([Invoice Number] ASC)
    ON [PS_Date] ([Transaction Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Customer_Transaction]
    ON [Fact].[Customer Transaction]
    ON [PS_Date] ([Transaction Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Accounts receivable ledger fact, split from Fact.Transaction in 2014',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Customer Transaction';
GO
