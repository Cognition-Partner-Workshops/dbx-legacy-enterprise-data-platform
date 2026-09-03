/*
    Fact.Credit Note

    Object        : [Fact].[Credit Note] - transaction fact, one row per credit
                    note line raised against a customer.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Customer, Dimension.Stock Item,
                    Dimension.Currency, Dimension.Date (WP05).
    Called by     : loaded by Integration.usp_LoadFactCreditNote.
    Grain         : one credit note line.

    Not every credit note comes from a return - price adjustments, goodwill
    gestures and rebate settlements are credited without any goods moving, which
    is why this is a separate fact from [Fact].[Return] and why
    [Return Key] is nullable.

    Tax credit handling diverges: EU credit notes must reference the original
    invoice number for the VAT credit to be valid, and the load rejects rows
    where it is missing; APAC GST adjustment notes carry an adjustment reason
    code from a controlled list; NA credit memos simply reverse the sales tax
    that was charged, using the rate captured on the original sale.
*/
CREATE TABLE [Fact].[Credit Note] (
    [Credit Note Key]               BIGINT          IDENTITY (1, 1) NOT NULL,
    [Credit Note Date Key]          DATE            NOT NULL,
    [Original Invoice Date Key]     DATE            NULL,
    [Customer Key]                  INT             NOT NULL,
    [Bill To Customer Key]          INT             NULL,
    [Stock Item Key]                INT             NULL,
    [Salesperson Key]               INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Currency Key]                  INT             NULL,
    [Return Key]                    BIGINT          NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Credit Note Number]            NVARCHAR (20)   NOT NULL,
    [Credit Note Line Number]       INT             NOT NULL,
    [Original Invoice Number]       NVARCHAR (20)   NULL,
    [Credit Reason Code]            NVARCHAR (6)    NOT NULL,
    [Tax Adjustment Reason Code]    NVARCHAR (6)    NULL,
    [Approved By Employee Key]      INT             NULL,
    [Transaction Currency Code]     NCHAR (3)       NULL,
    [Quantity Credited]             DECIMAL (18, 4) NULL,
    [Credit Excluding Tax]          DECIMAL (18, 2) NOT NULL,
    [Tax Credit Amount]             DECIMAL (18, 2) NULL,
    [Credit Including Tax]          DECIMAL (18, 2) NOT NULL,
    [FX Rate To Reporting]          DECIMAL (19, 9) NULL,
    [Credit Including Tax Reporting] DECIMAL (18, 2) NULL,
    [Tax Regime Code]               NVARCHAR (10)   NULL,
    [VAT Rate]                      DECIMAL (18, 3) NULL,
    [GST Rate]                      DECIMAL (18, 3) NULL,
    [Sales Tax Rate]                DECIMAL (18, 3) NULL,
    [Goodwill Flag]                 BIT             NULL,
    [Rebate Settlement Flag]        BIT             NULL,
    [Fiscal Year]                   SMALLINT        NULL,
    [Fiscal Period]                 TINYINT         NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Inferred Member Flag]          BIT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Credit_Note] PRIMARY KEY NONCLUSTERED ([Credit Note Key] ASC, [Credit Note Date Key] ASC) ON [PS_Date] ([Credit Note Date Key]),
    CONSTRAINT [FK_Fact_Credit_Note_Credit_Note_Date_Key_Dimension_Date] FOREIGN KEY ([Credit Note Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Credit_Note_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key])
)
ON [PS_Date] ([Credit Note Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Credit_Note_Natural_Key]
    ON [Fact].[Credit Note] ([Credit Note Number] ASC, [Credit Note Line Number] ASC, [Credit Note Date Key] ASC)
    ON [PS_Date] ([Credit Note Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Credit_Note_Original_Invoice]
    ON [Fact].[Credit Note] ([Original Invoice Number] ASC)
    INCLUDE ([Credit Including Tax Reporting], [Credit Reason Code])
    ON [PS_Date] ([Credit Note Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Credit_Note]
    ON [Fact].[Credit Note]
    ON [PS_Date] ([Credit Note Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Credit note line fact covering returns, price adjustments, goodwill and rebates',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Credit Note';
GO
