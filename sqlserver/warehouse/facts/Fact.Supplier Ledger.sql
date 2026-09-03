/*
    Supplier-side ledger facts, deployed together because the AP load writes
    both in one transaction:
      [Fact].[Supplier Transaction]
      [Fact].[Supplier Payment]
*/

/*
    Fact.Supplier Transaction

    Object        : [Fact].[Supplier Transaction] - transaction fact, one row per
                    AP ledger entry (supplier invoice, debit memo, payment,
                    accrual reversal).
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Supplier, Dimension.Transaction Type,
                    Dimension.Payment Terms, Dimension.Cost Center (WP05).
    Called by     : loaded by Integration.usp_LoadFactSupplierPayment and read by
                    Integration.usp_LoadFactApAgingSnapshot and the finance
                    close aggregate.
    Grain         : one AP ledger line.

    The AP side carries accrual and three-way-match state that the AR side has
    no equivalent of, which is why the two ledgers were split. Recoverable tax
    is meaningful in EU (input VAT) and APAC (input GST credit) and is always
    zero in NA, where sales tax paid on purchases is a cost, not a receivable.
*/
CREATE TABLE [Fact].[Supplier Transaction] (
    [Supplier Transaction Key]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [Transaction Date Key]          DATE            NOT NULL,
    [Due Date Key]                  DATE            NULL,
    [Supplier Key]                  INT             NOT NULL,
    [Transaction Type Key]          INT             NOT NULL,
    [Payment Terms Key]             INT             NULL,
    [Cost Center Key]               INT             NULL,
    [Currency Key]                  INT             NULL,
    [Vendor Contract Key]           INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [WWI Supplier Transaction ID]   INT             NULL,
    [Supplier Invoice Number]       NVARCHAR (20)   NULL,
    [Purchase Order Number]         NVARCHAR (20)   NULL,
    [Receipt Number]                NVARCHAR (20)   NULL,
    [GL Account Code]               NVARCHAR (20)   NULL,
    [Transaction Currency Code]     NCHAR (3)       NULL,
    [Amount Excluding Tax]          DECIMAL (18, 2) NOT NULL,
    [Recoverable Tax Amount]        DECIMAL (18, 2) NULL,
    [Non Recoverable Tax Amount]    DECIMAL (18, 2) NULL,
    [Transaction Amount]            DECIMAL (18, 2) NOT NULL,
    [Outstanding Balance]           DECIMAL (18, 2) NULL,
    [FX Rate To Reporting]          DECIMAL (19, 9) NULL,
    [Transaction Amount Reporting]  DECIMAL (18, 2) NULL,
    [Accrual Flag]                  BIT             NULL,
    [Accrual Reversal Date Key]     DATE            NULL,
    [Match Status Code]             NVARCHAR (4)    NULL,
    [Price Variance Amount]         DECIMAL (18, 2) NULL,
    [Quantity Variance Amount]      DECIMAL (18, 2) NULL,
    [Payment Block Reason Code]     NVARCHAR (6)    NULL,
    [Fiscal Year]                   SMALLINT        NULL,
    [Fiscal Period]                 TINYINT         NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Inferred Member Flag]          BIT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Supplier_Transaction] PRIMARY KEY NONCLUSTERED ([Supplier Transaction Key] ASC, [Transaction Date Key] ASC) ON [PS_Date] ([Transaction Date Key]),
    CONSTRAINT [FK_Fact_Supplier_Transaction_Transaction_Date_Key_Dimension_Date] FOREIGN KEY ([Transaction Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Supplier_Transaction_Supplier_Key_Dimension_Supplier] FOREIGN KEY ([Supplier Key]) REFERENCES [Dimension].[Supplier] ([Supplier Key]),
    CONSTRAINT [FK_Fact_Supplier_Transaction_Transaction_Type_Key_Dimension_Transaction Type] FOREIGN KEY ([Transaction Type Key]) REFERENCES [Dimension].[Transaction Type] ([Transaction Type Key])
)
ON [PS_Date] ([Transaction Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Supplier_Transaction_Open_Items]
    ON [Fact].[Supplier Transaction] ([Supplier Key] ASC, [Due Date Key] ASC)
    INCLUDE ([Outstanding Balance], [Transaction Amount Reporting], [Match Status Code])
    ON [PS_Date] ([Transaction Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Supplier_Transaction_Po_Number]
    ON [Fact].[Supplier Transaction] ([Purchase Order Number] ASC, [Supplier Invoice Number] ASC)
    ON [PS_Date] ([Transaction Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Supplier_Transaction]
    ON [Fact].[Supplier Transaction]
    ON [PS_Date] ([Transaction Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Accounts payable ledger fact with three-way-match variance measures',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Supplier Transaction';
GO

/*
    Fact.Supplier Payment

    Object        : [Fact].[Supplier Payment] - transaction fact, one row per AP
                    payment allocation line (supplier disbursement).
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Supplier, Dimension.Payment Terms,
                    Dimension.Currency, Dimension.Cost Center (WP05).
    Called by     : loaded by Integration.usp_LoadFactSupplierPayment, driven by
                    FIN_Load_SupplierPayments. Feeds the procure-to-pay
                    accumulating snapshot (final milestone).
    Grain         : one allocation of one payment run line against one supplier
                    invoice.

    Payment runs are regionally different instruments and the fact carries all
    three because the Oracle AP module writes them into one table: NA cuts
    cheques and ACH files with a cheque number, EU issues SEPA direct debits and
    credit transfers keyed on the creditor scheme id, APAC pays by telegraphic
    transfer with a bank reference and, in a couple of jurisdictions, deducts
    withholding tax that AP still owes to the revenue authority.
*/
CREATE TABLE [Fact].[Supplier Payment] (
    [Supplier Payment Key]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [Payment Date Key]              DATE            NOT NULL,
    [Supplier Invoice Date Key]     DATE            NULL,
    [Due Date Key]                  DATE            NULL,
    [Supplier Key]                  INT             NOT NULL,
    [Payment Method Key]            INT             NOT NULL,
    [Payment Terms Key]             INT             NULL,
    [Currency Key]                  INT             NULL,
    [Cost Center Key]               INT             NULL,
    [Vendor Contract Key]           INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Payment Run Reference]         NVARCHAR (20)   NOT NULL,
    [Payment Line Number]           INT             NOT NULL,
    [Supplier Invoice Number]       NVARCHAR (20)   NULL,
    [Purchase Order Number]         NVARCHAR (20)   NULL,
    [Cheque Number]                 NVARCHAR (15)   NULL,
    [Bank Reference]                NVARCHAR (35)   NULL,
    [Creditor Scheme Id]            NVARCHAR (35)   NULL,
    [Transaction Currency Code]     NCHAR (3)       NULL,
    [Gross Payment Amount]          DECIMAL (18, 2) NOT NULL,
    [Early Settlement Discount]     DECIMAL (18, 2) NULL,
    [Withholding Tax Amount]        DECIMAL (18, 2) NULL,
    [Net Payment Amount]            DECIMAL (18, 2) NOT NULL,
    [FX Rate To Reporting]          DECIMAL (19, 9) NULL,
    [Net Payment Amount Reporting]  DECIMAL (18, 2) NULL,
    [Realised FX Gain Loss]         DECIMAL (18, 2) NULL,
    [Days Paid Early Or Late]       INT             NULL,
    [Discount Captured Flag]        BIT             NULL,
    [Discount Lost Amount]          DECIMAL (18, 2) NULL,
    [Payment Block Reason Code]     NVARCHAR (6)    NULL,
    [Match Status Code]             NVARCHAR (4)    NULL,
    [Approval Level Code]           NVARCHAR (4)    NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Inferred Member Flag]          BIT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Supplier_Payment] PRIMARY KEY NONCLUSTERED ([Supplier Payment Key] ASC, [Payment Date Key] ASC) ON [PS_Date] ([Payment Date Key]),
    CONSTRAINT [FK_Fact_Supplier_Payment_Payment_Date_Key_Dimension_Date] FOREIGN KEY ([Payment Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Supplier_Payment_Supplier_Key_Dimension_Supplier] FOREIGN KEY ([Supplier Key]) REFERENCES [Dimension].[Supplier] ([Supplier Key]),
    CONSTRAINT [FK_Fact_Supplier_Payment_Payment_Method_Key_Dimension_Payment Method] FOREIGN KEY ([Payment Method Key]) REFERENCES [Dimension].[Payment Method] ([Payment Method Key])
)
ON [PS_Date] ([Payment Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Supplier_Payment_Natural_Key]
    ON [Fact].[Supplier Payment] ([Payment Run Reference] ASC, [Payment Line Number] ASC, [Payment Date Key] ASC)
    ON [PS_Date] ([Payment Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Supplier_Payment_Supplier_Invoice]
    ON [Fact].[Supplier Payment] ([Supplier Key] ASC, [Supplier Invoice Number] ASC)
    INCLUDE ([Net Payment Amount Reporting], [Days Paid Early Or Late], [Discount Captured Flag])
    ON [PS_Date] ([Payment Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Supplier_Payment]
    ON [Fact].[Supplier Payment]
    ON [PS_Date] ([Payment Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Accounts payable disbursement allocation fact',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Supplier Payment';
GO
