/*
    Fact.Payment

    Object        : [Fact].[Payment] - transaction fact, one row per customer
                    cash receipt allocation line (AR cash application).
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Customer, Dimension.Payment Method,
                    Dimension.Currency, Dimension.Date (WP05).
    Called by     : loaded by Integration.usp_LoadFactPayment, driven by
                    FIN_Load_Payments; corrected by
                    Integration.usp_ApplyFactCorrections.
    Grain         : one allocation of one receipt against one invoice.

    Correction pattern: Fact.Payment is restated *in place*. Cash application is
    re-run by the collections team all day and the reversal pattern used on
    Fact.Sale produced unusable cash reports, so in 2011 the load was changed to
    update the existing row and bump [Restatement Version]. Fact.Sale keeps the
    reversal pattern. Both patterns therefore live in the estate on purpose.

    Regional divergence: NA receipts arrive as ACH/lockbox files and carry a
    lockbox batch; EU receipts arrive as SEPA credit transfers with an
    end-to-end id and are matched on the structured remittance reference; APAC
    receipts are largely bank transfers with a withholding-tax deduction that
    has to be grossed back up before the invoice is considered settled.
*/
CREATE TABLE [Fact].[Payment] (
    [Payment Key]                   BIGINT          IDENTITY (1, 1) NOT NULL,
    [Payment Date Key]              DATE            NOT NULL,
    [Value Date Key]                DATE            NULL,
    [Invoice Date Key]              DATE            NULL,
    [Customer Key]                  INT             NOT NULL,
    [Bill To Customer Key]          INT             NULL,
    [Payment Method Key]            INT             NOT NULL,
    [Currency Key]                  INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Cost Center Key]               INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Receipt Number]                NVARCHAR (20)   NOT NULL,
    [Receipt Line Number]           INT             NOT NULL,
    [Invoice Number]                NVARCHAR (20)   NULL,
    [Remittance Reference]          NVARCHAR (40)   NULL,
    [Lockbox Batch Reference]       NVARCHAR (20)   NULL,
    [SEPA End To End Id]            NVARCHAR (35)   NULL,
    [Transaction Currency Code]     NCHAR (3)       NULL,
    [Payment Amount]                DECIMAL (18, 2) NOT NULL,
    [Allocated Amount]              DECIMAL (18, 2) NOT NULL,
    [Unallocated Amount]            DECIMAL (18, 2) NULL,
    [Settlement Discount Amount]    DECIMAL (18, 2) NULL,
    [Withholding Tax Amount]        DECIMAL (18, 2) NULL,
    [Bank Charge Amount]            DECIMAL (18, 2) NULL,
    [Write Off Amount]              DECIMAL (18, 2) NULL,
    [FX Rate To Reporting]          DECIMAL (19, 9) NULL,
    [FX Rate Source Code]           NVARCHAR (10)   NULL,
    [Allocated Amount Reporting]    DECIMAL (18, 2) NULL,
    [Realised FX Gain Loss]         DECIMAL (18, 2) NULL,
    [Days To Pay]                   INT             NULL,
    [Days Beyond Terms]             INT             NULL,
    [Payment Status Code]           NVARCHAR (6)    NULL,
    [Restatement Version]           INT             CONSTRAINT [DF_Fact_Payment_Restatement_Version] DEFAULT (1) NOT NULL,
    [Restated Datetime]             DATETIME2 (3)   NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Inferred Member Flag]          BIT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Payment] PRIMARY KEY NONCLUSTERED ([Payment Key] ASC, [Payment Date Key] ASC) ON [PS_Date] ([Payment Date Key]),
    CONSTRAINT [FK_Fact_Payment_Payment_Date_Key_Dimension_Date] FOREIGN KEY ([Payment Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Payment_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key]),
    CONSTRAINT [FK_Fact_Payment_Payment_Method_Key_Dimension_Payment Method] FOREIGN KEY ([Payment Method Key]) REFERENCES [Dimension].[Payment Method] ([Payment Method Key])
)
ON [PS_Date] ([Payment Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Payment_Natural_Key]
    ON [Fact].[Payment] ([Receipt Number] ASC, [Receipt Line Number] ASC, [Payment Date Key] ASC)
    ON [PS_Date] ([Payment Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Payment_Invoice_Number]
    ON [Fact].[Payment] ([Invoice Number] ASC)
    INCLUDE ([Allocated Amount Reporting], [Days To Pay])
    ON [PS_Date] ([Payment Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Payment_Customer_Period]
    ON [Fact].[Payment] ([Customer Key] ASC, [Payment Date Key] ASC)
    INCLUDE ([Allocated Amount Reporting], [Days Beyond Terms], [Region Code])
    ON [PS_Date] ([Payment Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Payment]
    ON [Fact].[Payment]
    ON [PS_Date] ([Payment Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Customer cash receipt allocation fact; restated in place, see Restatement Version',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Payment';
GO
