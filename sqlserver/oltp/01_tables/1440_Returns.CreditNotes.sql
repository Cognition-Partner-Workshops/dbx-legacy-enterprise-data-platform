/*
    Returns.CreditNotes

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1440 - after Returns.ReturnAuthorizations
    Depends on    : Sales.Customers, Sales.Invoices, Returns.ReturnAuthorizations,
                    Application.People
    Called by     : Returns.CreditNoteLines, Returns.usp_IssueCreditNote,
                    Returns.vw_CreditNoteExtract

    Credit note header. Tax handling diverges by region and is snapshotted, not
    looked up: NA credits reverse sales tax at the rate in force on the
    original invoice date, EU credits reverse VAT and must carry the original
    invoice's VAT number and a sequential credit-note number with no gaps, and
    APAC credits reverse GST at the credit date's rate. The gapless EU
    numbering is maintained by the issuing procedure, not by a sequence, which
    is why NumberSeriesCode and NumberWithinSeries exist.
*/
CREATE TABLE [Returns].[CreditNotes] (
    [CreditNoteID]          INT             CONSTRAINT [DF_Returns_CreditNotes_CreditNoteID] DEFAULT (NEXT VALUE FOR [Sequences].[CreditNoteID]) NOT NULL,
    [CreditNoteNumber]      NVARCHAR (24)   NOT NULL,
    [NumberSeriesCode]      NVARCHAR (10)   NOT NULL,
    [NumberWithinSeries]    INT             NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [ReturnAuthorizationID] INT             NULL,
    [OriginalInvoiceID]     INT             NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [CreditReasonCode]      NVARCHAR (12)   NOT NULL,
    [IssuedDate]            DATE            NOT NULL,
    [TaxPointDate]          DATE            NOT NULL,
    [TaxRegimeCode]         NVARCHAR (12)   NOT NULL,
    [CustomerTaxNumber]     NVARCHAR (24)   NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [ExchangeRateToUsd]     DECIMAL (18, 8) CONSTRAINT [DF_Returns_CreditNotes_ExchangeRateToUsd] DEFAULT (1) NOT NULL,
    [NetAmount]             DECIMAL (18, 2) CONSTRAINT [DF_Returns_CreditNotes_NetAmount] DEFAULT (0) NOT NULL,
    [TaxAmount]             DECIMAL (18, 2) CONSTRAINT [DF_Returns_CreditNotes_TaxAmount] DEFAULT (0) NOT NULL,
    [RestockingFeeAmount]   DECIMAL (18, 2) CONSTRAINT [DF_Returns_CreditNotes_RestockingFeeAmount] DEFAULT (0) NOT NULL,
    [TotalAmount]           AS (CONVERT(DECIMAL (18, 2), [NetAmount] + [TaxAmount] - [RestockingFeeAmount])) PERSISTED,
    [AppliedAmount]         DECIMAL (18, 2) CONSTRAINT [DF_Returns_CreditNotes_AppliedAmount] DEFAULT (0) NOT NULL,
    [CreditNoteStatus]      NVARCHAR (12)   CONSTRAINT [DF_Returns_CreditNotes_CreditNoteStatus] DEFAULT (N'DRAFT') NOT NULL,
    [IsRefundToCard]        BIT             CONSTRAINT [DF_Returns_CreditNotes_IsRefundToCard] DEFAULT (0) NOT NULL,
    [RefundReference]       NVARCHAR (40)   NULL,
    [PostedToLedgerWhen]    DATETIME2 (7)   NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Returns_CreditNotes_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Returns_CreditNotes] PRIMARY KEY CLUSTERED ([CreditNoteID] ASC),
    CONSTRAINT [UQ_Returns_CreditNotes_Number] UNIQUE ([CreditNoteNumber]),
    CONSTRAINT [UQ_Returns_CreditNotes_Series] UNIQUE ([NumberSeriesCode], [NumberWithinSeries]),
    CONSTRAINT [CK_Returns_CreditNotes_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [CK_Returns_CreditNotes_TaxRegime] CHECK ([TaxRegimeCode] IN (N'SALESTAX', N'VAT', N'GST', N'CONSUMPTION', N'NONE')),
    CONSTRAINT [CK_Returns_CreditNotes_Status] CHECK ([CreditNoteStatus] IN (N'DRAFT', N'ISSUED', N'APPLIED', N'REFUNDED', N'CANCELLED')),
    CONSTRAINT [CK_Returns_CreditNotes_EuTaxNumber] CHECK ([TaxRegimeCode] <> N'VAT' OR [CreditNoteStatus] = N'DRAFT' OR [CustomerTaxNumber] IS NOT NULL),
    CONSTRAINT [CK_Returns_CreditNotes_Applied] CHECK ([AppliedAmount] >= 0),
    CONSTRAINT [FK_Returns_CreditNotes_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Returns_CreditNotes_Authorizations] FOREIGN KEY ([ReturnAuthorizationID]) REFERENCES [Returns].[ReturnAuthorizations] ([ReturnAuthorizationID]),
    CONSTRAINT [FK_Returns_CreditNotes_Invoices] FOREIGN KEY ([OriginalInvoiceID]) REFERENCES [Sales].[Invoices] ([InvoiceID]),
    CONSTRAINT [FK_Returns_CreditNotes_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Returns_CreditNotes_Customer]
    ON [Returns].[CreditNotes] ([CustomerID] ASC, [IssuedDate] DESC)
    INCLUDE ([TotalAmount], [CreditNoteStatus], [CurrencyCode]);
GO

CREATE NONCLUSTERED INDEX [IX_Returns_CreditNotes_Unapplied]
    ON [Returns].[CreditNotes] ([IssuedDate] ASC)
    INCLUDE ([CustomerID], [TotalAmount], [AppliedAmount])
    WHERE [CreditNoteStatus] = N'ISSUED';
GO
