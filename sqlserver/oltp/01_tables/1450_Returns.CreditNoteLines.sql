/*
    Returns.CreditNoteLines

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1450 - after Returns.CreditNotes
    Depends on    : Returns.CreditNotes, Returns.ReturnLines, Warehouse.StockItems
    Called by     : Returns.usp_IssueCreditNote, Returns.vw_CreditNoteExtract

    Credit note detail. Lines can be goods (linked to a return line), freight
    (no stock item), or a goodwill adjustment raised by customer service with
    no return behind it at all - the last of these is why StockItemID and
    ReturnLineID are both nullable.
*/
CREATE TABLE [Returns].[CreditNoteLines] (
    [CreditNoteLineID]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [CreditNoteID]          INT             NOT NULL,
    [LineNumber]            SMALLINT        NOT NULL,
    [CreditLineType]        NVARCHAR (12)   NOT NULL,
    [ReturnLineID]          BIGINT          NULL,
    [StockItemID]           INT             NULL,
    [Description]           NVARCHAR (200)  NOT NULL,
    [Quantity]              DECIMAL (18, 3) CONSTRAINT [DF_Returns_CreditNoteLines_Quantity] DEFAULT (1) NOT NULL,
    [UnitCreditAmount]      DECIMAL (18, 2) NOT NULL,
    [TaxRatePercent]        DECIMAL (18, 3) CONSTRAINT [DF_Returns_CreditNoteLines_TaxRatePercent] DEFAULT (0) NOT NULL,
    [LineNetAmount]         AS (CONVERT(DECIMAL (18, 2), [Quantity] * [UnitCreditAmount])) PERSISTED,
    [LineTaxAmount]         AS (CONVERT(DECIMAL (18, 2), [Quantity] * [UnitCreditAmount] * [TaxRatePercent] / 100.0)) PERSISTED,
    [GeneralLedgerCode]     NVARCHAR (20)   NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Returns_CreditNoteLines_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Returns_CreditNoteLines] PRIMARY KEY CLUSTERED ([CreditNoteLineID] ASC),
    CONSTRAINT [UQ_Returns_CreditNoteLines_LineNumber] UNIQUE ([CreditNoteID], [LineNumber]),
    CONSTRAINT [CK_Returns_CreditNoteLines_Type] CHECK ([CreditLineType] IN (N'GOODS', N'FREIGHT', N'GOODWILL', N'PRICEADJ', N'RESTOCKFEE')),
    CONSTRAINT [CK_Returns_CreditNoteLines_Goods] CHECK ([CreditLineType] <> N'GOODS' OR [StockItemID] IS NOT NULL),
    CONSTRAINT [FK_Returns_CreditNoteLines_CreditNotes] FOREIGN KEY ([CreditNoteID]) REFERENCES [Returns].[CreditNotes] ([CreditNoteID]),
    CONSTRAINT [FK_Returns_CreditNoteLines_ReturnLines] FOREIGN KEY ([ReturnLineID]) REFERENCES [Returns].[ReturnLines] ([ReturnLineID]),
    CONSTRAINT [FK_Returns_CreditNoteLines_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Returns_CreditNoteLines_ReturnLine]
    ON [Returns].[CreditNoteLines] ([ReturnLineID] ASC)
    WHERE [ReturnLineID] IS NOT NULL;
GO
