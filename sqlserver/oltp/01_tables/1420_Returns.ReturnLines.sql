/*
    Returns.ReturnLines

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1420 - after Returns.ReturnAuthorizations
    Depends on    : Returns.ReturnAuthorizations, Returns.ReturnReasons,
                    Sales.InvoiceLines, Warehouse.StockItems
    Called by     : Returns.usp_PostReturnInspection, Returns.usp_IssueCreditNote

    RMA detail. The unit price is snapshotted from the original invoice line at
    authorisation, so a later price change does not move the credit. Quantity
    authorised, received and accepted are all held because they routinely
    differ.
*/
CREATE TABLE [Returns].[ReturnLines] (
    [ReturnLineID]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [ReturnAuthorizationID] INT             NOT NULL,
    [LineNumber]            SMALLINT        NOT NULL,
    [OriginalInvoiceLineID] INT             NULL,
    [StockItemID]           INT             NOT NULL,
    [ReturnReasonID]        INT             NOT NULL,
    [LotNumber]             NVARCHAR (30)   NULL,
    [QuantityAuthorized]    DECIMAL (18, 3) NOT NULL,
    [QuantityReceived]      DECIMAL (18, 3) NULL,
    [QuantityAccepted]      DECIMAL (18, 3) NULL,
    [QuantityScrapped]      DECIMAL (18, 3) NULL,
    [UnitPriceAtSale]       DECIMAL (18, 2) NOT NULL,
    [TaxRatePercentAtSale]  DECIMAL (18, 3) CONSTRAINT [DF_Returns_ReturnLines_TaxRatePercentAtSale] DEFAULT (0) NOT NULL,
    [RestockingPercent]     DECIMAL (5, 2)  NULL,
    [GrossCreditAmount]     AS (CONVERT(DECIMAL (18, 2), ISNULL([QuantityAccepted], [QuantityAuthorized]) * [UnitPriceAtSale])) PERSISTED,
    [DispositionCode]       NVARCHAR (12)   NULL,
    [LineStatus]            NVARCHAR (12)   CONSTRAINT [DF_Returns_ReturnLines_LineStatus] DEFAULT (N'AUTHORIZED') NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Returns_ReturnLines_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Returns_ReturnLines] PRIMARY KEY CLUSTERED ([ReturnLineID] ASC),
    CONSTRAINT [UQ_Returns_ReturnLines_LineNumber] UNIQUE ([ReturnAuthorizationID], [LineNumber]),
    CONSTRAINT [CK_Returns_ReturnLines_Quantity] CHECK ([QuantityAuthorized] > 0),
    CONSTRAINT [CK_Returns_ReturnLines_Disposition] CHECK ([DispositionCode] IS NULL OR [DispositionCode] IN (N'RESTOCK', N'REWORK', N'SCRAP', N'SUPPLIER', N'QUARANTINE')),
    CONSTRAINT [CK_Returns_ReturnLines_Status] CHECK ([LineStatus] IN (N'AUTHORIZED', N'RECEIVED', N'INSPECTED', N'CREDITED', N'REJECTED')),
    CONSTRAINT [FK_Returns_ReturnLines_Authorizations] FOREIGN KEY ([ReturnAuthorizationID]) REFERENCES [Returns].[ReturnAuthorizations] ([ReturnAuthorizationID]),
    CONSTRAINT [FK_Returns_ReturnLines_Reasons] FOREIGN KEY ([ReturnReasonID]) REFERENCES [Returns].[ReturnReasons] ([ReturnReasonID]),
    CONSTRAINT [FK_Returns_ReturnLines_InvoiceLines] FOREIGN KEY ([OriginalInvoiceLineID]) REFERENCES [Sales].[InvoiceLines] ([InvoiceLineID]),
    CONSTRAINT [FK_Returns_ReturnLines_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Returns_ReturnLines_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Returns_ReturnLines_StockItem]
    ON [Returns].[ReturnLines] ([StockItemID] ASC, [LastEditedWhen] DESC)
    INCLUDE ([ReturnReasonID], [QuantityAccepted], [DispositionCode]);
GO
