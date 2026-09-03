/*
    Warehouse.StockItemDeletionLog

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2320 - after 2310
    Depends on    : none
    Called by     : Warehouse.tr_StockItems_LoggedDelete, item master extracts

    Item master deletion log. Items are supposed to be discontinued rather
    than deleted, and the item master extract treats a row here as a hard
    delete downstream; the handful of rows that exist all date from the 2015
    catalogue clean-up and each one broke a warehouse report.
*/
CREATE TABLE [Warehouse].[StockItemDeletionLog] (
    [StockItemDeletionLogID] BIGINT         IDENTITY (1, 1) NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [StockItemName]         NVARCHAR (100)  NULL,
    [SupplierID]            INT             NULL,
    [WasDiscontinued]       BIT             CONSTRAINT [DF_Warehouse_StockItemDeletionLog_WasDiscontinued] DEFAULT (0) NOT NULL,
    [QuantityOnHandAtDelete] DECIMAL (18, 3) NULL,
    [DeletedWhen]           DATETIME2 (7)   CONSTRAINT [DF_Warehouse_StockItemDeletionLog_DeletedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [DeletedByLogin]        NVARCHAR (128)  CONSTRAINT [DF_Warehouse_StockItemDeletionLog_DeletedByLogin] DEFAULT (SUSER_SNAME()) NOT NULL,
    [DeleteReasonCode]      NVARCHAR (10)   NULL,
    CONSTRAINT [PK_Warehouse_StockItemDeletionLog] PRIMARY KEY CLUSTERED ([StockItemDeletionLogID] ASC)
);
GO

CREATE NONCLUSTERED INDEX [IX_Warehouse_StockItemDeletionLog_When]
    ON [Warehouse].[StockItemDeletionLog] ([DeletedWhen] ASC)
    INCLUDE ([StockItemID]);
GO
