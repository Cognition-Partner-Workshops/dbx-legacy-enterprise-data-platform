/*
    Sales.OrderDeletionLog

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2310 - after 2300
    Depends on    : none
    Called by     : Sales.tr_Orders_LoggedDelete, order extract packages

    Order-specific deletion log, kept alongside the generic one because the
    order extract needs the customer and order date to route the delete to the
    right downstream partition, and reading them back out of the generic
    log's text snapshot was too slow.

    There is no foreign key to Sales.Orders: by the time a row lands here the
    order is gone.
*/
CREATE TABLE [Sales].[OrderDeletionLog] (
    [OrderDeletionLogID]    BIGINT          IDENTITY (1, 1) NOT NULL,
    [OrderID]               INT             NOT NULL,
    [CustomerID]            INT             NULL,
    [SalesTerritoryID]      INT             NULL,
    [OrderDate]             DATE            NULL,
    [OrderStatusAtDelete]   NVARCHAR (12)   NULL,
    [OrderValueAtDelete]    DECIMAL (18, 2) NULL,
    [DeletedWhen]           DATETIME2 (7)   CONSTRAINT [DF_Sales_OrderDeletionLog_DeletedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [DeletedByLogin]        NVARCHAR (128)  CONSTRAINT [DF_Sales_OrderDeletionLog_DeletedByLogin] DEFAULT (SUSER_SNAME()) NOT NULL,
    [DeletedByApplication]  NVARCHAR (128)  CONSTRAINT [DF_Sales_OrderDeletionLog_DeletedByApplication] DEFAULT (APP_NAME()) NOT NULL,
    [LineCountAtDelete]     SMALLINT        NULL,
    [IsCascadeFromCustomer] BIT             CONSTRAINT [DF_Sales_OrderDeletionLog_IsCascadeFromCustomer] DEFAULT (0) NOT NULL,
    [ExtractedByConsumerList] NVARCHAR (200) NULL,
    CONSTRAINT [PK_Sales_OrderDeletionLog] PRIMARY KEY CLUSTERED ([OrderDeletionLogID] ASC)
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_OrderDeletionLog_When]
    ON [Sales].[OrderDeletionLog] ([DeletedWhen] ASC)
    INCLUDE ([OrderID], [CustomerID], [SalesTerritoryID]);
GO
