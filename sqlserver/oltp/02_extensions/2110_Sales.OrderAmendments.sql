/*
    Sales.OrderAmendments

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2110 - after 2100
    Depends on    : Sales.Orders, Application.People
    Called by     : order maintenance screen, audit extracts

    Hand-rolled amendment history: one row per changed field, with the before
    and after values held as text regardless of the column's real type. This
    predates temporal tables in this database by a decade and was never
    retired, so amendments to the sample's temporal columns are recorded
    twice, in two different shapes.
*/
CREATE TABLE [Sales].[OrderAmendments] (
    [OrderAmendmentID]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [OrderID]               INT             NOT NULL,
    [AmendmentSequence]     SMALLINT        NOT NULL,
    [AmendedWhen]           DATETIME2 (7)   CONSTRAINT [DF_Sales_OrderAmendments_AmendedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [AmendedByPersonID]     INT             NOT NULL,
    [AmendmentTypeCode]     NVARCHAR (12)   NOT NULL,
    [TargetTableName]       NVARCHAR (60)   NOT NULL,
    [TargetKeyValue]        NVARCHAR (40)   NULL,
    [ChangedColumnName]     NVARCHAR (60)   NOT NULL,
    [OldValueText]          NVARCHAR (400)  NULL,
    [NewValueText]          NVARCHAR (400)  NULL,
    [ReasonCode]            NVARCHAR (10)   NULL,
    [ReasonNarrative]       NVARCHAR (400)  NULL,
    [RequiresCustomerApproval] BIT          CONSTRAINT [DF_Sales_OrderAmendments_RequiresCustomerApproval] DEFAULT (0) NOT NULL,
    [CustomerApprovedWhen]  DATETIME2 (7)   NULL,
    [SourceApplication]     NVARCHAR (30)   NULL,
    CONSTRAINT [PK_Sales_OrderAmendments] PRIMARY KEY CLUSTERED ([OrderAmendmentID] ASC),
    CONSTRAINT [UQ_Sales_OrderAmendments_Sequence] UNIQUE ([OrderID], [AmendmentSequence], [ChangedColumnName]),
    CONSTRAINT [CK_Sales_OrderAmendments_Type] CHECK ([AmendmentTypeCode] IN (N'QUANTITY', N'PRICE', N'DATE', N'ADDRESS', N'ITEM', N'CANCEL', N'OTHER')),
    CONSTRAINT [FK_Sales_OrderAmendments_Orders] FOREIGN KEY ([OrderID]) REFERENCES [Sales].[Orders] ([OrderID]),
    CONSTRAINT [FK_Sales_OrderAmendments_Application_People] FOREIGN KEY ([AmendedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_OrderAmendments_Order_When]
    ON [Sales].[OrderAmendments] ([OrderID] ASC, [AmendedWhen] DESC)
    INCLUDE ([AmendmentTypeCode], [ChangedColumnName]);
GO
