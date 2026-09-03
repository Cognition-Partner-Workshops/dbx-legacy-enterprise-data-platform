/*
    Sales.CustomerWriteOffs

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2180 - after 2170
    Depends on    : Sales.Customers, Sales.Invoices, Sales.CustomerDisputes,
                    Application.People
    Called by     : bad-debt run, finance interface

    Bad debt and small-balance write-offs. Approval thresholds differ by
    region and are held as data on the row that was approved rather than in a
    policy table, so historical rows record thresholds that no longer exist.
*/
CREATE TABLE [Sales].[CustomerWriteOffs] (
    [CustomerWriteOffID]    BIGINT          IDENTITY (1, 1) NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [InvoiceID]             INT             NULL,
    [CustomerDisputeID]     BIGINT          NULL,
    [WriteOffTypeCode]      NVARCHAR (12)   NOT NULL,
    [WriteOffAmount]        DECIMAL (18, 2) NOT NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [WriteOffDate]          DATE            NOT NULL,
    [ReasonCode]            NVARCHAR (10)   NOT NULL,
    [ReasonNarrative]       NVARCHAR (400)  NULL,
    [ApprovalThresholdUsed] DECIMAL (18, 2) NULL,
    [ApprovedByPersonID]    INT             NULL,
    [ApprovedWhen]          DATETIME2 (7)   NULL,
    [GeneralLedgerCode]     NVARCHAR (20)   NULL,
    [PostedToLedgerWhen]    DATETIME2 (7)   NULL,
    [IsRecovered]           BIT             CONSTRAINT [DF_Sales_CustomerWriteOffs_IsRecovered] DEFAULT (0) NOT NULL,
    [RecoveredAmount]       DECIMAL (18, 2) NULL,
    [RecoveredWhen]         DATETIME2 (7)   NULL,
    CONSTRAINT [PK_Sales_CustomerWriteOffs] PRIMARY KEY CLUSTERED ([CustomerWriteOffID] ASC),
    CONSTRAINT [CK_Sales_CustomerWriteOffs_Type] CHECK ([WriteOffTypeCode] IN (N'BADDEBT', N'SMALLBAL', N'GOODWILL', N'FXDIFF', N'DISPUTE')),
    CONSTRAINT [CK_Sales_CustomerWriteOffs_Amount] CHECK ([WriteOffAmount] > 0),
    CONSTRAINT [CK_Sales_CustomerWriteOffs_Recovery] CHECK ([IsRecovered] = 0 OR [RecoveredAmount] IS NOT NULL),
    CONSTRAINT [FK_Sales_CustomerWriteOffs_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Sales_CustomerWriteOffs_Invoices] FOREIGN KEY ([InvoiceID]) REFERENCES [Sales].[Invoices] ([InvoiceID]),
    CONSTRAINT [FK_Sales_CustomerWriteOffs_Disputes] FOREIGN KEY ([CustomerDisputeID]) REFERENCES [Sales].[CustomerDisputes] ([CustomerDisputeID]),
    CONSTRAINT [FK_Sales_CustomerWriteOffs_Application_People] FOREIGN KEY ([ApprovedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_CustomerWriteOffs_Customer_Date]
    ON [Sales].[CustomerWriteOffs] ([CustomerID] ASC, [WriteOffDate] DESC)
    INCLUDE ([WriteOffAmount], [WriteOffTypeCode]);
GO
