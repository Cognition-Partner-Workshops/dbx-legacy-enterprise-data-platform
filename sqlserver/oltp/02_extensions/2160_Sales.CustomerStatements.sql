/*
    Sales.CustomerStatements

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2160 - after 2150
    Depends on    : Sales.Customers, Application.People
    Called by     : statement run, credit control screens

    Month-end statement snapshots. The ageing buckets are stored, not derived,
    because the bucket boundaries changed in 2014 and reprinting an old
    statement has to reproduce what was sent. The 30/60/90 columns are fixed
    in the table shape, so the two-bucket APAC statements pack their figures
    into the 30 and 60 columns and leave 90 null.
*/
CREATE TABLE [Sales].[CustomerStatements] (
    [CustomerStatementID]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [StatementDate]         DATE            NOT NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [OpeningBalance]        DECIMAL (18, 2) NOT NULL,
    [InvoicedAmount]        DECIMAL (18, 2) CONSTRAINT [DF_Sales_CustomerStatements_InvoicedAmount] DEFAULT (0) NOT NULL,
    [CreditedAmount]        DECIMAL (18, 2) CONSTRAINT [DF_Sales_CustomerStatements_CreditedAmount] DEFAULT (0) NOT NULL,
    [ReceivedAmount]        DECIMAL (18, 2) CONSTRAINT [DF_Sales_CustomerStatements_ReceivedAmount] DEFAULT (0) NOT NULL,
    [AdjustmentAmount]      DECIMAL (18, 2) CONSTRAINT [DF_Sales_CustomerStatements_AdjustmentAmount] DEFAULT (0) NOT NULL,
    [ClosingBalance]        AS ([OpeningBalance] + [InvoicedAmount] - [CreditedAmount] - [ReceivedAmount] + [AdjustmentAmount]) PERSISTED,
    [CurrentBucketAmount]   DECIMAL (18, 2) NULL,
    [Overdue30Amount]       DECIMAL (18, 2) NULL,
    [Overdue60Amount]       DECIMAL (18, 2) NULL,
    [Overdue90PlusAmount]   DECIMAL (18, 2) NULL,
    [DisputedAmount]        DECIMAL (18, 2) NULL,
    [StatementStatus]       NVARCHAR (12)   CONSTRAINT [DF_Sales_CustomerStatements_StatementStatus] DEFAULT (N'GENERATED') NOT NULL,
    [DeliveryMethod]        NVARCHAR (12)   NULL,
    [SentWhen]              DATETIME2 (7)   NULL,
    [GeneratedByRun]        NVARCHAR (40)   NULL,
    CONSTRAINT [PK_Sales_CustomerStatements] PRIMARY KEY CLUSTERED ([CustomerStatementID] ASC),
    CONSTRAINT [UQ_Sales_CustomerStatements_Period] UNIQUE ([CustomerID], [StatementDate], [CurrencyCode]),
    CONSTRAINT [CK_Sales_CustomerStatements_Status] CHECK ([StatementStatus] IN (N'GENERATED', N'SENT', N'FAILED', N'SUPPRESSED', N'REPRINTED')),
    CONSTRAINT [CK_Sales_CustomerStatements_Delivery] CHECK ([DeliveryMethod] IS NULL OR [DeliveryMethod] IN (N'EMAIL', N'POST', N'PORTAL', N'EDI')),
    CONSTRAINT [FK_Sales_CustomerStatements_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_CustomerStatements_Date]
    ON [Sales].[CustomerStatements] ([StatementDate] DESC, [CustomerID] ASC)
    INCLUDE ([ClosingBalance], [Overdue90PlusAmount]);
GO
