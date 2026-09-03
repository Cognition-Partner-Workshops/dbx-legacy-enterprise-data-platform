/*
    Sales.CustomerDisputes

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2170 - after 2160
    Depends on    : Sales.Customers, Sales.Invoices, Application.People
    Called by     : credit control screens, Sales.vw_InvoiceExtract

    Invoice disputes. Raising a dispute suspends chasing on the disputed
    amount only, but the chasing job reads Sales.Invoices.DisputeFlag which is
    set for the whole invoice, so a partially disputed invoice stops being
    chased entirely. This has been on the backlog since 2016.
*/
CREATE TABLE [Sales].[CustomerDisputes] (
    [CustomerDisputeID]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [DisputeReference]      NVARCHAR (24)   NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [InvoiceID]             INT             NULL,
    [RaisedWhen]            DATETIME2 (7)   CONSTRAINT [DF_Sales_CustomerDisputes_RaisedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [RaisedByPersonID]      INT             NULL,
    [RaisedChannel]         NVARCHAR (12)   NULL,
    [DisputeCategoryCode]   NVARCHAR (12)   NOT NULL,
    [DisputedAmount]        DECIMAL (18, 2) NOT NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [DisputeNarrative]      NVARCHAR (MAX)  NULL,
    [OwnerPersonID]         INT             NULL,
    [TargetResolutionDate]  DATE            NULL,
    [DisputeStatus]         NVARCHAR (12)   CONSTRAINT [DF_Sales_CustomerDisputes_DisputeStatus] DEFAULT (N'OPEN') NOT NULL,
    [ResolutionCode]        NVARCHAR (12)   NULL,
    [ResolvedWhen]          DATETIME2 (7)   NULL,
    [CreditNoteID]          INT             NULL,
    [WriteOffAmount]        DECIMAL (18, 2) NULL,
    [EscalationLevel]       TINYINT         CONSTRAINT [DF_Sales_CustomerDisputes_EscalationLevel] DEFAULT (1) NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_CustomerDisputes_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_CustomerDisputes] PRIMARY KEY CLUSTERED ([CustomerDisputeID] ASC),
    CONSTRAINT [UQ_Sales_CustomerDisputes_Reference] UNIQUE ([DisputeReference]),
    CONSTRAINT [CK_Sales_CustomerDisputes_Category] CHECK ([DisputeCategoryCode] IN (N'PRICE', N'QUANTITY', N'QUALITY', N'DELIVERY', N'TAX', N'DUPLICATE', N'OTHER')),
    CONSTRAINT [CK_Sales_CustomerDisputes_Status] CHECK ([DisputeStatus] IN (N'OPEN', N'INVESTIGATING', N'AWAITINGCUST', N'RESOLVED', N'REJECTED', N'ESCALATED')),
    CONSTRAINT [CK_Sales_CustomerDisputes_Resolution] CHECK ([DisputeStatus] <> N'RESOLVED' OR ([ResolutionCode] IS NOT NULL AND [ResolvedWhen] IS NOT NULL)),
    CONSTRAINT [FK_Sales_CustomerDisputes_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Sales_CustomerDisputes_Invoices] FOREIGN KEY ([InvoiceID]) REFERENCES [Sales].[Invoices] ([InvoiceID]),
    CONSTRAINT [FK_Sales_CustomerDisputes_CreditNotes] FOREIGN KEY ([CreditNoteID]) REFERENCES [Returns].[CreditNotes] ([CreditNoteID]),
    CONSTRAINT [FK_Sales_CustomerDisputes_Owner] FOREIGN KEY ([OwnerPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Sales_CustomerDisputes_RaisedBy] FOREIGN KEY ([RaisedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_CustomerDisputes_Open]
    ON [Sales].[CustomerDisputes] ([TargetResolutionDate] ASC)
    INCLUDE ([CustomerID], [DisputedAmount], [OwnerPersonID])
    WHERE [DisputeStatus] IN (N'OPEN', N'INVESTIGATING', N'AWAITINGCUST', N'ESCALATED');
GO
