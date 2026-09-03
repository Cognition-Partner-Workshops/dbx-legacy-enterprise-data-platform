/*
    Sales.PaymentAllocations

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2150 - after 2140
    Depends on    : Sales.CustomerPayments, Sales.Invoices, Returns.CreditNotes,
                    Application.People
    Called by     : cash matching run, credit control screens

    Matching between receipts and what they settle. A receipt can settle
    invoices and credit notes at once, and an allocation can be reversed by
    writing a negative row rather than deleting - the audit requirement that
    finance imposed in 2009 and the reason the table has no update path.
*/
CREATE TABLE [Sales].[PaymentAllocations] (
    [PaymentAllocationID]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [CustomerPaymentID]     BIGINT          NOT NULL,
    [AllocatedWhen]         DATETIME2 (7)   CONSTRAINT [DF_Sales_PaymentAllocations_AllocatedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [TargetTypeCode]        NVARCHAR (12)   NOT NULL,
    [InvoiceID]             INT             NULL,
    [CreditNoteID]          INT             NULL,
    [AllocatedAmount]       DECIMAL (18, 2) NOT NULL,
    [SettlementDiscount]    DECIMAL (18, 2) CONSTRAINT [DF_Sales_PaymentAllocations_SettlementDiscount] DEFAULT (0) NOT NULL,
    [ExchangeDifference]    DECIMAL (18, 2) CONSTRAINT [DF_Sales_PaymentAllocations_ExchangeDifference] DEFAULT (0) NOT NULL,
    [MatchMethodCode]       NVARCHAR (12)   NOT NULL,
    [MatchConfidence]       TINYINT         NULL,
    [ReversalOfAllocationID] BIGINT         NULL,
    [AllocatedByPersonID]   INT             NULL,
    CONSTRAINT [PK_Sales_PaymentAllocations] PRIMARY KEY CLUSTERED ([PaymentAllocationID] ASC),
    CONSTRAINT [CK_Sales_PaymentAllocations_Target] CHECK ([TargetTypeCode] IN (N'INVOICE', N'CREDITNOTE', N'ONACCOUNT', N'WRITEOFF')),
    CONSTRAINT [CK_Sales_PaymentAllocations_TargetKey] CHECK (([TargetTypeCode] = N'INVOICE' AND [InvoiceID] IS NOT NULL)
                                                              OR ([TargetTypeCode] = N'CREDITNOTE' AND [CreditNoteID] IS NOT NULL)
                                                              OR [TargetTypeCode] IN (N'ONACCOUNT', N'WRITEOFF')),
    CONSTRAINT [CK_Sales_PaymentAllocations_Method] CHECK ([MatchMethodCode] IN (N'AUTOREF', N'AUTOAMOUNT', N'MANUAL', N'IMPORTED')),
    CONSTRAINT [FK_Sales_PaymentAllocations_Payments] FOREIGN KEY ([CustomerPaymentID]) REFERENCES [Sales].[CustomerPayments] ([CustomerPaymentID]),
    CONSTRAINT [FK_Sales_PaymentAllocations_Invoices] FOREIGN KEY ([InvoiceID]) REFERENCES [Sales].[Invoices] ([InvoiceID]),
    CONSTRAINT [FK_Sales_PaymentAllocations_CreditNotes] FOREIGN KEY ([CreditNoteID]) REFERENCES [Returns].[CreditNotes] ([CreditNoteID]),
    CONSTRAINT [FK_Sales_PaymentAllocations_Reversal] FOREIGN KEY ([ReversalOfAllocationID]) REFERENCES [Sales].[PaymentAllocations] ([PaymentAllocationID]),
    CONSTRAINT [FK_Sales_PaymentAllocations_Application_People] FOREIGN KEY ([AllocatedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_PaymentAllocations_Invoice]
    ON [Sales].[PaymentAllocations] ([InvoiceID] ASC)
    INCLUDE ([AllocatedAmount], [AllocatedWhen])
    WHERE [InvoiceID] IS NOT NULL;
GO
