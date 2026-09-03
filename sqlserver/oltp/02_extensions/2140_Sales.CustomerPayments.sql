/*
    Sales.CustomerPayments

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2140 - after 2130
    Depends on    : Sales.Customers, Application.People
    Called by     : Sales.PaymentAllocations, cash-posting interface

    Cash received. The sample has CustomerTransactions for the ledger effect;
    this is the receipt as banked, before it is matched to invoices, which is
    a distinction the finance team insisted on after the 2011 audit.

    Card receipts carry only the masked last four digits and the acquirer's
    reference. No card number, expiry or authentication data is stored here or
    anywhere else in this database.
*/
CREATE TABLE [Sales].[CustomerPayments] (
    [CustomerPaymentID]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [PaymentReference]      NVARCHAR (30)   NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [ReceivedWhen]          DATETIME2 (7)   NOT NULL,
    [ValueDate]             DATE            NOT NULL,
    [PaymentMethodCode]     NVARCHAR (12)   NOT NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [ReceivedAmount]        DECIMAL (18, 2) NOT NULL,
    [ExchangeRateToUsd]     DECIMAL (18, 8) CONSTRAINT [DF_Sales_CustomerPayments_ExchangeRateToUsd] DEFAULT (1) NOT NULL,
    [BankChargeAmount]      DECIMAL (18, 2) CONSTRAINT [DF_Sales_CustomerPayments_BankChargeAmount] DEFAULT (0) NOT NULL,
    [AllocatedAmount]       DECIMAL (18, 2) CONSTRAINT [DF_Sales_CustomerPayments_AllocatedAmount] DEFAULT (0) NOT NULL,
    [UnallocatedAmount]     AS ([ReceivedAmount] - [AllocatedAmount]) PERSISTED,
    [BankAccountCode]       NVARCHAR (20)   NULL,
    [BankStatementRef]      NVARCHAR (40)   NULL,
    [CardLastFourDigits]    NCHAR (4)       NULL,
    [AcquirerReference]     NVARCHAR (40)   NULL,
    [PaymentStatus]         NVARCHAR (12)   CONSTRAINT [DF_Sales_CustomerPayments_PaymentStatus] DEFAULT (N'RECEIVED') NOT NULL,
    [ReversalReasonCode]    NVARCHAR (10)   NULL,
    [ReversedWhen]          DATETIME2 (7)   NULL,
    [PostedByPersonID]      INT             NULL,
    [SourceInterfaceCode]   NVARCHAR (20)   NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_CustomerPayments_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_CustomerPayments] PRIMARY KEY CLUSTERED ([CustomerPaymentID] ASC),
    CONSTRAINT [UQ_Sales_CustomerPayments_Reference] UNIQUE ([PaymentReference]),
    CONSTRAINT [CK_Sales_CustomerPayments_Method] CHECK ([PaymentMethodCode] IN (N'BANKXFER', N'DIRECTDEBIT', N'CARD', N'CHEQUE', N'CASH', N'BILLOFEXCH', N'OFFSET')),
    CONSTRAINT [CK_Sales_CustomerPayments_Status] CHECK ([PaymentStatus] IN (N'RECEIVED', N'PARTALLOCATED', N'ALLOCATED', N'UNAPPLIED', N'REVERSED', N'BOUNCED')),
    CONSTRAINT [CK_Sales_CustomerPayments_Amount] CHECK ([ReceivedAmount] <> 0),
    CONSTRAINT [CK_Sales_CustomerPayments_Allocated] CHECK ([AllocatedAmount] >= 0),
    CONSTRAINT [FK_Sales_CustomerPayments_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Sales_CustomerPayments_Application_People] FOREIGN KEY ([PostedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_CustomerPayments_Customer_Value]
    ON [Sales].[CustomerPayments] ([CustomerID] ASC, [ValueDate] DESC)
    INCLUDE ([ReceivedAmount], [AllocatedAmount], [PaymentStatus]);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_CustomerPayments_Unallocated]
    ON [Sales].[CustomerPayments] ([ValueDate] ASC)
    INCLUDE ([CustomerID], [UnallocatedAmount])
    WHERE [PaymentStatus] IN (N'RECEIVED', N'PARTALLOCATED', N'UNAPPLIED');
GO
