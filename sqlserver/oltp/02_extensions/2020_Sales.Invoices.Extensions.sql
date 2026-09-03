/*
    Sales.Invoices - additive column extensions

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2020 - after 2010
    Depends on    : Sales.Invoices (Microsoft sample), Sales.SalesTerritories,
                    Loyalty.LoyaltyMembers
    Called by     : Sales.vw_InvoiceExtract, Loyalty.usp_AccruePointsForInvoice,
                    Sales.usp_RecalculateCommissionAccruals

    Settlement, tax-regime and collection columns added to the sample invoice
    header. AmountOutstanding is maintained by the payment allocation
    procedure and is the figure credit control works from; it is not derived
    on read, and it is known to disagree with the allocation ledger on
    invoices touched by the 2018 currency reset.
*/
IF COL_LENGTH(N'Sales.Invoices', N'SalesTerritoryID') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [SalesTerritoryID] INT NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'TaxRegimeCode') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [TaxRegimeCode] NVARCHAR (12) NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'CustomerTaxNumber') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [CustomerTaxNumber] NVARCHAR (24) NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'TaxPointDate') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [TaxPointDate] DATE NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'CurrencyCode') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [CurrencyCode] NCHAR (3) NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'ExchangeRateToUsd') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [ExchangeRateToUsd] DECIMAL (18, 8) NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'InvoiceTotalExTax') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [InvoiceTotalExTax] DECIMAL (18, 2) NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'InvoiceTaxAmount') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [InvoiceTaxAmount] DECIMAL (18, 2) NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'AmountOutstanding') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [AmountOutstanding] DECIMAL (18, 2) NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'SettlementStatus') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [SettlementStatus] NVARCHAR (12) NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'PaymentDueDate') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [PaymentDueDate] DATE NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'DisputeFlag') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [DisputeFlag] BIT NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'LoyaltyMemberID') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [LoyaltyMemberID] INT NULL;
GO

IF COL_LENGTH(N'Sales.Invoices', N'LoyaltyPointsAccrued') IS NULL
    ALTER TABLE [Sales].[Invoices] ADD [LoyaltyPointsAccrued] INT NULL;
GO

IF OBJECT_ID(N'Sales.CK_Sales_Invoices_SettlementStatus', N'C') IS NULL
    ALTER TABLE [Sales].[Invoices]
        ADD CONSTRAINT [CK_Sales_Invoices_SettlementStatus]
        CHECK ([SettlementStatus] IS NULL OR [SettlementStatus] IN (N'OPEN', N'PARTPAID', N'PAID', N'DISPUTED', N'WRITTENOFF', N'CREDITED'));
GO

IF OBJECT_ID(N'Sales.FK_Sales_Invoices_SalesTerritories', N'F') IS NULL
    ALTER TABLE [Sales].[Invoices]
        ADD CONSTRAINT [FK_Sales_Invoices_SalesTerritories]
        FOREIGN KEY ([SalesTerritoryID]) REFERENCES [Sales].[SalesTerritories] ([SalesTerritoryID]);
GO

IF OBJECT_ID(N'Sales.FK_Sales_Invoices_LoyaltyMembers', N'F') IS NULL
    ALTER TABLE [Sales].[Invoices]
        ADD CONSTRAINT [FK_Sales_Invoices_LoyaltyMembers]
        FOREIGN KEY ([LoyaltyMemberID]) REFERENCES [Loyalty].[LoyaltyMembers] ([LoyaltyMemberID]);
GO
