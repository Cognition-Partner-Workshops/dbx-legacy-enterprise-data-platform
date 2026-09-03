/*
    Additive indexes on Sales.Invoices and Sales.Customers

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 03_indexes / 3010 - after 3000
    Depends on    : Sales.Invoices, Sales.Customers and their 02_extensions columns
    Called by     : Sales.vw_InvoiceExtract, credit control queries, statement run

    The aged-debt index is filtered on the settlement statuses credit control
    cares about; it does not include disputed invoices, which is why the aged
    debt report and the dispute report never agree on the total.
*/
IF INDEXPROPERTY(OBJECT_ID(N'Sales.Invoices'), N'IX_Sales_Invoices_ChangeExtract', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Sales_Invoices_ChangeExtract]
        ON [Sales].[Invoices] ([LastEditedWhen] ASC)
        INCLUDE ([CustomerID], [InvoiceDate], [SettlementStatus], [InvoiceTotalExTax]);
GO

IF INDEXPROPERTY(OBJECT_ID(N'Sales.Invoices'), N'IX_Sales_Invoices_AgedDebt', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Sales_Invoices_AgedDebt]
        ON [Sales].[Invoices] ([PaymentDueDate] ASC, [CustomerID] ASC)
        INCLUDE ([AmountOutstanding], [CurrencyCode])
        WHERE [SettlementStatus] IN (N'OPEN', N'PARTPAID');
GO

IF INDEXPROPERTY(OBJECT_ID(N'Sales.Customers'), N'IX_Sales_Customers_TerritorySegment', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Sales_Customers_TerritorySegment]
        ON [Sales].[Customers] ([SalesTerritoryID] ASC, [RegionCode] ASC)
        INCLUDE ([CustomerName], [CreditLimit], [IsOnCreditHold]);
GO

IF INDEXPROPERTY(OBJECT_ID(N'Sales.Customers'), N'IX_Sales_Customers_RetentionDue', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Sales_Customers_RetentionDue]
        ON [Sales].[Customers] ([DataRetentionExpiresOn] ASC)
        INCLUDE ([RegionCode], [MarketingConsentFlag])
        WHERE [DataRetentionExpiresOn] IS NOT NULL;
GO
