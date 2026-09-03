/*
    Sales.Customers - additive column extensions

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2030 - after 2020
    Depends on    : Sales.Customers (Microsoft sample), Sales.SalesTerritories
    Called by     : Sales.usp_AssignCustomerSegments, Sales.ufn_DiscountPercentForCustomer,
                    Sales.vw_CustomerSegmentCurrent

    Credit control, tax registration and consent columns. The sample already
    has CreditLimit and IsOnCreditHold; those stay authoritative for the order
    entry screen, and CreditHoldReasonCode / CreditHoldSetWhen were added
    beside them by credit control without the screen ever being changed to
    write them, so they are populated only for holds applied after 2015.

    Consent is region-specific: EU rows require an explicit opt-in with a
    captured timestamp, NA rows default to opt-out, APAC rows vary by country
    and the country's rule is not held here at all - it lives in the web tier.
*/
IF COL_LENGTH(N'Sales.Customers', N'SalesTerritoryID') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [SalesTerritoryID] INT NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'RegionCode') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [RegionCode] NCHAR (4) NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'TaxRegistrationNumber') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [TaxRegistrationNumber] NVARCHAR (24) NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'TaxExemptionCertificate') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [TaxExemptionCertificate] NVARCHAR (40) NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'DefaultPriceListID') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [DefaultPriceListID] INT NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'CreditHoldReasonCode') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [CreditHoldReasonCode] NVARCHAR (10) NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'CreditHoldSetWhen') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [CreditHoldSetWhen] DATETIME2 (7) NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'CreditScoreValue') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [CreditScoreValue] SMALLINT NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'CreditScoreAgency') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [CreditScoreAgency] NVARCHAR (20) NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'CreditScoreCheckedOn') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [CreditScoreCheckedOn] DATE NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'AverageDaysToPay') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [AverageDaysToPay] SMALLINT NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'MarketingConsentFlag') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [MarketingConsentFlag] BIT NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'ConsentCapturedWhen') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [ConsentCapturedWhen] DATETIME2 (7) NULL;
GO

IF COL_LENGTH(N'Sales.Customers', N'DataRetentionExpiresOn') IS NULL
    ALTER TABLE [Sales].[Customers] ADD [DataRetentionExpiresOn] DATE NULL;
GO

IF OBJECT_ID(N'Sales.CK_Sales_Customers_RegionCode', N'C') IS NULL
    ALTER TABLE [Sales].[Customers]
        ADD CONSTRAINT [CK_Sales_Customers_RegionCode]
        CHECK ([RegionCode] IS NULL OR [RegionCode] IN (N'NA', N'EU', N'APAC'));
GO

IF OBJECT_ID(N'Sales.FK_Sales_Customers_SalesTerritories', N'F') IS NULL
    ALTER TABLE [Sales].[Customers]
        ADD CONSTRAINT [FK_Sales_Customers_SalesTerritories]
        FOREIGN KEY ([SalesTerritoryID]) REFERENCES [Sales].[SalesTerritories] ([SalesTerritoryID]);
GO

IF OBJECT_ID(N'Sales.FK_Sales_Customers_PriceLists', N'F') IS NULL
    ALTER TABLE [Sales].[Customers]
        ADD CONSTRAINT [FK_Sales_Customers_PriceLists]
        FOREIGN KEY ([DefaultPriceListID]) REFERENCES [Sales].[PriceLists] ([PriceListID]);
GO
