/*
    Sales.SalesTerritories

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1020
    Depends on    : Application.People, Application.StateProvinces
    Called by     : Sales.SalesQuotas, Sales.CommissionAccruals,
                    Sales.vw_SalespersonPerformance

    Territory hierarchy. Self-referencing parent, so a territory rolls up to a
    district, a district to a region. The three regions diverge here rather
    than in application code: TaxRegimeCode drives which tax routine the order
    pricing procedure calls (sales tax / VAT / GST), FiscalCalendarCode drives
    which period a quota is measured in, and PostalStandardCode drives the
    address cleanse rule applied on despatch.
*/
CREATE TABLE [Sales].[SalesTerritories] (
    [SalesTerritoryID]      INT             IDENTITY (1, 1) NOT NULL,
    [TerritoryCode]         NVARCHAR (12)   NOT NULL,
    [TerritoryName]         NVARCHAR (80)   NOT NULL,
    [ParentTerritoryID]     INT             NULL,
    [TerritoryLevel]        TINYINT         NOT NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [CountryISO3]           NCHAR (3)       NULL,
    [TaxRegimeCode]         NVARCHAR (10)   NOT NULL,
    [FiscalCalendarCode]    NVARCHAR (10)   NOT NULL,
    [ReportingCurrencyCode] NCHAR (3)       NOT NULL,
    [PostalStandardCode]    NVARCHAR (10)   NOT NULL,
    [ManagerPersonID]       INT             NULL,
    [IsActive]              BIT             CONSTRAINT [DF_Sales_SalesTerritories_IsActive] DEFAULT (1) NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_SalesTerritories_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_SalesTerritories] PRIMARY KEY CLUSTERED ([SalesTerritoryID] ASC),
    CONSTRAINT [UQ_Sales_SalesTerritories_TerritoryCode] UNIQUE ([TerritoryCode]),
    CONSTRAINT [CK_Sales_SalesTerritories_Level] CHECK ([TerritoryLevel] BETWEEN 1 AND 4),
    CONSTRAINT [CK_Sales_SalesTerritories_TaxRegime] CHECK ([TaxRegimeCode] IN (N'USSALESTAX', N'CAGSTHST', N'EUVAT', N'UKVAT', N'AUGST', N'JPCT', N'SGGST')),
    CONSTRAINT [CK_Sales_SalesTerritories_FiscalCalendar] CHECK ([FiscalCalendarCode] IN (N'NA445', N'EUCAL', N'APACJUN')),
    CONSTRAINT [CK_Sales_SalesTerritories_PostalStandard] CHECK ([PostalStandardCode] IN (N'USPS', N'CANADAPOST', N'ROYALMAIL', N'EUDIN5008', N'AUSPOST', N'JPPOST')),
    CONSTRAINT [FK_Sales_SalesTerritories_Parent] FOREIGN KEY ([ParentTerritoryID]) REFERENCES [Sales].[SalesTerritories] ([SalesTerritoryID]),
    CONSTRAINT [FK_Sales_SalesTerritories_Manager] FOREIGN KEY ([ManagerPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Sales_SalesTerritories_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_SalesTerritories_Parent]
    ON [Sales].[SalesTerritories] ([ParentTerritoryID] ASC)
    INCLUDE ([TerritoryCode], [RegionCode], [TaxRegimeCode]);
GO
