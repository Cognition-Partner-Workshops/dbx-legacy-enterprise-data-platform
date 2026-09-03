/*
    Sales.SalesQuotas

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1090 - after Sales.SalesTerritories
    Depends on    : Sales.SalesTerritories, Application.People
    Called by     : Sales.usp_RefreshSalesQuotaAttainment, Sales.vw_SalespersonPerformance

    Quota per salesperson per fiscal period. The fiscal period label is not a
    date: NA uses a 4-4-5 retail calendar ('FY2019-P07'), EU uses calendar
    months ('2019-07') and APAC uses a July-June year with quarters
    ('FY20-Q1'). The period is resolved against the territory's
    FiscalCalendarCode. Nothing normalises these three shapes in the OLTP; the
    warehouse load is where that argument gets settled.

    AttainmentAmount and AttainmentPercent are denormalised caches maintained
    by Sales.usp_RefreshSalesQuotaAttainment. They drift between runs.
*/
CREATE TABLE [Sales].[SalesQuotas] (
    [SalesQuotaID]          INT             IDENTITY (1, 1) NOT NULL,
    [SalesTerritoryID]      INT             NOT NULL,
    [SalespersonPersonID]   INT             NOT NULL,
    [FiscalCalendarCode]    NVARCHAR (10)   NOT NULL,
    [FiscalPeriodLabel]     NVARCHAR (20)   NOT NULL,
    [PeriodStartDate]       DATE            NOT NULL,
    [PeriodEndDate]         DATE            NOT NULL,
    [QuotaAmount]           DECIMAL (18, 2) NOT NULL,
    [QuotaCurrencyCode]     NCHAR (3)       NOT NULL,
    [StretchQuotaAmount]    DECIMAL (18, 2) NULL,
    [AttainmentAmount]      DECIMAL (18, 2) CONSTRAINT [DF_Sales_SalesQuotas_AttainmentAmount] DEFAULT (0) NOT NULL,
    [AttainmentPercent]     AS (CASE WHEN [QuotaAmount] = 0 THEN NULL
                                     ELSE CONVERT(DECIMAL (9, 4), [AttainmentAmount] / [QuotaAmount] * 100) END) PERSISTED,
    [AttainmentRefreshedWhen] DATETIME2 (7) NULL,
    [QuotaStatus]           NVARCHAR (12)   CONSTRAINT [DF_Sales_SalesQuotas_QuotaStatus] DEFAULT (N'OPEN') NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_SalesQuotas_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_SalesQuotas] PRIMARY KEY CLUSTERED ([SalesQuotaID] ASC),
    CONSTRAINT [UQ_Sales_SalesQuotas_Person_Period] UNIQUE ([SalespersonPersonID], [SalesTerritoryID], [FiscalPeriodLabel]),
    CONSTRAINT [CK_Sales_SalesQuotas_Period] CHECK ([PeriodEndDate] >= [PeriodStartDate]),
    CONSTRAINT [CK_Sales_SalesQuotas_Quota] CHECK ([QuotaAmount] >= 0),
    CONSTRAINT [CK_Sales_SalesQuotas_Status] CHECK ([QuotaStatus] IN (N'OPEN', N'LOCKED', N'RESTATED')),
    CONSTRAINT [FK_Sales_SalesQuotas_Territories] FOREIGN KEY ([SalesTerritoryID]) REFERENCES [Sales].[SalesTerritories] ([SalesTerritoryID]),
    CONSTRAINT [FK_Sales_SalesQuotas_Salesperson] FOREIGN KEY ([SalespersonPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Sales_SalesQuotas_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_SalesQuotas_Period]
    ON [Sales].[SalesQuotas] ([PeriodStartDate] ASC, [PeriodEndDate] ASC)
    INCLUDE ([SalespersonPersonID], [QuotaAmount], [AttainmentAmount]);
GO
