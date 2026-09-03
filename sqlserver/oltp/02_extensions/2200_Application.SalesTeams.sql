/*
    Application.SalesTeams

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2200 - after 2190
    Depends on    : Sales.SalesTerritories, Application.People
    Called by     : Application.SalesTeamMembers, Sales.vw_SalespersonPerformance

    Selling teams. Teams nest (a national team owns regional teams) via
    ParentSalesTeamID with no depth limit, and the performance view walks the
    hierarchy with a recursive CTE that assumes three levels because nobody
    has ever built a fourth.
*/
CREATE TABLE [Application].[SalesTeams] (
    [SalesTeamID]           INT             IDENTITY (1, 1) NOT NULL,
    [TeamCode]              NVARCHAR (12)   NOT NULL,
    [TeamName]              NVARCHAR (60)   NOT NULL,
    [ParentSalesTeamID]     INT             NULL,
    [SalesTerritoryID]      INT             NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [TeamType]              NVARCHAR (12)   NOT NULL,
    [ManagerPersonID]       INT             NULL,
    [CostCentreCode]        NVARCHAR (20)   NULL,
    [FormedOnDate]          DATE            NOT NULL,
    [DisbandedOnDate]       DATE            NULL,
    [IsActive]              BIT             CONSTRAINT [DF_Application_SalesTeams_IsActive] DEFAULT (1) NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Application_SalesTeams_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Application_SalesTeams] PRIMARY KEY CLUSTERED ([SalesTeamID] ASC),
    CONSTRAINT [UQ_Application_SalesTeams_Code] UNIQUE ([TeamCode]),
    CONSTRAINT [CK_Application_SalesTeams_Type] CHECK ([TeamType] IN (N'FIELD', N'INSIDE', N'KEYACCOUNT', N'ECOMMERCE', N'DISTRIBUTOR')),
    CONSTRAINT [CK_Application_SalesTeams_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [FK_Application_SalesTeams_Parent] FOREIGN KEY ([ParentSalesTeamID]) REFERENCES [Application].[SalesTeams] ([SalesTeamID]),
    CONSTRAINT [FK_Application_SalesTeams_Territories] FOREIGN KEY ([SalesTerritoryID]) REFERENCES [Sales].[SalesTerritories] ([SalesTerritoryID]),
    CONSTRAINT [FK_Application_SalesTeams_Manager] FOREIGN KEY ([ManagerPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Application_SalesTeams_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
