/*
    Application.SalesTeamMembers

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2210 - after 2200
    Depends on    : Application.SalesTeams, Application.People, Sales.CommissionPlans
    Called by     : Sales.usp_RecalculateCommissionAccruals, Sales.vw_SalespersonPerformance

    Effective-dated membership, maintained by hand rather than by a temporal
    table: closing a row means stamping ValidTo and inserting a new one, and
    the two operations are not in a transaction in the maintenance screen, so
    gaps and overlaps both exist. The unique filtered index below stops a
    person having two open rows on the same team but not on two teams.
*/
CREATE TABLE [Application].[SalesTeamMembers] (
    [SalesTeamMemberID]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [SalesTeamID]           INT             NOT NULL,
    [PersonID]              INT             NOT NULL,
    [RoleCode]              NVARCHAR (12)   NOT NULL,
    [CommissionPlanID]      INT             NULL,
    [QuotaSharePercent]     DECIMAL (5, 2)  CONSTRAINT [DF_Application_SalesTeamMembers_QuotaSharePercent] DEFAULT (100.00) NOT NULL,
    [ValidFrom]             DATE            NOT NULL,
    [ValidTo]               DATE            NULL,
    [IsPrimaryAssignment]   BIT             CONSTRAINT [DF_Application_SalesTeamMembers_IsPrimaryAssignment] DEFAULT (1) NOT NULL,
    [ReplacedBySalesTeamMemberID] BIGINT    NULL,
    [ChangeReasonCode]      NVARCHAR (10)   NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Application_SalesTeamMembers_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Application_SalesTeamMembers] PRIMARY KEY CLUSTERED ([SalesTeamMemberID] ASC),
    CONSTRAINT [CK_Application_SalesTeamMembers_Role] CHECK ([RoleCode] IN (N'REP', N'SENIORREP', N'MANAGER', N'SUPPORT', N'SPECIALIST')),
    CONSTRAINT [CK_Application_SalesTeamMembers_Validity] CHECK ([ValidTo] IS NULL OR [ValidTo] > [ValidFrom]),
    CONSTRAINT [CK_Application_SalesTeamMembers_Share] CHECK ([QuotaSharePercent] > 0 AND [QuotaSharePercent] <= 100),
    CONSTRAINT [FK_Application_SalesTeamMembers_Teams] FOREIGN KEY ([SalesTeamID]) REFERENCES [Application].[SalesTeams] ([SalesTeamID]),
    CONSTRAINT [FK_Application_SalesTeamMembers_People] FOREIGN KEY ([PersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Application_SalesTeamMembers_CommissionPlans] FOREIGN KEY ([CommissionPlanID]) REFERENCES [Sales].[CommissionPlans] ([CommissionPlanID]),
    CONSTRAINT [FK_Application_SalesTeamMembers_Replacement] FOREIGN KEY ([ReplacedBySalesTeamMemberID]) REFERENCES [Application].[SalesTeamMembers] ([SalesTeamMemberID]),
    CONSTRAINT [FK_Application_SalesTeamMembers_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Application_SalesTeamMembers_Open]
    ON [Application].[SalesTeamMembers] ([SalesTeamID] ASC, [PersonID] ASC)
    WHERE [ValidTo] IS NULL;
GO

CREATE NONCLUSTERED INDEX [IX_Application_SalesTeamMembers_Person]
    ON [Application].[SalesTeamMembers] ([PersonID] ASC, [ValidFrom] DESC)
    INCLUDE ([SalesTeamID], [RoleCode], [ValidTo]);
GO
