/*
    Warehouse.CycleCounts

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1260 - after Warehouse.WarehouseSites
    Depends on    : Warehouse.WarehouseSites, Application.People
    Called by     : Warehouse.CycleCountLines, Warehouse.usp_ReconcileCycleCount

    Cycle count header. A count is raised against a zone or an ABC class, is
    counted blind (the counter does not see the system quantity), and is then
    reconciled. A second count is required when the first exceeds the site's
    tolerance, which is why RecountOfCycleCountID exists.
*/
CREATE TABLE [Warehouse].[CycleCounts] (
    [CycleCountID]          INT             IDENTITY (1, 1) NOT NULL,
    [CountReference]        NVARCHAR (20)   NOT NULL,
    [WarehouseSiteID]       INT             NOT NULL,
    [CountType]             NVARCHAR (12)   NOT NULL,
    [ZoneCode]              NVARCHAR (8)    NULL,
    [AbcClass]              NCHAR (1)       NULL,
    [ScheduledDate]         DATE            NOT NULL,
    [StartedWhen]           DATETIME2 (7)   NULL,
    [CompletedWhen]         DATETIME2 (7)   NULL,
    [CountedByPersonID]     INT             NULL,
    [ApprovedByPersonID]    INT             NULL,
    [ToleranceValueAmount]  DECIMAL (18, 2) NULL,
    [TotalVarianceValue]    DECIMAL (18, 2) NULL,
    [CountStatus]           NVARCHAR (12)   CONSTRAINT [DF_Warehouse_CycleCounts_CountStatus] DEFAULT (N'SCHEDULED') NOT NULL,
    [RecountOfCycleCountID] INT             NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Warehouse_CycleCounts_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Warehouse_CycleCounts] PRIMARY KEY CLUSTERED ([CycleCountID] ASC),
    CONSTRAINT [UQ_Warehouse_CycleCounts_Reference] UNIQUE ([CountReference]),
    CONSTRAINT [CK_Warehouse_CycleCounts_Type] CHECK ([CountType] IN (N'ZONE', N'ABC', N'ITEM', N'FULL', N'SPOT')),
    CONSTRAINT [CK_Warehouse_CycleCounts_AbcClass] CHECK ([AbcClass] IS NULL OR [AbcClass] IN (N'A', N'B', N'C')),
    CONSTRAINT [CK_Warehouse_CycleCounts_Status] CHECK ([CountStatus] IN (N'SCHEDULED', N'COUNTING', N'COUNTED', N'RECONCILED', N'RECOUNT', N'CANCELLED')),
    CONSTRAINT [FK_Warehouse_CycleCounts_Sites] FOREIGN KEY ([WarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Warehouse_CycleCounts_Recount] FOREIGN KEY ([RecountOfCycleCountID]) REFERENCES [Warehouse].[CycleCounts] ([CycleCountID]),
    CONSTRAINT [FK_Warehouse_CycleCounts_CountedBy] FOREIGN KEY ([CountedByPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Warehouse_CycleCounts_ApprovedBy] FOREIGN KEY ([ApprovedByPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Warehouse_CycleCounts_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Warehouse_CycleCounts_Due]
    ON [Warehouse].[CycleCounts] ([ScheduledDate] ASC, [WarehouseSiteID] ASC)
    WHERE [CountStatus] IN (N'SCHEDULED', N'COUNTING');
GO
