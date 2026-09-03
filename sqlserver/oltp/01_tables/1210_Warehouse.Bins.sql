/*
    Warehouse.Bins

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1210 - after Warehouse.WarehouseSites
    Depends on    : Warehouse.WarehouseSites, Application.People
    Called by     : Warehouse.BinContents, Warehouse.CycleCountLines,
                    Warehouse.usp_PostStockMovement

    Bin (location) master. BinCode is the printed label and is only unique
    within a site. The composite location string is computed so that the
    hand-held terminals, which expect one field, can scan it directly.
*/
CREATE TABLE [Warehouse].[Bins] (
    [BinID]                 INT             IDENTITY (1, 1) NOT NULL,
    [WarehouseSiteID]       INT             NOT NULL,
    [BinCode]               NVARCHAR (16)   NOT NULL,
    [ZoneCode]              NVARCHAR (8)    NOT NULL,
    [AisleCode]             NVARCHAR (6)    NULL,
    [RackCode]              NVARCHAR (6)    NULL,
    [ShelfCode]             NVARCHAR (6)    NULL,
    [LocationLabel]         AS (CONCAT([ZoneCode], N'-', ISNULL([AisleCode], N'00'), N'-',
                                       ISNULL([RackCode], N'00'), N'-', ISNULL([ShelfCode], N'00'))) PERSISTED,
    [BinType]               NVARCHAR (12)   NOT NULL,
    [IsChiller]             BIT             CONSTRAINT [DF_Warehouse_Bins_IsChiller] DEFAULT (0) NOT NULL,
    [IsQuarantine]          BIT             CONSTRAINT [DF_Warehouse_Bins_IsQuarantine] DEFAULT (0) NOT NULL,
    [MaximumWeightKg]       DECIMAL (12, 3) NULL,
    [MaximumVolumeM3]       DECIMAL (12, 4) NULL,
    [PickSequence]          INT             NULL,
    [BinStatus]             NVARCHAR (12)   CONSTRAINT [DF_Warehouse_Bins_BinStatus] DEFAULT (N'AVAILABLE') NOT NULL,
    [LastCountedWhen]       DATETIME2 (7)   NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Warehouse_Bins_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Warehouse_Bins] PRIMARY KEY CLUSTERED ([BinID] ASC),
    CONSTRAINT [UQ_Warehouse_Bins_Site_BinCode] UNIQUE ([WarehouseSiteID], [BinCode]),
    CONSTRAINT [CK_Warehouse_Bins_Type] CHECK ([BinType] IN (N'PICK', N'BULK', N'STAGE', N'RETURNS', N'DAMAGE')),
    CONSTRAINT [CK_Warehouse_Bins_Status] CHECK ([BinStatus] IN (N'AVAILABLE', N'BLOCKED', N'COUNTING', N'RETIRED')),
    CONSTRAINT [FK_Warehouse_Bins_Sites] FOREIGN KEY ([WarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Warehouse_Bins_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Warehouse_Bins_PickPath]
    ON [Warehouse].[Bins] ([WarehouseSiteID] ASC, [PickSequence] ASC)
    INCLUDE ([BinCode], [ZoneCode], [BinType])
    WHERE [BinStatus] = N'AVAILABLE';
GO
