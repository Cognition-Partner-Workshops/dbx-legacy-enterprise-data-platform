/*
    Returns.ReturnInspections

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1430 - after Returns.ReturnLines
    Depends on    : Returns.ReturnLines, Warehouse.WarehouseSites, Application.People
    Called by     : Returns.usp_PostReturnInspection

    Goods-in inspection outcome, one row per inspection attempt; a line can be
    inspected twice when the first outcome is challenged by the customer.
    MeasurementText is free text captured from the handheld and is
    deliberately unparsed - the QA team paste readings, photo references and
    entire email fragments into it.
*/
CREATE TABLE [Returns].[ReturnInspections] (
    [ReturnInspectionID]    BIGINT          IDENTITY (1, 1) NOT NULL,
    [ReturnLineID]          BIGINT          NOT NULL,
    [InspectionSequence]    SMALLINT        CONSTRAINT [DF_Returns_ReturnInspections_InspectionSequence] DEFAULT (1) NOT NULL,
    [InspectedWhen]         DATETIME2 (7)   CONSTRAINT [DF_Returns_ReturnInspections_InspectedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [InspectedByPersonID]   INT             NOT NULL,
    [WarehouseSiteID]       INT             NOT NULL,
    [ConditionGrade]        NCHAR (1)       NOT NULL,
    [QuantityInspected]     DECIMAL (18, 3) NOT NULL,
    [QuantityPassed]        DECIMAL (18, 3) NOT NULL,
    [QuantityFailed]        AS ([QuantityInspected] - [QuantityPassed]) PERSISTED,
    [FaultCode]             NVARCHAR (10)   NULL,
    [DispositionCode]       NVARCHAR (12)   NOT NULL,
    [MeasurementText]       NVARCHAR (MAX)  NULL,
    [PhotoEvidenceRef]      NVARCHAR (120)  NULL,
    [SupplierClaimRef]      NVARCHAR (40)   NULL,
    [IsChallenged]          BIT             CONSTRAINT [DF_Returns_ReturnInspections_IsChallenged] DEFAULT (0) NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Returns_ReturnInspections_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Returns_ReturnInspections] PRIMARY KEY CLUSTERED ([ReturnInspectionID] ASC),
    CONSTRAINT [UQ_Returns_ReturnInspections_Sequence] UNIQUE ([ReturnLineID], [InspectionSequence]),
    CONSTRAINT [CK_Returns_ReturnInspections_Grade] CHECK ([ConditionGrade] IN (N'A', N'B', N'C', N'D', N'X')),
    CONSTRAINT [CK_Returns_ReturnInspections_Quantities] CHECK ([QuantityPassed] >= 0 AND [QuantityPassed] <= [QuantityInspected]),
    CONSTRAINT [CK_Returns_ReturnInspections_Disposition] CHECK ([DispositionCode] IN (N'RESTOCK', N'REWORK', N'SCRAP', N'SUPPLIER', N'QUARANTINE')),
    CONSTRAINT [FK_Returns_ReturnInspections_Lines] FOREIGN KEY ([ReturnLineID]) REFERENCES [Returns].[ReturnLines] ([ReturnLineID]),
    CONSTRAINT [FK_Returns_ReturnInspections_Sites] FOREIGN KEY ([WarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Returns_ReturnInspections_Application_People] FOREIGN KEY ([InspectedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Returns_ReturnInspections_Failures]
    ON [Returns].[ReturnInspections] ([InspectedWhen] ASC)
    INCLUDE ([ReturnLineID], [FaultCode], [ConditionGrade])
    WHERE [FaultCode] IS NOT NULL;
GO
