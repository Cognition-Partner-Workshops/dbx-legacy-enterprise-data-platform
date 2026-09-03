/*
    Integration.ChangeTrackingWatermark

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1720 - after 00_schemas
    Depends on    : none
    Called by     : Integration.usp_GetChangeWatermark, Integration.usp_SetChangeWatermark,
                    extract packages

    Per-consumer, per-table high-water marks for incremental extraction. This
    is the OLTP-side mirror of etl.Watermark in the control database: the
    extracts read from here because they run with a login that cannot see the
    control database, and the two are reconciled by hand when they disagree.

    Both a datetime and a binary mark are kept because some tables are chased
    on LastEditedWhen and the temporal ones on their rowversion. Whichever is
    not used for a given table is left null.
*/
CREATE TABLE [Integration].[ChangeTrackingWatermark] (
    [ChangeTrackingWatermarkID] INT         IDENTITY (1, 1) NOT NULL,
    [ConsumerCode]          NVARCHAR (30)   NOT NULL,
    [SourceSchemaName]      NVARCHAR (30)   NOT NULL,
    [SourceTableName]       NVARCHAR (60)   NOT NULL,
    [WatermarkColumnName]   NVARCHAR (60)   NOT NULL,
    [LastExtractedWhen]     DATETIME2 (7)   NULL,
    [LastExtractedVersion]  BINARY (8)      NULL,
    [LastExtractedKeyValue] NVARCHAR (60)   NULL,
    [OverlapMinutes]        SMALLINT        CONSTRAINT [DF_Integration_ChangeTrackingWatermark_OverlapMinutes] DEFAULT (15) NOT NULL,
    [FullReloadRequested]   BIT             CONSTRAINT [DF_Integration_ChangeTrackingWatermark_FullReloadRequested] DEFAULT (0) NOT NULL,
    [LastRunBatchID]        BIGINT          NULL,
    [LastRowCount]          BIGINT          NULL,
    [LastUpdatedWhen]       DATETIME2 (7)   CONSTRAINT [DF_Integration_ChangeTrackingWatermark_LastUpdatedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [UpdatedByProcess]      NVARCHAR (60)   NULL,
    CONSTRAINT [PK_Integration_ChangeTrackingWatermark] PRIMARY KEY CLUSTERED ([ChangeTrackingWatermarkID] ASC),
    CONSTRAINT [UQ_Integration_ChangeTrackingWatermark_Consumer] UNIQUE ([ConsumerCode], [SourceSchemaName], [SourceTableName]),
    CONSTRAINT [CK_Integration_ChangeTrackingWatermark_Overlap] CHECK ([OverlapMinutes] >= 0 AND [OverlapMinutes] <= 1440)
);
GO
