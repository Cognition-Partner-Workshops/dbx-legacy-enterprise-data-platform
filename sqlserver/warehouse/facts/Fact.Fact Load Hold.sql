/*
    Fact.Fact Load Hold

    Object        : [Fact].[Fact Load Hold] - the hold-and-retry pen for fact
                    rows that arrived before a dimension member existed and
                    could not be keyed even by inferred-member creation.
    Deploy target : WideWorldImportersDW
    Deploy order  : after the Fact schema exists; before any fact load runs.
    Called by     : written by every Integration.usp_LoadFact* procedure;
                    drained by Integration.usp_RekeyLateArrivingDimensions.

    Not a fact table despite living in the Fact schema - it was put here in 2010
    so the retry sweep did not need cross-database permissions, and it never
    moved. Rows carry the source payload as XML because each fact has a
    different shape and nobody wanted one hold table per fact.

    Retry policy: the sweep retries a row on every run until
    [Retry Count] reaches [Max Retry Count], after which the row is marked
    'ABANDON' and routed to etl.RejectedRecord by the sweep, not by the
    original load.
*/
CREATE TABLE [Fact].[Fact Load Hold] (
    [Fact Load Hold Key]            BIGINT          IDENTITY (1, 1) NOT NULL,
    [Target Fact Name]              NVARCHAR (128)  NOT NULL,
    [Source System Code]            NVARCHAR (10)   NOT NULL,
    [Region Code]                   NVARCHAR (4)    NULL,
    [Natural Key Hash]              BINARY (32)     NOT NULL,
    [Natural Key Text]              NVARCHAR (400)  NOT NULL,
    [Business Date]                 DATE            NULL,
    [Missing Dimension Name]        NVARCHAR (128)  NOT NULL,
    [Missing Business Key]          NVARCHAR (100)  NOT NULL,
    [Hold Reason Code]              NVARCHAR (10)   NOT NULL,
    [Source Payload]                XML             NULL,
    [Retry Count]                   INT             CONSTRAINT [DF_Fact_Fact_Load_Hold_Retry_Count] DEFAULT (0) NOT NULL,
    [Max Retry Count]               INT             CONSTRAINT [DF_Fact_Fact_Load_Hold_Max_Retry_Count] DEFAULT (14) NOT NULL,
    [Hold Status Code]              NVARCHAR (10)   CONSTRAINT [DF_Fact_Fact_Load_Hold_Hold_Status_Code] DEFAULT (N'HELD') NOT NULL,
    [First Held Datetime]           DATETIME2 (3)   CONSTRAINT [DF_Fact_Fact_Load_Hold_First_Held_Datetime] DEFAULT (SYSDATETIME()) NOT NULL,
    [Last Retry Datetime]           DATETIME2 (3)   NULL,
    [Released Datetime]             DATETIME2 (3)   NULL,
    [Released Fact Key]             BIGINT          NULL,
    [Original Batch Id]             BIGINT          NULL,
    [Last Batch Id]                 BIGINT          NULL,
    [Package Execution Id]          BIGINT          NULL,
    CONSTRAINT [PK_Fact_Fact_Load_Hold] PRIMARY KEY CLUSTERED ([Fact Load Hold Key] ASC)
);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Fact_Load_Hold_Sweep]
    ON [Fact].[Fact Load Hold] ([Hold Status Code] ASC, [Missing Dimension Name] ASC, [Retry Count] ASC)
    INCLUDE ([Target Fact Name], [Missing Business Key], [Natural Key Hash]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Fact_Load_Hold_Natural_Key]
    ON [Fact].[Fact Load Hold] ([Target Fact Name] ASC, [Natural Key Hash] ASC);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Hold-and-retry pen for unkeyable fact rows awaiting late-arriving dimensions',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Fact Load Hold';
GO
