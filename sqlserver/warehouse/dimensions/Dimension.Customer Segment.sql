/*
    Object        : [Dimension].[Customer Segment]  (SCD Type 2 - segment migration is reportable)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.CustomerSegmentKey
    Called by     : Integration.usp_MigrateStagedCustomerSegmentData

    Behavioural segmentation produced by the monthly marketing model in
    Sales.CustomerSegments. It is Type 2 because "how many customers moved from
    Growth to At Risk this quarter" is the whole point of the dimension, and a
    Type 1 overwrite would erase the movement.

    The scoring model differs by region and has never been harmonised:
      NA    : RFM deciles, recomputed monthly, 8 segments.
      EU    : CLV bands with an explicit consent gate - a customer who has not
              given profiling consent is scored into 'NOPROFILE' and is excluded
              from the model entirely.
      APAC  : channel-weighted RFM with a separate marketplace segment set,
              recomputed fortnightly, 6 segments plus 2 marketplace-only segments.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Customer Segment', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Customer Segment]
    (
        [Customer Segment Key]          INT             CONSTRAINT [DF_Dimension_Customer_Segment_Key] DEFAULT (NEXT VALUE FOR [Sequences].[CustomerSegmentKey]) NOT NULL,
        [WWI Customer Segment ID]       INT             NULL,
        [Segment Code]                  NVARCHAR(15)    NOT NULL,
        [Customer Segment]              NVARCHAR(60)    NOT NULL,
        [Segment Family Code]           NVARCHAR(10)    NULL,   -- VALUE / LIFECYCLE / CHANNEL / RISK
        [Region Code]                   NVARCHAR(10)    NOT NULL,
        [Scoring Model Code]            NVARCHAR(20)    NULL,   -- RFM_NA_V4 / CLV_EU_V2 / RFMC_APAC_V3
        [Scoring Model Version]         NVARCHAR(10)    NULL,
        [Scoring Frequency Code]        NVARCHAR(10)    NULL,   -- MONTHLY / FORTNIGHTLY
        [Last Scored On]                DATE            NULL,

        [Recency Band]                  NVARCHAR(10)    NULL,
        [Frequency Band]                NVARCHAR(10)    NULL,
        [Monetary Band]                 NVARCHAR(10)    NULL,
        [Lifetime Value Band]           NVARCHAR(10)    NULL,
        [Churn Risk Band]               NVARCHAR(10)    NULL,
        [Minimum Score]                 DECIMAL(9, 4)   NULL,
        [Maximum Score]                 DECIMAL(9, 4)   NULL,
        [Target Contact Frequency]      SMALLINT        NULL,

        [Requires Profiling Consent]    BIT             NULL,   -- set for every EU segment
        [Excluded From Modelling]       BIT             NULL,   -- the EU 'NOPROFILE' bucket
        [Marketplace Only]              BIT             NULL,   -- APAC marketplace segments

        [Source System Code]            NVARCHAR(20)    NULL,
        [Effective From]                DATETIME2(7)    NOT NULL,
        [Effective To]                  DATETIME2(7)    NOT NULL,
        [Effective From Date]           DATE            NULL,
        [Effective Sequence]            SMALLINT        NULL,
        [Is Current Row]                BIT             NOT NULL,
        [Version Number]                INT             NULL,
        [Row Hash Type 2]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Customer_Segment] PRIMARY KEY CLUSTERED ([Customer Segment Key] ASC),
        CONSTRAINT [CK_Dimension_Customer_Segment_Region]
            CHECK ([Region Code] IN (N'NA', N'EU', N'APAC', N'GLOBAL'))
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Customer_Segment_Current]
        ON [Dimension].[Customer Segment] ([Segment Code] ASC, [Region Code] ASC, [Is Current Row] ASC);
END;
GO
