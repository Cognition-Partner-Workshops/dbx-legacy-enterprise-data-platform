/*
    Object        : [Dimension].[Customer Demographic]  (mini dimension for rapidly-changing customer attributes)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Customer.sql
    Depends on    : Sequences.CustomerDemographicKey
    Called by     : Integration.usp_LoadCustomerDemographicMiniDimension

    The classic mini-dimension split. Credit band, spend band, order-frequency band
    and risk score change monthly for a large share of the customer base; leaving
    them on Dimension.Customer produced roughly forty thousand Type 2 rows a month
    and the 2011 rebuild moved them here. Dimension.Customer keeps
    [Customer Demographic Key] pointing at the *current* profile and the facts
    carry their own [Customer Demographic Key] snapshotted at transaction time,
    which is what makes point-in-time demographic analysis possible without
    versioning the customer.

    Every attribute is banded, never continuous - that is what keeps the row count
    bounded. The bands themselves differ by region because the currencies and the
    credit-scoring conventions differ, so the band definitions carry a region and
    the same band label means different things in different regions.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Customer Demographic', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Customer Demographic]
    (
        [Customer Demographic Key]      INT             CONSTRAINT [DF_Dimension_Customer_Demographic_Key] DEFAULT (NEXT VALUE FOR [Sequences].[CustomerDemographicKey]) NOT NULL,
        [Region Code]                   NVARCHAR(10)    NOT NULL,
        [Credit Limit Band]             NVARCHAR(15)    NOT NULL,   -- 0 / 1-5K / 5-25K / 25-100K / 100K+
        [Credit Score Band]             NVARCHAR(15)    NOT NULL,   -- NA: FICO bands, EU: agency grades, APAC: internal 1-5
        [Credit Risk Class]             NVARCHAR(10)    NOT NULL,   -- LOW / MED / HIGH / WATCH / DEFAULT
        [Annual Spend Band]             NVARCHAR(15)    NOT NULL,
        [Order Frequency Band]          NVARCHAR(15)    NOT NULL,   -- WEEKLY / FORTNIGHTLY / MONTHLY / QUARTERLY / RARE
        [Average Order Value Band]      NVARCHAR(15)    NOT NULL,
        [Tenure Band]                   NVARCHAR(15)    NOT NULL,   -- <1Y / 1-3Y / 3-5Y / 5-10Y / 10Y+
        [Recency Band]                  NVARCHAR(15)    NOT NULL,
        [Payment Behaviour Band]        NVARCHAR(15)    NOT NULL,   -- EARLY / ONTIME / LATE30 / LATE60 / DELINQUENT
        [Return Rate Band]              NVARCHAR(15)    NOT NULL,
        [Channel Preference Code]       NVARCHAR(15)    NOT NULL,
        [Loyalty Tier Code]             NVARCHAR(15)    NULL,
        [Marketing Consent Flag]        BIT             NOT NULL,
        [Profiling Consent Flag]        BIT             NOT NULL,

        [Band Definition Version]       NVARCHAR(10)    NULL,   -- band boundaries were re-cut in 2015 and 2020
        [Profile Hash]                  VARBINARY(32)   NULL,
        [First Seen On]                 DATETIME2(7)    NULL,
        [Last Assigned On]              DATETIME2(7)    NULL,
        [Assigned Customer Count]       INT             NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Customer_Demographic] PRIMARY KEY CLUSTERED ([Customer Demographic Key] ASC),
        CONSTRAINT [UQ_Dimension_Customer_Demographic_Profile] UNIQUE ([Profile Hash])
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Customer_Demographic_Bands]
        ON [Dimension].[Customer Demographic] ([Region Code] ASC, [Credit Risk Class] ASC, [Annual Spend Band] ASC);
END;
GO
