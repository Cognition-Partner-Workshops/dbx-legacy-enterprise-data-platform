/*
    Customer aggregate tables

    Objects       : [Aggregate].[Customer 360]
                    [Aggregate].[Customer Rolling 12 Month]
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.00 Schema and the customer-facing facts.
    Called by     : both refreshed by
                    Integration.usp_RefreshAggregateCustomer360; the rolling
                    table is rebuilt first because the 360 row reads from it.

    Customer 360 is a one-row-per-customer current-state table (the "golden
    customer record" project of 2017 that never got past reporting). The
    rolling table keeps 12 monthly buckets per customer so trend and churn
    scoring do not have to touch the transaction facts.

    Retention divergence lands here: EU customers whose retention period has
    expired are anonymised in place by the refresh (name and contact columns
    nulled, [Anonymised Flag] set) while the numeric history is kept; APAC
    customers are anonymised on a shorter clock; NA customers are retained
    indefinitely unless they opted out.
*/
CREATE TABLE [Aggregate].[Customer 360] (
    [Customer 360 Key]              BIGINT          IDENTITY (1, 1) NOT NULL,
    [Customer Key]                  INT             NOT NULL,
    [Customer Segment Key]          INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Loyalty Tier Key]              INT             NULL,
    [Primary Sales Channel Key]     INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Customer Name]                 NVARCHAR (100)  NULL,
    [Primary Contact Email]         NVARCHAR (256)  NULL,
    [Account Manager Employee Key]  INT             NULL,
    [First Order Date]              DATE            NULL,
    [Last Order Date]               DATE            NULL,
    [Last Payment Date]             DATE            NULL,
    [Tenure Months]                 INT             NULL,
    [Lifetime Order Count]          INT             NULL,
    [Lifetime Net Revenue]          DECIMAL (18, 2) NULL,
    [Lifetime Gross Margin]         DECIMAL (18, 2) NULL,
    [Lifetime Returns Amount]       DECIMAL (18, 2) NULL,
    [Average Order Value]           DECIMAL (18, 2) NULL,
    [Average Days To Pay]           DECIMAL (9, 2)  NULL,
    [Current Balance Reporting]     DECIMAL (18, 2) NULL,
    [Overdue Balance Reporting]     DECIMAL (18, 2) NULL,
    [Credit Limit Reporting]        DECIMAL (18, 2) NULL,
    [Credit Utilisation Percent]    DECIMAL (9, 4)  NULL,
    [Loyalty Point Balance]         DECIMAL (18, 2) NULL,
    [Web Session Count 90 Day]      INT             NULL,
    [Days Since Last Order]         INT             NULL,
    [Churn Risk Score]              DECIMAL (9, 4)  NULL,
    [Churn Risk Band]               NVARCHAR (10)   NULL,
    [Rfm Score]                     NVARCHAR (6)    NULL,
    [Marketing Consent Flag]        BIT             NULL,
    [Retention Expiry Date]         DATE            NULL,
    [Anonymised Flag]               BIT             CONSTRAINT [DF_Aggregate_Customer_360_Anonymised_Flag] DEFAULT (0) NOT NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Aggregate_Customer_360] PRIMARY KEY NONCLUSTERED ([Customer 360 Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Customer_360_Customer]
    ON [Aggregate].[Customer 360] ([Customer Key] ASC);
GO

CREATE NONCLUSTERED INDEX [IX_Aggregate_Customer_360_Churn]
    ON [Aggregate].[Customer 360] ([Churn Risk Band] ASC, [Region Code] ASC)
    INCLUDE ([Lifetime Net Revenue], [Days Since Last Order], [Churn Risk Score]);
GO

CREATE TABLE [Aggregate].[Customer Rolling 12 Month] (
    [Customer Rolling Key]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [Customer Key]                  INT             NOT NULL,
    [Month Offset]                  TINYINT         NOT NULL,
    [Calendar Month]                DATE            NOT NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Order Count]                   INT             NULL,
    [Net Revenue Reporting]         DECIMAL (18, 2) NULL,
    [Gross Margin Reporting]        DECIMAL (18, 2) NULL,
    [Returns Reporting]             DECIMAL (18, 2) NULL,
    [Cash Received Reporting]       DECIMAL (18, 2) NULL,
    [Distinct Product Count]        INT             NULL,
    [Loyalty Points Earned]         DECIMAL (18, 2) NULL,
    [Loyalty Points Redeemed]       DECIMAL (18, 2) NULL,
    [Web Session Count]             INT             NULL,
    [Rolling 12 Month Revenue]      DECIMAL (18, 2) NULL,
    [Rolling 12 Month Margin]       DECIMAL (18, 2) NULL,
    [Rolling 3 Month Revenue]       DECIMAL (18, 2) NULL,
    [Revenue Trend Percent]         DECIMAL (9, 4)  NULL,
    [Inactive Month Flag]           BIT             NULL,
    [Consecutive Inactive Months]   TINYINT         NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Aggregate_Customer_Rolling_12_Month] PRIMARY KEY NONCLUSTERED ([Customer Rolling Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Customer_Rolling_12_Month_Grain]
    ON [Aggregate].[Customer Rolling 12 Month] ([Customer Key] ASC, [Calendar Month] ASC);
GO
