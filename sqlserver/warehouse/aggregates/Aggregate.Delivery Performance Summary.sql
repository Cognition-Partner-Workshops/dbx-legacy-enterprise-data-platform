/*
    Aggregate.Delivery Performance Summary

    Object        : [Aggregate].[Delivery Performance Summary] - carrier x
                    warehouse site x week summary of despatch and delivery
                    reliability and freight cost.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.00 Schema, [Fact].[Shipment],
                    [Fact].[Order Fulfilment].
    Called by     : Integration.usp_RefreshAggregateDeliveryPerformance.

    Weekly, not monthly, because the carrier contracts are reviewed weekly and
    the service credits are calculated on an ISO week. The week start date is
    stored as a date rather than a year/week pair after the 2021 year-boundary
    incident where week 53 was reported twice.
*/
CREATE TABLE [Aggregate].[Delivery Performance Summary] (
    [Delivery Performance Key]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [Iso Week Start Date]           DATE            NOT NULL,
    [Iso Week Number]               TINYINT         NULL,
    [Carrier Key]                   INT             NOT NULL,
    [Warehouse Site Key]            INT             NOT NULL,
    [Sales Territory Key]           INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Service Level Code]            NVARCHAR (6)    NULL,
    [Consignment Count]             INT             NULL,
    [Package Count]                 INT             NULL,
    [Delivered Count]               INT             NULL,
    [On Time Count]                 INT             NULL,
    [Late Count]                    INT             NULL,
    [Failed Attempt Count]          INT             NULL,
    [Damaged Count]                 INT             NULL,
    [Lost Count]                    INT             NULL,
    [On Time Percent]               DECIMAL (9, 4)  NULL,
    [First Attempt Success Percent] DECIMAL (9, 4)  NULL,
    [Damage Rate Percent]           DECIMAL (9, 4)  NULL,
    [Average Transit Days]          DECIMAL (9, 2)  NULL,
    [Average Customs Hold Days]     DECIMAL (9, 2)  NULL,
    [Average Pick To Despatch Days] DECIMAL (9, 2)  NULL,
    [Total Weight Kg]               DECIMAL (18, 3) NULL,
    [Chargeable Weight Kg]          DECIMAL (18, 3) NULL,
    [Freight Cost Reporting]        DECIMAL (18, 2) NULL,
    [Fuel Surcharge Reporting]      DECIMAL (18, 2) NULL,
    [Duty And Clearance Reporting]  DECIMAL (18, 2) NULL,
    [Cost Per Consignment]          DECIMAL (18, 4) NULL,
    [Cost Per Chargeable Kg]        DECIMAL (18, 4) NULL,
    [Sla Target Percent]            DECIMAL (9, 4)  NULL,
    [Sla Breach Flag]               BIT             NULL,
    [Service Credit Reporting]      DECIMAL (18, 2) NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Aggregate_Delivery_Performance_Summary] PRIMARY KEY NONCLUSTERED ([Delivery Performance Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Delivery_Performance_Summary_Grain]
    ON [Aggregate].[Delivery Performance Summary] ([Iso Week Start Date] ASC, [Carrier Key] ASC, [Warehouse Site Key] ASC, [Service Level Code] ASC);
GO

CREATE NONCLUSTERED INDEX [IX_Aggregate_Delivery_Performance_Summary_Breach]
    ON [Aggregate].[Delivery Performance Summary] ([Sla Breach Flag] ASC, [Iso Week Start Date] ASC)
    INCLUDE ([Carrier Key], [On Time Percent], [Service Credit Reporting]);
GO
