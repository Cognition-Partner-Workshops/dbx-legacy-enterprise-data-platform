/*
    Fact.Shipment

    Object        : [Fact].[Shipment] - accumulating snapshot fact, one row per
                    despatched consignment, updated in place as the consignment
                    moves through the carrier network.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Carrier, Dimension.Customer, Dimension.City,
                    Dimension.Warehouse Site (WP05).
    Called by     : loaded and updated by Integration.usp_LoadFactShipment.
    Grain         : one consignment (despatch note).

    Accumulating snapshot: the milestone date keys start NULL and are filled in
    by later runs of the load as carrier scan events arrive. Lag measures are
    recomputed on every update, so a row is only final once
    [Delivery Confirmed Date Key] is populated or the consignment is written off
    as lost.

    Regional divergence: NA consignments are LTL/parcel with a signature scan;
    EU consignments cross borders and carry a customs declaration and an
    incoterm that decides who owns the duty; APAC consignments are largely sea
    freight with a vessel/voyage and a long, lumpy transit time, so the
    on-time measure is computed against the promised window rather than a date.
*/
CREATE TABLE [Fact].[Shipment] (
    [Shipment Key]                  BIGINT          IDENTITY (1, 1) NOT NULL,
    [Despatch Date Key]             DATE            NOT NULL,
    [Order Date Key]                DATE            NULL,
    [Picked Date Key]               DATE            NULL,
    [Packed Date Key]               DATE            NULL,
    [Carrier Collection Date Key]   DATE            NULL,
    [Customs Cleared Date Key]      DATE            NULL,
    [First Delivery Attempt Key]    DATE            NULL,
    [Delivery Confirmed Date Key]   DATE            NULL,
    [Promised Delivery Date Key]    DATE            NULL,
    [Customer Key]                  INT             NOT NULL,
    [Carrier Key]                   INT             NOT NULL,
    [Warehouse Site Key]            INT             NOT NULL,
    [City Key]                      INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Despatch Note Number]          NVARCHAR (20)   NOT NULL,
    [Order Number]                  NVARCHAR (20)   NULL,
    [Invoice Number]                NVARCHAR (20)   NULL,
    [Carrier Tracking Number]       NVARCHAR (40)   NULL,
    [Customs Declaration Number]    NVARCHAR (30)   NULL,
    [Incoterm Code]                 NVARCHAR (3)    NULL,
    [Vessel Voyage Reference]       NVARCHAR (30)   NULL,
    [Service Level Code]            NVARCHAR (6)    NULL,
    [Package Count]                 INT             NULL,
    [Total Weight Kg]               DECIMAL (18, 3) NULL,
    [Chargeable Weight Kg]          DECIMAL (18, 3) NULL,
    [Total Volume M3]               DECIMAL (18, 4) NULL,
    [Freight Charge]                DECIMAL (18, 2) NULL,
    [Fuel Surcharge]                DECIMAL (18, 2) NULL,
    [Duty And Clearance Amount]     DECIMAL (18, 2) NULL,
    [Freight Charge Reporting]      DECIMAL (18, 2) NULL,
    [FX Rate To Reporting]          DECIMAL (19, 9) NULL,
    [Pick To Despatch Lag Days]     INT             NULL,
    [Despatch To Delivery Lag Days] INT             NULL,
    [Customs Hold Days]             INT             NULL,
    [Order To Delivery Lag Days]    INT             NULL,
    [Delivery Attempt Count]        INT             NULL,
    [On Time Delivery Flag]         BIT             NULL,
    [Damaged Flag]                  BIT             NULL,
    [Lost In Transit Flag]          BIT             NULL,
    [Shipment Status Code]          NVARCHAR (6)    NULL,
    [Milestone Complete Flag]       BIT             CONSTRAINT [DF_Fact_Shipment_Milestone_Complete_Flag] DEFAULT (0) NOT NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Inferred Member Flag]          BIT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    [Last Milestone Update]         DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Shipment] PRIMARY KEY NONCLUSTERED ([Shipment Key] ASC, [Despatch Date Key] ASC) ON [PS_Date] ([Despatch Date Key]),
    CONSTRAINT [FK_Fact_Shipment_Despatch_Date_Key_Dimension_Date] FOREIGN KEY ([Despatch Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Shipment_Carrier_Key_Dimension_Carrier] FOREIGN KEY ([Carrier Key]) REFERENCES [Dimension].[Carrier] ([Carrier Key]),
    CONSTRAINT [FK_Fact_Shipment_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key])
)
ON [PS_Date] ([Despatch Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Shipment_Natural_Key]
    ON [Fact].[Shipment] ([Despatch Note Number] ASC, [Despatch Date Key] ASC)
    ON [PS_Date] ([Despatch Date Key]);
GO

/* the accumulating snapshot update path scans open consignments every run */
CREATE NONCLUSTERED INDEX [IX_Fact_Shipment_Open_Milestones]
    ON [Fact].[Shipment] ([Milestone Complete Flag] ASC, [Carrier Key] ASC)
    INCLUDE ([Carrier Tracking Number], [Delivery Confirmed Date Key], [Shipment Status Code])
    ON [PS_Date] ([Despatch Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Shipment_Carrier_Performance]
    ON [Fact].[Shipment] ([Carrier Key] ASC, [Despatch Date Key] ASC)
    INCLUDE ([On Time Delivery Flag], [Despatch To Delivery Lag Days], [Freight Charge Reporting])
    ON [PS_Date] ([Despatch Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Consignment accumulating snapshot; milestones filled in as carrier scans arrive',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Shipment';
GO
