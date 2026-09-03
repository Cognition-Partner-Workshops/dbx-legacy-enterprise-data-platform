/*
    Shipping.Carriers

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1300 - after 00_schemas
    Depends on    : Application.People
    Called by     : Shipping.FreightRates, Shipping.ShipmentHeaders,
                    Shipping.ufn_FreightCost

    Carrier master. Service levels are held as a pipe-delimited list on the
    carrier rather than in a child table (a second delimited-list legacy
    pattern, inherited from the 2004 freight interface which sent them as one
    fixed-width field). The rate table then re-states the service level as its
    own column, so the two can and do disagree.

    Tracking URLs carry a {0} placeholder that the despatch note formatter
    substitutes; they are not credentials and carry no query authentication.
*/
CREATE TABLE [Shipping].[Carriers] (
    [CarrierID]             INT             IDENTITY (1, 1) NOT NULL,
    [CarrierCode]           NVARCHAR (12)   NOT NULL,
    [CarrierName]           NVARCHAR (80)   NOT NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [ScacCode]              NCHAR (4)       NULL,
    [ServiceLevelList]      NVARCHAR (200)  NULL,
    [TrackingUrlTemplate]   NVARCHAR (300)  NULL,
    [AccountReference]      NVARCHAR (40)   NULL,
    [SupportsCustoms]       BIT             CONSTRAINT [DF_Shipping_Carriers_SupportsCustoms] DEFAULT (0) NOT NULL,
    [SupportsChiller]       BIT             CONSTRAINT [DF_Shipping_Carriers_SupportsChiller] DEFAULT (0) NOT NULL,
    [FuelSurchargePercent]  DECIMAL (5, 2)  CONSTRAINT [DF_Shipping_Carriers_FuelSurchargePercent] DEFAULT (0) NOT NULL,
    [OnTimeTargetPercent]   DECIMAL (5, 2)  NULL,
    [ContractFromDate]      DATE            NULL,
    [ContractToDate]        DATE            NULL,
    [CarrierStatus]         NVARCHAR (12)   CONSTRAINT [DF_Shipping_Carriers_CarrierStatus] DEFAULT (N'ACTIVE') NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Shipping_Carriers_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Shipping_Carriers] PRIMARY KEY CLUSTERED ([CarrierID] ASC),
    CONSTRAINT [UQ_Shipping_Carriers_Code] UNIQUE ([CarrierCode]),
    CONSTRAINT [CK_Shipping_Carriers_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC', N'GLOB')),
    CONSTRAINT [CK_Shipping_Carriers_Status] CHECK ([CarrierStatus] IN (N'ACTIVE', N'SUSPENDED', N'TERMINATED')),
    CONSTRAINT [CK_Shipping_Carriers_Contract] CHECK ([ContractToDate] IS NULL OR [ContractFromDate] IS NULL OR [ContractToDate] >= [ContractFromDate]),
    CONSTRAINT [FK_Shipping_Carriers_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
