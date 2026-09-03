/*
    Shipping.FreightRates

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1320 - after Shipping.Carriers
    Depends on    : Shipping.Carriers, Application.People
    Called by     : Shipping.ufn_FreightCost, Shipping.usp_RateShipment

    Rate card rows. Rating is by weight break within a zone, with a volumetric
    divisor applied when the dimensional weight exceeds the actual weight. The
    divisor differs by region (139 for the NA cards inherited in pounds, 5000
    for the EU and APAC cards in kilograms) and that difference is the single
    most common cause of freight disputes in this estate.
*/
CREATE TABLE [Shipping].[FreightRates] (
    [FreightRateID]         INT             IDENTITY (1, 1) NOT NULL,
    [CarrierID]             INT             NOT NULL,
    [ServiceLevelCode]      NVARCHAR (12)   NOT NULL,
    [OriginZoneCode]        NVARCHAR (10)   NOT NULL,
    [DestinationZoneCode]   NVARCHAR (10)   NOT NULL,
    [WeightFromKg]          DECIMAL (9, 3)  NOT NULL,
    [WeightToKg]            DECIMAL (9, 3)  NOT NULL,
    [BaseCharge]            DECIMAL (18, 2) NOT NULL,
    [PerKgCharge]           DECIMAL (18, 4) CONSTRAINT [DF_Shipping_FreightRates_PerKgCharge] DEFAULT (0) NOT NULL,
    [MinimumCharge]         DECIMAL (18, 2) NULL,
    [VolumetricDivisor]     INT             NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [ResidentialSurcharge]  DECIMAL (18, 2) CONSTRAINT [DF_Shipping_FreightRates_ResidentialSurcharge] DEFAULT (0) NOT NULL,
    [EffectiveFromDate]     DATE            NOT NULL,
    [EffectiveToDate]       DATE            NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Shipping_FreightRates_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Shipping_FreightRates] PRIMARY KEY CLUSTERED ([FreightRateID] ASC),
    CONSTRAINT [UQ_Shipping_FreightRates_Band] UNIQUE ([CarrierID], [ServiceLevelCode], [OriginZoneCode], [DestinationZoneCode], [WeightFromKg], [EffectiveFromDate]),
    CONSTRAINT [CK_Shipping_FreightRates_Weights] CHECK ([WeightToKg] > [WeightFromKg]),
    CONSTRAINT [CK_Shipping_FreightRates_Charges] CHECK ([BaseCharge] >= 0 AND [PerKgCharge] >= 0),
    CONSTRAINT [FK_Shipping_FreightRates_Carriers] FOREIGN KEY ([CarrierID]) REFERENCES [Shipping].[Carriers] ([CarrierID]),
    CONSTRAINT [FK_Shipping_FreightRates_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Shipping_FreightRates_Lookup]
    ON [Shipping].[FreightRates] ([CarrierID] ASC, [ServiceLevelCode] ASC, [DestinationZoneCode] ASC, [WeightFromKg] ASC)
    INCLUDE ([BaseCharge], [PerKgCharge], [MinimumCharge], [VolumetricDivisor], [EffectiveFromDate], [EffectiveToDate]);
GO
