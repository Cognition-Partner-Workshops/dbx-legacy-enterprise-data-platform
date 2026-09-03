/*
    Shipping.PackagingTypes

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1310
    Depends on    : Application.People
    Called by     : Shipping.ShipmentLines, Shipping.ufn_FreightCost

    Outer packaging used on despatch, distinct from the sample's
    Warehouse.PackageTypes which describes the selling unit. Dimensions are
    metric here even for the NA sites, because the carrier rate cards were
    normalised to centimetres in 2013 while the warehouse floor was not; the
    despatch screen converts on display.
*/
CREATE TABLE [Shipping].[PackagingTypes] (
    [PackagingTypeID]       INT             IDENTITY (1, 1) NOT NULL,
    [PackagingCode]         NVARCHAR (12)   NOT NULL,
    [PackagingName]         NVARCHAR (60)   NOT NULL,
    [PackagingClass]        NVARCHAR (12)   NOT NULL,
    [LengthCm]              DECIMAL (9, 2)  NULL,
    [WidthCm]               DECIMAL (9, 2)  NULL,
    [HeightCm]              DECIMAL (9, 2)  NULL,
    [VolumeM3]              AS (CASE WHEN [LengthCm] IS NULL OR [WidthCm] IS NULL OR [HeightCm] IS NULL THEN NULL
                                     ELSE CONVERT(DECIMAL (12, 6), [LengthCm] * [WidthCm] * [HeightCm] / 1000000.0) END) PERSISTED,
    [TareWeightKg]          DECIMAL (9, 3)  NULL,
    [MaximumPayloadKg]      DECIMAL (9, 3)  NULL,
    [IsReturnable]          BIT             CONSTRAINT [DF_Shipping_PackagingTypes_IsReturnable] DEFAULT (0) NOT NULL,
    [IsChillerRated]        BIT             CONSTRAINT [DF_Shipping_PackagingTypes_IsChillerRated] DEFAULT (0) NOT NULL,
    [IsActive]              BIT             CONSTRAINT [DF_Shipping_PackagingTypes_IsActive] DEFAULT (1) NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Shipping_PackagingTypes_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Shipping_PackagingTypes] PRIMARY KEY CLUSTERED ([PackagingTypeID] ASC),
    CONSTRAINT [UQ_Shipping_PackagingTypes_Code] UNIQUE ([PackagingCode]),
    CONSTRAINT [CK_Shipping_PackagingTypes_Class] CHECK ([PackagingClass] IN (N'CARTON', N'PALLET', N'TOTE', N'ENVELOPE', N'CRATE')),
    CONSTRAINT [FK_Shipping_PackagingTypes_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
