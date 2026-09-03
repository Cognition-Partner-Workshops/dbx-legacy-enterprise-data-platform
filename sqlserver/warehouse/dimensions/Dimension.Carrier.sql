/*
    Object        : [Dimension].[Carrier]  (SCD Type 1)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Geography.sql
    Depends on    : Sequences.CarrierKey
    Called by     : Integration.usp_MigrateStagedCarrierData

    Shipping carriers and their service levels, one row per (carrier, service).
    Tracking-number formats are stored as a pattern because the carrier-scan file
    feed (ING_FILE_CarrierScan) arrives with no carrier column and the ingestion
    has to infer the carrier from the shape of the tracking number - a genuinely
    fragile 2010 arrangement that is still in place.

    Regional divergence is in the service catalogue itself: NA carriers quote in
    zones by ZIP prefix, EU carriers quote by country band with a customs
    surcharge post-2021, APAC carriers quote per lane with a fuel and a security
    surcharge and several are marketplace-nominated rather than chosen by us.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Carrier', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Carrier]
    (
        [Carrier Key]                   INT             CONSTRAINT [DF_Dimension_Carrier_Key] DEFAULT (NEXT VALUE FOR [Sequences].[CarrierKey]) NOT NULL,
        [WWI Carrier ID]                INT             NULL,
        [Carrier Code]                  NVARCHAR(15)    NOT NULL,
        [Carrier Name]                  NVARCHAR(80)    NOT NULL,
        [Service Code]                  NVARCHAR(15)    NOT NULL,
        [Service Name]                  NVARCHAR(60)    NULL,
        [Service Level Code]            NVARCHAR(10)    NULL,   -- SAMEDAY / NEXTDAY / EXPRESS / STD / ECON / FREIGHT
        [Mode Code]                     NVARCHAR(10)    NULL,   -- ROAD / AIR / SEA / RAIL / PARCEL
        [Region Code]                   NVARCHAR(10)    NULL,
        [Coverage Country List]         NVARCHAR(400)   NULL,
        [Is International]              BIT             NULL,
        [Is Marketplace Nominated]      BIT             NULL,

        [Transit Days Minimum]          SMALLINT        NULL,
        [Transit Days Maximum]          SMALLINT        NULL,
        [Cutoff Local Time]             TIME(0)         NULL,
        [On Time Target Percentage]     DECIMAL(9, 4)   NULL,
        [Rating Basis Code]             NVARCHAR(10)    NULL,   -- ZONE (NA) / BAND (EU) / LANE (APAC)
        [Fuel Surcharge Applies]        BIT             NULL,
        [Security Surcharge Applies]    BIT             NULL,
        [Customs Surcharge Applies]     BIT             NULL,
        [Residential Surcharge Applies] BIT             NULL,
        [Dimensional Weight Divisor]    INT             NULL,
        [Maximum Parcel Weight Kg]      DECIMAL(9, 3)   NULL,
        [Handles Hazardous]             BIT             NULL,
        [Handles Chilled]               BIT             NULL,

        [Tracking Number Pattern]       NVARCHAR(80)    NULL,   -- used to infer the carrier from the scan file
        [Tracking URL Template]         NVARCHAR(200)   NULL,
        [Scan Event Code Set]           NVARCHAR(20)    NULL,   -- each carrier emits its own event vocabulary
        [EDI Partner Identifier]        NVARCHAR(30)    NULL,
        [Account Reference]             NVARCHAR(30)    NULL,   -- our account number with the carrier, not a credential

        [Is Active]                     BIT             NULL,
        [Contract End Date]             DATE            NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Carrier] PRIMARY KEY CLUSTERED ([Carrier Key] ASC),
        CONSTRAINT [UQ_Dimension_Carrier_Service] UNIQUE ([Carrier Code], [Service Code])
    );
END;
GO
