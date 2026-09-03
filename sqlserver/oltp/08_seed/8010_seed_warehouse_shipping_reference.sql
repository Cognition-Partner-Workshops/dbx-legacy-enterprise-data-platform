/*
    08_seed / 8010 - Warehouse and shipping reference data

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : after 8000_seed_sales_reference.sql
    Depends on    : Warehouse.WarehouseSites, Shipping.Carriers,
                    Shipping.PackagingTypes, Application.People
    Called by     : Warehouse.usp_PostStockMovement,
                    Warehouse.usp_TransferStockBetweenSites,
                    Shipping.usp_CreateShipmentFromOrder, Shipping.usp_RateShipment

    Grouped reference/seed script, guarded by NOT EXISTS on the natural key.

    Regional divergence: NA sites are imperial and Fahrenheit because the WMS
    was never converted, EU and APAC sites are metric and Celsius. Carrier
    service levels are held as a delimited list (legacy pattern) rather than a
    child table - the 2004 interface expected a pipe-separated string and the
    downstream rating code still splits it.
*/
INSERT INTO [Warehouse].[WarehouseSites]
(
    [SiteCode], [SiteName], [RegionCode], [SiteType], [UnitOfMeasureSystem],
    [TemperatureScale], [TimeZoneIdentifier], [OperatingHoursText], [IsBonded],
    [DefaultCarrierCode], [CycleCountPolicyCode], [IsActive], [LastEditedBy]
)
SELECT * FROM (VALUES
    (N'US-SEA-01', N'Seattle main distribution centre',   N'NA  ', N'DC',         N'IMPERIAL', N'F', N'Pacific Standard Time',   N'06:00-22:00 Mon-Sat', 0, N'UPSNA',   N'ABC',    1, 1),
    (N'US-DAL-01', N'Dallas forward stocking location',   N'NA  ', N'FORWARD',    N'IMPERIAL', N'F', N'Central Standard Time',   N'07:00-19:00 Mon-Fri', 0, N'FDXNA',   N'RANDOM', 1, 1),
    (N'US-NJ-RTN', N'Edison returns processing centre',   N'NA  ', N'RETURNS',    N'IMPERIAL', N'F', N'Eastern Standard Time',   N'08:00-17:00 Mon-Fri', 0, N'USPSNA',  N'ZONE',   1, 1),
    (N'CA-TOR-01', N'Toronto distribution centre',        N'NA  ', N'DC',         N'METRIC',   N'C', N'Eastern Standard Time',   N'07:00-20:00 Mon-Fri', 1, N'PURCA',   N'ABC',    1, 1),
    (N'GB-MAN-01', N'Manchester distribution centre',     N'EU  ', N'DC',         N'METRIC',   N'C', N'GMT Standard Time',       N'05:00-23:00 Mon-Sat', 0, N'DPDEU',   N'ABC',    1, 1),
    (N'DE-HAM-01', N'Hamburg bonded warehouse',           N'EU  ', N'DC',         N'METRIC',   N'C', N'W. Europe Standard Time', N'06:00-22:00 Mon-Sat', 1, N'DHLEU',   N'ZONE',   1, 1),
    (N'NL-RTM-XD', N'Rotterdam cross dock',               N'EU  ', N'CROSSDOCK',  N'METRIC',   N'C', N'W. Europe Standard Time', N'00:00-24:00 Mon-Sun', 1, N'DHLEU',   N'NONE',   1, 1),
    (N'AU-SYD-01', N'Sydney distribution centre',         N'APAC', N'DC',         N'METRIC',   N'C', N'AUS Eastern Standard Time', N'06:00-18:00 Mon-Fri', 0, N'TOLLAP', N'ABC',   1, 1),
    (N'SG-SIN-3P', N'Singapore third party warehouse',    N'APAC', N'THIRDPARTY', N'METRIC',   N'C', N'Singapore Standard Time', N'08:00-20:00 Mon-Sat', 1, N'SFAP',    N'RANDOM', 1, 1),
    (N'JP-OSA-01', N'Osaka forward stocking location',    N'APAC', N'FORWARD',    N'METRIC',   N'C', N'Tokyo Standard Time',     N'09:00-18:00 Mon-Fri', 0, N'YAMAP',   N'ZONE',   1, 1)
) AS s ([SiteCode], [SiteName], [RegionCode], [SiteType], [UnitOfMeasureSystem],
        [TemperatureScale], [TimeZoneIdentifier], [OperatingHoursText], [IsBonded],
        [DefaultCarrierCode], [CycleCountPolicyCode], [IsActive], [LastEditedBy])
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Warehouse].[WarehouseSites] AS w
    WHERE w.[SiteCode] = s.[SiteCode]
);
GO

/* Legacy pattern: ServiceLevelList is a pipe-delimited list of service codes.
   Shipping.usp_RateShipment still parses it with CHARINDEX rather than joining. */
INSERT INTO [Shipping].[Carriers]
(
    [CarrierCode], [CarrierName], [RegionCode], [ScacCode], [ServiceLevelList],
    [TrackingUrlTemplate], [AccountReference], [SupportsCustoms], [SupportsChiller],
    [FuelSurchargePercent], [OnTimeTargetPercent], [ContractFromDate], [ContractToDate],
    [CarrierStatus], [LastEditedBy]
)
SELECT * FROM (VALUES
    (N'UPSNA',  N'United Parcel Service - NA contract', N'NA  ', N'UPSN', N'GND|2DAY|NDA|NDAAM',       N'https://track.example.com/ups?n={0}',  N'ACCT-NA-UPS',  1, 0, 12.50, 96.00, CONVERT(DATE, '2016-01-01'), NULL, N'ACTIVE', 1),
    (N'FDXNA',  N'FedEx - NA contract',                 N'NA  ', N'FDEN', N'GND|EXP2|EXP1|FRT',        N'https://track.example.com/fdx?n={0}',  N'ACCT-NA-FDX',  1, 1, 13.25, 95.00, CONVERT(DATE, '2014-04-01'), NULL, N'ACTIVE', 1),
    (N'USPSNA', N'US Postal Service - returns only',    N'NA  ', N'USPS', N'PRIORITY|FIRST',           N'https://track.example.com/usps?n={0}', N'ACCT-NA-RTN',  0, 0,  0.00, 88.00, CONVERT(DATE, '2010-01-01'), NULL, N'ACTIVE', 1),
    (N'PURCA',  N'Purolator - Canada',                  N'NA  ', N'PURO', N'GND|EXP',                  N'https://track.example.com/puro?n={0}', N'ACCT-CA-PUR',  1, 0, 11.00, 94.00, CONVERT(DATE, '2013-06-01'), NULL, N'ACTIVE', 1),
    (N'DPDEU',  N'DPD - UK and Ireland',                N'EU  ', N'DPDX', N'CLASSIC|NEXTDAY|PREDICT',  N'https://track.example.com/dpd?n={0}',  N'ACCT-EU-DPD',  0, 0,  9.75, 97.00, CONVERT(DATE, '2015-01-01'), NULL, N'ACTIVE', 1),
    (N'DHLEU',  N'DHL Freight - continental Europe',    N'EU  ', N'DHLE', N'ECONOMY|EURAPID|CHILLED',  N'https://track.example.com/dhl?n={0}',  N'ACCT-EU-DHL',  1, 1, 10.40, 96.50, CONVERT(DATE, '2012-01-01'), NULL, N'ACTIVE', 1),
    (N'GLSEU',  N'GLS - legacy parcel contract',        N'EU  ', N'GLSX', N'BUSINESS|EXPRESS',         N'https://track.example.com/gls?n={0}',  N'ACCT-EU-GLS',  0, 0,  8.90, 92.00, CONVERT(DATE, '2008-01-01'), CONVERT(DATE, '2019-12-31'), N'TERMINATED', 1),
    (N'TOLLAP', N'Toll - Australia domestic',           N'APAC', N'TOLL', N'ROAD|AIR|SAMEDAY',         N'https://track.example.com/toll?n={0}', N'ACCT-AP-TOL',  0, 1, 14.00, 91.00, CONVERT(DATE, '2017-07-01'), NULL, N'ACTIVE', 1),
    (N'SFAP',   N'SF Express - south east Asia',        N'APAC', N'SFEX', N'STANDARD|EXPRESS',         N'https://track.example.com/sf?n={0}',   N'ACCT-AP-SFX',  1, 0, 15.50, 89.00, CONVERT(DATE, '2019-07-01'), NULL, N'ACTIVE', 1),
    (N'YAMAP',  N'Yamato - Japan domestic',             N'APAC', N'YAMA', N'TAKKYUBIN|COOL',           N'https://track.example.com/yam?n={0}',  N'ACCT-AP-YAM',  0, 1, 12.00, 98.00, CONVERT(DATE, '2018-04-01'), NULL, N'ACTIVE', 1),
    (N'GLOBFF', N'Global freight forwarder',            N'GLOB', N'GFFX', N'SEALCL|SEAFCL|AIRFRT',     N'https://track.example.com/gff?n={0}',  N'ACCT-GL-FFW',  1, 0,  6.50, 80.00, CONVERT(DATE, '2011-01-01'), NULL, N'ACTIVE', 1)
) AS s ([CarrierCode], [CarrierName], [RegionCode], [ScacCode], [ServiceLevelList],
        [TrackingUrlTemplate], [AccountReference], [SupportsCustoms], [SupportsChiller],
        [FuelSurchargePercent], [OnTimeTargetPercent], [ContractFromDate], [ContractToDate],
        [CarrierStatus], [LastEditedBy])
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Shipping].[Carriers] AS c
    WHERE c.[CarrierCode] = s.[CarrierCode]
);
GO

INSERT INTO [Shipping].[PackagingTypes]
(
    [PackagingCode], [PackagingName], [PackagingClass], [LengthCm], [WidthCm], [HeightCm],
    [TareWeightKg], [MaximumPayloadKg], [IsReturnable], [IsChillerRated], [IsActive], [LastEditedBy]
)
SELECT * FROM (VALUES
    (N'CTNSM',  N'Small carton',                 N'CARTON',   30.00,  22.00,  15.00,  0.180,   8.000, 0, 0, 1, 1),
    (N'CTNMD',  N'Medium carton',                N'CARTON',   45.00,  35.00,  25.00,  0.360,  18.000, 0, 0, 1, 1),
    (N'CTNLG',  N'Large carton',                 N'CARTON',   60.00,  45.00,  40.00,  0.640,  30.000, 0, 0, 1, 1),
    (N'CTNCHL', N'Insulated chiller carton',     N'CARTON',   45.00,  35.00,  30.00,  1.250,  15.000, 0, 1, 1, 1),
    (N'TOTERT', N'Returnable tote - EU stores',  N'TOTE',     60.00,  40.00,  32.00,  2.100,  25.000, 1, 0, 1, 1),
    (N'PLTEUR', N'Euro pallet 1200x800',         N'PALLET',  120.00,  80.00, 150.00, 25.000, 900.000, 1, 0, 1, 1),
    (N'PLTUS',  N'US pallet 48x40 inch',         N'PALLET',  121.92, 101.60, 152.40, 22.700, 1000.000, 1, 0, 1, 1),
    (N'PLTAP',  N'APAC pallet 1100x1100',        N'PALLET',  110.00, 110.00, 150.00, 20.000, 850.000, 1, 0, 1, 1),
    (N'ENVDOC', N'Document envelope',            N'ENVELOPE', 33.00,  24.00,   1.00,  0.030,   0.500, 0, 0, 1, 1),
    (N'CRTEXP', N'Export crate - freight only',  N'CRATE',   150.00, 110.00, 120.00, 48.000, 1200.000, 0, 0, 1, 1)
) AS s ([PackagingCode], [PackagingName], [PackagingClass], [LengthCm], [WidthCm], [HeightCm],
        [TareWeightKg], [MaximumPayloadKg], [IsReturnable], [IsChillerRated], [IsActive], [LastEditedBy])
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Shipping].[PackagingTypes] AS p
    WHERE p.[PackagingCode] = s.[PackagingCode]
);
GO
