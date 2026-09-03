/*
    08_seed / 8000 - Sales reference data

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : after 01_tables and 02_extensions
    Depends on    : Sales.SalesChannels, Sales.SalesTerritories,
                    Sales.CustomerSegments, Sales.CommissionPlans,
                    Application.People
    Called by     : Sales.usp_CalculateOrderDiscounts,
                    Sales.usp_AssignCustomerSegments,
                    Sales.usp_RecalculateCommissionAccruals

    Grouped reference/seed script. Idempotent: every insert is guarded by a
    NOT EXISTS on the natural key so the script can be re-run after a partial
    deployment, which is how the DBAs have always run it.

    Regional divergence lives in the data, not in the code: NA is sales-tax /
    USD / 4-4-5 fiscal, EU is VAT / EUR / calendar fiscal with opt-in consent,
    APAC is GST / mixed currency / July fiscal year.
*/
/* Sales channels. The DIGITAL rows were added during the 2011 web project and
   reuse PartnerIdentifier for the storefront code - there is no separate column. */
INSERT INTO [Sales].[SalesChannels]
(
    [ChannelCode], [ChannelName], [ChannelClass], [RegionCode], [ChannelStatus],
    [PartnerIdentifier], [DefaultPriceListCode], [CommissionModifierPercent],
    [RequiresManualApproval], [OrderPrefix], [ValidFromDate], [LastEditedBy]
)
SELECT * FROM (VALUES
    (N'FIELD',    N'Field sales - North America',      N'DIRECT',     N'NA  ', N'ACTIVE',    NULL,            N'NA-STD-USD',  0.00, 0, N'NAO',  CONVERT(DATE, '2003-01-01'), 1),
    (N'TELESALE', N'Call centre - North America',      N'CALLCENTRE', N'NA  ', N'ACTIVE',    NULL,            N'NA-STD-USD', -0.50, 0, N'NAC',  CONVERT(DATE, '2004-07-01'), 1),
    (N'EDINA',    N'EDI 850 inbound - North America',  N'EDI',        N'NA  ', N'ACTIVE',    N'AS2-WWI-NA',   N'NA-EDI-USD', -1.00, 0, N'NAE',  CONVERT(DATE, '2006-03-01'), 1),
    (N'WEBNA',    N'Storefront - North America',       N'DIGITAL',    N'NA  ', N'ACTIVE',    N'SF-NA-01',     N'NA-WEB-USD', -1.50, 0, N'NAW',  CONVERT(DATE, '2011-11-01'), 1),
    (N'FIELDEU',  N'Field sales - Europe',             N'DIRECT',     N'EU  ', N'ACTIVE',    NULL,            N'EU-STD-EUR',  0.00, 0, N'EUO',  CONVERT(DATE, '2005-01-01'), 1),
    (N'EDIEU',    N'EDIFACT ORDERS inbound - Europe',  N'EDI',        N'EU  ', N'ACTIVE',    N'OFTP2-WWI-EU', N'EU-EDI-EUR', -1.00, 1, N'EUE',  CONVERT(DATE, '2007-09-01'), 1),
    (N'WEBEU',    N'Storefront - Europe',              N'DIGITAL',    N'EU  ', N'ACTIVE',    N'SF-EU-01',     N'EU-WEB-EUR', -1.50, 0, N'EUW',  CONVERT(DATE, '2012-04-01'), 1),
    (N'DISTEU',   N'Distributor programme - Europe',   N'PARTNER',    N'EU  ', N'SUSPENDED', N'DIST-EU-2009', N'EU-DIS-EUR', -2.50, 1, N'EUD',  CONVERT(DATE, '2009-02-01'), 1),
    (N'FIELDAP',  N'Field sales - Asia Pacific',       N'DIRECT',     N'APAC', N'ACTIVE',    NULL,            N'AP-STD-AUD',  0.00, 0, N'APO',  CONVERT(DATE, '2008-07-01'), 1),
    (N'MARKETAP', N'Marketplace resellers - APAC',     N'PARTNER',    N'APAC', N'PILOT',     N'MKT-AP-2019',  N'AP-MKT-AUD', -3.00, 1, N'APM',  CONVERT(DATE, '2019-07-01'), 1),
    (N'WEBAP',    N'Storefront - Asia Pacific',        N'DIGITAL',    N'APAC', N'PILOT',     N'SF-AP-01',     N'AP-WEB-AUD', -1.50, 0, N'APW',  CONVERT(DATE, '2018-10-01'), 1),
    (N'FAXAP',    N'Fax and phone order desk - APAC',  N'CALLCENTRE', N'APAC', N'CLOSED',    NULL,            N'AP-STD-AUD',  0.00, 1, N'APF',  CONVERT(DATE, '2008-07-01'), 1)
) AS s ([ChannelCode], [ChannelName], [ChannelClass], [RegionCode], [ChannelStatus],
        [PartnerIdentifier], [DefaultPriceListCode], [CommissionModifierPercent],
        [RequiresManualApproval], [OrderPrefix], [ValidFromDate], [LastEditedBy])
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Sales].[SalesChannels] AS c
    WHERE c.[ChannelCode] = s.[ChannelCode]
        AND c.[RegionCode] = s.[RegionCode]
);
GO

/* Territory hierarchy: level 1 region, level 2 country/district. The tax
   regime, fiscal calendar and postal standard are all carried as data. */
INSERT INTO [Sales].[SalesTerritories]
(
    [TerritoryCode], [TerritoryName], [ParentTerritoryID], [TerritoryLevel], [RegionCode],
    [CountryISO3], [TaxRegimeCode], [FiscalCalendarCode], [ReportingCurrencyCode],
    [PostalStandardCode], [IsActive], [LastEditedBy]
)
SELECT * FROM (VALUES
    (N'NA',        N'North America',            NULL, 1, N'NA  ', NULL,  N'USSALESTAX', N'NA445',   N'USD', N'USPS',       1, 1),
    (N'EU',        N'Europe',                   NULL, 1, N'EU  ', NULL,  N'EUVAT',      N'EUCAL',   N'EUR', N'EUDIN5008',  1, 1),
    (N'APAC',      N'Asia Pacific',             NULL, 1, N'APAC', NULL,  N'AUGST',      N'APACJUN', N'AUD', N'AUSPOST',    1, 1)
) AS s ([TerritoryCode], [TerritoryName], [ParentTerritoryID], [TerritoryLevel], [RegionCode],
        [CountryISO3], [TaxRegimeCode], [FiscalCalendarCode], [ReportingCurrencyCode],
        [PostalStandardCode], [IsActive], [LastEditedBy])
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Sales].[SalesTerritories] AS t
    WHERE t.[TerritoryCode] = s.[TerritoryCode]
);
GO

INSERT INTO [Sales].[SalesTerritories]
(
    [TerritoryCode], [TerritoryName], [ParentTerritoryID], [TerritoryLevel], [RegionCode],
    [CountryISO3], [TaxRegimeCode], [FiscalCalendarCode], [ReportingCurrencyCode],
    [PostalStandardCode], [IsActive], [LastEditedBy]
)
SELECT
    s.[TerritoryCode], s.[TerritoryName], p.[SalesTerritoryID], s.[TerritoryLevel], s.[RegionCode],
    s.[CountryISO3], s.[TaxRegimeCode], s.[FiscalCalendarCode], s.[ReportingCurrencyCode],
    s.[PostalStandardCode], s.[IsActive], s.[LastEditedBy]
FROM (VALUES
    (N'NA-US-EAST', N'United States East',   N'NA',   2, N'NA  ', N'USA', N'USSALESTAX', N'NA445',   N'USD', N'USPS',       1, 1),
    (N'NA-US-WEST', N'United States West',   N'NA',   2, N'NA  ', N'USA', N'USSALESTAX', N'NA445',   N'USD', N'USPS',       1, 1),
    (N'NA-CA',      N'Canada',               N'NA',   2, N'NA  ', N'CAN', N'CAGSTHST',   N'NA445',   N'CAD', N'CANADAPOST', 1, 1),
    (N'EU-UK',      N'United Kingdom',       N'EU',   2, N'EU  ', N'GBR', N'UKVAT',      N'EUCAL',   N'GBP', N'ROYALMAIL',  1, 1),
    (N'EU-DE',      N'Germany',              N'EU',   2, N'EU  ', N'DEU', N'EUVAT',      N'EUCAL',   N'EUR', N'EUDIN5008',  1, 1),
    (N'EU-NL',      N'Netherlands',          N'EU',   2, N'EU  ', N'NLD', N'EUVAT',      N'EUCAL',   N'EUR', N'EUDIN5008',  1, 1),
    (N'AP-AU',      N'Australia',            N'APAC', 2, N'APAC', N'AUS', N'AUGST',      N'APACJUN', N'AUD', N'AUSPOST',    1, 1),
    (N'AP-SG',      N'Singapore',            N'APAC', 2, N'APAC', N'SGP', N'SGGST',      N'APACJUN', N'SGD', N'AUSPOST',    1, 1),
    (N'AP-JP',      N'Japan',                N'APAC', 2, N'APAC', N'JPN', N'JPCT',       N'APACJUN', N'JPY', N'JPPOST',     1, 1)
) AS s ([TerritoryCode], [TerritoryName], [ParentCode], [TerritoryLevel], [RegionCode],
        [CountryISO3], [TaxRegimeCode], [FiscalCalendarCode], [ReportingCurrencyCode],
        [PostalStandardCode], [IsActive], [LastEditedBy])
    INNER JOIN [Sales].[SalesTerritories] AS p
        ON p.[TerritoryCode] = s.[ParentCode]
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Sales].[SalesTerritories] AS t
    WHERE t.[TerritoryCode] = s.[TerritoryCode]
);
GO

/* Customer segments. EU rows carry consent and a shorter retention because of
   the 2018 privacy programme; NA and APAC kept the original ten-year retention. */
INSERT INTO [Sales].[CustomerSegments]
(
    [SegmentCode], [SegmentName], [SegmentFamily], [RegionCode], [SegmentRuleText],
    [ConsentRequired], [ConsentBasisCode], [RetentionMonths], [PriorityOrder],
    [IsExclusive], [IsActive], [LastEditedBy]
)
SELECT * FROM (VALUES
    (N'PLATINUM',  N'Platinum accounts',            N'VALUE',     N'NA  ', N'Rolling 12 month invoiced revenue >= 250000 USD',       0, NULL,          120, 10,  1, 1, 1),
    (N'GOLD',      N'Gold accounts',                N'VALUE',     N'NA  ', N'Rolling 12 month invoiced revenue >= 75000 USD',        0, NULL,          120, 20,  1, 1, 1),
    (N'LAPSING',   N'No order in 180 days',         N'LIFECYCLE', N'NA  ', N'DATEDIFF(day, last order date, today) > 180',           0, NULL,           60, 40,  0, 1, 1),
    (N'CREDITWCH', N'Credit watch list',            N'RISK',      N'NA  ', N'Overdue balance > 10 percent of credit limit',          0, NULL,           84, 15,  0, 1, 1),
    (N'PLATINUM',  N'Platinum accounts (EU)',       N'VALUE',     N'EU  ', N'Rolling 12 month invoiced revenue >= 200000 EUR',       1, N'CONTRACT',    36, 10,  1, 1, 1),
    (N'MARKETOK',  N'Marketing contactable (EU)',   N'BEHAVIOUR', N'EU  ', N'Explicit marketing opt-in recorded and not withdrawn',  1, N'CONSENT',     24, 30,  0, 1, 1),
    (N'LAPSING',   N'No order in 90 days (EU)',     N'LIFECYCLE', N'EU  ', N'DATEDIFF(day, last order date, today) > 90',            1, N'LEGITIMATE',  24, 40,  0, 1, 1),
    (N'PLATINUM',  N'Platinum accounts (APAC)',     N'VALUE',     N'APAC', N'Rolling 12 month invoiced revenue >= 300000 AUD',       0, NULL,          120, 10,  1, 1, 1),
    (N'WEBFIRST',  N'Web-first buyers (APAC)',      N'CHANNEL',   N'APAC', N'More than half of orders raised through WEBAP',         0, NULL,           60, 35,  0, 1, 1),
    (N'DISTRIB',   N'Distributor accounts (APAC)',  N'CHANNEL',   N'APAC', N'Customer category is distributor or marketplace',       0, NULL,          120, 25,  1, 1, 1)
) AS s ([SegmentCode], [SegmentName], [SegmentFamily], [RegionCode], [SegmentRuleText],
        [ConsentRequired], [ConsentBasisCode], [RetentionMonths], [PriorityOrder],
        [IsExclusive], [IsActive], [LastEditedBy])
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Sales].[CustomerSegments] AS cs
    WHERE cs.[SegmentCode] = s.[SegmentCode]
        AND cs.[RegionCode] = s.[RegionCode]
);
GO

/* Commission plans. NA pays on invoiced margin with an accelerator, EU pays on
   net revenue with a long clawback window, APAC only pays on collected cash. */
INSERT INTO [Sales].[CommissionPlans]
(
    [PlanCode], [PlanName], [RegionCode], [CommissionBasis],
    [Band1UpperPercent], [Band1RatePercent], [Band2UpperPercent], [Band2RatePercent],
    [Band3UpperPercent], [Band3RatePercent], [AcceleratorPercent], [ClawbackWindowDays],
    [MinimumMarginPercent], [EffectiveFromDate], [EffectiveToDate], [LastEditedBy]
)
SELECT * FROM (VALUES
    (N'NA-FIELD-2019',  N'NA field sales plan (2019 revision)',  N'NA  ', N'INVOICEDMARGIN',  80.00, 1.50, 100.00, 2.50, 130.00, 3.50, 1.00,  90, 12.00, CONVERT(DATE, '2019-01-01'), NULL, 1),
    (N'NA-TELE-2019',   N'NA call centre plan',                  N'NA  ', N'NETREVENUE',     100.00, 0.75, 140.00, 1.10, NULL,   NULL, NULL,  60,  8.00, CONVERT(DATE, '2019-01-01'), NULL, 1),
    (N'NA-FIELD-2012',  N'NA field sales plan (superseded)',     N'NA  ', N'INVOICEDMARGIN',  90.00, 1.25, 120.00, 2.00, NULL,   NULL, NULL,  30, 10.00, CONVERT(DATE, '2012-01-01'), CONVERT(DATE, '2018-12-31'), 1),
    (N'EU-FIELD-2016',  N'EU field sales plan',                  N'EU  ', N'NETREVENUE',      85.00, 1.20, 105.00, 2.00, 125.00, 2.60, NULL, 180, NULL,  CONVERT(DATE, '2016-01-01'), NULL, 1),
    (N'EU-DIST-2016',   N'EU distributor management plan',       N'EU  ', N'NETREVENUE',     100.00, 0.60, NULL,   NULL, NULL,   NULL, NULL, 180, NULL,  CONVERT(DATE, '2016-01-01'), NULL, 1),
    (N'AP-FIELD-2020',  N'APAC field sales plan',                N'APAC', N'COLLECTEDCASH',   90.00, 1.00, 115.00, 1.80, NULL,   NULL, 0.50, 120,  9.00, CONVERT(DATE, '2020-07-01'), NULL, 1),
    (N'AP-MARKET-2020', N'APAC marketplace partner plan',        N'APAC', N'COLLECTEDCASH',  100.00, 0.40, NULL,   NULL, NULL,   NULL, NULL, 120, NULL,  CONVERT(DATE, '2020-07-01'), NULL, 1)
) AS s ([PlanCode], [PlanName], [RegionCode], [CommissionBasis],
        [Band1UpperPercent], [Band1RatePercent], [Band2UpperPercent], [Band2RatePercent],
        [Band3UpperPercent], [Band3RatePercent], [AcceleratorPercent], [ClawbackWindowDays],
        [MinimumMarginPercent], [EffectiveFromDate], [EffectiveToDate], [LastEditedBy])
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Sales].[CommissionPlans] AS cp
    WHERE cp.[PlanCode] = s.[PlanCode]
);
GO
