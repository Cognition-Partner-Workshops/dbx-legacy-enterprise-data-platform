/*
    stg.vw_CustomerReadyForDimension

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Read by       : the DIM_Customer warehouse load

    Only survivor rows that passed data quality are exposed, and the EU consent
    rules are applied here rather than in the warehouse so that every consumer
    inherits them: an EU customer without marketing consent, or one past its
    retention expiry, is published with its contact-shaped attributes blanked and
    a flag the dimension load uses to skip the marketing attributes entirely.
*/

IF OBJECT_ID(N'stg.vw_CustomerReadyForDimension', N'V') IS NOT NULL
    DROP VIEW stg.vw_CustomerReadyForDimension;
GO

CREATE VIEW stg.vw_CustomerReadyForDimension
AS
SELECT
    c.CustomerBusinessKey,
    c.SourceSystemCode,
    c.SourceCustomerId,
    c.OltpCustomerId,
    c.ErpCustomerNumber,
    c.CustomerName,
    ISNULL(c.CustomerNameStandardized, c.CustomerName)  AS CustomerNameConformed,
    c.CustomerLegalName,
    c.ParentCustomerBusinessKey,
    c.BuyingGroupName,
    c.CustomerCategoryCode,
    c.CustomerStatusCode,
    c.CreditLimitAmountUsd,
    c.CreditRatingCode,
    c.PaymentTermsCode,
    c.StandardTermsNetDays,
    CASE
        WHEN c.RegionCode = N'EU' AND ISNULL(c.MarketingConsentFlag, 0) = 0 THEN NULL
        ELSE c.TaxRegistrationNumber
    END                                                 AS TaxRegistrationNumber,
    c.RegionCode,
    c.PrimaryCountryCode,
    c.SalespersonBusinessKey,
    a.AddressBusinessKey                                AS PrimaryAddressBusinessKey,
    a.CityNameStandardized                              AS PrimaryCityName,
    a.StateProvinceCode                                 AS PrimaryStateProvinceCode,
    a.PostalCodeStandardized                            AS PrimaryPostalCode,
    a.GeographyBusinessKey                              AS PrimaryGeographyBusinessKey,
    CASE
        WHEN c.RegionCode = N'EU' AND ISNULL(c.MarketingConsentFlag, 0) = 0 THEN CONVERT(BIT, 1)
        WHEN c.RetentionExpiryDate IS NOT NULL AND c.RetentionExpiryDate < CONVERT(DATE, SYSUTCDATETIME()) THEN CONVERT(BIT, 1)
        ELSE CONVERT(BIT, 0)
    END                                                 AS SuppressMarketingAttributesFlag,
    c.MarketingConsentFlag,
    c.RetentionClassCode,
    c.RetentionExpiryDate,
    c.AccountOpenedDate,
    c.LastActivityDate,
    c.RowHash,
    c.ChangeHash,
    c.BatchId,
    c.PackageExecutionId,
    c.LoadedAtUtc
FROM stg.Customer AS c
LEFT JOIN stg.CustomerAddress AS a
    ON  a.CustomerBusinessKey = c.CustomerBusinessKey
    AND a.BatchId             = c.BatchId
    AND a.IsPrimaryAddress    = 1
WHERE c.IsSurvivorRow = 1
  AND c.DqStatusCode IN (N'PASS', N'WARN');
GO
