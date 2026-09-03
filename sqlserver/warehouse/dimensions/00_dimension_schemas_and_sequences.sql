/*
    Object        : Dimension / Integration schema objects and dimension key sequences
    Deploy target : WideWorldImportersDW
    Deploy order  : 1 (first file in sqlserver/warehouse/dimensions)
    Depends on    : the WideWorldImportersDW baseline (Dimension, Integration,
                    Sequences schemas shipped with the Microsoft sample)
    Called by     : deployment only; nothing calls this at run time

    Twenty years of accretion left this estate with three different ways of
    allocating a surrogate key, and all three are still in use:

      1. SEQUENCE objects  - the WideWorldImportersDW original (Sequences.CustomerKey
                             and friends). New dimensions added after 2011 follow it.
      2. IDENTITY          - a handful of dimensions added by the 2007 finance
                             project, kept because the reports hard-code key ranges.
      3. Integration.DimensionKeyRegistry - a hand-rolled "next key" table added in
                             2014 when the nightly load started allocating key blocks
                             for late-arriving members. See 01_dimension_key_registry.sql.

    Nothing here drops or redefines a sequence that the Microsoft sample already
    creates; the guards below make the script re-runnable against a database that
    already has the baseline.
*/
SET NOCOUNT ON;
GO

IF SCHEMA_ID(N'Dimension') IS NULL
    EXEC (N'CREATE SCHEMA [Dimension] AUTHORIZATION dbo;');
GO

IF SCHEMA_ID(N'Integration') IS NULL
    EXEC (N'CREATE SCHEMA [Integration] AUTHORIZATION dbo;');
GO

IF SCHEMA_ID(N'Sequences') IS NULL
    EXEC (N'CREATE SCHEMA [Sequences] AUTHORIZATION dbo;');
GO

/*
    Reserved key ranges. Every dimension in this estate obeys them, and the fact
    loads in sqlserver/procedures/facts rely on them, so they may not be changed
    without a full warehouse reload.

        -1  Unknown              source value present but unresolvable
        -2  Not Applicable       the business rule says no member can apply
        -3  Invalid              source value present and rejected by a DQ screen
        -4  Inferred Pending     stub row created by a fact load, not yet enriched
        -9  Error                lookup itself failed (kept out of reporting)
         0  Not Yet Assigned     used only inside work tables, never in a fact
         1+ Real members         allocated by sequence, identity or key registry

    Sequences therefore start at 1 and never produce a non-positive key.
*/
DECLARE @SequenceName SYSNAME;
DECLARE @Sql NVARCHAR(MAX);

DECLARE DimensionSequenceCursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT SequenceName
    FROM (VALUES
        (N'BuyingGroupKey'),
        (N'CarrierKey'),
        (N'CostCenterKey'),
        (N'CountryKey'),
        (N'CurrencyKey'),
        (N'CustomerCategoryKey'),
        (N'CustomerDemographicKey'),
        (N'CustomerSegmentKey'),
        (N'GeographyKey'),
        (N'GlAccountKey'),
        (N'LoyaltyTierKey'),
        (N'OrderStatusJunkKey'),
        (N'PaymentTermsKey'),
        (N'ProductCategoryKey'),
        (N'ProductHierarchyKey'),
        (N'PromotionKey'),
        (N'RegionKey'),
        (N'ReturnReasonKey'),
        (N'SalesChannelKey'),
        (N'SalespersonKey'),
        (N'SalesTerritoryKey'),
        (N'SupplierCategoryKey'),
        (N'VendorContractKey'),
        (N'WarehouseSiteKey')
    ) AS s (SequenceName);

OPEN DimensionSequenceCursor;
FETCH NEXT FROM DimensionSequenceCursor INTO @SequenceName;

-- Row-by-row on purpose: CREATE SEQUENCE is not allowed in a set-based statement
-- and the 2014 deployment script this was lifted from looked exactly like this.
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(N'Sequences.' + QUOTENAME(@SequenceName), N'SO') IS NULL
    BEGIN
        SET @Sql = N'CREATE SEQUENCE [Sequences].' + QUOTENAME(@SequenceName)
                 + N' AS INT START WITH 1 INCREMENT BY 1 NO CYCLE CACHE 100;';
        EXEC sys.sp_executesql @Sql;
    END;

    FETCH NEXT FROM DimensionSequenceCursor INTO @SequenceName;
END;

CLOSE DimensionSequenceCursor;
DEALLOCATE DimensionSequenceCursor;
GO
