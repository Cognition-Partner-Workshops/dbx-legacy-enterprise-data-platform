/*
    Object        : [Integration].[usp_InsertInferredMember]
    Deploy target : WideWorldImportersDW
    Depends on    : Integration.DimensionKeyRegistry, Integration.InferredMemberQueue,
                    Integration.usp_AllocateDimensionKeyRange,
                    Dimension.Customer, Dimension.Supplier, Dimension.Stock Item,
                    Dimension.City, Dimension.Geography, Dimension.Promotion
    Called by     : the fact loads in sqlserver/procedures/facts

    Late-arriving dimension handling, the insert half.

    A fact load that cannot resolve a business key calls this, gets a surrogate
    key back and carries on. The stub row it creates is marked
    [Is Inferred Member] = 1 and carries only the business key; the next run of
    the owning dimension load fills it in *in place*, so the key the fact row is
    already carrying never changes.

    Only the dimensions flagged [Supports Inferred] in the registry accept a stub.
    Everything else gets -1 back and the fact load points at Unknown. That flag is
    data, not code, because in 2019 the finance team wanted stubs turned off for
    Geography for a quarter and nobody wanted to redeploy for it.

    The per-dimension insert is written out by hand rather than generated: the
    NOT NULL surface of each dimension is different and the 2011 attempt at a
    generic version produced stubs that failed the fact-load foreign keys.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_InsertInferredMember]
    @DimensionName      NVARCHAR(100),
    @BusinessKey        NVARCHAR(100),
    @RegionCode         NVARCHAR(10)  = N'GLOBAL',
    @SourceSystemCode   NVARCHAR(20)  = NULL,
    @RequestedByPackage NVARCHAR(200) = NULL,
    @BatchId            BIGINT        = NULL,
    @SurrogateKey       INT           OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @SupportsInferred BIT;
    DECLARE @FirstKey         INT;
    DECLARE @LastKey          INT;
    DECLARE @Now              DATETIME2(7) = SYSDATETIME();
    DECLARE @HighDate         DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');

    SET @SurrogateKey = -1;

    SELECT @SupportsInferred = r.[Supports Inferred]
    FROM [Integration].[DimensionKeyRegistry] AS r
    WHERE r.[Dimension Name] = @DimensionName;

    IF ISNULL(@SupportsInferred, 0) = 0
        RETURN;     /* dimension does not take stubs; caller uses Unknown */

    /* Already stubbed by an earlier fact load in the same night. */
    SELECT @SurrogateKey = q.[Surrogate Key]
    FROM [Integration].[InferredMemberQueue] AS q
    WHERE q.[Dimension Name]    = @DimensionName
      AND q.[Business Key]      = @BusinessKey
      AND q.[Enrichment Status] = N'Pending';

    IF @SurrogateKey > 0
        RETURN;

    EXEC [Integration].[usp_AllocateDimensionKeyRange]
         @DimensionName  = @DimensionName,
         @RequestedCount = 1,
         @FirstKey       = @FirstKey OUTPUT,
         @LastKey        = @LastKey  OUTPUT,
         @RequestedBy    = @RequestedByPackage;

    IF @FirstKey IS NULL
    BEGIN
        SET @SurrogateKey = -1;
        RETURN;
    END;

    SET @SurrogateKey = @FirstKey;

    IF @DimensionName = N'Dimension.Customer'
    BEGIN
        INSERT INTO [Dimension].[Customer]
            ([Customer Key], [WWI Customer ID], [Customer], [Bill To Customer], [Category],
             [Buying Group], [Primary Contact], [Postal Code], [Region Code],
             [Is Current Row], [Version Number], [Effective From], [Effective To],
             [Is Inferred Member], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id])
        VALUES
            (@SurrogateKey, TRY_CONVERT(INT, @BusinessKey),
             CONCAT(N'Inferred customer ', @BusinessKey),
             CONCAT(N'Inferred customer ', @BusinessKey), N'Unknown', N'Unknown',
             N'Unknown', N'N/A', @RegionCode, 1, 1, @Now, @HighDate, 1,
             @Now, @HighDate, 0, @BatchId);
    END
    ELSE IF @DimensionName = N'Dimension.Supplier'
    BEGIN
        INSERT INTO [Dimension].[Supplier]
            ([Supplier Key], [WWI Supplier ID], [Supplier], [Category], [Primary Contact],
             [Supplier Reference], [Payment Days], [Postal Code], [Region Code],
             [Is Current Row], [Version Number], [Effective From], [Effective To],
             [Is Inferred Member], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id])
        VALUES
            (@SurrogateKey, TRY_CONVERT(INT, @BusinessKey),
             CONCAT(N'Inferred supplier ', @BusinessKey), N'Unknown', N'Unknown',
             @BusinessKey, 0, N'N/A', @RegionCode, 1, 1, @Now, @HighDate, 1,
             @Now, @HighDate, 0, @BatchId);
    END
    ELSE IF @DimensionName = N'Dimension.Stock Item'
    BEGIN
        INSERT INTO [Dimension].[Stock Item]
            ([Stock Item Key], [WWI Stock Item ID], [Stock Item], [Color], [Selling Package],
             [Buying Package], [Brand], [Size], [Lead Time Days], [Quantity Per Outer],
             [Is Chiller Stock], [Tax Rate], [Unit Price], [Typical Weight Per Unit],
             [Listing Region Code], [Is Current Row], [Version Number], [Effective From],
             [Effective To], [Is Inferred Member], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id])
        VALUES
            (@SurrogateKey, TRY_CONVERT(INT, @BusinessKey),
             CONCAT(N'Inferred stock item ', @BusinessKey), N'Unknown', N'Unknown',
             N'Unknown', N'Unknown', N'Unknown', 0, 0, 0, 0, 0, 0,
             @RegionCode, 1, 1, @Now, @HighDate, 1, @Now, @HighDate, 0, @BatchId);
    END
    ELSE IF @DimensionName = N'Dimension.City'
    BEGIN
        INSERT INTO [Dimension].[City]
            ([City Key], [WWI City ID], [City], [State Province], [Country], [Continent],
             [Sales Territory], [Region], [Subregion], [Latest Recorded Population],
             [Region Code], [Is Current Row], [Version Number], [Effective From],
             [Effective To], [Is Inferred Member], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id])
        VALUES
            (@SurrogateKey, TRY_CONVERT(INT, @BusinessKey),
             CONCAT(N'Inferred city ', @BusinessKey), N'Unknown', N'Unknown', N'Unknown',
             N'Unknown', N'Unknown', N'Unknown', 0,
             @RegionCode, 1, 1, @Now, @HighDate, 1, @Now, @HighDate, 0, @BatchId);
    END
    ELSE IF @DimensionName = N'Dimension.Geography'
    BEGIN
        /* Geography has no inferred-member flag of its own - it predates the
           2011 stub work - so an inferred subdivision is recognisable only by
           its 'Unknown' subdivision name and its queue row. */
        INSERT INTO [Dimension].[Geography]
            ([Geography Key], [Country Code], [Country Name], [Subdivision Code],
             [Subdivision Name], [Region Code], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id])
        VALUES
            (@SurrogateKey, LEFT(@BusinessKey, 3), N'Unknown',
             RIGHT(@BusinessKey, CASE WHEN LEN(@BusinessKey) > 4
                                      THEN LEN(@BusinessKey) - 4 ELSE LEN(@BusinessKey) END),
             N'Unknown', @RegionCode, @Now, @HighDate, 0, @BatchId);
    END
    ELSE IF @DimensionName = N'Dimension.Promotion'
    BEGIN
        INSERT INTO [Dimension].[Promotion]
            ([Promotion Key], [Promotion Code], [Promotion Name], [Region Code],
             [Effective From], [Effective To], [Is Current Row], [Version Number],
             [Is Inferred Member], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id])
        VALUES
            (@SurrogateKey, @BusinessKey, CONCAT(N'Inferred promotion ', @BusinessKey),
             @RegionCode, @Now, @HighDate, 1, 1, 1, @Now, @HighDate, 0, @BatchId);
    END
    ELSE
    BEGIN
        /* Registered as supporting inferred members but no stub shape is written
           for it. Give the caller Unknown rather than an orphaned key. */
        SET @SurrogateKey = -1;
        RETURN;
    END;

    INSERT INTO [Integration].[InferredMemberQueue]
        ([Dimension Name], [Business Key], [Surrogate Key], [Source System Code],
         [Region Code], [Requested By Package], [Requested Batch Id], [Requested On],
         [Enrichment Status], [Enrichment Attempts])
    VALUES
        (@DimensionName, @BusinessKey, @SurrogateKey, @SourceSystemCode, @RegionCode,
         @RequestedByPackage, @BatchId, @Now, N'Pending', 0);
END;
GO
