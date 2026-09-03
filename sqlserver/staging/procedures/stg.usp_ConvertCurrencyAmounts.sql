/*
    stg.usp_ConvertCurrencyAmounts

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_CONVERT_CURRENCY (SSIS), once per amount column
    Reads         : ref.FxRateDaily, ref.Currency, ref.Region
    Writes        : work.CurrencyConversionScratch, the caller's staging table (dynamic)
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Generic currency conversion. The caller names the staging table, the source
    amount column, the source currency column, the date column and the target
    column; this procedure stages every conversion into
    work.CurrencyConversionScratch (which is what treasury audits) and then
    writes the converted amounts back.

    Rate selection is where the regions diverge and why the fallback is recorded
    per row rather than assumed:
        NA   SPOT on the transaction date, falling back up to @MaxFallbackDays
             calendar days to the most recent prior rate.
        EU   PERIOD_END for anything in a closed period, SPOT otherwise. Treasury
             overrides (IsTreasuryOverride = 1) always win.
        APAC CORPORATE monthly rate: the rate effective on the first of the month
             is used for the whole month, which is why AppliedRateDate is often
             weeks before RequestedRateDate.
    Legacy euro-zone currencies convert through the fixed euro rate rather than
    through a daily rate; the fixed rates live in ref.Currency.EuroFixedRate.
*/

IF OBJECT_ID(N'stg.usp_ConvertCurrencyAmounts', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConvertCurrencyAmounts;
GO

CREATE PROCEDURE stg.usp_ConvertCurrencyAmounts
(
    @BatchId               BIGINT,
    @PackageExecutionId    BIGINT = NULL,
    @TargetSchemaName      NVARCHAR(128),
    @TargetTableName       NVARCHAR(128),
    @BusinessKeyColumnName NVARCHAR(128),
    @AmountColumnName      NVARCHAR(128),
    @CurrencyColumnName    NVARCHAR(128),
    @RateDateColumnName    NVARCHAR(128),
    @ConvertedColumnName   NVARCHAR(128),
    @ToCurrencyCode        NCHAR(3) = N'USD',
    @RegionCode            NVARCHAR(10) = NULL,
    @MaxFallbackDays       SMALLINT = 7
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName  NVARCHAR(200) = CONCAT(@TargetSchemaName, N'.', @TargetTableName);
    DECLARE @Sql         NVARCHAR(MAX);
    DECLARE @ParamList   NVARCHAR(1000);
    DECLARE @StagedRows  BIGINT = 0;
    DECLARE @UpdatedRows BIGINT = 0;
    DECLARE @MissingRows BIGINT = 0;
    DECLARE @RateTypeCode NVARCHAR(20);
    DECLARE @Qualified   NVARCHAR(300);

    BEGIN TRY
        IF OBJECT_ID(QUOTENAME(@TargetSchemaName) + N'.' + QUOTENAME(@TargetTableName), N'U') IS NULL
            THROW 55011, N'stg.usp_ConvertCurrencyAmounts: target table does not exist', 1;

        IF (
                SELECT COUNT(DISTINCT c.name)
                FROM sys.columns AS c
                WHERE c.object_id = OBJECT_ID(QUOTENAME(@TargetSchemaName) + N'.' + QUOTENAME(@TargetTableName))
                  AND c.name IN (@BusinessKeyColumnName, @AmountColumnName, @CurrencyColumnName,
                                 @RateDateColumnName, @ConvertedColumnName)
           ) < 5
            THROW 55012, N'stg.usp_ConvertCurrencyAmounts: one or more named columns do not exist', 1;

        SET @RateTypeCode =
            CASE ISNULL(@RegionCode, N'NA')
                WHEN N'EU'   THEN N'PERIOD_END'
                WHEN N'APAC' THEN N'CORPORATE'
                ELSE N'SPOT'
            END;

        SET @Qualified = QUOTENAME(@TargetSchemaName) + N'.' + QUOTENAME(@TargetTableName);

        SET @ParamList = N'@BatchId BIGINT, @PackageExecutionId BIGINT, @ObjectName NVARCHAR(200), '
                       + N'@AmountColumnName NVARCHAR(128), @ToCurrencyCode NCHAR(3), '
                       + N'@RateTypeCode NVARCHAR(20), @MaxFallbackDays SMALLINT, '
                       + N'@RowsAffected BIGINT OUTPUT';

        SET @Sql = N'
            INSERT INTO work.CurrencyConversionScratch
            (
                BatchId, PackageExecutionId, TargetObjectName, TargetBusinessKey, AmountColumnName,
                FromCurrencyCode, ToCurrencyCode, RateTypeCode, RequestedRateDate, AppliedRateDate,
                ConversionRate, SourceAmount, ConvertedAmount, RateResolutionCode, FallbackDaysUsed
            )
            SELECT
                @BatchId,
                @PackageExecutionId,
                @ObjectName,
                t.' + QUOTENAME(@BusinessKeyColumnName) + N',
                @AmountColumnName,
                t.' + QUOTENAME(@CurrencyColumnName) + N',
                @ToCurrencyCode,
                @RateTypeCode,
                CONVERT(DATE, t.' + QUOTENAME(@RateDateColumnName) + N'),
                r.AppliedRateDate,
                r.ConversionRate,
                t.' + QUOTENAME(@AmountColumnName) + N',
                CONVERT(DECIMAL(19,4), t.' + QUOTENAME(@AmountColumnName) + N' * r.ConversionRate),
                r.RateResolutionCode,
                r.FallbackDaysUsed
            FROM ' + @Qualified + N' AS t
            OUTER APPLY
            (
                SELECT TOP (1)
                    AppliedRateDate    = f.RateDate,
                    ConversionRate     = f.ConversionRate,
                    FallbackDaysUsed   = DATEDIFF(DAY, f.RateDate, CONVERT(DATE, t.' + QUOTENAME(@RateDateColumnName) + N')),
                    RateResolutionCode =
                        CASE
                            WHEN f.IsTreasuryOverride = 1 THEN N''OVERRIDE''
                            WHEN f.RateDate = CONVERT(DATE, t.' + QUOTENAME(@RateDateColumnName) + N') THEN N''EXACT''
                            ELSE N''PRIOR_DAY''
                        END
                FROM ref.FxRateDaily AS f
                WHERE f.FromCurrencyCode = t.' + QUOTENAME(@CurrencyColumnName) + N'
                  AND f.ToCurrencyCode   = @ToCurrencyCode
                  AND f.RateTypeCode     = @RateTypeCode
                  AND f.RateDate        <= CONVERT(DATE, t.' + QUOTENAME(@RateDateColumnName) + N')
                  AND f.RateDate        >= DATEADD(DAY, -@MaxFallbackDays, CONVERT(DATE, t.' + QUOTENAME(@RateDateColumnName) + N'))
                ORDER BY f.IsTreasuryOverride DESC, f.RateDate DESC
            ) AS r
            WHERE t.BatchId = @BatchId
              AND t.' + QUOTENAME(@AmountColumnName) + N' IS NOT NULL
              AND t.' + QUOTENAME(@CurrencyColumnName) + N' <> @ToCurrencyCode;
            SET @RowsAffected = @@ROWCOUNT;';

        EXEC sys.sp_executesql
            @Sql,
            @ParamList,
            @BatchId            = @BatchId,
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @AmountColumnName   = @AmountColumnName,
            @ToCurrencyCode     = @ToCurrencyCode,
            @RateTypeCode       = @RateTypeCode,
            @MaxFallbackDays    = @MaxFallbackDays,
            @RowsAffected       = @StagedRows OUTPUT;

        --  Legacy euro-zone currencies never had a daily rate after 2002.
        UPDATE s
        SET s.ConversionRate     = cur.EuroFixedRate * ISNULL(eur.ConversionRate, 1),
            s.ConvertedAmount    = CONVERT(DECIMAL(19,4),
                                       s.SourceAmount * cur.EuroFixedRate * ISNULL(eur.ConversionRate, 1)),
            s.RateResolutionCode = N'OVERRIDE',
            s.AppliedRateDate    = s.RequestedRateDate,
            s.FallbackDaysUsed   = 0
        FROM work.CurrencyConversionScratch AS s
        INNER JOIN ref.Currency AS cur
            ON  cur.CurrencyCode  = s.FromCurrencyCode
            AND cur.IsEuroLegacy  = 1
            AND cur.EuroFixedRate IS NOT NULL
        OUTER APPLY
        (
            SELECT TOP (1) f.ConversionRate
            FROM ref.FxRateDaily AS f
            WHERE f.FromCurrencyCode = N'EUR'
              AND f.ToCurrencyCode   = s.ToCurrencyCode
              AND f.RateTypeCode     = s.RateTypeCode
              AND f.RateDate        <= s.RequestedRateDate
            ORDER BY f.RateDate DESC
        ) AS eur
        WHERE s.BatchId          = @BatchId
          AND s.TargetObjectName = @ObjectName
          AND s.ConversionRate IS NULL;

        UPDATE work.CurrencyConversionScratch
        SET RateResolutionCode = N'MISSING'
        WHERE BatchId          = @BatchId
          AND TargetObjectName = @ObjectName
          AND ConversionRate IS NULL;

        SET @MissingRows = @@ROWCOUNT;

        SET @ParamList = N'@BatchId BIGINT, @ObjectName NVARCHAR(200), @AmountColumnName NVARCHAR(128), '
                       + N'@ToCurrencyCode NCHAR(3), @RowsAffected BIGINT OUTPUT';

        SET @Sql = N'
            UPDATE t
            SET t.' + QUOTENAME(@ConvertedColumnName) + N' = s.ConvertedAmount
            FROM ' + @Qualified + N' AS t
            INNER JOIN work.CurrencyConversionScratch AS s
                ON  s.BatchId           = @BatchId
                AND s.TargetObjectName  = @ObjectName
                AND s.AmountColumnName  = @AmountColumnName
                AND s.TargetBusinessKey = t.' + QUOTENAME(@BusinessKeyColumnName) + N'
            WHERE t.BatchId = @BatchId
              AND s.ConvertedAmount IS NOT NULL;
            SET @RowsAffected = @@ROWCOUNT;';

        EXEC sys.sp_executesql
            @Sql,
            @ParamList,
            @BatchId          = @BatchId,
            @ObjectName       = @ObjectName,
            @AmountColumnName = @AmountColumnName,
            @ToCurrencyCode   = @ToCurrencyCode,
            @RowsAffected     = @UpdatedRows OUTPUT;

        --  Same-currency rows need no rate and are copied straight across.
        SET @Sql = N'
            UPDATE t
            SET t.' + QUOTENAME(@ConvertedColumnName) + N' = t.' + QUOTENAME(@AmountColumnName) + N'
            FROM ' + @Qualified + N' AS t
            WHERE t.BatchId = @BatchId
              AND t.' + QUOTENAME(@CurrencyColumnName) + N' = @ToCurrencyCode;
            SET @RowsAffected = @@ROWCOUNT;';

        DECLARE @SameCurrencyRows BIGINT = 0;

        EXEC sys.sp_executesql
            @Sql,
            @ParamList,
            @BatchId          = @BatchId,
            @ObjectName       = @ObjectName,
            @AmountColumnName = @AmountColumnName,
            @ToCurrencyCode   = @ToCurrencyCode,
            @RowsAffected     = @SameCurrencyRows OUTPUT;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @StagedRows,
            @UpdateRowCount     = @UpdatedRows + @SameCurrencyRows,
            @RejectRowCount     = @MissingRows;
    END TRY
    BEGIN CATCH
        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_CONVERT_CURRENCY',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConvertCurrencyAmounts';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
