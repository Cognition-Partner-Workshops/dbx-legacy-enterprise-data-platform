/*
    ref.usp_LoadFxRateDaily

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_Currency (SSIS)
    Reads         : raw.OracleFxRate, ref.Currency
    Writes        : ref.FxRateDaily, err.RejectedConstraintViolation,
                    err.RejectedLookupFailure
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    One rate per currency pair, per day, per rate type. Treasury publishes SPOT
    and CORPORATE, the close process publishes PERIOD_END and the reporting pack
    uses AVERAGE; the finance facts pick the type they need, so all four are kept
    side by side rather than being collapsed into one "the rate" column.

    Fill-forward rule for non-trading days
    --------------------------------------
    The ERP only publishes rates on trading days, so weekends, public holidays
    and the days a feed simply failed have no row. Every fact load that converts
    an amount on such a day would otherwise miss its lookup. The rule this estate
    has always used, and which is reproduced here:

      * a missing day takes the most recent earlier rate for the same pair and
        rate type, carried forward unchanged;
      * the carried row is written with the same RateDate as the missing day and
        RateSourceCode suffixed _FF so a carried rate can be told apart from a
        published one;
      * a rate is carried forward for at most @MaxFillForwardDays calendar days
        (the default of 5 covers a long weekend plus one failed feed); beyond
        that the gap is left open and recorded in err.RejectedLookupFailure,
        because a fortnight-old rate is a finance problem, not a default;
      * treasury overrides (IsTreasuryOverride = 1) are never overwritten by a
        carried rate and are never themselves carried forward.

    Rates arrive as text and the inverse rate is published alongside the rate;
    where the two disagree by more than a rounding unit the published rate wins
    and the row is recorded as a constraint violation for the treasury team.
*/

IF OBJECT_ID(N'ref.usp_LoadFxRateDaily', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadFxRateDaily;
GO

CREATE PROCEDURE ref.usp_LoadFxRateDaily
(
    @BatchId               BIGINT,
    @PackageExecutionId    BIGINT = NULL,
    @SourceSystemCode      NVARCHAR(20) = N'ORA_ERP',
    @MaxFillForwardDays    SMALLINT = 5
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'ref.FxRateDaily';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @UpdatedRows  BIGINT = 0;
    DECLARE @CarriedRows  BIGINT = 0;
    DECLARE @RejectedRows BIGINT = 0;
    DECLARE @OpenGapRows  BIGINT = 0;
    DECLARE @MergeAction TABLE (ActionName NVARCHAR(10) NOT NULL);

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleFxRate AS r
        WHERE r.BatchId = @BatchId;

        SELECT
            FromCurrencyCode = NULLIF(LEFT(UPPER(LTRIM(RTRIM(r.FROM_CURRENCY_CD))), 3), N''),
            ToCurrencyCode   = NULLIF(LEFT(UPPER(LTRIM(RTRIM(r.TO_CURRENCY_CD))), 3), N''),
            RateDate         = CONVERT(DATE, stg.ufn_SafeDate(r.RATE_DT, N'NA')),
            RateTypeCode     = UPPER(LTRIM(RTRIM(ISNULL(NULLIF(r.RATE_TYPE_CD, N''), N'SPOT')))),
            ConversionRate   = TRY_CONVERT(DECIMAL(19,8), LTRIM(RTRIM(r.CONVERSION_RATE))),
            InverseRate      = TRY_CONVERT(DECIMAL(19,8), LTRIM(RTRIM(r.INVERSE_RATE))),
            RateSourceCode   = NULLIF(LEFT(UPPER(LTRIM(RTRIM(r.RATE_SOURCE_CD))), 20), N''),
            LedgerCode       = NULLIF(UPPER(LTRIM(RTRIM(r.LEDGER_CD))), N''),
            SourcePair       = CONCAT(LTRIM(RTRIM(r.FROM_CURRENCY_CD)), N'/', LTRIM(RTRIM(r.TO_CURRENCY_CD)))
        INTO #FxTyped
        FROM raw.OracleFxRate AS r
        WHERE r.BatchId = @BatchId;

        --  Unusable rows: no date, no positive rate, or a pair the conformed
        --  currency list does not know. None of them are dropped silently.
        INSERT INTO err.RejectedConstraintViolation
        (
            BatchId, PackageExecutionId, TargetObjectName, ConstraintName, ConstraintTypeCode,
            ViolatingBusinessKey, ViolatingColumnName, ViolatingValue, RejectReasonCode,
            RejectReason, RejectStage, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'ref.FxRateDaily', N'PK_refFxRateDaily', N'PK',
            f.SourcePair, N'ConversionRate', CONVERT(NVARCHAR(400), f.ConversionRate), N'CONSTRAINT',
            CASE
                WHEN f.RateDate IS NULL THEN N'RATE_DT is not a recognisable date'
                WHEN f.ConversionRate IS NULL THEN N'CONVERSION_RATE is not numeric'
                ELSE N'CONVERSION_RATE is not greater than zero'
            END,
            N'Reference',
            CONCAT(N'{"PAIR":"', f.SourcePair, N'","RATE_DT":"', ISNULL(CONVERT(NVARCHAR(10), f.RateDate), N''),
                   N'","RATE":"', ISNULL(CONVERT(NVARCHAR(40), f.ConversionRate), N''), N'"}')
        FROM #FxTyped AS f
        WHERE f.RateDate IS NULL
           OR f.ConversionRate IS NULL
           OR f.ConversionRate <= 0
           OR f.FromCurrencyCode IS NULL
           OR f.ToCurrencyCode IS NULL;

        SET @RejectedRows = @@ROWCOUNT;

        --  A pair quoted against a currency that is not in ref.Currency is a
        --  lookup failure: the rate is kept out but the pair is reported.
        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, QueuedForLateArrival, OccurrenceCount, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'raw.OracleFxRate', f.SourcePair, N'Currency',
            N'FROM_CURRENCY_CD/TO_CURRENCY_CD', f.SourcePair, @SourceSystemCode, N'LOOKUP_MISS',
            N'FX pair references a currency that is not in the conformed currency set',
            N'Reference', 0, 0, COUNT_BIG(*), NULL
        FROM #FxTyped AS f
        WHERE f.FromCurrencyCode IS NOT NULL
          AND f.ToCurrencyCode IS NOT NULL
          AND f.ConversionRate > 0
          AND
          (
              NOT EXISTS (SELECT 1 FROM ref.Currency AS c WHERE c.CurrencyCode = f.FromCurrencyCode)
              OR NOT EXISTS (SELECT 1 FROM ref.Currency AS c WHERE c.CurrencyCode = f.ToCurrencyCode)
          )
        GROUP BY f.SourcePair;

        --  The published inverse disagreeing with the published rate has been a
        --  recurring symptom of a partially applied treasury correction.
        INSERT INTO err.RejectedConstraintViolation
        (
            BatchId, PackageExecutionId, TargetObjectName, ConstraintName, ConstraintTypeCode,
            ViolatingBusinessKey, ViolatingColumnName, ViolatingValue, RejectReasonCode,
            RejectReason, RejectStage, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'ref.FxRateDaily', N'CK_refFxRateDaily_Inverse', N'CK',
            CONCAT(f.SourcePair, N'|', CONVERT(NVARCHAR(10), f.RateDate)), N'InverseRate',
            CONVERT(NVARCHAR(400), f.InverseRate), N'CONSTRAINT',
            N'published INVERSE_RATE does not agree with CONVERSION_RATE; the published rate is used',
            N'Reference', NULL
        FROM #FxTyped AS f
        WHERE f.ConversionRate > 0
          AND f.InverseRate > 0
          AND ABS((f.ConversionRate * f.InverseRate) - 1) > 0.0001;

        BEGIN TRANSACTION;

        --  Published rates first. Last row per key wins, which is how the ERP
        --  restates a rate: it re-sends the whole day.
        MERGE ref.FxRateDaily AS tgt
        USING
        (
            SELECT
                f.FromCurrencyCode,
                f.ToCurrencyCode,
                f.RateDate,
                f.RateTypeCode,
                ConversionRate = MAX(f.ConversionRate),
                RateSourceCode = MAX(ISNULL(f.RateSourceCode, N'ERP')),
                IsTreasuryOverride = MAX(CASE WHEN f.LedgerCode = N'TREASURY' THEN 1 ELSE 0 END)
            FROM #FxTyped AS f
            WHERE f.RateDate IS NOT NULL
              AND f.ConversionRate > 0
              AND f.FromCurrencyCode IS NOT NULL
              AND f.ToCurrencyCode IS NOT NULL
              AND EXISTS (SELECT 1 FROM ref.Currency AS c WHERE c.CurrencyCode = f.FromCurrencyCode)
              AND EXISTS (SELECT 1 FROM ref.Currency AS c WHERE c.CurrencyCode = f.ToCurrencyCode)
            GROUP BY f.FromCurrencyCode, f.ToCurrencyCode, f.RateDate, f.RateTypeCode
        ) AS src
            ON  tgt.FromCurrencyCode = src.FromCurrencyCode
            AND tgt.ToCurrencyCode   = src.ToCurrencyCode
            AND tgt.RateDate         = src.RateDate
            AND tgt.RateTypeCode     = src.RateTypeCode
        WHEN MATCHED AND tgt.IsTreasuryOverride = 0 THEN
            UPDATE SET
                tgt.ConversionRate     = src.ConversionRate,
                tgt.RateSourceCode     = src.RateSourceCode,
                tgt.IsTreasuryOverride = src.IsTreasuryOverride,
                tgt.EffectiveFromUtc   = SYSUTCDATETIME(),
                tgt.LoadedFromBatchId  = @BatchId
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                FromCurrencyCode, ToCurrencyCode, RateDate, RateTypeCode, ConversionRate,
                RateSourceCode, IsTreasuryOverride, EffectiveFromUtc, EffectiveToUtc,
                LoadedFromBatchId
            )
            VALUES
            (
                src.FromCurrencyCode, src.ToCurrencyCode, src.RateDate, src.RateTypeCode,
                src.ConversionRate, src.RateSourceCode, src.IsTreasuryOverride, SYSUTCDATETIME(),
                NULL, @BatchId
            )
        OUTPUT $action INTO @MergeAction (ActionName);

        SELECT
            @InsertedRows = COUNT_BIG(CASE WHEN a.ActionName = N'INSERT' THEN 1 END),
            @UpdatedRows  = COUNT_BIG(CASE WHEN a.ActionName = N'UPDATE' THEN 1 END)
        FROM @MergeAction AS a;

        --  Fill forward. The spine is every date between the first and last
        --  published rate in this batch; ref.Calendar is not used because the
        --  reference layer must load before the calendar package runs.
        DECLARE @FirstDate DATE;
        DECLARE @LastDate  DATE;

        SELECT
            @FirstDate = MIN(f.RateDate),
            @LastDate  = MAX(f.RateDate)
        FROM #FxTyped AS f
        WHERE f.RateDate IS NOT NULL;

        IF @FirstDate IS NOT NULL
        BEGIN
            WITH DateSpine AS
            (
                SELECT @FirstDate AS SpineDate
                UNION ALL
                SELECT DATEADD(day, 1, SpineDate)
                FROM DateSpine
                WHERE SpineDate < @LastDate
            ),
            RatePair AS
            (
                SELECT DISTINCT
                    r.FromCurrencyCode,
                    r.ToCurrencyCode,
                    r.RateTypeCode
                FROM ref.FxRateDaily AS r
                WHERE r.RateDate BETWEEN @FirstDate AND @LastDate
            ),
            MissingDay AS
            (
                SELECT
                    p.FromCurrencyCode,
                    p.ToCurrencyCode,
                    p.RateTypeCode,
                    d.SpineDate
                FROM RatePair AS p
                CROSS JOIN DateSpine AS d
                WHERE NOT EXISTS
                      (
                          SELECT 1
                          FROM ref.FxRateDaily AS x
                          WHERE x.FromCurrencyCode = p.FromCurrencyCode
                            AND x.ToCurrencyCode   = p.ToCurrencyCode
                            AND x.RateTypeCode     = p.RateTypeCode
                            AND x.RateDate         = d.SpineDate
                      )
            ),
            CarriedRate AS
            (
                SELECT
                    m.FromCurrencyCode,
                    m.ToCurrencyCode,
                    m.RateTypeCode,
                    m.SpineDate,
                    p.ConversionRate,
                    p.RateSourceCode,
                    p.RateDate AS CarriedFromDate
                FROM MissingDay AS m
                CROSS APPLY
                (
                    SELECT TOP (1)
                        x.ConversionRate,
                        x.RateSourceCode,
                        x.RateDate
                    FROM ref.FxRateDaily AS x
                    WHERE x.FromCurrencyCode = m.FromCurrencyCode
                      AND x.ToCurrencyCode   = m.ToCurrencyCode
                      AND x.RateTypeCode     = m.RateTypeCode
                      AND x.RateDate         < m.SpineDate
                      AND x.IsTreasuryOverride = 0
                    ORDER BY x.RateDate DESC
                ) AS p
            )
            INSERT INTO ref.FxRateDaily
            (
                FromCurrencyCode, ToCurrencyCode, RateDate, RateTypeCode, ConversionRate,
                RateSourceCode, IsTreasuryOverride, EffectiveFromUtc, EffectiveToUtc,
                LoadedFromBatchId
            )
            SELECT
                c.FromCurrencyCode,
                c.ToCurrencyCode,
                c.SpineDate,
                c.RateTypeCode,
                c.ConversionRate,
                LEFT(CONCAT(ISNULL(c.RateSourceCode, N'ERP'), N'_FF'), 20),
                0,
                SYSUTCDATETIME(),
                NULL,
                @BatchId
            FROM CarriedRate AS c
            WHERE DATEDIFF(day, c.CarriedFromDate, c.SpineDate) <= @MaxFillForwardDays
            OPTION (MAXRECURSION 32767);

            SET @CarriedRows = @@ROWCOUNT;

            --  Gaps too wide to carry are left open on purpose and reported.
            WITH OpenGap AS
            (
                SELECT
                    r.FromCurrencyCode,
                    r.ToCurrencyCode,
                    r.RateTypeCode,
                    r.RateDate,
                    NextRateDate = LEAD(r.RateDate) OVER
                                   (
                                       PARTITION BY r.FromCurrencyCode, r.ToCurrencyCode, r.RateTypeCode
                                       ORDER BY r.RateDate
                                   )
                FROM ref.FxRateDaily AS r
                WHERE r.RateDate BETWEEN @FirstDate AND @LastDate
            )
            INSERT INTO err.RejectedLookupFailure
            (
                BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
                LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
                RejectStage, RoutedToUnknownMember, QueuedForLateArrival, OccurrenceCount,
                RecordPayload
            )
            SELECT
                @BatchId, @PackageExecutionId, N'ref.FxRateDaily',
                CONCAT(g.FromCurrencyCode, N'/', g.ToCurrencyCode), N'FxRateDaily',
                N'RateDate', CONVERT(NVARCHAR(10), g.RateDate), @SourceSystemCode,
                N'FX_GAP_OPEN',
                CONCAT(N'no published rate for ', DATEDIFF(day, g.RateDate, g.NextRateDate),
                       N' days, which exceeds the fill-forward limit'),
                N'Reference', 0, 1, 1,
                CONCAT(N'{"PAIR":"', g.FromCurrencyCode, N'/', g.ToCurrencyCode,
                       N'","RATE_TYPE":"', g.RateTypeCode, N'"}')
            FROM OpenGap AS g
            WHERE g.NextRateDate IS NOT NULL
              AND DATEDIFF(day, g.RateDate, g.NextRateDate) > @MaxFillForwardDays;

            SET @OpenGapRows = @@ROWCOUNT;
        END;

        COMMIT TRANSACTION;

        DECLARE @TargetRowCountValue BIGINT = @InsertedRows + @UpdatedRows + @CarriedRows;
        DECLARE @InsertRowCountValue BIGINT = @InsertedRows + @CarriedRows;
        DECLARE @RejectRowCountValue BIGINT = @RejectedRows + @OpenGapRows;
        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @TargetRowCountValue,
            @InsertRowCount     = @InsertRowCountValue,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = @RejectRowCountValue;

        DROP TABLE #FxTyped;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'REF_Load_Currency',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'ref.usp_LoadFxRateDaily';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
