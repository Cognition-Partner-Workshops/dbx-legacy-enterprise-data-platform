/*
    ref.usp_LoadCurrency

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_Currency (SSIS)
    Reads         : raw.OracleCurrency, ref.CodeCrosswalk
    Writes        : ref.Currency, err.RejectedConstraintViolation
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Only Oracle publishes a currency master; the OLTP database carries currency
    codes on transactions but has no list of its own, so its values reach the
    conformed set through ref.CodeCrosswalk domain CURRENCY rather than through
    this procedure.

    The extract still contains the pre-1999 legacy currencies (DEM, FRF, ITL and
    the rest) with their irrevocable euro conversion rates. They are kept, not
    filtered: the general ledger still holds documents in them and the finance
    facts convert historical amounts through EuroFixedRate. They are marked
    IsEuroLegacy and carry a retirement date so nothing new can be posted in them.

    Rounding is a currency property in this estate, not a report property: JPY
    and KRW carry no minor units and round HALF_UP, CHF rounds to five centimes,
    everything else keeps two digits.
*/

IF OBJECT_ID(N'ref.usp_LoadCurrency', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadCurrency;
GO

CREATE PROCEDURE ref.usp_LoadCurrency
(
    @BatchId                  BIGINT,
    @PackageExecutionId       BIGINT = NULL,
    @SourceSystemCode         NVARCHAR(20) = N'ORA_ERP',
    @ReportingCurrencyCode    NCHAR(3) = N'USD'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'ref.Currency';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @UpdatedRows  BIGINT = 0;
    DECLARE @RejectedRows BIGINT = 0;
    DECLARE @MergeAction TABLE (ActionName NVARCHAR(10) NOT NULL);

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleCurrency AS r
        WHERE r.BatchId = @BatchId;

        SELECT
            CurrencyCode    = NULLIF(LEFT(UPPER(LTRIM(RTRIM(r.CURRENCY_CD))), 3), N''),
            CurrencyName    = LEFT(stg.ufn_CleanString(r.CURRENCY_NAME, 0), 100),
            CurrencySymbol  = NULLIF(LEFT(LTRIM(RTRIM(r.CURRENCY_SYMBOL)), 10), N''),
            MinorUnitDigits = CONVERT(TINYINT, ISNULL(TRY_CONVERT(INT, LTRIM(RTRIM(r.MINOR_UNIT_DIGITS))), 2)),
            IsActiveFlag    = CASE WHEN UPPER(LTRIM(RTRIM(r.ACTIVE_FLG))) IN (N'Y', N'YES', N'1') THEN 1 ELSE 0 END,
            IsEuroLegacy    = CASE WHEN UPPER(LTRIM(RTRIM(r.EURO_LEGACY_FLG))) IN (N'Y', N'YES', N'1') THEN 1 ELSE 0 END,
            EuroFixedRate   = TRY_CONVERT(DECIMAL(19,8), LTRIM(RTRIM(r.LEGACY_FIXED_RATE))),
            SourceValue     = LTRIM(RTRIM(r.CURRENCY_CD))
        INTO #CurrencyTyped
        FROM raw.OracleCurrency AS r
        WHERE r.BatchId = @BatchId;

        --  A currency code that is not three characters cannot key the FX table
        --  or the fact conversions; it is rejected rather than truncated.
        INSERT INTO err.RejectedConstraintViolation
        (
            BatchId, PackageExecutionId, TargetObjectName, ConstraintName, ConstraintTypeCode,
            ViolatingBusinessKey, ViolatingColumnName, ViolatingValue, RejectReasonCode,
            RejectReason, RejectStage, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'ref.Currency', N'PK_refCurrency', N'PK',
            c.SourceValue, N'CurrencyCode', c.SourceValue, N'CONSTRAINT',
            N'CURRENCY_CD is not a three-character ISO currency code',
            N'Reference', CONCAT(N'{"CURRENCY_CD":"', c.SourceValue, N'"}')
        FROM #CurrencyTyped AS c
        WHERE c.CurrencyCode IS NULL
           OR LEN(c.CurrencyCode) <> 3;

        SET @RejectedRows = @@ROWCOUNT;

        BEGIN TRANSACTION;

        MERGE ref.Currency AS tgt
        USING
        (
            SELECT
                c.CurrencyCode,
                CurrencyName    = ISNULL(NULLIF(c.CurrencyName, N''), c.CurrencyCode),
                c.CurrencySymbol,
                --  The ERP leaves the minor unit blank far more often than it
                --  gets it wrong, so the zero-decimal currencies are asserted.
                MinorUnitDigits = CASE
                                      WHEN c.CurrencyCode IN (N'JPY', N'KRW', N'CLP', N'ISK') THEN 0
                                      WHEN c.MinorUnitDigits BETWEEN 0 AND 4 THEN c.MinorUnitDigits
                                      ELSE 2
                                  END,
                RoundingRuleCode = CASE
                                       WHEN c.CurrencyCode = N'CHF' THEN N'FIVE_CENTIME'
                                       WHEN c.CurrencyCode IN (N'JPY', N'KRW') THEN N'HALF_UP_UNIT'
                                       ELSE N'HALF_UP'
                                   END,
                IsReportingCurrency = CASE WHEN c.CurrencyCode = @ReportingCurrencyCode THEN 1 ELSE 0 END,
                c.IsEuroLegacy,
                EuroFixedRate   = CASE WHEN c.IsEuroLegacy = 1 THEN c.EuroFixedRate END,
                --  Legacy currencies were retired at the euro cash changeover;
                --  the ERP does not carry the date so it is asserted here.
                RetiredDate     = CASE
                                      WHEN c.IsEuroLegacy = 1 THEN CONVERT(DATE, N'2002-01-01')
                                      WHEN c.IsActiveFlag = 0 THEN CONVERT(DATE, N'1900-01-01')
                                  END,
                IsActive        = CASE WHEN c.IsEuroLegacy = 1 THEN 0 ELSE c.IsActiveFlag END
            FROM #CurrencyTyped AS c
            WHERE c.CurrencyCode IS NOT NULL
              AND LEN(c.CurrencyCode) = 3
        ) AS src
            ON tgt.CurrencyCode = src.CurrencyCode
        WHEN MATCHED THEN
            UPDATE SET
                tgt.CurrencyName        = src.CurrencyName,
                tgt.CurrencySymbol      = COALESCE(src.CurrencySymbol, tgt.CurrencySymbol),
                tgt.MinorUnitDigits     = src.MinorUnitDigits,
                tgt.RoundingRuleCode    = src.RoundingRuleCode,
                tgt.IsReportingCurrency = src.IsReportingCurrency,
                tgt.IsEuroLegacy        = src.IsEuroLegacy,
                tgt.EuroFixedRate       = COALESCE(src.EuroFixedRate, tgt.EuroFixedRate),
                tgt.RetiredDate         = COALESCE(src.RetiredDate, tgt.RetiredDate),
                tgt.IsActive            = src.IsActive
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                CurrencyCode, CurrencyName, CurrencySymbol, MinorUnitDigits, RoundingRuleCode,
                IsReportingCurrency, IsEuroLegacy, EuroFixedRate, RetiredDate, IsActive
            )
            VALUES
            (
                src.CurrencyCode, src.CurrencyName, src.CurrencySymbol, src.MinorUnitDigits,
                src.RoundingRuleCode, src.IsReportingCurrency, src.IsEuroLegacy,
                src.EuroFixedRate, src.RetiredDate, src.IsActive
            )
        OUTPUT $action INTO @MergeAction (ActionName);

        SELECT
            @InsertedRows = COUNT_BIG(CASE WHEN a.ActionName = N'INSERT' THEN 1 END),
            @UpdatedRows  = COUNT_BIG(CASE WHEN a.ActionName = N'UPDATE' THEN 1 END)
        FROM @MergeAction AS a;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows + @UpdatedRows,
            @InsertRowCount     = @InsertedRows,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = @RejectedRows;

        DROP TABLE #CurrencyTyped;
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
            @ProcedureName      = N'ref.usp_LoadCurrency';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
