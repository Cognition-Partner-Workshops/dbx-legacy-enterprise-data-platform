/*
    ref.usp_LoadRegion

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_Geography (SSIS)
    Reads         : raw.OracleGeography, ref.CodeCrosswalk
    Writes        : ref.Region, err.RejectedLookupFailure
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    ref.Region is the smallest conformed list in the estate and the one every
    other reference load depends on, because the regional divergence (tax
    regime, fiscal calendar, address rules, weight unit, consent model) is held
    here rather than being re-derived in each package.

    The three operating regions are a business decision, not a source value, so
    the attribute grid below is maintained here and only the membership comes
    from the source: whichever REGION_CD values the ERP geography extract
    carries are mapped onto NA / EU / APAC through ref.CodeCrosswalk domain
    REGION. A REGION_CD with no mapping is recorded as a lookup failure and the
    geography rows behind it are attributed to NA by stg.usp_TruncateAndReload_Geography,
    which is the behaviour the 2009 load had and nobody has been willing to change.
*/

IF OBJECT_ID(N'ref.usp_LoadRegion', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadRegion;
GO

CREATE PROCEDURE ref.usp_LoadRegion
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'ref.Region';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @UpdatedRows  BIGINT = 0;
    DECLARE @LookupMisses BIGINT = 0;
    DECLARE @MergeAction TABLE (ActionName NVARCHAR(10) NOT NULL);

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(DISTINCT NULLIF(UPPER(LTRIM(RTRIM(r.REGION_CD))), N''))
        FROM raw.OracleGeography AS r
        WHERE r.BatchId = @BatchId;

        BEGIN TRANSACTION;

        --  The conformed grid. Held in the procedure because it is policy, not
        --  data, and because the stewards' spreadsheet was lost in 2013.
        WITH ConformedRegion AS
        (
            SELECT *
            FROM
            (
                VALUES
                    (N'NA',   N'North America',        N'SALESTAX', N'USD', N'NA_CAL',  7,
                     N'NA_USPS',    N'LB', N'OPT_OUT', 84,  N'MM/DD/YYYY', N'.'),
                    (N'EU',   N'Europe',               N'VAT',      N'EUR', N'EU_APR',  1,
                     N'EU_COUNTRY', N'KG', N'OPT_IN',  120, N'DD/MM/YYYY', N','),
                    (N'APAC', N'Asia Pacific',         N'GST',      N'AUD', N'APAC_JUL', 4,
                     N'APAC_LOCAL', N'KG', N'OPT_IN',  60,  N'YYYY-MM-DD', N'.')
            ) AS v (RegionCode, RegionName, TaxRegimeCode, DefaultCurrencyCode, FiscalCalendarCode,
                    FiscalYearStartMonth, AddressRuleSetCode, WeightUomCode, ConsentModelCode,
                    DefaultRetentionMonths, DateFormatHint, DecimalSeparator)
        )
        MERGE ref.Region AS tgt
        USING ConformedRegion AS src
            ON tgt.RegionCode = src.RegionCode
        WHEN MATCHED THEN
            UPDATE SET
                tgt.RegionName             = src.RegionName,
                tgt.TaxRegimeCode          = src.TaxRegimeCode,
                tgt.DefaultCurrencyCode    = src.DefaultCurrencyCode,
                tgt.FiscalCalendarCode     = src.FiscalCalendarCode,
                tgt.FiscalYearStartMonth   = src.FiscalYearStartMonth,
                tgt.AddressRuleSetCode     = src.AddressRuleSetCode,
                tgt.WeightUomCode          = src.WeightUomCode,
                tgt.ConsentModelCode       = src.ConsentModelCode,
                tgt.DefaultRetentionMonths = src.DefaultRetentionMonths,
                tgt.DateFormatHint         = src.DateFormatHint,
                tgt.DecimalSeparator       = src.DecimalSeparator,
                tgt.IsActive               = 1
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                RegionCode, RegionName, TaxRegimeCode, DefaultCurrencyCode, FiscalCalendarCode,
                FiscalYearStartMonth, AddressRuleSetCode, WeightUomCode, ConsentModelCode,
                DefaultRetentionMonths, DateFormatHint, DecimalSeparator, IsActive
            )
            VALUES
            (
                src.RegionCode, src.RegionName, src.TaxRegimeCode, src.DefaultCurrencyCode,
                src.FiscalCalendarCode, src.FiscalYearStartMonth, src.AddressRuleSetCode,
                src.WeightUomCode, src.ConsentModelCode, src.DefaultRetentionMonths,
                src.DateFormatHint, src.DecimalSeparator, 1
            )
        OUTPUT $action INTO @MergeAction (ActionName);

        SELECT
            @InsertedRows = COUNT_BIG(CASE WHEN a.ActionName = N'INSERT' THEN 1 END),
            @UpdatedRows  = COUNT_BIG(CASE WHEN a.ActionName = N'UPDATE' THEN 1 END)
        FROM @MergeAction AS a;

        --  Source regions that the crosswalk does not recognise. The row is not
        --  dropped anywhere - the geography load still stages it - but the value
        --  has to reach the steward list or nobody ever adds the mapping.
        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, QueuedForLateArrival, OccurrenceCount, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'raw.OracleGeography', MIN(LTRIM(RTRIM(r.GEOGRAPHY_ID))),
            N'Region', N'REGION_CD', UPPER(LTRIM(RTRIM(r.REGION_CD))), @SourceSystemCode,
            N'LOOKUP_MISS',
            N'REGION_CD has no active ref.CodeCrosswalk row in domain REGION and is not a conformed region',
            N'Reference', 1, 0, COUNT_BIG(*),
            CONCAT(N'{"REGION_CD":"', UPPER(LTRIM(RTRIM(r.REGION_CD))), N'"}')
        FROM raw.OracleGeography AS r
        WHERE r.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(r.REGION_CD)), N'') IS NOT NULL
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM ref.Region AS g
                  WHERE g.RegionCode = UPPER(LTRIM(RTRIM(r.REGION_CD)))
              )
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM ref.CodeCrosswalk AS x
                  WHERE x.CodeDomainCode   = N'REGION'
                    AND x.SourceSystemCode = @SourceSystemCode
                    AND x.SourceCodeValue  = UPPER(LTRIM(RTRIM(r.REGION_CD)))
                    AND x.EffectiveToDate IS NULL
              )
        GROUP BY UPPER(LTRIM(RTRIM(r.REGION_CD)));

        SET @LookupMisses = @@ROWCOUNT;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @UpdatedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @LookupMisses;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'REF_Load_Geography',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'ref.usp_LoadRegion';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
