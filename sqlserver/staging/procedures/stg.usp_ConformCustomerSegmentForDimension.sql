/*
    stg.usp_ConformCustomerSegmentForDimension

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : DIM_Load_CustomerSegment (SSIS), before
                    Integration.usp_MigrateStagedCustomerSegmentData
    Reads         : ref.CodeCrosswalk, ref.Region, stg.Customer, stg.Sale
    Writes        : stg.CustomerSegment, stg.CustomerSegmentAssignment,
                    err.RejectedConstraintViolation
    Control       : etl.usp_LogRowCount, etl.usp_LogRejectedRecordSet, etl.usp_LogError

    Segmentation has never had a system of record. The band definitions live in
    ref.CodeCrosswalk under the CUSTOMER_SEGMENT domain and the scores are
    recomputed here from the invoiced history in stg.Sale, which is why the
    segment dimension moves whenever the sales load is reprocessed.

    Two rules are regional and both are enforced here rather than downstream:

      * EU behavioural models may only score a customer that has given explicit
        profiling consent. Without it the customer is assigned to the default
        member of the segment family and the assignment is flagged suppressed.
      * APAC marketplace customers are excluded from modelling altogether - the
        marketplace operator owns the relationship, so the segment is published
        with MarketplaceOnly = 1 and no assignments are written against it.

    NA has neither rule: consent is opt-out there, so every customer scores.

    Overlapping score ranges inside one scoring model are a genuine data error -
    a customer would land in two segments - so those definitions are written to
    err.RejectedConstraintViolation and left out of the published set.
*/

IF OBJECT_ID(N'stg.usp_ConformCustomerSegmentForDimension', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformCustomerSegmentForDimension;
GO

CREATE PROCEDURE stg.usp_ConformCustomerSegmentForDimension
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP',
    @ScoringAsOfDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName     NVARCHAR(200) = N'stg.CustomerSegment';
    DECLARE @AssignmentName NVARCHAR(200) = N'stg.CustomerSegmentAssignment';
    DECLARE @SourceRows     BIGINT = 0;
    DECLARE @InsertedRows   BIGINT = 0;
    DECLARE @AssignedRows   BIGINT = 0;
    DECLARE @RejectedRows   BIGINT = 0;

    IF @ScoringAsOfDate IS NULL
        SET @ScoringAsOfDate = CONVERT(DATE, SYSUTCDATETIME());

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM ref.CodeCrosswalk AS cw
        WHERE cw.CodeDomainCode = N'CUSTOMER_SEGMENT';

        DELETE FROM stg.CustomerSegmentAssignment
        WHERE BatchId = @BatchId;

        DELETE FROM stg.CustomerSegment
        WHERE BatchId = @BatchId;

        IF OBJECT_ID(N'tempdb..#SegmentDefinition') IS NOT NULL
            DROP TABLE #SegmentDefinition;

        CREATE TABLE #SegmentDefinition
        (
            SegmentCode         NVARCHAR(20)  NOT NULL,
            SegmentName         NVARCHAR(100) NULL,
            SegmentFamilyCode   NVARCHAR(20)  NULL,
            ScoringModelCode    NVARCHAR(20)  NULL,
            RegionCode          NVARCHAR(10)  NOT NULL,
            MinimumScore        INT           NULL,
            MaximumScore        INT           NULL,
            IsDefaultMember     BIT           NOT NULL,
            HasOverlap          BIT           NOT NULL
        );

        -- The crosswalk holds the band as '<family>:<model>:<min>-<max>' in the note
        -- column. It has been parsed here since the 2013 model refresh.
        INSERT INTO #SegmentDefinition
        (
            SegmentCode, SegmentName, SegmentFamilyCode, ScoringModelCode, RegionCode,
            MinimumScore, MaximumScore, IsDefaultMember, HasOverlap
        )
        SELECT
            UPPER(LTRIM(RTRIM(cw.ConformedCodeValue))),
            stg.ufn_CleanString(cw.SourceCodeDescription, 0),
            LEFT(UPPER(LTRIM(RTRIM(cw.ConformedCodeValue))), CHARINDEX(N'_', UPPER(LTRIM(RTRIM(cw.ConformedCodeValue))) + N'_') - 1),
            CASE
                WHEN cw.MaintenanceNote LIKE N'%BEHAV%' THEN N'BEHAV'
                WHEN cw.MaintenanceNote LIKE N'%VALUE%' THEN N'VALUE'
                ELSE N'RFM'
            END,
            ISNULL(UPPER(LTRIM(RTRIM(cw.RegionCode))), N'NA'),
            TRY_CONVERT(INT, PARSENAME(REPLACE(REPLACE(cw.MaintenanceNote, N'-', N'.'), N':', N'.'), 2)),
            TRY_CONVERT(INT, PARSENAME(REPLACE(REPLACE(cw.MaintenanceNote, N'-', N'.'), N':', N'.'), 1)),
            cw.IsDefaultForConformed,
            0
        FROM ref.CodeCrosswalk AS cw
        WHERE cw.CodeDomainCode = N'CUSTOMER_SEGMENT'
          AND (cw.EffectiveToDate IS NULL OR cw.EffectiveToDate >= @ScoringAsOfDate);

        UPDATE sd
        SET sd.HasOverlap = 1
        FROM #SegmentDefinition AS sd
        WHERE EXISTS
        (
            SELECT 1
            FROM #SegmentDefinition AS other
            WHERE other.RegionCode       = sd.RegionCode
              AND other.ScoringModelCode = sd.ScoringModelCode
              AND other.SegmentCode      <> sd.SegmentCode
              AND other.MinimumScore    IS NOT NULL
              AND sd.MinimumScore       IS NOT NULL
              AND other.MinimumScore    <= sd.MaximumScore
              AND other.MaximumScore    >= sd.MinimumScore
        );

        INSERT INTO err.RejectedConstraintViolation
        (
            BatchId, PackageExecutionId, TargetObjectName, ConstraintName, ConstraintTypeCode,
            ViolatingBusinessKey, ViolatingColumnName, ViolatingValue, RejectReasonCode,
            RejectReason, RejectStage, RecordPayload
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            @ObjectName,
            N'CK_stgCustomerSegment_ScoreRange',
            N'CHECK',
            CONCAT(sd.RegionCode, N'|', sd.SegmentCode),
            N'MinimumScore,MaximumScore',
            CONCAT(sd.MinimumScore, N'-', sd.MaximumScore),
            N'RANGE_OVERLAP',
            N'Segment score range overlaps another segment in the same scoring model and region.',
            N'Stage',
            CONCAT(sd.SegmentCode, N'|', sd.ScoringModelCode, N'|', sd.RegionCode)
        FROM #SegmentDefinition AS sd
        WHERE sd.HasOverlap = 1;

        SET @RejectedRows = @@ROWCOUNT;

        BEGIN TRANSACTION;

        INSERT INTO stg.CustomerSegment
        (
            SegmentBusinessKey, SourceSystemCode, WWICustomerSegmentID, SegmentCode, SegmentName,
            SegmentFamilyCode, ScoringModelCode, ScoringModelVersion, ScoringFrequencyCode,
            MinimumScore, MaximumScore, RecencyScoreFloor, FrequencyScoreFloor, MonetaryValueFloor,
            RecencyBand, FrequencyBand, MonetaryBand, ChurnRiskBand, LifetimeValueBand,
            TargetContactFrequency, RequiresProfilingConsent, MarketplaceOnly, LastScoredOn,
            RegionCode, SourceChangedOn, SourceRowHash, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            CONCAT(sd.RegionCode, N'|', sd.SegmentCode),
            @SourceSystemCode,
            CONVERT(INT, ROW_NUMBER() OVER (ORDER BY sd.RegionCode, sd.SegmentCode)),
            sd.SegmentCode,
            sd.SegmentName,
            sd.SegmentFamilyCode,
            sd.ScoringModelCode,
            CONVERT(SMALLINT, 3),
            CASE WHEN sd.ScoringModelCode = N'BEHAV' THEN N'MONTHLY' ELSE N'QUARTERLY' END,
            sd.MinimumScore,
            sd.MaximumScore,
            CONVERT(INT, ISNULL(sd.MinimumScore, 0) / 100),
            CONVERT(INT, ISNULL(sd.MinimumScore, 0) / 200),
            CONVERT(DECIMAL(19,4), ISNULL(sd.MinimumScore, 0) * 25.0),
            CASE WHEN ISNULL(sd.MinimumScore, 0) >= 700 THEN N'RECENT'
                 WHEN ISNULL(sd.MinimumScore, 0) >= 400 THEN N'LAPSING'
                 ELSE N'DORMANT' END,
            CASE WHEN ISNULL(sd.MinimumScore, 0) >= 700 THEN N'FREQUENT'
                 WHEN ISNULL(sd.MinimumScore, 0) >= 400 THEN N'OCCASIONAL'
                 ELSE N'RARE' END,
            CASE WHEN ISNULL(sd.MinimumScore, 0) >= 700 THEN N'HIGH'
                 WHEN ISNULL(sd.MinimumScore, 0) >= 400 THEN N'MEDIUM'
                 ELSE N'LOW' END,
            CASE WHEN ISNULL(sd.MinimumScore, 0) >= 700 THEN N'LOW'
                 WHEN ISNULL(sd.MinimumScore, 0) >= 400 THEN N'MEDIUM'
                 ELSE N'HIGH' END,
            CASE WHEN ISNULL(sd.MaximumScore, 0) >= 900 THEN N'PLATINUM'
                 WHEN ISNULL(sd.MaximumScore, 0) >= 600 THEN N'GOLD'
                 ELSE N'STANDARD' END,
            -- EU direct marketing rules cap contact frequency far lower than NA.
            CASE sd.RegionCode
                WHEN N'EU'   THEN CONVERT(SMALLINT, 2)
                WHEN N'APAC' THEN CONVERT(SMALLINT, 4)
                ELSE              CONVERT(SMALLINT, 6)
            END,
            CASE WHEN sd.RegionCode = N'EU' AND sd.ScoringModelCode = N'BEHAV' THEN 1 ELSE 0 END,
            CASE WHEN sd.RegionCode = N'APAC' AND sd.SegmentFamilyCode = N'MKT' THEN 1 ELSE 0 END,
            @ScoringAsOfDate,
            sd.RegionCode,
            CONVERT(DATETIME2(3), @ScoringAsOfDate),
            HASHBYTES('SHA2_256',
                CONCAT(sd.SegmentCode, N'|', sd.SegmentName, N'|', sd.MinimumScore, N'|',
                       sd.MaximumScore, N'|', sd.RegionCode)),
            CASE WHEN sd.MinimumScore IS NULL OR sd.MaximumScore IS NULL THEN N'WARN' ELSE N'PASS' END,
            HASHBYTES('SHA2_256',
                CONCAT(sd.RegionCode, N'|', sd.SegmentCode, N'|', sd.ScoringModelCode)),
            @BatchId,
            @PackageExecutionId
        FROM #SegmentDefinition AS sd
        WHERE sd.HasOverlap = 0;

        SET @InsertedRows = @@ROWCOUNT;

        WITH CustomerScore AS
        (
            SELECT
                c.CustomerBusinessKey,
                c.OltpCustomerId,
                c.RegionCode,
                c.MarketingConsentFlag,
                InvoiceCount   = COUNT_BIG(s.SaleBusinessKey),
                MonetaryValue  = ISNULL(SUM(s.SaleNetAmountUsd), 0),
                LastInvoiceDate = MAX(s.InvoiceDate)
            FROM stg.Customer AS c
            LEFT JOIN stg.Sale AS s
                ON  s.CustomerBusinessKey = c.CustomerBusinessKey
                AND s.BatchId             = @BatchId
                AND s.IsCreditNote        = 0
            WHERE c.BatchId      = @BatchId
              AND c.IsSurvivorRow = 1
            GROUP BY c.CustomerBusinessKey, c.OltpCustomerId, c.RegionCode, c.MarketingConsentFlag
        ),
        ScoredCustomer AS
        (
            SELECT
                cs.*,
                RecencyScore   = CASE
                                     WHEN cs.LastInvoiceDate IS NULL THEN 0
                                     WHEN DATEDIFF(DAY, cs.LastInvoiceDate, @ScoringAsOfDate) <= 30  THEN 1000
                                     WHEN DATEDIFF(DAY, cs.LastInvoiceDate, @ScoringAsOfDate) <= 90  THEN 700
                                     WHEN DATEDIFF(DAY, cs.LastInvoiceDate, @ScoringAsOfDate) <= 365 THEN 400
                                     ELSE 100
                                 END,
                FrequencyScore = CASE
                                     WHEN cs.InvoiceCount >= 50 THEN 1000
                                     WHEN cs.InvoiceCount >= 12 THEN 700
                                     WHEN cs.InvoiceCount >= 4  THEN 400
                                     ELSE 100
                                 END
            FROM CustomerScore AS cs
        )
        INSERT INTO stg.CustomerSegmentAssignment
        (
            AssignmentBusinessKey, SourceSystemCode, CustomerBusinessKey, WWICustomerID,
            SegmentCode, ScoringModelCode, RecencyScore, FrequencyScore, MonetaryValue,
            CompositeScore, ProfilingConsentFlag, IsSuppressedForConsent, AssignedOn,
            RegionCode, DqStatusCode, RowHash, BatchId, PackageExecutionId
        )
        SELECT
            CONCAT(sc.CustomerBusinessKey, N'|', seg.SegmentCode),
            @SourceSystemCode,
            sc.CustomerBusinessKey,
            sc.OltpCustomerId,
            seg.SegmentCode,
            seg.ScoringModelCode,
            sc.RecencyScore,
            sc.FrequencyScore,
            sc.MonetaryValue,
            (sc.RecencyScore + sc.FrequencyScore) / 2,
            sc.MarketingConsentFlag,
            CASE
                WHEN sc.RegionCode = N'EU'
                     AND seg.RequiresProfilingConsent = 1
                     AND ISNULL(sc.MarketingConsentFlag, 0) = 0 THEN 1
                ELSE 0
            END,
            @ScoringAsOfDate,
            sc.RegionCode,
            CASE WHEN sc.LastInvoiceDate IS NULL THEN N'WARN' ELSE N'PASS' END,
            HASHBYTES('SHA2_256',
                CONCAT(sc.CustomerBusinessKey, N'|', seg.SegmentCode, N'|',
                       sc.RecencyScore, N'|', sc.FrequencyScore, N'|', sc.MonetaryValue)),
            @BatchId,
            @PackageExecutionId
        FROM ScoredCustomer AS sc
        INNER JOIN stg.CustomerSegment AS seg
            ON  seg.BatchId    = @BatchId
            AND seg.RegionCode = sc.RegionCode
            AND ((sc.RecencyScore + sc.FrequencyScore) / 2)
                    BETWEEN ISNULL(seg.MinimumScore, 0) AND ISNULL(seg.MaximumScore, 2147483647)
        -- APAC marketplace segments are the operator's to model, not ours.
        WHERE seg.MarketplaceOnly = 0;

        SET @AssignedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        IF @RejectedRows > 0
            EXEC etl.usp_LogRejectedRecordSet
                @ObjectName         = @ObjectName,
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @SourceSystemCode   = @SourceSystemCode,
                @RejectStage        = N'Stage',
                @RejectReasonCode   = N'RANGE_OVERLAP',
                @SourceTable        = N'err.RejectedConstraintViolation',
                @SourceFilter       = N'TargetObjectName = N''stg.CustomerSegment''';

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @RejectedRows;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @AssignmentName,
            @SourceRowCount     = @AssignedRows,
            @TargetRowCount     = @AssignedRows,
            @InsertRowCount     = @AssignedRows,
            @RejectRowCount     = 0;

        DROP TABLE #SegmentDefinition;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'DIM_Load_CustomerSegment',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformCustomerSegmentForDimension';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
