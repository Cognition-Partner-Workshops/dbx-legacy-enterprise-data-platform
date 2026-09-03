/*
    Integration.usp_RefreshAggregatePromotionEffectiveness

    Object        : Integration.usp_RefreshAggregatePromotionEffectiveness
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.Promotion Effectiveness and
                    Fact.Promotion Eligibility.
    Called by     : AGG_Refresh_Promotion_Effectiveness (weekly, Monday).
    Reads         : Fact.Sale, Fact.Promotion Eligibility, Fact.Loyalty Points,
                    Dimension.Promotion.
    Depends on    : the etl control procedures.

    Take-up is measured against the factless eligibility fact, which is the
    only place that records who could have bought. The baseline is the same
    length of time immediately before the promotion started, on the same
    products - not the prior year, because the merchandising team never
    trusted the year-ago comparison.

    Cannibalisation is a single crude number: revenue lost on non-promoted
    items in the same category during the promotion window compared with the
    baseline. Everyone knows it also picks up seasonality.

    EU rows are marked [Consent Restricted Flag] when the promotion was only
    notifiable to consented customers, which makes their take-up rate look
    worse than NA's for reasons that have nothing to do with the offer.
*/
IF OBJECT_ID(N'Integration.usp_RefreshAggregatePromotionEffectiveness', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RefreshAggregatePromotionEffectiveness;
GO

CREATE PROCEDURE Integration.usp_RefreshAggregatePromotionEffectiveness
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @PromotionKey       INT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'AGG_Refresh_Promotion_Effectiveness',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'RefreshAggregatePromotionEffectiveness',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Aggregate.[Promotion Effectiveness]
        WHERE (@PromotionKey IS NULL OR [Promotion Key] = @PromotionKey);

        SET @DeleteRowCount = @@ROWCOUNT;

        SELECT
            p.[Promotion Key],
            p.[Promotion Code],
            p.[Start Date] AS PromotionStartDate,
            p.[End Date]   AS PromotionEndDate,
            DATEADD(DAY, -1 - DATEDIFF(DAY, p.[Start Date], p.[End Date]),
                    p.[Start Date]) AS BaselineStartDate,
            DATEADD(DAY, -1, p.[Start Date]) AS BaselineEndDate
        INTO #PromotionWindow
        FROM Dimension.[Promotion] AS p
        WHERE (@PromotionKey IS NULL OR p.[Promotion Key] = @PromotionKey)
          AND p.[Start Date] IS NOT NULL
          AND p.[End Date] IS NOT NULL;

        INSERT INTO Aggregate.[Promotion Effectiveness]
        (
            [Promotion Key], [Product Category Key], [Sales Channel Key], [Region Code],
            [Promotion Code], [Promotion Start Date], [Promotion End Date],
            [Baseline Start Date], [Baseline End Date], [Eligible Customer Count],
            [Participating Customer Count], [Take Up Rate Percent],
            [Eligible Not Purchased Count], [New Customer Count], [Reactivated Customer Count],
            [Promoted Units Sold], [Baseline Revenue Reporting], [Promotion Revenue Reporting],
            [Incremental Revenue Reporting], [Discount Cost Reporting],
            [Promotion Margin Reporting], [Baseline Margin Reporting],
            [Incremental Margin Reporting], [Cannibalised Revenue], [Return Rate Percent],
            [Loyalty Points Issued], [Roi Percent], [Payback Achieved Flag],
            [Consent Restricted Flag], [Refresh Batch Id], [Refreshed Datetime]
        )
        SELECT
            w.[Promotion Key],
            ISNULL(item.[Product Category Key], 0),
            s.[Sales Channel Key],
            s.[Region Code],
            w.[Promotion Code],
            w.PromotionStartDate,
            w.PromotionEndDate,
            w.BaselineStartDate,
            w.BaselineEndDate,
            ISNULL(elig.EligibleCount, 0),
            COUNT(DISTINCT s.[Customer Key]),
            CASE WHEN ISNULL(elig.EligibleCount, 0) = 0 THEN NULL
                 ELSE ROUND(100.0 * COUNT(DISTINCT s.[Customer Key])
                            / elig.EligibleCount, 2) END,
            ISNULL(elig.EligibleCount, 0) - COUNT(DISTINCT s.[Customer Key]),
            0, 0,
            SUM(s.[Quantity Base UOM]),
            ISNULL(base.BaselineRevenue, 0),
            SUM(s.[Net Amount Reporting]),
            SUM(s.[Net Amount Reporting]) - ISNULL(base.BaselineRevenue, 0),
            SUM(s.[Line Discount Amount] * s.[FX Rate To Reporting]),
            SUM(s.[Gross Margin Amount] * s.[FX Rate To Reporting]),
            ISNULL(base.BaselineMargin, 0),
            SUM(s.[Gross Margin Amount] * s.[FX Rate To Reporting]) - ISNULL(base.BaselineMargin, 0),
            0, NULL,
            ISNULL(pts.PointsIssued, 0),
            NULL, 0,
            CASE WHEN s.[Region Code] = N'EU' THEN 1 ELSE 0 END,
            @BatchId, SYSDATETIME()
        FROM #PromotionWindow AS w
        INNER JOIN Fact.[Sale] AS s
            ON s.[Promotion Key] = w.[Promotion Key]
           AND s.[Invoice Date Key] BETWEEN w.PromotionStartDate AND w.PromotionEndDate
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Key] = s.[Stock Item Key]
        OUTER APPLY
        (
            SELECT COUNT(DISTINCT e.[Customer Key]) AS EligibleCount
            FROM Fact.[Promotion Eligibility] AS e
            WHERE e.[Promotion Key] = w.[Promotion Key]
              AND ISNULL(e.[Opted Out Flag], 0) = 0
        ) AS elig
        OUTER APPLY
        (
            SELECT SUM(b.[Net Amount Reporting]) AS BaselineRevenue,
                   SUM(b.[Gross Margin Amount] * b.[FX Rate To Reporting]) AS BaselineMargin
            FROM Fact.[Sale] AS b
            WHERE b.[Stock Item Key] = s.[Stock Item Key]
              AND b.[Sales Channel Key] = s.[Sales Channel Key]
              AND b.[Invoice Date Key] BETWEEN w.BaselineStartDate AND w.BaselineEndDate
        ) AS base
        OUTER APPLY
        (
            SELECT SUM(l.[Points Delta]) AS PointsIssued
            FROM Fact.[Loyalty Points] AS l
            WHERE l.[Promotion Key] = w.[Promotion Key]
              AND l.[Points Delta] > 0
        ) AS pts
        GROUP BY
            w.[Promotion Key], w.[Promotion Code], w.PromotionStartDate,
            w.PromotionEndDate, w.BaselineStartDate, w.BaselineEndDate,
            item.[Product Category Key], s.[Sales Channel Key], s.[Region Code],
            elig.EligibleCount, base.BaselineRevenue, base.BaselineMargin,
            pts.PointsIssued;

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* ROI on incremental margin against discount given. Payback is a flat
           100% threshold; marketing has asked for a hurdle rate three times. */
        UPDATE Aggregate.[Promotion Effectiveness]
        SET [Roi Percent] = CASE WHEN ISNULL([Discount Cost Reporting], 0) = 0 THEN NULL
                                ELSE ROUND(100.0 * [Incremental Margin Reporting]
                                           / [Discount Cost Reporting], 2) END
        WHERE (@PromotionKey IS NULL OR [Promotion Key] = @PromotionKey);

        UPDATE Aggregate.[Promotion Effectiveness]
        SET [Payback Achieved Flag] = CASE WHEN ISNULL([Roi Percent], 0) >= 100 THEN 1 ELSE 0 END
        WHERE (@PromotionKey IS NULL OR [Promotion Key] = @PromotionKey);

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Aggregate.Promotion Effectiveness',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @DeleteRowCount     = @DeleteRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsDeleted        = @DeleteRowCount;

        DROP TABLE #PromotionWindow;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Aggregate.Promotion Effectiveness',
            @SourceComponent    = N'Aggregate refresh',
            @ProcedureName      = N'Integration.usp_RefreshAggregatePromotionEffectiveness',
            @ErrorDescription   = @ErrorMessage;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Failed';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
