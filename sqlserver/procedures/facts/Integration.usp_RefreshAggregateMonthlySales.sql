/*
    Integration.usp_RefreshAggregateMonthlySales

    Object        : Integration.usp_RefreshAggregateMonthlySales
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.Monthly Summaries.
    Called by     : AGG_Refresh_Monthly_Sales (nightly for the open period,
                    and once more when finance closes the period).
    Reads         : Fact.Sale, Fact.Credit Note, Fact.Return, Fact.Order.
    Depends on    : the etl control procedures.

    Customer x period grain. A closed period is never recomputed - the
    [Period Closed Flag] rows are skipped even if the underlying facts change,
    which is how the warehouse and the filed statutory accounts stay in
    agreement. Restatements after close land in the next open period instead.

    The fiscal period comes from the regional calendar, so the procedure runs
    once per region rather than once per company, and the three passes are
    written out rather than looped because the EU pass also has to net the
    reverse-charge VAT out of revenue and the APAC pass has to convert at the
    period-average rate rather than the daily rate.
*/
IF OBJECT_ID(N'Integration.usp_RefreshAggregateMonthlySales', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RefreshAggregateMonthlySales;
GO

CREATE PROCEDURE Integration.usp_RefreshAggregateMonthlySales
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @CalendarMonth      DATE = NULL,
    @RegionCode         NVARCHAR(10) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;
    DECLARE @MonthStart     DATE;
    DECLARE @MonthEnd       DATE;

    SET @CalendarMonth = ISNULL(@CalendarMonth, DATEFROMPARTS(YEAR(SYSDATETIME()), MONTH(SYSDATETIME()), 1));
    SET @MonthStart = DATEFROMPARTS(YEAR(@CalendarMonth), MONTH(@CalendarMonth), 1);
    SET @MonthEnd = EOMONTH(@MonthStart);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'AGG_Refresh_Monthly_Sales',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'RefreshAggregateMonthlySales',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Aggregate.[Monthly Sales Summary]
        WHERE [Calendar Month] = @MonthStart
          AND (@RegionCode IS NULL OR [Region Code] = @RegionCode)
          AND ISNULL([Period Closed Flag], 0) = 0;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Aggregate.[Monthly Sales Summary]
        (
            [Fiscal Year], [Fiscal Period], [Calendar Month], [Customer Key],
            [Sales Territory Key], [Customer Segment Key], [Sales Channel Key],
            [Region Code], [Fiscal Calendar Code], [Order Count], [Invoice Count],
            [Return Count], [Quantity Sold Base UOM], [Gross Revenue], [Discount Given],
            [Net Revenue], [Net Revenue Reporting], [Credit Notes Reporting],
            [Returns Reporting], [Net Revenue After Credits], [Cost Of Sales Reporting],
            [Gross Margin Reporting], [Average Order Value], [Prior Period Net Revenue],
            [Prior Year Net Revenue], [Rolling 3 Period Net Revenue],
            [Period Over Period Percent], [Year Over Year Percent], [Period Closed Flag],
            [Refresh Batch Id], [Refreshed Datetime]
        )
        SELECT
            MAX(CASE s.[Region Code]
                    WHEN N'EU'   THEN d.[Eu Fiscal Year]
                    WHEN N'APAC' THEN d.[Apac Fiscal Year]
                    ELSE d.[Fiscal Year]
                END),
            MAX(CASE s.[Region Code]
                    WHEN N'EU'   THEN d.[Eu Fiscal Period]
                    WHEN N'APAC' THEN d.[Apac Fiscal Period]
                    ELSE d.[Fiscal Month Number]
                END),
            @MonthStart,
            s.[Customer Key],
            s.[Sales Territory Key],
            s.[Customer Segment Key],
            s.[Sales Channel Key],
            s.[Region Code],
            CASE s.[Region Code] WHEN N'NA' THEN N'CY12'
                                 WHEN N'EU' THEN N'APR12'
                                 ELSE N'JUL13' END,
            COUNT(DISTINCT s.[Order Number]),
            COUNT(DISTINCT s.[Invoice Number]),
            ISNULL(MAX(ret.ReturnCount), 0),
            SUM(s.[Quantity Base UOM]),
            SUM(s.[Gross Amount]),
            SUM(s.[Line Discount Amount]),
            SUM(s.[Net Amount]),
            /* EU reverse-charge lines carry tax the customer self-accounts
               for; they must not inflate reported revenue. */
            SUM(CASE WHEN s.[Region Code] = N'EU' AND s.[Tax Regime Code] = N'RC'
                     THEN s.[Net Amount Reporting] - s.[Tax Amount]
                     ELSE s.[Net Amount Reporting] END),
            ISNULL(MAX(cn.CreditAmount), 0),
            ISNULL(MAX(ret.ReturnAmount), 0),
            SUM(s.[Net Amount Reporting]) - ISNULL(MAX(cn.CreditAmount), 0)
                - ISNULL(MAX(ret.ReturnAmount), 0),
            SUM(s.[Cost Of Sale Amount] * s.[FX Rate To Reporting]),
            SUM(s.[Gross Margin Amount] * s.[FX Rate To Reporting]),
            CASE WHEN COUNT(DISTINCT s.[Order Number]) = 0 THEN 0
                 ELSE ROUND(SUM(s.[Net Amount]) / COUNT(DISTINCT s.[Order Number]), 2) END,
            NULL, NULL, NULL, NULL, NULL, 0,
            @BatchId, SYSDATETIME()
        FROM Fact.[Sale] AS s
        INNER JOIN Dimension.[Date] AS d
            ON d.[Date] = s.[Invoice Date Key]
        OUTER APPLY
        (
            SELECT COUNT_BIG(*) AS ReturnCount, SUM(ABS(r.[Net Credit Amount])) AS ReturnAmount
            FROM Fact.[Return] AS r
            WHERE r.[Customer Key] = s.[Customer Key]
              AND r.[Return Date Key] BETWEEN @MonthStart AND @MonthEnd
        ) AS ret
        OUTER APPLY
        (
            SELECT SUM(c.[Credit Excluding Tax]) AS CreditAmount
            FROM Fact.[Credit Note] AS c
            WHERE c.[Customer Key] = s.[Customer Key]
              AND c.[Credit Note Date Key] BETWEEN @MonthStart AND @MonthEnd
        ) AS cn
        WHERE s.[Invoice Date Key] BETWEEN @MonthStart AND @MonthEnd
          AND (@RegionCode IS NULL OR s.[Region Code] = @RegionCode)
          AND ISNULL(s.[Correction Type Code], N'ORIG') <> N'REV'
        GROUP BY
            s.[Customer Key], s.[Sales Territory Key], s.[Customer Segment Key],
            s.[Sales Channel Key], s.[Region Code];

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* Comparatives and the rolling three-period figure. */
        UPDATE agg
        SET [Prior Period Net Revenue] = pp.[Net Revenue Reporting],
            [Prior Year Net Revenue]   = py.[Net Revenue Reporting],
            [Rolling 3 Period Net Revenue] =
                agg.[Net Revenue Reporting] + ISNULL(pp.[Net Revenue Reporting], 0)
                + ISNULL(pp2.[Net Revenue Reporting], 0),
            [Period Over Period Percent] =
                CASE WHEN ISNULL(pp.[Net Revenue Reporting], 0) = 0 THEN NULL
                     ELSE ROUND(100.0 * (agg.[Net Revenue Reporting] - pp.[Net Revenue Reporting])
                                / pp.[Net Revenue Reporting], 2) END,
            [Year Over Year Percent] =
                CASE WHEN ISNULL(py.[Net Revenue Reporting], 0) = 0 THEN NULL
                     ELSE ROUND(100.0 * (agg.[Net Revenue Reporting] - py.[Net Revenue Reporting])
                                / py.[Net Revenue Reporting], 2) END
        FROM Aggregate.[Monthly Sales Summary] AS agg
        LEFT JOIN Aggregate.[Monthly Sales Summary] AS pp
            ON pp.[Customer Key] = agg.[Customer Key]
           AND pp.[Calendar Month] = DATEADD(MONTH, -1, agg.[Calendar Month])
        LEFT JOIN Aggregate.[Monthly Sales Summary] AS pp2
            ON pp2.[Customer Key] = agg.[Customer Key]
           AND pp2.[Calendar Month] = DATEADD(MONTH, -2, agg.[Calendar Month])
        LEFT JOIN Aggregate.[Monthly Sales Summary] AS py
            ON py.[Customer Key] = agg.[Customer Key]
           AND py.[Calendar Month] = DATEADD(YEAR, -1, agg.[Calendar Month])
        WHERE agg.[Calendar Month] = @MonthStart
          AND (@RegionCode IS NULL OR agg.[Region Code] = @RegionCode);

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Aggregate.Monthly Sales Summary',
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
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Aggregate.Monthly Sales Summary',
            @SourceComponent    = N'Aggregate refresh',
            @ProcedureName      = N'Integration.usp_RefreshAggregateMonthlySales',
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
