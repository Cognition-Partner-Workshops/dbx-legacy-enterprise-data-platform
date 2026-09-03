/*
    Integration.usp_RefreshAggregateMarginAnalysis

    Object        : Integration.usp_RefreshAggregateMarginAnalysis
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.Monthly Summaries and Fact.Sales Margin.
    Called by     : AGG_Refresh_Margin_Analysis (monthly, day 2 of close).
    Reads         : Fact.Sales Margin, Fact.Sale, Fact.Purchase.
    Depends on    : the etl control procedures.

    Price/volume/mix/cost bridge at category x territory x channel grain. The
    four effects are computed against the prior period and do not add exactly
    to the margin movement - the residual is absorbed into the mix effect,
    which is the convention the FP&A pack has used since it was a spreadsheet.

    Cost basis differs by region: NA uses weighted average, EU uses FIFO and
    APAC uses standard cost with a purchase price variance. [Cost Basis Code]
    records which, because the three are not comparable and the group pack
    footnotes it every month.
*/
IF OBJECT_ID(N'Integration.usp_RefreshAggregateMarginAnalysis', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RefreshAggregateMarginAnalysis;
GO

CREATE PROCEDURE Integration.usp_RefreshAggregateMarginAnalysis
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @CalendarMonth      DATE = NULL
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
    DECLARE @PriorStart     DATE;

    SET @CalendarMonth = ISNULL(@CalendarMonth,
        DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, SYSDATETIME())),
                      MONTH(DATEADD(MONTH, -1, SYSDATETIME())), 1));
    SET @MonthStart = DATEFROMPARTS(YEAR(@CalendarMonth), MONTH(@CalendarMonth), 1);
    SET @MonthEnd   = EOMONTH(@MonthStart);
    SET @PriorStart = DATEADD(MONTH, -1, @MonthStart);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'AGG_Refresh_Margin_Analysis',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'RefreshAggregateMarginAnalysis',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Aggregate.[Monthly Margin Analysis]
        WHERE [Calendar Month] = @MonthStart;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Aggregate.[Monthly Margin Analysis]
        (
            [Fiscal Year], [Fiscal Period], [Calendar Month], [Product Category Key],
            [Sales Territory Key], [Sales Channel Key], [Region Code], [Cost Basis Code],
            [Quantity Sold Base UOM], [Net Revenue Reporting], [Cost Of Sales Reporting],
            [Standard Cost Reporting], [Purchase Price Variance], [Freight Cost Reporting],
            [Rebate Accrual Reporting], [Gross Margin Reporting], [Standard Margin Reporting],
            [Contribution Margin Reporting], [Margin Percent], [Standard Margin Percent],
            [Price Effect Amount], [Volume Effect Amount], [Mix Effect Amount],
            [Cost Effect Amount], [Negative Margin Line Count], [Prior Period Margin Percent],
            [Refresh Batch Id], [Refreshed Datetime]
        )
        SELECT
            MAX(CASE m.[Region Code]
                    WHEN N'EU'   THEN d.[Eu Fiscal Year]
                    WHEN N'APAC' THEN d.[Apac Fiscal Year]
                    ELSE d.[Fiscal Year] END),
            MAX(CASE m.[Region Code]
                    WHEN N'EU'   THEN d.[Eu Fiscal Period]
                    WHEN N'APAC' THEN d.[Apac Fiscal Period]
                    ELSE d.[Fiscal Month Number] END),
            @MonthStart,
            m.[Product Category Key],
            m.[Sales Territory Key],
            m.[Sales Channel Key],
            m.[Region Code],
            CASE m.[Region Code] WHEN N'NA' THEN N'WAVG'
                                 WHEN N'EU' THEN N'FIFO'
                                 ELSE N'STD' END,
            SUM(m.[Quantity Base UOM]),
            SUM(m.[Net Amount Reporting]),
            SUM((m.[Cost Of Sale Amount] * m.[FX Rate To Reporting])),
            SUM((m.[Standard Cost Amount] * m.[FX Rate To Reporting])),
            SUM((m.[Cost Of Sale Amount] * m.[FX Rate To Reporting]) - (m.[Standard Cost Amount] * m.[FX Rate To Reporting])),
            SUM(ISNULL(m.[Freight Cost Amount], 0)),
            SUM(ISNULL(m.[Rebate Accrual Amount], 0)),
            SUM(m.[Gross Margin Reporting]),
            SUM(m.[Net Amount Reporting] - (m.[Standard Cost Amount] * m.[FX Rate To Reporting])),
            SUM(m.[Gross Margin Reporting] - ISNULL(m.[Freight Cost Amount], 0)
                + ISNULL(m.[Rebate Accrual Amount], 0)),
            CASE WHEN SUM(m.[Net Amount Reporting]) = 0 THEN NULL
                 ELSE ROUND(100.0 * SUM(m.[Gross Margin Reporting])
                            / SUM(m.[Net Amount Reporting]), 2) END,
            CASE WHEN SUM(m.[Net Amount Reporting]) = 0 THEN NULL
                 ELSE ROUND(100.0 * SUM(m.[Net Amount Reporting] - (m.[Standard Cost Amount] * m.[FX Rate To Reporting]))
                            / SUM(m.[Net Amount Reporting]), 2) END,
            NULL, NULL, NULL, NULL,
            SUM(CASE WHEN m.[Gross Margin Reporting] < 0 THEN 1 ELSE 0 END),
            NULL,
            @BatchId, SYSDATETIME()
        FROM Fact.[Sales Margin] AS m
        INNER JOIN Dimension.[Date] AS d
            ON d.[Date] = m.[Invoice Date Key]
        WHERE m.[Invoice Date Key] BETWEEN @MonthStart AND @MonthEnd
        GROUP BY m.[Product Category Key], m.[Sales Territory Key],
                 m.[Sales Channel Key], m.[Region Code];

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* Price / volume / cost bridge against the prior period; the mix
           effect is the plug. */
        UPDATE agg
        SET [Prior Period Margin Percent] = prior.[Margin Percent],
            [Price Effect Amount] =
                ROUND((CASE WHEN agg.[Quantity Sold Base UOM] = 0 THEN 0
                            ELSE agg.[Net Revenue Reporting] / agg.[Quantity Sold Base UOM] END
                       - CASE WHEN ISNULL(prior.[Quantity Sold Base UOM], 0) = 0 THEN 0
                              ELSE prior.[Net Revenue Reporting] / prior.[Quantity Sold Base UOM] END)
                      * agg.[Quantity Sold Base UOM], 2),
            [Volume Effect Amount] =
                ROUND((agg.[Quantity Sold Base UOM] - ISNULL(prior.[Quantity Sold Base UOM], 0))
                      * CASE WHEN ISNULL(prior.[Quantity Sold Base UOM], 0) = 0 THEN 0
                             ELSE (prior.[Net Revenue Reporting] - prior.[Cost Of Sales Reporting])
                                  / prior.[Quantity Sold Base UOM] END, 2),
            [Cost Effect Amount] =
                ROUND((CASE WHEN ISNULL(prior.[Quantity Sold Base UOM], 0) = 0 THEN 0
                            ELSE prior.[Cost Of Sales Reporting] / prior.[Quantity Sold Base UOM] END
                       - CASE WHEN agg.[Quantity Sold Base UOM] = 0 THEN 0
                              ELSE agg.[Cost Of Sales Reporting] / agg.[Quantity Sold Base UOM] END)
                      * agg.[Quantity Sold Base UOM], 2)
        FROM Aggregate.[Monthly Margin Analysis] AS agg
        LEFT JOIN Aggregate.[Monthly Margin Analysis] AS prior
            ON prior.[Calendar Month] = @PriorStart
           AND prior.[Product Category Key] = agg.[Product Category Key]
           AND prior.[Sales Territory Key] = agg.[Sales Territory Key]
           AND prior.[Sales Channel Key] = agg.[Sales Channel Key]
        WHERE agg.[Calendar Month] = @MonthStart;

        UPDATE agg
        SET [Mix Effect Amount] =
                ROUND(agg.[Gross Margin Reporting] - ISNULL(prior.[Gross Margin Reporting], 0)
                      - ISNULL(agg.[Price Effect Amount], 0)
                      - ISNULL(agg.[Volume Effect Amount], 0)
                      - ISNULL(agg.[Cost Effect Amount], 0), 2)
        FROM Aggregate.[Monthly Margin Analysis] AS agg
        LEFT JOIN Aggregate.[Monthly Margin Analysis] AS prior
            ON prior.[Calendar Month] = @PriorStart
           AND prior.[Product Category Key] = agg.[Product Category Key]
           AND prior.[Sales Territory Key] = agg.[Sales Territory Key]
           AND prior.[Sales Channel Key] = agg.[Sales Channel Key]
        WHERE agg.[Calendar Month] = @MonthStart;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Aggregate.Monthly Margin Analysis',
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

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Aggregate.Monthly Margin Analysis',
            @SourceComponent    = N'Aggregate refresh',
            @ProcedureName      = N'Integration.usp_RefreshAggregateMarginAnalysis',
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
