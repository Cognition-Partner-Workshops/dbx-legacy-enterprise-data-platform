/*
    Integration.usp_RefreshAggregateCustomer360

    Object        : Integration.usp_RefreshAggregateCustomer360
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.Customer Summaries.
    Called by     : AGG_Refresh_Customer_360 (nightly).
    Reads         : Fact.Sale, Fact.Payment, Fact.Return, Fact.Loyalty Points,
                    Fact.Web Session, Fact.Monthly Customer Balance.
    Depends on    : the etl control procedures.

    Rebuilds both customer summaries: the wide Customer 360 row and the
    twelve-month rolling history behind it. They are refreshed together
    because the churn score reads the rolling table and the rolling table is
    only ever read by the churn score.

    Privacy divergence, all of it hand-coded here:
      NA   - retains everything; opt-out only suppresses marketing flags.
      EU   - contact details are blanked once the retention date passes and
             the row is marked anonymised; revenue history is kept because it
             is needed for statutory accounts.
      APAC - retention is shorter and consent must be explicit, so a customer
             with no recorded consent is treated as opted out.
*/
IF OBJECT_ID(N'Integration.usp_RefreshAggregateCustomer360', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RefreshAggregateCustomer360;
GO

CREATE PROCEDURE Integration.usp_RefreshAggregateCustomer360
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @RebuildRolling     BIT = 1,
    @RollingMonths      INT = 12
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @UpdateRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;
    DECLARE @Today          DATE = CONVERT(DATE, SYSDATETIME());
    DECLARE @RollingFrom    DATE;

    SET @RollingFrom = DATEFROMPARTS(YEAR(DATEADD(MONTH, -@RollingMonths, @Today)),
                                     MONTH(DATEADD(MONTH, -@RollingMonths, @Today)), 1);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'AGG_Refresh_Customer_360',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'RefreshAggregateCustomer360',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        TRUNCATE TABLE Aggregate.[Customer 360];

        INSERT INTO Aggregate.[Customer 360]
        (
            [Customer Key], [Customer Segment Key], [Sales Territory Key], [Loyalty Tier Key],
            [Primary Sales Channel Key], [Region Code], [Customer Name], [Primary Contact Email],
            [Account Manager Employee Key], [First Order Date], [Last Order Date],
            [Last Payment Date], [Tenure Months], [Lifetime Order Count], [Lifetime Net Revenue],
            [Lifetime Gross Margin], [Lifetime Returns Amount], [Average Order Value],
            [Average Days To Pay], [Current Balance Reporting], [Overdue Balance Reporting],
            [Credit Limit Reporting], [Credit Utilisation Percent], [Loyalty Point Balance],
            [Web Session Count 90 Day], [Days Since Last Order], [Churn Risk Score],
            [Churn Risk Band], [Rfm Score], [Marketing Consent Flag], [Retention Expiry Date],
            [Anonymised Flag], [Refresh Batch Id], [Refreshed Datetime]
        )
        SELECT
            c.[Customer Key],
            ISNULL(sales.SegmentKey, -1),
            ISNULL(sales.TerritoryKey, -1),
            ISNULL(loyalty.TierKey, -1),
            ISNULL(sales.ChannelKey, -1),
            c.[Region Code],
            c.[Customer],
            c.[Primary Contact Email],
            ISNULL(c.[Account Manager Employee Key], -1),
            sales.FirstOrderDate,
            sales.LastOrderDate,
            pay.LastPaymentDate,
            DATEDIFF(MONTH, sales.FirstOrderDate, @Today),
            ISNULL(sales.OrderCount, 0),
            ISNULL(sales.NetRevenue, 0),
            ISNULL(sales.GrossMargin, 0),
            ISNULL(ret.ReturnsAmount, 0),
            CASE WHEN ISNULL(sales.OrderCount, 0) = 0 THEN 0
                 ELSE ROUND(sales.NetRevenue / sales.OrderCount, 2) END,
            pay.AverageDaysToPay,
            ISNULL(bal.ClosingBalanceReporting, 0),
            ISNULL(bal.OverdueBalanceReporting, 0),
            c.[Credit Limit Amount],
            CASE WHEN ISNULL(c.[Credit Limit Amount], 0) = 0 THEN NULL
                 ELSE ROUND(100.0 * ISNULL(bal.ClosingBalanceReporting, 0)
                            / c.[Credit Limit Amount], 2) END,
            ISNULL(loyalty.PointBalance, 0),
            ISNULL(web.SessionCount, 0),
            DATEDIFF(DAY, sales.LastOrderDate, @Today),
            NULL, NULL, NULL,
            /* APAC requires opt-in; the absence of a consent record is a no. */
            CASE c.[Region Code]
                WHEN N'APAC' THEN CASE WHEN c.[Marketing Consent Flag] = 1 THEN 1 ELSE 0 END
                WHEN N'EU'   THEN CASE WHEN ISNULL(c.[Marketing Consent Flag], 0) = 1
                                            AND c.[Erasure Requested On] IS NULL
                                       THEN 1 ELSE 0 END
                ELSE CASE WHEN ISNULL(c.[Marketing Consent Flag], 1) = 0 THEN 0 ELSE 1 END
            END,
            CASE c.[Region Code]
                WHEN N'EU'   THEN DATEADD(YEAR, 7, ISNULL(sales.LastOrderDate, @Today))
                WHEN N'APAC' THEN DATEADD(YEAR, 3, ISNULL(sales.LastOrderDate, @Today))
                ELSE DATEADD(YEAR, 10, ISNULL(sales.LastOrderDate, @Today))
            END,
            0,
            @BatchId, SYSDATETIME()
        FROM Dimension.[Customer] AS c
        OUTER APPLY
        (
            SELECT MIN(s.[Invoice Date Key]) AS FirstOrderDate,
                   MAX(s.[Invoice Date Key]) AS LastOrderDate,
                   COUNT(DISTINCT s.[Order Number]) AS OrderCount,
                   SUM(s.[Net Amount Reporting]) AS NetRevenue,
                   SUM(s.[Gross Margin Amount] * s.[FX Rate To Reporting]) AS GrossMargin,
                   MAX(s.[Customer Segment Key]) AS SegmentKey,
                   MAX(s.[Sales Territory Key]) AS TerritoryKey,
                   MAX(s.[Sales Channel Key]) AS ChannelKey
            FROM Fact.[Sale] AS s
            WHERE s.[Customer Key] = c.[Customer Key]
              AND ISNULL(s.[Correction Type Code], N'ORIG') <> N'REV'
        ) AS sales
        OUTER APPLY
        (
            SELECT MAX(p.[Payment Date Key]) AS LastPaymentDate,
                   AVG(CONVERT(DECIMAL(18, 2), p.[Days To Pay])) AS AverageDaysToPay
            FROM Fact.[Payment] AS p
            WHERE p.[Customer Key] = c.[Customer Key]
        ) AS pay
        OUTER APPLY
        (
            SELECT SUM(ABS(r.[Net Credit Amount])) AS ReturnsAmount
            FROM Fact.[Return] AS r
            WHERE r.[Customer Key] = c.[Customer Key]
        ) AS ret
        OUTER APPLY
        (
            SELECT TOP (1) l.[Points Balance After] AS PointBalance,
                   l.[Loyalty Tier Key] AS TierKey
            FROM Fact.[Loyalty Points] AS l
            WHERE l.[Customer Key] = c.[Customer Key]
            ORDER BY l.[Movement Date Key] DESC, l.[Loyalty Points Key] DESC
        ) AS loyalty
        OUTER APPLY
        (
            SELECT COUNT_BIG(*) AS SessionCount
            FROM Fact.[Web Session] AS w
            WHERE w.[Customer Key] = c.[Customer Key]
              AND w.[Session Start Date Key] > DATEADD(DAY, -90, @Today)
        ) AS web
        OUTER APPLY
        (
            SELECT TOP (1) b.[Closing Balance Reporting] AS ClosingBalanceReporting,
                   b.[Overdue Balance Reporting] AS OverdueBalanceReporting
            FROM Fact.[Monthly Customer Balance] AS b
            WHERE b.[Customer Key] = c.[Customer Key]
            ORDER BY b.[Month End Date Key] DESC
        ) AS bal
        WHERE c.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31');

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* RFM and churn scoring. Recency / frequency / monetary are banded on
           thresholds that were set by a consultant in 2014. */
        UPDATE Aggregate.[Customer 360]
        SET [Rfm Score] =
                CONVERT(NVARCHAR(3),
                    CASE WHEN [Days Since Last Order] <= 30 THEN 3
                         WHEN [Days Since Last Order] <= 120 THEN 2 ELSE 1 END)
                + CONVERT(NVARCHAR(3),
                    CASE WHEN [Lifetime Order Count] >= 50 THEN 3
                         WHEN [Lifetime Order Count] >= 10 THEN 2 ELSE 1 END)
                + CONVERT(NVARCHAR(3),
                    CASE WHEN [Lifetime Net Revenue] >= 250000 THEN 3
                         WHEN [Lifetime Net Revenue] >= 25000 THEN 2 ELSE 1 END),
            [Churn Risk Score] =
                CASE WHEN [Days Since Last Order] IS NULL THEN 100
                     ELSE
                        CASE WHEN [Days Since Last Order] > 365 THEN 60
                             WHEN [Days Since Last Order] > 180 THEN 40
                             WHEN [Days Since Last Order] > 90 THEN 20 ELSE 0 END
                      + CASE WHEN ISNULL([Web Session Count 90 Day], 0) = 0 THEN 15 ELSE 0 END
                      + CASE WHEN ISNULL([Credit Utilisation Percent], 0) > 90 THEN 10 ELSE 0 END
                      + CASE WHEN ISNULL([Lifetime Returns Amount], 0)
                                  > 0.10 * NULLIF([Lifetime Net Revenue], 0) THEN 15 ELSE 0 END
                END;

        SET @UpdateRowCount = @@ROWCOUNT;

        UPDATE Aggregate.[Customer 360]
        SET [Churn Risk Band] = CASE WHEN [Churn Risk Score] >= 70 THEN N'HIGH'
                                   WHEN [Churn Risk Score] >= 40 THEN N'MED'
                                   ELSE N'LOW' END;

        /* Retention enforcement, EU and APAC only. */
        UPDATE Aggregate.[Customer 360]
        SET [Customer Name]         = N'REDACTED',
            [Primary Contact Email]  = NULL,
            [Marketing Consent Flag] = 0,
            [Anonymised Flag]       = 1
        WHERE [Region Code] IN (N'EU', N'APAC')
          AND [Retention Expiry Date] < @Today;

        IF @RebuildRolling = 1
        BEGIN
            DELETE FROM Aggregate.[Customer Rolling 12 Month]
            WHERE [Calendar Month] >= @RollingFrom;

            SET @DeleteRowCount = @@ROWCOUNT;

            INSERT INTO Aggregate.[Customer Rolling 12 Month]
            (
                [Customer Key], [Month Offset], [Calendar Month], [Region Code],
                [Order Count], [Net Revenue Reporting], [Gross Margin Reporting],
                [Returns Reporting], [Cash Received Reporting], [Distinct Product Count],
                [Loyalty Points Earned], [Loyalty Points Redeemed], [Web Session Count],
                [Rolling 12 Month Revenue], [Rolling 12 Month Margin], [Rolling 3 Month Revenue],
                [Revenue Trend Percent], [Inactive Month Flag], [Consecutive Inactive Months],
                [Refresh Batch Id], [Refreshed Datetime]
            )
            SELECT
                s.[Customer Key],
                DATEDIFF(MONTH, DATEFROMPARTS(YEAR(s.[Invoice Date Key]),
                                              MONTH(s.[Invoice Date Key]), 1), @Today),
                DATEFROMPARTS(YEAR(s.[Invoice Date Key]), MONTH(s.[Invoice Date Key]), 1),
                MAX(s.[Region Code]),
                COUNT(DISTINCT s.[Order Number]),
                SUM(s.[Net Amount Reporting]),
                SUM(s.[Gross Margin Amount] * s.[FX Rate To Reporting]),
                0, 0,
                COUNT(DISTINCT s.[Stock Item Key]),
                0, 0, 0,
                NULL, NULL, NULL, NULL,
                0, 0,
                @BatchId, SYSDATETIME()
            FROM Fact.[Sale] AS s
            WHERE s.[Invoice Date Key] >= @RollingFrom
              AND ISNULL(s.[Correction Type Code], N'ORIG') <> N'REV'
            GROUP BY s.[Customer Key],
                     DATEFROMPARTS(YEAR(s.[Invoice Date Key]), MONTH(s.[Invoice Date Key]), 1);

            SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT;

            UPDATE roll
            SET [Rolling 12 Month Revenue] = win.Revenue12,
                [Rolling 12 Month Margin]  = win.Margin12,
                [Rolling 3 Month Revenue]  = win3.Revenue3,
                [Revenue Trend Percent]   =
                    CASE WHEN ISNULL(win.Revenue12, 0) = 0 THEN NULL
                         ELSE ROUND(100.0 * (win3.Revenue3 * 4.0 - win.Revenue12)
                                    / win.Revenue12, 2) END,
                [Inactive Month Flag] = CASE WHEN roll.[Order Count] = 0 THEN 1 ELSE 0 END
            FROM Aggregate.[Customer Rolling 12 Month] AS roll
            CROSS APPLY
            (
                SELECT SUM(x.[Net Revenue Reporting]) AS Revenue12,
                       SUM(x.[Gross Margin Reporting]) AS Margin12
                FROM Aggregate.[Customer Rolling 12 Month] AS x
                WHERE x.[Customer Key] = roll.[Customer Key]
                  AND x.[Calendar Month] <= roll.[Calendar Month]
                  AND x.[Calendar Month] > DATEADD(MONTH, -12, roll.[Calendar Month])
            ) AS win
            CROSS APPLY
            (
                SELECT SUM(x3.[Net Revenue Reporting]) AS Revenue3
                FROM Aggregate.[Customer Rolling 12 Month] AS x3
                WHERE x3.[Customer Key] = roll.[Customer Key]
                  AND x3.[Calendar Month] <= roll.[Calendar Month]
                  AND x3.[Calendar Month] > DATEADD(MONTH, -3, roll.[Calendar Month])
            ) AS win3
            WHERE roll.[Calendar Month] >= @RollingFrom;
        END;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Aggregate.Customer 360',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @UpdateRowCount     = @UpdateRowCount,
            @DeleteRowCount     = @DeleteRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsUpdated        = @UpdateRowCount,
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
            @SourceName         = N'Aggregate.Customer 360',
            @SourceComponent    = N'Aggregate refresh',
            @ProcedureName      = N'Integration.usp_RefreshAggregateCustomer360',
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
