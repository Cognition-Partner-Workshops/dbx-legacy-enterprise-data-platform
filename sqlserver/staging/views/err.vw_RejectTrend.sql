/*
    err.vw_RejectTrend

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Read by       : the data quality scorecard and the weekly steward worklist

    Daily reject volume per object and reason with a seven-day trailing average
    and a simple spike flag. Built on err.vw_RejectSummaryByBatch so there is one
    definition of "what counts as a reject"; the batch dates come from
    etl.Batch in the control database schema, which is deployed alongside this
    one and is read-only from here.
*/

IF OBJECT_ID(N'err.vw_RejectTrend', N'V') IS NOT NULL
    DROP VIEW err.vw_RejectTrend;
GO

CREATE VIEW err.vw_RejectTrend
AS
WITH DailyRejects AS
(
    SELECT
        CONVERT(DATE, s.FirstRejectedAtUtc)     AS RejectDate,
        s.SourceObjectName,
        s.RejectReasonCode,
        s.SourceSystemCode,
        SUM(s.RejectRowCount)                   AS RejectRowCount,
        SUM(s.PendingRowCount)                  AS PendingRowCount,
        COUNT(DISTINCT s.BatchId)               AS BatchCount
    FROM err.vw_RejectSummaryByBatch AS s
    GROUP BY
        CONVERT(DATE, s.FirstRejectedAtUtc),
        s.SourceObjectName,
        s.RejectReasonCode,
        s.SourceSystemCode
)
SELECT
    d.RejectDate,
    d.SourceObjectName,
    d.RejectReasonCode,
    d.SourceSystemCode,
    d.BatchCount,
    d.RejectRowCount,
    d.PendingRowCount,
    AVG(d.RejectRowCount * 1.0) OVER
    (
        PARTITION BY d.SourceObjectName, d.RejectReasonCode, d.SourceSystemCode
        ORDER BY d.RejectDate
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    )                                           AS TrailingSevenDayAverage,
    d.RejectRowCount - LAG(d.RejectRowCount, 1) OVER
    (
        PARTITION BY d.SourceObjectName, d.RejectReasonCode, d.SourceSystemCode
        ORDER BY d.RejectDate
    )                                           AS DayOverDayChange,
    CASE
        WHEN AVG(d.RejectRowCount * 1.0) OVER
             (
                 PARTITION BY d.SourceObjectName, d.RejectReasonCode, d.SourceSystemCode
                 ORDER BY d.RejectDate
                 ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
             ) IS NULL THEN N'NEW_REASON'
        WHEN d.RejectRowCount > 3 * AVG(d.RejectRowCount * 1.0) OVER
             (
                 PARTITION BY d.SourceObjectName, d.RejectReasonCode, d.SourceSystemCode
                 ORDER BY d.RejectDate
                 ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
             ) AND d.RejectRowCount >= 25 THEN N'SPIKE'
        ELSE N'NORMAL'
    END                                         AS TrendStatusCode
FROM DailyRejects AS d;
GO
