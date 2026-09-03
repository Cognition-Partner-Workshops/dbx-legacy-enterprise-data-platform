/*
    err.vw_RejectSummaryByBatch

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Read by       : the morning ETL exception report and the data quality
                    scorecard extract

    One row per batch / object / reject reason across every err.* table. The
    per-object reject tables were added at different times and do not share a
    column list, so each branch of the UNION ALL projects its own columns onto the
    common shape. Adding a new reject table means adding a branch here - that has
    been true since 2009 and nobody has been given time to generalise it.
*/

IF OBJECT_ID(N'err.vw_RejectSummaryByBatch', N'V') IS NOT NULL
    DROP VIEW err.vw_RejectSummaryByBatch;
GO

CREATE VIEW err.vw_RejectSummaryByBatch
AS
WITH AllRejects AS
(
    SELECT BatchId, PackageExecutionId, N'err.RejectedCustomer'   AS RejectTableName, N'stg.Customer'      AS SourceObjectName, RejectReasonCode, RejectStage, SourceSystemCode, ReprocessStatusCode, RejectedAtUtc FROM err.RejectedCustomer
    UNION ALL
    SELECT BatchId, PackageExecutionId, N'err.RejectedSupplier',   N'stg.Supplier',       RejectReasonCode, RejectStage, SourceSystemCode, ReprocessStatusCode, RejectedAtUtc FROM err.RejectedSupplier
    UNION ALL
    SELECT BatchId, PackageExecutionId, N'err.RejectedProduct',    N'stg.Product',        RejectReasonCode, RejectStage, SourceSystemCode, ReprocessStatusCode, RejectedAtUtc FROM err.RejectedProduct
    UNION ALL
    SELECT BatchId, PackageExecutionId, N'err.RejectedOrderLine',  N'stg.OrderLine',      RejectReasonCode, RejectStage, SourceSystemCode, ReprocessStatusCode, RejectedAtUtc FROM err.RejectedOrderLine
    UNION ALL
    SELECT BatchId, PackageExecutionId, N'err.RejectedInvoiceLine', N'stg.ApInvoiceLine', RejectReasonCode, RejectStage, SourceSystemCode, ReprocessStatusCode, RejectedAtUtc FROM err.RejectedInvoiceLine
    UNION ALL
    SELECT BatchId, PackageExecutionId, N'err.RejectedPayment',    N'stg.Payment',        RejectReasonCode, RejectStage, SourceSystemCode, ReprocessStatusCode, RejectedAtUtc FROM err.RejectedPayment
    UNION ALL
    SELECT BatchId, PackageExecutionId, N'err.RejectedShipment',   N'stg.Shipment',       RejectReasonCode, RejectStage, SourceSystemCode, ReprocessStatusCode, RejectedAtUtc FROM err.RejectedShipment
    UNION ALL
    SELECT BatchId, PackageExecutionId, N'err.RejectedFileRow',    SourceFileName,        RejectReasonCode, RejectStage, SourceSystemCode, ReprocessStatusCode, RejectedAtUtc FROM err.RejectedFileRow
    UNION ALL
    SELECT BatchId, PackageExecutionId, N'err.RejectedLookupFailure', SourceObjectName,   RejectReasonCode, RejectStage, SourceSystemCode, ReprocessStatusCode, RejectedAtUtc FROM err.RejectedLookupFailure
    UNION ALL
    SELECT BatchId, PackageExecutionId, N'err.RejectedConstraintViolation', TargetObjectName, RejectReasonCode, RejectStage, CONVERT(NVARCHAR(20), NULL), ReprocessStatusCode, RejectedAtUtc FROM err.RejectedConstraintViolation
)
SELECT
    r.BatchId,
    r.RejectTableName,
    ISNULL(r.SourceObjectName, N'(unknown)')    AS SourceObjectName,
    r.RejectReasonCode,
    ISNULL(r.RejectStage, N'Stage')             AS RejectStage,
    ISNULL(r.SourceSystemCode, N'(unknown)')    AS SourceSystemCode,
    COUNT_BIG(*)                                AS RejectRowCount,
    SUM(CASE WHEN r.ReprocessStatusCode IN (N'NEW', N'PENDING') THEN 1 ELSE 0 END) AS PendingRowCount,
    SUM(CASE WHEN r.ReprocessStatusCode = N'REPROCESSED' THEN 1 ELSE 0 END) AS ReprocessedRowCount,
    SUM(CASE WHEN r.ReprocessStatusCode = N'WRITTEN_OFF' THEN 1 ELSE 0 END) AS WrittenOffRowCount,
    COUNT(DISTINCT r.PackageExecutionId)        AS PackageExecutionCount,
    MIN(r.RejectedAtUtc)                        AS FirstRejectedAtUtc,
    MAX(r.RejectedAtUtc)                        AS LastRejectedAtUtc
FROM AllRejects AS r
GROUP BY
    r.BatchId,
    r.RejectTableName,
    ISNULL(r.SourceObjectName, N'(unknown)'),
    r.RejectReasonCode,
    ISNULL(r.RejectStage, N'Stage'),
    ISNULL(r.SourceSystemCode, N'(unknown)');
GO
