/*
    ref.usp_LoadReasonCode

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_ReturnReason (SSIS)
    Reads         : ref.CodeCrosswalk, raw.SqlReturnLine, raw.SqlCreditNote
    Writes        : ref.ReasonCode, err.RejectedLookupFailure
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The conformed reason set, one list per domain. Fault attribution is the part
    the business cares about: IsCustomerFault and IsSupplierFault drive the
    returns recovery reporting and the supplier scorecard, and they are
    deliberately nullable - a reason can be neither party's fault (a carrier
    loss) and the difference between "nobody's fault" and "not yet decided" is
    one the stewards refuse to give up.

    RequiresApproval marks the reasons a credit cannot be raised against without
    a manager, which is a control the OLTP database never enforced and the
    warehouse has always had to report on.
*/

IF OBJECT_ID(N'ref.usp_LoadReasonCode', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadReasonCode;
GO

CREATE PROCEDURE ref.usp_LoadReasonCode
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP',
    @ReasonDomainCode   NVARCHAR(30) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'ref.ReasonCode';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @UpdatedRows  BIGINT = 0;
    DECLARE @LookupMisses BIGINT = 0;
    DECLARE @MergeAction TABLE (ActionName NVARCHAR(10) NOT NULL);

    BEGIN TRY
        SELECT *
        INTO #ConformedReason
        FROM
        (
            VALUES
                --  RETURN
                (N'RETURN',     N'DAMAGED',    N'Damaged in transit',        N'QUALITY',   CONVERT(BIT, 0), CONVERT(BIT, 0), 0),
                (N'RETURN',     N'DEFECTIVE',  N'Defective on arrival',      N'QUALITY',   CONVERT(BIT, 0), CONVERT(BIT, 1), 0),
                (N'RETURN',     N'WRONGITEM',  N'Wrong item shipped',        N'FULFILMENT', CONVERT(BIT, 0), CONVERT(BIT, 0), 0),
                (N'RETURN',     N'OVERSHIP',   N'Over-shipment',             N'FULFILMENT', CONVERT(BIT, 0), CONVERT(BIT, 0), 0),
                (N'RETURN',     N'NOTNEEDED',  N'No longer required',        N'COMMERCIAL', CONVERT(BIT, 1), CONVERT(BIT, 0), 0),
                (N'RETURN',     N'LATE',       N'Delivered too late',        N'FULFILMENT', CONVERT(BIT, 0), CONVERT(BIT, 0), 0),
                (N'RETURN',     N'EXPIRED',    N'Past shelf life on arrival', N'QUALITY',   CONVERT(BIT, 0), CONVERT(BIT, 1), 1),
                (N'RETURN',     N'UNKNOWN',    N'Unknown return reason',     N'EXCEPTION', CONVERT(BIT, NULL), CONVERT(BIT, NULL), 1),
                --  CREDIT
                (N'CREDIT',     N'PRICEERR',   N'Pricing error',             N'BILLING',   CONVERT(BIT, 0), CONVERT(BIT, 0), 1),
                (N'CREDIT',     N'GOODWILL',   N'Goodwill gesture',          N'COMMERCIAL', CONVERT(BIT, 0), CONVERT(BIT, 0), 1),
                (N'CREDIT',     N'RETURNCR',   N'Credit against a return',   N'RETURNS',   CONVERT(BIT, NULL), CONVERT(BIT, NULL), 0),
                (N'CREDIT',     N'SHORTSHIP',  N'Short shipment',            N'FULFILMENT', CONVERT(BIT, 0), CONVERT(BIT, 0), 0),
                (N'CREDIT',     N'TAXCORR',    N'Tax correction',            N'BILLING',   CONVERT(BIT, 0), CONVERT(BIT, 0), 1),
                (N'CREDIT',     N'UNKNOWN',    N'Unknown credit reason',     N'EXCEPTION', CONVERT(BIT, NULL), CONVERT(BIT, NULL), 1),
                --  HOLD
                (N'HOLD',       N'CREDIT',     N'Credit limit exceeded',     N'FINANCE',   CONVERT(BIT, 1), CONVERT(BIT, 0), 1),
                (N'HOLD',       N'PRICE',      N'Price awaiting approval',   N'COMMERCIAL', CONVERT(BIT, 0), CONVERT(BIT, 0), 1),
                (N'HOLD',       N'QUALITY',    N'Quality inspection',        N'QUALITY',   CONVERT(BIT, 0), CONVERT(BIT, 1), 0),
                (N'HOLD',       N'COMPLIANCE', N'Compliance review',         N'COMPLIANCE', CONVERT(BIT, NULL), CONVERT(BIT, NULL), 1),
                (N'HOLD',       N'UNKNOWN',    N'Unknown hold reason',       N'EXCEPTION', CONVERT(BIT, NULL), CONVERT(BIT, NULL), 1),
                --  ADJUSTMENT
                (N'ADJUSTMENT', N'CYCLECOUNT', N'Cycle count correction',    N'INVENTORY', CONVERT(BIT, 0), CONVERT(BIT, 0), 0),
                (N'ADJUSTMENT', N'SHRINKAGE',  N'Shrinkage',                 N'INVENTORY', CONVERT(BIT, 0), CONVERT(BIT, 0), 1),
                (N'ADJUSTMENT', N'DAMAGE',     N'Damaged in the warehouse',  N'INVENTORY', CONVERT(BIT, 0), CONVERT(BIT, 0), 0),
                (N'ADJUSTMENT', N'REVALUE',    N'Stock revaluation',         N'FINANCE',   CONVERT(BIT, 0), CONVERT(BIT, 0), 1),
                (N'ADJUSTMENT', N'UNKNOWN',    N'Unknown adjustment reason', N'EXCEPTION', CONVERT(BIT, NULL), CONVERT(BIT, NULL), 1),
                --  REJECT (goods rejected at receipt)
                (N'REJECT',     N'QUALITY',    N'Failed goods-in inspection', N'QUALITY',  CONVERT(BIT, 0), CONVERT(BIT, 1), 0),
                (N'REJECT',     N'QUANTITY',   N'Quantity does not match the order', N'FULFILMENT', CONVERT(BIT, 0), CONVERT(BIT, 1), 0),
                (N'REJECT',     N'DOCUMENT',   N'Paperwork missing or wrong', N'COMPLIANCE', CONVERT(BIT, 0), CONVERT(BIT, 1), 0),
                (N'REJECT',     N'UNKNOWN',    N'Unknown rejection reason',  N'EXCEPTION', CONVERT(BIT, NULL), CONVERT(BIT, NULL), 1)
        ) AS v (ReasonDomainCode, ConformedReasonCode, ConformedReasonName, ReasonGroupCode,
                IsCustomerFault, IsSupplierFault, RequiresApproval);

        SELECT @SourceRows = COUNT_BIG(*)
        FROM #ConformedReason AS r
        WHERE @ReasonDomainCode IS NULL
           OR r.ReasonDomainCode = @ReasonDomainCode;

        BEGIN TRANSACTION;

        MERGE ref.ReasonCode AS tgt
        USING
        (
            SELECT *
            FROM #ConformedReason AS r
            WHERE @ReasonDomainCode IS NULL
               OR r.ReasonDomainCode = @ReasonDomainCode
        ) AS src
            ON  tgt.ReasonDomainCode    = src.ReasonDomainCode
            AND tgt.ConformedReasonCode = src.ConformedReasonCode
        WHEN MATCHED THEN
            UPDATE SET
                tgt.ConformedReasonName = src.ConformedReasonName,
                tgt.ReasonGroupCode     = src.ReasonGroupCode,
                tgt.IsCustomerFault     = src.IsCustomerFault,
                tgt.IsSupplierFault     = src.IsSupplierFault,
                tgt.RequiresApproval    = src.RequiresApproval,
                tgt.IsActive            = 1
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                ReasonDomainCode, ConformedReasonCode, ConformedReasonName, ReasonGroupCode,
                IsCustomerFault, IsSupplierFault, RequiresApproval, IsActive
            )
            VALUES
            (
                src.ReasonDomainCode, src.ConformedReasonCode, src.ConformedReasonName,
                src.ReasonGroupCode, src.IsCustomerFault, src.IsSupplierFault,
                src.RequiresApproval, 1
            )
        OUTPUT $action INTO @MergeAction (ActionName);

        SELECT
            @InsertedRows = COUNT_BIG(CASE WHEN a.ActionName = N'INSERT' THEN 1 END),
            @UpdatedRows  = COUNT_BIG(CASE WHEN a.ActionName = N'UPDATE' THEN 1 END)
        FROM @MergeAction AS a;

        --  Return reasons the OLTP database is sending that the crosswalk does
        --  not recognise. The returns still load against UNKNOWN.
        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, QueuedForLateArrival, OccurrenceCount, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'raw.SqlReturnLine', NULL, N'ReasonCode',
            N'ReturnReasonCode', UPPER(LTRIM(RTRIM(r.ReturnReasonCode))), @SourceSystemCode,
            N'LOOKUP_MISS',
            N'return reason has no active ref.CodeCrosswalk row in domain RETURN',
            N'Reference', 1, 0, COUNT_BIG(*), NULL
        FROM raw.SqlReturnLine AS r
        WHERE r.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(r.ReturnReasonCode)), N'') IS NOT NULL
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM ref.CodeCrosswalk AS x
                  WHERE x.CodeDomainCode   = N'RETURN'
                    AND x.SourceSystemCode = @SourceSystemCode
                    AND x.SourceCodeValue  = UPPER(LTRIM(RTRIM(r.ReturnReasonCode)))
                    AND x.EffectiveToDate IS NULL
              )
        GROUP BY UPPER(LTRIM(RTRIM(r.ReturnReasonCode)));

        SET @LookupMisses = @@ROWCOUNT;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows + @UpdatedRows,
            @InsertRowCount     = @InsertedRows,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = @LookupMisses;

        DROP TABLE #ConformedReason;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'REF_Load_ReturnReason',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'ref.usp_LoadReasonCode';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
