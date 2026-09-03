/*
    ref.usp_ReportUnmappedCodes

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_CodeTranslation (SSIS), and by the data-quality
                    packages in ssis/12_data_quality through ref.vw_UnmappedSourceCode
    Reads         : raw.OracleCustomerMaster, raw.OracleSupplierMaster,
                    raw.OraclePurchaseOrderHdr, raw.OracleApInvoiceHdr,
                    raw.OracleReceiptLine, raw.OracleProductMaster,
                    raw.OracleGeography, raw.SqlOrder, raw.SqlShipment,
                    raw.SqlReturnLine, raw.SqlCreditNote, raw.SqlStockMovement,
                    ref.CodeCrosswalk
    Writes        : err.RejectedLookupFailure (through etl.usp_LogRejectedRecord)
    Control       : etl.usp_LogRowCount, etl.usp_LogError, etl.usp_LogRejectedRecord

    The maintenance half of the crosswalk. Every code column either source
    system sends is scanned against ref.CodeCrosswalk for its domain, and any
    value with no active mapping is reported: once as a result set, for the
    package that called it, and once as a control-framework reject, so the
    stewards' unmapped list survives the package run.

    The scan list is hand-maintained. Adding a code column to a raw table does
    not add it here; somebody has to. That is a known weakness of the design and
    the reason the estate has a data-quality package that counts the domains.

    The row loop at the end is deliberate: etl.usp_LogRejectedRecord takes one
    record at a time and there has never been a set-based version of it.
*/

IF OBJECT_ID(N'ref.usp_ReportUnmappedCodes', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_ReportUnmappedCodes;
GO

CREATE PROCEDURE ref.usp_ReportUnmappedCodes
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @CodeDomainCode     NVARCHAR(30) = NULL,
    @LogRejects         BIT = 1
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'ref.CodeCrosswalk';
    DECLARE @UnmappedRows  BIGINT = 0;
    DECLARE @ObservedRows  BIGINT = 0;

    BEGIN TRY
        CREATE TABLE #ObservedCode
        (
            CodeDomainCode   NVARCHAR(30)  NOT NULL,
            SourceSystemCode NVARCHAR(20)  NOT NULL,
            SourceObjectName NVARCHAR(200) NOT NULL,
            SourceColumnName NVARCHAR(100) NOT NULL,
            SourceCodeValue  NVARCHAR(50)  NOT NULL,
            OccurrenceCount  BIGINT        NOT NULL
        );

        --  Oracle ERP ---------------------------------------------------------
        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'CUSTOMER', N'ORA_ERP', N'raw.OracleCustomerMaster', N'CUST_STATUS_CD',
               LEFT(UPPER(LTRIM(RTRIM(c.CUST_STATUS_CD))), 50), COUNT_BIG(*)
        FROM raw.OracleCustomerMaster AS c
        WHERE c.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(c.CUST_STATUS_CD)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(c.CUST_STATUS_CD))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'SUPPLIER', N'ORA_ERP', N'raw.OracleSupplierMaster', N'SUPP_STATUS_CD',
               LEFT(UPPER(LTRIM(RTRIM(s.SUPP_STATUS_CD))), 50), COUNT_BIG(*)
        FROM raw.OracleSupplierMaster AS s
        WHERE s.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(s.SUPP_STATUS_CD)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(s.SUPP_STATUS_CD))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'PO', N'ORA_ERP', N'raw.OraclePurchaseOrderHdr', N'PO_STATUS_CD',
               LEFT(UPPER(LTRIM(RTRIM(p.PO_STATUS_CD))), 50), COUNT_BIG(*)
        FROM raw.OraclePurchaseOrderHdr AS p
        WHERE p.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(p.PO_STATUS_CD)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(p.PO_STATUS_CD))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'INVOICE', N'ORA_ERP', N'raw.OracleApInvoiceHdr', N'INVOICE_STATUS_CD',
               LEFT(UPPER(LTRIM(RTRIM(i.INVOICE_STATUS_CD))), 50), COUNT_BIG(*)
        FROM raw.OracleApInvoiceHdr AS i
        WHERE i.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(i.INVOICE_STATUS_CD)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(i.INVOICE_STATUS_CD))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'HOLD', N'ORA_ERP', N'raw.OracleApInvoiceHdr', N'HOLD_REASON_CD',
               LEFT(UPPER(LTRIM(RTRIM(i.HOLD_REASON_CD))), 50), COUNT_BIG(*)
        FROM raw.OracleApInvoiceHdr AS i
        WHERE i.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(i.HOLD_REASON_CD)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(i.HOLD_REASON_CD))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'REJECT', N'ORA_ERP', N'raw.OracleReceiptLine', N'REJECT_REASON_CD',
               LEFT(UPPER(LTRIM(RTRIM(r.REJECT_REASON_CD))), 50), COUNT_BIG(*)
        FROM raw.OracleReceiptLine AS r
        WHERE r.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(r.REJECT_REASON_CD)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(r.REJECT_REASON_CD))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'UOM', N'ORA_ERP', N'raw.OracleProductMaster', N'BASE_UOM_CD',
               LEFT(UPPER(LTRIM(RTRIM(p.BASE_UOM_CD))), 50), COUNT_BIG(*)
        FROM raw.OracleProductMaster AS p
        WHERE p.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(p.BASE_UOM_CD)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(p.BASE_UOM_CD))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'REGION', N'ORA_ERP', N'raw.OracleGeography', N'REGION_CD',
               LEFT(UPPER(LTRIM(RTRIM(g.REGION_CD))), 50), COUNT_BIG(*)
        FROM raw.OracleGeography AS g
        WHERE g.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(g.REGION_CD)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(g.REGION_CD))), 50);

        --  WideWorldImporters OLTP ---------------------------------------------
        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'ORDER', N'WWI_OLTP', N'raw.SqlOrder', N'OrderStatusCode',
               LEFT(UPPER(LTRIM(RTRIM(o.OrderStatusCode))), 50), COUNT_BIG(*)
        FROM raw.SqlOrder AS o
        WHERE o.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(o.OrderStatusCode)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(o.OrderStatusCode))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'SHIPMENT', N'WWI_OLTP', N'raw.SqlShipment', N'ShipmentStatusCode',
               LEFT(UPPER(LTRIM(RTRIM(s.ShipmentStatusCode))), 50), COUNT_BIG(*)
        FROM raw.SqlShipment AS s
        WHERE s.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(s.ShipmentStatusCode)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(s.ShipmentStatusCode))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'RETURN', N'WWI_OLTP', N'raw.SqlReturnLine', N'ReturnReasonCode',
               LEFT(UPPER(LTRIM(RTRIM(r.ReturnReasonCode))), 50), COUNT_BIG(*)
        FROM raw.SqlReturnLine AS r
        WHERE r.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(r.ReturnReasonCode)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(r.ReturnReasonCode))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'CREDIT', N'WWI_OLTP', N'raw.SqlCreditNote', N'CreditReasonCode',
               LEFT(UPPER(LTRIM(RTRIM(c.CreditReasonCode))), 50), COUNT_BIG(*)
        FROM raw.SqlCreditNote AS c
        WHERE c.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(c.CreditReasonCode)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(c.CreditReasonCode))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'ADJUSTMENT', N'WWI_OLTP', N'raw.SqlStockMovement', N'MovementReasonCode',
               LEFT(UPPER(LTRIM(RTRIM(m.MovementReasonCode))), 50), COUNT_BIG(*)
        FROM raw.SqlStockMovement AS m
        WHERE m.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(m.MovementReasonCode)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(m.MovementReasonCode))), 50);

        INSERT INTO #ObservedCode
        (CodeDomainCode, SourceSystemCode, SourceObjectName, SourceColumnName, SourceCodeValue, OccurrenceCount)
        SELECT N'TRANSACTION_TYPE', N'WWI_OLTP', N'raw.SqlStockMovement', N'TransactionTypeName',
               LEFT(UPPER(LTRIM(RTRIM(m.TransactionTypeName))), 50), COUNT_BIG(*)
        FROM raw.SqlStockMovement AS m
        WHERE m.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(m.TransactionTypeName)), N'') IS NOT NULL
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(m.TransactionTypeName))), 50);

        SELECT @ObservedRows = COUNT_BIG(*) FROM #ObservedCode;

        SELECT
            o.CodeDomainCode,
            o.SourceSystemCode,
            o.SourceObjectName,
            o.SourceColumnName,
            o.SourceCodeValue,
            OccurrenceCount = SUM(o.OccurrenceCount),
            FirstObservedAtUtc = SYSUTCDATETIME()
        INTO #UnmappedCode
        FROM #ObservedCode AS o
        WHERE (@CodeDomainCode IS NULL OR o.CodeDomainCode = @CodeDomainCode)
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM ref.CodeCrosswalk AS x
                  WHERE x.CodeDomainCode   = o.CodeDomainCode
                    AND x.SourceSystemCode = o.SourceSystemCode
                    AND x.SourceCodeValue  = o.SourceCodeValue
                    AND x.EffectiveFromDate <= CONVERT(DATE, SYSUTCDATETIME())
                    AND (x.EffectiveToDate IS NULL OR x.EffectiveToDate >= CONVERT(DATE, SYSUTCDATETIME()))
              )
        GROUP BY o.CodeDomainCode, o.SourceSystemCode, o.SourceObjectName, o.SourceColumnName,
                 o.SourceCodeValue;

        SELECT @UnmappedRows = COUNT_BIG(*) FROM #UnmappedCode;

        IF @LogRejects = 1 AND @UnmappedRows > 0
        BEGIN
            DECLARE @Domain      NVARCHAR(30);
            DECLARE @System      NVARCHAR(20);
            DECLARE @Code        NVARCHAR(50);
            DECLARE @Object      NVARCHAR(200);
            DECLARE @Column      NVARCHAR(100);
            DECLARE @Occurrences BIGINT;

            DECLARE unmapped_cur CURSOR LOCAL FAST_FORWARD FOR
                SELECT u.CodeDomainCode, u.SourceSystemCode, u.SourceCodeValue,
                       u.SourceObjectName, u.SourceColumnName, u.OccurrenceCount
                FROM #UnmappedCode AS u;

            OPEN unmapped_cur;
            FETCH NEXT FROM unmapped_cur INTO @Domain, @System, @Code, @Object, @Column, @Occurrences;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                EXEC etl.usp_LogRejectedRecord
                    @PackageExecutionId = @PackageExecutionId,
                    @BatchId            = @BatchId,
                    @SourceSystemCode   = @System,
                    @ObjectName         = @ObjectName,
                    @BusinessKey        = @Code,
                    @RejectReasonCode   = N'REF_UNMAPPED_CODE',
                    @RejectReason       = N'source code has no active ref.CodeCrosswalk mapping',
                    @RejectStage        = N'Reference',
                    @RecordPayload      = NULL;

                INSERT INTO err.RejectedLookupFailure
                (
                    BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
                    LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
                    RejectStage, RoutedToUnknownMember, QueuedForLateArrival, OccurrenceCount,
                    RecordPayload
                )
                VALUES
                (
                    @BatchId, @PackageExecutionId, @Object, NULL, N'CodeCrosswalk',
                    @Column, @Code, @System, N'REF_UNMAPPED_CODE',
                    CONCAT(N'no active mapping in domain ', @Domain),
                    N'Reference', 1, 0, @Occurrences,
                    CONCAT(N'{"DOMAIN":"', @Domain, N'","CODE":"', @Code, N'"}')
                );

                FETCH NEXT FROM unmapped_cur INTO @Domain, @System, @Code, @Object, @Column, @Occurrences;
            END

            CLOSE unmapped_cur;
            DEALLOCATE unmapped_cur;
        END;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @ObservedRows,
            @TargetRowCount     = @UnmappedRows,
            @RejectRowCount     = @UnmappedRows;

        --  The result set the calling package reads into its own variables.
        SELECT
            u.CodeDomainCode,
            u.SourceSystemCode,
            u.SourceObjectName,
            u.SourceColumnName,
            u.SourceCodeValue,
            u.OccurrenceCount,
            u.FirstObservedAtUtc
        FROM #UnmappedCode AS u
        ORDER BY u.CodeDomainCode, u.SourceSystemCode, u.SourceCodeValue;

        DROP TABLE #ObservedCode;
        DROP TABLE #UnmappedCode;
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS(N'local', N'unmapped_cur') >= 0
        BEGIN
            CLOSE unmapped_cur;
            DEALLOCATE unmapped_cur;
        END;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'REF_Load_CodeTranslation',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'ref.usp_ReportUnmappedCodes';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
