/*
    ref.usp_LoadStatusCode

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_CodeTranslation (SSIS)
    Reads         : ref.CodeCrosswalk
    Writes        : ref.StatusCode
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The conformed status set, one list per domain. Neither source system agrees
    with the other on status: Oracle uses one-letter codes on the customer and
    supplier masters and three-letter codes on documents, the OLTP database uses
    words. Both reach this set through ref.CodeCrosswalk, so this procedure only
    owns the conformed side - what the statuses are, which of them are terminal
    and what order a report shows them in.

    IsTerminalStatus is the column the fact loads actually depend on: an order in
    a terminal status is never re-opened, so the incremental loads stop chasing
    it. Getting it wrong on CANCELLED in 2014 is why the flag is asserted here
    rather than being derived from whatever the source last sent.

    Every domain carries an UNKNOWN member at sort order 999 so that
    stg.usp_TranslateSourceCodes always has somewhere to route an unmapped code.
*/

IF OBJECT_ID(N'ref.usp_LoadStatusCode', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadStatusCode;
GO

CREATE PROCEDURE ref.usp_LoadStatusCode
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @StatusDomainCode   NVARCHAR(30) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'ref.StatusCode';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @UpdatedRows  BIGINT = 0;
    DECLARE @MergeAction TABLE (ActionName NVARCHAR(10) NOT NULL);

    BEGIN TRY
        SELECT *
        INTO #ConformedStatus
        FROM
        (
            VALUES
                --  ORDER
                (N'ORDER',    N'DRAFT',      N'Draft',                 N'OPEN',      CONVERT(SMALLINT, 10),  0),
                (N'ORDER',    N'OPEN',       N'Open',                  N'OPEN',      CONVERT(SMALLINT, 20),  0),
                (N'ORDER',    N'BACKORDER',  N'Backordered',           N'OPEN',      CONVERT(SMALLINT, 30),  0),
                (N'ORDER',    N'PICKING',    N'Being picked',          N'OPEN',      CONVERT(SMALLINT, 40),  0),
                (N'ORDER',    N'HOLD',       N'On hold',               N'EXCEPTION', CONVERT(SMALLINT, 50),  0),
                (N'ORDER',    N'SHIPPED',    N'Shipped',               N'CLOSED',    CONVERT(SMALLINT, 60),  0),
                (N'ORDER',    N'INVOICED',   N'Invoiced',              N'CLOSED',    CONVERT(SMALLINT, 70),  1),
                (N'ORDER',    N'CANCELLED',  N'Cancelled',             N'CANCELLED', CONVERT(SMALLINT, 80),  1),
                (N'ORDER',    N'UNKNOWN',    N'Unknown order status',  N'EXCEPTION', CONVERT(SMALLINT, 999), 0),
                --  INVOICE
                (N'INVOICE',  N'DRAFT',      N'Draft',                 N'OPEN',      CONVERT(SMALLINT, 10),  0),
                (N'INVOICE',  N'ISSUED',     N'Issued',                N'OPEN',      CONVERT(SMALLINT, 20),  0),
                (N'INVOICE',  N'PARTPAID',   N'Partially paid',        N'OPEN',      CONVERT(SMALLINT, 30),  0),
                (N'INVOICE',  N'PAID',       N'Paid',                  N'CLOSED',    CONVERT(SMALLINT, 40),  1),
                (N'INVOICE',  N'CREDITED',   N'Credited',              N'CLOSED',    CONVERT(SMALLINT, 50),  1),
                (N'INVOICE',  N'DISPUTED',   N'Disputed',              N'EXCEPTION', CONVERT(SMALLINT, 60),  0),
                (N'INVOICE',  N'WRITEOFF',   N'Written off',           N'CLOSED',    CONVERT(SMALLINT, 70),  1),
                (N'INVOICE',  N'CANCELLED',  N'Cancelled',             N'CANCELLED', CONVERT(SMALLINT, 80),  1),
                (N'INVOICE',  N'UNKNOWN',    N'Unknown invoice status', N'EXCEPTION', CONVERT(SMALLINT, 999), 0),
                --  SHIPMENT
                (N'SHIPMENT', N'PLANNED',    N'Planned',               N'OPEN',      CONVERT(SMALLINT, 10),  0),
                (N'SHIPMENT', N'PICKED',     N'Picked',                N'OPEN',      CONVERT(SMALLINT, 20),  0),
                (N'SHIPMENT', N'INTRANSIT',  N'In transit',            N'OPEN',      CONVERT(SMALLINT, 30),  0),
                (N'SHIPMENT', N'DELIVERED',  N'Delivered',             N'CLOSED',    CONVERT(SMALLINT, 40),  1),
                (N'SHIPMENT', N'FAILED',     N'Delivery failed',       N'EXCEPTION', CONVERT(SMALLINT, 50),  0),
                (N'SHIPMENT', N'RETURNED',   N'Returned to sender',    N'CLOSED',    CONVERT(SMALLINT, 60),  1),
                (N'SHIPMENT', N'CANCELLED',  N'Cancelled',             N'CANCELLED', CONVERT(SMALLINT, 70),  1),
                (N'SHIPMENT', N'UNKNOWN',    N'Unknown shipment status', N'EXCEPTION', CONVERT(SMALLINT, 999), 0),
                --  PO
                (N'PO',       N'DRAFT',      N'Draft',                 N'OPEN',      CONVERT(SMALLINT, 10),  0),
                (N'PO',       N'APPROVED',   N'Approved',              N'OPEN',      CONVERT(SMALLINT, 20),  0),
                (N'PO',       N'PARTRECV',   N'Partially received',    N'OPEN',      CONVERT(SMALLINT, 30),  0),
                (N'PO',       N'RECEIVED',   N'Fully received',        N'CLOSED',    CONVERT(SMALLINT, 40),  0),
                (N'PO',       N'CLOSED',     N'Closed',                N'CLOSED',    CONVERT(SMALLINT, 50),  1),
                (N'PO',       N'CANCELLED',  N'Cancelled',             N'CANCELLED', CONVERT(SMALLINT, 60),  1),
                (N'PO',       N'UNKNOWN',    N'Unknown purchase order status', N'EXCEPTION', CONVERT(SMALLINT, 999), 0),
                --  SUPPLIER
                (N'SUPPLIER', N'ACTIVE',     N'Active',                N'OPEN',      CONVERT(SMALLINT, 10),  0),
                (N'SUPPLIER', N'HOLD',       N'On hold',               N'EXCEPTION', CONVERT(SMALLINT, 20),  0),
                (N'SUPPLIER', N'PENDING',    N'Pending approval',      N'OPEN',      CONVERT(SMALLINT, 30),  0),
                (N'SUPPLIER', N'INACTIVE',   N'Inactive',              N'CLOSED',    CONVERT(SMALLINT, 40),  0),
                (N'SUPPLIER', N'BLOCKED',    N'Blocked',               N'EXCEPTION', CONVERT(SMALLINT, 50),  1),
                (N'SUPPLIER', N'UNKNOWN',    N'Unknown supplier status', N'EXCEPTION', CONVERT(SMALLINT, 999), 0),
                --  CUSTOMER
                (N'CUSTOMER', N'ACTIVE',     N'Active',                N'OPEN',      CONVERT(SMALLINT, 10),  0),
                (N'CUSTOMER', N'HOLD',       N'Credit hold',           N'EXCEPTION', CONVERT(SMALLINT, 20),  0),
                (N'CUSTOMER', N'PROSPECT',   N'Prospect',              N'OPEN',      CONVERT(SMALLINT, 30),  0),
                (N'CUSTOMER', N'INACTIVE',   N'Inactive',              N'CLOSED',    CONVERT(SMALLINT, 40),  0),
                (N'CUSTOMER', N'CLOSED',     N'Account closed',        N'CLOSED',    CONVERT(SMALLINT, 50),  1),
                (N'CUSTOMER', N'UNKNOWN',    N'Unknown customer status', N'EXCEPTION', CONVERT(SMALLINT, 999), 0)
        ) AS v (StatusDomainCode, ConformedStatusCode, ConformedStatusName, StatusGroupCode,
                SortOrder, IsTerminalStatus);

        SELECT @SourceRows = COUNT_BIG(*)
        FROM #ConformedStatus AS s
        WHERE @StatusDomainCode IS NULL
           OR s.StatusDomainCode = @StatusDomainCode;

        BEGIN TRANSACTION;

        MERGE ref.StatusCode AS tgt
        USING
        (
            SELECT *
            FROM #ConformedStatus AS s
            WHERE @StatusDomainCode IS NULL
               OR s.StatusDomainCode = @StatusDomainCode
        ) AS src
            ON  tgt.StatusDomainCode    = src.StatusDomainCode
            AND tgt.ConformedStatusCode = src.ConformedStatusCode
        WHEN MATCHED THEN
            UPDATE SET
                tgt.ConformedStatusName = src.ConformedStatusName,
                tgt.StatusGroupCode     = src.StatusGroupCode,
                tgt.SortOrder           = src.SortOrder,
                tgt.IsTerminalStatus    = src.IsTerminalStatus,
                tgt.IsActive            = 1
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                StatusDomainCode, ConformedStatusCode, ConformedStatusName, StatusGroupCode,
                SortOrder, IsTerminalStatus, IsActive
            )
            VALUES
            (
                src.StatusDomainCode, src.ConformedStatusCode, src.ConformedStatusName,
                src.StatusGroupCode, src.SortOrder, src.IsTerminalStatus, 1
            )
        OUTPUT $action INTO @MergeAction (ActionName);

        SELECT
            @InsertedRows = COUNT_BIG(CASE WHEN a.ActionName = N'INSERT' THEN 1 END),
            @UpdatedRows  = COUNT_BIG(CASE WHEN a.ActionName = N'UPDATE' THEN 1 END)
        FROM @MergeAction AS a;

        --  A conformed status that no crosswalk row points at is retired rather
        --  than deleted; the dimensions still hold rows that reference it.
        UPDATE s
        SET s.IsActive = 0
        FROM ref.StatusCode AS s
        WHERE s.ConformedStatusCode <> N'UNKNOWN'
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM #ConformedStatus AS c
                  WHERE c.StatusDomainCode    = s.StatusDomainCode
                    AND c.ConformedStatusCode = s.ConformedStatusCode
              );

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows + @UpdatedRows,
            @InsertRowCount     = @InsertedRows,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = 0;

        DROP TABLE #ConformedStatus;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'REF_Load_CodeTranslation',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'ref.usp_LoadStatusCode';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
