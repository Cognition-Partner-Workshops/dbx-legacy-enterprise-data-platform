/*
    stg.usp_ConformLoyaltyPointsForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_LoyaltyPoints (SSIS)
    Reads         : raw.SqlLoyaltyLedger, stg.Customer, stg.Sale, ref.Region, ref.Currency
    Writes        : stg.LoyaltyPoints
    Control       : etl.usp_LogRowCount, etl.usp_LogRejectedRecordSet, etl.usp_LogError

    The loyalty ledger lands from the OLTP as raw.SqlLoyaltyLedger and is read
    here directly: stg.LoyaltyLedger is the typed member-ledger shape the
    membership reports use, while the fact package wants one event row with the
    qualifying spend attached, which is not the same grain of interest even
    though it is the same source rows.

    Points expiry is regional and always has been:

      * NA  - 24 months rolling from the earning event.
      * EU  - points do not expire while marketing consent stands; the row
              carries no expiry date and the rule code records why.
      * APAC- expire at the end of the fiscal year in which they were earned,
              which is 31 March for the AP01 ledger.

    Redemptions arrive with a negative points delta and no qualifying spend. They
    are kept, because the fact needs both sides to reconcile the balance.
*/

IF OBJECT_ID(N'stg.usp_ConformLoyaltyPointsForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformLoyaltyPointsForFact;
GO

CREATE PROCEDURE stg.usp_ConformLoyaltyPointsForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.LoyaltyPoints';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @RejectedRows BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.SqlLoyaltyLedger AS ll
        WHERE ll.BatchId = @BatchId;

        DELETE FROM stg.LoyaltyPoints
        WHERE BatchId = @BatchId;

        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, QueuedForLateArrival, RecordPayload
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            @ObjectName,
            ll.LoyaltyLedgerID,
            N'Customer',
            N'CustomerID',
            ll.CustomerID,
            @SourceSystemCode,
            CASE
                WHEN TRY_CONVERT(BIGINT, ll.LoyaltyLedgerID) IS NULL THEN N'BAD_KEY'
                WHEN stg.ufn_SafeDecimal(ll.PointsDelta, N'.') IS NULL THEN N'BAD_NUMBER'
                ELSE N'LOOKUP_MISS'
            END,
            N'Loyalty ledger entry could not be conformed to a customer, a key or a points value.',
            N'Transform',
            0,
            1,
            CONCAT(ll.LoyaltyLedgerID, N'|', ll.CustomerID, N'|', ll.PointsDelta, N'|', ll.EntryWhen)
        FROM raw.SqlLoyaltyLedger AS ll
        LEFT JOIN stg.Customer AS c
            ON  c.BatchId       = @BatchId
            AND c.OltpCustomerId = TRY_CONVERT(INT, ll.CustomerID)
            AND c.IsSurvivorRow  = 1
        WHERE ll.BatchId = @BatchId
          AND (
                  c.CustomerBusinessKey IS NULL
               OR TRY_CONVERT(BIGINT, ll.LoyaltyLedgerID) IS NULL
               OR stg.ufn_SafeDecimal(ll.PointsDelta, N'.') IS NULL
              );

        SET @RejectedRows = @@ROWCOUNT;

        BEGIN TRANSACTION;

        WITH RankedLedger AS
        (
            SELECT
                ll.LoyaltyLedgerID,
                ll.LoyaltyMemberID,
                ll.CustomerID,
                ll.ProgramCode,
                ll.TierCode,
                ll.EntryTypeCode,
                ll.PointsDelta,
                ll.PointsBalanceAfter,
                ll.SourceInvoiceID,
                ll.RedemptionReference,
                ll.EntryWhen,
                ll.ExpiryDate,
                ll.RegionCode,
                ll.LastEditedWhen,
                RowRank = ROW_NUMBER() OVER
                (
                    PARTITION BY ll.LoyaltyLedgerID
                    ORDER BY     stg.ufn_SafeDate(ll.LastEditedWhen, ISNULL(ll.RegionCode, N'NA')) DESC,
                                 ll.SourceRowNumber DESC
                )
            FROM raw.SqlLoyaltyLedger AS ll
            WHERE ll.BatchId = @BatchId
              AND TRY_CONVERT(BIGINT, ll.LoyaltyLedgerID) IS NOT NULL
              AND stg.ufn_SafeDecimal(ll.PointsDelta, N'.') IS NOT NULL
        )
        INSERT INTO stg.LoyaltyPoints
        (
            LoyaltyEventBusinessKey, SourceSystemCode, CustomerBusinessKey, LoyaltyMemberId,
            LoyaltyProgramCode, TierCode, EventTypeCode, EventDate, PointsQuantity,
            PointsBalanceAfter, QualifyingSpendAmount, TransactionCurrency,
            QualifyingSpendAmountUsd, SourceInvoiceNumber, RedemptionReference, ExpiryDate,
            ExpiryRuleCode, RegionCode, LastModifiedAt, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            TRY_CONVERT(BIGINT, rl.LoyaltyLedgerID),
            @SourceSystemCode,
            c.CustomerBusinessKey,
            NULLIF(LTRIM(RTRIM(rl.LoyaltyMemberID)), N''),
            UPPER(LTRIM(RTRIM(rl.ProgramCode))),
            UPPER(LTRIM(RTRIM(rl.TierCode))),
            UPPER(LTRIM(RTRIM(rl.EntryTypeCode))),
            CONVERT(DATE, stg.ufn_SafeDate(rl.EntryWhen, ISNULL(rl.RegionCode, N'NA'))),
            CONVERT(INT, stg.ufn_SafeDecimal(rl.PointsDelta, N'.')),
            CONVERT(INT, stg.ufn_SafeDecimal(rl.PointsBalanceAfter, N'.')),
            s.SaleNetAmount,
            LEFT(COALESCE(s.TransactionCurrencyCode, r.DefaultCurrencyCode, N'USD'), 3),
            s.SaleNetAmountUsd,
            NULLIF(LTRIM(RTRIM(rl.SourceInvoiceID)), N''),
            NULLIF(LTRIM(RTRIM(rl.RedemptionReference)), N''),
            CASE ISNULL(UPPER(LTRIM(RTRIM(rl.RegionCode))), N'NA')
                WHEN N'EU'   THEN NULL
                WHEN N'APAC' THEN DATEFROMPARTS(
                                      YEAR(DATEADD(MONTH, 9,
                                          stg.ufn_SafeDate(rl.EntryWhen, N'APAC'))), 3, 31)
                ELSE COALESCE(
                         CONVERT(DATE, stg.ufn_SafeDate(rl.ExpiryDate, N'NA')),
                         DATEADD(MONTH, 24, CONVERT(DATE, stg.ufn_SafeDate(rl.EntryWhen, N'NA'))))
            END,
            CASE ISNULL(UPPER(LTRIM(RTRIM(rl.RegionCode))), N'NA')
                WHEN N'EU'   THEN N'EU_NO_EXPIRY'
                WHEN N'APAC' THEN N'APAC_FY_END'
                ELSE              N'NA_ROLLING_24M'
            END,
            ISNULL(UPPER(LTRIM(RTRIM(rl.RegionCode))), N'NA'),
            CONVERT(DATETIME2(3),
                COALESCE(stg.ufn_SafeDate(rl.LastEditedWhen, ISNULL(rl.RegionCode, N'NA')),
                         stg.ufn_SafeDate(rl.EntryWhen, ISNULL(rl.RegionCode, N'NA')))),
            CASE
                WHEN stg.ufn_SafeDate(rl.EntryWhen, ISNULL(rl.RegionCode, N'NA')) IS NULL THEN N'FAIL'
                WHEN rl.SourceInvoiceID IS NOT NULL AND s.SaleBusinessKey IS NULL         THEN N'WARN'
                WHEN UPPER(LTRIM(RTRIM(rl.EntryTypeCode))) = N'REDEEM'
                     AND stg.ufn_SafeDecimal(rl.PointsDelta, N'.') > 0                   THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(rl.LoyaltyLedgerID, N'|', rl.PointsDelta, N'|', rl.EntryTypeCode, N'|',
                       rl.EntryWhen, N'|', rl.RedemptionReference)),
            @BatchId,
            @PackageExecutionId
        FROM RankedLedger AS rl
        INNER JOIN stg.Customer AS c
            ON  c.BatchId        = @BatchId
            AND c.OltpCustomerId = TRY_CONVERT(INT, rl.CustomerID)
            AND c.IsSurvivorRow  = 1
        LEFT JOIN ref.Region AS r
            ON r.RegionCode = ISNULL(UPPER(LTRIM(RTRIM(rl.RegionCode))), N'NA')
        LEFT JOIN stg.Sale AS s
            ON  s.BatchId        = @BatchId
            AND s.SourceInvoiceId = NULLIF(LTRIM(RTRIM(rl.SourceInvoiceID)), N'')
        WHERE rl.RowRank = 1;

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        IF @RejectedRows > 0
            EXEC etl.usp_LogRejectedRecordSet
                @ObjectName         = @ObjectName,
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @SourceSystemCode   = @SourceSystemCode,
                @RejectStage        = N'Transform',
                @RejectReasonCode   = N'LOOKUP_MISS',
                @SourceTable        = N'err.RejectedLookupFailure',
                @SourceFilter       = N'SourceObjectName = N''stg.LoyaltyPoints''';

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @RejectedRows;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'FACT_Load_LoyaltyPoints',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformLoyaltyPointsForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
