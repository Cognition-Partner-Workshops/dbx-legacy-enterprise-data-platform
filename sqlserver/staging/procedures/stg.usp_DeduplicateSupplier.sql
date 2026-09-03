/*
    stg.usp_DeduplicateSupplier

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_Load_Supplier (SSIS), after stg.usp_NormalizeSupplier
    Reads         : stg.Supplier, stg.PurchaseOrder, stg.ApInvoice
    Writes        : work.SupplierDedup, stg.Supplier (survivorship columns)
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The supplier counterpart of stg.usp_DeduplicateCustomer, and it works the
    same way: candidates are grouped by match key, scored, and the winner is
    stamped back on stg.Supplier while the losers keep a pointer to it. The
    working set is kept in work.SupplierDedup so the master data team can see
    why a supplier lost, which they ask for at every quarter end.

    Supplier matching is harder than customer matching because the same vendor is
    often set up once per ledger. Three keys are tried in order - tax identifier,
    DUNS, then normalized name - and open transactions beat everything: a
    supplier with open purchase orders or open invoices always survives, because
    merging it away strands the documents.
*/

IF OBJECT_ID(N'stg.usp_DeduplicateSupplier', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_DeduplicateSupplier;
GO

CREATE PROCEDURE stg.usp_DeduplicateSupplier
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'work.SupplierDedup';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @CandidateRows BIGINT = 0;
    DECLARE @SurvivorRows BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM stg.Supplier AS s
        WHERE s.BatchId = @BatchId;

        DELETE FROM work.SupplierDedup
        WHERE BatchId = @BatchId;

        BEGIN TRANSACTION;

        INSERT INTO work.SupplierDedup
        (
            BatchId, PackageExecutionId, DuplicateGroupId, CandidateSupplierBusinessKey,
            SourceSystemCode, MatchKeyName, MatchKeyTaxIdentifier, MatchKeyDuns,
            MatchRuleCode, HasOpenPurchaseOrders, HasOpenInvoices, SurvivorshipScore,
            IsSelectedSurvivor, LosesToBusinessKey, DecisionNote
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            CONVERT(INT, DENSE_RANK() OVER (ORDER BY cand.MatchKey)),
            cand.SupplierBusinessKey,
            cand.SourceSystemCode,
            cand.NormalizedName,
            cand.TaxIdentifier,
            cand.DunsNumber,
            cand.MatchRuleCode,
            cand.HasOpenPurchaseOrders,
            cand.HasOpenInvoices,
            cand.SurvivorshipScore,
            0,
            NULL,
            CONCAT(N'Grouped on ', cand.MatchRuleCode, N'; score ', cand.SurvivorshipScore)
        FROM
        (
            SELECT
                s.SupplierBusinessKey,
                s.SourceSystemCode,
                NormalizedName = UPPER(ISNULL(s.SupplierNameStandardized, s.SupplierName)),
                TaxIdentifier  = NULLIF(UPPER(LTRIM(RTRIM(s.TaxIdentifier))), N''),
                DunsNumber     = NULLIF(LTRIM(RTRIM(s.DunsNumber)), N''),
                MatchRuleCode  = CASE
                                     WHEN NULLIF(LTRIM(RTRIM(s.TaxIdentifier)), N'') IS NOT NULL THEN N'TAX_ID'
                                     WHEN NULLIF(LTRIM(RTRIM(s.DunsNumber)), N'')    IS NOT NULL THEN N'DUNS'
                                     ELSE N'NAME'
                                 END,
                MatchKey       = COALESCE(
                                     NULLIF(UPPER(LTRIM(RTRIM(s.TaxIdentifier))), N''),
                                     NULLIF(LTRIM(RTRIM(s.DunsNumber)), N''),
                                     UPPER(ISNULL(s.SupplierNameStandardized, s.SupplierName))),
                HasOpenPurchaseOrders = CASE WHEN EXISTS (SELECT 1
                                                          FROM stg.PurchaseOrder AS po
                                                          WHERE po.SupplierBusinessKey = s.SupplierBusinessKey
                                                            AND po.BatchId             = @BatchId
                                                            AND po.IsCancelled         = 0
                                                            AND po.PurchaseOrderStatusCode <> N'CLOSED')
                                             THEN 1 ELSE 0 END,
                HasOpenInvoices       = CASE WHEN EXISTS (SELECT 1
                                                          FROM stg.ApInvoice AS ai
                                                          WHERE ai.SupplierBusinessKey = s.SupplierBusinessKey
                                                            AND ai.BatchId             = @BatchId
                                                            AND ISNULL(ai.OpenAmount, 0) <> 0)
                                             THEN 1 ELSE 0 END,
                SurvivorshipScore     =
                      CASE WHEN s.SupplierStatusCode = N'ACTIVE' THEN 40 ELSE 0 END
                    + CASE WHEN s.TaxIdentifier IS NOT NULL      THEN 20 ELSE 0 END
                    + CASE WHEN s.DunsNumber IS NOT NULL         THEN 15 ELSE 0 END
                    + CASE WHEN s.PaymentTermsCode IS NOT NULL   THEN 10 ELSE 0 END
                    + CASE WHEN s.SourceSystemCode = @SourceSystemCode THEN 10 ELSE 0 END
                    + CASE WHEN s.OnHoldFlag = 1                 THEN -25 ELSE 0 END
            FROM stg.Supplier AS s
            WHERE s.BatchId = @BatchId
        ) AS cand;

        SET @CandidateRows = @@ROWCOUNT;

        -- Open transactions outrank the attribute score; the tie-break after that is
        -- the score and then the earliest business key, which keeps reruns stable.
        WITH RankedCandidate AS
        (
            SELECT
                sd.WorkRowId,
                sd.DuplicateGroupId,
                sd.CandidateSupplierBusinessKey,
                CandidateRank = ROW_NUMBER() OVER
                (
                    PARTITION BY sd.DuplicateGroupId
                    ORDER BY     sd.HasOpenPurchaseOrders DESC,
                                 sd.HasOpenInvoices DESC,
                                 sd.SurvivorshipScore DESC,
                                 sd.CandidateSupplierBusinessKey
                )
            FROM work.SupplierDedup AS sd
            WHERE sd.BatchId = @BatchId
        )
        UPDATE sd
        SET sd.IsSelectedSurvivor = CASE WHEN rc.CandidateRank = 1 THEN 1 ELSE 0 END,
            sd.LosesToBusinessKey = CASE WHEN rc.CandidateRank = 1 THEN NULL
                                         ELSE winner.CandidateSupplierBusinessKey END
        FROM work.SupplierDedup AS sd
        INNER JOIN RankedCandidate AS rc
            ON rc.WorkRowId = sd.WorkRowId
        LEFT JOIN RankedCandidate AS winner
            ON  winner.DuplicateGroupId = rc.DuplicateGroupId
            AND winner.CandidateRank    = 1
        WHERE sd.BatchId = @BatchId;

        UPDATE s
        SET s.DuplicateGroupId = sd.DuplicateGroupId,
            s.IsSurvivorRow    = sd.IsSelectedSurvivor
        FROM stg.Supplier AS s
        INNER JOIN work.SupplierDedup AS sd
            ON  sd.CandidateSupplierBusinessKey = s.SupplierBusinessKey
            AND sd.BatchId                      = @BatchId
        WHERE s.BatchId = @BatchId;

        SELECT @SurvivorRows = COUNT_BIG(*)
        FROM work.SupplierDedup AS sd
        WHERE sd.BatchId           = @BatchId
          AND sd.IsSelectedSurvivor = 1;

        COMMIT TRANSACTION;

        DECLARE @RejectRowCountValue BIGINT = @CandidateRows - @SurvivorRows;
        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @CandidateRows,
            @InsertRowCount     = @CandidateRows,
            @RejectRowCount     = @RejectRowCountValue;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_Load_Supplier',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_DeduplicateSupplier';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
