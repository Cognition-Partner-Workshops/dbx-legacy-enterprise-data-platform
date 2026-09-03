/*
    work.usp_BuildProductCrosswalk

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : WRK_BUILD_PRODUCT_XREF (SSIS), before STG_LOAD_PRODUCT
    Reads         : raw.OracleProductMaster, raw.SqlStockItem, raw.FileSupplierCatalog,
                    ref.SourceKeyCrosswalk
    Writes        : work.ProductCrosswalk, err.RejectedLookupFailure
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The ERP and the OLTP have never shared a product identifier. This builds the
    match between them for the batch, in four passes, best evidence first:

      Pass 1  MANUAL_XREF   the MDM-populated WWI_STOCK_ITEM_ID on the ERP row,
                            the merchandising-maintained ErpProductCode on the
                            OLTP row, or a ref.SourceKeyCrosswalk row keyed by
                            hand. Always wins; confidence 100.
      Pass 2  BARCODE       the supplier catalogue EAN for the ERP item against
                            the OLTP barcode. Confidence 95, but ambiguous where
                            one barcode hits several stock items (multipacks
                            share barcodes in the OLTP), and ambiguous rows are
                            left unresolved for the stewards.
      Pass 3  NAME          normalised name token overlap of 80 percent or more
                            within the same brand. Confidence is the overlap
                            percentage; this is the pass that produces the bad
                            matches everyone complains about.
      Pass 4  UNMATCHED     everything left. Written anyway so that the counts
                            reconcile and so the product load can report the
                            unmatched rate.

    Token overlap is computed with STRING_SPLIT over the normalised name; before
    2016 this was a scalar function with a WHILE loop, and the comment it left
    behind ("do not call this in a join") is still true of the replacement.
*/

IF OBJECT_ID(N'work.usp_BuildProductCrosswalk', N'P') IS NOT NULL
    DROP PROCEDURE work.usp_BuildProductCrosswalk;
GO

CREATE PROCEDURE work.usp_BuildProductCrosswalk
(
    @BatchId             BIGINT,
    @PackageExecutionId  BIGINT = NULL,
    @NameOverlapMinimum  DECIMAL(5,2) = 80.00
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'work.ProductCrosswalk';
    DECLARE @ErpRows       BIGINT = 0;
    DECLARE @MatchedRows   BIGINT = 0;
    DECLARE @AmbiguousRows BIGINT = 0;

    BEGIN TRY
        SELECT
            ErpProductCode        = LTRIM(RTRIM(r.PRODUCT_CD)),
            ErpProductBusinessKey = stg.ufn_SourceSystemKey(r.SourceSystemCode, r.PRODUCT_ID, 1),
            SourceSystemCode      = r.SourceSystemCode,
            MdmStockItemId        = TRY_CONVERT(INT, r.WWI_STOCK_ITEM_ID),
            Barcode               = cat.EanBarcode,
            NormalizedName        = stg.ufn_CleanString(LEFT(r.PRODUCT_DESC, 200), 1),
            BrandCode             = LTRIM(RTRIM(ISNULL(r.BRAND_CD, N'')))
        INTO #Erp
        FROM raw.OracleProductMaster AS r
        OUTER APPLY
        (
            SELECT TOP (1)
                EanBarcode = NULLIF(REPLACE(LTRIM(RTRIM(ISNULL(fc.EanBarcode, N''))), N' ', N''), N'')
            FROM raw.FileSupplierCatalog AS fc
            WHERE fc.BatchId          = @BatchId
              AND fc.SupplierItemCode = r.PRODUCT_CD
            ORDER BY fc.SourceRowNumber DESC
        ) AS cat
        WHERE r.BatchId = @BatchId;

        SELECT @ErpRows = COUNT_BIG(*) FROM #Erp;

        SELECT
            OltpStockItemId      = TRY_CONVERT(INT, s.StockItemID),
            StockItemBusinessKey = stg.ufn_SourceSystemKey(N'WWI_OLTP', s.StockItemID, 1),
            Barcode              = NULLIF(REPLACE(LTRIM(RTRIM(ISNULL(s.Barcode, N''))), N' ', N''), N''),
            NormalizedName       = stg.ufn_CleanString(s.StockItemName, 1),
            BrandCode            = LTRIM(RTRIM(ISNULL(s.Brand, N''))),
            ErpProductCode       = NULLIF(LTRIM(RTRIM(ISNULL(s.ErpProductCode, N''))), N'')
        INTO #Oltp
        FROM raw.SqlStockItem AS s
        WHERE s.BatchId = @BatchId;

        DELETE FROM work.ProductCrosswalk
        WHERE BatchId = @BatchId;

        --  Pass 1: hand-maintained cross-references.
        INSERT INTO work.ProductCrosswalk
        (
            BatchId, PackageExecutionId, ErpProductCode, ErpProductBusinessKey, OltpStockItemId,
            StockItemBusinessKey, Barcode, MatchMethodCode, MatchConfidence, NormalizedName,
            NameTokenOverlapPercent, IsAmbiguous, CandidateCount, ResolvedFlag, ReviewedByName
        )
        SELECT
            @BatchId, @PackageExecutionId, e.ErpProductCode, e.ErpProductBusinessKey,
            m.OltpStockItemId, m.StockItemBusinessKey, e.Barcode, N'MANUAL_XREF', 100.00,
            e.NormalizedName, NULL, 0, 1, 1, m.ReviewedByName
        FROM #Erp AS e
        CROSS APPLY
        (
            SELECT TOP (1) o.OltpStockItemId, o.StockItemBusinessKey, ReviewedByName = x.MaintainedByName
            FROM #Oltp AS o
            LEFT JOIN ref.SourceKeyCrosswalk AS x
                ON  x.EntityName       = N'Product'
                AND x.SourceSystemCode = e.SourceSystemCode
                AND x.SourceKeyValue   = e.ErpProductCode
                AND x.MatchMethodCode  = N'MANUAL'
                AND x.IsActive         = 1
            WHERE o.OltpStockItemId = e.MdmStockItemId
               OR o.ErpProductCode  = e.ErpProductCode
               OR x.ConformedBusinessKey = o.StockItemBusinessKey
            ORDER BY CASE WHEN x.ConformedBusinessKey IS NOT NULL THEN 0
                          WHEN o.ErpProductCode = e.ErpProductCode THEN 1
                          ELSE 2 END,
                     o.OltpStockItemId
        ) AS m;

        --  Pass 2: barcode.
        INSERT INTO work.ProductCrosswalk
        (
            BatchId, PackageExecutionId, ErpProductCode, ErpProductBusinessKey, OltpStockItemId,
            StockItemBusinessKey, Barcode, MatchMethodCode, MatchConfidence, NormalizedName,
            NameTokenOverlapPercent, IsAmbiguous, CandidateCount, ResolvedFlag
        )
        SELECT
            @BatchId, @PackageExecutionId, e.ErpProductCode, e.ErpProductBusinessKey,
            CASE WHEN b.CandidateCount = 1 THEN b.OltpStockItemId END,
            CASE WHEN b.CandidateCount = 1 THEN b.StockItemBusinessKey END,
            e.Barcode, N'BARCODE', 95.00, e.NormalizedName, NULL,
            CASE WHEN b.CandidateCount > 1 THEN 1 ELSE 0 END,
            b.CandidateCount,
            CASE WHEN b.CandidateCount = 1 THEN 1 ELSE 0 END
        FROM #Erp AS e
        CROSS APPLY
        (
            SELECT
                CandidateCount       = COUNT_BIG(*),
                OltpStockItemId      = MIN(o.OltpStockItemId),
                StockItemBusinessKey = MIN(o.StockItemBusinessKey)
            FROM #Oltp AS o
            WHERE o.Barcode = e.Barcode
              AND e.Barcode IS NOT NULL
        ) AS b
        WHERE b.CandidateCount > 0
          AND NOT EXISTS
              (
                  SELECT 1 FROM work.ProductCrosswalk AS p
                  WHERE p.BatchId               = @BatchId
                    AND p.ErpProductBusinessKey = e.ErpProductBusinessKey
              );

        --  Pass 3: normalised name token overlap inside the same category.
        INSERT INTO work.ProductCrosswalk
        (
            BatchId, PackageExecutionId, ErpProductCode, ErpProductBusinessKey, OltpStockItemId,
            StockItemBusinessKey, Barcode, MatchMethodCode, MatchConfidence, NormalizedName,
            NameTokenOverlapPercent, IsAmbiguous, CandidateCount, ResolvedFlag
        )
        SELECT
            @BatchId, @PackageExecutionId, e.ErpProductCode, e.ErpProductBusinessKey,
            n.OltpStockItemId, n.StockItemBusinessKey, e.Barcode, N'NAME',
            n.OverlapPercent, e.NormalizedName, n.OverlapPercent,
            CASE WHEN n.TieCount > 1 THEN 1 ELSE 0 END,
            n.TieCount,
            CASE WHEN n.TieCount = 1 THEN 1 ELSE 0 END
        FROM #Erp AS e
        CROSS APPLY
        (
            SELECT TOP (1)
                c.OltpStockItemId,
                c.StockItemBusinessKey,
                c.OverlapPercent,
                TieCount = COUNT(*) OVER ()
            FROM
            (
                SELECT
                    o.OltpStockItemId,
                    o.StockItemBusinessKey,
                    OverlapPercent = CONVERT(DECIMAL(5,2),
                        100.0 * ov.SharedTokens
                              / NULLIF(CONVERT(DECIMAL(9,2), et.TokenCount), 0))
                FROM #Oltp AS o
                CROSS APPLY (SELECT TokenCount = COUNT(*) FROM STRING_SPLIT(e.NormalizedName, N' ')) AS et
                CROSS APPLY
                (
                    SELECT SharedTokens = COUNT(*)
                    FROM STRING_SPLIT(e.NormalizedName, N' ') AS a
                    INNER JOIN STRING_SPLIT(o.NormalizedName, N' ') AS b2
                        ON b2.value = a.value
                    WHERE LEN(a.value) > 2
                ) AS ov
                WHERE e.NormalizedName IS NOT NULL
                  AND o.NormalizedName IS NOT NULL
                  AND o.BrandCode = e.BrandCode
            ) AS c
            WHERE c.OverlapPercent >= @NameOverlapMinimum
            ORDER BY c.OverlapPercent DESC, c.OltpStockItemId
        ) AS n
        WHERE NOT EXISTS
        (
            SELECT 1 FROM work.ProductCrosswalk AS p
            WHERE p.BatchId               = @BatchId
              AND p.ErpProductBusinessKey = e.ErpProductBusinessKey
        );

        --  Pass 4: whatever is left, recorded as unmatched.
        INSERT INTO work.ProductCrosswalk
        (
            BatchId, PackageExecutionId, ErpProductCode, ErpProductBusinessKey, Barcode,
            MatchMethodCode, MatchConfidence, NormalizedName, IsAmbiguous, CandidateCount, ResolvedFlag
        )
        SELECT
            @BatchId, @PackageExecutionId, e.ErpProductCode, e.ErpProductBusinessKey, e.Barcode,
            N'UNMATCHED', 0.00, e.NormalizedName, 0, 0, 0
        FROM #Erp AS e
        WHERE NOT EXISTS
        (
            SELECT 1 FROM work.ProductCrosswalk AS p
            WHERE p.BatchId               = @BatchId
              AND p.ErpProductBusinessKey = e.ErpProductBusinessKey
        );

        --  Partner catalog codes are attached afterwards; they are informational
        --  only and never drive the match.
        UPDATE p
        SET p.PartnerProductCode = f.PartnerProductCode
        FROM work.ProductCrosswalk AS p
        CROSS APPLY
        (
            SELECT TOP (1) PartnerProductCode = LEFT(LTRIM(RTRIM(fc.SupplierItemCode)), 60)
            FROM raw.FileSupplierCatalog AS fc
            WHERE fc.BatchId          = @BatchId
              AND fc.SupplierItemCode = p.ErpProductCode
            ORDER BY fc.SourceRowNumber
        ) AS f
        WHERE p.BatchId = @BatchId;

        SELECT
            @MatchedRows   = SUM(CASE WHEN p.ResolvedFlag = 1 THEN 1 ELSE 0 END),
            @AmbiguousRows = SUM(CASE WHEN p.IsAmbiguous  = 1 THEN 1 ELSE 0 END)
        FROM work.ProductCrosswalk AS p
        WHERE p.BatchId = @BatchId;

        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, QueuedForLateArrival, OccurrenceCount
        )
        SELECT
            @BatchId, @PackageExecutionId, N'raw.OracleProductMaster', p.ErpProductBusinessKey,
            N'work.ProductCrosswalk', N'ErpProductCode', p.ErpProductCode, N'ORA_ERP',
            CASE WHEN p.IsAmbiguous = 1 THEN N'AMBIGUOUS_MATCH' ELSE N'NO_OLTP_MATCH' END,
            CASE WHEN p.IsAmbiguous = 1
                 THEN N'more than one OLTP stock item matched this ERP product'
                 ELSE N'no OLTP stock item could be matched to this ERP product' END,
            N'Transform', 1, 0, 1
        FROM work.ProductCrosswalk AS p
        WHERE p.BatchId      = @BatchId
          AND p.ResolvedFlag = 0;

        DECLARE @RejectRowCountValue BIGINT = @ErpRows - @MatchedRows;
        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @ErpRows,
            @TargetRowCount     = @ErpRows,
            @InsertRowCount     = @MatchedRows,
            @RejectRowCount     = @RejectRowCountValue;
    END TRY
    BEGIN CATCH
        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'WRK_BUILD_PRODUCT_XREF',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'work.usp_BuildProductCrosswalk';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
