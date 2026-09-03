/*
    Integration.usp_LoadFactSale

    Object        : Integration.usp_LoadFactSale
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Sale.Extensions, Fact.Fact Load Hold and the
                    dimension loads.
    Called by     : FACT_Load_Sale (nightly) and FACT_Load_Sale_Intraday.
    Reads         : stg.SalesInvoiceLine, stg.SalesInvoiceHeader,
                    stg.ExchangeRateDaily, stg.TaxRateRegional (synonyms onto
                    the staging database named by SQLSERVER_STAGING_DB).
    Depends on    : etl.usp_GetWatermark, etl.usp_SetWatermark,
                    etl.usp_LogPackageStart/End, etl.usp_LogRowCount,
                    etl.usp_LogRejectedRecord, etl.usp_LogError.

    Load pattern  : delete-by-window then insert. The window is the invoice
                    date range returned by the watermark, widened by the
                    configured back-dating allowance because branch offices
                    post invoices up to five days late.

    Correction    : Fact.Sale uses the REVERSAL pattern. A restated source
                    invoice line produces two rows - a negated copy of the
                    previously loaded row tagged 'REV' and the new values
                    tagged 'RES'. Nothing is updated in place, because the
                    monthly revenue pack is signed off off this fact and
                    finance require the audit trail. Contrast with
                    Integration.usp_LoadFactPayment, which restates in place.

    Regional tax  : NA lines carry combined state/county sales tax already
                    calculated in the source. EU lines carry VAT, and
                    zero-rated intra-community supplies are flagged for the
                    reverse charge. APAC lines carry GST with a GST-free
                    indicator for exempt categories. The three are calculated
                    in three separate UPDATE statements below; attempts to
                    merge them in 2014 and 2019 both had to be backed out.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactSale', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactSale;
GO

CREATE PROCEDURE Integration.usp_LoadFactSale
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @LoadStartDate      DATE = NULL,
    @LoadEndDate        DATE = NULL,
    @ReloadFullHistory  BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution      BIT = 0;
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @InsertRowCount     BIGINT = 0;
    DECLARE @DeleteRowCount     BIGINT = 0;
    DECLARE @RejectRowCount     BIGINT = 0;
    DECLARE @HoldRowCount       BIGINT = 0;
    DECLARE @WatermarkFrom      NVARCHAR(50);
    DECLARE @WatermarkTo        NVARCHAR(50);
    DECLARE @BackDatingDays     INT = 5;
    DECLARE @LineageKey         INT = 0;

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Sale',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactSale',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode  = N'WWI_OLTP',
            @ObjectName        = N'Fact.Sale',
            @ReloadFullHistory = @ReloadFullHistory,
            @WatermarkFrom     = @WatermarkFrom OUTPUT,
            @WatermarkTo       = @WatermarkTo OUTPUT;

        SET @LoadStartDate = ISNULL(@LoadStartDate,
                                    DATEADD(DAY, -@BackDatingDays, TRY_CONVERT(DATE, @WatermarkFrom)));
        SET @LoadEndDate   = ISNULL(@LoadEndDate, TRY_CONVERT(DATE, @WatermarkTo));

        IF @LoadStartDate IS NULL SET @LoadStartDate = CONVERT(DATE, '2013-01-01');
        IF @LoadEndDate   IS NULL SET @LoadEndDate   = CONVERT(DATE, SYSDATETIME());

        /* ------------------------------------------------------------------
           1. Land the window into a work table and derive the natural key.
              The natural key hash is what deduplication and correction
              detection both key off.
           ------------------------------------------------------------------ */
        CREATE TABLE #SaleWork
        (
            [Invoice Number]        NVARCHAR(20)    NOT NULL,
            [Invoice Line Number]   INT             NOT NULL,
            [Order Number]          NVARCHAR(20)    NULL,
            [Invoice Date]          DATE            NOT NULL,
            [Delivery Date]         DATE            NULL,
            [Customer Business Key] NVARCHAR(50)    NULL,
            [Bill To Business Key]  NVARCHAR(50)    NULL,
            [Stock Item Code]       NVARCHAR(50)    NULL,
            [Salesperson Code]      NVARCHAR(20)    NULL,
            [City Code]             NVARCHAR(20)    NULL,
            [Channel Code]          NVARCHAR(10)    NULL,
            [Promotion Code]        NVARCHAR(20)    NULL,
            [Region Code]           NVARCHAR(4)     NOT NULL,
            [Transaction Currency]  NCHAR(3)        NULL,
            [Quantity Source UOM]   DECIMAL(18, 4)  NULL,
            [Source UOM Code]       NVARCHAR(10)    NULL,
            [UOM Conversion Factor] DECIMAL(18, 6)  NULL,
            [Unit Price]            DECIMAL(18, 4)  NULL,
            [Line Discount Amount]  DECIMAL(18, 2)  NULL,
            [Freight Amount]        DECIMAL(18, 2)  NULL,
            [Unit Cost]             DECIMAL(18, 4)  NULL,
            [Source Tax Amount]     DECIMAL(18, 2)  NULL,
            [Tax Rate]              DECIMAL(9, 4)   NULL,
            [Tax Regime Code]       NVARCHAR(10)    NULL,
            [Customer Vat Number]   NVARCHAR(20)    NULL,
            [Gst Free Flag]         BIT             NULL,
            [Source Row Version]    BIGINT          NULL,
            [Natural Key Hash]      VARBINARY(32)   NULL,
            [Customer Key]          INT             NULL,
            [Bill To Customer Key]  INT             NULL,
            [Stock Item Key]        INT             NULL,
            [Salesperson Key]       INT             NULL,
            [City Key]              INT             NULL,
            [Sales Channel Key]     INT             NULL,
            [Promotion Key]         INT             NULL,
            [Inferred Member Flag]  BIT             NOT NULL DEFAULT (0),
            [Fx Rate]               DECIMAL(19, 9)  NULL,
            [Fx Rate Date]          DATE            NULL,
            [Gross Amount]          DECIMAL(18, 2)  NULL,
            [Net Amount]            DECIMAL(18, 2)  NULL,
            [Tax Amount]            DECIMAL(18, 2)  NULL,
            [Cost Of Sale]          DECIMAL(18, 2)  NULL,
            [Gross Margin]          DECIMAL(18, 2)  NULL
        );

        INSERT INTO #SaleWork
        (
            [Invoice Number], [Invoice Line Number], [Order Number], [Invoice Date], [Delivery Date],
            [Customer Business Key], [Bill To Business Key], [Stock Item Code], [Salesperson Code],
            [City Code], [Channel Code], [Promotion Code], [Region Code], [Transaction Currency],
            [Quantity Source UOM], [Source UOM Code], [UOM Conversion Factor], [Unit Price],
            [Line Discount Amount], [Freight Amount], [Unit Cost], [Source Tax Amount], [Tax Rate],
            [Tax Regime Code], [Customer Vat Number], [Gst Free Flag], [Source Row Version]
        )
        SELECT
            hdr.InvoiceNumber,
            lin.InvoiceLineNumber,
            hdr.OrderNumber,
            hdr.InvoiceDate,
            hdr.DeliveryDate,
            hdr.CustomerBusinessKey,
            ISNULL(hdr.BillToBusinessKey, hdr.CustomerBusinessKey),
            lin.StockItemCode,
            hdr.SalespersonCode,
            hdr.DeliveryCityCode,
            ISNULL(hdr.SalesChannelCode, N'DIRECT'),
            lin.PromotionCode,
            hdr.RegionCode,
            ISNULL(hdr.CurrencyCode, N'USD'),
            lin.Quantity,
            ISNULL(lin.UnitOfMeasureCode, N'EA'),
            ISNULL(lin.UomConversionFactor, 1.0),
            lin.UnitPrice,
            ISNULL(lin.LineDiscountAmount, 0),
            ISNULL(lin.FreightAllocatedAmount, 0),
            lin.UnitCost,
            ISNULL(lin.TaxAmount, 0),
            ISNULL(lin.TaxRate, 0),
            hdr.TaxRegimeCode,
            hdr.CustomerVatNumber,
            lin.GstFreeFlag,
            lin.SourceRowVersion
        FROM stg.SalesInvoiceLine AS lin
        INNER JOIN stg.SalesInvoiceHeader AS hdr
            ON hdr.InvoiceNumber = lin.InvoiceNumber
        WHERE hdr.InvoiceDate >= @LoadStartDate
          AND hdr.InvoiceDate <= @LoadEndDate
          AND hdr.InvoiceStatusCode <> N'DRAFT';

        SET @SourceRowCount = @@ROWCOUNT;

        UPDATE #SaleWork
        SET [Natural Key Hash] = HASHBYTES('SHA2_256',
                CONCAT([Invoice Number], N'|', [Invoice Line Number], N'|', [Region Code]));

        /* ------------------------------------------------------------------
           2. Reject structurally unusable rows before they get anywhere near
              the fact. These are logged individually - the stewards work the
              reject queue every morning.
           ------------------------------------------------------------------ */
        DECLARE @RejectInvoice NVARCHAR(20);
        DECLARE @RejectLine    INT;
        DECLARE @RejectReason  NVARCHAR(500);

        DECLARE reject_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT [Invoice Number], [Invoice Line Number],
                   CASE
                       WHEN [Quantity Source UOM] IS NULL THEN N'Quantity is null'
                       WHEN [Unit Price] IS NULL THEN N'Unit price is null'
                       WHEN [Stock Item Code] IS NULL THEN N'Stock item code is null'
                       WHEN [Region Code] NOT IN (N'NA', N'EU', N'APAC') THEN N'Unmapped region code'
                       ELSE N'Unknown validation failure'
                   END
            FROM #SaleWork
            WHERE [Quantity Source UOM] IS NULL
               OR [Unit Price] IS NULL
               OR [Stock Item Code] IS NULL
               OR [Region Code] NOT IN (N'NA', N'EU', N'APAC');

        OPEN reject_cursor;
        FETCH NEXT FROM reject_cursor INTO @RejectInvoice, @RejectLine, @RejectReason;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'WWI_OLTP',
                @ObjectName         = N'Fact.Sale',
                @BusinessKey        = @RejectInvoice,
                @RejectReasonCode   = N'FACT_VALIDATION',
                @RejectReason       = @RejectReason,
                @RejectStage        = N'Fact';

            SET @RejectRowCount = @RejectRowCount + 1;

            FETCH NEXT FROM reject_cursor INTO @RejectInvoice, @RejectLine, @RejectReason;
        END;

        CLOSE reject_cursor;
        DEALLOCATE reject_cursor;

        DELETE FROM #SaleWork
        WHERE [Quantity Source UOM] IS NULL
           OR [Unit Price] IS NULL
           OR [Stock Item Code] IS NULL
           OR [Region Code] NOT IN (N'NA', N'EU', N'APAC');

        /* ------------------------------------------------------------------
           3. Deduplicate on the natural key. The web channel replays invoice
              extracts when a batch is retried, so the same line can arrive
              several times in one window. Highest source row version wins.
           ------------------------------------------------------------------ */
        ;WITH ranked AS
        (
            SELECT [Natural Key Hash],
                   ROW_NUMBER() OVER (PARTITION BY [Natural Key Hash]
                                      ORDER BY [Source Row Version] DESC) AS rn,
                   *
            FROM #SaleWork
        )
        DELETE FROM ranked WHERE rn > 1;

        /* ------------------------------------------------------------------
           4. Surrogate key lookups, with unknown-member fallback.
           ------------------------------------------------------------------ */
        UPDATE w
        SET [Customer Key]         = ISNULL(cust.[Customer Key], 0),
            [Bill To Customer Key] = ISNULL(bill.[Customer Key], 0),
            [Stock Item Key]       = ISNULL(item.[Stock Item Key], 0),
            [Salesperson Key]      = CASE WHEN w.[Salesperson Code] IS NULL THEN -1
                                          ELSE ISNULL(sp.[Salesperson Key], 0) END,
            [City Key]             = ISNULL(city.[City Key], 0),
            [Sales Channel Key]    = ISNULL(chan.[Sales Channel Key], 0),
            [Promotion Key]        = CASE WHEN w.[Promotion Code] IS NULL THEN -1
                                          ELSE ISNULL(promo.[Promotion Key], 0) END
        FROM #SaleWork AS w
        LEFT JOIN Dimension.[Customer] AS cust
            ON cust.[WWI Customer ID] = TRY_CONVERT(INT, w.[Customer Business Key])
           AND w.[Invoice Date] >= cust.[Valid From]
           AND w.[Invoice Date] <  cust.[Valid To]
        LEFT JOIN Dimension.[Customer] AS bill
            ON bill.[WWI Customer ID] = TRY_CONVERT(INT, w.[Bill To Business Key])
           AND w.[Invoice Date] >= bill.[Valid From]
           AND w.[Invoice Date] <  bill.[Valid To]
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Code] = w.[Stock Item Code]
           AND w.[Invoice Date] >= item.[Valid From]
           AND w.[Invoice Date] <  item.[Valid To]
        LEFT JOIN Dimension.[Salesperson] AS sp
            ON sp.[Salesperson Code] = w.[Salesperson Code]
           AND w.[Invoice Date] >= sp.[Valid From]
           AND w.[Invoice Date] <  sp.[Valid To]
        LEFT JOIN Dimension.[City] AS city
            ON city.[City Code] = w.[City Code]
           AND w.[Invoice Date] >= city.[Valid From]
           AND w.[Invoice Date] <  city.[Valid To]
        LEFT JOIN Dimension.[Sales Channel] AS chan
            ON chan.[Channel Code] = w.[Channel Code]
        LEFT JOIN Dimension.[Promotion] AS promo
            ON promo.[Promotion Code] = w.[Promotion Code]
           AND w.[Invoice Date] >= promo.[Valid From]
           AND w.[Invoice Date] <  promo.[Valid To];

        /* ------------------------------------------------------------------
           5. Early-arriving facts. A sale for a customer the MDM extract has
              not delivered yet gets an inferred dimension member so the
              revenue is not lost; the dimension load back-fills the attributes
              on the next run and clears [Is Inferred].
           ------------------------------------------------------------------ */
        INSERT INTO Dimension.[Customer] ([WWI Customer ID], [Customer], [Is Inferred],
                                          [Valid From], [Valid To], [Lineage Key])
        SELECT DISTINCT
            TRY_CONVERT(INT, w.[Customer Business Key]),
            N'*** INFERRED ' + w.[Customer Business Key],
            1,
            CONVERT(DATETIME2(7), '2013-01-01'),
            CONVERT(DATETIME2(7), '9999-12-31'),
            @LineageKey
        FROM #SaleWork AS w
        WHERE w.[Customer Key] = 0
          AND w.[Customer Business Key] IS NOT NULL
          AND TRY_CONVERT(INT, w.[Customer Business Key]) IS NOT NULL;

        UPDATE w
        SET [Customer Key]        = cust.[Customer Key],
            [Inferred Member Flag] = 1
        FROM #SaleWork AS w
        INNER JOIN Dimension.[Customer] AS cust
            ON cust.[WWI Customer ID] = TRY_CONVERT(INT, w.[Customer Business Key])
           AND cust.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        WHERE w.[Customer Key] = 0;

        /* ------------------------------------------------------------------
           6. Hold and retry. Rows whose stock item still cannot be keyed are
              parked rather than loaded against the unknown member, because a
              sale with no product corrupts margin reporting. The sweep in
              Integration.usp_RekeyLateArrivingDimensions releases them.
           ------------------------------------------------------------------ */
        INSERT INTO Fact.[Fact Load Hold]
        (
            [Target Fact Name], [Source System Code], [Region Code], [Natural Key Hash],
            [Natural Key Text], [Business Date], [Missing Dimension Name], [Missing Business Key],
            [Hold Reason Code], [Source Payload], [Retry Count], [Hold Status Code],
            [Batch Id], [Package Execution Id]
        )
        SELECT
            N'Fact.Sale',
            N'WWI_OLTP',
            w.[Region Code],
            w.[Natural Key Hash],
            CONCAT(w.[Invoice Number], N'|', w.[Invoice Line Number]),
            w.[Invoice Date],
            N'Dimension.Stock Item',
            w.[Stock Item Code],
            N'DIM_NOT_KEYED',
            (SELECT w2.* FROM #SaleWork AS w2
             WHERE w2.[Natural Key Hash] = w.[Natural Key Hash] FOR XML PATH('SaleLine')),
            0,
            N'HELD',
            @BatchId,
            @PackageExecutionId
        FROM #SaleWork AS w
        WHERE w.[Stock Item Key] = 0
          AND NOT EXISTS (SELECT 1 FROM Fact.[Fact Load Hold] AS h
                          WHERE h.[Natural Key Hash] = w.[Natural Key Hash]
                            AND h.[Target Fact Name] = N'Fact.Sale'
                            AND h.[Hold Status Code] = N'HELD');

        SET @HoldRowCount = @@ROWCOUNT;

        DELETE FROM #SaleWork WHERE [Stock Item Key] = 0;

        /* ------------------------------------------------------------------
           7. Effective-dated FX. The rate is the last published rate on or
              before the invoice date; APAC uses the rate published by the
              regional treasury feed, which is a day behind the group feed and
              is therefore looked up separately.
           ------------------------------------------------------------------ */
        UPDATE w
        SET [Fx Rate]      = fx.RateToReporting,
            [Fx Rate Date] = fx.RateDate
        FROM #SaleWork AS w
        OUTER APPLY
        (
            SELECT TOP (1) r.RateToReporting, r.RateDate
            FROM stg.ExchangeRateDaily AS r
            WHERE r.CurrencyCode = w.[Transaction Currency]
              AND r.RateSourceCode = CASE WHEN w.[Region Code] = N'APAC' THEN N'APAC_TREASURY'
                                          ELSE N'GROUP' END
              AND r.RateDate <= w.[Invoice Date]
            ORDER BY r.RateDate DESC
        ) AS fx;

        UPDATE #SaleWork
        SET [Fx Rate] = 1.0, [Fx Rate Date] = [Invoice Date]
        WHERE [Fx Rate] IS NULL;

        /* ------------------------------------------------------------------
           8. Measures. Gross, discount, net, cost and margin are computed the
              way the 2006 Access report computed them and have never been
              changed, including the quirk that freight is excluded from net
              but included in the invoice total.
           ------------------------------------------------------------------ */
        UPDATE #SaleWork
        SET [Gross Amount]  = ROUND([Quantity Source UOM] * [UOM Conversion Factor] * [Unit Price], 2),
            [Cost Of Sale]  = ROUND([Quantity Source UOM] * [UOM Conversion Factor] * ISNULL([Unit Cost], 0), 2);

        UPDATE #SaleWork
        SET [Net Amount]   = [Gross Amount] - ISNULL([Line Discount Amount], 0),
            [Gross Margin] = [Gross Amount] - ISNULL([Line Discount Amount], 0) - [Cost Of Sale];

        /* NA: sales tax is state + county, already combined by the OLTP tax
           engine. We keep the source amount rather than recomputing, because
           the county breakdown is not exposed to the warehouse. */
        UPDATE #SaleWork
        SET [Tax Amount] = [Source Tax Amount]
        WHERE [Region Code] = N'NA';

        /* EU: VAT is recomputed from the net so that rounding matches the
           statutory invoice. Intra-community supplies to a VAT-registered
           customer are zero-rated and carry the reverse charge. */
        UPDATE #SaleWork
        SET [Tax Amount] = CASE
                               WHEN [Customer Vat Number] IS NOT NULL AND [Tax Regime Code] = N'EU_RC' THEN 0
                               ELSE ROUND([Net Amount] * ISNULL([Tax Rate], 0) / 100.0, 2)
                           END
        WHERE [Region Code] = N'EU';

        /* APAC: GST at the item rate unless the line is GST-free. */
        UPDATE #SaleWork
        SET [Tax Amount] = CASE
                               WHEN ISNULL([Gst Free Flag], 0) = 1 THEN 0
                               ELSE ROUND([Net Amount] * ISNULL([Tax Rate], 0) / 100.0, 2)
                           END
        WHERE [Region Code] = N'APAC';

        /* ------------------------------------------------------------------
           9. Correction handling - reversal rows for lines already loaded with
              different values, then the delete-by-window for everything else.
           ------------------------------------------------------------------ */
        INSERT INTO Fact.[Sale]
        (
            [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key],
            [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID],
            [Description], [Package], [Quantity], [Unit Price], [Tax Rate],
            [Total Excluding Tax], [Tax Amount], [Profit], [Total Including Tax],
            [Total Dry Items], [Total Chiller Items], [Lineage Key],
            [Invoice Number], [Order Number], [Region Code], [Natural Key Hash],
            [Correction Type Code], [Correction Source Batch Id], [Batch Id], [Load Datetime]
        )
        SELECT
            f.[City Key], f.[Customer Key], f.[Bill To Customer Key], f.[Stock Item Key],
            f.[Invoice Date Key], f.[Delivery Date Key], f.[Salesperson Key], f.[WWI Invoice ID],
            f.[Description], f.[Package], -f.[Quantity], f.[Unit Price], f.[Tax Rate],
            -f.[Total Excluding Tax], -f.[Tax Amount], -f.[Profit], -f.[Total Including Tax],
            -f.[Total Dry Items], -f.[Total Chiller Items], f.[Lineage Key],
            f.[Invoice Number], f.[Order Number], f.[Region Code], f.[Natural Key Hash],
            N'REV', f.[Batch Id], @BatchId, SYSDATETIME()
        FROM Fact.[Sale] AS f
        INNER JOIN #SaleWork AS w
            ON w.[Natural Key Hash] = f.[Natural Key Hash]
        WHERE ISNULL(f.[Correction Type Code], N'ORIG') IN (N'ORIG', N'RES')
          AND f.[Invoice Date Key] < @LoadStartDate
          AND ROUND(f.[Total Excluding Tax], 2) <> ROUND(w.[Net Amount], 2);

        SET @InsertRowCount = @@ROWCOUNT;

        DELETE FROM Fact.[Sale]
        WHERE [Invoice Date Key] >= @LoadStartDate
          AND [Invoice Date Key] <= @LoadEndDate
          AND ISNULL([Correction Type Code], N'ORIG') <> N'REV';

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Fact.[Sale]
        (
            [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key],
            [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID],
            [Description], [Package], [Quantity], [Unit Price], [Tax Rate],
            [Total Excluding Tax], [Tax Amount], [Profit], [Total Including Tax],
            [Total Dry Items], [Total Chiller Items], [Lineage Key],
            [Invoice Number], [Order Number], [Region Code], [Sales Channel Key],
            [Promotion Key], [Transaction Currency Code], [Fx Rate], [Fx Rate Date],
            [Fx Rate Source Code], [Gross Amount], [Line Discount Amount], [Net Amount],
            [Freight Amount], [Cost Of Sale Amount], [Gross Margin Amount], [Margin Percent],
            [Quantity Base UOM], [Quantity Source UOM], [Source UOM Code],
            [Net Amount Reporting], [Tax Amount Reporting], [Gross Margin Reporting],
            [Tax Regime Code], [Customer Vat Number], [Gst Free Flag], [Natural Key Hash],
            [Correction Type Code], [Inferred Member Flag], [Batch Id], [Load Datetime]
        )
        SELECT
            w.[City Key], w.[Customer Key], w.[Bill To Customer Key], w.[Stock Item Key],
            w.[Invoice Date], w.[Delivery Date], w.[Salesperson Key],
            TRY_CONVERT(INT, RIGHT(w.[Invoice Number], 9)),
            item.[Stock Item], item.[Size], w.[Quantity Source UOM] * w.[UOM Conversion Factor],
            w.[Unit Price], w.[Tax Rate],
            w.[Net Amount], w.[Tax Amount], w.[Gross Margin], w.[Net Amount] + w.[Tax Amount],
            0, 0, @LineageKey,
            w.[Invoice Number], w.[Order Number], w.[Region Code], w.[Sales Channel Key],
            w.[Promotion Key], w.[Transaction Currency], w.[Fx Rate], w.[Fx Rate Date],
            CASE WHEN w.[Region Code] = N'APAC' THEN N'APAC_TREASURY' ELSE N'GROUP' END,
            w.[Gross Amount], w.[Line Discount Amount], w.[Net Amount],
            w.[Freight Amount], w.[Cost Of Sale], w.[Gross Margin],
            CASE WHEN w.[Net Amount] = 0 THEN NULL
                 ELSE ROUND(w.[Gross Margin] / w.[Net Amount] * 100.0, 4) END,
            w.[Quantity Source UOM] * w.[UOM Conversion Factor], w.[Quantity Source UOM],
            w.[Source UOM Code],
            ROUND(w.[Net Amount] * w.[Fx Rate], 2),
            ROUND(w.[Tax Amount] * w.[Fx Rate], 2),
            ROUND(w.[Gross Margin] * w.[Fx Rate], 2),
            w.[Tax Regime Code], w.[Customer Vat Number], w.[Gst Free Flag], w.[Natural Key Hash],
            CASE WHEN EXISTS (SELECT 1 FROM Fact.[Sale] AS p
                              WHERE p.[Natural Key Hash] = w.[Natural Key Hash]
                                AND p.[Correction Type Code] = N'REV'
                                AND p.[Batch Id] = @BatchId)
                 THEN N'RES' ELSE N'ORIG' END,
            w.[Inferred Member Flag], @BatchId, SYSDATETIME()
        FROM #SaleWork AS w
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Key] = w.[Stock Item Key];

        SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Sale',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @DeleteRowCount     = @DeleteRowCount,
            @RejectRowCount     = @RejectRowCount;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Fact Load Hold (Fact.Sale)',
            @InsertRowCount     = @HoldRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'WWI_OLTP',
            @ObjectName         = N'Fact.Sale',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsDeleted        = @DeleteRowCount,
                @RowsRejected       = @RejectRowCount,
                @WatermarkFrom      = @WatermarkFrom,
                @WatermarkTo        = @WatermarkTo;

        DROP TABLE #SaleWork;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF CURSOR_STATUS('local', 'reject_cursor') >= 0
        BEGIN
            CLOSE reject_cursor;
            DEALLOCATE reject_cursor;
        END;

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Sale',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactSale',
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
