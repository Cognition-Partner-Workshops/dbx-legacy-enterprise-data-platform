/*
    Integration.usp_LoadFactCreditNote

    Object        : Integration.usp_LoadFactCreditNote
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Credit Note, Fact.Return.
    Called by     : FACT_Load_Credit_Note, FIN_Load_Credit_Notes.
    Reads         : stg.CreditNoteLine, stg.CreditNoteHeader.
    Depends on    : the etl control procedures.

    Only NON-return credit notes are loaded here: price adjustments, goodwill
    gestures, volume rebates and billing errors. Credit notes raised against an
    RMA are already carried on Fact.Return and loading them here as well would
    double the credit - the NOT EXISTS against Fact.Return is the only thing
    stopping that and it has been removed by accident twice.

    Tax adjustment follows the regime of the original invoice, not the regime
    in force on the credit date. That matters for the 2021 EU VAT rate change
    and is why [Original Vat Rate] is carried on the row.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactCreditNote', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactCreditNote;
GO

CREATE PROCEDURE Integration.usp_LoadFactCreditNote
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @RejectRowCount BIGINT = 0;
    DECLARE @WatermarkFrom  NVARCHAR(50);
    DECLARE @WatermarkTo    NVARCHAR(50);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Credit_Note',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactCreditNote',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode = N'WWI_AR',
            @ObjectName       = N'Fact.Credit Note',
            @WatermarkFrom    = @WatermarkFrom OUTPUT,
            @WatermarkTo      = @WatermarkTo OUTPUT;

        INSERT INTO Fact.[Credit Note]
        (
            [Credit Note Date Key], [Original Invoice Date Key], [Customer Key],
            [Bill To Customer Key], [Stock Item Key], [Salesperson Key],
            [Sales Territory Key], [Currency Key], [Return Key], [Region Code],
            [Credit Note Number], [Credit Note Line Number], [Original Invoice Number],
            [Credit Reason Code], [Tax Adjustment Reason Code], [Approved By Employee Key],
            [Quantity Credited], [Credit Amount Excluding Tax], [Tax Adjustment Amount],
            [Credit Amount Including Tax], [Fx Rate], [Credit Amount Reporting],
            [Vat Rate], [Gst Rate], [Sales Tax Rate], [Original Vat Rate],
            [Goodwill Flag], [Rebate Flag], [Fiscal Year], [Fiscal Period],
            [Natural Key Hash], [Inferred Member Flag], [Lineage Key],
            [Batch Id], [Load Datetime]
        )
        SELECT
            hdr.CreditNoteDate,
            hdr.OriginalInvoiceDate,
            ISNULL(cust.[Customer Key], 0),
            ISNULL(bill.[Customer Key], ISNULL(cust.[Customer Key], 0)),
            CASE WHEN lin.StockItemCode IS NULL THEN -1 ELSE ISNULL(item.[Stock Item Key], 0) END,
            CASE WHEN hdr.SalespersonCode IS NULL THEN -1 ELSE ISNULL(sp.[Salesperson Key], 0) END,
            CASE WHEN hdr.SalesTerritoryCode IS NULL THEN -1
                 ELSE ISNULL(terr.[Sales Territory Key], 0) END,
            ISNULL(cur.[Currency Key], 0),
            NULL,
            hdr.RegionCode,
            hdr.CreditNoteNumber,
            lin.CreditNoteLineNumber,
            hdr.OriginalInvoiceNumber,
            hdr.CreditReasonCode,
            lin.TaxAdjustmentReasonCode,
            CASE WHEN hdr.ApprovedByCode IS NULL THEN -1 ELSE ISNULL(emp.[Employee Key], 0) END,
            -ABS(ISNULL(lin.QuantityCredited, 0)),
            -ABS(lin.CreditAmountExcludingTax),
            -ABS(ISNULL(lin.TaxAdjustmentAmount, 0)),
            -ABS(lin.CreditAmountExcludingTax) - ABS(ISNULL(lin.TaxAdjustmentAmount, 0)),
            ISNULL(hdr.FxRate, 1.0),
            ROUND((-ABS(lin.CreditAmountExcludingTax)
                   - ABS(ISNULL(lin.TaxAdjustmentAmount, 0))) * ISNULL(hdr.FxRate, 1.0), 2),
            CASE WHEN hdr.RegionCode = N'EU'   THEN lin.TaxRate END,
            CASE WHEN hdr.RegionCode = N'APAC' THEN lin.TaxRate END,
            CASE WHEN hdr.RegionCode = N'NA'   THEN lin.TaxRate END,
            hdr.OriginalTaxRate,
            CASE WHEN hdr.CreditReasonCode IN (N'GDWL', N'SERV') THEN 1 ELSE 0 END,
            CASE WHEN hdr.CreditReasonCode IN (N'REBT', N'VOLR') THEN 1 ELSE 0 END,
            hdr.FiscalYear,
            hdr.FiscalPeriod,
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256',
                CONCAT(hdr.CreditNoteNumber, N'|', lin.CreditNoteLineNumber))),
            CASE WHEN cust.[Customer Key] IS NULL THEN 1 ELSE 0 END,
            0, @BatchId, SYSDATETIME()
        FROM stg.CreditNoteLine AS lin
        INNER JOIN stg.CreditNoteHeader AS hdr
            ON hdr.CreditNoteNumber = lin.CreditNoteNumber
        LEFT JOIN Dimension.[Customer] AS cust
            ON cust.[WWI Customer ID] = TRY_CONVERT(INT, hdr.CustomerBusinessKey)
           AND hdr.CreditNoteDate >= cust.[Valid From] AND hdr.CreditNoteDate < cust.[Valid To]
        LEFT JOIN Dimension.[Customer] AS bill
            ON bill.[WWI Customer ID] = TRY_CONVERT(INT, hdr.BillToBusinessKey)
           AND bill.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Code] = lin.StockItemCode
           AND hdr.CreditNoteDate >= item.[Valid From] AND hdr.CreditNoteDate < item.[Valid To]
        LEFT JOIN Dimension.[Salesperson] AS sp
            ON sp.[Salesperson Code] = hdr.SalespersonCode
           AND sp.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Sales Territory] AS terr
            ON terr.[Territory Code] = hdr.SalesTerritoryCode
        LEFT JOIN Dimension.[Currency] AS cur
            ON cur.[Currency Code] = hdr.CurrencyCode
        LEFT JOIN Dimension.[Employee] AS emp
            ON emp.[Employee Code] = hdr.ApprovedByCode
           AND emp.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        WHERE hdr.CreditNoteDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
          AND hdr.RmaNumber IS NULL
          AND NOT EXISTS (SELECT 1 FROM Fact.[Return] AS r
                          WHERE r.[Credit Note Number] = hdr.CreditNoteNumber)
          AND NOT EXISTS (SELECT 1 FROM Fact.[Credit Note] AS f
                          WHERE f.[Credit Note Number] = hdr.CreditNoteNumber
                            AND f.[Credit Note Line Number] = lin.CreditNoteLineNumber);

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* Unapproved credit notes above the goodwill threshold are held back
           entirely - the approval workflow lives in the source system and the
           warehouse must not publish an unapproved credit. */
        SELECT @RejectRowCount = COUNT_BIG(*)
        FROM stg.CreditNoteHeader AS hdr
        WHERE hdr.CreditNoteDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
          AND hdr.ApprovedByCode IS NULL
          AND hdr.CreditTotalAmount > 1000;

        IF @RejectRowCount > 0
            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'WWI_AR',
                @ObjectName         = N'Fact.Credit Note',
                @BusinessKey        = N'(grouped)',
                @RejectReasonCode   = N'CREDIT_UNAPPROVED',
                @RejectReason       = N'Credit note over threshold has no approver',
                @RejectStage        = N'Fact';

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Credit Note',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @RejectRowCount     = @RejectRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'WWI_AR',
            @ObjectName         = N'Fact.Credit Note',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsRejected       = @RejectRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Credit Note',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactCreditNote',
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
