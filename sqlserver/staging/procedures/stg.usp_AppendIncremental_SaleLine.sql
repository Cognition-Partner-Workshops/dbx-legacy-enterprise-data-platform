/*
    stg.usp_AppendIncremental_SaleLine

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_LOAD_SALELINE (SSIS)
    Reads         : raw.SqlInvoiceLine, stg.Sale, stg.StockItem, ref.Region,
                    ref.UomConversion, ref.FxRateDaily
    Writes        : stg.SaleLine, err.RejectedInvoiceLine
    Control       : etl.usp_GetWatermark, etl.usp_SetWatermark, etl.usp_LogRowCount,
                    etl.usp_LogError, err.usp_LogRejectedRows

    Restatable incremental: the keys arriving in this batch are deleted from the
    target before insert, because invoice lines are edited in the OLTP for up to
    five days after the invoice is raised (credit adjustments and commission
    corrections) and the fact load cannot cope with two versions of a line.

    The tax check is the reason this load has its own reject table. The OLTP
    stores both TaxRate and TaxAmount and they disagree often enough that finance
    asked for a variance report rather than a silent recalculation:
        NA   tolerance is 0.02 per line - rounding at the register.
        EU   tolerance is 0.01 and a mismatch on a reverse-charge line is always
             a reject, because a reverse-charge line must carry zero VAT.
        APAC tolerance is 0.05 because two of the three APAC billing systems
             round GST at the invoice and back-allocate it to lines.
*/

IF OBJECT_ID(N'stg.usp_AppendIncremental_SaleLine', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_AppendIncremental_SaleLine;
GO

CREATE PROCEDURE stg.usp_AppendIncremental_SaleLine
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP',
    @ReloadFullHistory  BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'stg.SaleLine';
    DECLARE @WatermarkFrom NVARCHAR(50);
    DECLARE @WatermarkTo   NVARCHAR(50);
    DECLARE @FromUtc       DATETIME2(3);
    DECLARE @SourceRows    BIGINT = 0;
    DECLARE @InsertedRows  BIGINT = 0;
    DECLARE @DeletedRows   BIGINT = 0;
    DECLARE @RejectedRows  BIGINT = 0;
    DECLARE @MaxEditedWhen DATETIME2(3);

    BEGIN TRY
        EXEC etl.usp_GetWatermark
            @SourceSystemCode  = @SourceSystemCode,
            @ObjectName        = @ObjectName,
            @ReloadFullHistory = @ReloadFullHistory,
            @WatermarkFrom     = @WatermarkFrom OUTPUT,
            @WatermarkTo       = @WatermarkTo   OUTPUT;

        SET @FromUtc = ISNULL(TRY_CONVERT(DATETIME2(3), @WatermarkFrom, 126), CONVERT(DATETIME2(3), '1900-01-01'));

        SELECT
            SaleLineBusinessKey = CONCAT(stg.ufn_SourceSystemKey(l.SourceSystemCode, l.InvoiceID, 1), N'|', LTRIM(RTRIM(l.InvoiceLineID))),
            SaleBusinessKey     = stg.ufn_SourceSystemKey(l.SourceSystemCode, l.InvoiceID, 1),
            SourceInvoiceLineId = LTRIM(RTRIM(l.InvoiceLineID)),
            SourceInvoiceId     = LTRIM(RTRIM(l.InvoiceID)),
            LineNumber          = CONVERT(INT, stg.ufn_SafeDecimal(l.InvoiceLineID, N'.')),
            StockItemBusinessKey = stg.ufn_SourceSystemKey(l.SourceSystemCode, l.StockItemID, 1),
            LineDescription     = LEFT(stg.ufn_CleanString(l.Description, 0), 200),
            PackageTypeCode     = NULLIF(UPPER(LTRIM(RTRIM(l.PackageTypeID))), N''),
            Quantity            = CONVERT(DECIMAL(18,4), stg.ufn_SafeDecimal(l.Quantity, N'.')),
            UnitPriceAmount     = CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(l.UnitPrice, N'.')),
            SourceTaxRate       = CONVERT(DECIMAL(9,4), stg.ufn_SafeDecimal(l.TaxRate, N'.')),
            SourceTaxAmount     = CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(l.TaxAmount, N'.')),
            ExtendedPrice       = CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(l.ExtendedPrice, N'.')),
            LineProfitAmount    = CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(l.LineProfit, N'.')),
            CommissionRatePercent = CONVERT(DECIMAL(9,4), stg.ufn_SafeDecimal(l.CommissionRate, N'.')),
            PromotionBusinessKey = stg.ufn_SourceSystemKey(l.SourceSystemCode, l.PromotionLineID, 1),
            LastEditedWhenUtc   = CONVERT(DATETIME2(3), stg.ufn_SafeDate(l.LastEditedWhen, N'NA')),
            TaxAmountText       = l.TaxAmount
        INTO #IncomingSaleLine
        FROM raw.SqlInvoiceLine AS l
        WHERE l.BatchId = @BatchId
          AND (
                  @ReloadFullHistory = 1
               OR ISNULL(CONVERT(DATETIME2(3), stg.ufn_SafeDate(l.LastEditedWhen, N'NA')),
                         CONVERT(DATETIME2(3), '9999-12-31')) > @FromUtc
              );

        SELECT @SourceRows = COUNT_BIG(*) FROM #IncomingSaleLine;

        BEGIN TRANSACTION;

        DELETE sl
        FROM stg.SaleLine AS sl
        INNER JOIN #IncomingSaleLine AS i
            ON i.SaleLineBusinessKey = sl.SaleLineBusinessKey
        WHERE sl.BatchId = @BatchId;

        SET @DeletedRows = @@ROWCOUNT;

        INSERT INTO err.RejectedInvoiceLine
        (
            BatchId, PackageExecutionId, SourceSystemCode, InvoiceBusinessKey, InvoiceLineBusinessKey,
            InvoiceNumber, LineNumber, TaxCode, LineAmountText, RejectReasonCode, RejectReason,
            RejectStage, ExpectedTaxAmount, ActualTaxAmount, VarianceAmount, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, @SourceSystemCode, i.SaleBusinessKey, i.SaleLineBusinessKey,
            i.SourceInvoiceId, CONVERT(NVARCHAR(20), i.LineNumber), rg.TaxRegimeCode, i.TaxAmountText,
            CASE
                WHEN i.Quantity IS NULL OR i.UnitPriceAmount IS NULL THEN N'BAD_NUMERIC'
                WHEN s.SaleBusinessKey IS NULL                       THEN N'NO_PO_MATCH'
                ELSE N'TAX_MISMATCH'
            END,
            CASE
                WHEN i.Quantity IS NULL OR i.UnitPriceAmount IS NULL
                    THEN N'Quantity or UnitPrice will not convert to a decimal'
                WHEN s.SaleBusinessKey IS NULL
                    THEN N'no matching invoice header in stg.Sale for this batch'
                ELSE N'TaxAmount differs from rate x net beyond the regional tolerance'
            END,
            N'Stage',
            CONVERT(DECIMAL(19,4), (i.Quantity * i.UnitPriceAmount) * ISNULL(i.SourceTaxRate, 0) / 100.0),
            i.SourceTaxAmount,
            CONVERT(DECIMAL(19,4),
                ISNULL(i.SourceTaxAmount, 0)
                - ((i.Quantity * i.UnitPriceAmount) * ISNULL(i.SourceTaxRate, 0) / 100.0)),
            CONCAT(N'{"InvoiceLineID":"', i.SourceInvoiceLineId, N'","InvoiceID":"', i.SourceInvoiceId,
                   N'","TaxAmount":"', i.TaxAmountText, N'"}')
        FROM #IncomingSaleLine AS i
        LEFT JOIN stg.Sale AS s
            ON  s.SaleBusinessKey = i.SaleBusinessKey
            AND s.BatchId         = @BatchId
        LEFT JOIN ref.Region AS rg
            ON rg.RegionCode = s.RegionCode
        WHERE i.Quantity IS NULL
           OR i.UnitPriceAmount IS NULL
           OR s.SaleBusinessKey IS NULL
           OR ABS(ISNULL(i.SourceTaxAmount, 0)
                  - ((i.Quantity * i.UnitPriceAmount) * ISNULL(i.SourceTaxRate, 0) / 100.0))
              > CASE rg.RegionCode WHEN N'NA' THEN 0.02 WHEN N'EU' THEN 0.01 ELSE 0.05 END;

        SET @RejectedRows = @@ROWCOUNT;

        INSERT INTO stg.SaleLine
        (
            SaleLineBusinessKey, SaleBusinessKey, SourceSystemCode, LineNumber, StockItemBusinessKey,
            ProductBusinessKey, LineDescription, Quantity, UomCode, QuantityBaseUom, UnitPriceAmount,
            NetLineAmount, TaxRegimeCode, TaxRatePercent, TaxAmount, TaxRoundingRuleCode,
            GrossLineAmount, LineProfitAmount, CommissionRatePercent, CommissionAmount,
            TransactionCurrencyCode, NetLineAmountUsd, GrossLineAmountUsd, FxRateDate,
            PromotionBusinessKey, InvoiceDate, RegionCode, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            i.SaleLineBusinessKey,
            i.SaleBusinessKey,
            @SourceSystemCode,
            i.LineNumber,
            i.StockItemBusinessKey,
            si.ProductBusinessKey,
            i.LineDescription,
            i.Quantity,
            ISNULL(i.PackageTypeCode, N'EA'),
            CONVERT(DECIMAL(18,4), i.Quantity * ISNULL(uc.ConversionFactor, 1)),
            i.UnitPriceAmount,
            CONVERT(DECIMAL(19,4), ISNULL(i.ExtendedPrice, i.Quantity * i.UnitPriceAmount) - ISNULL(i.SourceTaxAmount, 0)),
            rg.TaxRegimeCode,
            i.SourceTaxRate,
            i.SourceTaxAmount,
            CASE rg.RegionCode WHEN N'APAC' THEN N'LINE' WHEN N'EU' THEN N'INVOICE' ELSE N'JURISDICTION' END,
            ISNULL(i.ExtendedPrice, (i.Quantity * i.UnitPriceAmount) + ISNULL(i.SourceTaxAmount, 0)),
            i.LineProfitAmount,
            i.CommissionRatePercent,
            CONVERT(DECIMAL(19,4),
                (ISNULL(i.ExtendedPrice, i.Quantity * i.UnitPriceAmount) - ISNULL(i.SourceTaxAmount, 0))
                * ISNULL(i.CommissionRatePercent, 0) / 100.0),
            s.TransactionCurrencyCode,
            CONVERT(DECIMAL(19,4),
                (ISNULL(i.ExtendedPrice, i.Quantity * i.UnitPriceAmount) - ISNULL(i.SourceTaxAmount, 0))
                * ISNULL(fx.ConversionRate, ISNULL(s.TransactionFxRate, 1))),
            CONVERT(DECIMAL(19,4),
                ISNULL(i.ExtendedPrice, (i.Quantity * i.UnitPriceAmount) + ISNULL(i.SourceTaxAmount, 0))
                * ISNULL(fx.ConversionRate, ISNULL(s.TransactionFxRate, 1))),
            s.InvoiceDate,
            i.PromotionBusinessKey,
            s.InvoiceDate,
            s.RegionCode,
            CASE
                WHEN si.StockItemBusinessKey IS NULL THEN N'WARN'
                WHEN fx.ConversionRate IS NULL AND s.TransactionCurrencyCode <> N'USD' THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(i.SaleLineBusinessKey, N'|', i.Quantity, N'|', i.UnitPriceAmount, N'|',
                       i.SourceTaxAmount, N'|', i.LineProfitAmount, N'|', i.CommissionRatePercent)),
            @BatchId,
            @PackageExecutionId
        FROM #IncomingSaleLine AS i
        INNER JOIN stg.Sale AS s
            ON  s.SaleBusinessKey = i.SaleBusinessKey
            AND s.BatchId         = @BatchId
        LEFT JOIN ref.Region AS rg
            ON rg.RegionCode = s.RegionCode
        LEFT JOIN stg.StockItem AS si
            ON  si.StockItemBusinessKey = i.StockItemBusinessKey
            AND si.BatchId              = @BatchId
        LEFT JOIN ref.UomConversion AS uc
            ON  uc.FromUomCode          = ISNULL(i.PackageTypeCode, N'EA')
            AND uc.ToUomCode            = N'EA'
            AND uc.StockItemBusinessKey = N'*'
        OUTER APPLY
        (
            SELECT TOP (1) f.ConversionRate
            FROM ref.FxRateDaily AS f
            WHERE f.FromCurrencyCode = s.TransactionCurrencyCode
              AND f.ToCurrencyCode   = N'USD'
              AND f.RateTypeCode     = CASE WHEN s.RegionCode = N'EU' THEN N'PERIOD_END' ELSE N'SPOT' END
              AND f.RateDate        <= s.InvoiceDate
            ORDER BY f.RateDate DESC
        ) AS fx
        WHERE i.Quantity IS NOT NULL
          AND i.UnitPriceAmount IS NOT NULL
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM err.RejectedInvoiceLine AS e
                  WHERE e.BatchId                = @BatchId
                    AND e.InvoiceLineBusinessKey = i.SaleLineBusinessKey
              );

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        SELECT @MaxEditedWhen = MAX(i.LastEditedWhenUtc) FROM #IncomingSaleLine AS i;

        IF @MaxEditedWhen IS NOT NULL
            SET @WatermarkTo = CONVERT(NVARCHAR(50), @MaxEditedWhen, 126);

        EXEC etl.usp_SetWatermark
            @SourceSystemCode   = @SourceSystemCode,
            @ObjectName         = @ObjectName,
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @DeleteRowCount     = @DeletedRows,
            @RejectRowCount     = @RejectedRows;

        IF @RejectedRows > 0
            EXEC err.usp_LogRejectedRows
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @RejectTableName    = N'err.RejectedInvoiceLine',
                @ObjectName         = @ObjectName,
                @BusinessKeyColumn  = N'InvoiceLineBusinessKey';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_LOAD_SALELINE',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_AppendIncremental_SaleLine';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
