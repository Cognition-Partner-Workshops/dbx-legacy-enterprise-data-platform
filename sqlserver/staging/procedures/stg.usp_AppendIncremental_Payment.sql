/*
    stg.usp_AppendIncremental_Payment

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_LOAD_PAYMENT (SSIS), before WORK_MATCH_PAYMENTS
    Reads         : raw.OracleApPayment, stg.Supplier, ref.Region, ref.Currency,
                    ref.FxRateDaily, ref.CodeCrosswalk
    Writes        : stg.Payment, err.RejectedPayment
    Control       : etl.usp_GetWatermark, etl.usp_SetWatermark, etl.usp_LogRowCount,
                    etl.usp_LogRejectedRecord, etl.usp_LogError

    Incremental on LAST_UPDATE_DT with a two-day overlap, because the Oracle AP
    module backdates the update timestamp when a payment is voided.

    Payment method conformance is regional and has to happen here rather than in
    the crosswalk, because the same source code means different things by ledger:
        NA   CHK is a printed check and ACH is domestic clearing.
        EU   SEPA and BACS both conform to SEPA; the BACS rows predate 2014.
        APAC BPAY is Australian only; every other APAC ledger sends WIRE.

    Realized FX gain/loss is computed here only for payments in a currency other
    than the ledger currency. The value is provisional: the final figure comes
    out of work.usp_MatchPaymentsToInvoices once the payment is applied to the
    invoices it settles.
*/

IF OBJECT_ID(N'stg.usp_AppendIncremental_Payment', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_AppendIncremental_Payment;
GO

CREATE PROCEDURE stg.usp_AppendIncremental_Payment
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP',
    @ReloadFullHistory  BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'stg.Payment';
    DECLARE @WatermarkFrom NVARCHAR(50);
    DECLARE @WatermarkTo   NVARCHAR(50);
    DECLARE @FromUtc       DATETIME2(3);
    DECLARE @SourceRows    BIGINT = 0;
    DECLARE @InsertedRows  BIGINT = 0;
    DECLARE @RejectedRows  BIGINT = 0;
    DECLARE @MaxUpdateDate DATETIME2(3);

    BEGIN TRY
        EXEC etl.usp_GetWatermark
            @SourceSystemCode  = @SourceSystemCode,
            @ObjectName        = @ObjectName,
            @ReloadFullHistory = @ReloadFullHistory,
            @WatermarkFrom     = @WatermarkFrom OUTPUT,
            @WatermarkTo       = @WatermarkTo   OUTPUT;

        --  Two-day overlap for backdated voids.
        SET @FromUtc = DATEADD(DAY, -2,
            ISNULL(TRY_CONVERT(DATETIME2(3), @WatermarkFrom, 126), CONVERT(DATETIME2(3), '1900-01-01')));

        SELECT
            PaymentBusinessKey  = stg.ufn_SourceSystemKey(p.SourceSystemCode, p.PAYMENT_ID, 1),
            SourcePaymentId     = LTRIM(RTRIM(p.PAYMENT_ID)),
            PaymentNumber       = stg.ufn_CleanString(p.PAYMENT_NUM, 1),
            SupplierBusinessKey = stg.ufn_SourceSystemKey(p.SourceSystemCode, p.SUPP_ID, 1),
            RegionCode          = UPPER(LTRIM(RTRIM(p.REGION_CD))),
            LedgerCode          = NULLIF(UPPER(LTRIM(RTRIM(p.LEDGER_CD))), N''),
            SourceMethodCode    = UPPER(LTRIM(RTRIM(p.PAYMENT_METHOD_CD))),
            PaymentDate         = stg.ufn_SafeDate(p.PAYMENT_DT, p.REGION_CD),
            ClearedDate         = stg.ufn_SafeDate(p.CLEARED_DT, p.REGION_CD),
            VoidDate            = stg.ufn_SafeDate(p.VOID_DT, p.REGION_CD),
            PaymentStatusCode   = COALESCE(cx.ConformedCodeValue, NULLIF(UPPER(LTRIM(RTRIM(p.PAYMENT_STATUS_CD))), N''), N'UNKNOWN'),
            CurrencyCode        = LEFT(UPPER(LTRIM(RTRIM(p.CURRENCY_CD))), 3),
            SourceFxRate        = CONVERT(DECIMAL(19,8), stg.ufn_SafeDecimal(p.FX_RATE, rg.DecimalSeparator)),
            PaymentAmount       = CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(p.PAYMENT_AMT, rg.DecimalSeparator)),
            DiscountTakenAmount = CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(p.DISCOUNT_TAKEN_AMT, rg.DecimalSeparator)),
            BankAccountReference = stg.ufn_CleanString(p.BANK_ACCOUNT_REF, 1),
            RemittanceReference = stg.ufn_CleanString(p.REMITTANCE_REF, 1),
            AppliedInvoiceList  = p.APPLIED_INVOICE_NUMS,
            LastUpdateUtc       = CONVERT(DATETIME2(3), stg.ufn_SafeDate(p.LAST_UPDATE_DT, p.REGION_CD)),
            PaymentAmountText   = p.PAYMENT_AMT,
            LedgerCurrencyCode  = rg.DefaultCurrencyCode
        INTO #IncomingPayment
        FROM raw.OracleApPayment AS p
        LEFT JOIN ref.Region AS rg
            ON rg.RegionCode = UPPER(LTRIM(RTRIM(p.REGION_CD)))
        LEFT JOIN ref.CodeCrosswalk AS cx
            ON  cx.CodeDomainCode   = N'PAYMENT_STATUS'
            AND cx.SourceSystemCode = p.SourceSystemCode
            AND cx.SourceCodeValue  = p.PAYMENT_STATUS_CD
            AND cx.EffectiveToDate IS NULL
        WHERE p.BatchId = @BatchId
          AND (
                  @ReloadFullHistory = 1
               OR ISNULL(CONVERT(DATETIME2(3), stg.ufn_SafeDate(p.LAST_UPDATE_DT, p.REGION_CD)),
                         CONVERT(DATETIME2(3), '9999-12-31')) > @FromUtc
              );

        SELECT @SourceRows = COUNT_BIG(*) FROM #IncomingPayment;

        BEGIN TRANSACTION;

        INSERT INTO err.RejectedPayment
        (
            BatchId, PackageExecutionId, SourceSystemCode, PaymentBusinessKey, PaymentNumber,
            SupplierReference, PaymentAmountText, CurrencyCode, RejectReasonCode, RejectReason,
            RejectStage, UnappliedAmount, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, @SourceSystemCode, i.PaymentBusinessKey, i.PaymentNumber,
            i.SupplierBusinessKey, i.PaymentAmountText, i.CurrencyCode,
            CASE
                WHEN s.SupplierBusinessKey IS NULL THEN N'UNKNOWN_SUPPLIER'
                WHEN i.PaymentAmount IS NULL       THEN N'BAD_NUMERIC'
                ELSE N'NO_FX_RATE'
            END,
            CASE
                WHEN s.SupplierBusinessKey IS NULL
                    THEN N'SUPP_ID does not resolve to a staged supplier for this batch'
                WHEN i.PaymentAmount IS NULL
                    THEN N'PAYMENT_AMT will not convert to a decimal with the regional separator'
                ELSE N'no FX rate to USD on or before the payment date and the payment is not in USD'
            END,
            N'Stage',
            i.PaymentAmount,
            CONCAT(N'{"PAYMENT_ID":"', i.SourcePaymentId, N'","PAYMENT_AMT":"', i.PaymentAmountText,
                   N'","CURRENCY_CD":"', i.CurrencyCode, N'"}')
        FROM #IncomingPayment AS i
        LEFT JOIN stg.Supplier AS s
            ON  s.SupplierBusinessKey = i.SupplierBusinessKey
            AND s.BatchId             = @BatchId
        OUTER APPLY
        (
            SELECT TOP (1) f.ConversionRate
            FROM ref.FxRateDaily AS f
            WHERE f.FromCurrencyCode = i.CurrencyCode
              AND f.ToCurrencyCode   = N'USD'
              AND f.RateTypeCode     = N'SPOT'
              AND f.RateDate        <= i.PaymentDate
            ORDER BY f.RateDate DESC
        ) AS fx
        WHERE s.SupplierBusinessKey IS NULL
           OR i.PaymentAmount IS NULL
           OR (i.CurrencyCode <> N'USD' AND fx.ConversionRate IS NULL AND i.SourceFxRate IS NULL);

        SET @RejectedRows = @@ROWCOUNT;

        INSERT INTO stg.Payment
        (
            PaymentBusinessKey, SourceSystemCode, PaymentNumber, SupplierBusinessKey, PaymentMethodCode,
            PaymentDate, ClearedDate, VoidDate, PaymentStatusCode, TransactionCurrencyCode,
            TransactionFxRate, PaymentAmount, PaymentAmountUsd, DiscountTakenAmount,
            RealizedFxGainLossUsd, BankAccountReference, RemittanceReference, AppliedInvoiceCount,
            UnappliedAmount, MatchStatusCode, LedgerCode, RegionCode, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            i.PaymentBusinessKey,
            @SourceSystemCode,
            i.PaymentNumber,
            i.SupplierBusinessKey,
            CASE
                WHEN i.RegionCode = N'EU'   AND i.SourceMethodCode IN (N'SEPA', N'BACS') THEN N'SEPA'
                WHEN i.RegionCode = N'APAC' AND i.SourceMethodCode = N'BPAY'             THEN N'BPAY'
                WHEN i.RegionCode = N'APAC'                                              THEN N'WIRE'
                WHEN i.SourceMethodCode = N'CHK'                                         THEN N'CHECK'
                WHEN i.SourceMethodCode = N'ACH'                                         THEN N'ACH'
                ELSE N'WIRE'
            END,
            i.PaymentDate,
            i.ClearedDate,
            i.VoidDate,
            CASE WHEN i.VoidDate IS NOT NULL THEN N'VOID' ELSE i.PaymentStatusCode END,
            i.CurrencyCode,
            COALESCE(i.SourceFxRate, fx.ConversionRate, 1),
            i.PaymentAmount,
            CONVERT(DECIMAL(19,4), i.PaymentAmount * COALESCE(i.SourceFxRate, fx.ConversionRate, 1)),
            ISNULL(i.DiscountTakenAmount, 0),
            --  Provisional: the difference between the rate the ERP booked at and
            --  the rate treasury published for the payment date.
            CASE
                WHEN i.CurrencyCode = i.LedgerCurrencyCode THEN 0
                WHEN i.SourceFxRate IS NULL OR fx.ConversionRate IS NULL THEN NULL
                ELSE CONVERT(DECIMAL(19,4), i.PaymentAmount * (i.SourceFxRate - fx.ConversionRate))
            END,
            i.BankAccountReference,
            i.RemittanceReference,
            CASE
                WHEN NULLIF(LTRIM(RTRIM(i.AppliedInvoiceList)), N'') IS NULL THEN 0
                ELSE LEN(i.AppliedInvoiceList) - LEN(REPLACE(i.AppliedInvoiceList, N'|', N'')) + 1
            END,
            i.PaymentAmount,      -- everything is unapplied until the matcher runs
            N'UNMATCHED',
            i.LedgerCode,
            ISNULL(i.RegionCode, N'UNKNOWN'),
            CASE
                WHEN i.VoidDate IS NOT NULL AND i.ClearedDate IS NOT NULL THEN N'WARN'
                WHEN i.PaymentDate IS NULL                                THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(i.PaymentBusinessKey, N'|', i.PaymentAmount, N'|', i.CurrencyCode, N'|',
                       i.PaymentStatusCode, N'|', i.VoidDate, N'|', i.ClearedDate)),
            @BatchId,
            @PackageExecutionId
        FROM #IncomingPayment AS i
        INNER JOIN stg.Supplier AS s
            ON  s.SupplierBusinessKey = i.SupplierBusinessKey
            AND s.BatchId             = @BatchId
        OUTER APPLY
        (
            SELECT TOP (1) f.ConversionRate
            FROM ref.FxRateDaily AS f
            WHERE f.FromCurrencyCode = i.CurrencyCode
              AND f.ToCurrencyCode   = N'USD'
              AND f.RateTypeCode     = N'SPOT'
              AND f.RateDate        <= i.PaymentDate
            ORDER BY f.RateDate DESC
        ) AS fx
        WHERE i.PaymentAmount IS NOT NULL
          AND (i.CurrencyCode = N'USD' OR fx.ConversionRate IS NOT NULL OR i.SourceFxRate IS NOT NULL);

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        SELECT @MaxUpdateDate = MAX(i.LastUpdateUtc) FROM #IncomingPayment AS i;

        IF @MaxUpdateDate IS NOT NULL
            SET @WatermarkTo = CONVERT(NVARCHAR(50), @MaxUpdateDate, 126);

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
            @RejectRowCount     = @RejectedRows;

        IF @RejectedRows > 0
            EXEC err.usp_LogRejectedRows
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @RejectTableName    = N'err.RejectedPayment',
                @ObjectName         = @ObjectName,
                @BusinessKeyColumn  = N'PaymentBusinessKey';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_LOAD_PAYMENT',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_AppendIncremental_Payment';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
