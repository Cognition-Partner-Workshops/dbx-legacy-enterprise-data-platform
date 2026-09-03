/*
    stg.usp_TruncateAndReload_Supplier

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_LOAD_SUPPLIER (SSIS)
    Reads         : raw.OracleSupplierMaster, ref.CodeCrosswalk, ref.Region,
                    ref.Currency, ref.FxRateDaily
    Writes        : stg.Supplier, err.RejectedSupplier
    Control       : etl.usp_LogRowCount, etl.usp_LogRejectedRecord, etl.usp_LogError

    Unlike the customer load this one MERGEs rather than delete/insert. It was
    rewritten in 2012 when the supplier extract grew past the point where the
    nightly delete blew the log file, and the MERGE has been left alone since.

    Tax identifier handling is the interesting part and is genuinely different per
    region:
        NA   TAX_ID_NUM is an EIN and WITHHOLDING_CD drives 1099 reporting.
        EU   VAT_REGISTRATION_NUM is authoritative; the recovery-eligible flag is
             derived from the country prefix matching the ledger country.
        APAC GST_REGISTRATION_NUM is authoritative and the presence of a number is
             itself the registration flag.

    Minimum order amounts are converted to USD with the corporate rate for the
    batch date, not the spot rate, because procurement reports on the corporate
    rate. Missing rate is a WARN, not a reject: the supplier is still usable.
*/

IF OBJECT_ID(N'stg.usp_TruncateAndReload_Supplier', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_TruncateAndReload_Supplier;
GO

CREATE PROCEDURE stg.usp_TruncateAndReload_Supplier
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP',
    @RateTypeCode       NVARCHAR(20) = N'CORPORATE'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.Supplier';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @UpdatedRows  BIGINT = 0;
    DECLARE @RejectedRows BIGINT = 0;
    DECLARE @RateDate     DATE = CAST(SYSUTCDATETIME() AS DATE);

    CREATE TABLE #MergeAction (MergeAction NVARCHAR(10) NOT NULL);

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleSupplierMaster AS r
        WHERE r.BatchId = @BatchId;

        BEGIN TRANSACTION;

        --  Rejects first: a supplier with no name or an unknown currency must not
        --  reach the MERGE, because the MERGE has no reject route of its own.
        INSERT INTO err.RejectedSupplier
        (
            BatchId, PackageExecutionId, SourceSystemCode, SourceSupplierId, SupplierBusinessKey,
            SupplierName, TaxIdentifier, RejectReasonCode, RejectReason, RejectStage,
            FailedColumnName, FailedValue, RegionCode, RecordPayload
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            r.SourceSystemCode,
            r.SUPP_ID,
            stg.ufn_SourceSystemKey(r.SourceSystemCode, r.SUPP_ID, 1),
            LEFT(ISNULL(r.SUPP_NAME, N''), 400),
            COALESCE(r.TAX_ID_NUM, r.VAT_REGISTRATION_NUM, r.GST_REGISTRATION_NUM),
            v.RejectReasonCode,
            v.RejectReason,
            N'Stage',
            v.FailedColumnName,
            v.FailedValue,
            UPPER(LTRIM(RTRIM(r.REGION_CD))),
            CONCAT(N'{"SUPP_ID":"', r.SUPP_ID, N'","SUPP_NAME":"',
                   REPLACE(ISNULL(r.SUPP_NAME, N''), N'"', N''''),
                   N'","CURRENCY_CD":"', r.CURRENCY_CD, N'"}')
        FROM raw.OracleSupplierMaster AS r
        CROSS APPLY
        (
            SELECT TOP (1) x.RejectReasonCode, x.RejectReason, x.FailedColumnName, x.FailedValue
            FROM
            (
                SELECT 1 AS Ordinal,
                       N'MISSING_NAME' AS RejectReasonCode,
                       N'SUPP_NAME is empty after cleaning' AS RejectReason,
                       N'SUPP_NAME' AS FailedColumnName,
                       r.SUPP_NAME AS FailedValue
                WHERE stg.ufn_CleanString(r.SUPP_NAME, 0) IS NULL
                UNION ALL
                SELECT 2,
                       N'UNKNOWN_CURRENCY',
                       N'CURRENCY_CD is not present in ref.Currency',
                       N'CURRENCY_CD',
                       r.CURRENCY_CD
                WHERE NOT EXISTS (SELECT 1 FROM ref.Currency AS c
                                  WHERE c.CurrencyCode = LEFT(UPPER(LTRIM(RTRIM(r.CURRENCY_CD))), 3))
                UNION ALL
                SELECT 3,
                       N'BAD_TAXID',
                       N'EU supplier has no VAT registration number and no EU exemption code',
                       N'VAT_REGISTRATION_NUM',
                       r.VAT_REGISTRATION_NUM
                WHERE UPPER(LTRIM(RTRIM(r.REGION_CD))) = N'EU'
                  AND stg.ufn_CleanString(r.VAT_REGISTRATION_NUM, 1) IS NULL
                  AND ISNULL(UPPER(r.SUPP_CATEGORY_CD), N'') <> N'VATEXEMPT'
                UNION ALL
                SELECT 4,
                       N'MISSING_TERMS',
                       N'PAYMENT_TERMS_CD is empty and the supplier is not on hold',
                       N'PAYMENT_TERMS_CD',
                       r.PAYMENT_TERMS_CD
                WHERE stg.ufn_CleanString(r.PAYMENT_TERMS_CD, 1) IS NULL
                  AND ISNULL(UPPER(r.ON_HOLD_FLG), N'N') <> N'Y'
            ) AS x
            ORDER BY x.Ordinal
        ) AS v
        WHERE r.BatchId = @BatchId;

        SET @RejectedRows = @@ROWCOUNT;

        WITH TypedSupplier AS
        (
            SELECT
                stg.ufn_SourceSystemKey(r.SourceSystemCode, r.SUPP_ID, 1)        AS SupplierBusinessKey,
                LTRIM(RTRIM(r.SUPP_ID))                                          AS SourceSupplierId,
                stg.ufn_CleanString(r.SUPP_NUMBER, 1)                            AS ErpSupplierNumber,
                LEFT(stg.ufn_CleanString(r.SUPP_NAME, 0), 200)                   AS SupplierName,
                LEFT(stg.ufn_CleanString(r.SUPP_SHORT_NAME, 0), 100)             AS SupplierShortName,
                NULLIF(UPPER(LTRIM(RTRIM(r.SUPP_CATEGORY_CD))), N'')             AS SupplierCategoryCode,
                COALESCE(sx.ConformedCodeValue, N'UNKNOWN')                      AS SupplierStatusCode,
                LEFT(NULLIF(REPLACE(LTRIM(RTRIM(r.DUNS_NUMBER)), N'-', N''), N''), 20) AS DunsNumber,
                UPPER(LTRIM(RTRIM(r.REGION_CD)))                                 AS RegionCode,
                NULLIF(UPPER(LTRIM(RTRIM(r.LEDGER_CD))), N'')                    AS LedgerCode,
                NULLIF(REPLACE(UPPER(LTRIM(RTRIM(r.TAX_ID_NUM))), N' ', N''), N'')            AS EinNumber,
                NULLIF(REPLACE(UPPER(LTRIM(RTRIM(r.VAT_REGISTRATION_NUM))), N' ', N''), N'')  AS VatNumber,
                NULLIF(REPLACE(UPPER(LTRIM(RTRIM(r.GST_REGISTRATION_NUM))), N' ', N''), N'')  AS GstNumber,
                NULLIF(UPPER(LTRIM(RTRIM(r.WITHHOLDING_CD))), N'')               AS WithholdingCode,
                NULLIF(UPPER(LTRIM(RTRIM(r.PAYMENT_TERMS_CD))), N'')             AS PaymentTermsCode,
                COALESCE(px.ConformedCodeValue, UPPER(LTRIM(RTRIM(r.PAYMENT_METHOD_CD)))) AS PaymentMethodCode,
                LEFT(UPPER(LTRIM(RTRIM(r.CURRENCY_CD))), 3)                      AS TransactionCurrencyCode,
                NULLIF(UPPER(LTRIM(RTRIM(r.DEFAULT_INCOTERM_CD))), N'')          AS DefaultIncotermCode,
                CONVERT(SMALLINT, stg.ufn_SafeDecimal(r.LEAD_TIME_DAYS, N'.'))   AS LeadTimeDays,
                CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(r.MIN_ORDER_AMT, rg.DecimalSeparator)) AS MinimumOrderAmount,
                NULLIF(UPPER(LTRIM(RTRIM(r.SCORECARD_RATING))), N'')             AS ScorecardRatingCode,
                NULLIF(UPPER(LTRIM(RTRIM(r.DIVERSITY_CLASS_CD))), N'')           AS DiversityClassCode,
                CASE WHEN UPPER(LTRIM(RTRIM(r.ON_HOLD_FLG))) = N'Y' THEN 1 ELSE 0 END AS OnHoldFlag,
                NULLIF(UPPER(LTRIM(RTRIM(r.HOLD_REASON_CD))), N'')               AS HoldReasonCode,
                CONVERT(DATETIME2(3), stg.ufn_SafeDate(r.CREATED_DT, r.REGION_CD))     AS SourceCreatedDate,
                CONVERT(DATETIME2(3), stg.ufn_SafeDate(r.LAST_UPDATE_DT, r.REGION_CD)) AS SourceModifiedDate,
                rg.TaxRegimeCode
            FROM raw.OracleSupplierMaster AS r
            LEFT JOIN ref.Region AS rg
                ON rg.RegionCode = UPPER(LTRIM(RTRIM(r.REGION_CD)))
            LEFT JOIN ref.CodeCrosswalk AS sx
                ON  sx.CodeDomainCode   = N'SUPPLIER_STATUS'
                AND sx.SourceSystemCode = r.SourceSystemCode
                AND sx.SourceCodeValue  = r.SUPP_STATUS_CD
                AND sx.EffectiveToDate IS NULL
            LEFT JOIN ref.CodeCrosswalk AS px
                ON  px.CodeDomainCode   = N'PAYMENT_METHOD'
                AND px.SourceSystemCode = r.SourceSystemCode
                AND px.SourceCodeValue  = r.PAYMENT_METHOD_CD
                AND px.EffectiveToDate IS NULL
            WHERE r.BatchId = @BatchId
              AND NOT EXISTS
                  (
                      SELECT 1
                      FROM err.RejectedSupplier AS e
                      WHERE e.BatchId          = @BatchId
                        AND e.SourceSupplierId = r.SUPP_ID
                  )
        ),
        ConvertedSupplier AS
        (
            SELECT
                t.*,
                CONVERT(DECIMAL(19,4),
                    t.MinimumOrderAmount * ISNULL(fx.ConversionRate, CASE WHEN t.TransactionCurrencyCode = N'USD' THEN 1 ELSE NULL END)
                ) AS MinimumOrderAmountUsd
            FROM TypedSupplier AS t
            OUTER APPLY
            (
                SELECT TOP (1) f.ConversionRate
                FROM ref.FxRateDaily AS f
                WHERE f.FromCurrencyCode = t.TransactionCurrencyCode
                  AND f.ToCurrencyCode   = N'USD'
                  AND f.RateTypeCode     = @RateTypeCode
                  AND f.RateDate        <= @RateDate
                ORDER BY f.RateDate DESC
            ) AS fx
        )
        MERGE stg.Supplier AS tgt
        USING
        (
            SELECT c.*, @BatchId AS MergeBatchId
            FROM ConvertedSupplier AS c
        ) AS src
            ON  tgt.SupplierBusinessKey = src.SupplierBusinessKey
            AND tgt.BatchId             = src.MergeBatchId
        WHEN MATCHED THEN
            UPDATE SET
                tgt.SupplierName             = src.SupplierName,
                tgt.SupplierNameStandardized = UPPER(src.SupplierName),
                tgt.SupplierShortName        = src.SupplierShortName,
                tgt.SupplierCategoryCode     = src.SupplierCategoryCode,
                tgt.SupplierStatusCode       = src.SupplierStatusCode,
                tgt.PaymentTermsCode         = src.PaymentTermsCode,
                tgt.PaymentMethodCode        = src.PaymentMethodCode,
                tgt.MinimumOrderAmount       = src.MinimumOrderAmount,
                tgt.MinimumOrderAmountUsd    = src.MinimumOrderAmountUsd,
                tgt.OnHoldFlag               = src.OnHoldFlag,
                tgt.HoldReasonCode           = src.HoldReasonCode,
                tgt.SourceModifiedDate       = src.SourceModifiedDate,
                tgt.DqStatusCode             = CASE WHEN src.MinimumOrderAmount IS NOT NULL
                                                     AND src.MinimumOrderAmountUsd IS NULL
                                                    THEN N'WARN' ELSE N'PASS' END,
                tgt.PackageExecutionId       = @PackageExecutionId,
                tgt.LoadedAtUtc              = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                SupplierBusinessKey, SourceSystemCode, SourceSupplierId, ErpSupplierNumber,
                SupplierName, SupplierNameStandardized, SupplierShortName, SupplierCategoryCode,
                SupplierStatusCode, DunsNumber, TaxIdentifier, TaxIdentifierTypeCode, WithholdingCode,
                VatRecoveryEligibleFlag, GstRegisteredFlag, PaymentTermsCode, PaymentMethodCode,
                TransactionCurrencyCode, DefaultIncotermCode, LeadTimeDays, MinimumOrderAmount,
                MinimumOrderAmountUsd, ScorecardRatingCode, DiversityClassCode, RegionCode, LedgerCode,
                OnHoldFlag, HoldReasonCode, SourceCreatedDate, SourceModifiedDate, DqStatusCode,
                RowHash, ChangeHash, BatchId, PackageExecutionId
            )
            VALUES
            (
                src.SupplierBusinessKey,
                @SourceSystemCode,
                src.SourceSupplierId,
                src.ErpSupplierNumber,
                src.SupplierName,
                UPPER(src.SupplierName),
                src.SupplierShortName,
                src.SupplierCategoryCode,
                src.SupplierStatusCode,
                src.DunsNumber,
                COALESCE(src.VatNumber, src.GstNumber, src.EinNumber),
                CASE
                    WHEN src.VatNumber IS NOT NULL THEN N'VATIN'
                    WHEN src.GstNumber IS NOT NULL AND src.RegionCode = N'APAC' THEN N'GSTIN'
                    WHEN src.EinNumber IS NOT NULL AND src.RegionCode = N'NA'   THEN N'EIN'
                    ELSE N'UNKNOWN'
                END,
                CASE WHEN src.RegionCode = N'NA' THEN src.WithholdingCode ELSE NULL END,
                CASE WHEN src.TaxRegimeCode = N'VAT'
                     THEN CASE WHEN src.VatNumber IS NOT NULL
                                AND LEFT(src.VatNumber, 2) LIKE N'[A-Z][A-Z]' THEN 1 ELSE 0 END
                     ELSE NULL
                END,
                CASE WHEN src.TaxRegimeCode = N'GST'
                     THEN CASE WHEN src.GstNumber IS NOT NULL THEN 1 ELSE 0 END
                     ELSE NULL
                END,
                src.PaymentTermsCode,
                src.PaymentMethodCode,
                src.TransactionCurrencyCode,
                src.DefaultIncotermCode,
                src.LeadTimeDays,
                src.MinimumOrderAmount,
                src.MinimumOrderAmountUsd,
                src.ScorecardRatingCode,
                CASE WHEN src.RegionCode = N'NA' THEN src.DiversityClassCode ELSE NULL END,
                ISNULL(src.RegionCode, N'UNKNOWN'),
                src.LedgerCode,
                src.OnHoldFlag,
                src.HoldReasonCode,
                src.SourceCreatedDate,
                src.SourceModifiedDate,
                CASE WHEN src.MinimumOrderAmount IS NOT NULL AND src.MinimumOrderAmountUsd IS NULL
                     THEN N'WARN' ELSE N'PASS' END,
                HASHBYTES('SHA2_256',
                    CONCAT(src.SupplierName, N'|', src.SupplierStatusCode, N'|', src.PaymentTermsCode, N'|',
                           src.PaymentMethodCode, N'|', src.ScorecardRatingCode, N'|', src.OnHoldFlag)),
                HASHBYTES('SHA2_256',
                    CONCAT(src.SupplierName, N'|', src.VatNumber, N'|', src.GstNumber, N'|',
                           src.EinNumber, N'|', src.RegionCode, N'|', src.LedgerCode)),
                @BatchId,
                @PackageExecutionId
            )
        OUTPUT $action INTO #MergeAction (MergeAction);

        SELECT @InsertedRows = SUM(CASE WHEN MergeAction = N'INSERT' THEN 1 ELSE 0 END),
               @UpdatedRows  = SUM(CASE WHEN MergeAction = N'UPDATE' THEN 1 ELSE 0 END)
        FROM #MergeAction;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = @RejectedRows;

        IF @RejectedRows > 0
            EXEC err.usp_LogRejectedRows
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @RejectTableName    = N'err.RejectedSupplier',
                @ObjectName         = @ObjectName,
                @BusinessKeyColumn  = N'SupplierBusinessKey';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_LOAD_SUPPLIER',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_TruncateAndReload_Supplier';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
