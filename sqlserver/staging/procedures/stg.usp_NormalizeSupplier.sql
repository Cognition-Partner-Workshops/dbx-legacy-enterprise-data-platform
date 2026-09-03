/*
    stg.usp_NormalizeSupplier

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_NORMALIZE_SUPPLIER (SSIS), after stg.usp_TruncateAndReload_Supplier
    Reads/writes  : stg.Supplier
    Reads         : ref.Region, ref.Country, stg.VendorContract, stg.PaymentTerms
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Supplier normalisation differs from customer normalisation in what it cares
    about: procurement matches suppliers on DUNS and tax identifier rather than
    on name, so the name work here is minimal and the identifier work is not.

    Tax identifier typing by region - the source sends one TaxIdentifier column
    and the type has to be inferred:
        NA   nine digits, optionally hyphenated after two -> EIN. A supplier with
             an EIN and no withholding code gets the default 1099 class, because
             the AP team would rather over-report than miss a filing.
        EU   country prefix plus 8-12 alphanumerics -> VATIN. Recovery
             eligibility follows the prefix matching an EU member state that had
             not exited on the contract date.
        APAC 15 characters -> GSTIN, 11 digits -> ABN. Both set GstRegisteredFlag.

    Payment method is left alone here; it is set on the payment rows themselves
    because the supplier default and the actual method disagree constantly.
*/

IF OBJECT_ID(N'stg.usp_NormalizeSupplier', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_NormalizeSupplier;
GO

CREATE PROCEDURE stg.usp_NormalizeSupplier
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @DefaultWithholdingCode NVARCHAR(20) = N'1099-MISC'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName  NVARCHAR(200) = N'stg.Supplier';
    DECLARE @UpdatedRows BIGINT = 0;
    DECLARE @TargetRows  BIGINT = 0;
    DECLARE @HeldRows    BIGINT = 0;

    BEGIN TRY
        SELECT @TargetRows = COUNT_BIG(*)
        FROM stg.Supplier AS s
        WHERE s.BatchId = @BatchId;

        BEGIN TRANSACTION;

        UPDATE s
        SET
            s.SupplierNameStandardized = LEFT(LTRIM(RTRIM(UPPER(
                    REPLACE(REPLACE(REPLACE(s.SupplierName, N'.', N' '), N',', N' '), N'  ', N' ')))), 200),
            s.SupplierShortName = LEFT(LTRIM(RTRIM(UPPER(s.SupplierName))), 100),
            s.DunsNumber = CASE
                               WHEN s.DunsNumber IS NULL THEN NULL
                               WHEN REPLACE(REPLACE(s.DunsNumber, N'-', N''), N' ', N'') LIKE N'%[^0-9]%' THEN NULL
                               ELSE RIGHT(N'000000000' + REPLACE(REPLACE(s.DunsNumber, N'-', N''), N' ', N''), 9)
                           END,
            s.TaxIdentifier = t.CleanIdentifier,
            s.TaxIdentifierTypeCode = t.IdentifierType,
            s.WithholdingCode =
                CASE
                    WHEN rg.RegionCode <> N'NA'                       THEN NULL
                    WHEN t.IdentifierType <> N'EIN'                   THEN s.WithholdingCode
                    WHEN s.WithholdingCode IS NOT NULL                THEN s.WithholdingCode
                    ELSE @DefaultWithholdingCode
                END,
            s.VatRecoveryEligibleFlag =
                CASE
                    WHEN rg.RegionCode <> N'EU'      THEN NULL
                    WHEN t.IdentifierType <> N'VATIN' THEN 0
                    WHEN cn.IsEuMemberState = 1
                     AND (cn.EuExitDate IS NULL OR cn.EuExitDate > CONVERT(DATE, SYSUTCDATETIME())) THEN 1
                    ELSE 0
                END,
            s.GstRegisteredFlag =
                CASE
                    WHEN rg.RegionCode <> N'APAC'                   THEN NULL
                    WHEN t.IdentifierType IN (N'GSTIN', N'ABN')     THEN 1
                    ELSE 0
                END,
            s.RowHash = HASHBYTES('SHA2_256',
                CONCAT(LEFT(LTRIM(RTRIM(UPPER(s.SupplierName))), 200), N'|', s.SupplierCategoryCode, N'|', s.SupplierStatusCode, N'|',
                       s.PaymentTermsCode, N'|', s.DefaultIncotermCode, N'|', s.ScorecardRatingCode)),
            s.ChangeHash = HASHBYTES('SHA2_256',
                CONCAT(t.CleanIdentifier, N'|', s.DunsNumber, N'|', s.TransactionCurrencyCode, N'|',
                       s.MinimumOrderAmountUsd, N'|', s.DiversityClassCode))
        FROM stg.Supplier AS s
        INNER JOIN ref.Region AS rg
            ON rg.RegionCode = s.RegionCode
        LEFT JOIN ref.Country AS cn
            ON cn.CountryCode = LEFT(s.TaxIdentifier, 2)
        CROSS APPLY
        (
            SELECT CleanIdentifier =
                NULLIF(UPPER(REPLACE(REPLACE(REPLACE(ISNULL(s.TaxIdentifier, N''), N'-', N''), N' ', N''), N'/', N'')), N'')
        ) AS c0
        CROSS APPLY
        (
            SELECT
                c0.CleanIdentifier,
                IdentifierType =
                    CASE
                        WHEN c0.CleanIdentifier IS NULL THEN NULL
                        WHEN rg.RegionCode = N'NA'
                             AND LEN(c0.CleanIdentifier) = 9
                             AND c0.CleanIdentifier NOT LIKE N'%[^0-9]%'          THEN N'EIN'
                        WHEN rg.RegionCode = N'EU'
                             AND LEN(c0.CleanIdentifier) BETWEEN 10 AND 14
                             AND LEFT(c0.CleanIdentifier, 2) NOT LIKE N'%[^A-Z]%' THEN N'VATIN'
                        WHEN rg.RegionCode = N'APAC' AND LEN(c0.CleanIdentifier) = 15 THEN N'GSTIN'
                        WHEN rg.RegionCode = N'APAC' AND LEN(c0.CleanIdentifier) = 11
                             AND c0.CleanIdentifier NOT LIKE N'%[^0-9]%'          THEN N'ABN'
                        ELSE N'UNKNOWN'
                    END
        ) AS t
        WHERE s.BatchId = @BatchId;

        SET @UpdatedRows = @@ROWCOUNT;

        --  Procurement policy: a supplier with an unrecognised tax identifier is
        --  put on hold rather than rejected, so existing POs keep working while
        --  the vendor master team fixes the record.
        UPDATE s
        SET s.OnHoldFlag     = 1,
            s.HoldReasonCode = N'TAXID_INVALID',
            s.DqStatusCode   = N'WARN'
        FROM stg.Supplier AS s
        WHERE s.BatchId               = @BatchId
          AND s.TaxIdentifierTypeCode = N'UNKNOWN'
          AND s.OnHoldFlag            = 0;

        SET @HeldRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @TargetRows,
            @TargetRowCount     = @TargetRows,
            @UpdateRowCount     = @UpdatedRows + @HeldRows;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_NORMALIZE_SUPPLIER',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_NormalizeSupplier';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
