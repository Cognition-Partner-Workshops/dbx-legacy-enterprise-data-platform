/*
    stg.usp_NormalizeCustomer

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_NORMALIZE_CUSTOMER (SSIS), after stg.usp_TruncateAndReload_Customer
    Reads/writes  : stg.Customer
    Reads         : ref.Region, ref.Country
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    In-place normalisation of the staged customer rows. Everything here is a
    presentation or matching concern rather than a typing concern, which is why
    it is a separate pass: the load procedure has to stay fast and this pass gets
    re-run on its own whenever the suffix list changes.

    What it does:
      * builds CustomerNameStandardized - upper case, punctuation removed,
        trailing legal suffix stripped (INC, LLC, LTD, GMBH, SARL, BV, PTY LTD,
        KK, PTE LTD) so the dedup pass can match across regions;
      * validates the tax registration number against the regional mask;
      * applies the regional consent model to MarketingConsentFlag;
      * derives RetentionExpiryDate from the regional retention window;
      * recomputes RowHash and ChangeHash after all of the above.

    Consent divergence is the important part:
        NA   opt-out. A null consent flag means the customer may be marketed to.
        EU   opt-in. A null consent flag means no consent, and a consent date
             older than the regional retention window expires the consent.
        APAC opt-in for the two countries with a private-data act, opt-out
             elsewhere; the country list is ref.Country.StateProvinceRequiredFlag
             era data, so the rule is keyed on the region default instead.
*/

IF OBJECT_ID(N'stg.usp_NormalizeCustomer', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_NormalizeCustomer;
GO

CREATE PROCEDURE stg.usp_NormalizeCustomer
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.Customer';
    DECLARE @UpdatedRows  BIGINT = 0;
    DECLARE @TargetRows   BIGINT = 0;

    BEGIN TRY
        SELECT @TargetRows = COUNT_BIG(*)
        FROM stg.Customer AS c
        WHERE c.BatchId = @BatchId;

        BEGIN TRANSACTION;

        UPDATE c
        SET
            c.CustomerNameStandardized = s.StrippedName,
            c.TaxRegistrationValidFlag =
                CASE
                    WHEN c.TaxRegistrationNumber IS NULL THEN NULL
                    WHEN rg.RegionCode = N'EU'
                        THEN CASE
                                 WHEN LEFT(c.TaxRegistrationNumber, 2) = c.PrimaryCountryCode
                                  AND LEN(c.TaxRegistrationNumber) BETWEEN 8 AND 14 THEN 1
                                 ELSE 0
                             END
                    WHEN rg.RegionCode = N'NA'
                        THEN CASE WHEN c.TaxRegistrationNumber NOT LIKE N'%[^0-9-]%' THEN 1 ELSE 0 END
                    ELSE CASE WHEN LEN(c.TaxRegistrationNumber) >= 8 THEN 1 ELSE 0 END
                END,
            c.MarketingConsentFlag =
                CASE rg.ConsentModelCode
                    WHEN N'OPT_IN' THEN
                        CASE
                            WHEN c.MarketingConsentFlag = 1
                             AND c.MarketingConsentDate IS NOT NULL
                             AND c.MarketingConsentDate >= DATEADD(MONTH, -rg.DefaultRetentionMonths, CONVERT(DATE, SYSUTCDATETIME()))
                                THEN 1
                            ELSE 0
                        END
                    ELSE ISNULL(c.MarketingConsentFlag, 1)
                END,
            c.RetentionClassCode =
                CASE
                    WHEN rg.ConsentModelCode = N'OPT_IN' AND c.CustomerStatusCode = N'CLOSED' THEN N'PURGE_SHORT'
                    WHEN rg.ConsentModelCode = N'OPT_IN'                                      THEN N'GDPR_STANDARD'
                    WHEN c.CustomerStatusCode = N'CLOSED'                                     THEN N'ARCHIVE'
                    ELSE N'STANDARD'
                END,
            c.RetentionExpiryDate =
                DATEADD(MONTH, rg.DefaultRetentionMonths,
                        COALESCE(c.LastActivityDate, c.AccountOpenedDate, CONVERT(DATE, c.LoadedAtUtc))),
            c.RowHash = HASHBYTES('SHA2_256',
                CONCAT(s.StrippedName, N'|', c.CustomerCategoryCode, N'|', c.CustomerStatusCode, N'|',
                       c.PaymentTermsCode, N'|', c.SalespersonBusinessKey, N'|', c.BuyingGroupName)),
            c.ChangeHash = HASHBYTES('SHA2_256',
                CONCAT(c.CreditLimitAmountUsd, N'|', c.CreditRatingCode, N'|', c.ParentCustomerBusinessKey, N'|',
                       c.PrimaryCountryCode, N'|', c.TaxRegistrationNumber))
        FROM stg.Customer AS c
        INNER JOIN ref.Region AS rg
            ON rg.RegionCode = c.RegionCode
        CROSS APPLY
        (
            --  Punctuation out, then the legal suffix off the end. Done with
            --  nested REPLACE because the 2006 version of this ran on SQL 2000
            --  and nobody has rewritten it.
            SELECT UpperName =
                LTRIM(RTRIM(UPPER(
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        ISNULL(c.CustomerLegalName, c.CustomerName),
                        N'.', N' '), N',', N' '), N'''', N''), N'-', N' '), N'&', N' AND '), N'  ', N' ')
                )))
        ) AS u
        CROSS APPLY
        (
            SELECT StrippedName =
                LEFT(LTRIM(RTRIM(
                    CASE
                        WHEN u.UpperName LIKE N'% PTY LTD' THEN LEFT(u.UpperName, LEN(u.UpperName) - 8)
                        WHEN u.UpperName LIKE N'% PTE LTD' THEN LEFT(u.UpperName, LEN(u.UpperName) - 8)
                        WHEN u.UpperName LIKE N'% GMBH'    THEN LEFT(u.UpperName, LEN(u.UpperName) - 5)
                        WHEN u.UpperName LIKE N'% SARL'    THEN LEFT(u.UpperName, LEN(u.UpperName) - 5)
                        WHEN u.UpperName LIKE N'% LTD'     THEN LEFT(u.UpperName, LEN(u.UpperName) - 4)
                        WHEN u.UpperName LIKE N'% LLC'     THEN LEFT(u.UpperName, LEN(u.UpperName) - 4)
                        WHEN u.UpperName LIKE N'% INC'     THEN LEFT(u.UpperName, LEN(u.UpperName) - 4)
                        WHEN u.UpperName LIKE N'% BV'      THEN LEFT(u.UpperName, LEN(u.UpperName) - 3)
                        WHEN u.UpperName LIKE N'% KK'      THEN LEFT(u.UpperName, LEN(u.UpperName) - 3)
                        ELSE u.UpperName
                    END
                )), 200)
        ) AS s
        WHERE c.BatchId = @BatchId;

        SET @UpdatedRows = @@ROWCOUNT;

        --  A customer whose consent has just been withdrawn by the opt-in rule
        --  is flagged so the dimension load can suppress the marketing columns.
        UPDATE c
        SET c.DqStatusCode = N'WARN'
        FROM stg.Customer AS c
        INNER JOIN ref.Region AS rg
            ON rg.RegionCode = c.RegionCode
        WHERE c.BatchId              = @BatchId
          AND rg.ConsentModelCode    = N'OPT_IN'
          AND c.MarketingConsentFlag = 0
          AND c.MarketingConsentDate IS NOT NULL
          AND c.DqStatusCode         = N'PASS';

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @TargetRows,
            @TargetRowCount     = @TargetRows,
            @UpdateRowCount     = @UpdatedRows;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_NORMALIZE_CUSTOMER',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_NormalizeCustomer';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
