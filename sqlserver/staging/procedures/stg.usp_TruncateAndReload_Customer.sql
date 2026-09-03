/*
    stg.usp_TruncateAndReload_Customer

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_LOAD_CUSTOMER (SSIS)
    Reads         : raw.OracleCustomerMaster, raw.OracleCustomerAddress,
                    ref.CodeCrosswalk, ref.Country, ref.Region, stg.PaymentTerms
    Writes        : stg.Customer, err.RejectedCustomer
    Control       : etl.usp_LogRowCount, etl.usp_LogRejectedRecord, etl.usp_LogError

    Full reload of the customer master for the batch. The ERP is small enough to
    reload every night and the merge team has never trusted the Oracle
    LAST_UPDATE_DT, which is not maintained by the two regional data-entry
    screens, so this is a delete-then-insert rather than an incremental load.

    The delete is scoped to the batch rather than the whole table: the previous
    batch's rows are kept until the housekeeping job runs so the morning
    reconciliation can diff two consecutive loads.

    Validation and reject rules
        MISSING_NAME   CUST_NAME empty after cleaning                 -> reject
        BAD_COUNTRY    country not in ref.Country                     -> reject
        BAD_REGION     REGION_CD not in ref.Region                    -> reject
        BAD_CREDIT     credit limit will not type as a decimal        -> reject
        NO_CONSENT     EU row with no explicit opt-in                 -> reject
        STALE_ACCOUNT  last activity older than the retention window  -> WARN only

    Regional divergence
        NA   consent is opt-out, so a NULL consent flag means consentable.
        EU   consent is opt-in and must carry a consent date; rows without one
             are rejected rather than defaulted, which is the 2018 privacy rule.
        APAC follows the EU model for JP and AU and the NA model everywhere else,
             which is a compromise nobody has ever revisited.
*/

IF OBJECT_ID(N'stg.usp_TruncateAndReload_Customer', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_TruncateAndReload_Customer;
GO

CREATE PROCEDURE stg.usp_TruncateAndReload_Customer
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName     NVARCHAR(200) = N'stg.Customer';
    DECLARE @SourceRows     BIGINT = 0;
    DECLARE @InsertedRows   BIGINT = 0;
    DECLARE @DeletedRows    BIGINT = 0;
    DECLARE @RejectedRows   BIGINT = 0;
    DECLARE @RetentionCutoff DATE  = DATEADD(YEAR, -7, CAST(SYSUTCDATETIME() AS DATE));

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleCustomerMaster AS r
        WHERE r.BatchId = @BatchId;

        BEGIN TRANSACTION;

        DELETE FROM stg.Customer
        WHERE BatchId = @BatchId;

        SET @DeletedRows = @@ROWCOUNT;

        /*  Typed projection of the batch. Everything downstream reads from this
            temp table twice: once to reject and once to insert, so the string
            handling only runs once. */
        CREATE TABLE #TypedCustomer
        (
            SourceCustomerId        NVARCHAR(50)    NULL,
            ErpCustomerNumber       NVARCHAR(50)    NULL,
            CustomerBusinessKey     NVARCHAR(100)   NULL,
            CustomerName            NVARCHAR(200)   NULL,
            CustomerLegalName       NVARCHAR(200)   NULL,
            ParentSourceCustomerId  NVARCHAR(50)    NULL,
            CustomerCategoryCode    NVARCHAR(20)    NULL,
            CustomerStatusCode      NVARCHAR(20)    NULL,
            CreditLimitText         NVARCHAR(50)    NULL,
            CreditLimitAmount       DECIMAL(19,4)   NULL,
            CreditLimitCurrencyCode NCHAR(3)        NULL,
            CreditRatingCode        NVARCHAR(10)    NULL,
            PaymentTermsCode        NVARCHAR(20)    NULL,
            TaxRegistrationNumber   NVARCHAR(50)    NULL,
            RegionCode              NVARCHAR(10)    NULL,
            LedgerCode              NVARCHAR(20)    NULL,
            PrimaryCountryCode      NVARCHAR(10)    NULL,
            SalespersonBusinessKey  NVARCHAR(100)   NULL,
            BuyingGroupName         NVARCHAR(100)   NULL,
            MarketingConsentRaw     NVARCHAR(5)     NULL,
            MarketingConsentDate    DATE            NULL,
            RetentionClassCode      NVARCHAR(20)    NULL,
            AccountOpenedDate       DATE            NULL,
            LastActivityDate        DATE            NULL,
            SourceCreatedDate       DATETIME2(3)    NULL,
            SourceModifiedDate      DATETIME2(3)    NULL,
            RejectReasonCode        NVARCHAR(50)    NULL,
            RejectReason            NVARCHAR(500)   NULL,
            FailedColumnName        NVARCHAR(100)   NULL,
            FailedValue             NVARCHAR(400)   NULL,
            RecordPayload           NVARCHAR(MAX)   NULL
        );

        INSERT INTO #TypedCustomer
        (
            SourceCustomerId, ErpCustomerNumber, CustomerBusinessKey, CustomerName, CustomerLegalName,
            ParentSourceCustomerId, CustomerCategoryCode, CustomerStatusCode, CreditLimitText,
            CreditLimitAmount, CreditLimitCurrencyCode, CreditRatingCode, PaymentTermsCode,
            TaxRegistrationNumber, RegionCode, LedgerCode, PrimaryCountryCode, SalespersonBusinessKey,
            BuyingGroupName, MarketingConsentRaw, MarketingConsentDate, RetentionClassCode,
            AccountOpenedDate, LastActivityDate, SourceCreatedDate, SourceModifiedDate, RecordPayload
        )
        SELECT
            LTRIM(RTRIM(r.CUST_ID)),
            stg.ufn_CleanString(r.CUST_NUMBER, 1),
            stg.ufn_SourceSystemKey(r.SourceSystemCode, r.CUST_ID, 1),
            LEFT(stg.ufn_CleanString(r.CUST_NAME, 0), 200),
            LEFT(stg.ufn_CleanString(r.CUST_LEGAL_NAME, 0), 200),
            LTRIM(RTRIM(r.PARENT_CUST_ID)),
            xw.ConformedCodeValue,
            COALESCE(sx.ConformedCodeValue,
                     CASE UPPER(LTRIM(RTRIM(r.CUST_STATUS_CD)))
                          WHEN N'A' THEN N'ACTIVE'
                          WHEN N'I' THEN N'INACTIVE'
                          WHEN N'H' THEN N'ON_HOLD'
                          WHEN N'P' THEN N'PROSPECT'
                          WHEN N'X' THEN N'CLOSED'
                          ELSE N'UNKNOWN'
                     END),
            r.CREDIT_LIMIT_AMT,
            CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(r.CREDIT_LIMIT_AMT, rg.DecimalSeparator)),
            NULLIF(LEFT(UPPER(LTRIM(RTRIM(r.CURRENCY_CD))), 3), N''),
            NULLIF(UPPER(LTRIM(RTRIM(r.CREDIT_RATING_CD))), N''),
            NULLIF(UPPER(LTRIM(RTRIM(r.PAYMENT_TERMS_CD))), N''),
            NULLIF(REPLACE(UPPER(LTRIM(RTRIM(r.TAX_REGISTRATION_NUM))), N' ', N''), N''),
            NULLIF(UPPER(LTRIM(RTRIM(r.REGION_CD))), N''),
            NULLIF(UPPER(LTRIM(RTRIM(r.LEDGER_CD))), N''),
            addr.CountryCode,
            stg.ufn_SourceSystemKey(r.SourceSystemCode, r.SALES_REP_ID, 1),
            LEFT(stg.ufn_CleanString(r.BUYING_GROUP_NAME, 0), 100),
            UPPER(LTRIM(RTRIM(r.MARKETING_CONSENT_FLG))),
            stg.ufn_SafeDate(r.CONSENT_DT, r.REGION_CD),
            NULLIF(UPPER(LTRIM(RTRIM(r.RETENTION_CLASS_CD))), N''),
            stg.ufn_SafeDate(r.ACCOUNT_OPENED_DT, r.REGION_CD),
            stg.ufn_SafeDate(r.LAST_ACTIVITY_DT, r.REGION_CD),
            CONVERT(DATETIME2(3), stg.ufn_SafeDate(r.CREATED_DT, r.REGION_CD)),
            CONVERT(DATETIME2(3), stg.ufn_SafeDate(r.LAST_UPDATE_DT, r.REGION_CD)),
            CONCAT(N'{"CUST_ID":"', r.CUST_ID,
                   N'","CUST_NAME":"', REPLACE(ISNULL(r.CUST_NAME, N''), N'"', N''''),
                   N'","REGION_CD":"', r.REGION_CD,
                   N'","CREDIT_LIMIT_AMT":"', r.CREDIT_LIMIT_AMT, N'"}')
        FROM raw.OracleCustomerMaster AS r
        LEFT JOIN ref.Region AS rg
            ON rg.RegionCode = UPPER(LTRIM(RTRIM(r.REGION_CD)))
        LEFT JOIN ref.CodeCrosswalk AS xw
            ON  xw.CodeDomainCode   = N'CUSTOMER_CATEGORY'
            AND xw.SourceSystemCode = r.SourceSystemCode
            AND xw.SourceCodeValue  = r.CUST_TYPE_CD
            AND xw.EffectiveToDate IS NULL
        LEFT JOIN ref.CodeCrosswalk AS sx
            ON  sx.CodeDomainCode   = N'CUSTOMER_STATUS'
            AND sx.SourceSystemCode = r.SourceSystemCode
            AND sx.SourceCodeValue  = r.CUST_STATUS_CD
            AND sx.EffectiveToDate IS NULL
        OUTER APPLY
        (
            SELECT TOP (1) LEFT(UPPER(LTRIM(RTRIM(a.COUNTRY_CD))), 2) AS CountryCode
            FROM raw.OracleCustomerAddress AS a
            WHERE a.CUST_ID = r.CUST_ID
              AND a.BatchId = r.BatchId
            ORDER BY CASE WHEN UPPER(a.PRIMARY_FLG) = N'Y' THEN 0 ELSE 1 END,
                     CASE WHEN a.ADDRESS_USAGE_CD = N'BILL' THEN 0 ELSE 1 END,
                     a.ADDRESS_ID
        ) AS addr
        WHERE r.BatchId = @BatchId
          AND (@SourceSystemCode IS NULL OR r.SourceSystemCode = @SourceSystemCode);

        /*  Reject evaluation. The order matters: the first rule that fires wins,
            because the AP team reports on reject reason and wants one reason per
            row rather than a list. */
        UPDATE t
        SET RejectReasonCode = N'MISSING_NAME',
            RejectReason     = N'CUST_NAME is empty or a placeholder value after cleaning',
            FailedColumnName = N'CUST_NAME',
            FailedValue      = t.CustomerName
        FROM #TypedCustomer AS t
        WHERE t.RejectReasonCode IS NULL
          AND t.CustomerName IS NULL;

        UPDATE t
        SET RejectReasonCode = N'BAD_REGION',
            RejectReason     = N'REGION_CD is not a known conformed region',
            FailedColumnName = N'REGION_CD',
            FailedValue      = t.RegionCode
        FROM #TypedCustomer AS t
        WHERE t.RejectReasonCode IS NULL
          AND NOT EXISTS (SELECT 1 FROM ref.Region AS rg WHERE rg.RegionCode = t.RegionCode);

        UPDATE t
        SET RejectReasonCode = N'BAD_COUNTRY',
            RejectReason     = N'primary address country is not in ref.Country',
            FailedColumnName = N'COUNTRY_CD',
            FailedValue      = t.PrimaryCountryCode
        FROM #TypedCustomer AS t
        WHERE t.RejectReasonCode IS NULL
          AND t.PrimaryCountryCode IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM ref.Country AS c WHERE c.CountryCode = t.PrimaryCountryCode);

        UPDATE t
        SET RejectReasonCode = N'BAD_CREDIT',
            RejectReason     = N'CREDIT_LIMIT_AMT does not convert to a decimal',
            FailedColumnName = N'CREDIT_LIMIT_AMT',
            FailedValue      = t.CreditLimitText
        FROM #TypedCustomer AS t
        WHERE t.RejectReasonCode IS NULL
          AND NULLIF(LTRIM(RTRIM(t.CreditLimitText)), N'') IS NOT NULL
          AND t.CreditLimitAmount IS NULL;

        --  EU opt-in rule. NA and the non-JP/AU part of APAC are opt-out and
        --  never reach this rule.
        UPDATE t
        SET RejectReasonCode = N'NO_CONSENT',
            RejectReason     = N'EU row carries no explicit marketing opt-in with a consent date',
            FailedColumnName = N'MARKETING_CONSENT_FLG',
            FailedValue      = t.MarketingConsentRaw
        FROM #TypedCustomer AS t
        INNER JOIN ref.Region AS rg
            ON rg.RegionCode = t.RegionCode
        WHERE t.RejectReasonCode IS NULL
          AND rg.ConsentModelCode = N'OPT_IN'
          AND (t.MarketingConsentRaw <> N'Y' OR t.MarketingConsentDate IS NULL);

        INSERT INTO err.RejectedCustomer
        (
            BatchId, PackageExecutionId, SourceSystemCode, SourceCustomerId, CustomerBusinessKey,
            CustomerName, RejectReasonCode, RejectReason, RejectStage, FailedColumnName, FailedValue,
            RegionCode, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, @SourceSystemCode, t.SourceCustomerId, t.CustomerBusinessKey,
            t.CustomerName, t.RejectReasonCode, t.RejectReason, N'Stage', t.FailedColumnName, t.FailedValue,
            t.RegionCode, t.RecordPayload
        FROM #TypedCustomer AS t
        WHERE t.RejectReasonCode IS NOT NULL;

        SET @RejectedRows = @@ROWCOUNT;

        INSERT INTO stg.Customer
        (
            CustomerBusinessKey, SourceSystemCode, SourceCustomerId, ErpCustomerNumber,
            CustomerName, CustomerLegalName, CustomerNameStandardized, ParentCustomerBusinessKey,
            BuyingGroupName, CustomerCategoryCode, CustomerStatusCode, CreditLimitAmount,
            CreditLimitCurrencyCode, CreditRatingCode, PaymentTermsCode, StandardTermsNetDays,
            TaxRegistrationNumber, TaxRegistrationValidFlag, RegionCode, LedgerCode,
            PrimaryCountryCode, SalespersonBusinessKey, MarketingConsentFlag, MarketingConsentDate,
            RetentionClassCode, RetentionExpiryDate, AccountOpenedDate, LastActivityDate,
            SourceCreatedDate, SourceModifiedDate, DqStatusCode, RowHash, ChangeHash,
            BatchId, PackageExecutionId
        )
        SELECT
            t.CustomerBusinessKey,
            @SourceSystemCode,
            t.SourceCustomerId,
            t.ErpCustomerNumber,
            t.CustomerName,
            t.CustomerLegalName,
            UPPER(t.CustomerName),
            NULLIF(stg.ufn_SourceSystemKey(@SourceSystemCode, t.ParentSourceCustomerId, 1), t.CustomerBusinessKey),
            t.BuyingGroupName,
            ISNULL(t.CustomerCategoryCode, N'UNCLASSIFIED'),
            t.CustomerStatusCode,
            t.CreditLimitAmount,
            COALESCE(t.CreditLimitCurrencyCode, rg.DefaultCurrencyCode),
            t.CreditRatingCode,
            t.PaymentTermsCode,
            pt.NetDays,
            t.TaxRegistrationNumber,
            CASE
                WHEN t.TaxRegistrationNumber IS NULL THEN NULL
                WHEN rg.TaxRegimeCode = N'VAT'
                     THEN CASE WHEN t.TaxRegistrationNumber LIKE N'[A-Z][A-Z]%' AND LEN(t.TaxRegistrationNumber) BETWEEN 8 AND 14
                               THEN 1 ELSE 0 END
                WHEN rg.TaxRegimeCode = N'GST'
                     THEN CASE WHEN LEN(t.TaxRegistrationNumber) BETWEEN 9 AND 15 THEN 1 ELSE 0 END
                ELSE CASE WHEN t.TaxRegistrationNumber NOT LIKE N'%[^0-9-]%' THEN 1 ELSE 0 END
            END,
            t.RegionCode,
            t.LedgerCode,
            LEFT(t.PrimaryCountryCode, 2),
            t.SalespersonBusinessKey,
            CASE
                WHEN rg.ConsentModelCode = N'OPT_IN' THEN CASE WHEN t.MarketingConsentRaw = N'Y' THEN 1 ELSE 0 END
                ELSE CASE WHEN t.MarketingConsentRaw = N'N' THEN 0 ELSE 1 END
            END,
            t.MarketingConsentDate,
            t.RetentionClassCode,
            DATEADD(MONTH,
                    CASE t.RetentionClassCode
                         WHEN N'SHORT'    THEN 24
                         WHEN N'STANDARD' THEN rg.DefaultRetentionMonths
                         WHEN N'LONG'     THEN 120
                         ELSE rg.DefaultRetentionMonths
                    END,
                    COALESCE(t.LastActivityDate, t.AccountOpenedDate, CAST(SYSUTCDATETIME() AS DATE))),
            t.AccountOpenedDate,
            t.LastActivityDate,
            t.SourceCreatedDate,
            t.SourceModifiedDate,
            CASE WHEN t.LastActivityDate IS NOT NULL AND t.LastActivityDate < @RetentionCutoff
                 THEN N'WARN' ELSE N'PASS' END,
            HASHBYTES('SHA2_256',
                CONCAT(t.CustomerName, N'|', t.CustomerStatusCode, N'|', t.CustomerCategoryCode, N'|',
                       t.CreditRatingCode, N'|', t.PaymentTermsCode, N'|', t.BuyingGroupName)),
            HASHBYTES('SHA2_256',
                CONCAT(t.CustomerName, N'|', t.PrimaryCountryCode, N'|', t.RegionCode, N'|',
                       t.TaxRegistrationNumber, N'|', t.ParentSourceCustomerId)),
            @BatchId,
            @PackageExecutionId
        FROM #TypedCustomer AS t
        LEFT JOIN ref.Region AS rg
            ON rg.RegionCode = t.RegionCode
        LEFT JOIN stg.PaymentTerms AS pt
            ON  pt.PaymentTermsCode = t.PaymentTermsCode
            AND pt.BatchId          = @BatchId
        WHERE t.RejectReasonCode IS NULL;

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        /*  Reject detail goes to the control framework one row at a time. It is
            slow and everybody knows it, but the reject viewer reads
            etl.RejectedRecord and nothing else. */
        DECLARE @RejKey     NVARCHAR(200);
        DECLARE @RejCode    NVARCHAR(50);
        DECLARE @RejText    NVARCHAR(500);
        DECLARE @RejPayload NVARCHAR(MAX);

        DECLARE reject_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT e.CustomerBusinessKey, e.RejectReasonCode, e.RejectReason, e.RecordPayload
            FROM err.RejectedCustomer AS e
            WHERE e.BatchId = @BatchId
              AND e.ReprocessStatusCode = N'NEW';

        OPEN reject_cursor;
        FETCH NEXT FROM reject_cursor INTO @RejKey, @RejCode, @RejText, @RejPayload;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = @SourceSystemCode,
                @ObjectName         = @ObjectName,
                @BusinessKey        = @RejKey,
                @RejectReasonCode   = @RejCode,
                @RejectReason       = @RejText,
                @RejectStage        = N'Stage',
                @RecordPayload      = @RejPayload;

            FETCH NEXT FROM reject_cursor INTO @RejKey, @RejCode, @RejText, @RejPayload;
        END;

        CLOSE reject_cursor;
        DEALLOCATE reject_cursor;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @DeleteRowCount     = @DeletedRows,
            @RejectRowCount     = @RejectedRows;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF CURSOR_STATUS('local', 'reject_cursor') >= 0
        BEGIN
            CLOSE reject_cursor;
            DEALLOCATE reject_cursor;
        END;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_LOAD_CUSTOMER',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_TruncateAndReload_Customer';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
