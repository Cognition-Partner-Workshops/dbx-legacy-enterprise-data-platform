/*
    stg.usp_DeduplicateCustomer

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_DEDUP_CUSTOMER (SSIS), after STG_NORMALIZE_ADDRESS
    Reads/writes  : stg.Customer
    Writes        : work.CustomerDedup, ref.SourceKeyCrosswalk
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    ------------------------------------------------------------------------
    SURVIVORSHIP RULE SET (as agreed with the MDM working group, 2014, amended
    2019 for the EU consent rules)
    ------------------------------------------------------------------------
    Candidates are grouped by, in priority order:

      Rule 1  EXACT_TAXNUM  identical non-null tax registration number.
      Rule 2  NAME_POSTAL   identical standardised name and identical
                            standardised postal code of the primary address.
      Rule 3  NAME_FUZZY    identical first 12 characters of the standardised
                            name plus identical country, used only when neither
                            candidate has a tax number.

    Within a group the survivor is the row with the highest survivorship score:

        SourceRank            ORA_ERP 30, WWI_OLTP 20, WWI_WEB 10
        AttributeCompleteness 2 points per populated significant attribute
        Recency               10 points if SourceModifiedDate is the group max
        Consent               EU only: a row carrying an explicit opt-in never
                              loses to a row without one, because losing it
                              would silently re-grant consent.

    Ties break on the lowest CustomerBusinessKey so the outcome is stable across
    reruns. Losers keep their rows (IsSurvivorRow = 0) - nothing is deleted,
    because the fact loads still need to resolve historical keys - and
    ref.SourceKeyCrosswalk records the retirement.
    ------------------------------------------------------------------------
*/

IF OBJECT_ID(N'stg.usp_DeduplicateCustomer', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_DeduplicateCustomer;
GO

CREATE PROCEDURE stg.usp_DeduplicateCustomer
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @FuzzyPrefixLength  SMALLINT = 12
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName     NVARCHAR(200) = N'stg.Customer';
    DECLARE @CandidateRows  BIGINT = 0;
    DECLARE @GroupCount     BIGINT = 0;
    DECLARE @RetiredRows    BIGINT = 0;

    BEGIN TRY
        SELECT
            c.CustomerBusinessKey,
            c.SourceSystemCode,
            c.SourceCustomerId,
            c.RegionCode,
            c.SourceModifiedDate,
            c.MarketingConsentFlag,
            MatchKeyName    = c.CustomerNameStandardized,
            MatchKeyTaxNumber = NULLIF(REPLACE(REPLACE(ISNULL(c.TaxRegistrationNumber, N''), N'-', N''), N' ', N''), N''),
            MatchKeyPostal  = a.PostalCodeStandardized,
            MatchKeyCountry = ISNULL(c.PrimaryCountryCode, a.CountryCode),
            SourceRank      = CASE c.SourceSystemCode
                                  WHEN N'ORA_ERP'  THEN 30
                                  WHEN N'WWI_OLTP' THEN 20
                                  WHEN N'WWI_WEB'  THEN 10
                                  ELSE 5
                              END,
            AttributeCompleteness =
                  CASE WHEN c.CustomerLegalName        IS NOT NULL THEN 2 ELSE 0 END
                + CASE WHEN c.TaxRegistrationNumber    IS NOT NULL THEN 2 ELSE 0 END
                + CASE WHEN c.CreditLimitAmount        IS NOT NULL THEN 2 ELSE 0 END
                + CASE WHEN c.PaymentTermsCode         IS NOT NULL THEN 2 ELSE 0 END
                + CASE WHEN c.SalespersonBusinessKey   IS NOT NULL THEN 2 ELSE 0 END
                + CASE WHEN a.PostalCodeStandardized   IS NOT NULL THEN 2 ELSE 0 END
                + CASE WHEN c.AccountOpenedDate        IS NOT NULL THEN 2 ELSE 0 END
        INTO #Candidate
        FROM stg.Customer AS c
        OUTER APPLY
        (
            SELECT TOP (1) ca.PostalCodeStandardized, ca.CountryCode
            FROM stg.CustomerAddress AS ca
            WHERE ca.CustomerBusinessKey = c.CustomerBusinessKey
              AND ca.BatchId             = @BatchId
            ORDER BY ca.IsPrimaryAddress DESC,
                     CASE ca.AddressUsageCode WHEN N'BILLTO' THEN 0 ELSE 1 END,
                     ca.AddressBusinessKey
        ) AS a
        WHERE c.BatchId = @BatchId;

        SELECT @CandidateRows = COUNT_BIG(*) FROM #Candidate;

        --  Group assignment, highest-priority rule first. A row already assigned
        --  by a stronger rule is not reconsidered by a weaker one.
        SELECT
            k.CustomerBusinessKey,
            k.MatchRuleCode,
            k.GroupKey,
            DuplicateGroupId = DENSE_RANK() OVER (ORDER BY k.MatchRuleCode, k.GroupKey)
        INTO #Grouped
        FROM
        (
            SELECT
                c.CustomerBusinessKey,
                MatchRuleCode =
                    CASE
                        WHEN c.MatchKeyTaxNumber IS NOT NULL                             THEN N'EXACT_TAXNUM'
                        WHEN c.MatchKeyName IS NOT NULL AND c.MatchKeyPostal IS NOT NULL THEN N'NAME_POSTAL'
                        ELSE N'NAME_FUZZY'
                    END,
                GroupKey =
                    CASE
                        WHEN c.MatchKeyTaxNumber IS NOT NULL
                            THEN CONCAT(N'TAX|', c.MatchKeyTaxNumber)
                        WHEN c.MatchKeyName IS NOT NULL AND c.MatchKeyPostal IS NOT NULL
                            THEN CONCAT(N'NP|', c.MatchKeyName, N'|', c.MatchKeyPostal)
                        ELSE CONCAT(N'NF|', LEFT(ISNULL(c.MatchKeyName, N'?'), @FuzzyPrefixLength),
                                    N'|', ISNULL(c.MatchKeyCountry, N'??'))
                    END
            FROM #Candidate AS c
        ) AS k;

        SELECT @GroupCount = COUNT(DISTINCT g.DuplicateGroupId)
        FROM #Grouped AS g
        WHERE EXISTS
        (
            SELECT 1 FROM #Grouped AS g2
            WHERE g2.DuplicateGroupId = g.DuplicateGroupId
            GROUP BY g2.DuplicateGroupId
            HAVING COUNT(*) > 1
        );

        BEGIN TRANSACTION;

        DELETE FROM work.CustomerDedup
        WHERE BatchId = @BatchId;

        INSERT INTO work.CustomerDedup
        (
            BatchId, PackageExecutionId, DuplicateGroupId, CandidateCustomerBusinessKey,
            SourceSystemCode, SourceCustomerId, MatchKeyName, MatchKeyPostal, MatchKeyTaxNumber,
            MatchRuleCode, MatchScore, SurvivorshipScore, SourceRank, AttributeCompleteness,
            SourceModifiedDate, IsSelectedSurvivor, LosesToBusinessKey, DecisionNote
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            s.DuplicateGroupId,
            s.CustomerBusinessKey,
            s.SourceSystemCode,
            s.SourceCustomerId,
            s.MatchKeyName,
            s.MatchKeyPostal,
            s.MatchKeyTaxNumber,
            s.MatchRuleCode,
            CASE s.MatchRuleCode
                WHEN N'EXACT_TAXNUM' THEN 100.00
                WHEN N'NAME_POSTAL'  THEN 85.00
                ELSE 60.00
            END,
            s.SurvivorshipScore,
            s.SourceRank,
            s.AttributeCompleteness,
            s.SourceModifiedDate,
            CASE WHEN s.SurvivorRank = 1 THEN 1 ELSE 0 END,
            CASE WHEN s.SurvivorRank = 1 THEN NULL ELSE s.WinnerBusinessKey END,
            CASE
                WHEN s.GroupSize = 1        THEN N'singleton; no merge'
                WHEN s.SurvivorRank = 1     THEN CONCAT(N'survivor by ', s.MatchRuleCode,
                                                        N'; score ', CONVERT(NVARCHAR(20), s.SurvivorshipScore))
                ELSE CONCAT(N'retired in favour of ', s.WinnerBusinessKey,
                            N'; score ', CONVERT(NVARCHAR(20), s.SurvivorshipScore))
            END
        FROM
        (
            SELECT
                x.*,
                SurvivorRank = ROW_NUMBER() OVER
                (
                    PARTITION BY x.DuplicateGroupId
                    ORDER BY x.SurvivorshipScore DESC, x.CustomerBusinessKey
                ),
                GroupSize = COUNT(*) OVER (PARTITION BY x.DuplicateGroupId),
                WinnerBusinessKey = FIRST_VALUE(x.CustomerBusinessKey) OVER
                (
                    PARTITION BY x.DuplicateGroupId
                    ORDER BY x.SurvivorshipScore DESC, x.CustomerBusinessKey
                )
            FROM
            (
                SELECT
                    c.CustomerBusinessKey,
                    c.SourceSystemCode,
                    c.SourceCustomerId,
                    c.MatchKeyName,
                    c.MatchKeyPostal,
                    c.MatchKeyTaxNumber,
                    c.SourceRank,
                    c.AttributeCompleteness,
                    c.SourceModifiedDate,
                    g.MatchRuleCode,
                    g.DuplicateGroupId,
                    SurvivorshipScore = CONVERT(DECIMAL(9,4),
                          c.SourceRank
                        + c.AttributeCompleteness
                        + CASE
                              WHEN c.SourceModifiedDate = MAX(c.SourceModifiedDate)
                                                          OVER (PARTITION BY g.DuplicateGroupId)
                                  THEN 10 ELSE 0
                          END
                        --  EU consent guard: an explicit opt-in outranks everything.
                        + CASE
                              WHEN c.RegionCode = N'EU' AND c.MarketingConsentFlag = 1 THEN 50 ELSE 0
                          END)
                FROM #Candidate AS c
                INNER JOIN #Grouped AS g
                    ON g.CustomerBusinessKey = c.CustomerBusinessKey
            ) AS x
        ) AS s;

        --  Flag the losers on the staging rows themselves.
        UPDATE c
        SET c.IsSurvivorRow          = 0,
            c.DuplicateGroupId       = d.DuplicateGroupId,
            c.SurvivorshipRuleApplied = d.MatchRuleCode,
            c.DqStatusCode           = CASE WHEN c.DqStatusCode = N'PASS' THEN N'WARN' ELSE c.DqStatusCode END
        FROM stg.Customer AS c
        INNER JOIN work.CustomerDedup AS d
            ON  d.CandidateCustomerBusinessKey = c.CustomerBusinessKey
            AND d.BatchId                      = @BatchId
        WHERE c.BatchId            = @BatchId
          AND d.IsSelectedSurvivor = 0;

        SET @RetiredRows = @@ROWCOUNT;

        UPDATE c
        SET c.IsSurvivorRow           = 1,
            c.DuplicateGroupId        = d.DuplicateGroupId,
            c.SurvivorshipRuleApplied = d.MatchRuleCode
        FROM stg.Customer AS c
        INNER JOIN work.CustomerDedup AS d
            ON  d.CandidateCustomerBusinessKey = c.CustomerBusinessKey
            AND d.BatchId                      = @BatchId
        WHERE c.BatchId            = @BatchId
          AND d.IsSelectedSurvivor = 1;

        --  Record the retirement so downstream key lookups follow the survivor.
        MERGE ref.SourceKeyCrosswalk AS tgt
        USING
        (
            SELECT
                EntityName           = N'Customer',
                d.SourceSystemCode,
                SourceKeyValue       = d.SourceCustomerId,
                ConformedBusinessKey = d.CandidateCustomerBusinessKey,
                SupersededByBusinessKey = d.LosesToBusinessKey
            FROM work.CustomerDedup AS d
            WHERE d.BatchId          = @BatchId
              AND d.SourceCustomerId IS NOT NULL
        ) AS src
            ON  tgt.EntityName       = src.EntityName
            AND tgt.SourceSystemCode = src.SourceSystemCode
            AND tgt.SourceKeyValue   = src.SourceKeyValue
        WHEN MATCHED THEN
            UPDATE SET
                tgt.ConformedBusinessKey    = src.ConformedBusinessKey,
                tgt.SupersededByBusinessKey = src.SupersededByBusinessKey,
                tgt.MatchMethodCode         = CASE WHEN src.SupersededByBusinessKey IS NULL
                                                   THEN N'LOADED' ELSE N'DEDUP_SURVIVOR' END,
                tgt.IsActive                = CASE WHEN src.SupersededByBusinessKey IS NULL THEN 1 ELSE 0 END
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (EntityName, SourceSystemCode, SourceKeyValue, ConformedBusinessKey,
                    MatchMethodCode, SupersededByBusinessKey, IsActive)
            VALUES (src.EntityName, src.SourceSystemCode, src.SourceKeyValue, src.ConformedBusinessKey,
                    CASE WHEN src.SupersededByBusinessKey IS NULL THEN N'LOADED' ELSE N'DEDUP_SURVIVOR' END,
                    src.SupersededByBusinessKey,
                    CASE WHEN src.SupersededByBusinessKey IS NULL THEN 1 ELSE 0 END);

        COMMIT TRANSACTION;

        DECLARE @TargetRowCountValue BIGINT = @CandidateRows - @RetiredRows;
        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @CandidateRows,
            @TargetRowCount     = @TargetRowCountValue,
            @UpdateRowCount     = @RetiredRows,
            @DeleteRowCount     = 0;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_DEDUP_CUSTOMER',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_DeduplicateCustomer';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
