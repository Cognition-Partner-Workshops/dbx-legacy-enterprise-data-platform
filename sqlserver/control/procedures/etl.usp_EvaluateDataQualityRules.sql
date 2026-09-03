/*
    Object        : [etl].[usp_EvaluateDataQualityRules]
    Deploy target : WWI_Staging and WideWorldImportersDW
    Deploy order  : after 04_tables_data_quality.sql and etl.usp_LogError.sql
    Depends on    : etl.DataQualityRule, etl.DataQualityResult,
                    etl.DataQualityRuleException, etl.usp_LogError
    Called by     : the DQ_* SSIS packages (via the "Evaluate Rules" Execute SQL
                    task) and the month-end finance close job

    Evaluates every active rule in a rule group and records one
    etl.DataQualityResult row per rule. Returns the number of rules that failed
    their threshold and are not waived, so the calling package can decide between
    a warning and a controlled failure.

    Each rule is a WHERE clause selecting the offending rows of its object, so
    the measure is a count of offenders and a clean object measures zero. The
    evaluation is a cursor over sp_executesql because the rule set is data the
    stewards edit, not code: a rule that will not parse records -1 and is counted
    as NotEvaluated rather than taking the batch down.
*/

SET NOCOUNT ON;
GO

IF OBJECT_ID(N'etl.usp_EvaluateDataQualityRules', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_EvaluateDataQualityRules;
GO

CREATE PROCEDURE etl.usp_EvaluateDataQualityRules
(
    @BatchId            BIGINT          = NULL,
    @PackageExecutionId BIGINT          = NULL,
    @RuleGroupCode      NVARCHAR(20)    = NULL,
    @ObjectName         NVARCHAR(200)   = NULL,
    @RegionCode         NVARCHAR(10)    = NULL,
    @BusinessDate       DATE            = NULL,
    @FailedRuleCount    INT             OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @RuleId             INT,
            @RuleCode           NVARCHAR(30),
            @RuleObject         NVARCHAR(200),
            @RuleExpression     NVARCHAR(1000),
            @Severity           NVARCHAR(10),
            @Threshold          DECIMAL(18, 4),
            @RuleRegion         NVARCHAR(10),
            @Sql                NVARCHAR(MAX),
            @Measure            DECIMAL(18, 4),
            @RowsEvaluated      BIGINT,
            @Status             NVARCHAR(20),
            @Detail             NVARCHAR(2000),
            @Evaluated          INT = 0;

    SET @FailedRuleCount = 0;
    SET @BusinessDate = ISNULL(@BusinessDate, CAST(SYSUTCDATETIME() AS DATE));

    DECLARE rule_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT  r.DataQualityRuleId,
                r.RuleCode,
                r.ObjectName,
                r.RuleExpression,
                r.SeverityCode,
                r.ThresholdValue,
                r.RegionCode
        FROM    etl.DataQualityRule AS r
        WHERE   r.IsActive = 1
                AND (@RuleGroupCode IS NULL OR r.RuleGroupCode = @RuleGroupCode)
                AND (@ObjectName    IS NULL OR r.ObjectName    = @ObjectName)
                AND (@RegionCode    IS NULL OR r.RegionCode    IS NULL OR r.RegionCode = @RegionCode)
        ORDER BY r.RuleGroupCode, r.RuleCode;

    OPEN rule_cur;
    FETCH NEXT FROM rule_cur
        INTO @RuleId, @RuleCode, @RuleObject, @RuleExpression, @Severity, @Threshold, @RuleRegion;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @Measure = NULL;
        SET @RowsEvaluated = NULL;
        SET @Detail = NULL;

        /*
            Object names come from the rule table, so they are concatenated
            through QUOTENAME on each part rather than trusted verbatim. The
            rule expression itself cannot be parameterised - it is the point of
            the engine - which is why rule maintenance is a controlled activity.
        */
        BEGIN TRY
            SET @Sql =
                N'SELECT @OutMeasure = CAST(COUNT_BIG(*) AS DECIMAL(18,4)) FROM '
                + QUOTENAME(PARSENAME(@RuleObject, 2)) + N'.' + QUOTENAME(PARSENAME(@RuleObject, 1))
                + N' WHERE ' + @RuleExpression + N';';

            EXEC sp_executesql @Sql,
                 N'@OutMeasure DECIMAL(18,4) OUTPUT',
                 @OutMeasure = @Measure OUTPUT;

            SET @Sql =
                N'SELECT @OutRows = COUNT_BIG(*) FROM '
                + QUOTENAME(PARSENAME(@RuleObject, 2)) + N'.' + QUOTENAME(PARSENAME(@RuleObject, 1)) + N';';

            EXEC sp_executesql @Sql, N'@OutRows BIGINT OUTPUT', @OutRows = @RowsEvaluated OUTPUT;
        END TRY
        BEGIN CATCH
            SET @Measure = -1;
            SET @Detail = LEFT(N'Rule could not be evaluated: ' + ERROR_MESSAGE(), 2000);

            BEGIN TRY
                EXEC etl.usp_LogError
                     @BatchId            = @BatchId,
                     @PackageExecutionId = @PackageExecutionId,
                     @SourceName         = N'etl.usp_EvaluateDataQualityRules',
                     @SourceComponent    = @RuleObject,
                     @ProcedureName      = N'etl.usp_EvaluateDataQualityRules',
                     @ErrorSeverity      = N'Warning',
                     @ErrorDescription   = @Detail;
            END TRY
            BEGIN CATCH
                /* Logging the logging failure would be the third attempt; stop here. */
                SET @Detail = @Detail;
            END CATCH
        END CATCH

        IF @Measure < 0
            SET @Status = N'NotEvaluated';
        ELSE IF @Measure <= @Threshold
            SET @Status = N'Passed';
        ELSE IF EXISTS (SELECT 1
                        FROM   etl.DataQualityRuleException AS x
                        WHERE  x.RuleCode = @RuleCode
                               AND (x.ObjectName IS NULL OR x.ObjectName = @RuleObject)
                               AND (x.RegionCode IS NULL OR x.RegionCode = @RuleRegion)
                               AND @BusinessDate BETWEEN x.EffectiveFrom AND x.EffectiveTo)
            SET @Status = N'Warned';
        ELSE IF @Severity = N'FAIL'
            SET @Status = N'Failed';
        ELSE
            SET @Status = N'Warned';

        INSERT INTO etl.DataQualityResult
            (BatchId, PackageExecutionId, ObjectName, RuleCode, MeasuredValue,
             ThresholdValue, RowsEvaluated, ResultStatus, RegionCode, DetailText)
        VALUES
            (@BatchId, @PackageExecutionId, @RuleObject, @RuleCode, @Measure,
             @Threshold, @RowsEvaluated, @Status, @RuleRegion, @Detail);

        IF @Status = N'Failed'
            SET @FailedRuleCount = @FailedRuleCount + 1;

        SET @Evaluated = @Evaluated + 1;

        FETCH NEXT FROM rule_cur
            INTO @RuleId, @RuleCode, @RuleObject, @RuleExpression, @Severity, @Threshold, @RuleRegion;
    END

    CLOSE rule_cur;
    DEALLOCATE rule_cur;

    SELECT  @FailedRuleCount    AS FailedRuleCount,
            @Evaluated          AS RulesEvaluated;
END
GO
