/*
    Objects       : [etl].[DataQualityRule], [etl].[DataQualityResult],
                    [etl].[DataQualityRuleException]
    Deploy target : WWI_Staging and WideWorldImportersDW (the control schema is
                    deployed to both; the warehouse copy keeps a longer history)
    Deploy order  : after 02_tables_control_framework.sql, before
                    03_seed_control_data.sql
    Called by     : etl.usp_EvaluateDataQualityRules, the DQ_* SSIS packages, and
                    the stewardship reports

    The rule set is data, not code. Stewards maintain rows in etl.DataQualityRule
    and the engine evaluates whatever is active at run time - which is why the
    evaluation is a cursor over dynamic SQL rather than a compiled statement, and
    why a rule that fails to parse records a measured value of -1 instead of
    taking the batch down with it.

    RuleExpression is the WHERE clause that selects the *offending* rows, so a
    measured value of zero is a clean object. ThresholdValue is the number of
    offending rows tolerated before the rule is considered failed; SeverityCode
    decides whether a failed rule warns or blocks.
*/

SET NOCOUNT ON;
GO

IF OBJECT_ID(N'etl.DataQualityRule', N'U') IS NULL
BEGIN
    CREATE TABLE etl.DataQualityRule
    (
        DataQualityRuleId   INT             IDENTITY(1, 1)  NOT NULL,
        RuleCode            NVARCHAR(30)                    NOT NULL,
        RuleGroupCode       NVARCHAR(20)                    NOT NULL,
        ObjectName          NVARCHAR(200)                   NOT NULL,
        RuleName            NVARCHAR(200)                   NOT NULL,
        RuleExpression      NVARCHAR(1000)                  NOT NULL,
        DimensionCode       NVARCHAR(20)                    NOT NULL
            CONSTRAINT DF_DataQualityRule_DimensionCode DEFAULT (N'Validity'),
        SeverityCode        NVARCHAR(10)                    NOT NULL
            CONSTRAINT DF_DataQualityRule_SeverityCode DEFAULT (N'WARN'),
        ThresholdValue      DECIMAL(18, 4)                  NOT NULL
            CONSTRAINT DF_DataQualityRule_ThresholdValue DEFAULT (0),
        RegionCode          NVARCHAR(10)                    NULL,
        SourceSystemCode    NVARCHAR(30)                    NULL,
        IsActive            BIT                             NOT NULL
            CONSTRAINT DF_DataQualityRule_IsActive DEFAULT (1),
        OwnerName           NVARCHAR(100)                   NULL,
        Notes               NVARCHAR(1000)                  NULL,
        CreatedAtUtc        DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_DataQualityRule_CreatedAtUtc DEFAULT (SYSUTCDATETIME()),
        UpdatedAtUtc        DATETIME2(3)                    NULL,
        CONSTRAINT PK_DataQualityRule PRIMARY KEY CLUSTERED (DataQualityRuleId),
        CONSTRAINT UQ_DataQualityRule_RuleCode UNIQUE (RuleCode),
        CONSTRAINT CK_DataQualityRule_Severity
            CHECK (SeverityCode IN (N'INFO', N'WARN', N'FAIL')),
        CONSTRAINT CK_DataQualityRule_Threshold CHECK (ThresholdValue >= 0)
    );

    CREATE NONCLUSTERED INDEX IX_DataQualityRule_Group
        ON etl.DataQualityRule (RuleGroupCode, IsActive)
        INCLUDE (RuleCode, ObjectName, SeverityCode, ThresholdValue);

    CREATE NONCLUSTERED INDEX IX_DataQualityRule_Object
        ON etl.DataQualityRule (ObjectName, IsActive);
END
GO

IF OBJECT_ID(N'etl.DataQualityResult', N'U') IS NULL
BEGIN
    CREATE TABLE etl.DataQualityResult
    (
        DataQualityResultId BIGINT          IDENTITY(1, 1)  NOT NULL,
        BatchId             BIGINT                          NULL,
        PackageExecutionId  BIGINT                          NULL,
        ObjectName          NVARCHAR(200)                   NOT NULL,
        RuleCode            NVARCHAR(30)                    NOT NULL,
        MeasuredValue       DECIMAL(18, 4)                  NULL,
        ThresholdValue      DECIMAL(18, 4)                  NULL,
        RowsEvaluated       BIGINT                          NULL,
        ResultStatus        NVARCHAR(20)                    NULL,
        RegionCode          NVARCHAR(10)                    NULL,
        DetailText          NVARCHAR(2000)                  NULL,
        EvaluatedAtUtc      DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_DataQualityResult_EvaluatedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_DataQualityResult PRIMARY KEY CLUSTERED (DataQualityResultId),
        CONSTRAINT CK_DataQualityResult_Status
            CHECK (ResultStatus IS NULL OR ResultStatus IN (N'Passed', N'Warned', N'Failed', N'NotEvaluated'))
    );

    CREATE NONCLUSTERED INDEX IX_DataQualityResult_Batch
        ON etl.DataQualityResult (BatchId, RuleCode)
        INCLUDE (MeasuredValue, ThresholdValue, ResultStatus);

    CREATE NONCLUSTERED INDEX IX_DataQualityResult_Object
        ON etl.DataQualityResult (ObjectName, EvaluatedAtUtc DESC);
END
GO

/*
    A rule can be waived for a named object and window. Waivers are how the
    stewards keep the nightly scorecard readable while a known source problem is
    being fixed upstream; they expire on purpose, because an unexpiring waiver is
    indistinguishable from deleting the rule.
*/
IF OBJECT_ID(N'etl.DataQualityRuleException', N'U') IS NULL
BEGIN
    CREATE TABLE etl.DataQualityRuleException
    (
        RuleExceptionId     INT             IDENTITY(1, 1)  NOT NULL,
        RuleCode            NVARCHAR(30)                    NOT NULL,
        ObjectName          NVARCHAR(200)                   NULL,
        RegionCode          NVARCHAR(10)                    NULL,
        EffectiveFrom       DATE                            NOT NULL,
        EffectiveTo         DATE                            NOT NULL,
        Reason              NVARCHAR(1000)                  NOT NULL,
        ApprovedBy          NVARCHAR(100)                   NOT NULL,
        CreatedAtUtc        DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_DataQualityRuleException_CreatedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_DataQualityRuleException PRIMARY KEY CLUSTERED (RuleExceptionId),
        CONSTRAINT CK_DataQualityRuleException_Window CHECK (EffectiveTo >= EffectiveFrom)
    );

    CREATE NONCLUSTERED INDEX IX_DataQualityRuleException_Rule
        ON etl.DataQualityRuleException (RuleCode, EffectiveFrom, EffectiveTo);
END
GO
