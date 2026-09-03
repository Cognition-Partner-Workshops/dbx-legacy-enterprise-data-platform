/*
    Objects       : [etl].[ReconciliationResult]
    Deploy target : WWI_Staging and WideWorldImportersDW (the finance and
                    inventory reconciliations run against the warehouse copy)
    Deploy order  : after 02_tables_control_framework.sql
    Written by    : FIN_Reconcile_SubledgerToGl, INV_Reconcile_OnHand,
                    ERR_Reconcile_RowCounts, DQ_Threshold_Gate
    Read by       : FIN_Close_PeriodLock, the operational views, and the
                    stewardship reports

    One table for every kind of reconciliation the estate performs, which is why
    it is as wide as it is: the finance comparison keys on ledger, period and
    account, the inventory comparison keys on a composite source key, and the
    row count comparison keys on an object name. Each writer populates the
    columns that mean something to it and leaves the rest NULL.

    VarianceStatus is not a closed set on purpose - it is what the writing
    package decided the difference was ('Matched', 'Variance', 'Timing',
    'Explained', 'TOLERATED', 'Negative on hand') - and the finance close reads
    only 'Variance' when deciding whether a period may be locked.
*/

SET NOCOUNT ON;
GO

IF OBJECT_ID(N'etl.ReconciliationResult', N'U') IS NULL
BEGIN
    CREATE TABLE etl.ReconciliationResult
    (
        ReconciliationResultId  BIGINT          IDENTITY(1, 1)  NOT NULL,
        BatchId                 BIGINT                          NULL,
        ReconciliationName      NVARCHAR(100)                   NOT NULL,
        ObjectName              NVARCHAR(200)                   NULL,
        SourceKey               NVARCHAR(200)                   NULL,
        LedgerCode              NVARCHAR(20)                    NULL,
        AccountingPeriod        NVARCHAR(10)                    NULL,
        AccountCode             NVARCHAR(30)                    NULL,
        RegionCode              NVARCHAR(10)                    NULL,
        SourceAmount            DECIMAL(19, 4)                  NULL,
        TargetAmount            DECIMAL(19, 4)                  NULL,
        VarianceAmount          DECIMAL(19, 4)                  NULL,
        VarianceStatus          NVARCHAR(30)                    NULL,
        ExplanationCode         NVARCHAR(50)                    NULL,
        EvaluatedAtUtc          DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_ReconciliationResult_EvaluatedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_ReconciliationResult PRIMARY KEY CLUSTERED (ReconciliationResultId)
    );

    CREATE NONCLUSTERED INDEX IX_ReconciliationResult_Batch
        ON etl.ReconciliationResult (BatchId, VarianceStatus)
        INCLUDE (ReconciliationName, ObjectName, VarianceAmount);

    /*
        The period lock reads by period without a batch id, because a period is
        closed against everything ever loaded into it rather than against one
        night's run.
    */
    CREATE NONCLUSTERED INDEX IX_ReconciliationResult_Period
        ON etl.ReconciliationResult (AccountingPeriod, VarianceStatus)
        INCLUDE (LedgerCode, AccountCode, VarianceAmount);
END
GO
