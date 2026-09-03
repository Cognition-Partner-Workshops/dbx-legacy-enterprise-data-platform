/*
    Aggregate.Finance Close Summary

    Object        : [Aggregate].[Finance Close Summary] - legal entity x fiscal
                    period close position, one row per entity per period per
                    account group.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.00 Schema, [Fact].[GL Posting],
                    [Fact].[Monthly Ar Aging], [Fact].[Monthly Ap Aging].
    Called by     : Integration.usp_RefreshAggregateFinanceClose, run repeatedly
                    during the close window and once more when the period is
                    hard-closed.

    The reconciliation columns are the point of the table: the sub-ledger
    totals are compared to the GL totals and the difference is stored. A close
    is "clean" when every [Sub Ledger To GL Difference] is inside tolerance,
    and the close status is driven off that, not off a person ticking a box.
*/
CREATE TABLE [Aggregate].[Finance Close Summary] (
    [Finance Close Summary Key]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [Fiscal Year]                   SMALLINT        NOT NULL,
    [Fiscal Period]                 TINYINT         NOT NULL,
    [Legal Entity Code]             NVARCHAR (10)   NOT NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Account Group Code]            NVARCHAR (20)   NOT NULL,
    [Ledger Currency Code]          NCHAR (3)       NULL,
    [Period End Date]               DATE            NULL,
    [Opening Balance Local]         DECIMAL (18, 2) NULL,
    [Period Debits Local]           DECIMAL (18, 2) NULL,
    [Period Credits Local]          DECIMAL (18, 2) NULL,
    [Closing Balance Local]         DECIMAL (18, 2) NULL,
    [Closing Balance Reporting]     DECIMAL (18, 2) NULL,
    [Consolidation Rate]            DECIMAL (19, 9) NULL,
    [Sub Ledger Balance Reporting]  DECIMAL (18, 2) NULL,
    [Sub Ledger To GL Difference]   DECIMAL (18, 2) NULL,
    [Tolerance Amount]              DECIMAL (18, 2) NULL,
    [Within Tolerance Flag]         BIT             NULL,
    [Manual Journal Count]          INT             NULL,
    [Manual Journal Value]          DECIMAL (18, 2) NULL,
    [Late Posting Count]            INT             NULL,
    [Unposted Journal Count]        INT             NULL,
    [Ar Balance Reporting]          DECIMAL (18, 2) NULL,
    [Ap Balance Reporting]          DECIMAL (18, 2) NULL,
    [Grni Accrual Reporting]        DECIMAL (18, 2) NULL,
    [Bad Debt Provision Reporting]  DECIMAL (18, 2) NULL,
    [Close Status Code]             NVARCHAR (10)   NULL,
    [Close Completed Datetime]      DATETIME2 (3)   NULL,
    [Days To Close]                 INT             NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Aggregate_Finance_Close_Summary] PRIMARY KEY NONCLUSTERED ([Finance Close Summary Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Finance_Close_Summary_Grain]
    ON [Aggregate].[Finance Close Summary] ([Fiscal Year] ASC, [Fiscal Period] ASC, [Legal Entity Code] ASC, [Account Group Code] ASC);
GO

CREATE NONCLUSTERED INDEX [IX_Aggregate_Finance_Close_Summary_Exceptions]
    ON [Aggregate].[Finance Close Summary] ([Within Tolerance Flag] ASC, [Fiscal Year] ASC, [Fiscal Period] ASC)
    INCLUDE ([Legal Entity Code], [Sub Ledger To GL Difference], [Close Status Code]);
GO
