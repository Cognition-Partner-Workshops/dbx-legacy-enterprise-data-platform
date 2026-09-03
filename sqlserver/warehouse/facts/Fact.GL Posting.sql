/*
    Fact.GL Posting

    Object        : [Fact].[GL Posting] - transaction fact, one row per general
                    ledger journal line posted from any feeder system.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Cost Center, Dimension.Currency,
                    Dimension.Date (WP05).
    Called by     : loaded by Integration.usp_LoadFactGlPosting from the Oracle
                    GL journal extract; read by
                    Integration.usp_RefreshAggregateFinanceClose.
    Grain         : one journal line.

    Debits and credits are kept as separate columns as well as a signed amount,
    because the trial balance report written in 2004 sums the two columns and
    nobody will re-point it. Local and reporting amounts are both stored: the
    GL posts in the ledger currency of the posting entity, and the group
    consolidation rate is a monthly average rate, not the daily spot rate used
    on the sales facts. That mismatch is a known reconciliation headache.

    Chart-of-accounts divergence: the NA entities post against a 6-segment
    account string, EU entities use a 5-segment string with a statutory account
    mapped on top, and the APAC entities were acquired with their own chart and
    are mapped through a translation table, which is why
    [Local Account Code] and [Group Account Code] can differ.
*/
CREATE TABLE [Fact].[GL Posting] (
    [GL Posting Key]                BIGINT          IDENTITY (1, 1) NOT NULL,
    [Posting Date Key]              DATE            NOT NULL,
    [Effective Date Key]            DATE            NULL,
    [Cost Center Key]               INT             NULL,
    [Currency Key]                  INT             NULL,
    [Employee Key]                  INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Legal Entity Code]             NVARCHAR (10)   NOT NULL,
    [Journal Number]                NVARCHAR (20)   NOT NULL,
    [Journal Line Number]           INT             NOT NULL,
    [Journal Source Code]           NVARCHAR (10)   NOT NULL,
    [Journal Category Code]         NVARCHAR (10)   NULL,
    [Local Account Code]            NVARCHAR (30)   NOT NULL,
    [Group Account Code]            NVARCHAR (20)   NULL,
    [Statutory Account Code]        NVARCHAR (20)   NULL,
    [Account Segment String]        NVARCHAR (80)   NULL,
    [Source Document Reference]     NVARCHAR (30)   NULL,
    [Invoice Number]                NVARCHAR (20)   NULL,
    [Purchase Order Number]         NVARCHAR (20)   NULL,
    [Ledger Code]                   NVARCHAR (10)   NULL,
    [GL Account Key]                INT             NULL,
    [Ledger Currency Code]          NCHAR (3)       NULL,
    [Debit Amount]                  DECIMAL (18, 2) NULL,
    [Credit Amount]                 DECIMAL (18, 2) NULL,
    [Signed Amount]                 DECIMAL (18, 2) NOT NULL,
    [Consolidation Rate]            DECIMAL (19, 9) NULL,
    [Consolidation Rate Type Code]  NVARCHAR (10)   NULL,
    [Signed Amount Reporting]       DECIMAL (18, 2) NULL,
    [Fiscal Year]                   SMALLINT        NOT NULL,
    [Fiscal Period]                 TINYINT         NOT NULL,
    [Period Status Code]            NVARCHAR (6)    NULL,
    [Reversal Flag]                 BIT             NULL,
    [Reversed Journal Number]       NVARCHAR (20)   NULL,
    [Manual Journal Flag]           BIT             NULL,
    [Late Posting Flag]             BIT             NULL,
    [Journal Description]           NVARCHAR (240)  NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_GL_Posting] PRIMARY KEY NONCLUSTERED ([GL Posting Key] ASC, [Posting Date Key] ASC) ON [PS_Date] ([Posting Date Key]),
    CONSTRAINT [FK_Fact_GL_Posting_Posting_Date_Key_Dimension_Date] FOREIGN KEY ([Posting Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_GL_Posting_Cost_Center_Key_Dimension_Cost Center] FOREIGN KEY ([Cost Center Key]) REFERENCES [Dimension].[Cost Center] ([Cost Center Key])
)
ON [PS_Date] ([Posting Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_GL_Posting_Natural_Key]
    ON [Fact].[GL Posting] ([Legal Entity Code] ASC, [Journal Number] ASC, [Journal Line Number] ASC, [Posting Date Key] ASC)
    ON [PS_Date] ([Posting Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_GL_Posting_Close]
    ON [Fact].[GL Posting] ([Fiscal Year] ASC, [Fiscal Period] ASC, [Legal Entity Code] ASC)
    INCLUDE ([Group Account Code], [Signed Amount Reporting], [Period Status Code])
    ON [PS_Date] ([Posting Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_GL_Posting]
    ON [Fact].[GL Posting]
    ON [PS_Date] ([Posting Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'General ledger journal line fact with local and group account codes',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'GL Posting';
GO
