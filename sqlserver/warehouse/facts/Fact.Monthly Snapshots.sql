/*
    Month-end periodic snapshot facts, deployed together because the finance
    close job builds all four in one pass:
      [Fact].[Monthly Ar Aging]
      [Fact].[Monthly Ap Aging]
      [Fact].[Monthly Customer Balance]
      [Fact].[Monthly Salesperson Performance]
*/

/*
    Fact.Monthly Ar Aging

    Object        : [Fact].[Monthly Ar Aging] - periodic snapshot fact, one row
                    per customer per aging bucket per month end.
    Deploy target : WideWorldImportersDW
    Deploy order  : after [Fact].[Customer Transaction] and
                    [Fact].[Transaction].
    Called by     : loaded by Integration.usp_LoadFactArAgingSnapshot at month
                    end; never updated afterwards.
    Grain         : customer x aging bucket x month end date.

    Supporting periodic snapshot behind vw_ApAgingCurrent's AR counterpart and
    the finance close aggregate. Once a month end is loaded the rows are frozen:
    the aging that was reported to the board is the aging that stays in the
    table, even when a payment is later backdated.

    Bucket definitions differ by region and are stored per row rather than
    assumed: NA ages on 30/60/90/120 from invoice date, EU ages from the due
    date because payment terms vary by country, APAC uses 30/60/90 from
    statement date. [Aging Basis Code] records which was used.
*/
CREATE TABLE [Fact].[Monthly Ar Aging] (
    [Monthly Ar Aging Key]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [Month End Date Key]            DATE            NOT NULL,
    [Customer Key]                  INT             NOT NULL,
    [Customer Segment Key]          INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Currency Key]                  INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Aging Bucket Code]             NVARCHAR (10)   NOT NULL,
    [Aging Bucket Sort Order]       TINYINT         NOT NULL,
    [Aging Basis Code]              NVARCHAR (10)   NOT NULL,
    [Fiscal Year]                   SMALLINT        NULL,
    [Fiscal Period]                 TINYINT         NULL,
    [Open Item Count]               INT             NULL,
    [Oldest Item Age Days]          INT             NULL,
    [Balance Transaction Currency]  DECIMAL (18, 2) NULL,
    [Balance Reporting]             DECIMAL (18, 2) NOT NULL,
    [Disputed Balance Reporting]    DECIMAL (18, 2) NULL,
    [On Credit Hold Flag]           BIT             NULL,
    [Credit Limit Reporting]        DECIMAL (18, 2) NULL,
    [Credit Utilisation Percent]    DECIMAL (9, 4)  NULL,
    [Bad Debt Provision Amount]     DECIMAL (18, 2) NULL,
    [Provision Rate Percent]        DECIMAL (9, 4)  NULL,
    [Days Sales Outstanding]        DECIMAL (9, 2)  NULL,
    [Collection Status Code]        NVARCHAR (6)    NULL,
    [Snapshot Frozen Flag]          BIT             CONSTRAINT [DF_Fact_Monthly_Ar_Aging_Snapshot_Frozen_Flag] DEFAULT (0) NOT NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Monthly_Ar_Aging] PRIMARY KEY NONCLUSTERED ([Monthly Ar Aging Key] ASC, [Month End Date Key] ASC) ON [PS_Date] ([Month End Date Key]),
    CONSTRAINT [FK_Fact_Monthly_Ar_Aging_Month_End_Date_Key_Dimension_Date] FOREIGN KEY ([Month End Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Monthly_Ar_Aging_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key])
)
ON [PS_Date] ([Month End Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Monthly_Ar_Aging_Grain]
    ON [Fact].[Monthly Ar Aging] ([Month End Date Key] ASC, [Customer Key] ASC, [Aging Bucket Code] ASC)
    ON [PS_Date] ([Month End Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Monthly_Ar_Aging]
    ON [Fact].[Monthly Ar Aging]
    ON [PS_Date] ([Month End Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Month-end accounts receivable aging periodic snapshot, frozen after close',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Monthly Ar Aging';
GO

/*
    Fact.Monthly Ap Aging

    Object        : [Fact].[Monthly Ap Aging] - periodic snapshot fact, one row
                    per supplier per aging bucket per month end.
    Deploy target : WideWorldImportersDW
    Deploy order  : after [Fact].[Supplier Transaction].
    Called by     : loaded by Integration.usp_LoadFactApAgingSnapshot at month
                    end.
    Grain         : supplier x aging bucket x month end date.

    Supporting periodic snapshot behind vw_ApAgingCurrent. Deliberately not a
    mirror of the AR aging: AP reports on what is not yet due as well as what is
    overdue (treasury needs the forward view), tracks discount still capturable,
    and carries the goods-received-not-invoiced accrual that has no AR analogue.
*/
CREATE TABLE [Fact].[Monthly Ap Aging] (
    [Monthly Ap Aging Key]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [Month End Date Key]            DATE            NOT NULL,
    [Supplier Key]                  INT             NOT NULL,
    [Payment Terms Key]             INT             NULL,
    [Cost Center Key]               INT             NULL,
    [Currency Key]                  INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Aging Bucket Code]             NVARCHAR (10)   NOT NULL,
    [Aging Bucket Sort Order]       TINYINT         NOT NULL,
    [Fiscal Year]                   SMALLINT        NULL,
    [Fiscal Period]                 TINYINT         NULL,
    [Open Invoice Count]            INT             NULL,
    [Balance Transaction Currency]  DECIMAL (18, 2) NULL,
    [Balance Reporting]             DECIMAL (18, 2) NOT NULL,
    [Not Yet Due Reporting]         DECIMAL (18, 2) NULL,
    [Blocked For Payment Reporting] DECIMAL (18, 2) NULL,
    [Discount Still Capturable]     DECIMAL (18, 2) NULL,
    [Discount Lost To Date]         DECIMAL (18, 2) NULL,
    [Grni Accrual Reporting]        DECIMAL (18, 2) NULL,
    [Recoverable Tax Reporting]     DECIMAL (18, 2) NULL,
    [Days Payable Outstanding]      DECIMAL (9, 2)  NULL,
    [Average Days Beyond Terms]     DECIMAL (9, 2)  NULL,
    [Match Exception Count]         INT             NULL,
    [Supplier Risk Rating Code]     NVARCHAR (4)    NULL,
    [Snapshot Frozen Flag]          BIT             CONSTRAINT [DF_Fact_Monthly_Ap_Aging_Snapshot_Frozen_Flag] DEFAULT (0) NOT NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Monthly_Ap_Aging] PRIMARY KEY NONCLUSTERED ([Monthly Ap Aging Key] ASC, [Month End Date Key] ASC) ON [PS_Date] ([Month End Date Key]),
    CONSTRAINT [FK_Fact_Monthly_Ap_Aging_Month_End_Date_Key_Dimension_Date] FOREIGN KEY ([Month End Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Monthly_Ap_Aging_Supplier_Key_Dimension_Supplier] FOREIGN KEY ([Supplier Key]) REFERENCES [Dimension].[Supplier] ([Supplier Key])
)
ON [PS_Date] ([Month End Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Monthly_Ap_Aging_Grain]
    ON [Fact].[Monthly Ap Aging] ([Month End Date Key] ASC, [Supplier Key] ASC, [Aging Bucket Code] ASC)
    ON [PS_Date] ([Month End Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Monthly_Ap_Aging]
    ON [Fact].[Monthly Ap Aging]
    ON [PS_Date] ([Month End Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Month-end accounts payable aging periodic snapshot including GRNI accrual',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Monthly Ap Aging';
GO

/*
    Fact.Monthly Customer Balance

    Object        : [Fact].[Monthly Customer Balance] - periodic snapshot fact,
                    one row per customer per month end holding the movement
                    reconciliation of the customer account.
    Deploy target : WideWorldImportersDW
    Deploy order  : after [Fact].[Customer Transaction], [Fact].[Payment],
                    [Fact].[Credit Note].
    Called by     : loaded by Integration.usp_LoadFactArAgingSnapshot in the
                    same month-end pass that builds the aging.
    Grain         : customer x month end date.

    Deliberately a roll-forward, not a position: opening balance + invoiced -
    credited - cash + adjustments = closing balance. The load computes the
    closing balance independently from the ledger and stores both it and the
    roll-forward result, so [Reconciliation Variance] is non-zero whenever the
    two disagree. Finance has lived with a small permanent variance on the APAC
    entities since the 2016 acquisition.
*/
CREATE TABLE [Fact].[Monthly Customer Balance] (
    [Monthly Customer Balance Key]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [Month End Date Key]            DATE            NOT NULL,
    [Customer Key]                  INT             NOT NULL,
    [Customer Segment Key]          INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Currency Key]                  INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Fiscal Year]                   SMALLINT        NULL,
    [Fiscal Period]                 TINYINT         NULL,
    [Opening Balance Reporting]     DECIMAL (18, 2) NOT NULL,
    [Invoiced Amount Reporting]     DECIMAL (18, 2) NULL,
    [Credited Amount Reporting]     DECIMAL (18, 2) NULL,
    [Cash Received Reporting]       DECIMAL (18, 2) NULL,
    [Write Off Amount Reporting]    DECIMAL (18, 2) NULL,
    [Adjustment Amount Reporting]   DECIMAL (18, 2) NULL,
    [FX Revaluation Amount]         DECIMAL (18, 2) NULL,
    [Rolled Forward Balance]        DECIMAL (18, 2) NULL,
    [Closing Balance Reporting]     DECIMAL (18, 2) NOT NULL,
    [Overdue Balance Reporting]     DECIMAL (18, 2) NULL,
    [Reconciliation Variance]       DECIMAL (18, 2) NULL,
    [Invoice Count]                 INT             NULL,
    [Receipt Count]                 INT             NULL,
    [Average Days To Pay]           DECIMAL (9, 2)  NULL,
    [Months Since Last Order]       INT             NULL,
    [Active Customer Flag]          BIT             NULL,
    [Statement Issued Flag]         BIT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Monthly_Customer_Balance] PRIMARY KEY NONCLUSTERED ([Monthly Customer Balance Key] ASC, [Month End Date Key] ASC) ON [PS_Date] ([Month End Date Key]),
    CONSTRAINT [FK_Fact_Monthly_Customer_Balance_Month_End_Date_Key_Dimension_Date] FOREIGN KEY ([Month End Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Monthly_Customer_Balance_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key])
)
ON [PS_Date] ([Month End Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Monthly_Customer_Balance_Grain]
    ON [Fact].[Monthly Customer Balance] ([Month End Date Key] ASC, [Customer Key] ASC)
    ON [PS_Date] ([Month End Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Monthly_Customer_Balance_Variance]
    ON [Fact].[Monthly Customer Balance] ([Month End Date Key] ASC, [Region Code] ASC)
    INCLUDE ([Reconciliation Variance], [Closing Balance Reporting])
    ON [PS_Date] ([Month End Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Month-end customer account roll-forward with reconciliation variance',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Monthly Customer Balance';
GO

/*
    Fact.Monthly Salesperson Performance

    Object        : [Fact].[Monthly Salesperson Performance] - periodic snapshot
                    fact, one row per salesperson per month.
    Deploy target : WideWorldImportersDW
    Deploy order  : after [Fact].[Daily Sales Snapshot] and [Fact].[Return].
    Called by     : loaded by Integration.usp_LoadFactDailySalesSnapshot on the
                    first run after a fiscal period closes.
    Grain         : salesperson x fiscal period.

    Commission is stored at three stages - accrued, adjusted, paid - because the
    scheme allows a clawback in the following period when a sale is returned or
    written off. The clawback columns are the reason this cannot simply be an
    aggregate over the daily snapshot.
*/
CREATE TABLE [Fact].[Monthly Salesperson Performance] (
    [Monthly Salesperson Perf Key]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [Period End Date Key]           DATE            NOT NULL,
    [Salesperson Key]               INT             NOT NULL,
    [Sales Territory Key]           INT             NULL,
    [Employee Key]                  INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Fiscal Year]                   SMALLINT        NOT NULL,
    [Fiscal Period]                 TINYINT         NOT NULL,
    [Fiscal Calendar Code]          NVARCHAR (10)   NULL,
    [Selling Days In Period]        TINYINT         NULL,
    [Order Count]                   INT             NULL,
    [Invoice Count]                 INT             NULL,
    [New Customer Count]            INT             NULL,
    [Lost Customer Count]           INT             NULL,
    [Active Customer Count]         INT             NULL,
    [Net Sales Reporting]           DECIMAL (18, 2) NULL,
    [Gross Margin Reporting]        DECIMAL (18, 2) NULL,
    [Margin Percent]                DECIMAL (9, 4)  NULL,
    [Returns Reporting]             DECIMAL (18, 2) NULL,
    [Discount Given Reporting]      DECIMAL (18, 2) NULL,
    [Average Order Value]           DECIMAL (18, 2) NULL,
    [Quota Reporting]               DECIMAL (18, 2) NULL,
    [Quota Attainment Percent]      DECIMAL (9, 4)  NULL,
    [Commission Rate Percent]       DECIMAL (9, 4)  NULL,
    [Commission Accrued Amount]     DECIMAL (18, 2) NULL,
    [Commission Clawback Amount]    DECIMAL (18, 2) NULL,
    [Commission Adjusted Amount]    DECIMAL (18, 2) NULL,
    [Commission Paid Amount]        DECIMAL (18, 2) NULL,
    [Accelerator Applied Flag]      BIT             NULL,
    [Rank In Territory]             INT             NULL,
    [Rank In Region]                INT             NULL,
    [Period Closed Flag]            BIT             CONSTRAINT [DF_Fact_Monthly_Salesperson_Performance_Period_Closed_Flag] DEFAULT (0) NOT NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Monthly_Salesperson_Performance] PRIMARY KEY NONCLUSTERED ([Monthly Salesperson Perf Key] ASC, [Period End Date Key] ASC) ON [PS_Date] ([Period End Date Key]),
    CONSTRAINT [FK_Fact_Monthly_Salesperson_Performance_Period_End_Date_Key_Dimension_Date] FOREIGN KEY ([Period End Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Monthly_Salesperson_Performance_Salesperson_Key_Dimension_Salesperson] FOREIGN KEY ([Salesperson Key]) REFERENCES [Dimension].[Salesperson] ([Salesperson Key])
)
ON [PS_Date] ([Period End Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Monthly_Salesperson_Performance_Grain]
    ON [Fact].[Monthly Salesperson Performance] ([Fiscal Year] ASC, [Fiscal Period] ASC, [Salesperson Key] ASC, [Period End Date Key] ASC)
    ON [PS_Date] ([Period End Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Monthly salesperson performance and commission periodic snapshot',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Monthly Salesperson Performance';
GO
