/*
    Object        : [Dimension].[Salesperson]  (SCD Type 2, sales-facing subset of Employee)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Employee.sql
    Depends on    : Sequences.SalespersonKey, Dimension.Employee, Dimension.Sales Territory
    Called by     : Integration.usp_MigrateStagedSalespersonData,
                    Integration.usp_LoadBridgeEmployeeTerritory

    Sales insisted in 2007 on a dimension of their own rather than filtering
    Dimension.Employee on [Is Salesperson], because they wanted quota, commission
    plan and territory history versioned on the sales calendar (which closes on
    the Saturday nearest month end) rather than on the HR effective date. The two
    dimensions therefore version independently and drift; [Employee Key] is the
    documented join back, and it points at the *current* employee row, not the
    contemporaneous one. Reports that need the contemporaneous row join on
    [WWI Employee ID] with an [Effective From]/[Effective To] between predicate.

    Type 2 : territory, quota, commission plan, manager, employment status.
    Type 1 : display name, contact details, photo URL.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Salesperson', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Salesperson]
    (
        [Salesperson Key]               INT             CONSTRAINT [DF_Dimension_Salesperson_Salesperson_Key] DEFAULT (NEXT VALUE FOR [Sequences].[SalespersonKey]) NOT NULL,
        [WWI Employee ID]               INT             NOT NULL,
        [Employee Key]                  INT             NULL,
        [Salesperson]                   NVARCHAR(50)    NOT NULL,
        [Preferred Name]                NVARCHAR(50)    NULL,
        [Salesperson Code]              NVARCHAR(20)    NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Region Code]                   NVARCHAR(10)    NULL,
        [Country Code]                  NVARCHAR(3)     NULL,

        [Sales Territory Key]           INT             NULL,
        [Sales Territory]               NVARCHAR(50)    NULL,
        [Sales Office Code]             NVARCHAR(20)    NULL,
        [Sales Manager Employee Key]    INT             NULL,
        [Sales Role Code]               NVARCHAR(10)    NULL,   -- AE / SDR / KAM / CHN / INS
        [Channel Responsibility Code]   NVARCHAR(10)    NULL,   -- DIR / RET / WHL / ECM / PTR

        [Commission Plan Code]          NVARCHAR(20)    NULL,
        [Commission Rate]               DECIMAL(9, 4)   NULL,
        [Commission Currency Code]      NVARCHAR(3)     NULL,
        [Annual Quota Amount]           DECIMAL(18, 2)  NULL,
        [Quota Currency Code]           NVARCHAR(3)     NULL,
        [Quota Fiscal Year]             SMALLINT        NULL,
        [Quota Basis Code]              NVARCHAR(10)    NULL,   -- REV / GM / UNITS

        /*
            Regional pay and disclosure rules. NA runs a calendar-year plan with
            accelerators; EU pay is bound by the collective agreement and the
            commission element is capped; APAC pays quarterly against a local
            currency quota converted at the plan-year budget rate.
        */
        [NA Accelerator Threshold Pct]  DECIMAL(9, 4)   NULL,
        [EU Commission Cap Pct]         DECIMAL(9, 4)   NULL,
        [EU Collective Agreement Code]  NVARCHAR(20)    NULL,
        [APAC Payout Frequency Code]    NVARCHAR(10)    NULL,   -- MTH / QTR / HLF
        [APAC Budget FX Rate]           DECIMAL(18, 6)  NULL,

        [Is Active]                     BIT             NULL,
        [Hire Date]                     DATE            NULL,
        [Sales Start Date]              DATE            NULL,
        [Sales End Date]                DATE            NULL,

        [Effective From]                DATETIME2(7)    NOT NULL,
        [Effective To]                  DATETIME2(7)    NOT NULL,
        [Effective From Date]           DATE            NULL,
        [Effective Sequence]            SMALLINT        NULL,
        [Is Current Row]                BIT             NOT NULL,
        [Version Number]                INT             NULL,
        [Row Hash Type 2]               VARBINARY(32)   NULL,
        [Is Inferred Member]            BIT             NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Salesperson] PRIMARY KEY CLUSTERED ([Salesperson Key] ASC),
        CONSTRAINT [CK_Dimension_Salesperson_Quota_Basis]
            CHECK ([Quota Basis Code] IS NULL OR [Quota Basis Code] IN (N'REV', N'GM', N'UNITS'))
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Salesperson_Current]
        ON [Dimension].[Salesperson] ([WWI Employee ID] ASC, [Is Current Row] ASC)
        INCLUDE ([Salesperson Key], [Sales Territory Key], [Row Hash Type 2]);
END;
GO
