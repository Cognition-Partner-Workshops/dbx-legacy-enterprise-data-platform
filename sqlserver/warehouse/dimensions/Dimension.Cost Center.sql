/*
    Object        : [Dimension].[Cost Center]  (SCD Type 2, ragged finance hierarchy)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Region.sql
    Depends on    : Sequences.CostCenterKey, Dimension.Region
    Called by     : Integration.usp_MigrateStagedCostCenterData

    Sourced from Oracle WWI_FIN.COST_CENTER. Type 2 because a cost centre moving
    between legal entities or company codes changes the consolidation path and the
    prior postings must stay under the old path.

    The rollup is ragged in the same way the product hierarchy is: a corporate
    function reports straight to the group, while a warehouse cost centre reports
    through site -> country -> region -> group. The load procedure walks the
    parent-child edges with a recursive CTE and materialises both the path and the
    fixed six-level flattening the finance cube expects.

    The company-code block differs per region because the ledgers were never
    merged: NA posts to a single US company code with departmental analysis, EU
    posts per country company code with a statutory chart, APAC posts per legal
    entity with an intercompany partner code on every centre.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Cost Center', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Cost Center]
    (
        [Cost Center Key]               INT             CONSTRAINT [DF_Dimension_Cost_Center_Key] DEFAULT (NEXT VALUE FOR [Sequences].[CostCenterKey]) NOT NULL,
        [Cost Center Code]              NVARCHAR(20)    NOT NULL,
        [Cost Center Name]              NVARCHAR(100)   NOT NULL,
        [Parent Cost Center Code]       NVARCHAR(20)    NULL,
        [Parent Cost Center Key]        INT             NULL,
        [Hierarchy Level]               SMALLINT        NULL,
        [Hierarchy Path]                NVARCHAR(400)   NULL,
        [Is Leaf Node]                  BIT             NULL,
        [Rollup Level 1 Code]           NVARCHAR(20)    NULL,
        [Rollup Level 2 Code]           NVARCHAR(20)    NULL,
        [Rollup Level 3 Code]           NVARCHAR(20)    NULL,
        [Rollup Level 4 Code]           NVARCHAR(20)    NULL,
        [Rollup Level 5 Code]           NVARCHAR(20)    NULL,
        [Rollup Level 6 Code]           NVARCHAR(20)    NULL,

        [Cost Center Type Code]         NVARCHAR(10)    NULL,   -- COST / PROFIT / INVEST / SERVICE
        [Function Code]                 NVARCHAR(10)    NULL,   -- SLS / MKT / OPS / WHS / FIN / IT / HR
        [Company Code]                  NVARCHAR(10)    NULL,
        [Legal Entity Code]             NVARCHAR(10)    NULL,
        [Region Key]                    INT             NULL,
        [Region Code]                   NVARCHAR(10)    NULL,
        [Country Code]                  NVARCHAR(3)     NULL,
        [Functional Currency Code]      NVARCHAR(3)     NULL,
        [Manager Employee Number]       NVARCHAR(20)    NULL,
        [Budget Owner Employee Number]  NVARCHAR(20)    NULL,
        [Annual Budget Amount]          DECIMAL(18, 2)  NULL,
        [Budget Fiscal Year]            SMALLINT        NULL,
        [Allocation Method Code]        NVARCHAR(20)    NULL,   -- DIRECT / HEADCOUNT / REVENUE / SQFT / NONE
        [Allocation Percentage]         DECIMAL(9, 4)   NULL,

        [EU Statutory Chart Code]       NVARCHAR(20)    NULL,
        [APAC Intercompany Partner Code] NVARCHAR(10)   NULL,
        [NA Department Analysis Code]   NVARCHAR(10)    NULL,

        [Is Active]                     BIT             NULL,
        [Blocked For Posting]           BIT             NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Effective From]                DATETIME2(7)    NOT NULL,
        [Effective To]                  DATETIME2(7)    NOT NULL,
        [Effective From Date]           DATE            NULL,
        [Effective Sequence]            SMALLINT        NULL,
        [Is Current Row]                BIT             NOT NULL,
        [Version Number]                INT             NULL,
        [Row Hash Type 2]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Cost_Center] PRIMARY KEY CLUSTERED ([Cost Center Key] ASC)
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Cost_Center_Current]
        ON [Dimension].[Cost Center] ([Cost Center Code] ASC, [Is Current Row] ASC)
        INCLUDE ([Cost Center Key], [Parent Cost Center Key], [Hierarchy Level]);
END;
GO
