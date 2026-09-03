/*
    Object        : [Dimension].[GL Account]  (SCD Type 1, ragged recursive account hierarchy)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Cost Center.sql
    Depends on    : Sequences.GlAccountKey
    Called by     : Integration.usp_LoadGlAccountHierarchy

    The chart of accounts as a parent-child tree. Ragged by construction: the
    balance-sheet branch is five levels deep, the statistical-accounts branch is
    two, and a handful of legacy accounts from the 1997 conversion hang directly
    off the root with no rollup at all ([Is Orphan Account] = 1).

    Two charts coexist. The group chart is the reporting chart; the EU statutory
    chart is a second numbering the local ledgers post to, mapped account by
    account in [Statutory Account Code]. The mapping is not one-to-one - several
    group accounts map to one statutory account - so the statutory reports
    aggregate and the group reports do not reconcile line by line.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.GL Account', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[GL Account]
    (
        [GL Account Key]                INT             CONSTRAINT [DF_Dimension_GL_Account_Key] DEFAULT (NEXT VALUE FOR [Sequences].[GlAccountKey]) NOT NULL,
        [GL Account Code]               NVARCHAR(20)    NOT NULL,
        [GL Account Name]               NVARCHAR(120)   NOT NULL,
        [Parent GL Account Code]        NVARCHAR(20)    NULL,
        [Parent GL Account Key]         INT             NULL,
        [Hierarchy Level]               SMALLINT        NULL,
        [Hierarchy Path]                NVARCHAR(400)   NULL,
        [Is Leaf Node]                  BIT             NULL,
        [Is Orphan Account]             BIT             NULL,   -- 1997 conversion leftovers with no rollup
        [Is Postable]                   BIT             NULL,

        [Account Type Code]             NVARCHAR(10)    NULL,   -- ASSET / LIAB / EQTY / REV / COGS / EXP / STAT
        [Account Class Code]            NVARCHAR(10)    NULL,
        [Normal Balance Side]           NVARCHAR(2)     NULL,   -- DR / CR
        [Financial Statement Code]      NVARCHAR(10)    NULL,   -- BS / PL / CF / MEMO
        [Statement Line Code]           NVARCHAR(20)    NULL,
        [Statement Sort Order]          INT             NULL,
        [Is Intercompany Account]       BIT             NULL,
        [Is Cash Account]               BIT             NULL,
        [Is Revaluation Account]        BIT             NULL,
        [Requires Cost Center]          BIT             NULL,
        [Requires Project Code]         BIT             NULL,

        [Statutory Account Code]        NVARCHAR(20)    NULL,   -- EU local chart
        [Statutory Chart Code]          NVARCHAR(20)    NULL,
        [NA Tax Line Reference]         NVARCHAR(20)    NULL,
        [APAC Local Account Code]       NVARCHAR(20)    NULL,
        [Consolidation Account Code]    NVARCHAR(20)    NULL,

        [Effective From Date]           DATE            NULL,
        [Blocked For Posting]           BIT             NULL,
        [Is Active]                     BIT             NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_GL_Account] PRIMARY KEY CLUSTERED ([GL Account Key] ASC),
        CONSTRAINT [UQ_Dimension_GL_Account_Code] UNIQUE ([GL Account Code]),
        CONSTRAINT [CK_Dimension_GL_Account_Balance_Side]
            CHECK ([Normal Balance Side] IS NULL OR [Normal Balance Side] IN (N'DR', N'CR'))
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_GL_Account_Parent]
        ON [Dimension].[GL Account] ([Parent GL Account Key] ASC)
        INCLUDE ([GL Account Code], [Hierarchy Level], [Is Leaf Node]);
END;
GO
