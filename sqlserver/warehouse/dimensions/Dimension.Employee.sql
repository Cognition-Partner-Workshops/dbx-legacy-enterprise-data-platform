/*
    Object        : [Dimension].[Employee]  (SCD Type 2, recursive organisation hierarchy)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.EmployeeKey (WideWorldImportersDW baseline),
                    Dimension.Cost Center, Dimension.Sales Territory
    Called by     : Integration.usp_MigrateStagedEmployeeData,
                    Integration.usp_LoadBridgeEmployeeTerritory

    The organisation hierarchy is ragged: a warehouse picker reports to a shift
    lead who reports to a site manager, while a regional director reports straight
    to the country manager. It is stored as a self-referencing parent key plus a
    denormalised path string, because the reporting tool cannot follow a recursive
    join and the 2010 workaround was to materialise the path.

    Employment changes open a new Type 2 row (department, manager, grade, cost
    centre, employment status). Personal contact details are Type 1.

    Regional divergence
      NA    : at-will employment, exempt/non-exempt classification, benefits
              eligibility date, EEO job category.
      EU    : works council flag, collective agreement code, contractual notice
              period in months, working-time-directive opt-out flag.
      APAC  : local employment type codes, provident-fund membership number type,
              visa/work-permit expiry tracked for contractors.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Employee', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Employee]
    (
        [Employee Key]      INT             CONSTRAINT [DF_Dimension_Employee_Employee_Key] DEFAULT (NEXT VALUE FOR [Sequences].[EmployeeKey]) NOT NULL,
        [WWI Employee ID]   INT             NOT NULL,
        [Employee]          NVARCHAR(50)    NOT NULL,
        [Preferred Name]    NVARCHAR(50)    NOT NULL,
        [Is Salesperson]    BIT             NOT NULL,
        [Photo]             VARBINARY(MAX)  NULL,
        [Valid From]        DATETIME2(7)    NOT NULL,
        [Valid To]          DATETIME2(7)    NOT NULL,
        [Lineage Key]       INT             NOT NULL,
        CONSTRAINT [PK_Dimension_Employee] PRIMARY KEY CLUSTERED ([Employee Key] ASC)
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Employee_WWIEmployeeID]
        ON [Dimension].[Employee] ([WWI Employee ID] ASC, [Valid From] ASC, [Valid To] ASC);
END;
GO

IF COL_LENGTH(N'Dimension.Employee', N'Source System Code') IS NULL
    ALTER TABLE [Dimension].[Employee] ADD
        [Source System Code]            NVARCHAR(20)    NULL,
        [Employee Number]               NVARCHAR(20)    NULL,
        [Region Code]                   NVARCHAR(10)    NULL,
        [Country Code]                  NVARCHAR(3)     NULL,
        [Department Code]               NVARCHAR(20)    NULL,
        [Department Name]               NVARCHAR(60)    NULL,
        [Job Title]                     NVARCHAR(60)    NULL,
        [Job Grade Code]                NVARCHAR(10)    NULL,
        [Cost Center Key]               INT             NULL,
        [Primary Sales Territory Key]   INT             NULL,
        [Work Location Code]            NVARCHAR(20)    NULL,
        [Hire Date]                     DATE            NULL,
        [Termination Date]              DATE            NULL,
        [Employment Status Code]        NVARCHAR(10)    NULL,   -- ACT / LOA / SUS / TRM / RET
        [Employment Type Code]          NVARCHAR(10)    NULL,   -- FT / PT / CON / TMP / INT
        [Is Manager]                    BIT             NULL,
        [Is Active]                     BIT             NULL;
GO

/*
    Ragged, recursive organisation hierarchy. [Manager Employee Key] points at
    another row of this dimension; [Organisation Path] is the materialised
    ancestry, maintained by the load procedure with a recursive CTE and stored as
    a slash-delimited string of employee numbers.
*/
IF COL_LENGTH(N'Dimension.Employee', N'Manager Employee Key') IS NULL
    ALTER TABLE [Dimension].[Employee] ADD
        [Manager Employee Key]          INT             NULL,
        [Manager Employee Number]       NVARCHAR(20)    NULL,
        [Organisation Level]            SMALLINT        NULL,
        [Organisation Path]             NVARCHAR(400)   NULL,
        [Organisation Unit Level 1]     NVARCHAR(60)    NULL,   -- company
        [Organisation Unit Level 2]     NVARCHAR(60)    NULL,   -- division
        [Organisation Unit Level 3]     NVARCHAR(60)    NULL,   -- department
        [Organisation Unit Level 4]     NVARCHAR(60)    NULL,   -- team; repeats level 3 where the branch is short
        [Is Leaf Node]                  BIT             NULL;
GO

/* NA employment attributes. */
IF COL_LENGTH(N'Dimension.Employee', N'FLSA Classification') IS NULL
    ALTER TABLE [Dimension].[Employee] ADD
        [FLSA Classification]           NVARCHAR(15)    NULL,   -- Exempt / NonExempt
        [Benefits Eligible From]        DATE            NULL,
        [EEO Job Category]              NVARCHAR(30)    NULL,
        [Union Local Code]              NVARCHAR(10)    NULL;
GO

/* EU employment attributes. */
IF COL_LENGTH(N'Dimension.Employee', N'Collective Agreement Code') IS NULL
    ALTER TABLE [Dimension].[Employee] ADD
        [Collective Agreement Code]     NVARCHAR(20)    NULL,
        [Is Works Council Member]       BIT             NULL,
        [Notice Period Months]          SMALLINT        NULL,
        [Working Time Opt Out]          BIT             NULL,
        [Data Retention Expiry Date]    DATE            NULL;
GO

/* APAC employment attributes. */
IF COL_LENGTH(N'Dimension.Employee', N'Provident Fund Number Type') IS NULL
    ALTER TABLE [Dimension].[Employee] ADD
        [Provident Fund Number Type]    NVARCHAR(15)    NULL,   -- CPF / EPF / SUPER / NPS
        [Work Permit Type Code]         NVARCHAR(15)    NULL,
        [Work Permit Expiry Date]       DATE            NULL,
        [Local Script Name]             NVARCHAR(200)   NULL;
GO

IF COL_LENGTH(N'Dimension.Employee', N'Effective From') IS NULL
    ALTER TABLE [Dimension].[Employee] ADD
        [Effective From]                DATETIME2(7)    NULL,
        [Effective To]                  DATETIME2(7)    NULL,
        [Effective From Date]           DATE            NULL,
        [Effective Sequence]            SMALLINT        NULL,
        [Is Current Row]                BIT             NULL,
        [Version Number]                INT             NULL,
        [Row Hash Type 2]               VARBINARY(32)   NULL,
        [Is Inferred Member]            BIT             NULL,
        [Last Load Batch Id]            BIGINT          NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Dimension_Employee_Current'
                 AND object_id = OBJECT_ID(N'Dimension.Employee'))
    CREATE NONCLUSTERED INDEX [IX_Dimension_Employee_Current]
        ON [Dimension].[Employee] ([WWI Employee ID] ASC, [Is Current Row] ASC)
        INCLUDE ([Employee Key], [Manager Employee Key], [Organisation Level]);
GO
