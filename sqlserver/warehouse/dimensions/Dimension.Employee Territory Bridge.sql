/*
    Object        : [Dimension].[Employee Territory Bridge]  (many-to-many bridge with allocation factors)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Employee.sql, Dimension.Salesperson.sql and Dimension.Sales Territory.sql
    Depends on    : Dimension.Employee, Dimension.Salesperson, Dimension.Sales Territory
    Called by     : Integration.usp_LoadBridgeEmployeeTerritory

    Salespeople cover more than one territory and a territory is covered by more
    than one salesperson (a field rep, an inside rep and, in EU, a national account
    manager who overlays both). Quota and commission are split between them by
    [Allocation Factor], and the split is not necessarily equal.

    The bridge is also where the *historical* territory alignment lives, because
    [Dimension].[Sales Territory] is Type 1 and overwrites at each January
    realignment. A period-correct territory report must go through this bridge.

    Regional divergence in the coverage model:
      NA    : one primary rep per territory plus an inside-sales overlay at a
              fixed 0.20 allocation; specialists carry a category scope.
      EU    : national account managers overlay the country territories, and the
              works-council agreement forbids a rep being allocated below 0.25,
              which the load enforces by rejecting the row.
      APAC  : distributor-managed territories have no rep at all and are
              allocated to a channel manager with [Is Distributor Managed] = 1.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Employee Territory Bridge', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Employee Territory Bridge]
    (
        [Employee Territory Bridge Key] BIGINT          IDENTITY(1, 1) NOT NULL,
        [WWI Employee ID]               INT             NOT NULL,
        [Sales Territory Code]          NVARCHAR(20)    NOT NULL,
        [Employee Key]                  INT             NULL,
        [Salesperson Key]               INT             NULL,
        [Sales Territory Key]           INT             NULL,
        [Alignment Year]                SMALLINT        NOT NULL,
        [Coverage From]                 DATE            NOT NULL,
        [Coverage To]                   DATE            NOT NULL,
        [Is Current Coverage]           BIT             NOT NULL,

        [Coverage Role Code]            NVARCHAR(15)    NOT NULL,   -- PRIMARY / OVERLAY / INSIDE / SPECIALIST / CHANNEL
        [Allocation Factor]             DECIMAL(9, 6)   NOT NULL,   -- sums to 1.0 per territory per period
        [Allocation Basis Code]         NVARCHAR(15)    NULL,       -- QUOTA / ACCOUNTS / EQUAL / MANUAL
        [Quota Share Amount]            DECIMAL(18, 2)  NULL,
        [Quota Currency Code]           NVARCHAR(3)     NULL,
        [Commission Share Percentage]   DECIMAL(9, 4)   NULL,
        [Product Category Scope]        NVARCHAR(40)    NULL,       -- specialists only
        [Is Distributor Managed]        BIT             NULL,       -- APAC channel territories
        [Region Code]                   NVARCHAR(10)    NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        [Last Load Package Execution Id] BIGINT         NULL,
        CONSTRAINT [PK_Dimension_Employee_Territory_Bridge]
            PRIMARY KEY CLUSTERED ([Employee Territory Bridge Key] ASC),
        CONSTRAINT [UQ_Dimension_Employee_Territory_Bridge_Period]
            UNIQUE ([WWI Employee ID], [Sales Territory Code], [Coverage From]),
        CONSTRAINT [CK_Dimension_Employee_Territory_Bridge_Factor]
            CHECK ([Allocation Factor] > 0 AND [Allocation Factor] <= 1),
        CONSTRAINT [CK_Dimension_Employee_Territory_Bridge_Role]
            CHECK ([Coverage Role Code] IN (N'PRIMARY', N'OVERLAY', N'INSIDE', N'SPECIALIST', N'CHANNEL'))
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Employee_Territory_Bridge_Territory]
        ON [Dimension].[Employee Territory Bridge] ([Sales Territory Code] ASC, [Alignment Year] ASC)
        INCLUDE ([WWI Employee ID], [Allocation Factor], [Coverage Role Code]);
END;
GO
