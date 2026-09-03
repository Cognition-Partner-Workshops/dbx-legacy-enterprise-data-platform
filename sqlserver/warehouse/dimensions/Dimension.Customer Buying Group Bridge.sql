/*
    Object        : [Dimension].[Customer Buying Group Bridge]  (many-to-many bridge with allocation factors)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Customer.sql and Dimension.Buying Group.sql
    Depends on    : Dimension.Customer, Dimension.Buying Group
    Called by     : Integration.usp_LoadBridgeCustomerBuyingGroup

    A customer can belong to several buying groups at once - a hospital group
    buying through a national consortium for pharmaceuticals and a regional
    co-operative for consumables is the standard example - and the revenue must be
    split between them for rebate accrual.

    [Allocation Factor] is that split. The load enforces that the factors for a
    customer sum to 1.0 within a tolerance of 0.0001 and routes the whole customer
    to the reject table when they do not, because a silent partial allocation
    understates a rebate and that has happened.

    Membership is time-bound ([Membership From]/[Membership To]) so a query for a
    historical period picks up the membership in force then. The bridge is keyed on
    the *durable* business keys rather than the surrogate keys, with the surrogate
    keys resolved to the current dimension rows, because a Type 2 change on the
    customer must not silently drop the membership.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Customer Buying Group Bridge', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Customer Buying Group Bridge]
    (
        [Customer Buying Group Bridge Key] BIGINT       IDENTITY(1, 1) NOT NULL,
        [WWI Customer ID]               INT             NOT NULL,
        [Buying Group Code]             NVARCHAR(20)    NOT NULL,
        [Customer Key]                  INT             NULL,       -- current customer row
        [Buying Group Key]              INT             NULL,       -- current buying group row
        [Membership From]               DATE            NOT NULL,
        [Membership To]                 DATE            NOT NULL,   -- 9999-12-31 while current
        [Is Current Membership]         BIT             NOT NULL,
        [Is Primary Affiliation]        BIT             NULL,

        [Allocation Factor]             DECIMAL(9, 6)   NOT NULL,   -- sums to 1.0 per customer per period
        [Allocation Basis Code]         NVARCHAR(15)    NULL,       -- CONTRACT / SPEND / CATEGORY / EQUAL / MANUAL
        [Allocation Category Scope]     NVARCHAR(40)    NULL,       -- populated when the split is category-specific
        [Allocation Reviewed On]        DATE            NULL,
        [Allocation Reviewed By]        NVARCHAR(60)    NULL,

        [Rebate Eligible]               BIT             NULL,
        [Rebate Agreement Reference]    NVARCHAR(30)    NULL,
        [Region Code]                   NVARCHAR(10)    NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Source Membership Reference]   NVARCHAR(40)    NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        [Last Load Package Execution Id] BIGINT         NULL,
        CONSTRAINT [PK_Dimension_Customer_Buying_Group_Bridge]
            PRIMARY KEY CLUSTERED ([Customer Buying Group Bridge Key] ASC),
        CONSTRAINT [UQ_Dimension_Customer_Buying_Group_Bridge_Period]
            UNIQUE ([WWI Customer ID], [Buying Group Code], [Membership From]),
        CONSTRAINT [CK_Dimension_Customer_Buying_Group_Bridge_Factor]
            CHECK ([Allocation Factor] >= 0 AND [Allocation Factor] <= 1)
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Customer_Buying_Group_Bridge_Current]
        ON [Dimension].[Customer Buying Group Bridge] ([Is Current Membership] ASC, [WWI Customer ID] ASC)
        INCLUDE ([Buying Group Key], [Allocation Factor]);

    CREATE NONCLUSTERED INDEX [IX_Dimension_Customer_Buying_Group_Bridge_Group]
        ON [Dimension].[Customer Buying Group Bridge] ([Buying Group Code] ASC, [Membership From] ASC, [Membership To] ASC);
END;
GO
