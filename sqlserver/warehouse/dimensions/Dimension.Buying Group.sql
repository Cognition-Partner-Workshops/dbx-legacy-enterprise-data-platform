/*
    Object        : [Dimension].[Buying Group]  (SCD Type 2 - ownership history matters)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.BuyingGroupKey, Dimension.Currency
    Called by     : Integration.usp_MigrateStagedCustomerCategoryData (the Oracle
                    classification extract carries buying groups on the same feed),
                    Integration.usp_LoadBridgeCustomerBuyingGroup

    A buying group is a purchasing consortium that negotiates one rebate agreement
    on behalf of many customers. Membership is many-to-many and time-bound, which
    is why it is resolved through [Dimension].[Customer Buying Group Bridge] with
    an allocation factor rather than a single key on the customer. The single
    [Buying Group Key] on Dimension.Customer is the *primary* affiliation only,
    kept for the pre-2012 reports that predate the bridge.

    Type 2 because a buying group merging into another changes the rebate basis,
    and the rebate accrual for prior periods must stay attached to the old group.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Buying Group', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Buying Group]
    (
        [Buying Group Key]              INT             CONSTRAINT [DF_Dimension_Buying_Group_Key] DEFAULT (NEXT VALUE FOR [Sequences].[BuyingGroupKey]) NOT NULL,
        [WWI Buying Group ID]           INT             NULL,
        [Buying Group Code]             NVARCHAR(20)    NOT NULL,
        [Buying Group]                  NVARCHAR(100)   NOT NULL,
        [Buying Group Type Code]        NVARCHAR(10)    NULL,   -- CONS / FRAN / COOP / GOVT / INTL
        [Region Code]                   NVARCHAR(10)    NULL,
        [Home Country Code]             NVARCHAR(3)     NULL,
        [Settlement Currency Code]      NVARCHAR(3)     NULL,

        [Rebate Agreement Reference]    NVARCHAR(30)    NULL,
        [Rebate Basis Code]             NVARCHAR(10)    NULL,   -- NETREV / GROSSREV / UNITS / TIERED
        [Rebate Percentage]             DECIMAL(9, 4)   NULL,
        [Rebate Tier Threshold Amount]  DECIMAL(18, 2)  NULL,
        [Rebate Accrual Account Code]   NVARCHAR(20)    NULL,
        [Agreement Start Date]          DATE            NULL,
        [Agreement End Date]            DATE            NULL,
        [Negotiating Entity Name]       NVARCHAR(100)   NULL,

        /* Merger and acquisition history: the surviving group after a merge. */
        [Successor Buying Group Code]   NVARCHAR(20)    NULL,
        [Merged On]                     DATE            NULL,
        [Is Dissolved]                  BIT             NULL,

        /*
            EU consortia must be declared for competition-law purposes; the flag
            drives a quarterly extract. APAC groups are frequently a single legal
            entity trading under several names, so the local registration is kept.
        */
        [EU Competition Declaration Ref] NVARCHAR(30)   NULL,
        [EU Is Declared]                BIT             NULL,
        [APAC Local Registration No]    NVARCHAR(30)    NULL,
        [NA Group Purchasing Org Code]  NVARCHAR(20)    NULL,

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
        CONSTRAINT [PK_Dimension_Buying_Group] PRIMARY KEY CLUSTERED ([Buying Group Key] ASC)
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Buying_Group_Current]
        ON [Dimension].[Buying Group] ([Buying Group Code] ASC, [Is Current Row] ASC);
END;
GO
