/*
    Object        : [Dimension].[Customer]  (hybrid SCD - Type 2 history plus Type 1 overwrite attributes)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.CustomerKey (WideWorldImportersDW baseline),
                    Dimension.Customer Category, Dimension.Buying Group,
                    Dimension.Customer Demographic (mini dimension), Dimension.City
    Called by     : Integration.usp_MigrateStagedCustomerDataV2 (all three regional
                    DIM_*_Load_Customer packages), Integration.usp_InsertInferredMember,
                    Integration.usp_EnrichInferredMembers

    History
      2004  Shipped as the Microsoft WideWorldImportersDW customer dimension:
            [Customer Key], [WWI Customer ID], [Valid From]/[Valid To], [Lineage Key].
      2009  NA sales tax and EU VAT columns bolted on when the estate went
            multi-region. APAC followed in 2012 with GST, hence the third set of
            columns rather than one generic tax block - nobody dared refactor it.
      2014  Type 2 mechanics made explicit ([Effective From]/[Effective To],
            [Is Current Row], [Version Number], [Row Hash Type 2]) because the
            [Valid From]/[Valid To] pair was being used for two different things.
            Both are still maintained by the load procedure.
      2018  Consent and retention columns added for EU data protection work; NA and
            APAC populate a subset with different defaults.

    Regional divergence carried in this table
      NA    : [Sales Tax Jurisdiction Code], [Sales Tax Exempt Flag], ZIP+4 postal,
              consent defaults to opt-out (implied consent), 7 year retention.
      EU    : [VAT Registration Number], [VAT Rate Category], alphanumeric postcode,
              explicit opt-in consent with a consent timestamp and source, right to
              erasure honoured through [Erasure Requested On], 6 year retention.
      APAC  : [GST Registration Number], [GST Treatment Code], mixed postal formats,
              consent per-channel (marketing versus profiling), 5 year retention,
              plus [Local Script Name] because the source carries a non-Latin name.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Customer', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Customer]
    (
        [Customer Key]                  INT             CONSTRAINT [DF_Dimension_Customer_Customer_Key] DEFAULT (NEXT VALUE FOR [Sequences].[CustomerKey]) NOT NULL,
        [WWI Customer ID]               INT             NOT NULL,
        [Customer]                      NVARCHAR(100)   NOT NULL,
        [Bill To Customer]              NVARCHAR(100)   NOT NULL,
        [Category]                      NVARCHAR(50)    NOT NULL,
        [Buying Group]                  NVARCHAR(50)    NOT NULL,
        [Primary Contact]               NVARCHAR(50)    NOT NULL,
        [Postal Code]                   NVARCHAR(10)    NOT NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        CONSTRAINT [PK_Dimension_Customer] PRIMARY KEY CLUSTERED ([Customer Key] ASC)
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Customer_WWICustomerID]
        ON [Dimension].[Customer] ([WWI Customer ID] ASC, [Valid From] ASC, [Valid To] ASC);
END;
GO

/*
    2009 - 2018 bolt-on columns. Written as guarded ALTERs so the script can be
    deployed on top of the shipped Microsoft dimension without recreating it.
*/
IF COL_LENGTH(N'Dimension.Customer', N'Source System Code') IS NULL
    ALTER TABLE [Dimension].[Customer] ADD
        [Source System Code]            NVARCHAR(20)    NULL,
        [Source Customer Reference]     NVARCHAR(50)    NULL,
        [Region Code]                   NVARCHAR(10)    NULL,
        [Country Code]                  NVARCHAR(3)     NULL,
        [Customer Category Key]         INT             NULL,
        [Buying Group Key]              INT             NULL,
        [Customer Segment Key]          INT             NULL,
        [Customer Demographic Key]      INT             NULL,
        [Delivery City Key]             INT             NULL,
        [Postal City Key]               INT             NULL;
GO

IF COL_LENGTH(N'Dimension.Customer', N'Account Opened Date') IS NULL
    ALTER TABLE [Dimension].[Customer] ADD
        [Account Opened Date]           DATE            NULL,
        [Account Status Code]           NVARCHAR(10)    NULL,   -- A / H / S / C / X, see Dimension.Customer Category notes
        [Credit Limit Amount]           DECIMAL(18, 2)  NULL,
        [Credit Limit Currency Code]    NVARCHAR(3)     NULL,
        [Payment Terms Code]            NVARCHAR(10)    NULL,
        [Is On Credit Hold]             BIT             NULL,
        [Standard Discount Percentage]  DECIMAL(9, 4)   NULL,
        [Primary Salesperson Reference] NVARCHAR(50)    NULL,
        [Website URL]                   NVARCHAR(256)   NULL,
        [Phone Number Standardized]     NVARCHAR(30)    NULL;
GO

/*
    NA. Sales tax is levied per jurisdiction (state / county / city), so the
    jurisdiction code is on the customer and the rate is looked up per transaction.
*/
IF COL_LENGTH(N'Dimension.Customer', N'Sales Tax Jurisdiction Code') IS NULL
    ALTER TABLE [Dimension].[Customer] ADD
        [Sales Tax Jurisdiction Code]   NVARCHAR(15)    NULL,
        [Sales Tax Exempt Flag]         BIT             NULL,
        [Sales Tax Exemption Reference] NVARCHAR(30)    NULL,
        [Resale Certificate Expiry]     DATE            NULL,
        [ZIP Plus Four]                 NVARCHAR(10)    NULL,
        [State Province Code]           NVARCHAR(5)     NULL;
GO

/*
    EU. VAT is a registration on the counterparty, reverse charge applies for
    cross-border B2B, and the estate keeps the VIES validation outcome.
*/
IF COL_LENGTH(N'Dimension.Customer', N'VAT Registration Number') IS NULL
    ALTER TABLE [Dimension].[Customer] ADD
        [VAT Registration Number]       NVARCHAR(20)    NULL,
        [VAT Rate Category]             NVARCHAR(10)    NULL,   -- STD / RED / SUP / ZER / EXE
        [VAT Validation Status]         NVARCHAR(15)    NULL,   -- Valid / Invalid / NotChecked
        [VAT Validated On]              DATE            NULL,
        [Is Reverse Charge Applicable]  BIT             NULL,
        [EU Member State Code]          NVARCHAR(2)     NULL,
        [Postcode Standardized]         NVARCHAR(12)    NULL;
GO

/*
    APAC. GST / consumption tax varies by country; the estate stores the treatment
    code the local finance team maintains by hand in a spreadsheet-fed reference file.
*/
IF COL_LENGTH(N'Dimension.Customer', N'GST Registration Number') IS NULL
    ALTER TABLE [Dimension].[Customer] ADD
        [GST Registration Number]       NVARCHAR(20)    NULL,
        [GST Treatment Code]            NVARCHAR(10)    NULL,   -- TAX / ZRL / EXP / OOS / EXM
        [Business Number Type]          NVARCHAR(10)    NULL,   -- ABN / GSTIN / UEN / BRN
        [Local Script Name]             NVARCHAR(200)   NULL,
        [Prefecture Or Province]        NVARCHAR(60)    NULL,
        [Postal Format Code]            NVARCHAR(10)    NULL;
GO

/*
    Consent and retention. Type 1 by policy: a consent withdrawal must apply to
    every historical row, so the load procedure overwrites these on all versions of
    the customer rather than opening a new one.
*/
IF COL_LENGTH(N'Dimension.Customer', N'Consent Basis Code') IS NULL
    ALTER TABLE [Dimension].[Customer] ADD
        [Consent Basis Code]            NVARCHAR(20)    NULL,   -- OPTIN / OPTOUT / CONTRACT / LEGITINT
        [Marketing Consent Flag]        BIT             NULL,
        [Profiling Consent Flag]        BIT             NULL,
        [Consent Captured On]           DATETIME2(7)    NULL,
        [Consent Source Code]           NVARCHAR(20)    NULL,   -- WEB / CALL / PAPER / IMPORT
        [Erasure Requested On]          DATETIME2(7)    NULL,
        [Retention Expiry Date]         DATE            NULL,
        [Is Pseudonymized]              BIT             NULL;
GO

/*
    Type 2 mechanics. [Valid From]/[Valid To] are kept for the reports written
    against the Microsoft sample; [Effective From]/[Effective To] plus
    [Is Current Row] are what the 2014-and-later loads and the fact lookups use.
    [Effective Sequence] disambiguates two changes on the same day, which the
    date-grain [Effective From] cannot.
*/
IF COL_LENGTH(N'Dimension.Customer', N'Effective From') IS NULL
    ALTER TABLE [Dimension].[Customer] ADD
        [Effective From]                DATETIME2(7)    NULL,
        [Effective To]                  DATETIME2(7)    NULL,
        [Effective From Date]           DATE            NULL,
        [Effective Sequence]            SMALLINT        NULL,
        [Is Current Row]                BIT             NULL,
        [Version Number]                INT             NULL,
        [Row Hash Type 2]               VARBINARY(32)   NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Is Inferred Member]            BIT             NULL,
        [Inferred Created On]           DATETIME2(7)    NULL,
        [Enriched On]                   DATETIME2(7)    NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        [Last Load Package Execution Id] BIGINT         NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Dimension_Customer_Current'
                 AND object_id = OBJECT_ID(N'Dimension.Customer'))
    CREATE NONCLUSTERED INDEX [IX_Dimension_Customer_Current]
        ON [Dimension].[Customer] ([WWI Customer ID] ASC, [Is Current Row] ASC)
        INCLUDE ([Customer Key], [Region Code], [Row Hash Type 2], [Version Number]);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Dimension_Customer_Inferred'
                 AND object_id = OBJECT_ID(N'Dimension.Customer'))
    CREATE NONCLUSTERED INDEX [IX_Dimension_Customer_Inferred]
        ON [Dimension].[Customer] ([Is Inferred Member] ASC)
        INCLUDE ([Customer Key], [WWI Customer ID], [Source System Code]);
GO

EXECUTE sp_addextendedproperty @name = N'Description'
      , @value = N'Hybrid SCD: Type 2 on trading name, category, buying group, address and tax registration; Type 1 on consent, credit and contact attributes.'
      , @level0type = N'SCHEMA', @level0name = N'Dimension'
      , @level1type = N'TABLE',  @level1name = N'Customer'
      , @level2type = N'COLUMN', @level2name = N'Row Hash Type 2';
GO
