/*
    Object        : [Dimension].[Supplier]  (hybrid SCD - Type 2 on trading and compliance attributes)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.SupplierKey (WideWorldImportersDW baseline),
                    Dimension.Supplier Category, Dimension.Payment Terms, Dimension.Geography
    Called by     : Integration.usp_MigrateStagedSupplierDataV2, Integration.usp_EnrichInferredMembers

    Sourced from Oracle WWI_MDM.SUPP_MASTER, which the 2006 procurement project
    made the system of record. The Oracle side carries the supplier under a
    six-character mnemonic ([Source Supplier Reference]) while the SQL Server OLTP
    still uses the integer [WWI Supplier ID]; both are kept because half the
    downstream reports join on one and half on the other.

    Type 2 : category, payment terms, bank country, approval status, risk rating,
             preferred flag, contract-mandated lead time.
    Type 1 : contact details, notes, sanction-screening outcome (always current).

    Regional divergence
      NA    : W-9 on file flag, 1099 reportable flag, state withholding code.
      EU    : VAT number and VIES validation, EORI number, Intrastat obligation,
              late-payment directive terms (max 60 days) enforced in the load.
      APAC  : withholding tax rate by country, local business number type,
              consumption-tax registration, invoice-language code.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Supplier', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Supplier]
    (
        [Supplier Key]          INT             CONSTRAINT [DF_Dimension_Supplier_Supplier_Key] DEFAULT (NEXT VALUE FOR [Sequences].[SupplierKey]) NOT NULL,
        [WWI Supplier ID]       INT             NOT NULL,
        [Supplier]              NVARCHAR(100)   NOT NULL,
        [Category]              NVARCHAR(50)    NOT NULL,
        [Primary Contact]       NVARCHAR(50)    NOT NULL,
        [Supplier Reference]    NVARCHAR(20)    NULL,
        [Payment Days]          INT             NOT NULL,
        [Postal Code]           NVARCHAR(10)    NOT NULL,
        [Valid From]            DATETIME2(7)    NOT NULL,
        [Valid To]              DATETIME2(7)    NOT NULL,
        [Lineage Key]           INT             NOT NULL,
        CONSTRAINT [PK_Dimension_Supplier] PRIMARY KEY CLUSTERED ([Supplier Key] ASC)
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Supplier_WWISupplierID]
        ON [Dimension].[Supplier] ([WWI Supplier ID] ASC, [Valid From] ASC, [Valid To] ASC);
END;
GO

IF COL_LENGTH(N'Dimension.Supplier', N'Source System Code') IS NULL
    ALTER TABLE [Dimension].[Supplier] ADD
        [Source System Code]            NVARCHAR(20)    NULL,
        [Source Supplier Reference]     NVARCHAR(10)    NULL,   -- Oracle six-character mnemonic
        [Supplier Category Key]         INT             NULL,
        [Payment Terms Key]             INT             NULL,
        [Geography Key]                 INT             NULL,
        [Region Code]                   NVARCHAR(10)    NULL,
        [Country Code]                  NVARCHAR(3)     NULL,
        [Supplier Status Code]          NVARCHAR(10)    NULL,   -- PND / APP / HLD / BLK / TRM
        [Approval Status Code]          NVARCHAR(10)    NULL,
        [Approved On]                   DATE            NULL,
        [Onboarded On]                  DATE            NULL,
        [Is Preferred Supplier]         BIT             NULL,
        [Is Single Source]              BIT             NULL,
        [Risk Rating Code]              NVARCHAR(5)     NULL,   -- A / B / C / D, refreshed quarterly by hand
        [Contract Lead Time Days]       INT             NULL,
        [Quality Rating]                DECIMAL(5, 2)   NULL;
GO

IF COL_LENGTH(N'Dimension.Supplier', N'Bank Country Code') IS NULL
    ALTER TABLE [Dimension].[Supplier] ADD
        [Bank Country Code]             NVARCHAR(3)     NULL,
        [Settlement Currency Code]      NVARCHAR(3)     NULL,
        [Remittance Method Code]        NVARCHAR(10)    NULL,   -- ACH / SEPA / WIRE / CHQ / BPAY
        [Sanction Screening Status]     NVARCHAR(15)    NULL,   -- Clear / Review / Blocked
        [Sanction Screened On]          DATE            NULL,
        [Diversity Classification Code] NVARCHAR(10)    NULL;
GO

/* NA procurement compliance. */
IF COL_LENGTH(N'Dimension.Supplier', N'Has W9 On File') IS NULL
    ALTER TABLE [Dimension].[Supplier] ADD
        [Has W9 On File]                BIT             NULL,
        [Is 1099 Reportable]            BIT             NULL,
        [Taxpayer Identification Type]  NVARCHAR(10)    NULL,   -- EIN / SSN / ITIN
        [State Withholding Code]        NVARCHAR(5)     NULL;
GO

/* EU procurement compliance. */
IF COL_LENGTH(N'Dimension.Supplier', N'EU VAT Number') IS NULL
    ALTER TABLE [Dimension].[Supplier] ADD
        [EU VAT Number]                 NVARCHAR(20)    NULL,
        [EU VAT Validation Status]      NVARCHAR(15)    NULL,
        [EORI Number]                   NVARCHAR(20)    NULL,
        [Is Intrastat Reportable]       BIT             NULL,
        [Late Payment Directive Days]   INT             NULL,   -- capped at 60 by the load procedure
        [Is Reverse Charge Supplier]    BIT             NULL;
GO

/* APAC procurement compliance. */
IF COL_LENGTH(N'Dimension.Supplier', N'Withholding Tax Rate') IS NULL
    ALTER TABLE [Dimension].[Supplier] ADD
        [Withholding Tax Rate]          DECIMAL(9, 4)   NULL,
        [Business Number Type]          NVARCHAR(10)    NULL,   -- ABN / GSTIN / UEN / BRN
        [Business Number]               NVARCHAR(20)    NULL,
        [Consumption Tax Registered]    BIT             NULL,
        [Invoice Language Code]         NVARCHAR(5)     NULL;
GO

IF COL_LENGTH(N'Dimension.Supplier', N'Effective From') IS NULL
    ALTER TABLE [Dimension].[Supplier] ADD
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
        [Payment Terms Code]            NVARCHAR(10)    NULL,   -- kept alongside the key: the AP extract sends the code
        /*
            The 2010 Oracle conversion matched on the mnemonic where it existed and
            on the integer where it did not, and created a second row for the
            suppliers that had both. Those rows are still here and every consuming
            query filters them out on this flag rather than deleting them, because
            the 2010 purchase orders point at their keys.
        */
        [Is Superseded Duplicate]       BIT             NULL,
        [Superseded By Supplier Key]    INT             NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        [Last Load Package Execution Id] BIGINT         NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Dimension_Supplier_Current'
                 AND object_id = OBJECT_ID(N'Dimension.Supplier'))
    CREATE NONCLUSTERED INDEX [IX_Dimension_Supplier_Current]
        ON [Dimension].[Supplier] ([WWI Supplier ID] ASC, [Is Current Row] ASC)
        INCLUDE ([Supplier Key], [Row Hash Type 2], [Region Code]);
GO
