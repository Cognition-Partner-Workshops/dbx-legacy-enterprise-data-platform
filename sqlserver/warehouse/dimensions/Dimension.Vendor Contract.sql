/*
    Object        : [Dimension].[Vendor Contract]  (SCD Type 2 - amendment history is the dimension)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Supplier.sql
    Depends on    : Sequences.VendorContractKey, Dimension.Supplier, Dimension.Currency
    Called by     : Integration.usp_MigrateStagedVendorContractData

    Source is Oracle WWI_PROC.VENDOR_CONTRACT, one row per contract *amendment*.
    The Oracle side never deletes: an amendment supersedes its predecessor by
    incrementing AMEND_NO, and the estate turns that into Type 2 versions. A
    contract can be amended twice on the same day (a price schedule change and a
    term extension are separate amendments), which is exactly the same-day case
    [Effective Sequence] exists for.

    Price protection, rebate tiers and penalty clauses differ by region:
      NA    : firm-fixed pricing for the contract term, liquidated damages clause.
      EU    : indexed pricing tied to a published index, statutory late-payment
              interest, and a mandatory 30-day termination notice.
      APAC  : FX-collar pricing with a revaluation trigger, local-content
              undertakings, and per-country stamp duty on the contract itself.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Vendor Contract', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Vendor Contract]
    (
        [Vendor Contract Key]           INT             CONSTRAINT [DF_Dimension_Vendor_Contract_Key] DEFAULT (NEXT VALUE FOR [Sequences].[VendorContractKey]) NOT NULL,
        [Contract Number]               NVARCHAR(30)    NOT NULL,
        [Amendment Number]              SMALLINT        NOT NULL,
        [Supplier Key]                  INT             NULL,
        [Source Supplier Reference]     NVARCHAR(10)    NULL,
        [Contract Title]                NVARCHAR(200)   NULL,
        [Contract Type Code]            NVARCHAR(10)    NULL,   -- MSA / SOW / PRICE / FRAME / SLA
        [Contract Status Code]          NVARCHAR(10)    NULL,   -- DRAFT / ACT / SUSP / EXP / TERM
        [Region Code]                   NVARCHAR(10)    NULL,
        [Governing Law Country Code]    NVARCHAR(3)     NULL,
        [Contract Currency Code]        NVARCHAR(3)     NULL,

        [Contract Start Date]           DATE            NULL,
        [Contract End Date]             DATE            NULL,
        [Auto Renew Flag]               BIT             NULL,
        [Renewal Notice Days]           INT             NULL,
        [Committed Spend Amount]        DECIMAL(18, 2)  NULL,
        [Minimum Order Value]           DECIMAL(18, 2)  NULL,
        [Payment Terms Code]            NVARCHAR(10)    NULL,
        [Price Protection Code]         NVARCHAR(10)    NULL,   -- FIRM / INDEX / COLLAR / NONE
        [Price Index Reference]         NVARCHAR(30)    NULL,
        [FX Collar Lower Rate]          DECIMAL(18, 6)  NULL,
        [FX Collar Upper Rate]          DECIMAL(18, 6)  NULL,
        [Rebate Tier 1 Threshold]       DECIMAL(18, 2)  NULL,
        [Rebate Tier 1 Percentage]      DECIMAL(9, 4)   NULL,
        [Rebate Tier 2 Threshold]       DECIMAL(18, 2)  NULL,
        [Rebate Tier 2 Percentage]      DECIMAL(9, 4)   NULL,
        [Service Level Target Pct]      DECIMAL(9, 4)   NULL,
        [Penalty Clause Code]           NVARCHAR(20)    NULL,
        [Penalty Rate]                  DECIMAL(9, 4)   NULL,

        [EU Statutory Interest Applies] BIT             NULL,
        [EU Termination Notice Days]    INT             NULL,
        [APAC Local Content Percentage] DECIMAL(9, 4)   NULL,
        [APAC Stamp Duty Amount]        DECIMAL(18, 2)  NULL,
        [NA Liquidated Damages Cap]     DECIMAL(18, 2)  NULL,

        [Signed On]                     DATE            NULL,
        [Signed By Employee Number]     NVARCHAR(20)    NULL,
        [Amendment Reason Code]         NVARCHAR(20)    NULL,   -- PRICE / TERM / SCOPE / SLA / ADMIN
        [Document Reference]            NVARCHAR(200)   NULL,

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
        CONSTRAINT [PK_Dimension_Vendor_Contract] PRIMARY KEY CLUSTERED ([Vendor Contract Key] ASC),
        CONSTRAINT [CK_Dimension_Vendor_Contract_Price_Protection]
            CHECK ([Price Protection Code] IS NULL
                   OR [Price Protection Code] IN (N'FIRM', N'INDEX', N'COLLAR', N'NONE'))
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Vendor_Contract_Current]
        ON [Dimension].[Vendor Contract] ([Contract Number] ASC, [Is Current Row] ASC)
        INCLUDE ([Vendor Contract Key], [Amendment Number], [Supplier Key]);
END;
GO
