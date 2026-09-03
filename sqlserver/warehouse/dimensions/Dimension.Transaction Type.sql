/*
    Object        : [Dimension].[Transaction Type]  (SCD Type 1)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.TransactionTypeKey (WideWorldImportersDW baseline)
    Called by     : Integration.usp_MigrateStagedTransactionTypeData

    The Microsoft sample dimension extended with the posting semantics the finance
    facts need: which sign the amount carries, whether the type affects the ledger,
    the customer balance, or both, and which GL account pair it posts to. Several
    of the codes are cryptic 1990s artefacts (`CINV`, `CCRN`, `SPMT`, `WOFF`) and
    are kept verbatim because the AP and AR extracts emit them.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Transaction Type', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Transaction Type]
    (
        [Transaction Type Key]          INT             CONSTRAINT [DF_Dimension_Transaction_Type_Transaction_Type_Key] DEFAULT (NEXT VALUE FOR [Sequences].[TransactionTypeKey]) NOT NULL,
        [WWI Transaction Type ID]       INT             NOT NULL,
        [Transaction Type]              NVARCHAR(50)    NOT NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        CONSTRAINT [PK_Dimension_Transaction_Type] PRIMARY KEY CLUSTERED ([Transaction Type Key] ASC)
    );
END;
GO

IF COL_LENGTH(N'Dimension.Transaction Type', N'Transaction Type Code') IS NULL
    ALTER TABLE [Dimension].[Transaction Type] ADD
        [Transaction Type Code]         NVARCHAR(10)    NULL,   -- CINV / CCRN / CPMT / SINV / SCRN / SPMT / WOFF / ADJ
        [Transaction Category Code]     NVARCHAR(10)    NULL,   -- AR / AP / GL / INV / STAT
        [Ledger Impact Code]            NVARCHAR(10)    NULL,   -- LEDGER / SUBLEDGER / BOTH / NONE
        [Amount Sign]                   SMALLINT        NULL,   -- +1 or -1; applied by the fact loads
        [Affects Customer Balance]      BIT             NULL,
        [Affects Supplier Balance]      BIT             NULL,
        [Affects Inventory Value]       BIT             NULL,
        [Is Reversal Type]              BIT             NULL,
        [Reversal Of Type Code]         NVARCHAR(10)    NULL,
        [Debit GL Account Code]         NVARCHAR(20)    NULL,
        [Credit GL Account Code]        NVARCHAR(20)    NULL,
        [Tax Point Rule Code]           NVARCHAR(15)    NULL,   -- INVOICEDATE (NA) / SUPPLYDATE (EU) / PAYMENTDATE (APAC cash-basis)
        [Requires Tax Analysis]         BIT             NULL,
        [Region Code]                   NVARCHAR(10)    NULL,
        [Legacy Source Code]            NVARCHAR(4)     NULL,   -- as emitted by the 1997 ledger
        [Is Active]                     BIT             NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Last Load Batch Id]            BIGINT          NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'CK_Dimension_Transaction_Type_Sign')
    ALTER TABLE [Dimension].[Transaction Type]
        ADD CONSTRAINT [CK_Dimension_Transaction_Type_Sign]
        CHECK ([Amount Sign] IS NULL OR [Amount Sign] IN (-1, 1));
GO
