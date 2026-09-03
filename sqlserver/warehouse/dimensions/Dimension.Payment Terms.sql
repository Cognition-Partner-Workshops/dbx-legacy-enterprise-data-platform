/*
    Object        : [Dimension].[Payment Terms]  (SCD Type 1)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.PaymentTermsKey
    Called by     : Integration.usp_MigrateStagedPaymentTermsData

    Sourced from Oracle WWI_FIN.PAYMENT_TERMS. The due-date rule is the interesting
    part and it is not expressible as a single number, so the dimension carries the
    rule components and the AR/AP loads evaluate them:

        NET      : due = invoice date + [Net Days]
        EOM      : due = end of invoice month + [Net Days]
        PROX     : due on [Proximo Day] of the month [Proximo Month Offset] later
        INSTAL   : [Instalment Count] equal instalments [Instalment Interval Days] apart

    Discount terms are separate again (2/10 net 30 is [Discount Percentage] = 2,
    [Discount Days] = 10, [Net Days] = 30) and only NA uses them in practice; the
    EU ledgers were forbidden from offering settlement discounts in 2011, and APAC
    negotiates them per contract rather than per term code.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Payment Terms', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Payment Terms]
    (
        [Payment Terms Key]             INT             CONSTRAINT [DF_Dimension_Payment_Terms_Key] DEFAULT (NEXT VALUE FOR [Sequences].[PaymentTermsKey]) NOT NULL,
        [Payment Terms Code]            NVARCHAR(10)    NOT NULL,
        [Payment Terms]                 NVARCHAR(80)    NOT NULL,
        [Due Date Rule Code]            NVARCHAR(10)    NOT NULL,   -- NET / EOM / PROX / INSTAL / IMMED
        [Net Days]                      SMALLINT        NULL,
        [Proximo Day]                   SMALLINT        NULL,
        [Proximo Month Offset]          SMALLINT        NULL,
        [Instalment Count]              SMALLINT        NULL,
        [Instalment Interval Days]      SMALLINT        NULL,
        [Discount Percentage]           DECIMAL(9, 4)   NULL,
        [Discount Days]                 SMALLINT        NULL,
        [Grace Days]                    SMALLINT        NULL,

        [Applies To Code]               NVARCHAR(10)    NULL,   -- AR / AP / BOTH
        [Region Code]                   NVARCHAR(10)    NULL,
        [Is Statutory Maximum]          BIT             NULL,   -- EU late-payment directive ceiling
        [Statutory Maximum Days]        SMALLINT        NULL,
        [Late Interest Basis Code]      NVARCHAR(15)    NULL,   -- NONE / FIXED / REFPLUS8 (EU) / STATE (NA)
        [Late Interest Rate]            DECIMAL(9, 4)   NULL,
        [Withholding Applies]           BIT             NULL,   -- several APAC jurisdictions
        [Legacy Terms Code]             NVARCHAR(4)     NULL,   -- the 1998 AP system's code, still keyed by clerks

        [Is Active]                     BIT             NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Payment_Terms] PRIMARY KEY CLUSTERED ([Payment Terms Key] ASC),
        CONSTRAINT [UQ_Dimension_Payment_Terms_Code] UNIQUE ([Payment Terms Code]),
        CONSTRAINT [CK_Dimension_Payment_Terms_Rule]
            CHECK ([Due Date Rule Code] IN (N'NET', N'EOM', N'PROX', N'INSTAL', N'IMMED'))
    );
END;
GO
