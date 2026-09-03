/*
    Object        : [Dimension].[Payment Method]  (SCD Type 1)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.PaymentMethodKey (WideWorldImportersDW baseline)
    Called by     : Integration.usp_MigrateStagedPaymentMethodData

    The Microsoft sample dimension extended with the settlement attributes the
    payment fact needs. Payment instruments diverge sharply by region and the
    estate never normalised them, so the reference set is unioned rather than
    mapped: SEPA direct debit, ACH, BPAY, PayNow, UPI and cheque all coexist and
    the [Instrument Family Code] is the only column that groups them.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Payment Method', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Payment Method]
    (
        [Payment Method Key]            INT             CONSTRAINT [DF_Dimension_Payment_Method_Payment_Method_Key] DEFAULT (NEXT VALUE FOR [Sequences].[PaymentMethodKey]) NOT NULL,
        [WWI Payment Method ID]         INT             NOT NULL,
        [Payment Method]                NVARCHAR(50)    NOT NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        CONSTRAINT [PK_Dimension_Payment_Method] PRIMARY KEY CLUSTERED ([Payment Method Key] ASC)
    );
END;
GO

IF COL_LENGTH(N'Dimension.Payment Method', N'Payment Method Code') IS NULL
    ALTER TABLE [Dimension].[Payment Method] ADD
        [Payment Method Code]           NVARCHAR(15)    NULL,
        [Instrument Family Code]        NVARCHAR(10)    NULL,   -- CARD / BANK / CASH / CHEQUE / WALLET / CREDIT
        [Region Code]                   NVARCHAR(10)    NULL,   -- GLOBAL where accepted everywhere
        [Settlement Currency Code]      NVARCHAR(3)     NULL,
        [Clearing Scheme Code]          NVARCHAR(20)    NULL,   -- ACH / SEPA / BACS / NPP / FAST / UPI / SWIFT
        [Settlement Days]               SMALLINT        NULL,
        [Is Prepayment]                 BIT             NULL,
        [Is Card Present Capable]       BIT             NULL,
        [Is Online Capable]             BIT             NULL,
        [Is Refundable To Source]       BIT             NULL,
        [Chargeback Window Days]        SMALLINT        NULL,
        [Merchant Fee Fixed Amount]     DECIMAL(18, 4)  NULL,
        [Merchant Fee Percentage]       DECIMAL(9, 4)   NULL,
        [Requires Mandate]              BIT             NULL,   -- SEPA direct debit
        [Mandate Reference Format]      NVARCHAR(40)    NULL,
        [Requires Strong Authentication] BIT            NULL,   -- EU PSD2 SCA
        [NA Card Network Code]          NVARCHAR(10)    NULL,
        [APAC Local Scheme Name]        NVARCHAR(60)    NULL,
        [GL Clearing Account Code]      NVARCHAR(20)    NULL,
        [Is Active]                     BIT             NULL,
        [Retired On]                    DATE            NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Last Load Batch Id]            BIGINT          NULL;
GO
