/*
    Object        : [Dimension].[Return Reason]  (SCD Type 1)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Region.sql
    Depends on    : Sequences.ReturnReasonKey
    Called by     : Integration.usp_MigrateStagedReturnReasonData

    Three code sets are unioned here and the load procedure maps each to a common
    [Reason Group Code] so the returns-rate reporting can aggregate across regions:

      NA    : the 1999 RMA codes, two digits, agent-selected, notoriously
              over-using '99 - Other'.
      EU    : statutory withdrawal codes required by the consumer-rights
              directive, where a 14-day no-reason withdrawal is its own reason
              and cannot be refused; these drive the refund obligation.
      APAC  : marketplace-supplied reason codes, one set per marketplace, arriving
              as free text in some feeds - the load bands unmapped text into
              'UNMAPPED' and routes the row to the reject table for review.

    Whether the return is chargeable to the supplier, restockable, or scrapped is
    an attribute of the reason and drives the returns fact's cost allocation.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Return Reason', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Return Reason]
    (
        [Return Reason Key]             INT             CONSTRAINT [DF_Dimension_Return_Reason_Key] DEFAULT (NEXT VALUE FOR [Sequences].[ReturnReasonKey]) NOT NULL,
        [WWI Return Reason ID]          INT             NULL,
        [Return Reason Code]            NVARCHAR(15)    NOT NULL,
        [Return Reason]                 NVARCHAR(100)   NOT NULL,
        [Reason Group Code]             NVARCHAR(15)    NULL,   -- DAMAGE / QUALITY / WRONGITEM / LATE / CHANGEDMIND / FRAUD / UNMAPPED
        [Reason Source Set]             NVARCHAR(15)    NULL,   -- NA_RMA / EU_STATUTORY / APAC_MKTPL
        [Region Code]                   NVARCHAR(10)    NULL,
        [Marketplace Code]              NVARCHAR(20)    NULL,
        [Source Reason Text]            NVARCHAR(200)   NULL,   -- retained verbatim for the unmapped cases

        [Is Customer Fault]             BIT             NULL,
        [Is Supplier Chargeable]        BIT             NULL,
        [Is Carrier Chargeable]         BIT             NULL,
        [Is Quality Defect]             BIT             NULL,
        [Is Statutory Withdrawal]       BIT             NULL,   -- EU 14-day right, refund is mandatory
        [Refund Obligation Code]        NVARCHAR(10)    NULL,   -- FULL / PARTIAL / CREDIT / NONE
        [Restocking Fee Percentage]     DECIMAL(9, 4)   NULL,
        [Disposition Code]              NVARCHAR(10)    NULL,   -- RESTOCK / REWORK / SCRAP / RTV / DONATE
        [Requires Inspection]           BIT             NULL,
        [Requires Photo Evidence]       BIT             NULL,
        [Quality Notification Required] BIT             NULL,
        [Return Window Days]            SMALLINT        NULL,
        [Cost Allocation Code]          NVARCHAR(15)    NULL,   -- COGS / SUPPLIER / CARRIER / MARKETING
        [Is Active]                     BIT             NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Return_Reason] PRIMARY KEY CLUSTERED ([Return Reason Key] ASC),
        CONSTRAINT [UQ_Dimension_Return_Reason_Code] UNIQUE ([Return Reason Code], [Reason Source Set])
    );
END;
GO
