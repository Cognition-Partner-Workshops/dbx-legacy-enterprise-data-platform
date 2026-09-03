/*
    Returns.ReturnReasons

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1400 - after 00_schemas
    Depends on    : Application.People
    Called by     : Returns.ReturnLines, Returns.ufn_RestockingFee

    Reason-code master for returns. Three code sets coexist because the NA
    call centre, the EU webshop and the APAC distributors each brought their
    own; RegionCode partitions them and the same customer-facing meaning has a
    different code in each. Restocking behaviour is per reason and per region:
    EU distance-selling returns inside the cooling-off period must be free,
    which is why RestockingPercent is nullable and DefaultRestockingApplies
    exists separately.
*/
CREATE TABLE [Returns].[ReturnReasons] (
    [ReturnReasonID]        INT             IDENTITY (1, 1) NOT NULL,
    [ReasonCode]            NVARCHAR (10)   NOT NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [ReasonDescription]     NVARCHAR (120)  NOT NULL,
    [ReasonCategory]        NVARCHAR (16)   NOT NULL,
    [IsCustomerFault]       BIT             CONSTRAINT [DF_Returns_ReturnReasons_IsCustomerFault] DEFAULT (0) NOT NULL,
    [DefaultRestockingApplies] BIT          CONSTRAINT [DF_Returns_ReturnReasons_DefaultRestockingApplies] DEFAULT (0) NOT NULL,
    [RestockingPercent]     DECIMAL (5, 2)  NULL,
    [RequiresInspection]    BIT             CONSTRAINT [DF_Returns_ReturnReasons_RequiresInspection] DEFAULT (1) NOT NULL,
    [RequiresPhotoEvidence] BIT             CONSTRAINT [DF_Returns_ReturnReasons_RequiresPhotoEvidence] DEFAULT (0) NOT NULL,
    [AllowsResale]          BIT             CONSTRAINT [DF_Returns_ReturnReasons_AllowsResale] DEFAULT (1) NOT NULL,
    [ReturnWindowDays]      SMALLINT        NULL,
    [SupplierRecoverable]   BIT             CONSTRAINT [DF_Returns_ReturnReasons_SupplierRecoverable] DEFAULT (0) NOT NULL,
    [IsActive]              BIT             CONSTRAINT [DF_Returns_ReturnReasons_IsActive] DEFAULT (1) NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Returns_ReturnReasons_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Returns_ReturnReasons] PRIMARY KEY CLUSTERED ([ReturnReasonID] ASC),
    CONSTRAINT [UQ_Returns_ReturnReasons_Code] UNIQUE ([RegionCode], [ReasonCode]),
    CONSTRAINT [CK_Returns_ReturnReasons_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [CK_Returns_ReturnReasons_Category] CHECK ([ReasonCategory] IN (N'QUALITY', N'DAMAGE', N'PICKERROR', N'CHANGEOFMIND', N'LATE', N'RECALL', N'OTHER')),
    CONSTRAINT [CK_Returns_ReturnReasons_Restocking] CHECK ([RestockingPercent] IS NULL OR ([RestockingPercent] >= 0 AND [RestockingPercent] <= 50)),
    CONSTRAINT [FK_Returns_ReturnReasons_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
