/*
    Sales.PriceLists

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1030
    Depends on    : Application.People, Sales.BuyingGroups, Sales.CustomerCategories
    Called by     : Sales.PriceListLines, Sales.usp_CalculateOrderDiscounts,
                    Sales.ufn_DiscountPercentForCustomer

    Price list header. Regional divergence is structural, not parametric:

      * NA lists are held tax-exclusive; sales tax is added at invoicing from
        the ship-to jurisdiction.
      * EU lists are held VAT-inclusive at the list's own VAT rate, because the
        catalogue is printed with gross prices. Net is derived on extract.
      * APAC lists are tax-exclusive but carry a GST code per list, and the
        Japanese lists carry consumption tax at a different rounding rule
        (round half down on the line, not the invoice).
*/
CREATE TABLE [Sales].[PriceLists] (
    [PriceListID]           INT             IDENTITY (1, 1) NOT NULL,
    [PriceListCode]         NVARCHAR (20)   NOT NULL,
    [PriceListName]         NVARCHAR (80)   NOT NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [PriceBasis]            NVARCHAR (12)   NOT NULL,
    [TaxTreatment]          NVARCHAR (12)   NOT NULL,
    [TaxRatePercent]        DECIMAL (5, 2)  NULL,
    [RoundingRuleCode]      NVARCHAR (12)   NOT NULL,
    [BuyingGroupID]         INT             NULL,
    [CustomerCategoryID]    INT             NULL,
    [EffectiveFromDate]     DATE            NOT NULL,
    [EffectiveToDate]       DATE            NULL,
    [SupersedesPriceListID] INT             NULL,
    [ApprovalStatus]        NVARCHAR (12)   NOT NULL,
    [ApprovedByPersonID]    INT             NULL,
    [ApprovedWhen]          DATETIME2 (7)   NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_PriceLists_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_PriceLists] PRIMARY KEY CLUSTERED ([PriceListID] ASC),
    CONSTRAINT [UQ_Sales_PriceLists_Code_From] UNIQUE ([PriceListCode], [EffectiveFromDate]),
    CONSTRAINT [CK_Sales_PriceLists_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [CK_Sales_PriceLists_Basis] CHECK ([PriceBasis] IN (N'LIST', N'CONTRACT', N'PROMOTIONAL', N'CLEARANCE')),
    CONSTRAINT [CK_Sales_PriceLists_TaxTreatment] CHECK ([TaxTreatment] IN (N'EXCLUSIVE', N'INCLUSIVE')),
    CONSTRAINT [CK_Sales_PriceLists_TaxRate] CHECK ([TaxTreatment] = N'EXCLUSIVE' OR [TaxRatePercent] IS NOT NULL),
    CONSTRAINT [CK_Sales_PriceLists_Rounding] CHECK ([RoundingRuleCode] IN (N'HALFUP2', N'HALFDOWN2', N'HALFEVEN2', N'UP0')),
    CONSTRAINT [CK_Sales_PriceLists_Approval] CHECK ([ApprovalStatus] IN (N'DRAFT', N'PENDING', N'APPROVED', N'WITHDRAWN')),
    CONSTRAINT [FK_Sales_PriceLists_BuyingGroups] FOREIGN KEY ([BuyingGroupID]) REFERENCES [Sales].[BuyingGroups] ([BuyingGroupID]),
    CONSTRAINT [FK_Sales_PriceLists_CustomerCategories] FOREIGN KEY ([CustomerCategoryID]) REFERENCES [Sales].[CustomerCategories] ([CustomerCategoryID]),
    CONSTRAINT [FK_Sales_PriceLists_Supersedes] FOREIGN KEY ([SupersedesPriceListID]) REFERENCES [Sales].[PriceLists] ([PriceListID]),
    CONSTRAINT [FK_Sales_PriceLists_ApprovedBy] FOREIGN KEY ([ApprovedByPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Sales_PriceLists_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_PriceLists_Current]
    ON [Sales].[PriceLists] ([RegionCode] ASC, [EffectiveFromDate] DESC)
    INCLUDE ([PriceListCode], [CurrencyCode], [TaxTreatment], [TaxRatePercent])
    WHERE [ApprovalStatus] = N'APPROVED';
GO
