/*
    Sales.CommissionPlans

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1100
    Depends on    : Application.People
    Called by     : Sales.ufn_CommissionRate, Sales.usp_RecalculateCommissionAccruals

    Commission plans are banded, and the bands are held as three pairs of
    columns rather than a child table because the 1990s payroll interface
    expected a fixed-width record with exactly three bands. Anyone needing a
    fourth band creates a second plan and alternates the assignment, which is
    why plan codes such as 'NA-ENT-2019-B' exist.

    Regional divergence: NA plans pay on invoiced margin, EU plans pay on net
    revenue excluding VAT, APAC plans pay on collected cash (so an accrual
    stays 'PENDINGCASH' until the payment allocation lands).
*/
CREATE TABLE [Sales].[CommissionPlans] (
    [CommissionPlanID]      INT             IDENTITY (1, 1) NOT NULL,
    [PlanCode]              NVARCHAR (20)   NOT NULL,
    [PlanName]              NVARCHAR (80)   NOT NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [CommissionBasis]       NVARCHAR (16)   NOT NULL,
    [Band1UpperPercent]     DECIMAL (6, 2)  NOT NULL,
    [Band1RatePercent]      DECIMAL (5, 2)  NOT NULL,
    [Band2UpperPercent]     DECIMAL (6, 2)  NULL,
    [Band2RatePercent]      DECIMAL (5, 2)  NULL,
    [Band3UpperPercent]     DECIMAL (6, 2)  NULL,
    [Band3RatePercent]      DECIMAL (5, 2)  NULL,
    [AcceleratorPercent]    DECIMAL (5, 2)  NULL,
    [ClawbackWindowDays]    SMALLINT        CONSTRAINT [DF_Sales_CommissionPlans_ClawbackWindowDays] DEFAULT (0) NOT NULL,
    [MinimumMarginPercent]  DECIMAL (5, 2)  NULL,
    [EffectiveFromDate]     DATE            NOT NULL,
    [EffectiveToDate]       DATE            NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_CommissionPlans_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_CommissionPlans] PRIMARY KEY CLUSTERED ([CommissionPlanID] ASC),
    CONSTRAINT [UQ_Sales_CommissionPlans_Code] UNIQUE ([PlanCode]),
    CONSTRAINT [CK_Sales_CommissionPlans_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [CK_Sales_CommissionPlans_Basis] CHECK ([CommissionBasis] IN (N'INVOICEDMARGIN', N'NETREVENUE', N'COLLECTEDCASH')),
    CONSTRAINT [CK_Sales_CommissionPlans_Bands] CHECK ([Band2UpperPercent] IS NULL OR [Band2UpperPercent] > [Band1UpperPercent]),
    CONSTRAINT [CK_Sales_CommissionPlans_Band3] CHECK ([Band3UpperPercent] IS NULL OR [Band3UpperPercent] > [Band2UpperPercent]),
    CONSTRAINT [FK_Sales_CommissionPlans_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
