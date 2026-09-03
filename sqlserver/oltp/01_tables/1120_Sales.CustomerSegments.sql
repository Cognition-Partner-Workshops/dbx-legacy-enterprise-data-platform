/*
    Sales.CustomerSegments

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1120
    Depends on    : Application.People
    Called by     : Sales.CustomerSegmentAssignments, Sales.usp_AssignCustomerSegments,
                    Sales.vw_CustomerSegmentCurrent

    Marketing segments. The rule that puts a customer in a segment is held as
    free text in SegmentRuleText and is re-implemented (badly, and separately)
    inside Sales.usp_AssignCustomerSegments; the text is documentation, not
    executable.

    Regional divergence in consent and retention:
      * NA  - opt-out marketing, 7 year retention, no explicit consent record
              required to segment a customer.
      * EU  - opt-in only, 24 month retention on behavioural attributes, and a
              segment may not be assigned without ConsentRequired satisfied.
      * APAC - opt-in for Australia and Japan, opt-out elsewhere, retention set
              per country by RetentionMonths on the assignment row.
*/
CREATE TABLE [Sales].[CustomerSegments] (
    [CustomerSegmentID]     INT             IDENTITY (1, 1) NOT NULL,
    [SegmentCode]           NVARCHAR (12)   NOT NULL,
    [SegmentName]           NVARCHAR (60)   NOT NULL,
    [SegmentFamily]         NVARCHAR (20)   NOT NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [SegmentRuleText]       NVARCHAR (600)  NULL,
    [ConsentRequired]       BIT             CONSTRAINT [DF_Sales_CustomerSegments_ConsentRequired] DEFAULT (0) NOT NULL,
    [ConsentBasisCode]      NVARCHAR (16)   NULL,
    [RetentionMonths]       SMALLINT        NOT NULL,
    [PriorityOrder]         SMALLINT        CONSTRAINT [DF_Sales_CustomerSegments_PriorityOrder] DEFAULT (100) NOT NULL,
    [IsExclusive]           BIT             CONSTRAINT [DF_Sales_CustomerSegments_IsExclusive] DEFAULT (0) NOT NULL,
    [IsActive]              BIT             CONSTRAINT [DF_Sales_CustomerSegments_IsActive] DEFAULT (1) NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_CustomerSegments_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_CustomerSegments] PRIMARY KEY CLUSTERED ([CustomerSegmentID] ASC),
    CONSTRAINT [UQ_Sales_CustomerSegments_Code] UNIQUE ([SegmentCode], [RegionCode]),
    CONSTRAINT [CK_Sales_CustomerSegments_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [CK_Sales_CustomerSegments_Family] CHECK ([SegmentFamily] IN (N'VALUE', N'BEHAVIOUR', N'LIFECYCLE', N'RISK', N'CHANNEL')),
    CONSTRAINT [CK_Sales_CustomerSegments_Consent] CHECK ([ConsentRequired] = 0 OR [ConsentBasisCode] IS NOT NULL),
    CONSTRAINT [CK_Sales_CustomerSegments_Retention] CHECK ([RetentionMonths] BETWEEN 1 AND 120),
    CONSTRAINT [FK_Sales_CustomerSegments_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
