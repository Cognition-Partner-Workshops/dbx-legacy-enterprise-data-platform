/*
    Fact.Salesperson Territory Coverage

    Object        : [Fact].[Salesperson Territory Coverage] - factless fact
                    table recording which salesperson covered which territory
                    and customer segment over which period, and in what role.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Salesperson, Dimension.Sales Territory,
                    Dimension.Customer Segment (WP05).
    Called by     : loaded by Integration.usp_LoadFactDailySalesSnapshot from
                    the territory assignment extract.
    Grain         : salesperson x territory x customer segment x assignment
                    period.

    Factless: no measures. It answers coverage questions - which territories
    had no primary owner in a period, which salespeople were double-hatted, and
    which customer segments were uncovered when a quota was missed. Overlapping
    assignments are legal (a maternity cover and the returning owner both hold
    the territory for a fortnight) so the table is not uniquely keyed on
    territory and date.
*/
CREATE TABLE [Fact].[Salesperson Territory Coverage] (
    [Coverage Key]                  BIGINT          IDENTITY (1, 1) NOT NULL,
    [Assignment Start Date Key]     DATE            NOT NULL,
    [Assignment End Date Key]       DATE            NULL,
    [Salesperson Key]               INT             NOT NULL,
    [Sales Territory Key]           INT             NOT NULL,
    [Customer Segment Key]          INT             NULL,
    [Sales Channel Key]             INT             NULL,
    [Manager Employee Key]          INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Assignment Reference]          NVARCHAR (20)   NOT NULL,
    [Coverage Role Code]            NVARCHAR (6)    NOT NULL,
    [Coverage Type Code]            NVARCHAR (6)    NULL,
    [Assignment Reason Code]        NVARCHAR (6)    NULL,
    [Primary Owner Flag]            BIT             NOT NULL,
    [Interim Cover Flag]            BIT             NULL,
    [Overlapping Assignment Flag]   BIT             NULL,
    [Assignment Days]               INT             NULL,
    [Quota Share Percent]           DECIMAL (9, 4)  NULL,
    [Commission Share Percent]      DECIMAL (9, 4)  NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Salesperson_Territory_Coverage] PRIMARY KEY NONCLUSTERED ([Coverage Key] ASC, [Assignment Start Date Key] ASC) ON [PS_Date] ([Assignment Start Date Key]),
    CONSTRAINT [FK_Fact_Salesperson_Territory_Coverage_Assignment_Start_Date_Key_Dimension_Date] FOREIGN KEY ([Assignment Start Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Salesperson_Territory_Coverage_Salesperson_Key_Dimension_Salesperson] FOREIGN KEY ([Salesperson Key]) REFERENCES [Dimension].[Salesperson] ([Salesperson Key]),
    CONSTRAINT [FK_Fact_Salesperson_Territory_Coverage_Sales_Territory_Key_Dimension_Sales Territory] FOREIGN KEY ([Sales Territory Key]) REFERENCES [Dimension].[Sales Territory] ([Sales Territory Key])
)
ON [PS_Date] ([Assignment Start Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Salesperson_Territory_Coverage_Territory_Period]
    ON [Fact].[Salesperson Territory Coverage] ([Sales Territory Key] ASC, [Assignment Start Date Key] ASC, [Assignment End Date Key] ASC)
    INCLUDE ([Salesperson Key], [Primary Owner Flag], [Quota Share Percent])
    ON [PS_Date] ([Assignment Start Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Factless fact: salesperson to territory coverage assignments',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Salesperson Territory Coverage';
GO
