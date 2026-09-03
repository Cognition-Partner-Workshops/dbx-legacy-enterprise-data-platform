/*
    Object        : Role-playing views over [Dimension].[Date] and [Dimension].[Time]
                    [Dimension].[vw_OrderDate], [Dimension].[vw_InvoiceDate],
                    [Dimension].[vw_DueDate], [Dimension].[vw_ShipDate],
                    [Dimension].[vw_DeliveryDate], [Dimension].[vw_PaymentDate],
                    [Dimension].[vw_ReceivedDate], [Dimension].[vw_OrderTime],
                    [Dimension].[vw_ShipTime]
    Deploy target : WideWorldImportersDW
    Deploy order  : 91 (after Dimension.Date.sql, Dimension.Time.sql and 90_unknown_members.sql)
    Depends on    : Dimension.Date, Dimension.Time
    Called by     : the fact tables in sqlserver/warehouse/facts join to these views;
                    the SSAS model binds one dimension per view

    One physical date dimension, nine roles. Each view aliases the columns with the
    role prefix so the cube shows "Order Fiscal Period" rather than nine
    identically-named attributes, and so a report can put order date and ship date
    on the same axis.

    The views expose the regional fiscal columns unprefixed within the role - a
    report picks the region's column set explicitly. That was a deliberate 2011
    decision: an attempt to expose a single "Fiscal Period" that switched on the
    user's region produced numbers nobody could reconcile.

    Roles that can legitimately have no date (an unshipped order has no ship date)
    point at DateKey -2, Not Applicable; roles where the date failed to resolve
    point at -1, Unknown. The views filter neither out - the facts must be able to
    join to them.
*/
SET NOCOUNT ON;
GO

CREATE VIEW [Dimension].[vw_OrderDate]
AS
SELECT
      [DateKey]                         AS [Order Date Key]
    , [Date]                            AS [Order Date]
    , [Day]                             AS [Order Day]
    , [Day of Week]                     AS [Order Day of Week]
    , [Month]                           AS [Order Month]
    , [Short Month]                     AS [Order Short Month]
    , [Calendar Month Number]           AS [Order Calendar Month Number]
    , [Calendar Quarter Number]         AS [Order Calendar Quarter Number]
    , [Calendar Year]                   AS [Order Calendar Year]
    , [NA Fiscal Year Label]            AS [Order NA Fiscal Year]
    , [NA Fiscal Period Label]          AS [Order NA Fiscal Period]
    , [NA Fiscal Week Label]            AS [Order NA Fiscal Week]
    , [EU Fiscal Period Label]          AS [Order EU Fiscal Period]
    , [EU ISO Week Label]               AS [Order EU ISO Week]
    , [APAC Fiscal Year Label]          AS [Order APAC Fiscal Year]
    , [APAC Fiscal Period Label]        AS [Order APAC Fiscal Period]
    , [NA Is Trading Day]               AS [Order NA Is Trading Day]
    , [EU Is Trading Day]               AS [Order EU Is Trading Day]
    , [APAC Is Trading Day]             AS [Order APAC Is Trading Day]
    , [Is Reserved Member]              AS [Order Date Is Reserved Member]
FROM [Dimension].[Date];
GO

CREATE VIEW [Dimension].[vw_InvoiceDate]
AS
SELECT
      [DateKey]                         AS [Invoice Date Key]
    , [Date]                            AS [Invoice Date]
    , [Day]                             AS [Invoice Day]
    , [Month]                           AS [Invoice Month]
    , [Calendar Month Number]           AS [Invoice Calendar Month Number]
    , [Calendar Quarter Number]         AS [Invoice Calendar Quarter Number]
    , [Calendar Year]                   AS [Invoice Calendar Year]
    , [NA Fiscal Period Label]          AS [Invoice NA Fiscal Period]
    , [NA Is Period End]                AS [Invoice NA Is Period End]
    , [EU Fiscal Period Label]          AS [Invoice EU Fiscal Period]
    , [EU VAT Return Period Label]      AS [Invoice EU VAT Return Period]
    , [APAC Fiscal Period Label]        AS [Invoice APAC Fiscal Period]
    , [APAC GST Return Period Label]    AS [Invoice APAC GST Return Period]
    , [Is Month End Close Day]          AS [Invoice Is Month End Close Day]
    , [ETL Business Day Number]         AS [Invoice Business Day Number]
    , [Is Reserved Member]              AS [Invoice Date Is Reserved Member]
FROM [Dimension].[Date];
GO

CREATE VIEW [Dimension].[vw_DueDate]
AS
SELECT
      [DateKey]                         AS [Due Date Key]
    , [Date]                            AS [Due Date]
    , [Day of Week]                     AS [Due Day of Week]
    , [Calendar Month Number]           AS [Due Calendar Month Number]
    , [Calendar Year]                   AS [Due Calendar Year]
    , [NA Fiscal Period Label]          AS [Due NA Fiscal Period]
    , [EU Fiscal Period Label]          AS [Due EU Fiscal Period]
    , [APAC Fiscal Period Label]        AS [Due APAC Fiscal Period]
    , [NA Is Trading Day]               AS [Due NA Is Trading Day]
    , [EU Is Trading Day]               AS [Due EU Is Trading Day]
    , [APAC Is Trading Day]             AS [Due APAC Is Trading Day]
    , [Is Reserved Member]              AS [Due Date Is Reserved Member]
FROM [Dimension].[Date];
GO

CREATE VIEW [Dimension].[vw_ShipDate]
AS
SELECT
      [DateKey]                         AS [Ship Date Key]
    , [Date]                            AS [Ship Date]
    , [Day of Week]                     AS [Ship Day of Week]
    , [Calendar Month Number]           AS [Ship Calendar Month Number]
    , [Calendar Year]                   AS [Ship Calendar Year]
    , [NA Fiscal Week Label]            AS [Ship NA Fiscal Week]
    , [EU ISO Week Label]               AS [Ship EU ISO Week]
    , [APAC Fiscal Period Label]        AS [Ship APAC Fiscal Period]
    , [NA Is Trading Day]               AS [Ship NA Is Trading Day]
    , [EU Is Trading Day]               AS [Ship EU Is Trading Day]
    , [APAC Is Trading Day]             AS [Ship APAC Is Trading Day]
    , [Is Weekend NA]                   AS [Ship Is Weekend NA]
    , [Is Reserved Member]              AS [Ship Date Is Reserved Member]
FROM [Dimension].[Date];
GO

CREATE VIEW [Dimension].[vw_DeliveryDate]
AS
SELECT
      [DateKey]                         AS [Delivery Date Key]
    , [Date]                            AS [Delivery Date]
    , [Day of Week]                     AS [Delivery Day of Week]
    , [Calendar Month Number]           AS [Delivery Calendar Month Number]
    , [Calendar Year]                   AS [Delivery Calendar Year]
    , [NA Fiscal Week Label]            AS [Delivery NA Fiscal Week]
    , [EU ISO Week Label]               AS [Delivery EU ISO Week]
    , [APAC Fiscal Period Label]        AS [Delivery APAC Fiscal Period]
    , [NA Is Trading Day]               AS [Delivery NA Is Trading Day]
    , [EU Is Trading Day]               AS [Delivery EU Is Trading Day]
    , [APAC Is Trading Day]             AS [Delivery APAC Is Trading Day]
    , [Is Reserved Member]              AS [Delivery Date Is Reserved Member]
FROM [Dimension].[Date];
GO

CREATE VIEW [Dimension].[vw_PaymentDate]
AS
SELECT
      [DateKey]                         AS [Payment Date Key]
    , [Date]                            AS [Payment Date]
    , [Calendar Month Number]           AS [Payment Calendar Month Number]
    , [Calendar Quarter Number]         AS [Payment Calendar Quarter Number]
    , [Calendar Year]                   AS [Payment Calendar Year]
    , [NA Fiscal Period Label]          AS [Payment NA Fiscal Period]
    , [EU Fiscal Period Label]          AS [Payment EU Fiscal Period]
    , [APAC Fiscal Period Label]        AS [Payment APAC Fiscal Period]
    , [Is Month End Close Day]          AS [Payment Is Month End Close Day]
    , [ETL Business Day Number]         AS [Payment Business Day Number]
    , [Is Reserved Member]              AS [Payment Date Is Reserved Member]
FROM [Dimension].[Date];
GO

CREATE VIEW [Dimension].[vw_ReceivedDate]
AS
SELECT
      [DateKey]                         AS [Received Date Key]
    , [Date]                            AS [Received Date]
    , [Day of Week]                     AS [Received Day of Week]
    , [Calendar Month Number]           AS [Received Calendar Month Number]
    , [Calendar Year]                   AS [Received Calendar Year]
    , [NA Fiscal Period Label]          AS [Received NA Fiscal Period]
    , [EU Fiscal Period Label]          AS [Received EU Fiscal Period]
    , [APAC Fiscal Period Label]        AS [Received APAC Fiscal Period]
    , [NA Is Trading Day]               AS [Received NA Is Trading Day]
    , [Is Reserved Member]              AS [Received Date Is Reserved Member]
FROM [Dimension].[Date];
GO

CREATE VIEW [Dimension].[vw_OrderTime]
AS
SELECT
      [Time Key]                        AS [Order Time Key]
    , [Time Label 24]                   AS [Order Time]
    , [Hour 24]                         AS [Order Hour]
    , [Hour Label]                      AS [Order Hour Label]
    , [Half Hour Label]                 AS [Order Half Hour]
    , [Daypart Code]                    AS [Order Daypart]
    , [Is Order Cutoff Minute]          AS [Order Is Cutoff Minute]
    , [NA Is Core Business Hour]        AS [Order NA Is Core Business Hour]
    , [EU Is Core Business Hour]        AS [Order EU Is Core Business Hour]
    , [APAC Is Core Business Hour]      AS [Order APAC Is Core Business Hour]
FROM [Dimension].[Time];
GO

CREATE VIEW [Dimension].[vw_ShipTime]
AS
SELECT
      [Time Key]                        AS [Ship Time Key]
    , [Time Label 24]                   AS [Ship Time]
    , [Hour 24]                         AS [Ship Hour]
    , [Hour Label]                      AS [Ship Hour Label]
    , [Daypart Code]                    AS [Ship Daypart]
    , [NA Shift Code]                   AS [Ship NA Shift]
    , [EU Shift Code]                   AS [Ship EU Shift]
    , [APAC Shift Code]                 AS [Ship APAC Shift]
    , [Is Batch Window]                 AS [Ship Is Batch Window]
FROM [Dimension].[Time];
GO
