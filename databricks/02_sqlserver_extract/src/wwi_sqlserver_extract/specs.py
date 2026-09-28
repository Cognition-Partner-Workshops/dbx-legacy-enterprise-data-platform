"""Package specifications for the 22 EXT_SQL_* extracts.

Generated from ssis/02_sqlserver_extract/generate_sqlserver_extracts.py (the SSIS
generator is the source of truth for the SQL text, column contracts and
derived-column expressions). `?` placeholders in the SQL keep their legacy
position; PackageSpec.sqlParams documents what binds to each of them.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

PROJECT_NAME = "WWI_Extract_SqlServer"
SRC_OLTP = "WWI_OLTP"
SRC_WEB = "WWI_WEB"

NUMERIC_KEY_BOUNDED = "numeric_key_bounded"
NUMERIC_KEY_OPEN = "numeric_key_open"
TIMESTAMP = "timestamp"
DATE_WINDOW = "date_window"
FULL_RELOAD = "full_reload"


@dataclass(frozen=True)
class DeleteDetection:
    """Change-tracking delete pass (second data flow in the legacy package)."""

    name: str
    changeTrackingObject: str
    versionSql: Optional[str]
    sql: str
    columns: Tuple[Tuple[str, str], ...]
    derivations: Tuple[Tuple[str, str, str], ...]


@dataclass(frozen=True)
class PackageSpec:
    name: str
    description: str
    sourceSystemCode: str
    loadPattern: str
    legacyTarget: str
    targetTable: str
    keyColumns: Tuple[str, ...]
    sourceSql: str
    sqlParams: Tuple[str, ...]
    columns: Tuple[Tuple[str, str], ...]
    derivations: Tuple[Tuple[str, str, str], ...]
    transform: str
    errorDisposition: str
    watermarkObject: Optional[str] = None
    maxKeySql: Optional[str] = None
    keyColumn: Optional[str] = None
    windowColumn: Optional[str] = None
    recordKind: Optional[str] = None
    rejectTarget: Optional[str] = None
    splits: Tuple[Tuple[str, str, str], ...] = ()
    clearSql: Optional[str] = None
    deleteDetection: Optional[DeleteDetection] = None
    extraRowCountObjects: Tuple[str, ...] = ()
    extraVariables: Tuple[Tuple[str, object], ...] = ()
    sourceTimeoutSeconds: int = 0
    batchSize: int = 100000

    @property
    def isIncremental(self) -> bool:
        return self.loadPattern != FULL_RELOAD

    @property
    def dataColumns(self) -> Tuple[str, ...]:
        return tuple(c for c, _ in self.columns) + tuple(n for n, _, _ in self.derivations)


PACKAGES: Dict[str, PackageSpec] = {}


def _register(spec: PackageSpec) -> PackageSpec:
    PACKAGES[spec.name] = spec
    return spec


ORDERS = _register(PackageSpec(
    name='EXT_SQL_Orders',
    description='Numeric-key incremental order header extract from Sales.Orders, denormalised with the salesperson, contact person and buying group. A second pass reads SQL Server change tracking for deletes because order cancellation removes the header row rather than flagging it.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_bounded',
    legacyTarget='raw.SqlOrder',
    targetTable='raw_sql_order',
    keyColumns=('OrderID', 'DeleteFlag'),
    sourceSql="""SELECT  o.OrderID,
        o.CustomerID,
        o.SalespersonPersonID,
        o.PickedByPersonID,
        o.ContactPersonID,
        o.BackorderOrderID,
        o.OrderDate,
        o.ExpectedDeliveryDate,
        o.CustomerPurchaseOrderNumber,
        o.IsUndersupplyBackordered,
        o.Comments,
        o.DeliveryInstructions,
        st.SalesTerritoryID,
        ISNULL(sc.SalesChannelCode, N'DIRECT')  AS SalesChannelCode,
        o.PickingCompletedWhen,
        o.LastEditedWhen,
        o.LastEditedBy
FROM    Sales.Orders AS o WITH (NOLOCK)
        LEFT OUTER JOIN Sales.SalesTerritories AS st WITH (NOLOCK)
            ON st.SalesTerritoryID = o.SalesTerritoryID
        LEFT OUTER JOIN Sales.SalesChannels AS sc WITH (NOLOCK)
            ON sc.SalesChannelID = o.SalesChannelID
WHERE   o.OrderID > ?
  AND   o.OrderID <= ?""",
    sqlParams=('watermarkFrom', 'watermarkTo'),
    columns=(
        ('OrderID', 'int'),
        ('CustomerID', 'int'),
        ('SalespersonPersonID', 'int'),
        ('PickedByPersonID', 'int'),
        ('ContactPersonID', 'int'),
        ('BackorderOrderID', 'int'),
        ('OrderDate', 'timestamp'),
        ('ExpectedDeliveryDate', 'timestamp'),
        ('CustomerPurchaseOrderNumber', 'string'),
        ('IsUndersupplyBackordered', 'boolean'),
        ('Comments', 'string'),
        ('DeliveryInstructions', 'string'),
        ('SalesTerritoryID', 'int'),
        ('SalesChannelCode', 'string'),
        ('PickingCompletedWhen', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
        ('LastEditedBy', 'int'),
    ),
    derivations=(
        ('BackorderFlag', 'ISNULL(BackorderOrderID) ? "N" : "Y"', 'string'),
        (
            'PickCycleHours',
            'ISNULL(PickingCompletedWhen) ? -1 : DATEDIFF("hh", OrderDate, PickingCompletedWhen)',
            'int',
        ),
        ('DeleteFlag', '"N"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveOrders',
    errorDisposition='RedirectRow',
    watermarkObject='Sales.Orders',
    maxKeySql='SELECT ISNULL(MAX(OrderID), 0) AS MaxKey FROM Sales.Orders WITH (NOLOCK)',
    keyColumn='OrderID',
    rejectTarget='err.RejectedConstraintViolation',
    deleteDetection=DeleteDetection(
        name='Detect Deleted Orders',
        changeTrackingObject='Sales.Orders',
        versionSql="SELECT ISNULL(LastSyncVersion, 0) AS LastSyncVersion FROM Integration.ChangeTrackingWatermark WITH (NOLOCK) WHERE ObjectName = N'Sales.Orders'",
        sql="""SELECT  ct.OrderID,
        ct.SYS_CHANGE_OPERATION      AS ChangeOperation,
        ct.SYS_CHANGE_VERSION        AS ChangeVersion
FROM    CHANGETABLE(CHANGES Sales.Orders, ?) AS ct
WHERE   ct.SYS_CHANGE_OPERATION = 'D'""",
        columns=(
            ('OrderID', 'int'),
            ('ChangeOperation', 'string'),
            ('ChangeVersion', 'bigint'),
        ),
        derivations=(
            ('SourceSystemCode', '"WWI_OLTP"', 'string'),
            ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
            ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
            ('DeleteFlag', '"Y"', 'string'),
        ),
    ),
    extraVariables=(
        ('ChangeTrackingVersion', 0),
    ),
    sourceTimeoutSeconds=3600,
    batchSize=100000,
))


ORDER_LINES = _register(PackageSpec(
    name='EXT_SQL_OrderLines',
    description='Numeric-key incremental order line extract over Sales.vw_OrderLineExtract, joined to Sales.OrderDiscounts so the applied promotion and discount amount land with the line. Runs with a large commit size because it is the widest extract in the nightly batch.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_bounded',
    legacyTarget='raw.SqlOrderLine',
    targetTable='raw_sql_order_line',
    keyColumns=('OrderLineID',),
    sourceSql="""SELECT  ol.OrderLineID,
        ol.OrderID,
        ol.StockItemID,
        ol.Description,
        ol.PackageTypeName,
        ol.Quantity,
        ol.UnitPrice,
        ol.TaxRate,
        CAST(ol.Quantity * ol.UnitPrice * (1.0 + ol.TaxRate / 100.0) AS decimal(18,2)) AS ExtendedPrice,
        ISNULL(od.DiscountAmount, 0.00)                                                AS LineDiscountAmount,
        od.PromotionCode,
        ol.PickedQuantity,
        ol.PickingCompletedWhen,
        ol.LastEditedWhen
FROM    Sales.vw_OrderLineExtract AS ol WITH (NOLOCK)
        LEFT OUTER JOIN Sales.OrderDiscounts AS od WITH (NOLOCK)
            ON od.OrderLineID = ol.OrderLineID
           AND od.IsVoided = 0
WHERE   ol.OrderLineID > ?
  AND   ol.OrderLineID <= ?""",
    sqlParams=('watermarkFrom', 'watermarkTo'),
    columns=(
        ('OrderLineID', 'int'),
        ('OrderID', 'int'),
        ('StockItemID', 'int'),
        ('Description', 'string'),
        ('PackageTypeName', 'string'),
        ('Quantity', 'int'),
        ('UnitPrice', 'decimal(18,2)'),
        ('TaxRate', 'decimal(18,3)'),
        ('ExtendedPrice', 'decimal(18,2)'),
        ('LineDiscountAmount', 'decimal(18,2)'),
        ('PromotionCode', 'string'),
        ('PickedQuantity', 'int'),
        ('PickingCompletedWhen', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        ('NetLineAmount', 'ExtendedPrice - LineDiscountAmount', 'decimal(18,2)'),
        ('ShortPickFlag', 'PickedQuantity < Quantity ? "Y" : "N"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveOrderLines',
    errorDisposition='RedirectRow',
    watermarkObject='Sales.OrderLines',
    maxKeySql='SELECT ISNULL(MAX(OrderLineID), 0) AS MaxKey FROM Sales.OrderLines WITH (NOLOCK)',
    keyColumn='OrderLineID',
    rejectTarget='err.RejectedConstraintViolation',
    extraVariables=(
        ('DiscountedLineCount', 0),
    ),
    sourceTimeoutSeconds=7200,
    batchSize=250000,
))


INVOICES = _register(PackageSpec(
    name='EXT_SQL_Invoices',
    description='Numeric-key incremental invoice header extract over Sales.vw_InvoiceExtract. The tax treatment is resolved from the delivery geography - sales tax for NA, VAT with the customer registration number for EU, GST for APAC - because the OLTP schema stores one TaxRate column for all three.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_bounded',
    legacyTarget='raw.SqlInvoice',
    targetTable='raw_sql_invoice',
    keyColumns=('InvoiceID',),
    sourceSql="""SELECT  i.InvoiceID,
        i.CustomerID,
        i.BillToCustomerID,
        i.OrderID,
        i.DeliveryMethodID,
        i.ContactPersonID,
        i.SalespersonPersonID,
        i.InvoiceDate,
        i.CustomerPurchaseOrderNumber,
        i.IsCreditNote,
        i.CreditNoteReason,
        i.TotalExcludingTax,
        i.TotalTaxAmount,
        i.TotalIncludingTax,
        i.RegionCode,
        CASE i.RegionCode
            WHEN N'NA'   THEN N'SALESTAX'
            WHEN N'EU'   THEN N'VAT'
            WHEN N'APAC' THEN N'GST'
            ELSE N'NONE'
        END                                     AS TaxTreatmentCode,
        CASE WHEN i.RegionCode = N'EU' THEN i.CustomerTaxRegistrationNumber ELSE NULL END
                                                AS CustomerTaxRegistrationNumber,
        i.ConfirmedDeliveryTime,
        i.LastEditedWhen
FROM    Sales.vw_InvoiceExtract AS i WITH (NOLOCK)
WHERE   i.InvoiceID > ?
  AND   i.InvoiceID <= ?""",
    sqlParams=('watermarkFrom', 'watermarkTo'),
    columns=(
        ('InvoiceID', 'int'),
        ('CustomerID', 'int'),
        ('BillToCustomerID', 'int'),
        ('OrderID', 'int'),
        ('DeliveryMethodID', 'int'),
        ('ContactPersonID', 'int'),
        ('SalespersonPersonID', 'int'),
        ('InvoiceDate', 'timestamp'),
        ('CustomerPurchaseOrderNumber', 'string'),
        ('IsCreditNote', 'boolean'),
        ('CreditNoteReason', 'string'),
        ('TotalExcludingTax', 'decimal(18,2)'),
        ('TotalTaxAmount', 'decimal(18,2)'),
        ('TotalIncludingTax', 'decimal(18,2)'),
        ('RegionCode', 'string'),
        ('TaxTreatmentCode', 'string'),
        ('CustomerTaxRegistrationNumber', 'string'),
        ('ConfirmedDeliveryTime', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        (
            'EffectiveTaxRate',
            'TotalExcludingTax == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(TotalTaxAmount / TotalExcludingTax)',
            'decimal(9,4)',
        ),
        (
            'SignedTotalIncludingTax',
            'IsCreditNote ? -TotalIncludingTax : TotalIncludingTax',
            'decimal(18,2)',
        ),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveInvoices',
    errorDisposition='RedirectRow',
    watermarkObject='Sales.Invoices',
    maxKeySql='SELECT ISNULL(MAX(InvoiceID), 0) AS MaxKey FROM Sales.Invoices WITH (NOLOCK)',
    keyColumn='InvoiceID',
    splits=(
        ('Split Credit Notes', 'Invoices: IsCreditNote == FALSE', 'Credit Notes'),
    ),
    extraVariables=(
        ('CreditNoteCount', 0),
    ),
    sourceTimeoutSeconds=3600,
    batchSize=100000,
))


INVOICE_LINES = _register(PackageSpec(
    name='EXT_SQL_InvoiceLines',
    description='Numeric-key incremental invoice line extract joined to Warehouse.StockItems for the last cost price, so gross margin can be computed in staging without a second pass over the OLTP database.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_bounded',
    legacyTarget='raw.SqlInvoiceLine',
    targetTable='raw_sql_invoice_line',
    keyColumns=('InvoiceLineID',),
    sourceSql="""SELECT  il.InvoiceLineID,
        il.InvoiceID,
        il.StockItemID,
        il.Description,
        il.PackageTypeID,
        il.Quantity,
        il.UnitPrice,
        il.TaxRate,
        il.TaxAmount,
        il.LineProfit,
        il.ExtendedPrice,
        si.LastCostPrice,
        il.LastEditedWhen
FROM    Sales.InvoiceLines AS il WITH (NOLOCK)
        INNER JOIN Warehouse.StockItems AS si WITH (NOLOCK)
            ON si.StockItemID = il.StockItemID
WHERE   il.InvoiceLineID > ?
  AND   il.InvoiceLineID <= ?""",
    sqlParams=('watermarkFrom', 'watermarkTo'),
    columns=(
        ('InvoiceLineID', 'int'),
        ('InvoiceID', 'int'),
        ('StockItemID', 'int'),
        ('Description', 'string'),
        ('PackageTypeID', 'int'),
        ('Quantity', 'int'),
        ('UnitPrice', 'decimal(18,2)'),
        ('TaxRate', 'decimal(18,3)'),
        ('TaxAmount', 'decimal(18,2)'),
        ('LineProfit', 'decimal(18,2)'),
        ('ExtendedPrice', 'decimal(18,2)'),
        ('LastCostPrice', 'decimal(18,2)'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        (
            'GrossMarginPct',
            'ExtendedPrice == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)(LineProfit / ExtendedPrice)',
            'decimal(9,4)',
        ),
        ('NegativeMarginFlag', 'LineProfit < 0 ? "Y" : "N"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveInvoiceLines',
    errorDisposition='RedirectRow',
    watermarkObject='Sales.InvoiceLines',
    maxKeySql='SELECT ISNULL(MAX(InvoiceLineID), 0) AS MaxKey FROM Sales.InvoiceLines WITH (NOLOCK)',
    keyColumn='InvoiceLineID',
    rejectTarget='err.RejectedConstraintViolation',
    sourceTimeoutSeconds=7200,
    batchSize=250000,
))


PROMOTIONS = _register(PackageSpec(
    name='EXT_SQL_Promotions',
    description='Full reload of Sales.Promotions with promotion lines and redemption counts aggregated in the source query. Regional promotions differ in mechanic (percentage off in NA, VAT-inclusive price points in EU, bundle quantities in APAC) so the mechanic code is carried through untranslated.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='full_reload',
    legacyTarget='raw.SqlOrder',
    targetTable='raw_sql_promotion',
    keyColumns=('PromotionID',),
    sourceSql="""SELECT  p.PromotionID,
        p.PromotionCode,
        p.PromotionName,
        p.PromotionMechanicCode,
        p.RegionCode,
        p.DiscountPercentage,
        p.DiscountAmount,
        p.BundleQuantity,
        ISNULL(pl.PromotionLineCount, 0)    AS PromotionLineCount,
        ISNULL(pr.RedemptionCount, 0)       AS RedemptionCount,
        ISNULL(pr.RedeemedValue, 0.00)      AS RedeemedValue,
        p.StartDate,
        p.EndDate,
        CASE WHEN SYSDATETIME() BETWEEN p.StartDate AND p.EndDate THEN 1 ELSE 0 END AS IsActive
FROM    Sales.Promotions AS p WITH (NOLOCK)
        LEFT OUTER JOIN (
            SELECT PromotionID, COUNT(*) AS PromotionLineCount
            FROM   Sales.PromotionLines WITH (NOLOCK)
            GROUP BY PromotionID
        ) AS pl ON pl.PromotionID = p.PromotionID
        LEFT OUTER JOIN (
            SELECT PromotionID, COUNT(*) AS RedemptionCount, SUM(RedeemedValue) AS RedeemedValue
            FROM   Sales.PromotionRedemptions WITH (NOLOCK)
            GROUP BY PromotionID
        ) AS pr ON pr.PromotionID = p.PromotionID""",
    sqlParams=(),
    columns=(
        ('PromotionID', 'int'),
        ('PromotionCode', 'string'),
        ('PromotionName', 'string'),
        ('PromotionMechanicCode', 'string'),
        ('RegionCode', 'string'),
        ('DiscountPercentage', 'decimal(9,4)'),
        ('DiscountAmount', 'decimal(18,2)'),
        ('BundleQuantity', 'int'),
        ('PromotionLineCount', 'int'),
        ('RedemptionCount', 'int'),
        ('RedeemedValue', 'decimal(18,2)'),
        ('StartDate', 'timestamp'),
        ('EndDate', 'timestamp'),
        ('IsActive', 'boolean'),
    ),
    derivations=(
        ('RecordKind', '"PROMOTION"', 'string'),
        (
            'RedemptionRatePct',
            'PromotionLineCount == 0 ? (DT_NUMERIC,9,4)0 : (DT_NUMERIC,9,4)((DT_NUMERIC,18,4)RedemptionCount / PromotionLineCount)',
            'decimal(9,4)',
        ),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='derivePromotions',
    errorDisposition='FailComponent',
    recordKind='PROMOTION',
    clearSql="DELETE FROM raw.SqlOrder WHERE RecordKind = N'PROMOTION';",
    sourceTimeoutSeconds=900,
    batchSize=5000,
))


SALES_TERRITORIES = _register(PackageSpec(
    name='EXT_SQL_SalesTerritories',
    description='Full reload of Sales.SalesTerritories joined to the current quota and commission plan. Quota periods follow the regional fiscal calendar, so the period code is taken from the quota row rather than derived.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='full_reload',
    legacyTarget='raw.SqlOrder',
    targetTable='raw_sql_sales_territory',
    keyColumns=('SalesTerritoryID',),
    sourceSql="""SELECT  t.SalesTerritoryID,
        t.SalesTerritoryCode,
        t.SalesTerritoryName,
        t.RegionCode,
        t.ParentTerritoryID,
        t.ManagerPersonID,
        q.FiscalPeriodCode,
        q.QuotaAmount,
        q.QuotaCurrencyCode,
        cp.CommissionPlanCode,
        cp.CommissionRate,
        t.IsActive,
        t.ValidFrom
FROM    Sales.SalesTerritories AS t WITH (NOLOCK)
        OUTER APPLY (
            SELECT TOP (1) sq.FiscalPeriodCode, sq.QuotaAmount, sq.QuotaCurrencyCode
            FROM   Sales.SalesQuotas AS sq WITH (NOLOCK)
            WHERE  sq.SalesTerritoryID = t.SalesTerritoryID
            ORDER BY sq.PeriodStartDate DESC
        ) AS q
        LEFT OUTER JOIN Sales.CommissionPlans AS cp WITH (NOLOCK)
            ON cp.CommissionPlanID = t.CommissionPlanID""",
    sqlParams=(),
    columns=(
        ('SalesTerritoryID', 'int'),
        ('SalesTerritoryCode', 'string'),
        ('SalesTerritoryName', 'string'),
        ('RegionCode', 'string'),
        ('ParentTerritoryID', 'int'),
        ('ManagerPersonID', 'int'),
        ('FiscalPeriodCode', 'string'),
        ('QuotaAmount', 'decimal(18,2)'),
        ('QuotaCurrencyCode', 'string'),
        ('CommissionPlanCode', 'string'),
        ('CommissionRate', 'decimal(9,4)'),
        ('IsActive', 'boolean'),
        ('ValidFrom', 'timestamp'),
    ),
    derivations=(
        ('RecordKind', '"TERRITORY"', 'string'),
        (
            'FiscalCalendarCode',
            'RegionCode == "NA" ? "445" : (RegionCode == "EU" ? "CAL" : "APR_MAR")',
            'string',
        ),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveSalesTerritories',
    errorDisposition='FailComponent',
    recordKind='TERRITORY',
    clearSql="DELETE FROM raw.SqlOrder WHERE RecordKind = N'TERRITORY';",
    sourceTimeoutSeconds=600,
    batchSize=2000,
))


CUSTOMER_SEGMENTS = _register(PackageSpec(
    name='EXT_SQL_CustomerSegments',
    description='Full reload of the current customer segment assignment from Sales.vw_CustomerSegmentCurrent. Consent and retention differ by region: EU assignments older than the retention window are landed without the scoring attributes that drove them.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='full_reload',
    legacyTarget='raw.SqlOrder',
    targetTable='raw_sql_customer_segment',
    keyColumns=('CustomerSegmentID',),
    sourceSql="""SELECT  a.CustomerSegmentAssignmentID,
        a.CustomerID,
        s.SegmentCode,
        s.SegmentName,
        a.RegionCode,
        CASE WHEN a.RegionCode = N'EU' AND a.AssignedDate < DATEADD(month, -24, SYSDATETIME())
             THEN NULL ELSE a.SegmentScore END      AS SegmentScore,
        CASE WHEN a.RegionCode = N'EU' AND a.AssignedDate < DATEADD(month, -24, SYSDATETIME())
             THEN NULL ELSE a.ScoringModelCode END  AS ScoringModelCode,
        a.ConsentStatusCode,
        a.AssignedDate,
        a.ValidFrom,
        a.ValidTo
FROM    Sales.vw_CustomerSegmentCurrent AS a WITH (NOLOCK)
        INNER JOIN Sales.CustomerSegments AS s WITH (NOLOCK)
            ON s.CustomerSegmentID = a.CustomerSegmentID
WHERE   a.ValidTo > SYSDATETIME()""",
    sqlParams=(),
    columns=(
        ('CustomerSegmentAssignmentID', 'int'),
        ('CustomerID', 'int'),
        ('SegmentCode', 'string'),
        ('SegmentName', 'string'),
        ('RegionCode', 'string'),
        ('SegmentScore', 'decimal(9,4)'),
        ('ScoringModelCode', 'string'),
        ('ConsentStatusCode', 'string'),
        ('AssignedDate', 'timestamp'),
        ('ValidFrom', 'timestamp'),
        ('ValidTo', 'timestamp'),
    ),
    derivations=(
        ('RecordKind', '"SEGMENT"', 'string'),
        (
            'MarketableFlag',
            'RegionCode == "EU" ? (ConsentStatusCode == "OPTIN" ? "Y" : "N") : (ConsentStatusCode == "OPTOUT" ? "N" : "Y")',
            'string',
        ),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveCustomerSegments',
    errorDisposition='RedirectRow',
    recordKind='SEGMENT',
    rejectTarget='err.RejectedCustomer',
    clearSql="DELETE FROM raw.SqlOrder WHERE RecordKind = N'SEGMENT';",
    extraVariables=(
        ('SuppressedSegmentCount', 0),
    ),
    sourceTimeoutSeconds=900,
    batchSize=25000,
))


STOCK_ITEMS = _register(PackageSpec(
    name='EXT_SQL_StockItems',
    description='Timestamp-watermark incremental over Warehouse.StockItems using the temporal ValidFrom column with a lookback window for late-arriving edits, joined to the stock holding and replenishment rule. Deletions are picked up from change tracking.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='timestamp',
    legacyTarget='raw.SqlStockItem',
    targetTable='raw_sql_stock_item',
    keyColumns=('StockItemID', 'DeleteFlag'),
    sourceSql="""SELECT  si.StockItemID,
        si.StockItemName,
        si.SupplierID,
        si.ColorID,
        si.UnitPackageID,
        si.OuterPackageID,
        si.Brand,
        si.Size,
        si.LeadTimeDays,
        si.QuantityPerOuter,
        si.IsChillerStock,
        si.Barcode,
        si.TaxRate,
        si.UnitPrice,
        si.RecommendedRetailPrice,
        si.TypicalWeightPerUnit,
        sh.QuantityOnHand,
        sh.ReorderLevel,
        sh.TargetStockLevel,
        rr.ReplenishmentRuleCode,
        si.ValidFrom,
        si.LastEditedWhen
FROM    Warehouse.StockItems AS si WITH (NOLOCK)
        LEFT OUTER JOIN Warehouse.StockItemHoldings AS sh WITH (NOLOCK)
            ON sh.StockItemID = si.StockItemID
        LEFT OUTER JOIN Warehouse.ReplenishmentRules AS rr WITH (NOLOCK)
            ON rr.StockItemID = si.StockItemID
           AND rr.IsCurrent = 1
WHERE   si.ValidFrom >= DATEADD(minute, -240, CAST(? AS datetime2(7)))
  AND   si.ValidFrom <  CAST(? AS datetime2(7))""",
    sqlParams=('watermarkFrom', 'watermarkTo'),
    columns=(
        ('StockItemID', 'int'),
        ('StockItemName', 'string'),
        ('SupplierID', 'int'),
        ('ColorID', 'int'),
        ('UnitPackageID', 'int'),
        ('OuterPackageID', 'int'),
        ('Brand', 'string'),
        ('Size', 'string'),
        ('LeadTimeDays', 'int'),
        ('QuantityPerOuter', 'int'),
        ('IsChillerStock', 'boolean'),
        ('Barcode', 'string'),
        ('TaxRate', 'decimal(18,2)'),
        ('UnitPrice', 'decimal(18,2)'),
        ('RecommendedRetailPrice', 'decimal(18,2)'),
        ('TypicalWeightPerUnit', 'decimal(18,2)'),
        ('QuantityOnHand', 'int'),
        ('ReorderLevel', 'int'),
        ('TargetStockLevel', 'int'),
        ('ReplenishmentRuleCode', 'string'),
        ('ValidFrom', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        ('BelowReorderFlag', 'QuantityOnHand < ReorderLevel ? "Y" : "N"', 'string'),
        ('HandlingClass', 'IsChillerStock ? "CHILL" : "AMB"', 'string'),
        ('DeleteFlag', '"N"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveStockItems',
    errorDisposition='RedirectRow',
    watermarkObject='Warehouse.StockItems',
    rejectTarget='err.RejectedProduct',
    deleteDetection=DeleteDetection(
        name='Detect Deleted Stock Items',
        changeTrackingObject='Warehouse.StockItems',
        versionSql=None,
        sql="""SELECT  ct.StockItemID,
        ct.SYS_CHANGE_OPERATION  AS ChangeOperation,
        ct.SYS_CHANGE_VERSION    AS ChangeVersion
FROM    CHANGETABLE(CHANGES Warehouse.StockItems, ?) AS ct
WHERE   ct.SYS_CHANGE_OPERATION = 'D'""",
        columns=(
            ('StockItemID', 'int'),
            ('ChangeOperation', 'string'),
            ('ChangeVersion', 'bigint'),
        ),
        derivations=(
            ('SourceSystemCode', '"WWI_OLTP"', 'string'),
            ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
            ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
            ('DeleteFlag', '"Y"', 'string'),
        ),
    ),
    extraVariables=(
        ('LookbackMinutes', 240),
    ),
    sourceTimeoutSeconds=2400,
    batchSize=50000,
))


STOCK_MOVEMENTS = _register(PackageSpec(
    name='EXT_SQL_StockMovements',
    description='Numeric-key incremental over Warehouse.StockItemTransactions via Warehouse.vw_StockMovementExtract, carrying the bin and site so the movement can be attributed to a location. Transaction types are split into receipts, issues and adjustments on the way in.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_bounded',
    legacyTarget='raw.SqlStockMovement',
    targetTable='raw_sql_stock_movement',
    keyColumns=('StockItemTransactionID',),
    sourceSql="""SELECT  m.StockItemTransactionID,
        m.StockItemID,
        m.TransactionTypeID,
        tt.TransactionTypeName,
        m.CustomerID,
        m.SupplierID,
        m.InvoiceID,
        m.PurchaseOrderID,
        m.Quantity,
        ws.WarehouseSiteCode,
        b.BinCode,
        CASE WHEN m.Quantity >= 0 THEN N'IN' ELSE N'OUT' END AS MovementDirectionCode,
        m.TransactionOccurredWhen,
        m.LastEditedWhen
FROM    Warehouse.vw_StockMovementExtract AS m WITH (NOLOCK)
        INNER JOIN Application.TransactionTypes AS tt WITH (NOLOCK)
            ON tt.TransactionTypeID = m.TransactionTypeID
        LEFT OUTER JOIN Warehouse.Bins AS b WITH (NOLOCK)
            ON b.BinID = m.BinID
        LEFT OUTER JOIN Warehouse.WarehouseSites AS ws WITH (NOLOCK)
            ON ws.WarehouseSiteID = b.WarehouseSiteID
WHERE   m.StockItemTransactionID > ?
  AND   m.StockItemTransactionID <= ?""",
    sqlParams=('watermarkFrom', 'watermarkTo'),
    columns=(
        ('StockItemTransactionID', 'bigint'),
        ('StockItemID', 'int'),
        ('TransactionTypeID', 'int'),
        ('TransactionTypeName', 'string'),
        ('CustomerID', 'int'),
        ('SupplierID', 'int'),
        ('InvoiceID', 'int'),
        ('PurchaseOrderID', 'int'),
        ('Quantity', 'decimal(18,3)'),
        ('WarehouseSiteCode', 'string'),
        ('BinCode', 'string'),
        ('MovementDirectionCode', 'string'),
        ('TransactionOccurredWhen', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        ('AbsoluteQuantity', 'ABS(Quantity)', 'decimal(18,3)'),
        (
            'MovementClass',
            'ISNULL(InvoiceID) && ISNULL(PurchaseOrderID) ? "ADJ" : (ISNULL(InvoiceID) ? "RCPT" : "ISSUE")',
            'string',
        ),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveStockMovements',
    errorDisposition='RedirectRow',
    watermarkObject='Warehouse.StockItemTransactions',
    maxKeySql='SELECT ISNULL(MAX(StockItemTransactionID), 0) AS MaxKey FROM Warehouse.StockItemTransactions WITH (NOLOCK)',
    keyColumn='StockItemTransactionID',
    rejectTarget='err.RejectedConstraintViolation',
    splits=(
        ('Split Adjustments', 'Movements: MovementClass != "ADJ"', 'Adjustments'),
    ),
    extraVariables=(
        ('AdjustmentRowCount', 0),
    ),
    sourceTimeoutSeconds=7200,
    batchSize=250000,
))


STOCK_TRANSFERS = _register(PackageSpec(
    name='EXT_SQL_StockTransfers',
    description='Numeric-key incremental over Warehouse.StockTransferLines joined to the transfer header and both site records, so in-transit stock between sites can be reconciled. In-transit lines older than the tolerance are flagged rather than rejected.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_open',
    legacyTarget='raw.SqlStockMovement',
    targetTable='raw_sql_stock_transfer',
    keyColumns=('StockTransferLineID',),
    sourceSql="""SELECT  tl.StockTransferLineID,
        tl.StockTransferID,
        t.TransferReference,
        tl.StockItemID,
        fs.WarehouseSiteCode    AS FromSiteCode,
        ts.WarehouseSiteCode    AS ToSiteCode,
        tl.TransferQuantity,
        tl.ReceivedQuantity,
        t.TransferStatusCode,
        t.DispatchedWhen,
        tl.ReceivedWhen,
        tl.LastEditedWhen
FROM    Warehouse.StockTransferLines AS tl WITH (NOLOCK)
        INNER JOIN Warehouse.StockTransfers AS t WITH (NOLOCK)
            ON t.StockTransferID = tl.StockTransferID
        INNER JOIN Warehouse.WarehouseSites AS fs WITH (NOLOCK)
            ON fs.WarehouseSiteID = t.FromWarehouseSiteID
        INNER JOIN Warehouse.WarehouseSites AS ts WITH (NOLOCK)
            ON ts.WarehouseSiteID = t.ToWarehouseSiteID
WHERE   tl.StockTransferLineID > ?
  AND   t.TransferStatusCode <> N'CANC'""",
    sqlParams=('watermarkFrom',),
    columns=(
        ('StockTransferLineID', 'bigint'),
        ('StockTransferID', 'int'),
        ('TransferReference', 'string'),
        ('StockItemID', 'int'),
        ('FromSiteCode', 'string'),
        ('ToSiteCode', 'string'),
        ('TransferQuantity', 'decimal(18,3)'),
        ('ReceivedQuantity', 'decimal(18,3)'),
        ('TransferStatusCode', 'string'),
        ('DispatchedWhen', 'timestamp'),
        ('ReceivedWhen', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        ('InTransitQuantity', 'TransferQuantity - ReceivedQuantity', 'decimal(18,3)'),
        (
            'StaleTransitFlag',
            'ISNULL(ReceivedWhen) && DATEDIFF("dd", DispatchedWhen, GETDATE()) > 14 ? "Y" : "N"',
            'string',
        ),
        ('MovementClass', '"XFER"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveStockTransfers',
    errorDisposition='RedirectRow',
    watermarkObject='Warehouse.StockTransferLines',
    rejectTarget='err.RejectedConstraintViolation',
    extraVariables=(
        ('InTransitToleranceDays', 14),
    ),
    sourceTimeoutSeconds=1800,
    batchSize=50000,
))


SHIPMENTS = _register(PackageSpec(
    name='EXT_SQL_Shipments',
    description='Numeric-key incremental shipment header extract over Shipping.vw_ShipmentExtract, joined to the carrier, delivery route and freight rate. Customs declarations are attached for cross-border shipments only, which in practice means EU and APAC.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_bounded',
    legacyTarget='raw.SqlShipment',
    targetTable='raw_sql_shipment',
    keyColumns=('ShipmentHeaderID',),
    sourceSql="""SELECT  s.ShipmentHeaderID,
        s.ShipmentReference,
        s.InvoiceID,
        s.CustomerID,
        s.CarrierID,
        c.CarrierCode,
        s.ServiceLevelCode,
        dr.DeliveryRouteCode,
        s.RegionCode,
        s.OriginCountryCode,
        s.DestinationCountryCode,
        s.ShipmentWeightKg,
        fr.FreightCharge,
        fr.FreightCurrencyCode,
        cd.CustomsDeclarationNumber,
        s.ShipmentStatusCode,
        s.DispatchedWhen,
        s.DeliveredWhen,
        s.LastEditedWhen
FROM    Shipping.vw_ShipmentExtract AS s WITH (NOLOCK)
        INNER JOIN Shipping.Carriers AS c WITH (NOLOCK)
            ON c.CarrierID = s.CarrierID
        LEFT OUTER JOIN Shipping.DeliveryRoutes AS dr WITH (NOLOCK)
            ON dr.DeliveryRouteID = s.DeliveryRouteID
        LEFT OUTER JOIN Shipping.FreightRates AS fr WITH (NOLOCK)
            ON fr.CarrierID = s.CarrierID
           AND fr.ServiceLevelCode = s.ServiceLevelCode
           AND s.DispatchedWhen >= fr.EffectiveFrom
           AND s.DispatchedWhen <  fr.EffectiveTo
        LEFT OUTER JOIN Shipping.CustomsDeclarations AS cd WITH (NOLOCK)
            ON cd.ShipmentHeaderID = s.ShipmentHeaderID
WHERE   s.ShipmentHeaderID > ?
  AND   s.ShipmentHeaderID <= ?
  AND   s.ShipmentStatusCode <> N'VOID'""",
    sqlParams=('watermarkFrom', 'watermarkTo'),
    columns=(
        ('ShipmentHeaderID', 'int'),
        ('ShipmentReference', 'string'),
        ('InvoiceID', 'int'),
        ('CustomerID', 'int'),
        ('CarrierID', 'int'),
        ('CarrierCode', 'string'),
        ('ServiceLevelCode', 'string'),
        ('DeliveryRouteCode', 'string'),
        ('RegionCode', 'string'),
        ('OriginCountryCode', 'string'),
        ('DestinationCountryCode', 'string'),
        ('ShipmentWeightKg', 'decimal(18,3)'),
        ('FreightCharge', 'decimal(18,2)'),
        ('FreightCurrencyCode', 'string'),
        ('CustomsDeclarationNumber', 'string'),
        ('ShipmentStatusCode', 'string'),
        ('DispatchedWhen', 'timestamp'),
        ('DeliveredWhen', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        (
            'TransitHours',
            'ISNULL(DeliveredWhen) ? -1 : DATEDIFF("hh", DispatchedWhen, DeliveredWhen)',
            'int',
        ),
        ('CrossBorderFlag', 'OriginCountryCode == DestinationCountryCode ? "N" : "Y"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveShipments',
    errorDisposition='RedirectRow',
    watermarkObject='Shipping.ShipmentHeaders',
    maxKeySql='SELECT ISNULL(MAX(ShipmentHeaderID), 0) AS MaxKey FROM Shipping.ShipmentHeaders WITH (NOLOCK)',
    keyColumn='ShipmentHeaderID',
    rejectTarget='err.RejectedConstraintViolation',
    extraVariables=(
        ('CrossBorderCount', 0),
    ),
    sourceTimeoutSeconds=3600,
    batchSize=100000,
))


SHIPMENT_LINES = _register(PackageSpec(
    name='EXT_SQL_ShipmentLines',
    description='Numeric-key incremental shipment line extract joined to the packaging type and the latest scan event, so partially delivered shipments can be reconciled against the carrier scan files ingested by ING_FILE_CarrierScan.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_bounded',
    legacyTarget='raw.SqlShipmentLine',
    targetTable='raw_sql_shipment_line',
    keyColumns=('ShipmentLineID',),
    sourceSql="""SELECT  sl.ShipmentLineID,
        sl.ShipmentHeaderID,
        sl.InvoiceLineID,
        sl.StockItemID,
        sl.ShippedQuantity,
        pt.PackagingTypeCode,
        sl.PackageWeightKg,
        sl.TrackingNumber,
        ev.ScanStatusCode       AS LastScanStatusCode,
        ev.ScanOccurredWhen     AS LastScanWhen,
        sl.LastEditedWhen
FROM    Shipping.ShipmentLines AS sl WITH (NOLOCK)
        LEFT OUTER JOIN Shipping.PackagingTypes AS pt WITH (NOLOCK)
            ON pt.PackagingTypeID = sl.PackagingTypeID
        OUTER APPLY (
            SELECT TOP (1) se.ScanStatusCode, se.ScanOccurredWhen
            FROM   Shipping.ShipmentEvents AS se WITH (NOLOCK)
            WHERE  se.ShipmentLineID = sl.ShipmentLineID
            ORDER BY se.ScanOccurredWhen DESC
        ) AS ev
WHERE   sl.ShipmentLineID > ?
  AND   sl.ShipmentLineID <= ?""",
    sqlParams=('watermarkFrom', 'watermarkTo'),
    columns=(
        ('ShipmentLineID', 'bigint'),
        ('ShipmentHeaderID', 'int'),
        ('InvoiceLineID', 'int'),
        ('StockItemID', 'int'),
        ('ShippedQuantity', 'int'),
        ('PackagingTypeCode', 'string'),
        ('PackageWeightKg', 'decimal(18,3)'),
        ('TrackingNumber', 'string'),
        ('LastScanStatusCode', 'string'),
        ('LastScanWhen', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        ('AwaitingScanFlag', 'ISNULL(LastScanWhen) ? "Y" : "N"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveShipmentLines',
    errorDisposition='RedirectRow',
    watermarkObject='Shipping.ShipmentLines',
    maxKeySql='SELECT ISNULL(MAX(ShipmentLineID), 0) AS MaxKey FROM Shipping.ShipmentLines WITH (NOLOCK)',
    keyColumn='ShipmentLineID',
    rejectTarget='err.RejectedConstraintViolation',
    sourceTimeoutSeconds=3600,
    batchSize=150000,
))


RETURNS = _register(PackageSpec(
    name='EXT_SQL_Returns',
    description='Numeric-key incremental return line extract over Returns.vw_ReturnExtract, carrying the authorisation, the reason code and the inspection outcome. Lines still awaiting inspection are landed with a pending disposition rather than being held back.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_open',
    legacyTarget='raw.SqlReturnLine',
    targetTable='raw_sql_return_line',
    keyColumns=('ReturnLineID',),
    sourceSql="""SELECT  rl.ReturnLineID,
        ra.ReturnAuthorizationID,
        ra.ReturnAuthorizationNumber,
        ra.CustomerID,
        rl.InvoiceLineID,
        rl.StockItemID,
        rl.ReturnedQuantity,
        rr.ReturnReasonCode,
        rr.ReturnReasonDescription,
        ISNULL(ri.InspectionOutcomeCode, N'PENDING')    AS InspectionOutcomeCode,
        ISNULL(ri.DispositionCode, N'UNKNOWN')          AS DispositionCode,
        rl.RefundAmount,
        ra.RegionCode,
        ra.ReturnedWhen,
        ri.InspectedWhen,
        rl.LastEditedWhen
FROM    Returns.vw_ReturnExtract AS rl WITH (NOLOCK)
        INNER JOIN Returns.ReturnAuthorizations AS ra WITH (NOLOCK)
            ON ra.ReturnAuthorizationID = rl.ReturnAuthorizationID
        INNER JOIN Returns.ReturnReasons AS rr WITH (NOLOCK)
            ON rr.ReturnReasonID = rl.ReturnReasonID
        LEFT OUTER JOIN Returns.ReturnInspections AS ri WITH (NOLOCK)
            ON ri.ReturnLineID = rl.ReturnLineID
WHERE   rl.ReturnLineID > ?""",
    sqlParams=('watermarkFrom',),
    columns=(
        ('ReturnLineID', 'bigint'),
        ('ReturnAuthorizationID', 'int'),
        ('ReturnAuthorizationNumber', 'string'),
        ('CustomerID', 'int'),
        ('InvoiceLineID', 'int'),
        ('StockItemID', 'int'),
        ('ReturnedQuantity', 'int'),
        ('ReturnReasonCode', 'string'),
        ('ReturnReasonDescription', 'string'),
        ('InspectionOutcomeCode', 'string'),
        ('DispositionCode', 'string'),
        ('RefundAmount', 'decimal(18,2)'),
        ('RegionCode', 'string'),
        ('ReturnedWhen', 'timestamp'),
        ('InspectedWhen', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        (
            'DaysToInspect',
            'ISNULL(InspectedWhen) ? -1 : DATEDIFF("dd", ReturnedWhen, InspectedWhen)',
            'int',
        ),
        ('RestockableFlag', 'DispositionCode == "RESTOCK" ? "Y" : "N"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveReturns',
    errorDisposition='RedirectRow',
    watermarkObject='Returns.ReturnLines',
    splits=(
        ('Route Pending Inspections', 'Inspected: InspectionOutcomeCode != "PENDING"', 'Pending'),
    ),
    extraVariables=(
        ('PendingInspectionCount', 0),
    ),
    sourceTimeoutSeconds=1800,
    batchSize=50000,
))


CREDIT_NOTES = _register(PackageSpec(
    name='EXT_SQL_CreditNotes',
    description='Numeric-key incremental credit note line extract over Returns.vw_CreditNoteExtract. The tax reversal is region-specific: NA reverses state sales tax at the original rate, EU reverses VAT and needs the credit note reference on the VAT return, APAC reverses GST in the period of issue.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_open',
    legacyTarget='raw.SqlCreditNote',
    targetTable='raw_sql_credit_note',
    keyColumns=('CreditNoteLineID',),
    sourceSql="""SELECT  cl.CreditNoteLineID,
        cn.CreditNoteID,
        cn.CreditNoteNumber,
        cn.CustomerID,
        cn.InvoiceID,
        cl.StockItemID,
        cl.CreditedQuantity,
        cl.CreditedExcludingTax,
        cl.CreditedTaxAmount,
        cl.CreditedIncludingTax,
        cn.RegionCode,
        CASE cn.RegionCode
            WHEN N'NA'   THEN N'SALESTAX'
            WHEN N'EU'   THEN N'VAT'
            WHEN N'APAC' THEN N'GST'
            ELSE N'NONE'
        END                                 AS TaxTreatmentCode,
        CASE cn.RegionCode
            WHEN N'NA'   THEN N'ORIGRATE'
            WHEN N'EU'   THEN N'CREDITREF'
            WHEN N'APAC' THEN N'ISSUEPRD'
            ELSE N'NONE'
        END                                 AS TaxReversalBasisCode,
        cn.CreditNoteDate,
        cl.LastEditedWhen
FROM    Returns.vw_CreditNoteExtract AS cl WITH (NOLOCK)
        INNER JOIN Returns.CreditNotes AS cn WITH (NOLOCK)
            ON cn.CreditNoteID = cl.CreditNoteID
WHERE   cl.CreditNoteLineID > ?
  AND   cn.IsVoided = 0""",
    sqlParams=('watermarkFrom',),
    columns=(
        ('CreditNoteLineID', 'bigint'),
        ('CreditNoteID', 'int'),
        ('CreditNoteNumber', 'string'),
        ('CustomerID', 'int'),
        ('InvoiceID', 'int'),
        ('StockItemID', 'int'),
        ('CreditedQuantity', 'int'),
        ('CreditedExcludingTax', 'decimal(18,2)'),
        ('CreditedTaxAmount', 'decimal(18,2)'),
        ('CreditedIncludingTax', 'decimal(18,2)'),
        ('RegionCode', 'string'),
        ('TaxTreatmentCode', 'string'),
        ('TaxReversalBasisCode', 'string'),
        ('CreditNoteDate', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        ('SignedCreditAmount', '-CreditedIncludingTax', 'decimal(18,2)'),
        ('VatReturnRequiredFlag', 'TaxTreatmentCode == "VAT" ? "Y" : "N"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveCreditNotes',
    errorDisposition='RedirectRow',
    watermarkObject='Returns.CreditNoteLines',
    rejectTarget='err.RejectedConstraintViolation',
    sourceTimeoutSeconds=1800,
    batchSize=25000,
))


WEB_SESSIONS = _register(PackageSpec(
    name='EXT_SQL_WebSessions',
    description='Date-window extract over Ecommerce.WebSessions. The window comes from etl.usp_GetWatermark and the target rows for that window are deleted before the load, so re-running a window is idempotent. EU sessions without analytics consent are landed without the device fingerprint and referrer.',
    sourceSystemCode='WWI_WEB',
    loadPattern='date_window',
    legacyTarget='raw.SqlWebSession',
    targetTable='raw_sql_web_session',
    keyColumns=('WebSessionID',),
    sourceSql="""SELECT  ws.WebSessionID,
        ws.SessionGuid,
        ws.CustomerID,
        ws.RegionCode,
        ws.ChannelCode,
        ws.DeviceCategoryCode,
        CASE WHEN ws.RegionCode = N'EU' AND ws.AnalyticsConsentCode <> N'GRANTED'
             THEN NULL ELSE ws.DeviceFingerprint END    AS DeviceFingerprint,
        CASE WHEN ws.RegionCode = N'EU' AND ws.AnalyticsConsentCode <> N'GRANTED'
             THEN NULL ELSE ws.ReferrerDomain END       AS ReferrerDomain,
        ws.LandingPagePath,
        ws.PageViewCount,
        DATEDIFF(second, ws.SessionStartedWhen, ws.SessionEndedWhen) AS SessionDurationSeconds,
        CASE WHEN ch.CartHeaderID IS NULL THEN 0 ELSE 1 END          AS HasCartActivity,
        ws.HasCheckout,
        ws.AnalyticsConsentCode,
        ws.SessionStartedWhen,
        ws.SessionEndedWhen
FROM    Ecommerce.WebSessions AS ws WITH (NOLOCK)
        LEFT OUTER JOIN Ecommerce.CartHeaders AS ch WITH (NOLOCK)
            ON ch.WebSessionID = ws.WebSessionID
WHERE   ws.SessionStartedWhen >= CAST(? AS datetime2(7))
  AND   ws.SessionStartedWhen <  CAST(? AS datetime2(7))""",
    sqlParams=('watermarkFrom', 'watermarkTo'),
    columns=(
        ('WebSessionID', 'bigint'),
        ('SessionGuid', 'string'),
        ('CustomerID', 'int'),
        ('RegionCode', 'string'),
        ('ChannelCode', 'string'),
        ('DeviceCategoryCode', 'string'),
        ('DeviceFingerprint', 'string'),
        ('ReferrerDomain', 'string'),
        ('LandingPagePath', 'string'),
        ('PageViewCount', 'int'),
        ('SessionDurationSeconds', 'int'),
        ('HasCartActivity', 'boolean'),
        ('HasCheckout', 'boolean'),
        ('AnalyticsConsentCode', 'string'),
        ('SessionStartedWhen', 'timestamp'),
        ('SessionEndedWhen', 'timestamp'),
    ),
    derivations=(
        ('BounceFlag', 'PageViewCount <= 1 ? "Y" : "N"', 'string'),
        ('ConversionFlag', 'HasCheckout ? "Y" : "N"', 'string'),
        ('SourceSystemCode', '"WWI_WEB"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveWebSessions',
    errorDisposition='IgnoreFailure',
    watermarkObject='Ecommerce.WebSessions',
    windowColumn='SessionStartedWhen',
    clearSql='DELETE FROM raw.SqlWebSession WHERE SessionStartedWhen >= CAST(? AS datetime2(7)) AND SessionStartedWhen < CAST(? AS datetime2(7));',
    extraVariables=(
        ('WindowDays', 1),
        ('ConsentSuppressedCount', 0),
    ),
    sourceTimeoutSeconds=3600,
    batchSize=200000,
))


LOYALTY_LEDGER = _register(PackageSpec(
    name='EXT_SQL_LoyaltyLedger',
    description='Numeric-key incremental over Loyalty.LoyaltyPointsLedger into raw.SqlLoyaltyLedger, plus a lookback re-read of rows the expiry sweep touched since the previous run. Points expiry rules differ by region, so the region code travels with every ledger entry.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_open',
    legacyTarget='raw.SqlLoyaltyLedger',
    targetTable='raw_sql_loyalty_ledger',
    keyColumns=('LoyaltyLedgerID',),
    sourceSql="""SELECT  CONVERT(nvarchar(50), l.LoyaltyLedgerID)          AS LoyaltyLedgerID,
        CONVERT(nvarchar(50), l.LoyaltyMemberID)          AS LoyaltyMemberID,
        CONVERT(nvarchar(50), m.CustomerID)               AS CustomerID,
        p.ProgramCode,
        m.TierCode,
        l.EntryTypeCode,
        CONVERT(nvarchar(50), l.PointsDelta)              AS PointsDelta,
        CONVERT(nvarchar(50), l.PointsRemaining)          AS PointsBalanceAfter,
        CONVERT(nvarchar(50), l.SourceInvoiceID)          AS SourceInvoiceID,
        l.SourceReference                                 AS RedemptionReference,
        CONVERT(nvarchar(40), l.EntryWhen, 126)           AS EntryWhen,
        CONVERT(nvarchar(40), l.ExpiresOnDate, 23)        AS ExpiryDate,
        m.RegionCode,
        CONVERT(nvarchar(40), ISNULL(l.ExpiredWhen, l.EntryWhen), 126) AS LastEditedWhen
FROM    Loyalty.LoyaltyPointsLedger AS l WITH (NOLOCK)
        INNER JOIN Loyalty.LoyaltyMembers AS m WITH (NOLOCK)
            ON m.LoyaltyMemberID = l.LoyaltyMemberID
        INNER JOIN Loyalty.LoyaltyPrograms AS p WITH (NOLOCK)
            ON p.LoyaltyProgramID = m.LoyaltyProgramID
WHERE   l.LoyaltyLedgerID > CAST(? AS bigint)
   OR   l.ExpiredWhen >= DATEADD(day, -1 * CAST(? AS int), SYSDATETIME())""",
    sqlParams=('watermarkFrom', 'expiryLookbackDays'),
    columns=(
        ('LoyaltyLedgerID', 'string'),
        ('LoyaltyMemberID', 'string'),
        ('CustomerID', 'string'),
        ('ProgramCode', 'string'),
        ('TierCode', 'string'),
        ('EntryTypeCode', 'string'),
        ('PointsDelta', 'string'),
        ('PointsBalanceAfter', 'string'),
        ('SourceInvoiceID', 'string'),
        ('RedemptionReference', 'string'),
        ('EntryWhen', 'string'),
        ('ExpiryDate', 'string'),
        ('RegionCode', 'string'),
        ('LastEditedWhen', 'string'),
    ),
    derivations=(
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveLoyaltyLedger',
    errorDisposition='FailComponent',
    watermarkObject='Loyalty.LoyaltyPointsLedger',
    clearSql='DELETE FROM raw.SqlLoyaltyLedger WHERE TRY_CONVERT(bigint, LoyaltyLedgerID) IN       (SELECT TRY_CONVERT(bigint, LoyaltyLedgerID) FROM raw.SqlLoyaltyLedger        WHERE TRY_CONVERT(datetime2(3), LastEditedWhen) >=              DATEADD(day, -1 * ?, SYSUTCDATETIME()));',
    extraVariables=(
        ('ExpiryLookbackDays', 7),
        ('SourceMaxLedgerId', 0),
    ),
    sourceTimeoutSeconds=3600,
    batchSize=100000,
))


CUSTOMER_TRANSACTIONS = _register(PackageSpec(
    name='EXT_SQL_CustomerTransactions',
    description='Numeric-key incremental over Sales.CustomerTransactions - the AR ledger side of the OLTP database - joined to the payment method and transaction type. Outstanding balances are carried so AR aging can be rebuilt in staging.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_bounded',
    legacyTarget='raw.SqlInvoice',
    targetTable='raw_sql_customer_transaction',
    keyColumns=('CustomerTransactionID',),
    sourceSql="""SELECT  ct.CustomerTransactionID,
        ct.CustomerID,
        ct.TransactionTypeID,
        tt.TransactionTypeName,
        ct.InvoiceID,
        ct.PaymentMethodID,
        pm.PaymentMethodName,
        ct.TransactionDate,
        ct.AmountExcludingTax,
        ct.TaxAmount,
        ct.TransactionAmount,
        ct.OutstandingBalance,
        ct.FinalizationDate,
        ct.LastEditedWhen
FROM    Sales.CustomerTransactions AS ct WITH (NOLOCK)
        INNER JOIN Application.TransactionTypes AS tt WITH (NOLOCK)
            ON tt.TransactionTypeID = ct.TransactionTypeID
        LEFT OUTER JOIN Application.PaymentMethods AS pm WITH (NOLOCK)
            ON pm.PaymentMethodID = ct.PaymentMethodID
WHERE   ct.CustomerTransactionID > ?
  AND   ct.CustomerTransactionID <= ?""",
    sqlParams=('watermarkFrom', 'watermarkTo'),
    columns=(
        ('CustomerTransactionID', 'int'),
        ('CustomerID', 'int'),
        ('TransactionTypeID', 'int'),
        ('TransactionTypeName', 'string'),
        ('InvoiceID', 'int'),
        ('PaymentMethodID', 'int'),
        ('PaymentMethodName', 'string'),
        ('TransactionDate', 'timestamp'),
        ('AmountExcludingTax', 'decimal(18,2)'),
        ('TaxAmount', 'decimal(18,2)'),
        ('TransactionAmount', 'decimal(18,2)'),
        ('OutstandingBalance', 'decimal(18,2)'),
        ('FinalizationDate', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        ('RecordKind', '"ARTRAN"', 'string'),
        ('SettledFlag', 'ISNULL(FinalizationDate) ? "N" : "Y"', 'string'),
        (
            'DaysOutstanding',
            'ISNULL(FinalizationDate) ? DATEDIFF("dd", TransactionDate, GETDATE()) : DATEDIFF("dd", TransactionDate, FinalizationDate)',
            'int',
        ),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveCustomerTransactions',
    errorDisposition='RedirectRow',
    watermarkObject='Sales.CustomerTransactions',
    maxKeySql='SELECT ISNULL(MAX(CustomerTransactionID), 0) AS MaxKey FROM Sales.CustomerTransactions WITH (NOLOCK)',
    keyColumn='CustomerTransactionID',
    recordKind='ARTRAN',
    rejectTarget='err.RejectedConstraintViolation',
    sourceTimeoutSeconds=3600,
    batchSize=150000,
))


SUPPLIER_TRANSACTIONS = _register(PackageSpec(
    name='EXT_SQL_SupplierTransactions',
    description='Numeric-key incremental over Purchasing.SupplierTransactions. This ledger overlaps the Oracle AP ledger for suppliers that were never migrated off the OLTP system, so the extract carries the supplier reference used by the downstream duplicate-payment check.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='numeric_key_open',
    legacyTarget='raw.SqlInvoice',
    targetTable='raw_sql_supplier_transaction',
    keyColumns=('SupplierTransactionID',),
    sourceSql="""SELECT  st.SupplierTransactionID,
        st.SupplierID,
        s.SupplierReference,
        st.TransactionTypeID,
        tt.TransactionTypeName,
        st.PurchaseOrderID,
        st.SupplierInvoiceNumber,
        st.TransactionDate,
        st.AmountExcludingTax,
        st.TaxAmount,
        st.TransactionAmount,
        st.OutstandingBalance,
        st.FinalizationDate,
        st.LastEditedWhen
FROM    Purchasing.SupplierTransactions AS st WITH (NOLOCK)
        INNER JOIN Purchasing.Suppliers AS s WITH (NOLOCK)
            ON s.SupplierID = st.SupplierID
        INNER JOIN Application.TransactionTypes AS tt WITH (NOLOCK)
            ON tt.TransactionTypeID = st.TransactionTypeID
WHERE   st.SupplierTransactionID > ?""",
    sqlParams=('watermarkFrom',),
    columns=(
        ('SupplierTransactionID', 'int'),
        ('SupplierID', 'int'),
        ('SupplierReference', 'string'),
        ('TransactionTypeID', 'int'),
        ('TransactionTypeName', 'string'),
        ('PurchaseOrderID', 'int'),
        ('SupplierInvoiceNumber', 'string'),
        ('TransactionDate', 'timestamp'),
        ('AmountExcludingTax', 'decimal(18,2)'),
        ('TaxAmount', 'decimal(18,2)'),
        ('TransactionAmount', 'decimal(18,2)'),
        ('OutstandingBalance', 'decimal(18,2)'),
        ('FinalizationDate', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        ('RecordKind', '"APTRAN"', 'string'),
        (
            'DuplicateCheckKey',
            'UPPER(TRIM(SupplierReference)) + "|" + UPPER(TRIM(SupplierInvoiceNumber))',
            'string',
        ),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveSupplierTransactions',
    errorDisposition='RedirectRow',
    watermarkObject='Purchasing.SupplierTransactions',
    recordKind='APTRAN',
    rejectTarget='err.RejectedConstraintViolation',
    extraVariables=(
        ('OverlapSupplierCount', 0),
    ),
    sourceTimeoutSeconds=2400,
    batchSize=75000,
))


PEOPLE = _register(PackageSpec(
    name='EXT_SQL_People',
    description='Full reload of Application.People restricted to the roles the warehouse needs (salespeople, employees, pickers). Login and photo columns are deliberately not extracted; the search name is carried for the customer-360 matcher.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='full_reload',
    legacyTarget='raw.SqlOrder',
    targetTable='raw_sql_person',
    keyColumns=('PersonID',),
    sourceSql="""SELECT  p.PersonID,
        p.FullName,
        p.PreferredName,
        p.SearchName,
        p.IsPermittedToLogon,
        p.IsEmployee,
        p.IsSalesperson,
        p.PhoneNumber,
        p.EmailAddress,
        p.EmployeeCode,
        p.ValidFrom,
        p.LastEditedWhen
FROM    Application.People AS p WITH (NOLOCK)
WHERE   p.IsEmployee = 1
   OR   p.IsSalesperson = 1""",
    sqlParams=(),
    columns=(
        ('PersonID', 'int'),
        ('FullName', 'string'),
        ('PreferredName', 'string'),
        ('SearchName', 'string'),
        ('IsPermittedToLogon', 'boolean'),
        ('IsEmployee', 'boolean'),
        ('IsSalesperson', 'boolean'),
        ('PhoneNumber', 'string'),
        ('EmailAddress', 'string'),
        ('EmployeeCode', 'string'),
        ('ValidFrom', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        ('RecordKind', '"PERSON"', 'string'),
        ('RoleCode', 'IsSalesperson ? "SALES" : "EMP"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='derivePeople',
    errorDisposition='FailComponent',
    recordKind='PERSON',
    clearSql="DELETE FROM raw.SqlOrder WHERE RecordKind = N'PERSON';",
    sourceTimeoutSeconds=600,
    batchSize=5000,
))


CITIES = _register(PackageSpec(
    name='EXT_SQL_Cities',
    description="Full truncate-and-load of Application.Cities joined to StateProvinces and Countries, landing into the shared raw.OracleGeography table so OLTP and ERP geography resolve through one lookup. Postal formatting follows the country's regional standard.",
    sourceSystemCode='WWI_OLTP',
    loadPattern='full_reload',
    legacyTarget='raw.OracleGeography',
    targetTable='raw_sql_city',
    keyColumns=('CityID',),
    sourceSql="""SELECT  c.CityID                AS GeographyKey,
        c.CityID,
        c.CityName,
        sp.StateProvinceCode,
        sp.StateProvinceName,
        co.IsoAlpha3Code        AS CountryCode,
        co.CountryName,
        co.Continent,
        co.Region,
        co.Subregion,
        c.LatestRecordedPopulation,
        sp.SalesTerritory
FROM    Application.Cities AS c WITH (NOLOCK)
        INNER JOIN Application.StateProvinces AS sp WITH (NOLOCK)
            ON sp.StateProvinceID = c.StateProvinceID
        INNER JOIN Application.Countries AS co WITH (NOLOCK)
            ON co.CountryID = sp.CountryID""",
    sqlParams=(),
    columns=(
        ('GeographyKey', 'int'),
        ('CityID', 'int'),
        ('CityName', 'string'),
        ('StateProvinceCode', 'string'),
        ('StateProvinceName', 'string'),
        ('CountryCode', 'string'),
        ('CountryName', 'string'),
        ('Continent', 'string'),
        ('Region', 'string'),
        ('Subregion', 'string'),
        ('LatestRecordedPopulation', 'int'),
        ('SalesTerritory', 'string'),
    ),
    derivations=(
        ('RecordKind', '"OLTPCITY"', 'string'),
        (
            'RegionCode',
            'Continent == "North America" ? "NA" : (Continent == "Europe" ? "EU" : (Continent == "Asia" || Continent == "Oceania" ? "APAC" : "ROW"))',
            'string',
        ),
        (
            'PostalFormatCode',
            'Continent == "North America" ? "ZIP5_PLUS4" : (Continent == "Europe" ? "ALPHANUM" : "NUMERIC6")',
            'string',
        ),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveCities',
    errorDisposition='RedirectRow',
    recordKind='OLTPCITY',
    rejectTarget='err.RejectedConstraintViolation',
    clearSql="DELETE FROM raw.OracleGeography WHERE RecordKind = N'OLTPCITY';",
    sourceTimeoutSeconds=900,
    batchSize=50000,
))


PAYMENT_METHODS = _register(PackageSpec(
    name='EXT_SQL_PaymentMethods',
    description='Reference refresh of Application.PaymentMethods with the regional availability map - direct debit and SEPA in EU, ACH and cheque in NA, local wallets in APAC - expressed as separate rows rather than one merged code list.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='full_reload',
    legacyTarget='raw.SqlInvoice',
    targetTable='raw_sql_payment_method',
    keyColumns=('PaymentMethodID', 'RegionCode'),
    sourceSql="""SELECT  pm.PaymentMethodID,
        pm.PaymentMethodName,
        pm.PaymentMethodCode,
        r.RegionCode,
        pm.SettlementTypeCode,
        pm.SettlementDays,
        pm.IsActive,
        pm.ValidFrom,
        pm.LastEditedWhen
FROM    Application.PaymentMethods AS pm WITH (NOLOCK)
        CROSS APPLY (
            SELECT N'NA'   AS RegionCode WHERE pm.PaymentMethodCode IN (N'ACH', N'CHEQUE', N'CARD')
            UNION ALL
            SELECT N'EU'   AS RegionCode WHERE pm.PaymentMethodCode IN (N'SEPA', N'DD', N'CARD')
            UNION ALL
            SELECT N'APAC' AS RegionCode WHERE pm.PaymentMethodCode IN (N'WALLET', N'BANKXFER', N'CARD')
        ) AS r""",
    sqlParams=(),
    columns=(
        ('PaymentMethodID', 'int'),
        ('PaymentMethodName', 'string'),
        ('PaymentMethodCode', 'string'),
        ('RegionCode', 'string'),
        ('SettlementTypeCode', 'string'),
        ('SettlementDays', 'int'),
        ('IsActive', 'boolean'),
        ('ValidFrom', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        ('RecordKind', '"PAYMETHOD"', 'string'),
        ('ImmediateSettlementFlag', 'SettlementDays == 0 ? "Y" : "N"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='derivePaymentMethods',
    errorDisposition='FailComponent',
    recordKind='PAYMETHOD',
    clearSql="DELETE FROM raw.SqlInvoice WHERE RecordKind = N'PAYMETHOD';",
    sourceTimeoutSeconds=300,
    batchSize=1000,
))


TRANSACTION_TYPES = _register(PackageSpec(
    name='EXT_SQL_TransactionTypes',
    description='Reference refresh of Application.TransactionTypes with the GL posting hint the finance mart needs. Twenty-odd rows, reloaded whole; it runs first in the nightly batch because several extracts join to it.',
    sourceSystemCode='WWI_OLTP',
    loadPattern='full_reload',
    legacyTarget='raw.SqlInvoice',
    targetTable='raw_sql_transaction_type',
    keyColumns=('TransactionTypeID',),
    sourceSql="""SELECT  tt.TransactionTypeID,
        tt.TransactionTypeName,
        tt.TransactionTypeCode,
        CASE WHEN tt.TransactionTypeName LIKE N'%Credit%' THEN N'CR' ELSE N'DR' END AS LedgerSideCode,
        tt.GlPostingHintCode,
        CASE WHEN tt.TransactionTypeName LIKE N'%Reversal%' THEN 1 ELSE 0 END       AS IsReversal,
        tt.ValidFrom,
        tt.LastEditedWhen
FROM    Application.TransactionTypes AS tt WITH (NOLOCK)""",
    sqlParams=(),
    columns=(
        ('TransactionTypeID', 'int'),
        ('TransactionTypeName', 'string'),
        ('TransactionTypeCode', 'string'),
        ('LedgerSideCode', 'string'),
        ('GlPostingHintCode', 'string'),
        ('IsReversal', 'boolean'),
        ('ValidFrom', 'timestamp'),
        ('LastEditedWhen', 'timestamp'),
    ),
    derivations=(
        ('RecordKind', '"TRANTYPE"', 'string'),
        ('SourceSystemCode', '"WWI_OLTP"', 'string'),
        ('ExtractedAtUtc', 'GETUTCDATE()', 'timestamp'),
        ('PackageExecutionId', '@[User::PackageExecutionId]', 'bigint'),
    ),
    transform='deriveTransactionTypes',
    errorDisposition='FailComponent',
    recordKind='TRANTYPE',
    clearSql="DELETE FROM raw.SqlInvoice WHERE RecordKind = N'TRANTYPE';",
    extraRowCountObjects=('Application.TransactionTypes',),
    sourceTimeoutSeconds=120,
    batchSize=500,
))


PACKAGE_ORDER: List[str] = list(PACKAGES)


def getPackage(name: str) -> PackageSpec:
    try:
        return PACKAGES[name]
    except KeyError:
        raise KeyError("Unknown package %s; known: %s" % (name, ", ".join(PACKAGES)))
