"""02_sqlserver_extract: EXT_SQL_Orders / OrderLines / Invoices / InvoiceLines / CustomerTransactions.

Load type incremental_key: watermark on the surrogate id (etl.usp_GetWatermark, WatermarkType=NumericKey),
source read `WHERE id > @from AND id <= @to`, landed into raw_* Delta tables (MERGE on the id so reruns are
idempotent), then the delete feed (SSIS: CHANGETABLE(CHANGES ...) which federation cannot express; we read the
OLTP's own delete log Integration.vw_DeletedKeysForExtract) flags deleted rows with delete_flag = 'Y'.
"""
from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_o2c.config import OLTP_CATALOG, SOURCE_SYSTEM_CODE, RunContext
from sales_o2c.tables import mergeUpsert, tableExists, withAudit
from sales_o2c.watermark import NUMERIC_KEY, getWatermark, setWatermark

OLTP = OLTP_CATALOG


# ----------------------------------------------------------------------------------------------
# Pure derived-column rules (SSIS Derived Column components), unit-tested on local Spark
# ----------------------------------------------------------------------------------------------
def deriveOrder(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("backorder_flag", F.when(F.col("backorder_order_id").isNull(), "N").otherwise("Y"))
        .withColumn(
            "pick_cycle_hours",
            F.when(F.col("picking_completed_when").isNull(), F.lit(-1)).otherwise(
                (F.unix_timestamp("picking_completed_when") - F.unix_timestamp("order_date")) / 3600
            ).cast("int"),
        )
        .withColumn("delete_flag", F.lit("N"))
    )


def deriveOrderLine(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "net_line_amount", (F.col("extended_price") - F.col("line_discount_amount")).cast("decimal(18,2)")
    ).withColumn("short_pick_flag", F.when(F.col("picked_quantity") < F.col("quantity"), "Y").otherwise("N"))


def deriveInvoice(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "effective_tax_rate",
        F.when(F.col("total_excluding_tax") == 0, F.lit(0.0)).otherwise(
            F.col("total_tax_amount") / F.col("total_excluding_tax")
        ).cast("decimal(18,6)"),
    ).withColumn(
        "signed_total_including_tax",
        F.when(F.col("is_credit_note"), -F.col("total_including_tax")).otherwise(F.col("total_including_tax")).cast("decimal(18,2)"),
    )


def deriveInvoiceLine(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "gross_margin_pct",
        F.when(F.col("extended_price") == 0, F.lit(0.0)).otherwise(F.col("line_profit") / F.col("extended_price")).cast("decimal(18,6)"),
    ).withColumn("negative_margin_flag", F.when(F.col("line_profit") < 0, "Y").otherwise("N"))


def deriveCustomerTransaction(df: DataFrame, asOf=None) -> DataFrame:
    today = F.lit(asOf).cast("date") if asOf is not None else F.current_date()
    return (
        df.withColumn("record_kind", F.lit("ARTRAN"))
        .withColumn("settled_flag", F.when(F.col("finalization_date").isNull(), "N").otherwise("Y"))
        .withColumn(
            "days_outstanding",
            F.when(F.col("finalization_date").isNull(), F.datediff(today, F.col("transaction_date"))).otherwise(
                F.datediff(F.col("finalization_date"), F.col("transaction_date"))
            ),
        )
    )


# ----------------------------------------------------------------------------------------------
# Source queries (the OLE DB Source SQL of each package, federated; ? -> watermark bounds)
# ----------------------------------------------------------------------------------------------
def ordersSql(lo: int, hi: int) -> str:
    return f"""
    SELECT o.OrderID AS order_id, o.CustomerID AS customer_id, o.SalespersonPersonID AS salesperson_person_id,
           o.PickedByPersonID AS picked_by_person_id, o.ContactPersonID AS contact_person_id,
           o.BackorderOrderID AS backorder_order_id, CAST(o.OrderDate AS date) AS order_date,
           CAST(o.ExpectedDeliveryDate AS date) AS expected_delivery_date,
           o.CustomerPurchaseOrderNumber AS customer_purchase_order_number,
           o.IsUndersupplyBackordered AS is_undersupply_backordered, o.Comments AS comments,
           o.DeliveryInstructions AS delivery_instructions, c.DeliveryCityID AS delivery_city_id, st.SalesTerritoryID AS sales_territory_id,
           st.TerritoryCode AS sales_territory_code, st.RegionCode AS region_code,
           COALESCE(sc.ChannelCode, 'DIRECT') AS sales_channel_code, o.OrderStatusCode AS order_status_code,
           o.CurrencyCode AS currency_code, o.PickingCompletedWhen AS picking_completed_when,
           o.LastEditedWhen AS last_edited_when, o.LastEditedBy AS last_edited_by
    FROM {OLTP}.Sales.Orders o
    LEFT JOIN {OLTP}.Sales.Customers c ON c.CustomerID = o.CustomerID
    LEFT JOIN {OLTP}.Sales.SalesTerritories st ON st.SalesTerritoryID = o.SalesTerritoryID
    LEFT JOIN {OLTP}.Sales.SalesChannels sc ON sc.SalesChannelID = o.SalesChannelID
    WHERE o.OrderID > {lo} AND o.OrderID <= {hi}
    """


def orderLinesSql(lo: int, hi: int) -> str:
    return f"""
    SELECT ol.OrderLineID AS order_line_id, ol.OrderID AS order_id, ol.StockItemID AS stock_item_id,
           ol.LineDescription AS description, pt.PackageTypeName AS package_type_name, ol.Quantity AS quantity,
           ol.UnitPrice AS unit_price, ol.TaxRate AS tax_rate,
           CAST(ol.Quantity * ol.UnitPrice * (1.0 + ol.TaxRate / 100.0) AS decimal(18,2)) AS extended_price,
           CAST(COALESCE(ol.DiscountAmount, 0.00) AS decimal(18,2)) AS line_discount_amount,
           p.PromotionCode AS promotion_code, ol.DiscountPercent AS line_discount_percent,
           ol.LineStatusCode AS line_status_code, base.PickedQuantity AS picked_quantity,
           base.PickingCompletedWhen AS picking_completed_when, ol.ChangedWhen AS last_edited_when
    FROM {OLTP}.Sales.vw_OrderLineExtract ol
    JOIN {OLTP}.Sales.OrderLines base ON base.OrderLineID = ol.OrderLineID
    LEFT JOIN {OLTP}.Warehouse.PackageTypes pt ON pt.PackageTypeID = base.PackageTypeID
    LEFT JOIN {OLTP}.Sales.Promotions p ON p.PromotionID = ol.PromotionID
    WHERE ol.OrderLineID > {lo} AND ol.OrderLineID <= {hi}
    """


def invoicesSql(lo: int, hi: int) -> str:
    return f"""
    SELECT i.InvoiceID AS invoice_id, i.CustomerID AS customer_id, i.BillToCustomerID AS bill_to_customer_id,
           i.OrderID AS order_id, i.DeliveryMethodID AS delivery_method_id, dm.DeliveryMethodName AS delivery_method_name,
           i.ContactPersonID AS contact_person_id, i.AccountsPersonID AS accounts_person_id,
           i.SalespersonPersonID AS salesperson_person_id, i.PackedByPersonID AS packed_by_person_id,
           CAST(i.InvoiceDate AS date) AS invoice_date, i.CustomerPurchaseOrderNumber AS customer_purchase_order_number,
           i.IsCreditNote AS is_credit_note, i.CreditNoteReason AS credit_note_reason,
           CAST(COALESCE(v.InvoiceTotalExTax, 0) AS decimal(18,2)) AS total_excluding_tax,
           CAST(COALESCE(v.InvoiceTaxAmount, 0) AS decimal(18,2)) AS total_tax_amount,
           CAST(COALESCE(v.InvoiceTotalIncTax, 0) AS decimal(18,2)) AS total_including_tax,
           v.RegionCode AS region_code, bc.RegionCode AS bill_to_region_code, c.DeliveryCityID AS delivery_city_id,
           CASE v.RegionCode WHEN 'NA' THEN 'SALESTAX' WHEN 'EU' THEN 'VAT' WHEN 'APAC' THEN 'GST' ELSE 'NONE' END AS tax_treatment_code,
           CASE WHEN v.RegionCode = 'EU' THEN v.CustomerTaxNumber END AS customer_tax_registration_number,
           v.CurrencyCode AS currency_code, v.ExchangeRateToUsd AS conversion_rate,
           i.TotalDryItems AS total_dry_items, i.TotalChillerItems AS total_chiller_items,
           i.DeliveryRun AS delivery_run, i.RunPosition AS run_position,
           i.ConfirmedDeliveryTime AS confirmed_delivery_time, i.ConfirmedReceivedBy AS confirmed_received_by,
           i.LastEditedWhen AS last_edited_when, i.LastEditedBy AS last_edited_by
    FROM {OLTP}.Sales.Invoices i
    JOIN {OLTP}.Sales.vw_InvoiceExtract v ON v.InvoiceID = i.InvoiceID
    LEFT JOIN {OLTP}.Sales.Customers c ON c.CustomerID = i.CustomerID
    LEFT JOIN {OLTP}.Sales.Customers bc ON bc.CustomerID = i.BillToCustomerID
    LEFT JOIN {OLTP}.Application.DeliveryMethods dm ON dm.DeliveryMethodID = i.DeliveryMethodID
    WHERE i.InvoiceID > {lo} AND i.InvoiceID <= {hi}
    """


def invoiceLinesSql(lo: int, hi: int) -> str:
    return f"""
    SELECT il.InvoiceLineID AS invoice_line_id, il.InvoiceID AS invoice_id, il.StockItemID AS stock_item_id,
           il.Description AS description, il.PackageTypeID AS package_type_id, pt.PackageTypeName AS package_type_name,
           il.Quantity AS quantity, il.UnitPrice AS unit_price, il.TaxRate AS tax_rate, il.TaxAmount AS tax_amount,
           il.LineProfit AS line_profit, il.ExtendedPrice AS extended_price,
           sh.LastCostPrice AS last_cost_price, si.IsChillerStock AS is_chiller_stock,
           il.LastEditedWhen AS last_edited_when, il.LastEditedBy AS last_edited_by
    FROM {OLTP}.Sales.InvoiceLines il
    JOIN {OLTP}.Warehouse.StockItems si ON si.StockItemID = il.StockItemID
    LEFT JOIN {OLTP}.Warehouse.StockItemHoldings sh ON sh.StockItemID = il.StockItemID
    LEFT JOIN {OLTP}.Warehouse.PackageTypes pt ON pt.PackageTypeID = il.PackageTypeID
    WHERE il.InvoiceLineID > {lo} AND il.InvoiceLineID <= {hi}
    """


def customerTransactionsSql(lo: int, hi: int) -> str:
    return f"""
    SELECT ct.CustomerTransactionID AS customer_transaction_id, ct.CustomerID AS customer_id,
           ct.TransactionTypeID AS transaction_type_id, tt.TransactionTypeName AS transaction_type_name,
           ct.InvoiceID AS invoice_id, ct.PaymentMethodID AS payment_method_id, pm.PaymentMethodName AS payment_method_name,
           CAST(ct.TransactionDate AS date) AS transaction_date, ct.AmountExcludingTax AS amount_excluding_tax,
           ct.TaxAmount AS tax_amount, ct.TransactionAmount AS transaction_amount,
           ct.OutstandingBalance AS outstanding_balance, CAST(ct.FinalizationDate AS date) AS finalization_date,
           ct.IsFinalized AS is_finalized, ct.LastEditedWhen AS last_edited_when, ct.LastEditedBy AS last_edited_by
    FROM {OLTP}.Sales.CustomerTransactions ct
    JOIN {OLTP}.Application.TransactionTypes tt ON tt.TransactionTypeID = ct.TransactionTypeID
    LEFT JOIN {OLTP}.Application.PaymentMethods pm ON pm.PaymentMethodID = ct.PaymentMethodID
    WHERE ct.CustomerTransactionID > {lo} AND ct.CustomerTransactionID <= {hi}
    """


@dataclass(frozen=True)
class ExtractSpec:
    packageName: str
    objectName: str  # legacy watermark ObjectName (etl.Watermark.ObjectName)
    sourceTable: str  # 3-level federated table used for the max-key probe
    keyColumn: str  # source column name
    targetTable: str
    targetKey: str
    sql: Callable[[int, int], str]
    derive: Callable[[DataFrame], DataFrame]
    deleteLogTable: str | None = None  # SourceTableName in Integration.vw_DeletedKeysForExtract
    extraCounters: dict = field(default_factory=dict)


SPECS = {
    "EXT_SQL_Orders": ExtractSpec(
        "EXT_SQL_Orders", "Sales.Orders", f"{OLTP}.Sales.Orders", "OrderID", "raw_sql_order", "order_id", ordersSql, deriveOrder, "Orders"
    ),
    "EXT_SQL_OrderLines": ExtractSpec(
        "EXT_SQL_OrderLines", "Sales.OrderLines", f"{OLTP}.Sales.OrderLines", "OrderLineID", "raw_sql_order_line", "order_line_id", orderLinesSql, deriveOrderLine, "OrderLines"
    ),
    "EXT_SQL_Invoices": ExtractSpec(
        "EXT_SQL_Invoices", "Sales.Invoices", f"{OLTP}.Sales.Invoices", "InvoiceID", "raw_sql_invoice", "invoice_id", invoicesSql, deriveInvoice, "Invoices"
    ),
    "EXT_SQL_InvoiceLines": ExtractSpec(
        "EXT_SQL_InvoiceLines", "Sales.InvoiceLines", f"{OLTP}.Sales.InvoiceLines", "InvoiceLineID", "raw_sql_invoice_line", "invoice_line_id", invoiceLinesSql, deriveInvoiceLine, "InvoiceLines"
    ),
    # Inventory/package target is raw.SqlInvoice (generator bug: no raw.SqlCustomerTransaction exists);
    # we land it in its own table and document the deviation in README.
    "EXT_SQL_CustomerTransactions": ExtractSpec(
        "EXT_SQL_CustomerTransactions", "Sales.CustomerTransactions", f"{OLTP}.Sales.CustomerTransactions", "CustomerTransactionID", "raw_sql_customer_transaction", "customer_transaction_id", customerTransactionsSql, deriveCustomerTransaction, "CustomerTransactions"
    ),
}


def applyDeleteFeed(spark: SparkSession, ctx: RunContext, spec: ExtractSpec) -> int:
    """Rows whose key appears in the OLTP delete log are kept in raw with delete_flag = 'Y' (SSIS delete branch)."""
    target = ctx.table(spec.targetTable)
    if not spec.deleteLogTable or not tableExists(spark, target):
        return 0
    deleted = spark.sql(
        f"""
        SELECT DISTINCT CAST(SourceKeyValue AS bigint) AS k
        FROM {OLTP}.Integration.vw_DeletedKeysForExtract
        WHERE SourceSchemaName = 'Sales' AND SourceTableName = '{spec.deleteLogTable}'
        """
    )
    deleted.createOrReplaceTempView("deleted_keys")
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {target} t JOIN deleted_keys d ON d.k = t.`{spec.targetKey}` WHERE t.delete_flag = 'N'").first()["n"]
    if n:
        spark.sql(f"UPDATE {target} SET delete_flag = 'Y' WHERE `{spec.targetKey}` IN (SELECT k FROM deleted_keys)")
    return int(n)


def runExtract(spark: SparkSession, ctx: RunContext, packageName: str) -> dict:
    spec = SPECS[packageName]
    lo = int(getWatermark(spark, ctx, spec.objectName, NUMERIC_KEY))
    hi = spark.sql(f"SELECT COALESCE(MAX({spec.keyColumn}), 0) AS m FROM {spec.sourceTable}").first()["m"]
    hi = int(hi or 0)
    rowsRead = rowsInserted = 0
    if hi > lo:
        src = spark.sql(spec.sql(lo, hi))
        df = withAudit(spec.derive(src), ctx).withColumn("source_system_code", F.lit(SOURCE_SYSTEM_CODE))
        if "delete_flag" not in df.columns:
            df = df.withColumn("delete_flag", F.lit("N"))
        rowsRead = mergeUpsert(spark, ctx.table(spec.targetTable), df, [spec.targetKey])
        rowsInserted = rowsRead
    rowsDeleted = applyDeleteFeed(spark, ctx, spec)
    setWatermark(spark, ctx, spec.objectName, NUMERIC_KEY, max(hi, lo))
    return {"rowsRead": rowsRead, "rowsInserted": rowsInserted, "rowsDeleted": rowsDeleted, "watermarkFrom": lo, "watermarkTo": hi}
