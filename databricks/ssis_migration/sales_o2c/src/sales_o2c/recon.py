"""Reconciliation evidence for the 18 packages -> otterorders_migration.evidence.recon_results (append-only).

Every package gets a row_count and an order-independent checksum check (SUM(CAST(xxhash64(business columns) AS DECIMAL(38,0))), no bigint overflow).
Populated legacy targets (Fact.Sale / Fact.Order / Fact.Transaction) are compared directly -> PASS only when both
match. Empty legacy targets are compared with an expectation derived from the legacy source with the package's
own rules ("baseline":"source_derived") -> at best PARTIAL. Zero-row regions (EU/APAC) are PARTIAL too.
"""
import json
import uuid
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_o2c.config import ACTOR, BRANCH, DW_CATALOG, HARNESS_VERSION, OLTP_CATALOG, PACKAGES, RunContext
from sales_o2c.tables import tableExists

OLTP, DW = OLTP_CATALOG, DW_CATALOG
LEGACY_DW, LEGACY_STG = "WideWorldImportersDW", "WideWorldImporters_Staging"


def checksum(df: DataFrame, exprs) -> str | None:
    """Order-independent checksum over already string-normalised expressions."""
    if df is None:
        return None
    v = df.select(F.sum(F.xxhash64(F.concat_ws("|", *exprs)).cast("decimal(38,0)")).alias("c")).first()["c"]
    return None if v is None else str(v)


def s(col):  # string
    return F.coalesce(F.col(col).cast("string"), F.lit(""))


def i(col):  # integer-like (ints, bits, booleans)
    return F.coalesce(F.col(col).cast("bigint").cast("string"), F.lit(""))


def d2(col):  # money
    return F.coalesce(F.col(col).cast("decimal(18,2)").cast("string"), F.lit(""))


def d3(col):  # rates
    return F.coalesce(F.col(col).cast("decimal(18,3)").cast("string"), F.lit(""))


def dt(col):  # dates
    return F.coalesce(F.date_format(F.col(col).cast("date"), "yyyy-MM-dd"), F.lit(""))


def _rowCount(df: DataFrame | None) -> int:
    return 0 if df is None else df.count()


def _target(spark: SparkSession, ctx: RunContext, name: str) -> DataFrame | None:
    full = ctx.table(name)
    return spark.table(full) if tableExists(spark, full) else None


def _latestBatch(df: DataFrame | None) -> DataFrame | None:
    if df is None:
        return None
    mx = df.agg(F.max("batch_id")).first()[0]
    return df.filter(F.col("batch_id") == mx) if mx is not None else df.limit(0)


def compare(sourceDf, targetDf, sourceExprs, targetExprs, baseline: str, method: str):
    sc, tc = _rowCount(sourceDf), _rowCount(targetDf)
    sHash, tHash = checksum(sourceDf, sourceExprs), checksum(targetDf, targetExprs)
    checks = [
        {"check": "row_count", "source": sc, "target": tc, "pass": sc == tc, "baseline": baseline},
        {"check": "checksum", "method": f"sum(cast(xxhash64({method}) as decimal(38,0)))", "source": sHash, "target": tHash, "pass": sHash == tHash, "baseline": baseline},
    ]
    return checks, sc, tc


def verdictFor(checks, baseline: str, rows: int) -> tuple[str, str]:
    allPass = all(c.get("pass") for c in checks if "pass" in c)
    if not allPass:
        return "FAIL", "row_count/checksum mismatch against " + ("legacy target" if baseline == "legacy_target" else "source-derived expectation")
    if baseline != "legacy_target":
        return "PARTIAL", "legacy target is empty on the host; matched an expectation derived from the legacy source with the package's rules"
    if rows == 0:
        return "PARTIAL", "no rows for this slice in the baseline (0 = 0); logic covered by unit tests only"
    return "PASS", "row count and checksum match the populated legacy target"


# ------------------------------------------------------------------ per-package specs
def _extract(spark, ctx, sourceSql, sourceExprs, targetTable, targetExprs, legacyRaw):
    src = spark.sql(sourceSql)
    tgt = _target(spark, ctx, targetTable)
    checks, sc, tc = compare(src, tgt, sourceExprs, targetExprs, "source_derived", "business cols of the OLTP source row")
    legacyCount = spark.table(f"wwi_legacy_staging.raw.{legacyRaw}").count()
    checks.append({"check": "legacy_target_row_count", "note": "legacy raw.* holds a 3k-row synthetic sample unrelated to the OLTP key space", "source": legacyCount, "target": tc})
    v, why = verdictFor(checks, "source_derived", tc)
    if v == "PARTIAL":
        why = f"legacy raw.{legacyRaw} ({legacyCount} rows) is a synthetic sample, not an extract of the OLTP; full OLTP extract matched source row-for-row (source_derived)"
    return checks, v, why, f"{LEGACY_STG}.raw.{legacyRaw}", ctx.table(targetTable)


def reconExtOrders(spark, ctx):
    return _extract(spark, ctx, f"SELECT OrderID, CustomerID, OrderDate, ExpectedDeliveryDate, SalespersonPersonID FROM {OLTP}.Sales.Orders",
                    [i("OrderID"), i("CustomerID"), dt("OrderDate"), dt("ExpectedDeliveryDate"), i("SalespersonPersonID")],
                    "raw_sql_order", [i("order_id"), i("customer_id"), dt("order_date"), dt("expected_delivery_date"), i("salesperson_person_id")], "SqlOrder")


def reconExtOrderLines(spark, ctx):
    return _extract(spark, ctx, f"SELECT OrderLineID, OrderID, StockItemID, Quantity, UnitPrice, TaxRate FROM {OLTP}.Sales.OrderLines",
                    [i("OrderLineID"), i("OrderID"), i("StockItemID"), i("Quantity"), d2("UnitPrice"), d3("TaxRate")],
                    "raw_sql_order_line", [i("order_line_id"), i("order_id"), i("stock_item_id"), i("quantity"), d2("unit_price"), d3("tax_rate")], "SqlOrderLine")


def reconExtInvoices(spark, ctx):
    return _extract(spark, ctx, f"SELECT InvoiceID, CustomerID, BillToCustomerID, InvoiceDate, IsCreditNote FROM {OLTP}.Sales.Invoices",
                    [i("InvoiceID"), i("CustomerID"), i("BillToCustomerID"), dt("InvoiceDate"), i("IsCreditNote")],
                    "raw_sql_invoice", [i("invoice_id"), i("customer_id"), i("bill_to_customer_id"), dt("invoice_date"), i("is_credit_note")], "SqlInvoice")


def reconExtInvoiceLines(spark, ctx):
    return _extract(spark, ctx, f"SELECT InvoiceLineID, InvoiceID, StockItemID, Quantity, UnitPrice, TaxRate, TaxAmount, LineProfit, ExtendedPrice FROM {OLTP}.Sales.InvoiceLines",
                    [i("InvoiceLineID"), i("InvoiceID"), i("StockItemID"), i("Quantity"), d2("UnitPrice"), d3("TaxRate"), d2("TaxAmount"), d2("LineProfit"), d2("ExtendedPrice")],
                    "raw_sql_invoice_line", [i("invoice_line_id"), i("invoice_id"), i("stock_item_id"), i("quantity"), d2("unit_price"), d3("tax_rate"), d2("tax_amount"), d2("line_profit"), d2("extended_price")], "SqlInvoiceLine")


def reconExtCustomerTransactions(spark, ctx):
    checks, v, why, src, tgt = _extract(
        spark, ctx, f"SELECT CustomerTransactionID, CustomerID, TransactionTypeID, TransactionDate, TransactionAmount, OutstandingBalance FROM {OLTP}.Sales.CustomerTransactions",
        [i("CustomerTransactionID"), i("CustomerID"), i("TransactionTypeID"), dt("TransactionDate"), d2("TransactionAmount"), d2("OutstandingBalance")],
        "raw_sql_customer_transaction", [i("customer_transaction_id"), i("customer_id"), i("transaction_type_id"), dt("transaction_date"), d2("transaction_amount"), d2("outstanding_balance")], "SqlInvoice")
    why += "; inventory declares raw.SqlInvoice as destination (generator defect) - landed in raw_sql_customer_transaction instead"
    return checks, v, why, src, tgt


def reconStgLoadOrder(spark, ctx):
    srcOrders = spark.sql(f"SELECT OrderID, CustomerID, OrderDate FROM {OLTP}.Sales.Orders")
    tgtOrders = _target(spark, ctx, "stg_order")
    checks, sc, tc = compare(srcOrders, tgtOrders, [i("OrderID"), i("CustomerID"), dt("OrderDate")], [i("order_id"), i("customer_id"), dt("order_date")], "source_derived", "order_id, customer_id, order_date")
    srcLines = spark.sql(f"SELECT ol.OrderLineID, ol.OrderID, ol.StockItemID, ol.Quantity FROM {OLTP}.Sales.OrderLines ol JOIN {OLTP}.Sales.Orders o ON o.OrderID = ol.OrderID WHERE ol.Quantity IS NOT NULL AND ol.Quantity >= 0 AND ol.UnitPrice IS NOT NULL")
    tgtLines = _target(spark, ctx, "stg_order_line")
    c2, _, _ = compare(srcLines, tgtLines, [i("OrderLineID"), i("OrderID"), i("StockItemID"), i("Quantity")], [i("order_line_id"), i("order_id"), i("stock_item_id"), i("ordered_quantity")], "source_derived", "order_line_id, order_id, stock_item_id, quantity")
    for c in c2:
        c["object"] = "stg.OrderLine"
    checks += c2
    v, why = verdictFor(checks, "source_derived", tc)
    return checks, v, why, f"{LEGACY_STG}.stg.Order;{LEGACY_STG}.stg.OrderLine", f"{ctx.table('stg_order')};{ctx.table('stg_order_line')}"


def reconStgLoadSale(spark, ctx):
    srcInv = spark.sql(f"SELECT InvoiceID, CustomerID, InvoiceDate FROM {OLTP}.Sales.Invoices")
    tgtInv = _target(spark, ctx, "stg_sale")
    checks, sc, tc = compare(srcInv, tgtInv, [i("InvoiceID"), i("CustomerID"), dt("InvoiceDate")], [i("invoice_id"), i("customer_id"), dt("invoice_date")], "source_derived", "invoice_id, customer_id, invoice_date")
    srcLines = spark.sql(
        f"SELECT InvoiceLineID, InvoiceID, StockItemID, Quantity, UnitPrice FROM {OLTP}.Sales.InvoiceLines "
        f"WHERE Quantity <> 0 AND ABS(COALESCE(TaxAmount,0) - CAST(Quantity * UnitPrice * COALESCE(TaxRate,0) / 100 AS decimal(18,2))) <= 0.02"
    )
    tgtLines = _target(spark, ctx, "stg_sale_line")
    c2, _, _ = compare(srcLines, tgtLines, [i("InvoiceLineID"), i("InvoiceID"), i("StockItemID"), i("Quantity"), d2("UnitPrice")], [i("invoice_line_id"), i("invoice_id"), i("stock_item_id"), i("quantity"), d2("unit_price_amount")], "source_derived", "invoice_line_id, invoice_id, stock_item_id, quantity, unit_price")
    for c in c2:
        c["object"] = "stg.SaleLine"
    checks += c2
    v, why = verdictFor(checks, "source_derived", tc)
    return checks, v, why, f"{LEGACY_STG}.stg.Sale;{LEGACY_STG}.stg.SaleLine", f"{ctx.table('stg_sale')};{ctx.table('stg_sale_line')}"


def reconDqOrderLine(spark, ctx):
    src = spark.sql(f"SELECT CONCAT('WWI_OLTP|', OrderLineID) AS k FROM {OLTP}.Sales.OrderLines WHERE Quantity <= 0 OR Quantity > 10000 OR UnitPrice > 250000")
    tgt = _latestBatch(_target(spark, ctx, "err_rejected_order_line"))
    tgt = tgt.filter(F.col("reject_stage") == "Quality").select(F.col("order_line_business_key").alias("k")) if tgt is not None else None
    checks, sc, tc = compare(src, tgt, [s("k")], [s("k")], "source_derived", "rejected order_line_business_key")
    stg = _target(spark, ctx, "stg_order_line")
    passed = stg.filter(F.col("dq_status_code") == "PASS").count() if stg is not None and "dq_status_code" in stg.columns else 0
    checks.append({"check": "dq_pass_rows", "target": passed, "note": "stg_order_line rows stamped PASS"})
    v, why = verdictFor(checks, "source_derived", tc)
    return checks, v, why, f"{LEGACY_STG}.err.RejectedOrderLine", ctx.table("err_rejected_order_line")


def reconDqInvoiceLine(spark, ctx):
    src = spark.sql(
        f"SELECT CONCAT('WWI_OLTP|', InvoiceLineID) AS k FROM {OLTP}.Sales.InvoiceLines WHERE Quantity <> 0 "
        f"AND ABS(COALESCE(TaxAmount,0) - CAST(Quantity * UnitPrice * COALESCE(TaxRate,0) / 100 AS decimal(18,2))) > 0.02"
    )
    tgt = _latestBatch(_target(spark, ctx, "err_rejected_invoice_line"))
    tgt = tgt.filter(F.col("reject_stage") == "Quality").select(F.col("invoice_line_business_key").alias("k")) if tgt is not None else None
    checks, sc, tc = compare(src, tgt, [s("k")], [s("k")], "source_derived", "rejected invoice_line_business_key")
    stg = _target(spark, ctx, "stg_sale_line")
    passed = stg.filter(F.col("dq_status_code") == "PASS").count() if stg is not None and "dq_status_code" in stg.columns else 0
    checks.append({"check": "dq_pass_rows", "target": passed, "note": "stg_sale_line rows stamped PASS"})
    v, why = verdictFor(checks, "source_derived", tc)
    return checks, v, why, f"{LEGACY_STG}.err.RejectedInvoiceLine", ctx.table("err_rejected_invoice_line")


SALE_LEGACY = [i("City Key"), i("Customer Key"), i("Bill To Customer Key"), i("Stock Item Key"), dt("Invoice Date Key"), dt("Delivery Date Key"), i("Salesperson Key"), i("WWI Invoice ID"), s("Description"), s("Package"), i("Quantity"), d2("Unit Price"), d3("Tax Rate"), d2("Total Excluding Tax"), d2("Tax Amount"), d2("Profit"), d2("Total Including Tax"), i("Total Dry Items"), i("Total Chiller Items")]
SALE_TARGET = [i("city_key"), i("customer_key"), i("bill_to_customer_key"), i("stock_item_key"), dt("invoice_date_key"), dt("delivery_date_key"), i("salesperson_key"), i("wwi_invoice_id"), s("description"), s("package"), i("quantity"), d2("unit_price"), d3("tax_rate"), d2("total_excluding_tax"), d2("tax_amount"), d2("profit"), d2("total_including_tax"), i("total_dry_items"), i("total_chiller_items")]
SALE_METHOD = "city_key, customer_key, bill_to_customer_key, stock_item_key, invoice_date_key, delivery_date_key, salesperson_key, wwi_invoice_id, description, package, quantity, unit_price, tax_rate, total_excluding_tax, tax_amount, profit, total_including_tax, total_dry_items, total_chiller_items"


def _legacySale(spark):
    return spark.table(f"{DW}.Fact.Sale")


def _reconSaleRegion(spark, ctx, region):
    tgt = _target(spark, ctx, "gold_fact_sale")
    tgt = tgt.filter(F.col("region_code") == region) if tgt is not None else None
    # legacy Fact.Sale carries no region; every legacy row derives from the NA default (RegionCode null -> NA)
    src = _legacySale(spark) if region == "NA" else _legacySale(spark).limit(0)
    checks, sc, tc = compare(src, tgt, SALE_LEGACY, SALE_TARGET, "legacy_target", SALE_METHOD)
    srcRegion = spark.sql(f"SELECT COUNT(*) AS n FROM {OLTP}.Sales.Invoices i LEFT JOIN {OLTP}.Sales.Customers c ON c.CustomerID = i.BillToCustomerID WHERE UPPER(TRIM(COALESCE(c.RegionCode, 'NA'))) = '{region}'").first()["n"]
    checks.append({"check": "source_region_invoice_count", "region": region, "source": srcRegion, "note": "OLTP invoices whose bill-to region resolves to this package's region"})
    v, why = verdictFor(checks, "legacy_target", tc)
    if region != "NA" and tc == 0:
        why = f"OLTP has no {region} invoices (Customers.RegionCode is null on the host -> every sale defaults to NA); regional tax/FX rules are covered by unit tests"
    return checks, v, why, f"{LEGACY_DW}.Fact.Sale", ctx.table("gold_fact_sale")


def reconFactNaSale(spark, ctx):
    return _reconSaleRegion(spark, ctx, "NA")


def reconFactEuSale(spark, ctx):
    return _reconSaleRegion(spark, ctx, "EU")


def reconFactApacSale(spark, ctx):
    return _reconSaleRegion(spark, ctx, "APAC")


def reconFactDedupSale(spark, ctx):
    tgt = _target(spark, ctx, "gold_fact_sale")
    checks, sc, tc = compare(_legacySale(spark), tgt, SALE_LEGACY, SALE_TARGET, "legacy_target", SALE_METHOD)
    dupTarget = 0 if tgt is None else tgt.groupBy("wwi_invoice_id", "stock_item_key", "description", "quantity", "unit_price").count().filter("count > 1").count()
    dupSource = _legacySale(spark).groupBy("`WWI Invoice ID`", "`Stock Item Key`", "Description", "Quantity", "`Unit Price`").count().filter("count > 1").count()
    checks.append({"check": "duplicate_natural_keys", "source": dupSource, "target": dupTarget, "pass": dupSource == dupTarget})
    archive = _target(spark, ctx, "work_fact_sale_duplicate_archive")
    checks.append({"check": "archived_duplicates", "target": _rowCount(archive)})
    v, why = verdictFor(checks, "legacy_target", tc)
    return checks, v, why, f"{LEGACY_DW}.Fact.Sale", ctx.table("gold_fact_sale")


def reconFactLoadOrder(spark, ctx):
    legacy = [i("City Key"), i("Customer Key"), i("Stock Item Key"), dt("Order Date Key"), dt("Picked Date Key"), i("Salesperson Key"), i("Picker Key"), i("WWI Order ID"), i("WWI Backorder ID"), s("Description"), s("Package"), i("Quantity"), d2("Unit Price"), d3("Tax Rate"), d2("Total Excluding Tax"), d2("Tax Amount"), d2("Total Including Tax")]
    target = [i("city_key"), i("customer_key"), i("stock_item_key"), dt("order_date_key"), dt("picked_date_key"), i("salesperson_key"), i("picker_key"), i("wwi_order_id"), i("wwi_backorder_id"), s("description"), s("package"), i("quantity"), d2("unit_price"), d3("tax_rate"), d2("total_excluding_tax"), d2("tax_amount"), d2("total_including_tax")]
    tgt = _target(spark, ctx, "gold_fact_order")
    checks, sc, tc = compare(spark.table(f"{DW}.Fact.Order"), tgt, legacy, target, "legacy_target", "city_key, customer_key, stock_item_key, order_date_key, picked_date_key, salesperson_key, picker_key, wwi_order_id, wwi_backorder_id, description, package, quantity, unit_price, tax_rate, total_excluding_tax, tax_amount, total_including_tax")
    held = _target(spark, ctx, "work_order_line_enriched")
    checks.append({"check": "held_rows", "target": _rowCount(held), "note": "work.OrderLineEnriched rows still waiting for a dimension member"})
    v, why = verdictFor(checks, "legacy_target", tc)
    return checks, v, why, f"{LEGACY_DW}.Fact.Order", ctx.table("gold_fact_order")


def reconFactLoadCustomerTransaction(spark, ctx):
    src = spark.sql(f"SELECT CustomerTransactionID, CustomerID, TransactionTypeID, TransactionDate, AmountExcludingTax, TaxAmount, TransactionAmount, OutstandingBalance, IsFinalized FROM {OLTP}.Sales.CustomerTransactions")
    tgt = _target(spark, ctx, "gold_fact_customer_transaction")
    checks, sc, tc = compare(
        src, tgt,
        [i("CustomerTransactionID"), i("CustomerID"), dt("TransactionDate"), d2("AmountExcludingTax"), d2("TaxAmount"), d2("TransactionAmount"), d2("OutstandingBalance"), i("IsFinalized")],
        [i("wwi_customer_transaction_id"), i("wwi_customer_id"), dt("transaction_date_key"), d2("amount_excluding_tax"), d2("tax_amount"), d2("transaction_amount"), d2("outstanding_balance"), i("is_finalized")],
        "source_derived", "wwi_customer_transaction_id, customer id, transaction_date, amount_excluding_tax, tax_amount, transaction_amount, outstanding_balance, is_finalized",
    ) if tgt is not None and "wwi_customer_id" in tgt.columns else compare(src, tgt, [i("CustomerTransactionID")], [i("wwi_customer_transaction_id")], "source_derived", "wwi_customer_transaction_id")
    legacyCount = spark.sql(f"SELECT COUNT(*) AS n FROM remote_query('wwi_legacy_sqlserver', database => '{LEGACY_DW}', query => 'SELECT 1 AS x FROM Fact.[Customer Transaction]')").first()["n"]
    checks.append({"check": "legacy_target_row_count", "source": legacyCount, "target": tc, "note": "Fact.[Customer Transaction] is empty on the host"})
    v, why = verdictFor(checks, "source_derived", tc)
    return checks, v, why, f"{LEGACY_DW}.Fact.Customer Transaction", ctx.table("gold_fact_customer_transaction")


def reconFactLoadTransaction(spark, ctx):
    legacy = [dt("Date Key"), i("Customer Key"), i("Bill To Customer Key"), i("Supplier Key"), i("Transaction Type Key"), i("Payment Method Key"), i("WWI Customer Transaction ID"), i("WWI Supplier Transaction ID"), i("WWI Invoice ID"), i("WWI Purchase Order ID"), s("Supplier Invoice Number"), d2("Total Excluding Tax"), d2("Tax Amount"), d2("Total Including Tax"), d2("Outstanding Balance"), i("Is Finalized")]
    target = [dt("date_key"), i("customer_key"), i("bill_to_customer_key"), i("supplier_key"), i("transaction_type_key"), i("payment_method_key"), i("wwi_customer_transaction_id"), i("wwi_supplier_transaction_id"), i("wwi_invoice_id"), i("wwi_purchase_order_id"), s("supplier_invoice_number"), d2("total_excluding_tax"), d2("tax_amount"), d2("total_including_tax"), d2("outstanding_balance"), i("is_finalized")]
    tgt = _target(spark, ctx, "gold_fact_transaction")
    checks, sc, tc = compare(spark.table(f"{DW}.Fact.Transaction"), tgt, legacy, target, "legacy_target", "date_key, customer_key, bill_to_customer_key, supplier_key, transaction_type_key, payment_method_key, wwi ids, supplier_invoice_number, total_excluding_tax, tax_amount, total_including_tax, outstanding_balance, is_finalized")
    unbalanced = _latestBatch(_target(spark, ctx, "err_rejected_constraint_violation"))
    checks.append({"check": "unbalanced_rows_logged", "target": 0 if unbalanced is None else unbalanced.filter(F.col("constraint_name") == "CK_Transaction_Balanced").count(), "note": "loaded and logged, not rejected, to match the legacy fact"})
    v, why = verdictFor(checks, "legacy_target", tc)
    return checks, v, why, f"{LEGACY_DW}.Fact.Transaction", ctx.table("gold_fact_transaction")


def reconSnapshot(spark, ctx):
    tgt = _target(spark, ctx, "gold_fact_daily_sales_snapshot")
    if tgt is None or tgt.count() == 0:
        return [{"check": "row_count", "source": None, "target": 0, "pass": False, "baseline": "source_derived"}, {"check": "checksum", "source": None, "target": None, "pass": False, "baseline": "source_derived"}], "FAIL", "snapshot table missing/empty", f"{LEGACY_DW}.Fact.Daily Sales Snapshot", ctx.table("gold_fact_daily_sales_snapshot")
    lo, hi = tgt.agg(F.min("snapshot_date_key"), F.max("snapshot_date_key")).first()
    src = (
        _legacySale(spark).filter(F.col("`Invoice Date Key`").between(F.lit(lo), F.lit(hi)))
        .groupBy(F.col("`Invoice Date Key`").alias("dk"), F.col("`Salesperson Key`").alias("sk"))
        .agg(F.count("*").alias("lines"), F.sum("`Total Excluding Tax`").alias("net"), F.countDistinct("`WWI Invoice ID`").alias("inv"), F.sum("Profit").alias("margin"))
        .filter((F.col("margin") >= 0) & (F.col("net") != 0))  # the package rejects NEGATIVE_MARGIN / ZERO_VALUE cells
    )
    tgtAgg = tgt.groupBy(F.col("snapshot_date_key").alias("dk"), F.col("salesperson_key").alias("sk")).agg(F.sum("line_count").alias("lines"), F.sum("net_sales_amount").alias("net"), F.sum("invoice_count").alias("inv"))
    exprs = [dt("dk"), i("sk"), i("lines"), d2("net"), i("inv")]
    checks, sc, tc = compare(src, tgtAgg, exprs, exprs, "source_derived", "snapshot_date, salesperson_key, line_count, net_sales_amount, invoice_count")
    checks.append({"check": "window", "from": str(lo), "to": str(hi), "note": "legacy default window is GETDATE()-anchored; anchored on MAX(invoice date) because WWI data ends in 2016"})
    checks.append({"check": "legacy_target_row_count", "source": 0, "target": tgt.count(), "note": "Fact.[Daily Sales Snapshot] is empty on the host"})
    v, why = verdictFor(checks, "source_derived", tc)
    return checks, v, why, f"{LEGACY_DW}.Fact.Daily Sales Snapshot", ctx.table("gold_fact_daily_sales_snapshot")


def reconAggregate(spark, ctx):
    tgt = _target(spark, ctx, "gold_agg_daily_sales_summary")
    if tgt is None or tgt.count() == 0:
        return [{"check": "row_count", "source": None, "target": 0, "pass": False, "baseline": "source_derived"}, {"check": "checksum", "source": None, "target": None, "pass": False, "baseline": "source_derived"}], "FAIL", "aggregate table missing/empty", f"{LEGACY_DW}.Aggregate.Daily Sales Summary", ctx.table("gold_agg_daily_sales_summary")
    lo, hi = tgt.agg(F.min("sales_date"), F.max("sales_date")).first()
    src = (
        _legacySale(spark).filter(F.col("`Invoice Date Key`").between(F.lit(lo), F.lit(hi)))
        .groupBy(F.col("`Invoice Date Key`").alias("dk"), F.col("`Stock Item Key`").alias("sik"))
        .agg(F.count("*").alias("lines"), F.sum("`Total Excluding Tax`").alias("net"), F.countDistinct("`Customer Key`").alias("cust"))
        .filter(F.col("cust") >= 3)
    )
    tgtAgg = tgt.groupBy(F.col("sales_date").alias("dk"), F.col("stock_item_key").alias("sik")).agg(F.sum("line_count").alias("lines"), F.sum("net_sales_amount").alias("net"), F.sum("distinct_customer_count").alias("cust"))
    exprs = [dt("dk"), i("sik"), i("lines"), d2("net"), i("cust")]
    checks, sc, tc = compare(src, tgtAgg, exprs, exprs, "source_derived", "sales_date, stock_item_key, line_count, net_sales_amount, distinct_customer_count (cells with >= 3 customers)")
    suppressed = _latestBatch(_target(spark, ctx, "err_rejected_summary_cell"))
    checks.append({"check": "suppressed_cells", "target": _rowCount(suppressed), "note": "cells with < 3 distinct customers"})
    checks.append({"check": "window", "from": str(lo), "to": str(hi)})
    checks.append({"check": "legacy_target_row_count", "source": 0, "target": tgt.count(), "note": "Aggregate.[Daily Sales Summary] is empty on the host"})
    v, why = verdictFor(checks, "source_derived", tc)
    return checks, v, why, f"{LEGACY_DW}.Aggregate.Daily Sales Summary", ctx.table("gold_agg_daily_sales_summary")


RECON: dict[str, Callable] = {
    "EXT_SQL_Orders": reconExtOrders,
    "EXT_SQL_OrderLines": reconExtOrderLines,
    "EXT_SQL_Invoices": reconExtInvoices,
    "EXT_SQL_InvoiceLines": reconExtInvoiceLines,
    "EXT_SQL_CustomerTransactions": reconExtCustomerTransactions,
    "STG_Load_Order": reconStgLoadOrder,
    "STG_Load_Sale": reconStgLoadSale,
    "DQ_OrderLine_Screen": reconDqOrderLine,
    "DQ_InvoiceLine_Screen": reconDqInvoiceLine,
    "FACT_NA_Load_Sale": reconFactNaSale,
    "FACT_EU_Load_Sale": reconFactEuSale,
    "FACT_APAC_Load_Sale": reconFactApacSale,
    "FACT_Dedup_Sale": reconFactDedupSale,
    "FACT_Load_Order": reconFactLoadOrder,
    "FACT_Load_CustomerTransaction": reconFactLoadCustomerTransaction,
    "FACT_Load_Transaction": reconFactLoadTransaction,
    "FACT_Load_DailySalesSnapshot": reconSnapshot,
    "AGG_Refresh_DailySalesSummary": reconAggregate,
}


def runRecon(spark: SparkSession, ctx: RunContext, packages=None) -> DataFrame:
    runId = str(uuid.uuid4())
    rows = []
    for pkg in packages or PACKAGES:
        try:
            checks, verdict, summary, sourceObject, targetObject = RECON[pkg](spark, ctx)
        except Exception as exc:  # noqa: BLE001 - evidence must still be written for a failed check
            checks, verdict, summary = [{"check": "row_count", "pass": False, "error": str(exc)[:500]}, {"check": "checksum", "pass": False, "error": str(exc)[:500]}], "FAIL", f"recon raised: {str(exc)[:300]}"
            sourceObject, targetObject = "unknown", "unknown"
        rows.append((runId, pkg, "ssis_package", verdict, BRANCH, sourceObject, targetObject, json.dumps(checks, default=str), summary, ctx.gitSha, ACTOR, HARNESS_VERSION))
    df = spark.createDataFrame(rows, "run_id string, unit string, unit_type string, verdict string, branch string, source_object string, target_object string, checks string, summary string, git_sha string, actor string, harness_version string").withColumn("run_at", F.current_timestamp())
    df = df.select("run_id", "run_at", "unit", "unit_type", "verdict", "branch", "source_object", "target_object", "checks", "summary", "git_sha", "actor", "harness_version")
    df.write.format("delta").mode("append").saveAsTable(ctx.evidenceTable)
    return df
