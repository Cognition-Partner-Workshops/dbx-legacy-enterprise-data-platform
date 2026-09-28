"""Regional sale load (fact_sale.loadRegion), sale dedup ranking and correction/reversal semantics."""
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

import fact_common as fc
import fact_rules as rules
import fact_sale
from conftest import writeDelta
from dbx_etl_common import control

OPEN_END = datetime(9999, 12, 31, 23, 59, 59)
TS = datetime(2024, 3, 14, 12, 0, 0)


class Run:
    packageExecutionId = 601
    rowsRead = rowsInserted = rowsUpdated = rowsDeleted = rowsRejected = 0


def naTaxRule(spark, catalog, df):
    return (
        df.withColumn("tax_amount", rules.naSalesTaxAmount(F.col("SourceTaxAmount")))
        .withColumn("tax_rate", F.coalesce(F.col("SourceTaxRate"), F.lit(0)))
        .withColumn("is_reverse_charge", F.lit(False))
        .withColumn("vat_rate_applied", F.lit(None).cast("decimal(5,2)"))
        .withColumn("gst_rate_applied", F.lit(None).cast("decimal(5,2)"))
        .withColumn("is_price_inclusive", F.lit(None).cast("boolean"))
    )


LINE_SCHEMA = ("SaleLineBusinessKey string, SaleBusinessKey string, LineNumber int, RegionCode string, StockItemBusinessKey string, "
               "PromotionBusinessKey string, InvoiceDate date, LineDescription string, Quantity decimal(18,4), QuantityBaseUom decimal(18,4), "
               "UomCode string, UnitPriceAmount decimal(18,4), NetLineAmount decimal(18,2), GrossLineAmount decimal(18,2), LineProfitAmount decimal(18,2), "
               "TaxRegimeCode string, TaxRatePercent decimal(5,2), TaxAmount decimal(18,2), TransactionCurrencyCode string, FxRateDate date, LoadedAtUtc timestamp")
HEADER_SCHEMA = ("SaleBusinessKey string, SourceInvoiceId string, SourceSystemCode string, CustomerBusinessKey string, BillToCustomerBusinessKey string, "
                 "SalespersonBusinessKey string, OrderBusinessKey string, ConfirmedDeliveryUtc timestamp, IsCreditNote boolean, DqStatusCode string, "
                 "TransactionCurrencyCode string, DeliveryMethodCode string, FiscalPeriodLabel string, SourceModifiedDate timestamp")


def line(lineKey, saleKey, lineNo, stockItem, qty, price, net, profit, tax=Decimal("0.00"), region="NA", ccy="USD", loadedAt=TS, invoiceDate=date(2024, 3, 14)):
    return (lineKey, saleKey, lineNo, region, stockItem, None, invoiceDate, "desc", qty, None, "EA", price, net, net, profit, "SALESTAX", Decimal("8.00"), tax, ccy, None, loadedAt)


def header(saleKey, invoiceId, customer, salesperson="E1", credit=False, dq="OK", modified=TS):
    return (saleKey, invoiceId, "WWI", customer, None, salesperson, "ORD-" + invoiceId, datetime(2024, 3, 15, 8, 0, 0), credit, dq, "USD", "STD", "FY2024-P3", modified)


def setupSaleEstate(spark, catalog, scd2Dimension, lines, headers, fx=()):
    for schema, t in (("gold", fact_sale.FACT_TABLE), ("gold", fc.FACT_LOAD_HOLD_TABLE), ("silver", fc.LATE_ARRIVING_QUEUE_TABLE), ("silver", fact_sale.CURRENCY_SCRATCH_TABLE)):
        spark.sql("DROP TABLE IF EXISTS %s" % fc.tableName(catalog, schema, t))
    cust = fc.DIMENSIONS["Customer"]
    scd2Dimension(fc.tableName(catalog, "gold", cust.table), cust, [
        (10, "C1", "v1", datetime(2020, 1, 1), datetime(2024, 3, 1), False, False, "NA", 1),
        (11, "C1", "v2", datetime(2024, 3, 1), OPEN_END, True, False, "NA", 1),
    ])
    stock = fc.DIMENSIONS["Stock Item"]
    scd2Dimension(fc.tableName(catalog, "gold", stock.table), stock, [(20, "S1", "Item", datetime(2020, 1, 1), OPEN_END, True, False, "NA", 1)])
    sp = fc.DIMENSIONS["Salesperson"]
    scd2Dimension(fc.tableName(catalog, "gold", sp.table), sp, [(30, "E1", "Rep", datetime(2020, 1, 1), OPEN_END, True, False, "NA", 1)])
    spark.sql("DROP TABLE IF EXISTS %s" % fc.tableName(catalog, "gold", "dim_promotion"))
    writeDelta(spark, fc.tableName(catalog, "silver", "stg_sale_line"), spark.createDataFrame(lines, LINE_SCHEMA))
    writeDelta(spark, fc.tableName(catalog, "silver", "stg_sale"), spark.createDataFrame(headers, HEADER_SCHEMA))
    writeDelta(spark, fc.tableName(catalog, "silver", fc.FX_RATE_TABLE), spark.createDataFrame(
        list(fx), "FromCurrencyCode string, ToCurrencyCode string, RateTypeCode string, RateDate date, ConversionRate decimal(18,8), RateSourceCode string"))


def loadNa(spark, catalog, jobParams, batchId):
    control.watermarks.clear()
    return fact_sale.loadRegion(spark, catalog, jobParams, "NA", "FACT_NA_Load_Sale", Run(), batchId, naTaxRule)


def test_na_sale_load_effective_dates_rejects_drafts_and_infers_customers(spark, catalog, scd2Dimension, jobParams):
    setupSaleEstate(spark, catalog, scd2Dimension, [
        line("L1", "INV1", 1, "S1", Decimal("2"), Decimal("10.00"), Decimal("18.00"), Decimal("6.00"), tax=Decimal("1.44")),
        line("L2", "INV1", 2, "S1", Decimal("1"), Decimal("10.00"), Decimal("10.00"), Decimal("4.00"), invoiceDate=date(2024, 2, 20)),  # before the C1 SCD2 change -> keys to version 10
        line("L3", "INV2", 1, "S1", Decimal("1"), Decimal("5.00"), Decimal("5.00"), Decimal("1.00")),  # draft header
        line("L4", "INV3", 1, "S1", Decimal("1"), Decimal("5.00"), Decimal("5.00"), Decimal("1.00")),  # unknown customer -> inferred
        line("L5", "INV4", 1, "S1", None, Decimal("5.00"), Decimal("5.00"), Decimal("1.00")),  # structural reject
        line("L6", "INV5", 1, "S9", Decimal("1"), Decimal("5.00"), Decimal("5.00"), Decimal("1.00")),  # stock item missing -> held
        line("L7", "INV6", 1, "S1", Decimal("1"), Decimal("5.00"), Decimal("5.00"), Decimal("1.00"), region="EU"),  # other region
    ], [header("INV1", "1001", "C1"), header("INV2", "1002", "C1", dq="DRAFT"), header("INV3", "1003", "C7"), header("INV4", "1004", "C1"), header("INV5", "1005", "C1"), header("INV6", "1006", "C1")])
    jobParams = dict(jobParams, businessDate=date(2024, 3, 14))
    result = loadNa(spark, catalog, jobParams, 7)
    fact = spark.table(fc.tableName(catalog, "gold", fact_sale.FACT_TABLE))
    rows = {(r["invoice_number"], r["invoice_line_number"]): r for r in fact.collect()}

    assert result["rowsRead"] == 5  # L3 draft and L7 other region are filtered at source; epoch watermark -> no backdating cut
    assert result["rejected"] == 1 and result["held"] == 1 and result["inferredCustomers"] == 1
    assert set(rows) == {("1001", 1), ("1001", 2), ("1003", 1)}
    r1 = rows[("1001", 1)]
    assert rows[("1001", 2)]["customer_key"] == 10  # effective-dated SCD2 lookup, not the current row
    assert r1["customer_key"] == 11 and r1["stock_item_key"] == 20 and r1["salesperson_key"] == 30 and r1["promotion_key"] == fc.NOT_APPLICABLE_KEY
    assert (r1["gross_amount"], r1["line_discount_amount"], r1["net_amount"], r1["cost_of_sale_amount"], r1["profit"]) == (Decimal("20.00"), Decimal("2.00"), Decimal("18.00"), Decimal("12.00"), Decimal("6.00"))
    assert (r1["tax_amount"], r1["total_including_tax"], r1["fx_rate_to_reporting"], r1["net_amount_reporting"]) == (Decimal("1.44"), Decimal("19.44"), Decimal("1.000000000"), Decimal("18.00"))
    assert r1["correction_type_code"] == fc.CORRECTION_ORIGINAL and r1["region_code"] == "NA" and r1["fiscal_year"] == 2024 and r1["fiscal_period"] == 3
    assert r1["natural_key_hash"] == fact.select(fc.naturalKeyHash(F.lit("1001"), F.lit(1), F.lit("NA"))).first()[0]
    assert rows[("1003", 1)]["inferred_member_flag"] is True and rows[("1003", 1)]["customer_key"] > 11
    assert spark.table(fc.tableName(catalog, "gold", fc.FACT_LOAD_HOLD_TABLE)).where("missing_business_key = 'S9'").count() == 1

    # Idempotent re-run of the same window: nothing new, no reversals.
    again = loadNa(spark, catalog, jobParams, 8)
    assert again["inserted"] == 0 and again["reversals"] == 0 and again["unchanged"] == 3


def test_changed_sale_line_gets_reversal_and_restatement(spark, catalog, scd2Dimension, jobParams):
    setupSaleEstate(spark, catalog, scd2Dimension, [line("L1", "INV1", 1, "S1", Decimal("2"), Decimal("10.00"), Decimal("20.00"), Decimal("8.00"))], [header("INV1", "1001", "C1")])
    jobParams = dict(jobParams, businessDate=date(2024, 3, 14))
    loadNa(spark, catalog, jobParams, 7)
    fact = fc.tableName(catalog, "gold", fact_sale.FACT_TABLE)
    origKey = spark.table(fact).first()["sale_key"]

    later = datetime(2024, 3, 15, 9, 0, 0)
    writeDelta(spark, fc.tableName(catalog, "silver", "stg_sale_line"), spark.createDataFrame(
        [line("L1", "INV1", 1, "S1", Decimal("3"), Decimal("10.00"), Decimal("30.00"), Decimal("12.00"), loadedAt=later)], LINE_SCHEMA))
    writeDelta(spark, fc.tableName(catalog, "silver", "stg_sale"), spark.createDataFrame([header("INV1", "1001", "C1", modified=later)], HEADER_SCHEMA))
    result = loadNa(spark, catalog, jobParams, 8)
    assert result["reversals"] == 1
    rows = {r["correction_type_code"]: r for r in spark.table(fact).collect()}
    assert set(rows) == {fc.CORRECTION_REVERSAL, fc.CORRECTION_RESTATEMENT}
    rev, res = rows[fc.CORRECTION_REVERSAL], rows[fc.CORRECTION_RESTATEMENT]
    assert rev["quantity"] == Decimal("-2") and rev["net_amount"] == Decimal("-20.00") and rev["profit"] == Decimal("-8.00")
    assert res["sale_key"] == origKey and res["quantity"] == Decimal("3") and res["net_amount"] == Decimal("30.00")
    assert rev["corrected_sale_key"] == origKey and res["corrected_sale_key"] == origKey and rev["sale_key"] != origKey
    assert rev["batch_id"] == 8 and res["batch_id"] == 8
    assert spark.table(fact).agg(F.sum("net_amount")).first()[0] == Decimal("10.00")  # net effect = restated value - original


def test_eu_reverse_charge_and_fx_hold(spark, catalog, scd2Dimension, jobParams):
    def euTaxRule(spark, catalog, df):
        # Same shape as FACT_EU_Load_Sale.euVatRule with the customer/tax-rate joins replaced by literals:
        # cross-border VAT-registered customer -> reverse charge, 20% standard rate otherwise.
        isReverse = rules.euIsReverseCharge(F.lit("FR123"), F.lit("FR"), F.when(F.col("InvoiceNumber") == "1001", F.lit("DE")).otherwise(F.lit("FR")))
        df = df.withColumn("is_reverse_charge", isReverse).withColumn("vat_rate_applied", rules.euVatRateApplied(isReverse, F.lit(20)))
        return (
            df.withColumn("tax_amount", rules.euVatAmount(F.col("net_amount"), F.col("vat_rate_applied")))
            .withColumn("tax_rate", F.col("vat_rate_applied"))
            .withColumn("gst_rate_applied", F.lit(None).cast("decimal(5,2)"))
            .withColumn("is_price_inclusive", F.lit(None).cast("boolean"))
            .withColumn("TaxRegimeCode", F.lit("VAT"))
        )

    setupSaleEstate(spark, catalog, scd2Dimension, [
        line("L1", "INV1", 1, "S1", Decimal("1"), Decimal("100.00"), Decimal("100.00"), Decimal("40.00"), region="EU", ccy="EUR"),
        line("L2", "INV2", 1, "S1", Decimal("1"), Decimal("100.00"), Decimal("100.00"), Decimal("40.00"), region="EU", ccy="GBP"),  # no FX -> held
    ], [header("INV1", "1001", "C1"), header("INV2", "1002", "C1")],
        fx=[("EUR", "USD", "CLOSE", date(2024, 3, 10), Decimal("1.10000000"), "ECB")])
    control.watermarks.clear()
    result = fact_sale.loadRegion(spark, catalog, dict(jobParams, businessDate=date(2024, 3, 14)), "EU", "FACT_EU_Load_Sale", Run(), 7, euTaxRule, fxSourceCode="ECB")
    rows = spark.table(fc.tableName(catalog, "gold", fact_sale.FACT_TABLE)).collect()
    assert result["held"] == 1 and len(rows) == 1
    r = rows[0]
    assert r["fx_rate_to_reporting"] == Decimal("1.100000000") and r["fx_rate_effective_date"] == date(2024, 3, 10) and r["fx_rate_source_code"] == "ECB"
    assert r["net_amount_reporting"] == Decimal("110.00") and r["tax_regime_code"] == "VAT"
    assert r["vat_reverse_charge_flag"] is True and r["vat_rate"] == Decimal("0.000") and r["tax_amount"] == Decimal("0.00")
    hold = spark.table(fc.tableName(catalog, "gold", fc.FACT_LOAD_HOLD_TABLE)).first()
    assert hold["hold_reason_code"] == fc.HOLD_REASON_FX_MISSING and hold["max_retry_count"] == fc.HOLD_RETRY_LIMITS["EU"]
    assert spark.table(fc.tableName(catalog, "silver", fact_sale.CURRENCY_SCRATCH_TABLE)).count() == 1
