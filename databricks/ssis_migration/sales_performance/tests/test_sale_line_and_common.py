from datetime import date

from pyspark.sql import functions as F

from conftest import d
from sales_performance.aggregates import activeSales
from sales_performance.common import mergeInto, orderIndependentChecksum, readTable
from sales_performance.sale_line import buildSaleLine

FACT_SCHEMA = "sale_key bigint, wwi_invoice_id bigint, invoice_date_key date, delivery_date_key date, customer_key bigint, bill_to_customer_key bigint, city_key bigint, stock_item_key bigint, salesperson_key bigint, description string, package string, quantity int, unit_price decimal(18,2), tax_rate decimal(18,3), total_excluding_tax decimal(18,2), tax_amount decimal(18,2), profit decimal(18,2), total_including_tax decimal(18,2), lineage_key int"


def test_sale_line_unknown_members_regions_and_line_numbers(spark):
    fact = spark.createDataFrame(
        [
            (1, 100, date(2016, 5, 2), date(2016, 5, 3), 1, 1, 1, 1, 1, "x", "Each", 2, d(10), d(15), d(20), d(3), d(8), d(23), 1),
            (2, 100, date(2016, 5, 2), date(2016, 5, 3), 1, 2, 1, 999, 999, "y", "Each", 1, d(0), d(15), d(0), d(0), d(0), d(0), 1),
            (3, 101, date(2016, 5, 2), date(2016, 5, 3), None, None, 2, 1, 1, "z", "Each", 1, d(5), d(20), d(5), d(1), d(2), d(6), 1),
            (4, 102, date(2016, 5, 2), date(2016, 5, 3), 1, 1, 3, 1, 1, "w", "Each", 1, d(5), d(10), d(5), d(0.5), d(2), d(5.5), 1),
        ],
        FACT_SCHEMA,
    )
    customers = spark.createDataFrame(
        [(1, 10, "Acme", "Novelty Shop", "Tailspin", "10001"), (1, 10, "Acme dup", "Novelty Shop", "Tailspin", "10001")],
        "customer_key bigint, wwi_customer_id int, customer string, category string, buying_group string, postal_code string",
    )
    employees = spark.createDataFrame([(1, 20, "Kayla")], "employee_key bigint, wwi_employee_id int, employee string")
    cities = spark.createDataFrame(
        [(1, "New York", "New York", "United States"), (2, "Berlin", "Berlin", "Germany"), (3, "Sydney", "New South Wales", "Australia")],
        "city_key bigint, city string, state_province string, country string",
    )
    items = spark.createDataFrame(
        [(1, 30, "Widget", "WWI", "123")], "stock_item_key bigint, wwi_stock_item_id int, stock_item string, brand string, barcode string"
    )
    out = {r["sale_key"]: r for r in buildSaleLine(fact, customers, employees, cities, items).collect()}
    assert len(out) == 4  # duplicate customer row deduplicated
    assert (
        out[1]["region_code"] == "NA"
        and out[1]["territory_code"] == "NA-US-EAST"
        and out[1]["currency_code"] == "USD"
        and out[1]["invoice_line_number"] == 1
    )
    assert (
        out[2]["invoice_line_number"] == 2 and out[2]["stock_item_key"] == 999 and out[2]["stock_item_id"] is None
    )  # late-arriving item keeps key, no attrs
    assert out[2]["line_type_code"] == "SAMPLE" and out[2]["is_house_account"] is True
    assert (
        out[3]["customer_key"] == -1
        and out[3]["bill_to_customer_key"] == -1
        and out[3]["region_code"] == "EU"
        and out[3]["country_code_iso3"] == "DEU"
    )
    assert str(out[3]["vat_amount"]) == "1.0000" and out[3]["gst_amount"] is None and out[3]["fiscal_calendar_code"] == "EUCAL"
    assert out[4]["region_code"] == "APAC" and str(out[4]["gst_amount"]) == "0.5000" and out[4]["fiscal_period_label"] == "FY2016-P11"
    assert str(out[1]["cost_amount"]) == "12.0000"


def test_active_sales_excludes_reversals_and_reversed_originals(spark):
    fact = spark.createDataFrame(
        [(1, False, None, True), (2, True, 1, True), (3, False, 1, True), (4, False, None, False)],
        "sale_key bigint, is_reversal boolean, reverses_sale_key bigint, is_correction boolean",
    )
    assert {r["sale_key"] for r in activeSales(fact).collect()} == {3, 4}


def test_checksum_is_order_independent_and_sensitive(spark):
    a = spark.createDataFrame([(1, "x", d(1.5)), (2, "y", d(2.5))], "k int, s string, m decimal(10,2)")
    b = spark.createDataFrame([(2, "y", d(2.5)), (1, "x", d(1.5))], "k int, s string, m decimal(10,2)")
    c = spark.createDataFrame([(2, "y", d(2.5)), (1, "x", d(1.6))], "k int, s string, m decimal(10,2)")
    cols = ["k", "s", "m"]
    assert orderIndependentChecksum(a, cols) == orderIndependentChecksum(b, cols) != orderIndependentChecksum(c, cols)


def test_merge_into_upserts_without_duplicates(spark):
    first = spark.createDataFrame([(1, "a", 1), (2, "b", 1)], "id int, val string, ver int")
    mergeInto(spark, first, "t_merge_test", ["id"])
    second = spark.createDataFrame([(2, "b2", 2), (3, "c", 2)], "id int, val string, ver int")
    mergeInto(spark, second, "t_merge_test", ["id"])
    mergeInto(spark, second, "t_merge_test", ["id"])  # idempotent re-run
    rows = {r["id"]: (r["val"], r["ver"]) for r in readTable(spark, "t_merge_test").collect()}
    assert rows == {1: ("a", 1), 2: ("b2", 2), 3: ("c", 2)}
    assert readTable(spark, "t_merge_test").groupBy("id").count().filter(F.col("count") > 1).count() == 0
