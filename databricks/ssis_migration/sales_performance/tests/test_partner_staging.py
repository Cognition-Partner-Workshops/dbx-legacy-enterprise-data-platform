from pyspark.sql import functions as F

from sales_performance.partner_staging import PACKAGE, builtinCountryReference, readWatermark, stagePartnerSales, writeWatermark

RAW_COLS = "partner_code string, partner_order_ref string, customer_ref string, item_ref string, sale_date_text string, transaction_date date, quantity_text string, quantity decimal(18,3), amount_text string, gross_amount decimal(19,4), tax_amount decimal(19,4), net_amount decimal(19,4), currency_text string, country_text string, tax_treatment_code string, marketable_flag string, partner_outlet_code string, barcode string, source_file_name string, source_row_number bigint"


def _raw(spark):
    rows = [
        (
            " bigbox ",
            " tx1 ",
            "st001",
            "wwi 1001",
            "2016-05-30",
            None,
            "1,200",
            None,
            "$600.00",
            None,
            None,
            None,
            "",
            "united states",
            "SALESTAX",
            None,
            "ST001",
            None,
            "f",
            1,
        ),
        (
            "BIGBOX",
            "TX2",
            "ST001",
            "WWI-1002",
            "30/05/2016",
            None,
            "2",
            None,
            "abc",
            None,
            None,
            None,
            "usd",
            "UNITED STATES",
            "SALESTAX",
            None,
            "ST001",
            None,
            "f",
            2,
        ),
        (
            "BIGBOX",
            "TX3",
            "ST999",
            "WWI-1003",
            "2016/05/30",
            None,
            "2",
            None,
            "10.00",
            None,
            None,
            None,
            "USD",
            "UNITED STATES",
            "SALESTAX",
            None,
            "ST999",
            None,
            "f",
            3,
        ),
        (
            "BIGBOX",
            "TX4",
            "ST001",
            "WWI-1004",
            "2016-05-30",
            None,
            "2",
            None,
            "10.00",
            None,
            None,
            None,
            "USD",
            "ATLANTIS",
            "SALESTAX",
            None,
            "ST001",
            None,
            "f",
            4,
        ),
        (
            "BIGBOX",
            "",
            "ST001",
            "WWI-1005",
            "2016-05-30",
            None,
            "2",
            None,
            "10.00",
            None,
            None,
            None,
            "USD",
            "UNITED STATES",
            "SALESTAX",
            None,
            "ST001",
            None,
            "f",
            5,
        ),
        (
            "BIGBOX",
            "TX6",
            "ST001",
            "WWI-1006",
            "2016-05-30",
            None,
            "0",
            None,
            "10.00",
            None,
            None,
            None,
            "USD",
            "UNITED STATES",
            "SALESTAX",
            None,
            "ST001",
            None,
            "f",
            6,
        ),
    ]
    return spark.createDataFrame(rows, RAW_COLS)


def test_staging_normalises_and_rejects_by_rule(spark):
    crosswalk = spark.createDataFrame([("ST001", "1")], "customer_ref string, customer_code string")
    staged, rejected = stagePartnerSales(_raw(spark), builtinCountryReference(spark), crosswalk, 7)
    ok = staged.collect()
    assert len(ok) == 1
    row = ok[0]
    assert row["partner_code"] == "BIGBOX" and row["transaction_reference"] == "TX1" and row["partner_product_code"] == "WWI1001"
    assert str(row["quantity_sold"]) == "1200.0000" and str(row["gross_amount"]) == "600.0000"
    assert row["transaction_currency_code"] == "USD" and row["country_code"] == "US" and row["customer_code"] == "1"
    assert row["transaction_date"].isoformat() == "2016-05-30"
    reasons = {r["source_row_number"]: r["reject_reason_code"] for r in rejected.collect()}
    assert reasons == {
        2: "CONVERSION_ERROR",
        3: "UNKNOWN_CUSTOMER",
        4: "UNKNOWN_COUNTRY",
        5: "MISSING_ORDER_REFERENCE",
        6: "UNPARSABLE_AMOUNT",
    }
    assert staged.filter(F.col("package_name") == PACKAGE).count() == 1


def test_watermark_round_trip(spark):
    assert readWatermark(spark, "unit_a") == 0
    writeWatermark(spark, "unit_a", 5)
    writeWatermark(spark, "unit_b", 9)
    writeWatermark(spark, "unit_a", 6)
    assert readWatermark(spark, "unit_a") == 6 and readWatermark(spark, "unit_b") == 9
