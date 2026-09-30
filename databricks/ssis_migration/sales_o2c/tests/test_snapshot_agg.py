from datetime import date
from decimal import Decimal

from sales_o2c.snapshot_agg import buildDailySalesSnapshot, buildDailySalesSummary

FACT = "wwi_invoice_id int, invoice_date_key date, salesperson_key int, stock_item_key int, customer_key int, region_code string, quantity int, gross_amount decimal(18,2), discount_amount decimal(18,2), net_amount decimal(18,2), tax_amount decimal(18,2), total_cost_amount decimal(18,2), margin_amount decimal(18,2), net_amount_reporting decimal(18,2), total_including_tax decimal(18,2)"


def _dates(spark):
    return spark.createDataFrame([(date(2016, 1, 5), 2016, 7, 1)], "invoice_date_key date, fiscal_year int, fiscal_period int, fiscal_week int")


def _fact(spark):
    d = date(2016, 1, 5)
    return spark.createDataFrame(
        [
            (1, d, 3, 50, 1, "NA", 2, Decimal("100.00"), Decimal("10.00"), Decimal("90.00"), Decimal("13.50"), Decimal("40.00"), Decimal("50.00"), Decimal("90.00"), Decimal("103.50")),
            (2, d, 3, 50, 2, "NA", 1, Decimal("100.00"), Decimal("0.00"), Decimal("100.00"), Decimal("15.00"), Decimal("40.00"), Decimal("60.00"), Decimal("100.00"), Decimal("115.00")),
            (3, d, 3, 50, 3, "NA", 1, Decimal("10.00"), Decimal("0.00"), Decimal("10.00"), Decimal("1.50"), Decimal("4.00"), Decimal("6.00"), Decimal("10.00"), Decimal("11.50")),
            (4, d, 4, 60, 1, "NA", 1, Decimal("10.00"), Decimal("0.00"), Decimal("10.00"), Decimal("1.50"), Decimal("20.00"), Decimal("-10.00"), Decimal("10.00"), Decimal("11.50")),
        ],
        FACT,
    )


def test_snapshot_ratios_and_negative_margin_status(spark):
    out = {r.salesperson_key: r for r in buildDailySalesSnapshot(_fact(spark), _dates(spark)).collect()}
    s3 = out[3]
    assert s3.invoice_count == 3 and s3.line_count == 3 and s3.distinct_customer_count == 3
    assert s3.net_sales_amount == Decimal("200.00") and s3.effective_discount_percent == Decimal("4.7619")
    assert s3.margin_percent == Decimal("58.0000") and s3.average_line_value == Decimal("66.67") and s3.snapshot_status_code == "OK"
    assert s3.fiscal_year == 2016 and s3.fiscal_period == 7
    assert out[4].snapshot_status_code == "NEGATIVE_MARGIN"


def test_summary_suppression_channel_default_and_loss_flag(spark):
    out = {r.stock_item_key: r for r in buildDailySalesSummary(_fact(spark), _dates(spark)).collect()}
    assert out[50].sales_channel_code == "DIRECT" and not out[50].is_suppressed and out[50].distinct_customer_count == 3
    assert out[50].margin_percent == Decimal("58.0000") and out[50].line_discount_amount == Decimal("10.00") and out[50].source_row_count == 3
    assert out[60].is_suppressed and out[60].is_loss_making
