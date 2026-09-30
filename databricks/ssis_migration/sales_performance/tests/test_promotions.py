from datetime import date, datetime

from conftest import d
from sales_performance.promotions import attributeRedemptions, summarizePromotions

PROMO_SCHEMA = "promotion_id int, promotion_code string, promotion_name string, region_code string, promotion_type string, start_date date, end_date date, budget_amount decimal(19,4), budget_currency_code string"
LINE_SCHEMA = "promotion_line_id int, promotion_id int, line_sequence int, discount_percent decimal(9,4), discount_amount decimal(19,4)"
RED_SCHEMA = "promotion_redemption_id int, promotion_id int, order_id int, customer_id int, coupon_code string, redeemed_when timestamp, redeemed_amount decimal(19,4), discount_value decimal(19,4), currency_code string, redemption_status string"


def _fixtures(spark):
    promos = spark.createDataFrame(
        [
            (1, "NA10", "NA ten", "NA ", "PERCENTAGE", date(2016, 5, 1), date(2016, 5, 31), d(30), "USD"),
            (2, "EUFIX", "EU fixed", "EU", "FIXED", date(2016, 5, 1), date(2016, 5, 31), d(1000), "EUR"),
            (3, "AP5", "APAC five", "APAC", "PERCENTAGE", date(2016, 5, 1), date(2016, 5, 31), None, "AUD"),
        ],
        PROMO_SCHEMA,
    )
    lines = spark.createDataFrame(
        [(1, 1, 1, d(10), None), (2, 1, 2, d(50), None), (3, 2, 1, None, d(5)), (4, 3, 1, d(5), None)], LINE_SCHEMA
    )
    reds = spark.createDataFrame(
        [
            (1, 1, 1, 1, "A", datetime(2016, 5, 15), d(200), d(20), "USD", "REDEEMED"),
            (2, 1, 2, 1, "A", datetime(2016, 6, 20), d(100), d(10), "USD", "REDEEMED"),
            (3, 1, 3, 2, "A", datetime(2016, 7, 5), d(100), d(10), "USD", "REDEEMED"),
            (4, 2, 4, 3, "B", datetime(2016, 6, 1), d(50), d(5), "EUR", "REDEEMED"),
            (5, 3, 5, 4, "C", datetime(2016, 6, 14), d(100), d(5), "AUD", "REDEEMED"),
            (6, 3, 6, 4, "C", datetime(2016, 6, 15), d(100), d(5), "AUD", "REDEEMED"),
            (7, 99, 7, 5, "Z", datetime(2016, 6, 15), d(100), d(5), "AUD", "REDEEMED"),
        ],
        RED_SCHEMA,
    )
    return promos, lines, reds


def test_regional_spill_windows_and_costing(spark):
    promos, lines, reds = _fixtures(spark)
    out = {r["promotion_redemption_id"]: r for r in attributeRedemptions(reds, promos, lines).collect()}
    assert out[1]["attribution_code"] == "ATTRIBUTED" and str(out[1]["discount_cost"]) == "20.0000"  # 200 * first line 10%
    assert out[2]["attribution_code"] == "SPILL" and out[2]["attribution_window_end"].isoformat() == "2016-06-30"
    assert out[3]["attribution_code"] == "OUTSIDE"
    assert out[4]["attribution_code"] == "OUTSIDE" and str(out[4]["discount_cost"]) == "5.0000"  # EU window ends at end_date
    assert out[5]["attribution_code"] == "SPILL" and out[6]["attribution_code"] == "OUTSIDE"  # APAC +14 days
    assert out[7]["attribution_code"] == "UNKNOWN_PROMOTION"
    summary = {r["promotion_code"]: r for r in summarizePromotions(attributeRedemptions(reds, promos, lines), promos).collect()}
    assert (
        summary["NA10"]["redemption_count"] == 2
        and summary["NA10"]["attributed_redemption_count"] == 1
        and summary["NA10"]["spill_redemption_count"] == 1
    )
    assert str(summary["NA10"]["redeemed_amount"]) == "300.0000" and str(summary["NA10"]["discount_cost"]) == "30.0000"
    assert summary["NA10"]["is_over_budget"] is False and summary["NA10"]["distinct_customer_count"] == 1
    assert summary["EUFIX"]["redemption_count"] == 0 and summary["EUFIX"]["is_over_budget"] is False
    assert summary["AP5"]["redemption_count"] == 1


def test_strict_mode_uses_promotion_window_only(spark):
    promos, lines, reds = _fixtures(spark)
    out = {r["promotion_redemption_id"]: r for r in attributeRedemptions(reds, promos, lines, strictMode=True).collect()}
    assert (
        out[2]["attribution_code"] == "OUTSIDE" and out[5]["attribution_code"] == "OUTSIDE" and out[1]["attribution_code"] == "ATTRIBUTED"
    )
    promos2 = promos.withColumn("budget_amount", promos["budget_amount"] * 0 + 10)
    summary = {r["promotion_code"]: r for r in summarizePromotions(attributeRedemptions(reds, promos2, lines), promos2).collect()}
    assert summary["NA10"]["is_over_budget"] is True
