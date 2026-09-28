from datetime import date
from decimal import Decimal

from c360_lib import rolling as RM

AS_OF = date(2024, 6, 30)


def test_aggregate_rolling_window(spark):
    sale = spark.createDataFrame([
        (1, date(2024, 1, 10), 1, 100.0, 2, 7),
        (1, date(2024, 2, 10), 1, 50.0, 1, 8),      # same invoice -> distinct count 1
        (1, date(2024, 3, 10), 2, -20.0, -1, 7),    # return
        (1, date(2023, 1, 1), 3, 999.0, 1, 7),      # outside window
        (2, date(2024, 6, 1), 4, 10.0, 1, 9),
    ], "CustomerKey int, InvoiceDateKey date, WWIInvoiceID int, TotalExcludingTax double, Quantity int, StockItemKey int")
    dim = spark.createDataFrame([(1, "NA"), (2, "EU")], "CustomerKey int, RegionCode string")
    out = {r.CustomerKey: r for r in RM.aggregateRollingWindow(sale, dim, AS_OF, 12).collect()}
    assert out[1].OrderCount == 2 and out[1].NetRevenue == Decimal("130.00") and out[1].ReturnAmount == Decimal("20.00")
    assert out[1].DistinctItemCount == 2 and out[1].LastOrderDate == date(2024, 3, 10)
    assert out[1].WindowStartDate == date(2023, 6, 30) and out[1].WindowEndDate == AS_OF
    assert out[2].RegionCode == "EU"


def metrics(spark, rows):
    return spark.createDataFrame(rows, "CustomerKey int, RegionCode string, WindowStartDate date, WindowEndDate date, "
                                       "OrderCount long, NetRevenue decimal(18,2), ReturnAmount decimal(18,2), "
                                       "DistinctItemCount long, LastOrderDate date")


def test_derive_rolling_metrics(spark):
    df = metrics(spark, [
        (1, "NA", date(2023, 6, 30), AS_OF, 0, Decimal("0.00"), Decimal("0.00"), 0, None),
        (2, "NA", date(2023, 6, 30), AS_OF, 4, Decimal("200.00"), Decimal("50.00"), 3, date(2024, 6, 20)),
        (3, "NA", date(2023, 6, 30), AS_OF, 12, Decimal("120.00"), Decimal("0.00"), 3, date(2024, 6, 29)),
        (4, "NA", date(2023, 6, 30), AS_OF, 2, Decimal("10.00"), Decimal("0.00"), 1, date(2023, 7, 1)),
    ])
    out = {r.CustomerKey: r for r in RM.deriveRollingMetrics(df, 12, 270).collect()}
    assert out[1].AverageBasketAmount == 0 and out[1].ReturnRatePercent == 0 and out[1].DaysSinceLastOrder == 9999
    assert out[1].ActivityStatusCode == "INACTIVE"
    assert out[2].AverageBasketAmount == Decimal("50.00") and out[2].ReturnRatePercent == Decimal("25.00")
    assert out[2].DaysSinceLastOrder == 10 and out[2].ActivityStatusCode == "ACTIVE"
    assert out[3].ActivityStatusCode == "FREQUENT" and out[3].OrdersPerMonth == Decimal("1.00")
    assert out[4].DaysSinceLastOrder == 365 and out[4].ActivityStatusCode == "INACTIVE"


def test_apac_realignment_and_rfm(spark):
    df = metrics(spark, [
        (i, "APAC" if i % 2 else "NA", date(2023, 6, 30), AS_OF, i, Decimal(i * 10), Decimal("0.00"), 1, date(2024, 6, 1))
        for i in range(1, 21)
    ])
    fiscal = spark.createDataFrame([
        (date(2024, 3, 3), date(2024, 5, 4)), (date(2024, 5, 5), date(2024, 6, 1)), (date(2024, 6, 2), date(2024, 7, 6)),
    ], "PeriodStartDate date, PeriodEndDate date")
    period = RM.latestCompleted445Period(fiscal, AS_OF)
    assert period["PeriodStartDate"] == date(2024, 5, 5) and period["PeriodEndDate"] == date(2024, 6, 1)
    realigned = RM.realignApacWindow(df, period)
    rows = {r.CustomerKey: r for r in realigned.collect()}
    assert rows[1].WindowStartDate == date(2024, 5, 5) and rows[1].WindowEndDate == date(2024, 6, 1)
    assert rows[2].WindowStartDate == date(2023, 6, 30) and rows[2].WindowEndDate == AS_OF
    assert RM.latestCompleted445Period(fiscal, date(2024, 1, 1)) is None
    assert RM.realignApacWindow(df, None) is df

    rfm = {r.CustomerKey: r for r in RM.assignRfmDeciles(RM.deriveRollingMetrics(realigned)).collect()}
    # 10 customers per region -> NTILE(10) gives each its own decile
    assert rfm[20].MonetaryDecile == 10 and rfm[2].MonetaryDecile == 1          # NA: highest revenue -> 10
    assert rfm[19].FrequencyDecile == 10 and rfm[1].FrequencyDecile == 1        # APAC: highest orders -> 10
    assert rfm[1].RecencyDecile == 1 and rfm[2].RecencyDecile == 1              # DESC days since: all equal -> tie by key
    assert all(1 <= r.RecencyDecile <= 10 for r in rfm.values())
