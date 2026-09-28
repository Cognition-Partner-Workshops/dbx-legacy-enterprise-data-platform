from datetime import date, datetime
from decimal import Decimal

from c360_lib import churn as C

AS_OF = date(2024, 6, 30)
SRC_SCHEMA = ("CustomerKey int, CustomerId int, RegionCode string, DaysSinceLastOrder int, AverageOrderGapDays decimal(18,2), "
              "AverageBasketAmount decimal(18,2), PriorAverageBasketAmount decimal(18,2), ReturnRatePercent decimal(18,2), "
              "TierCode string, PreviousTierCode string, IsOnCreditHold boolean")


def test_rule_scores_bands_and_version(spark):
    src = spark.createDataFrame([
        # gap rule: 30 -> MEDIUM
        (1, 1, "NA", 100, Decimal(40), Decimal(100), Decimal(100), Decimal(0), "NONE", "NONE", False),
        # gap boundary: 80 is not > 80 -> 0; basket decline 20 -> LOW
        (2, 2, "NA", 80, Decimal(40), Decimal(69), Decimal(100), Decimal(0), "NONE", "NONE", False),
        # returns 15 + tier lapse 15 = 30 -> MEDIUM
        (3, 3, "NA", 0, Decimal(0), Decimal(0), Decimal(0), Decimal("15.01"), "GOLD", "PLATINUM", False),
        # tier change from NONE is not a lapse; credit hold 25 -> LOW
        (4, 4, "NA", 0, Decimal(0), Decimal(0), Decimal(0), Decimal(15), "GOLD", "NONE", True),
        # everything: 105 -> HIGH
        (5, 5, "EU", 500, Decimal(10), Decimal(1), Decimal(100), Decimal(50), "PLUS", "PREMIER", True),
        # 30 + 20 + 15 = 65 -> HIGH at default threshold 60
        (6, 6, "EU", 500, Decimal(10), Decimal(1), Decimal(100), Decimal(50), "NONE", "NONE", False),
    ], SRC_SCHEMA)
    out = {r.CustomerId: r for r in C.scoreChurnRisk(src).collect()}
    assert out[1].RuleOrderGapScore == 30 and out[1].ChurnScore == 30 and out[1].ChurnRiskBandCode == "MEDIUM"
    assert out[2].RuleOrderGapScore == 0 and out[2].RuleBasketDeclineScore == 20 and out[2].ChurnRiskBandCode == "LOW"
    assert out[3].RuleReturnsScore == 15 and out[3].RuleTierLapseScore == 15 and out[3].ChurnRiskBandCode == "MEDIUM"
    assert out[4].RuleTierLapseScore == 0 and out[4].RuleReturnsScore == 0 and out[4].RuleCreditHoldScore == 25 and out[4].ChurnRiskBandCode == "LOW"
    assert out[5].ChurnScore == 105 and out[5].ChurnRiskBandCode == "HIGH"
    assert out[6].ChurnScore == 65 and out[6].ChurnRiskBandCode == "HIGH"
    assert all(r.RuleSetVersion == "2019.3" for r in out.values())
    strict = {r.CustomerId: r.ChurnRiskBandCode for r in C.scoreChurnRisk(src, highRiskThreshold=70).collect()}
    assert strict[6] == "MEDIUM" and strict[5] == "HIGH"
    relaxedGap = {r.CustomerId: r.RuleOrderGapScore for r in C.scoreChurnRisk(src, orderGapMultiplier=1).collect()}
    assert relaxedGap[2] == 30
    assert set(r.CustomerId for r in C.splitHighRisk(C.scoreChurnRisk(src)).collect()) == {5, 6}


def test_churn_source_defaults_and_outreach_queue(spark):
    features = spark.createDataFrame([(1, 1, "NA", 0, Decimal(0), Decimal(0), Decimal(0), Decimal(0), None)],
                                     "CustomerKey int, CustomerId int, RegionCode string, DaysSinceLastOrder int, AverageOrderGapDays decimal(18,2), "
                                     "AverageBasketAmount decimal(18,2), PriorAverageBasketAmount decimal(18,2), ReturnRatePercent decimal(18,2), IsOnCreditHold boolean")
    overlay = spark.createDataFrame([], "CustomerId int, TierCode string, PreviousTierCode string")
    src = C.buildChurnSource(features, overlay).collect()[0]
    assert src.TierCode == "NONE" and src.PreviousTierCode == "NONE" and src.IsOnCreditHold is False

    highRisk = spark.createDataFrame([(1, "NA"), (2, "EU"), (3, "APAC")], "CustomerId int, RegionCode string")
    queue = spark.createDataFrame([(1, "NA", "CHURN_HIGH", datetime(2024, 1, 1)), (2, "EU", "OTHER", datetime(2024, 1, 1))],
                                  "CustomerId int, RegionCode string, QueueReasonCode string, QueuedAtUtc timestamp")
    new = {r.CustomerId: r for r in C.newOutreachQueueRows(highRisk, queue, "2024-06-30 00:00:00").collect()}
    assert set(new) == {2, 3} and new[2].QueueReasonCode == "CHURN_HIGH"


def test_feature_set_reconstruction(spark):
    rolling = spark.createDataFrame([(1, "NA", 100, Decimal(50), Decimal(5)), (2, "NA", 10, Decimal(50), Decimal(0))],
                                    "CustomerKey int, RegionCode string, DaysSinceLastOrder int, AverageBasketAmount decimal(18,2), ReturnRatePercent decimal(18,2)")
    dim = spark.createDataFrame([(1, 11, datetime(9999, 12, 31), True), (2, 22, datetime(9999, 12, 31), None), (3, 33, datetime(2000, 1, 1), True)],
                                "CustomerKey int, WWICustomerID int, ValidTo timestamp, IsOnCreditHold boolean")
    sales = spark.createDataFrame([(1, date(2024, 1, 1), date(2024, 3, 1), 3), (2, date(2024, 1, 1), date(2024, 1, 1), 1)],
                                  "CustomerKey int, FirstOrderDate date, LastOrderDate date, LifetimeOrderCount long")
    agg = spark.createDataFrame([(1, 12, 100.0, 2), (1, 13, 100.0, 2), (1, 5, 999.0, 1), (1, 24, 999.0, 1)],
                                "CustomerKey int, MonthOffset int, NetRevenueReporting double, OrderCount int")
    out = {r.CustomerId: r for r in C.buildChurnFeatureSet(rolling, dim, sales, agg, AS_OF).collect()}
    assert set(out) == {11, 22}
    assert out[11].AverageOrderGapDays == Decimal("30.00") and out[11].PriorAverageBasketAmount == Decimal("50.00") and out[11].IsOnCreditHold is True
    assert out[22].AverageOrderGapDays == Decimal("0.00") and out[22].PriorAverageBasketAmount == Decimal("0.00") and out[22].IsOnCreditHold is False
