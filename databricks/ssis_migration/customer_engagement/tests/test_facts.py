from datetime import date, datetime

from pyspark.sql import Row

from customer_engagement import facts

NOW = datetime(2016, 6, 1)


def stgPointRow(**overrides):
    base = dict(
        LoyaltyEventBusinessKey=1,
        CustomerBusinessKey="10",
        LoyaltyProgramCode="BASE",
        EventTypeCode="EARN",
        EventDate=date(2014, 1, 10),
        PointsQuantity=100,
        RegionCode="NA",
        SourceInvoiceNumber="7",
        LastModifiedAt=NOW,
    )
    base.update(overrides)
    return Row(**base)


def customerKeys(spark):
    return spark.createDataFrame([Row(CustomerBusinessKey="10", CustomerKey=110), Row(CustomerBusinessKey="11", CustomerKey=111)])


def dateDim(spark):
    return spark.createDataFrame([Row(Date=date(2014, 1, 10)), Row(Date=date(2014, 2, 1)), Row(Date=date(2016, 1, 10))])


def test_loyaltyMeasuresUnknownMemberAndRegionalRates(spark):
    stg = spark.createDataFrame(
        [
            stgPointRow(LoyaltyEventBusinessKey=1, RegionCode="NA"),
            stgPointRow(LoyaltyEventBusinessKey=2, RegionCode="EU", EventTypeCode="REDEEM", PointsQuantity=40),
            stgPointRow(LoyaltyEventBusinessKey=3, RegionCode="APAC", LoyaltyProgramCode="DBL"),
            stgPointRow(LoyaltyEventBusinessKey=4, CustomerBusinessKey="999"),
        ]
    )
    fact, held = facts.deriveLoyaltyPointMeasures(stg, customerKeys(spark), dateDim(spark), 24, 36, 1)
    rows = {r.LoyaltyEventBusinessKey: r for r in fact.collect()}
    assert set(rows) == {1, 2, 3}
    assert rows[1].SignedPoints == 100 and float(rows[1].PointCashValue) == 0.01 and float(rows[1].EarnRatePerCurrencyUnit) == 1.0
    assert str(rows[1].ExpiryDate) == "2016-01-10" and rows[1].RouteCode == "EARNINGS" and rows[1].CustomerKeyResolved == 110
    assert rows[2].SignedPoints == -40 and float(rows[2].PointCashValue) == 0.0085 and str(rows[2].ExpiryDate) == "2017-01-10"
    assert rows[2].RouteCode == "REDEMPTIONS" and float(rows[2].LiabilityAmount) == 0.34
    assert float(rows[3].EarnRatePerCurrencyUnit) == 2.0 and float(rows[3].PointCashValue) == 0.006
    heldRows = held.collect()
    assert len(heldRows) == 1 and heldRows[0].RejectReasonCode == "LOOKUP_CUSTOMER_NO_MATCH" and heldRows[0].CustomerKeyResolved == 0


def test_expiryRowsAndRunningBalanceAreIdempotent(spark):
    stg = spark.createDataFrame(
        [
            stgPointRow(LoyaltyEventBusinessKey=1, EventDate=date(2014, 1, 10), PointsQuantity=100),
            stgPointRow(LoyaltyEventBusinessKey=2, EventDate=date(2014, 2, 1), EventTypeCode="REDEEM", PointsQuantity=30),
            stgPointRow(LoyaltyEventBusinessKey=3, EventDate=date(2016, 1, 10), PointsQuantity=50),
        ]
    )
    fact, _ = facts.deriveLoyaltyPointMeasures(stg, customerKeys(spark), dateDim(spark), 24, 36, 1)
    expiries = facts.generateExpiryRows(fact, date(2016, 6, 1), 1)
    exp = expiries.collect()
    assert len(exp) == 1 and exp[0].MovementReference == "EXP-1" and exp[0].SignedPoints == -100 and str(exp[0].EventDate) == "2016-01-10"
    assert float(exp[0].LiabilityAmount) == -1.0
    combined = fact.unionByName(expiries)
    # a second pass must not generate the same expiry twice
    assert facts.generateExpiryRows(combined, date(2016, 6, 1), 1).count() == 0
    balances = {r.MovementReference: r.PointsBalanceAfter for r in facts.recomputeRunningBalance(combined).collect()}
    assert balances == {"1": 100, "2": 70, "3": 120, "EXP-1": 20}  # same-day rows order by reference


def stgSessionRow(**overrides):
    base = dict(
        SessionBusinessKey="S1",
        VisitorBusinessKey="v1",
        CustomerBusinessKey="10",
        CustomerId=10,
        SessionStartedAt=datetime(2016, 3, 1, 10),
        SessionEndedAt=datetime(2016, 3, 1, 10, 10),
        PageViewCount=10,
        CartAddCount=0,
        SessionDurationSeconds=600,
        ChannelCode="WEB",
        DeviceTypeCode="MOBILE",
        UserAgentFamily="Chrome",
        CountryIsoCode="USA",
        RegionCode="NA",
        ConsentGivenFlag="Y",
        LandingPagePath="/",
        CampaignCode="X",
        BounceFlag="N",
    )
    base.update(overrides)
    return Row(**base)


def test_webSessionFactDedupBotsBouncesAndPrivacy(spark):
    stg = spark.createDataFrame(
        [
            stgSessionRow(SessionBusinessKey="S1"),
            stgSessionRow(SessionBusinessKey="S1", PageViewCount=99),
            stgSessionRow(SessionBusinessKey="S2", UserAgentFamily="Googlebot"),
            stgSessionRow(SessionBusinessKey="S3", PageViewCount=501),
            stgSessionRow(SessionBusinessKey="S4", PageViewCount=1),
            stgSessionRow(SessionBusinessKey="S5", SessionDurationSeconds=2, PageViewCount=5),
            stgSessionRow(SessionBusinessKey="S6", SessionEndedAt=None),
            stgSessionRow(SessionBusinessKey="S7", RegionCode="APAC", ConsentGivenFlag="N", CustomerBusinessKey="10"),
            stgSessionRow(SessionBusinessKey="S8", RegionCode="EU", ConsentGivenFlag="Y", CustomerBusinessKey="777", CustomerId=777),
            stgSessionRow(SessionBusinessKey="S9", SessionDurationSeconds=0),
        ]
    )
    fact, rejects = facts.deriveWebSessionFact(stg, customerKeys(spark), 3, 1)
    rows = {r.SessionBusinessKey: r for r in fact.collect()}
    rj = {r.SessionBusinessKey: r.RejectReasonCode for r in rejects.collect()}
    assert rj == {"S2": "BOT_TRAFFIC", "S3": "BOT_TRAFFIC", "S6": "SESSION_OPEN"}
    assert len(rows) == 6 and rows["S1"].CustomerKeyResolved == 110
    assert rows["S4"].IsBounce and rows["S4"].RouteCode == "BOUNCES"
    assert rows["S5"].IsBounce
    assert rows["S7"].CustomerKeyResolved == 0 and rows["S7"].PseudonymisedFlag and rows["S7"].VisitorBusinessKey != "v1"
    assert str(rows["S7"].PurgeAfterDate) == "2016-09-01"
    assert rows["S8"].IsAnonymous and rows["S8"].RouteCode == "ANONYMOUS_SESSIONS" and str(rows["S8"].PurgeAfterDate) == "2017-05-01"
    assert float(rows["S1"].PagesPerMinute) == 1.0 and float(rows["S9"].PagesPerMinute) == 0.0
    assert str(rows["S1"].PurgeAfterDate) == "2019-03-01"
