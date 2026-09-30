from datetime import date, datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

from customer_engagement import customer360 as c360

AS_OF = date(2016, 5, 31)
NOW = datetime(2016, 6, 1)


def saleRow(customerKey, invoice, invoiceDate, amount, qty=1, item=1):
    return Row(
        CustomerKey=customerKey,
        InvoiceNumber=invoice,
        InvoiceDate=invoiceDate,
        NetAmount=amount,
        GrossAmount=amount,
        TotalExcludingTax=amount,
        TaxAmount=0.0,
        Quantity=qty,
        StockItemKey=item,
        MarginAmount=amount / 2,
        NetAmountReporting=amount,
        FxRateToReporting=1.0,
        CorrectionTypeCode="ORIG",
        SaleRegionCode="NA",
    )


def customerRow(customerKey, customerId, region="NA", creditHold=False):
    return Row(CustomerKey=customerKey, CustomerId=customerId, RegionCode=region, IsCurrentRow=True, IsOnCreditHold=creditHold)


def test_rollingMetricsWindowRatiosAndActivity(spark):
    sales = spark.createDataFrame(
        [
            saleRow(1, "I1", date(2015, 6, 15), 100.0),
            saleRow(1, "I2", date(2016, 5, 20), 300.0),
            saleRow(1, "I2", date(2016, 5, 20), -20.0, qty=-1),
            saleRow(1, "I0", date(2015, 5, 30), 999.0),  # just before the 12-month window start: excluded
            saleRow(2, "I3", date(2015, 9, 1), 50.0),
            saleRow(3, "I4", date(2014, 1, 1), 50.0),  # outside window entirely
        ]
    )
    customers = spark.createDataFrame([customerRow(1, 1), customerRow(2, 2), customerRow(3, 3)])
    rows = {r.CustomerId: r for r in c360.buildRollingMetrics(sales, customers, AS_OF).collect()}
    assert set(rows) == {1, 2}
    c1 = rows[1]
    assert c1.OrderCount == 2 and float(c1.NetRevenue) == 380.0 and float(c1.ReturnAmount) == 20.0
    assert c1.DaysSinceLastOrder == 11 and c1.ActivityStatusCode == "ACTIVE"
    assert float(c1.AverageBasketAmount) == 190.0 and float(c1.ReturnRatePercent) == 5.26
    c2 = rows[2]
    assert c2.DaysSinceLastOrder == 273 and c2.ActivityStatusCode == "INACTIVE"
    assert str(c1.WindowStartDate) == "2015-05-31" and str(c1.WindowEndDate) == "2016-05-31"


def test_fourFourFivePeriodIsLatestComplete():
    start, end = c360.fourFourFivePeriod(date(2016, 5, 31))
    assert (start, end) == (date(2016, 4, 29), date(2016, 5, 26))
    start, end = c360.fourFourFivePeriod(date(2016, 1, 10))
    assert end.year == 2015


def featureRow(**overrides):
    base = dict(
        CustomerKey=1,
        DaysSinceLastOrder=10,
        AverageOrderGapDays=30.0,
        AverageBasketAmount=100.0,
        PriorAverageBasketAmount=100.0,
        ReturnRatePercent=0.0,
        TierCode="NONE",
        PreviousTierCode="NONE",
        IsOnCreditHold=False,
    )
    base.update(overrides)
    return Row(**base)


def test_scoreChurnRulesAndBands(spark):
    features = spark.createDataFrame(
        [
            featureRow(CustomerKey=1),
            featureRow(CustomerKey=2, DaysSinceLastOrder=61),
            featureRow(CustomerKey=3, DaysSinceLastOrder=60),
            featureRow(CustomerKey=4, AverageBasketAmount=69.0),
            featureRow(CustomerKey=5, AverageBasketAmount=70.0),
            featureRow(CustomerKey=6, ReturnRatePercent=15.01),
            featureRow(CustomerKey=7, TierCode="GLD", PreviousTierCode="PLT"),
            featureRow(CustomerKey=8, TierCode="GLD", PreviousTierCode="NONE"),
            featureRow(CustomerKey=9, IsOnCreditHold=True),
            featureRow(CustomerKey=10, DaysSinceLastOrder=61, IsOnCreditHold=True, ReturnRatePercent=20.0),
            featureRow(CustomerKey=11, AverageOrderGapDays=0.0, DaysSinceLastOrder=5000),
        ]
    )
    rows = {r.CustomerKey: r for r in c360.scoreChurn(features).collect()}
    assert rows[1].ChurnRiskScore == 0 and rows[1].ChurnRiskBand == "LOW"
    assert rows[2].RuleOrderGapScore == 30 and rows[3].RuleOrderGapScore == 0
    assert rows[4].RuleBasketDeclineScore == 20 and rows[5].RuleBasketDeclineScore == 0
    assert rows[6].RuleReturnsScore == 15
    assert rows[7].RuleTierLapseScore == 15 and rows[8].RuleTierLapseScore == 0
    assert rows[9].RuleCreditHoldScore == 25 and rows[9].ChurnRiskBand == "LOW"
    assert rows[10].ChurnRiskScore == 70 and rows[10].ChurnRiskBand == "HIGH" and rows[10].IsHighRisk
    assert rows[11].RuleOrderGapScore == 0  # no gap history -> rule cannot fire


def test_assignSegmentsPrecedence(spark):
    def prof(k, region="NA", contactable=True):
        return Row(CustomerKey=k, CustomerId=k, RegionCode=region, IsContactable=contactable)

    def roll(k, r=5, f=5, m=5, status="ACTIVE"):
        return Row(CustomerKey=k, RecencyDecile=r, FrequencyDecile=f, MonetaryDecile=m, ActivityStatusCode=status)

    def churn(k, band="LOW"):
        return Row(CustomerKey=k, ChurnRiskBand=band, ChurnRiskScore=0)

    profile = spark.createDataFrame([prof(1, "EU", False), prof(2), prof(3), prof(4), prof(5), prof(6), prof(7), prof(8), prof(9)])
    rolling = spark.createDataFrame(
        [
            roll(1, m=10, f=10),
            roll(2, m=9, f=9),
            roll(3, m=5),
            roll(4, m=10, f=9),
            roll(5, r=9, f=2),
            roll(6),
            roll(7, status="INACTIVE"),
            roll(8),
            roll(9, m=9, f=8, status="INACTIVE"),
        ]
    )
    churnDf = spark.createDataFrame([churn(1, "HIGH"), churn(2, "HIGH"), churn(3, "HIGH"), churn(4), churn(5), churn(6), churn(7), churn(8), churn(9)])
    overlay = spark.createDataFrame([Row(CustomerId=6, TierCode="PLATINUM"), Row(CustomerId=7, TierCode="GOLD")])
    previous = spark.createDataFrame([Row(CustomerId=2, SegmentCode="CORE"), Row(CustomerId=8, SegmentCode="CORE")])
    result = c360.assignSegments(profile, rolling, churnDf, overlay, previous)
    seg = {r.CustomerId: (r.SegmentCode, r.SegmentMovementCode) for r in result.collect()}
    assert 1 not in seg  # suppressed EU non-contactable rows are not published by default
    assert seg[2] == ("AT_RISK_HIGH_VALUE", "CHANGED")
    assert seg[3] == ("AT_RISK", "NEW")
    assert seg[4][0] == "CHAMPION"
    assert seg[5][0] == "NEW_PROMISING"
    assert seg[6][0] == "LOYAL_PREMIUM"
    assert seg[7][0] == "LOYAL_PREMIUM"  # premium tier beats dormant
    assert seg[8] == ("CORE", "UNCHANGED")
    assert seg[9][0] == "CHAMPION"
    withSuppressed = c360.assignSegments(profile, rolling, churnDf, overlay, previous, publishSuppressed=True)
    assert {r.SegmentCode for r in withSuppressed.where(F.col("CustomerId") == 1).collect()} == {"SUPPRESSED"}


def test_loyaltyOverlayAccrualExpiryAndTiers(spark):
    qualifying = spark.createDataFrame(
        [
            Row(CustomerId=1, RegionCode="NA", InvoiceNumber="A", InvoiceDate=date(2016, 1, 1), GrossAmount=120.99, NetAmount=100.0, GstAmount=0.0),
            Row(CustomerId=1, RegionCode="NA", InvoiceNumber="B", InvoiceDate=date(2013, 1, 1), GrossAmount=50000.0, NetAmount=1.0, GstAmount=0.0),
            Row(CustomerId=2, RegionCode="EU", InvoiceNumber="C", InvoiceDate=date(2016, 1, 1), GrossAmount=999.0, NetAmount=100.0, GstAmount=0.0),
            Row(CustomerId=3, RegionCode="APAC", InvoiceNumber="D", InvoiceDate=date(2016, 1, 1), GrossAmount=110.0, NetAmount=100.0, GstAmount=10.0),
        ]
    )
    ledger = c360.accrueNewPoints(None, qualifying, NOW)
    pts = {(r.CustomerId, r.SourceReference): (r.PointsAccrued, float(r.QualifyingAmount)) for r in ledger.collect()}
    assert pts[(1, "A")] == (120, 120.99)  # NA: floor(gross)
    assert pts[(2, "C")] == (150, 100.0)  # EU: floor(net*1.5)
    assert pts[(3, "D")] == (200, 100.0)  # APAC: floor((gross-gst)*2)
    # re-running accrual against the existing ledger inserts nothing
    assert c360.accrueNewPoints(ledger, qualifying, NOW).count() == 0
    expired = c360.expireAgedPoints(ledger, AS_OF, NOW)
    status = {r.SourceReference: r.PointStatusCode for r in expired.collect()}
    assert status == {"A": "ACTIVE", "B": "EXPIRED", "C": "ACTIVE", "D": "ACTIVE"}
    overlay = {r.CustomerId: r for r in c360.buildLoyaltyOverlay(expired, None, AS_OF).collect()}
    assert overlay[1].ActivePoints == 120 and overlay[1].ExpiredPoints == 50000 and overlay[1].TierMovementCode == "NEW"
    assert float(overlay[1].LifetimeQualifyingAmount) == 50120.99


def test_identityGraphMatchesTaxNumberEmailAndNamePostcode(spark):
    customers = spark.createDataFrame(
        [
            Row(
                CustomerKey=1,
                CustomerId=1,
                CustomerName="Tailspin Toys (Head Office)",
                PostalCode="90210",
                RegionCode="NA",
                CountryCode="USA",
                ContactEmail="Sales@Tailspin.com",
                ContactPhone="(555) 123-4567",
                TaxRegistrationNumber="US-1",
            ),
            Row(
                CustomerKey=2,
                CustomerId=2,
                CustomerName="Tailspin  Toys (Head Office)",
                PostalCode="90210-1234",
                RegionCode="NA",
                CountryCode="USA",
                ContactEmail="other@tailspin.com",
                ContactPhone=None,
                TaxRegistrationNumber=None,
            ),
            Row(
                CustomerKey=3,
                CustomerId=3,
                CustomerName="Wingtip",
                PostalCode="10115",
                RegionCode="EU",
                CountryCode="DEU",
                ContactEmail="sales@tailspin.com",
                ContactPhone=None,
                TaxRegistrationNumber="DE-9",
            ),
            Row(
                CustomerKey=4,
                CustomerId=4,
                CustomerName="Unrelated",
                PostalCode="2000",
                RegionCode="APAC",
                CountryCode="AUS",
                ContactEmail=None,
                ContactPhone=None,
                TaxRegistrationNumber=None,
            ),
        ]
    )
    graph = {r.CustomerId: r for r in c360.resolveIdentityGraph(c360.standardiseAddresses(customers)).collect()}
    assert graph[2].SurvivingCustomerId == 1  # same normalised name + ZIP5
    assert graph[3].SurvivingCustomerId == 1  # same e-mail
    assert graph[4].SurvivingCustomerId == 4
