from datetime import date, datetime

from pyspark.sql import Row

from customer_engagement import extracts

NOW = datetime(2016, 6, 1)


def test_loyaltyExtractIncrementalKeyPlusExpiryLookback(spark):
    ledger = spark.createDataFrame(
        [
            Row(
                LoyaltyLedgerID=1,
                LoyaltyMemberID=1,
                EntryWhen=datetime(2016, 1, 1),
                EntryTypeCode="EARN",
                PointsDelta=10,
                PointsRemaining=10,
                SourceInvoiceID=1,
                SourceReference="r",
                ExpiresOnDate=date(2018, 1, 1),
                ExpiredWhen=None,
            ),
            Row(
                LoyaltyLedgerID=2,
                LoyaltyMemberID=1,
                EntryWhen=datetime(2016, 2, 1),
                EntryTypeCode="EARN",
                PointsDelta=10,
                PointsRemaining=0,
                SourceInvoiceID=2,
                SourceReference="r",
                ExpiresOnDate=date(2016, 5, 1),
                ExpiredWhen=datetime(2016, 5, 30),
            ),
            Row(
                LoyaltyLedgerID=3,
                LoyaltyMemberID=1,
                EntryWhen=datetime(2016, 3, 1),
                EntryTypeCode="EARN",
                PointsDelta=10,
                PointsRemaining=10,
                SourceInvoiceID=3,
                SourceReference="r",
                ExpiresOnDate=None,
                ExpiredWhen=None,
            ),
        ]
    )
    members = spark.createDataFrame([Row(LoyaltyMemberID=1, CustomerID=10, LoyaltyProgramID=1, CurrentTierID=1)])
    programs = spark.createDataFrame([Row(LoyaltyProgramID=1, ProgramCode="BASE", RegionCode="NA")])
    tiers = spark.createDataFrame([Row(LoyaltyTierID=1, TierCode="BRZ")])
    out = extracts.transformLoyaltyLedgerExtract(ledger, members, programs, tiers, lastLedgerId=2, lookbackDays=7, now=NOW, batchId=1)
    ids = sorted(int(r.LoyaltyLedgerID) for r in out.collect())
    assert ids == [2, 3]  # 3 is new by key; 2 re-extracted because it expired inside the lookback


def test_webSessionExtractWindowAndEuSuppression(spark):
    sessions = spark.createDataFrame(
        [
            Row(
                WebSessionID=1,
                SessionGuid="a" * 36,
                CustomerID=1,
                RegionCode="NA",
                StartedWhen=datetime(2016, 3, 1),
                EndedWhen=datetime(2016, 3, 1, 1),
                DurationSeconds=3600,
                DeviceCategory="MOBILE",
                BrowserFamily="Chrome",
                LandingPageUrl="/",
                ReferrerUrl="https://g.com",
                CampaignCode="c",
                PageViewCount=5,
                IpAddressText="1.1.1.1",
                CountryISO3="USA",
                ConsentStateCode="OPTOUT",
            ),
            Row(
                WebSessionID=2,
                SessionGuid="b" * 36,
                CustomerID=2,
                RegionCode="EU",
                StartedWhen=datetime(2016, 3, 2),
                EndedWhen=datetime(2016, 3, 2, 1),
                DurationSeconds=3600,
                DeviceCategory="DESKTOP",
                BrowserFamily="Firefox",
                LandingPageUrl="/",
                ReferrerUrl="https://g.com",
                CampaignCode="c",
                PageViewCount=5,
                IpAddressText="2.2.2.2",
                CountryISO3="DEU",
                ConsentStateCode="OPTOUT",
            ),
            Row(
                WebSessionID=3,
                SessionGuid="c" * 36,
                CustomerID=3,
                RegionCode="EU",
                StartedWhen=datetime(2016, 4, 2),
                EndedWhen=None,
                DurationSeconds=None,
                DeviceCategory="DESKTOP",
                BrowserFamily="Firefox",
                LandingPageUrl="/",
                ReferrerUrl=None,
                CampaignCode="c",
                PageViewCount=5,
                IpAddressText="3.3.3.3",
                CountryISO3="DEU",
                ConsentStateCode="OPTIN",
            ),
        ]
    )
    carts = spark.createDataFrame([Row(WebSessionID=1, ConvertedOrderID=77), Row(WebSessionID=1, ConvertedOrderID=None)])
    out = extracts.transformWebSessionExtract(sessions, carts, datetime(2016, 3, 1), datetime(2016, 3, 31), batchId=1, now=NOW)
    rows = {r.WebSessionID: r for r in out.collect()}
    assert set(rows) == {"a" * 36, "b" * 36}
    assert rows["a" * 36].CartCreatedFlag == "Y" and rows["a" * 36].OrderPlacedFlag == "Y" and rows["a" * 36].OrderID == "77"
    assert rows["a" * 36].ReferrerUrl == "https://g.com"
    assert rows["b" * 36].ReferrerUrl is None and rows["b" * 36].AnonymousVisitorKey is None
