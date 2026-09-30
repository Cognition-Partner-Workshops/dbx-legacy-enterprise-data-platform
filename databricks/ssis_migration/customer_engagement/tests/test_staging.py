from datetime import datetime

from pyspark.sql import Row

from customer_engagement import staging

NOW = datetime(2016, 6, 1, 0, 0, 0)


def rawLedgerRow(**overrides):
    base = dict(
        LoyaltyLedgerID="1",
        CustomerID="10",
        LoyaltyMemberID="5",
        EntryTypeCode="EARN",
        ProgramCode="BASE",
        RegionCode="NA",
        TierCode="BRZ",
        PointsDelta="120",
        PointsBalanceAfter="120",
        SourceInvoiceID="7",
        RedemptionReference="R",
        EntryWhen="2016-01-15T10:00:00",
        ExpiryDate=None,
        LastEditedWhen="2016-01-15T10:00:00",
        BatchId=1,
    )
    base.update(overrides)
    return Row(**base)


def test_typeLoyaltyEntriesDefaultsAndSignedSplit(spark):
    raw = spark.createDataFrame(
        [
            rawLedgerRow(LoyaltyLedgerID="1", EntryTypeCode=None, ProgramCode=" ", RegionCode=None, PointsDelta=None),
            rawLedgerRow(LoyaltyLedgerID="2", EntryTypeCode="redeem", PointsDelta="-40", RegionCode="EU"),
            rawLedgerRow(LoyaltyLedgerID="3", EntryTypeCode="EARN", PointsDelta="500", RegionCode="APAC", ExpiryDate="2016-12-31"),
        ]
    )
    typed = {r.LoyaltyEntryId: r for r in staging.typeLoyaltyEntries(raw, "NA").collect()}
    assert typed[1].EntryTypeCode == "ADJ" and typed[1].ProgramCode == "BASE" and typed[1].RegionCode == "NA"
    assert typed[1].PointsDelta == 0 and typed[1].PointsEarned == 0 and typed[1].PointsRedeemed == 0
    assert typed[2].EntryTypeCode == "REDEEM" and typed[2].PointsEarned == 0 and typed[2].PointsRedeemed == 40
    assert typed[3].PointsEarned == 500 and typed[3].PointsRedeemed == 0
    # regional expiry defaults: NA 24 months, EU 12 months, explicit expiry wins
    assert str(typed[1].ExpiryDate) == "2018-01-15"
    assert str(typed[2].ExpiryDate) == "2017-01-15"
    assert str(typed[3].ExpiryDate) == "2016-12-31"


def test_aggregateCustomerPointsTierThresholds(spark):
    raw = spark.createDataFrame(
        [
            rawLedgerRow(LoyaltyLedgerID="1", CustomerID="1", PointsDelta="60000", ExpiryDate="2020-01-01"),
            rawLedgerRow(LoyaltyLedgerID="2", CustomerID="1", EntryTypeCode="REDEEM", PointsDelta="-5000"),
            rawLedgerRow(LoyaltyLedgerID="3", CustomerID="2", PointsDelta="20000"),
            rawLedgerRow(LoyaltyLedgerID="4", CustomerID="3", PointsDelta="5000"),
            rawLedgerRow(LoyaltyLedgerID="5", CustomerID="4", PointsDelta="10"),
        ]
    )
    agg = {r.CustomerId: r for r in staging.aggregateCustomerPoints(staging.typeLoyaltyEntries(raw, "NA")).collect()}
    assert agg[1].NetPointsBalance == 55000 and agg[1].LoyaltyTierCode == "PLT"
    assert agg[2].LoyaltyTierCode == "GLD"
    assert agg[3].LoyaltyTierCode == "SLV"
    assert agg[4].LoyaltyTierCode == "BRZ"


def rawSessionRow(**overrides):
    base = dict(
        WebSessionID="0f8fad5b-d9cb-469f-a165-70867728950e",
        CustomerID="10",
        AnonymousVisitorKey="v-1",
        DeviceCategory="mobile",
        CountryCode="usa",
        BrowserFamily="Chrome",
        LandingPageUrl="https://shop.wwi.com/deals?x=1",
        PageViewCount="4",
        SessionStartedWhen="2016-03-01 10:00:00",
        SessionEndedWhen="2016-03-01 10:10:00",
        ConsentCategories="OPTIN",
        CartCreatedFlag="Y",
        CampaignCode="SPRING",
        BatchId=1,
    )
    base.update(overrides)
    return Row(**base)


def test_conformWebSessionConsentAndScreening(spark):
    raw = spark.createDataFrame(
        [
            rawSessionRow(WebSessionID="0f8fad5b-d9cb-469f-a165-70867728950e"),
            rawSessionRow(WebSessionID="1f8fad5b-d9cb-469f-a165-70867728950e", CountryCode="DEU", ConsentCategories="OPTOUT"),
            rawSessionRow(WebSessionID="2f8fad5b-d9cb-469f-a165-70867728950e", CountryCode="DEU", ConsentCategories="OPTIN", PageViewCount="1"),
            rawSessionRow(WebSessionID="short-key"),
            rawSessionRow(WebSessionID="3f8fad5b-d9cb-469f-a165-70867728950e", SessionEndedWhen="2016-03-03 10:00:00"),
        ]
    )
    valid, rejects = staging.conformWebSession(raw, None, "NA", NOW, 1)
    v = {r.SessionBusinessKey: r for r in valid.collect()}
    assert set(v) == {"0F8FAD5B-D9CB-469F-A165-70867728950E", "1F8FAD5B-D9CB-469F-A165-70867728950E", "2F8FAD5B-D9CB-469F-A165-70867728950E"}
    na = v["0F8FAD5B-D9CB-469F-A165-70867728950E"]
    assert na.RegionCode == "NA" and na.CustomerId == 10 and na.UserAgentFamily == "Chrome" and na.LandingPagePath == "shop.wwi.com/deals?x=1"
    assert na.DeviceTypeCode == "MOBILE" and na.CountryIsoCode == "USA" and na.SessionDurationSeconds == 600 and na.BounceFlag == "N"
    euNoConsent = v["1F8FAD5B-D9CB-469F-A165-70867728950E"]
    assert euNoConsent.RegionCode == "EU" and euNoConsent.CustomerId == -1 and euNoConsent.UserAgentFamily == "SUPPRESSED"
    assert euNoConsent.CustomerBusinessKey is None
    euConsent = v["2F8FAD5B-D9CB-469F-A165-70867728950E"]
    assert euConsent.CustomerId == 10 and euConsent.BounceFlag == "Y"
    rj = {r.BusinessKey: r.RejectReasonCode for r in rejects.collect()}
    assert rj["SHORT-KEY"] == "MALFORMED_SESSION_KEY"
    assert rj["3F8FAD5B-D9CB-469F-A165-70867728950E"] == "IMPLAUSIBLE_DURATION"
