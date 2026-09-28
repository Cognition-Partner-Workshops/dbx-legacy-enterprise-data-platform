from datetime import date, datetime

from c360_lib import profile as P

AS_OF = date(2024, 6, 30)
FUTURE = datetime(9999, 12, 31)


def addrRows(spark, rows):
    return spark.createDataFrame(rows, "CustomerId int, RegionCode string, CountryCode string, RawPostalCode string, RawEmail string, TaxRegistrationNumber string, CustomerNameNormalised string, HouseholdNameKey string")


def test_standardise_addresses_by_region(spark):
    df = addrRows(spark, [
        (1, "NA", "US", "902 10-1234", " Bob@X.com ", None, "BOB", "BOB"),
        (2, "EU", "DE", "10 115", "a@b.de", None, "A", "A"),
        (3, "APAC", "AU", "nsw 2000", "c@d.au", None, "C", "C"),
        (4, "LATAM", "BR", "01310 x", "e@f.br", None, "E", "E"),
    ])
    out = {r.CustomerId: r for r in P.standardiseAddresses(df, "2024-06-30 00:00:00").collect()}
    assert out[1].StandardPostalCode == "90210" and out[1].StandardEmail == "bob@x.com"
    assert out[2].StandardPostalCode == "DE-10115"
    assert out[3].StandardPostalCode == "NSW2000"
    assert out[4].StandardPostalCode == "01310 X"


def test_identity_graph_scores_and_survivor(spark):
    df = P.standardiseAddresses(addrRows(spark, [
        (10, "NA", "US", "11111", "same@x.com", "TAX1", "ACME", "ACME"),
        (11, "NA", "US", "22222", "SAME@x.com", None, "OTHER", "OTHER"),        # email match 95 -> survivor 10
        (12, "NA", "US", "11111", "diff@x.com", None, "ACME", "ACME"),          # name+postal 88 to 10, self email 95 -> MAX 95, survivor 10
        (13, "NA", "US", "33333", "z@x.com", "TAX1", "ZED", "ZED"),             # tax 100 -> survivor 10
        (14, "NA", "US", "44444", "lonely@x.com", None, "LONELY", "LONELY"),    # self-match on own email -> 95, own survivor
        (15, "NA", "US", "55555", None, None, "NOEMAIL", "NOEMAIL"),               # self-match on name+postal -> 88
    ]), "2024-06-30 00:00:00")
    out = {r.CustomerId: r for r in P.resolveIdentityGraph(df, 85).collect()}
    assert set(out) == {10, 11, 12, 13, 14, 15}
    assert out[14].SurvivingCustomerId == 14 and out[14].MatchScore == 95
    assert out[15].SurvivingCustomerId == 15 and out[15].MatchScore == 88
    assert out[11].SurvivingCustomerId == 10 and out[11].MatchScore == 95
    assert out[12].SurvivingCustomerId == 10 and out[12].MatchScore == 95
    assert out[13].SurvivingCustomerId == 10 and out[13].MatchScore == 100
    assert out[10].SurvivingCustomerId == 10 and out[10].MatchScore == 100  # self-join on tax
    assert P.countDuplicateClusters(P.resolveIdentityGraph(df, 85)) == 1
    assert set(r.CustomerId for r in P.resolveIdentityGraph(df, 90).collect()) == {10, 11, 12, 13, 14}


def profileSource(spark, rows):
    schema = ("CustomerKey int, CustomerId int, CustomerName string, CustomerCategory string, BuyingGroup string, "
              "PostalCode string, RegionCode string, CountryCode string, ContactEmail string, ContactPhone string, "
              "MarketingConsentFlag boolean, ConsentCapturedDate date, FirstOrderDate date, LastOrderDate date, "
              "LifetimeOrderCount long, LifetimeNetAmount decimal(18,2), LastPaymentDate date, "
              "OpenBalanceAmount decimal(18,2), AveragePaymentDays decimal(18,2)")
    return spark.createDataFrame(rows, schema)


def test_consent_rules_and_profile_attributes(spark):
    from decimal import Decimal
    src = profileSource(spark, [
        (1, 100, "EU no consent", "Cat", "BG", "1", "EU", "DE", "eu@x.de", None, None, None, date(2024, 1, 1), date(2024, 6, 1), 4, Decimal("100.00"), None, None, None),
        (2, 101, "EU consent", "Cat", "BG", "1", "EU", "DE", "ok@x.de", None, True, None, None, None, 0, Decimal("0.00"), None, None, None),
        (3, 102, "APAC old", "Cat", "BG", "1", "APAC", "AU", "a@x.au", None, True, date(2021, 1, 1), None, None, None, None, None, None, None),
        (4, 103, "APAC fresh", "Cat", "BG", "1", "APAC", "AU", "b@x.au", None, True, date(2024, 1, 1), None, None, None, None, None, None, None),
        (5, 104, "NA", "Cat", "BG", "1", "NA", "US", "n@x.us", None, None, None, None, None, None, None, None, None, None),
    ])
    graph = spark.createDataFrame([(101, 100, 95)], "CustomerId int, SurvivingCustomerId int, MatchScore int")
    out = {r.CustomerId: r for r in P.buildCustomerProfile(src, graph, AS_OF).collect()}
    assert out[100].IsContactable is False and out[100].ContactEmail == "WITHHELD"
    assert out[101].IsContactable is True and out[101].ContactEmail == "ok@x.de"
    assert out[102].IsContactable is False and out[103].IsContactable is True
    assert out[104].IsContactable is True
    assert out[101].MasterCustomerId == 100 and out[100].MasterCustomerId == 100 and out[104].MasterCustomerId == 104
    assert out[100].TenureDays == (AS_OF - date(2024, 1, 1)).days and out[101].TenureDays == 0
    assert out[100].AverageOrderValue == Decimal("25.00") and out[101].AverageOrderValue == Decimal("0.00")


def test_households_dense_rank(spark):
    profile = spark.createDataFrame([(1,), (2,), (3,), (4,)], "CustomerId int")
    addr = spark.createDataFrame([
        (1, "90210", "SMITH"), (2, "90210", "SMITH"), (3, "90210", "JONES"), (4, "10001", "SMITH"),
    ], "CustomerId int, StandardPostalCode string, HouseholdNameKey string")
    out = {r.CustomerId: r.HouseholdKey for r in P.assignHouseholds(profile, addr).collect()}
    assert out[1] == out[2] and out[1] != out[3] and out[4] == 1 and out[3] == 2 and out[1] == 3


def test_active_customer_filter_and_sales_summary(spark):
    dim = spark.createDataFrame([
        (1, 5, FUTURE, "A B", "1", "NA", "US", "a@b", None, "G1"),
        (2, 6, datetime(2020, 1, 1), "Old", "1", "NA", "US", None, None, None),
        (3, 0, FUTURE, "Unknown", "1", "NA", "US", None, None, None),
    ], "CustomerKey int, WWICustomerID int, ValidTo timestamp, Customer string, PostalCode string, RegionCode string, "
       "CountryCode string, PrimaryContactEmail string, VATRegistrationNumber string, GSTRegistrationNumber string")
    out = P.buildAddressStandardisationInput(dim, AS_OF).collect()
    assert [r.CustomerId for r in out] == [5]
    assert out[0].TaxRegistrationNumber == "G1" and out[0].CustomerNameNormalised == "AB"

    sale = spark.createDataFrame([
        (1, date(2024, 1, 1), 100, 10.0), (1, date(2024, 3, 1), 100, 5.0), (1, date(2024, 5, 1), 101, 15.0),
    ], "CustomerKey int, InvoiceDateKey date, WWIInvoiceID int, TotalExcludingTax double")
    s = P.buildCustomerSalesSummary(sale).collect()[0]
    assert s.LifetimeOrderCount == 2 and float(s.LifetimeNetAmount) == 30.0
    assert s.FirstOrderDate == date(2024, 1, 1) and s.LastOrderDate == date(2024, 5, 1)
