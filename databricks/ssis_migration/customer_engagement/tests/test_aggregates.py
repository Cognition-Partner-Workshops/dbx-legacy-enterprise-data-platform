from datetime import date

from pyspark.sql import Row

from customer_engagement import aggregates

AS_AT = date(2016, 5, 31)


def saleRow(customerKey, invoice, invoiceDate, amount, qty=1):
    return Row(
        CustomerKey=customerKey,
        InvoiceNumber=invoice,
        InvoiceDate=invoiceDate,
        NetAmount=amount,
        GrossAmount=amount,
        TotalExcludingTax=amount,
        TaxAmount=0.0,
        Quantity=qty,
        StockItemKey=1,
        MarginAmount=amount / 2,
        NetAmountReporting=amount,
        FxRateToReporting=1.0,
        CorrectionTypeCode="ORIG",
        SaleRegionCode="NA",
    )


def custRow(k, region="NA", credit=500.0, consent=True, erased=None, current=True):
    return Row(
        CustomerKey=k,
        CustomerId=k,
        CustomerName=f"C{k}",
        ContactEmail=f"c{k}@x.com",
        RegionCode=region,
        IsCurrentRow=current,
        CreditLimitAmount=credit,
        MarketingConsentFlag=consent,
        ErasureRequestedOn=erased,
        CustomerSegmentKey=7,
        AccountManagerEmployeeKey=1,
    )


def test_customer360ScoringRoutingAndErasure(spark):
    sales = spark.createDataFrame(
        [
            saleRow(1, "A", date(2016, 5, 1), 30000.0),
            saleRow(1, "B", date(2016, 5, 2), 100.0),
            saleRow(2, "C", date(2013, 1, 1), 10.0),  # inactive (> 730 days)
            saleRow(3, "D", date(2015, 1, 1), 1000.0),  # 516 days: recency 60 + no web 15 = 75 HIGH
            saleRow(4, "E", date(2016, 5, 30), 500.0),
            saleRow(5, "F", date(2016, 5, 30), 500.0),
        ]
    )
    customers = spark.createDataFrame(
        [
            custRow(1, credit=1000.0, consent=None),
            custRow(2),
            custRow(3),
            custRow(4, region="EU", consent=True, erased=date(2016, 1, 1)),
            custRow(5, current=False),
            custRow(6, region="APAC", consent=None),
        ]
    )
    web = spark.createDataFrame([Row(CustomerKey=1, SessionCount=5, SessionCount90Day=2)])
    rows, rejects = aggregates.buildCustomer360(customers, sales, None, None, web, AS_AT, False, 42)
    out = {r.CustomerKey: r for r in rows.collect()}
    rj = [r.CustomerKey for r in rejects.collect()]
    assert rj == [2] and set(out) == {1, 3, 4, 6}
    c1 = out[1]
    assert c1.LifetimeOrderCount == 2 and float(c1.LifetimeNetAmount) == 30100.0 and float(c1.AverageOrderValue) == 15050.0
    assert c1.RfmScore == "312" and c1.ChurnRiskScore == 0 and c1.ChurnRiskBand == "LOW" and c1.RouteCode == "ACTIVE_CUSTOMERS"
    assert c1.MarketingConsentPublished is True  # NA: absence of consent is a yes
    c3 = out[3]
    assert c3.RecencyDays == 516 and c3.ChurnRiskScore == 75 and c3.ChurnRiskBand == "HIGH"
    c4 = out[4]
    assert c4.RouteCode == "ERASED_CUSTOMERS" and c4.CustomerName == "REDACTED" and c4.PrimaryContactEmail is None
    assert c4.PublishedSegmentCode == "REDACTED" and c4.MarketingConsentPublished is False and c4.AnonymisedFlag
    assert str(c4.RetentionExpiryDate) == "2023-05-30"
    c6 = out[6]
    assert c6.RouteCode == "NO_PURCHASE_HISTORY" and c6.ChurnRiskScore == 100 and c6.MarketingConsentPublished is False
    assert str(c6.RetentionExpiryDate) == "2019-05-31"


def test_rolling12MonthTrendCodesAndPeriods(spark):
    erasedDate = date(2016, 1, 1)
    sales = spark.createDataFrame(
        [
            saleRow(1, "A", date(2015, 3, 15), 1000.0),
            saleRow(1, "B", date(2016, 5, 15), 100.0),
            saleRow(1, "B", date(2016, 5, 15), -50.0, qty=-1),
            saleRow(2, "C", date(2016, 5, 1), 10.0),
        ]
    )
    customers = spark.createDataFrame([custRow(1, erased=erasedDate), custRow(2)])
    rows = aggregates.buildCustomerRolling12Month(sales, customers, "2016-05", 12, None, 1)
    out = {(r.CustomerKey, r.AccountingPeriodCode): r for r in rows.collect()}
    periods = sorted({p for _, p in out})
    assert periods[0] == "2015-06" and periods[-1] == "2016-05" and "2016-04" not in periods  # no window sales -> no row
    assert out[(1, "2016-02")].TrendCode == "STABLE" and float(out[(1, "2016-02")].RollingNetAmount) == 1000.0
    # March 2016 window (2015-03-31 .. 2016-03-31) drops the 2015-03-15 invoice -> nothing left, so no row
    assert (1, "2016-03") not in out
    may = out[(1, "2016-05")]
    assert float(may.RollingNetAmount) == 50.0 and may.RollingReturnCount == 1 and float(may.NetReturnRatePercent) == 100.0
    assert may.TrendCode == "NEW" and may.RouteCode == "NEW_CUSTOMERS"  # no prior period row -> NEW
    assert out[(2, "2016-05")].RouteCode == "NEW_CUSTOMERS"
    assert out[(1, "2015-06")].PriorRollingNetAmount == 0 and out[(1, "2015-07")].TrendCode == "STABLE"
