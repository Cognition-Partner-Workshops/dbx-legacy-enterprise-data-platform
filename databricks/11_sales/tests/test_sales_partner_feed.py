import os
from datetime import date
from decimal import Decimal

import sales_partner_feed as feed


def _money(v):
    return Decimal(str(v)).quantize(Decimal("0.01"))


def _inputs(spark):
    factSale = feed.legacyFactSale(spark.createDataFrame(
        [(1, 10, "INV-2", date(2024, 3, 9), 2, _money(100), "USD"),
         (2, 11, "INV-1", date(2024, 3, 9), 1, _money(200), "GBP"),
         (3, 11, "INV-3", date(2024, 3, 9), 1, _money(50), "GBP"),
         (1, 10, "INV-0", date(2024, 3, 1), 1, _money(999), "USD")],   # too old
        "`Customer Key` int, `Stock Item Key` int, `WWI Invoice ID` string, `Invoice Date Key` date, "
        "Quantity int, `Total Excluding Tax` decimal(18,2), `Transaction Currency Code` string"))
    dimCustomer = feed.legacyDimCustomer(spark.createDataFrame(
        [(1, 100, "NA", False, "ACME-1"), (2, 100, "EU", True, "EU-OK"), (3, 100, "EU", False, "EU-NOCONSENT")],
        "CustomerKey int, PartnerKey int, RegionCode string, ShareConsentFlag boolean, SourceCustomerReference string"))
    dimStockItem = feed.legacyDimStockItem(spark.createDataFrame([(10, "SKU-10"), (11, "SKU-11")], "StockItemKey int, StockItemCode string"))
    dimPartner = feed.legacyDimPartner(spark.createDataFrame([(100, "PARTNER-A", "EUR")], "PartnerKey int, PartnerCode string, SettlementCurrencyCode string"))
    fx = feed.legacyFxRevaluationRates(spark.createDataFrame(
        [("GBP", "EUR", "AVERAGE", Decimal("1.20000000")), ("USD", "EUR", "CLOSING", Decimal("0.50000000"))],
        "CurrencyCode string, QuoteCurrencyCode string, RateTypeCode string, ConversionRate decimal(18,8)"))
    return factSale, dimCustomer, dimStockItem, dimPartner, fx


def test_build_rows_redaction_fx_and_scope(spark):
    factSale, dimCustomer, dimStockItem, dimPartner, fx = _inputs(spark)
    rows = feed.buildPartnerFeedRows(factSale, dimCustomer, dimStockItem, dimPartner, fx, "ALL", date(2024, 3, 8))
    out = {r["InvoiceNumber"]: r for r in rows.collect()}
    assert sorted(out) == ["INV-1", "INV-2", "INV-3"]
    assert out["INV-2"]["CustomerReference"] == "ACME-1" and out["INV-2"]["NetAmount"] == Decimal("100.00")  # no AVERAGE USD rate -> 1
    assert out["INV-1"]["CustomerReference"] == "EU-OK" and out["INV-1"]["NetAmount"] == Decimal("240.00")
    assert out["INV-3"]["CustomerReference"] == "REDACTED"
    kept = feed.suppressUnconsentedEuRows(rows)
    assert sorted(r["InvoiceNumber"] for r in kept.collect()) == ["INV-1", "INV-2"]
    scoped = feed.buildPartnerFeedRows(factSale, dimCustomer, dimStockItem, dimPartner, fx, "PARTNER-Z", date(2024, 3, 8))
    assert scoped.count() == 0


def test_render_and_write_feed(spark, tmp_path):
    factSale, dimCustomer, dimStockItem, dimPartner, fx = _inputs(spark)
    rows = feed.buildPartnerFeedRows(factSale, dimCustomer, dimStockItem, dimPartner, fx, "ALL", date(2024, 3, 8))
    content = feed.renderFeed(feed.orderedFeedRows(rows).toLocalIterator())
    lines = content.split("\r\n")
    assert lines[0] == "PartnerCode,InvoiceNumber,InvoiceDate,CustomerReference,StockItemCode,Quantity,NetAmount,SettlementCurrencyCode,RegionCode"
    assert lines[1] == "PARTNER-A,INV-1,2024-03-09,EU-OK,SKU-11,1,240.00,EUR,EU"
    assert lines[2] == "PARTNER-A,INV-2,2024-03-09,ACME-1,SKU-10,2,100.00,EUR,NA"
    assert lines[3] == "PARTNER-A,INV-3,2024-03-09,REDACTED,SKU-11,1,60.00,EUR,EU"
    assert lines[4] == "" and len(lines) == 5
    businessDate = date(2024, 3, 9)
    fileName = feed.outboundFileName(businessDate)
    assert fileName == "partner_feed_20240309.csv"
    outbound, archive = feed.writeFeedFiles(content, str(tmp_path), fileName, businessDate)
    assert outbound == os.path.join(str(tmp_path), "outbound", "partner_feed", fileName)
    assert archive == os.path.join(str(tmp_path), "archive", "partner_feed", "2024", "03", fileName)
    with open(outbound, "rb") as handle:
        raw = handle.read()
    assert raw == content.encode("cp1252")
    assert open(archive, "rb").read() == raw


def test_render_encodes_cp1252_characters():
    content = feed.renderFeed([{"PartnerCode": "P", "InvoiceNumber": "1", "InvoiceDate": date(2024, 1, 1),
                                "CustomerReference": "Müller", "StockItemCode": "S", "Quantity": Decimal("1.5"),
                                "NetAmount": Decimal("1.005"), "SettlementCurrencyCode": "EUR", "RegionCode": "EU"}])
    assert "Müller" in content and ",1.5,1.01," in content
    assert content.encode("cp1252")
