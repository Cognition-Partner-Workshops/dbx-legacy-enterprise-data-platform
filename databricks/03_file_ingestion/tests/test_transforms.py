"""Package-by-package parsing rules (the Data Flow derived columns / conditional splits)."""

import datetime
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql import types as T

from wwi_file_ingestion import feeds, transforms
from wwi_file_ingestion.runner import landedRows, rejectedRows

from helpers import byRow, parseText

EU_HEADER = ",".join(feeds.PARTNER_SALES_EU.columns) + "\n"
APAC_COLS = feeds.PARTNER_SALES_APAC.columns
CARRIER_HEADER = ",".join(feeds.CARRIER_SCAN.columns) + "\n"
SUPPLIER_HEADER = "|".join(feeds.SUPPLIER_CATALOG.columns) + "\n"
FX_HEADER = ",".join(feeds.FX_OVERRIDE.columns) + "\n"


def test_eu_decimal_comma_vat_backout_and_consent(spark):
    text = EU_HEADER + (
        "1,PARTEU,,,,,,,,,,,,,,\n"
        "2,PARTEU,OUT1,R-1,17.05.2024,ART1,4006381333931,\"2,000\",\"119,00\",\"19,0\",DE123456789,EUR,DE,10115,J,\n"
        "2,PARTEU,OUT1,R-2,17.05.2024,ART2,4006381333932,1,10,0,DE1234,EUR,DE,10115,N,\n"
        "9,,,,,,,,,,,,,,,\"129,00\"\n"
    )
    parsed = parseText(spark, feeds.PARTNER_SALES_EU, text)
    rows = byRow(parsed)
    good = rows[3]
    assert good["RejectReasonCode"] is None and good["RecordClass"] == "Detail"
    assert good["TransactionDate"] == datetime.date(2024, 5, 17)
    assert good["Quantity"] == Decimal("2.000")
    assert good["GrossAmount"] == Decimal("119.0000")
    assert good["NetAmount"] == Decimal("100.0000") and good["VatAmount"] == Decimal("19.0000")
    assert good["MarketableFlag"] == "Y" and good["TaxTreatmentCode"] == "VAT"
    assert good["RegionCode"] == "EU"
    assert rows[4]["RejectReasonCode"] == "VAT_MISSING" and rows[4]["MarketableFlag"] == "N"
    assert rows[5]["RecordClass"] == "Footer" and rows[5]["RejectReasonCode"] == "FOOTER"
    assert rows[2]["RecordClass"] == "Header" and rows[2]["RejectReasonCode"] is None
    assert landedRows(parsed).count() == 1


def test_eu_quoted_fields_are_split_correctly(spark):
    text = EU_HEADER + '2,PARTEU,OUT1,R-1,17.05.2024,ART1,EAN,"1,5","10,50","19,0",DE123456789,EUR,DE,10115,Y,\n'
    row = byRow(parseText(spark, feeds.PARTNER_SALES_EU, text))[2]
    assert row["ActualColumnCount"] == 16
    assert row["Quantity"] == Decimal("1.500") and row["GrossAmount"] == Decimal("10.5000")


def test_apac_tab_separated_no_header_markers_and_postal_padding(spark):
    line = "\t".join
    text = (
        line(["#HEAD", "PARTAP", "", "", "", "", "", "", "", "", "", "", "", ""]) + "\n"
        + line(["", "PARTAP", "OUT9", "Sydney Outlet", "S-1", "2024/05/17", "ITEM1", "3", "30.00", "3.00", "AUD", "AU", "2000", ""]) + "\n"
        + line(["", "PARTAP", "OUT9", "Sydney Outlet", "S-2", "2024/05/17", "ITEM2", "1", "10.00", "1.00", "AUD", "AU", "  8", ""]) + "\n"
        + line(["", "PARTAP", "OUT9", "Sydney Outlet", "", "2024/05/17", "ITEM3", "1", "10.00", "1.00", "AUD", "AU", "2000", ""]) + "\n"
        + line(["#TOTAL", "", "", "", "", "", "", "", "", "", "", "", "", "44.00"]) + "\n"
    )
    parsed = parseText(spark, feeds.PARTNER_SALES_APAC, text)
    rows = byRow(parsed)
    assert rows[1]["RecordClass"] == "Header"
    assert rows[2]["RejectReasonCode"] is None
    assert rows[2]["GrossAmount"] == Decimal("33.0000") and rows[2]["TaxTreatmentCode"] == "GST"
    assert rows[2]["PostalCode"] == "002000"
    assert rows[3]["PostalCode"] == "000008"
    assert rows[4]["RejectReasonCode"] == "MALFORMED"  # SlipNumber missing
    assert rows[5]["RecordClass"] == "Footer" and rows[5]["FooterTotalText"] == "44.00"
    assert rows[2]["RegionCode"] == "APAC"


def test_na_record_types_and_unknown_type(spark):
    header = ",".join(feeds.PARTNER_SALES_NA.columns) + "\n"
    text = header + (
        "X,ACME,0101,T-9,05/17/2024,P-1,012,1,1.00,0,0,1.00,USD,WA,98101,\n"
        "D,ACME,0101,T-1,13/17/2024,P-1,012,1,1.00,0,0,1.00,USD,WA,98101,\n"
        "D,ACME,0101,T-2,05/17/2024,P-1,012,0,1.00,0,0,1.00,USD,WA,98101,\n"
    )
    rows = byRow(parseText(spark, feeds.PARTNER_SALES_NA, text))
    assert rows[2]["RejectReasonCode"] == "UNKNOWN_RECORD_TYPE"
    assert rows[3]["RejectReasonCode"] == "CONVERSION"  # month 13
    assert rows[4]["RejectReasonCode"] == "MALFORMED"  # quantity must be > 0


def test_carrier_scan_timestamp_split_flags_and_duplicates(spark):
    text = CARRIER_HEADER + (
        "DHL,TRK1,SHP1,DLV,Delivered,2024-05-17T10:15:30+02:00,BER,DE,,Jane Doe\n"
        "DHL,TRK1,SHP1,DLV,Delivered,2024-05-17T10:15:30+02:00,BER,DE,,Jane Doe\n"
        "DHL,TRK2,SHP2,EXC,Exception,2024-05-17T11:00:00-05:30,NYC,US,WX,\n"
        "DHL,TRK3,SHP3,SCN,Scanned,2024-05-17,NYC,US,,\n"
        "DHL,,SHP4,SCN,Scanned,2024-05-17T11:00:00+00:00,NYC,US,,\n"  # a bare Z suffix would fail the offset (DT_I4) cast first
        "DHL,TRK5,SHP5,SCN,Scanned,2024-05-17T11:00:00Z,NYC,US,,\n"
    )
    parsed = parseText(spark, feeds.CARRIER_SCAN, text)
    rows = byRow(parsed)
    assert rows[2]["ScanTimestampUtc"] == datetime.datetime(2024, 5, 17, 10, 15, 30)
    assert rows[2]["ScanOffsetMinutes"] == 120 and rows[2]["DeliveredFlag"] == "Y" and rows[2]["ExceptionFlag"] == "N"
    # the package multiplies the digits only: -05:30 -> 330, sign ignored exactly as the derived column did
    assert rows[4]["ScanOffsetMinutes"] == 330 and rows[4]["ExceptionFlag"] == "Y" and rows[4]["DeliveredFlag"] == "N"
    # (DT_DBTIMESTAMP)SUBSTRING(ScanTimestampText,1,19) fails before Validate Scan Rows sees the row: Conversion output
    assert rows[5]["RejectReasonCode"] == "CONVERSION"
    assert rows[6]["RejectReasonCode"] == "SCAN_MALFORMED"  # tracking number missing
    assert rows[7]["RejectReasonCode"] == "CONVERSION"  # Z suffix: SUBSTRING(...,21,2) is empty, (DT_I4)"" fails
    landed = landedRows(parsed)
    assert landed.count() == 3
    assert transforms.countDuplicateScans(landed) == 1
    assert rows[2]["RegionCode"] == "GLOBAL"


def test_supplier_catalog_dates_prices_hazard_and_checksum(spark):
    text = SUPPLIER_HEADER + (
        "HDR|SUP1|||||||||||||||\n"
        "DTL|SUP1|ITEM1|MPN1|Widget|EA|1|12.50|10.00|USD|10|14|20240101||UN1234||\n"
        "DTL|SUP1|ITEM2|MPN2|Gadget|EA|1|9.00|10.00|USD|10|14|20240101|20241231|||\n"
        "DTL|SUP1||MPN3|NoCode|EA|1|9.00|8.00|USD|10|14|20240101||||\n"
        "DTL|SUP1|ITEM4|MPN4|BadDate|EA|1|9.00|8.00|USD|10|14|2024-01-01||||\n"
        "TRL|||||||||||||||1|1000\n"
    )
    parsed = parseText(spark, feeds.SUPPLIER_CATALOG, text)
    rows = byRow(parsed)
    good = rows[3]
    assert good["RejectReasonCode"] is None
    assert good["EffectiveFromDate"] == datetime.date(2024, 1, 1)
    assert good["EffectiveToDate"] == datetime.date(9999, 12, 31)
    assert good["HazardousFlag"] == "Y" and good["NetPrice"] == Decimal("10.0000")
    assert rows[4]["RejectReasonCode"] == "CATALOG_BAD"  # ListPrice < NetPrice
    assert rows[4]["EffectiveToDate"] == datetime.date(2024, 12, 31) and rows[4]["HazardousFlag"] == "N"
    assert rows[5]["RejectReasonCode"] == "CATALOG_BAD"  # SupplierItemCode missing
    assert rows[6]["RejectReasonCode"] == "CONVERSION"  # 2024-01-01 breaks the (DT_DBDATE) SUBSTRING cast first
    assert rows[7]["RecordClass"] == "Footer" and rows[7]["FooterChecksumText"] == "1000"
    assert transforms.priceChecksum(landedRows(parsed)) == 1000


def publishedRates(spark, rows):
    schema = T.StructType([
        T.StructField("RatePairCode", T.StringType()),
        T.StructField("RateDate", T.DateType()),
        T.StructField("PublishedRate", T.DecimalType(18, 8)),
    ])
    return spark.createDataFrame([(p, d, Decimal(r)) for p, d, r in rows], schema)


def test_fx_override_precedence_unknown_pair_then_four_eyes_then_tolerance(spark):
    rates = publishedRates(spark, [("EUR/USD", datetime.date(2024, 5, 17), "1.08000000")])
    text = FX_HEADER + (
        "FXO,2024-05-17,EUR,USD,SPOT,1.09000000,MKT,Market move,alice,bob,CHG-1,\n"  # ok: 92 bp, four eyes, ticket
        "FXO,2024-05-17,EUR,USD,SPOT,1.20000000,MKT,Market move,alice,bob,CHG-2,\n"  # 1111 bp -> FX_TOLERANCE
        "FXO,2024-05-17,EUR,USD,SPOT,1.09000000,MKT,Market move,alice,alice,CHG-3,\n"  # self-approved -> FX_TOLERANCE
        "FXO,2024-05-17,EUR,USD,SPOT,1.09000000,MKT,Market move,alice,bob,,\n"  # no ticket -> FX_TOLERANCE
        "FXO,2024-05-17,GBP,USD,SPOT,1.30000000,MKT,Market move,alice,bob,CHG-5,\n"  # unknown pair
        "FXO,2024-05-17,EUR,USD,SPOT,abc,MKT,Market move,alice,bob,CHG-6,\n"  # conversion
        "CTL,2024-05-17,,,,,,,,,,6\n"  # control record, not a detail
    )
    parsed = parseText(spark, feeds.FX_OVERRIDE, text, publishedRatesDf=rates)
    rows = byRow(parsed)
    assert rows[2]["RejectReasonCode"] is None
    assert rows[2]["PublishedRate"] == Decimal("1.08000000") and rows[2]["DeviationBasisPoints"] == 92
    assert rows[2]["RatePairCode"] == "EUR/USD" and rows[2]["FourEyesFlag"] == "Y"
    assert rows[3]["RejectReasonCode"] == "FX_TOLERANCE" and rows[3]["DeviationBasisPoints"] == 1111
    assert rows[4]["RejectReasonCode"] == "FX_TOLERANCE" and rows[4]["FourEyesFlag"] == "N"
    assert rows[5]["RejectReasonCode"] == "FX_TOLERANCE"
    assert rows[6]["RejectReasonCode"] == "FX_UNKNOWN_PAIR" and rows[6]["PublishedRate"] is None
    assert rows[7]["RejectReasonCode"] == "CONVERSION"
    assert rows[8]["RecordClass"] == "Control" and rows[8]["RejectReasonCode"] is None
    assert landedRows(parsed).count() == 1
    assert rejectedRows(parsed).count() == 5


def test_fx_override_without_published_rates_refuses_everything(spark):
    text = FX_HEADER + "FXO,2024-05-17,EUR,USD,SPOT,1.09000000,MKT,Market move,alice,bob,CHG-1,\n"
    rows = byRow(parseText(spark, feeds.FX_OVERRIDE, text))
    assert rows[2]["RejectReasonCode"] == "FX_UNKNOWN_PAIR"


def test_quarantine_classification(spark):
    text = "D,ACME,broken line\n\nbinary\x00junk\n"
    parsed = parseText(spark, feeds.QUARANTINE_MALFORMED, text, fileName="quarantine_rejects_partner_sales_na_20240517_001.dat")
    rows = byRow(parsed)
    # QuarantineMalformed's connection manager has a header row, so line 1 is skipped like the other feeds
    assert set(rows) == {2, 3}
    assert rows[2]["RejectReasonCode"] == "EMPTY_LINE" and rows[2]["ReplayEligibleFlag"] == "N"
    assert rows[3]["RejectReasonCode"] == "QUARANTINED" and rows[3]["ReplayEligibleFlag"] == "N"
    assert rows[3]["OriginFeedCode"] == "PARTNER_NA"
    assert transforms.originFeedCode(F.lit("x_supplier_catalog_y")).alias("c") is not None


def test_column_count_short_and_long_lines(spark):
    header = ",".join(feeds.PARTNER_SALES_NA.columns) + "\n"
    text = header + "D,ACME,0101\n" + "D,ACME,0101,T-1,05/17/2024,P-1,012,1,1.00,0,0,1.00,USD,WA,98101,,extra\n"
    rows = byRow(parseText(spark, feeds.PARTNER_SALES_NA, text))
    assert rows[2]["RejectReasonCode"] == "COLUMN_COUNT" and rows[2]["ActualColumnCount"] == 3
    assert rows[3]["RejectReasonCode"] == "COLUMN_COUNT" and rows[3]["ActualColumnCount"] == 17
