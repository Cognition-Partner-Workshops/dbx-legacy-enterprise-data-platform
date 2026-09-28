from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

import fact_rules as rules


def rowOf(spark, exprs, data=None, schema=None):
    df = spark.createDataFrame(data or [(1,)], schema or ["x"])
    return df.select(*exprs).first()


def test_sale_amounts_round_half_and_apply_uom_factor(spark):
    df = spark.createDataFrame([(Decimal("3"), Decimal("2"), Decimal("10.005"), Decimal("1.50"), Decimal("4.00"))], "qty decimal(18,4), uom decimal(18,4), price decimal(18,4), discount decimal(18,2), cost decimal(18,4)")
    r = df.select(
        rules.saleGrossAmount(F.col("qty"), F.col("price"), F.col("uom")).alias("gross"),
        rules.saleNetAmount(rules.saleGrossAmount(F.col("qty"), F.col("price"), F.col("uom")), F.col("discount")).alias("net"),
        rules.saleCostAmount(F.col("qty"), F.col("cost"), F.col("uom")).alias("cost"),
    ).first()
    assert r["gross"] == Decimal("60.03")
    assert r["net"] == Decimal("58.53")
    assert r["cost"] == Decimal("24.00")


def test_eu_reverse_charge_zeroes_vat_when_cross_border_registered(spark):
    df = spark.createDataFrame(
        [("FR123", "FR", "DE", Decimal("100.00"), Decimal("20.00")), ("DE999", "DE", "DE", Decimal("100.00"), Decimal("19.00")), (None, "FR", "DE", Decimal("100.00"), Decimal("20.00"))],
        "reg string, custCountry string, shipTo string, net decimal(18,2), vat decimal(5,2)",
    )
    rc = rules.euIsReverseCharge(F.col("reg"), F.col("custCountry"), F.col("shipTo"))
    rows = df.select(rc.alias("rc"), rules.euVatAmount(F.col("net"), rules.euVatRateApplied(rc, F.col("vat"))).alias("vat")).collect()
    assert [r["rc"] for r in rows] == [True, False, False]
    assert [r["vat"] for r in rows] == [Decimal("0.00"), Decimal("19.00"), Decimal("20.00")]


def test_apac_gst_inclusive_exclusive_and_gst_free(spark):
    df = spark.createDataFrame(
        [(Decimal("1"), Decimal("110.00"), Decimal("10.00"), True), (Decimal("1"), Decimal("100.00"), Decimal("10.00"), False), (Decimal("1"), Decimal("100.00"), Decimal("0.00"), False)],
        "qty decimal(18,4), price decimal(18,2), gst decimal(5,2), inclusive boolean",
    )
    gst = rules.apacGstAmount(F.col("qty"), F.col("price"), F.col("gst"), F.col("inclusive"))
    rows = df.select(gst.alias("gst"), rules.apacNetOfGst(F.col("qty") * F.col("price"), gst, F.col("inclusive")).alias("net")).collect()
    assert [r["gst"] for r in rows] == [Decimal("10.00"), Decimal("10.00"), Decimal("0.00")]
    assert [r["net"] for r in rows] == [Decimal("100.00"), Decimal("100.00"), Decimal("100.00")]


def test_apac_fiscal_year_starts_in_april(spark):
    df = spark.createDataFrame([(date(2024, 3, 31),), (date(2024, 4, 1),), (date(2024, 12, 31),)], "d date")
    rows = df.select(rules.fiscalYear(F.col("d"), rules.FISCAL_YEAR_START_MONTH_APAC).alias("fy"), rules.fiscalPeriod(F.col("d"), rules.FISCAL_YEAR_START_MONTH_APAC).alias("fp")).collect()
    assert [(r["fy"], r["fp"]) for r in rows] == [(2024, 12), (2025, 1), (2025, 9)]  # legacy: month >= start ? YEAR + 1 : YEAR
    cal = df.select(rules.fiscalYear(F.col("d"), 1).alias("fy"), rules.fiscalPeriod(F.col("d"), 1).alias("fp")).collect()
    assert [(r["fy"], r["fp"]) for r in cal] == [(2024, 3), (2024, 4), (2024, 12)]


def test_signed_amounts_follow_transaction_type(spark):
    df = spark.createDataFrame([(Decimal("50.00"), "INVOICE"), (Decimal("50.00"), "PAYMENT"), (Decimal("50.00"), "CREDIT")], "amt decimal(18,2), t string")
    rows = df.select(rules.signedAmount(F.col("amt"), F.col("t"), rules.NEGATIVE_CUSTOMER_TRANSACTION_TYPES).alias("s")).collect()
    assert [r["s"] for r in rows] == [Decimal("50.00"), Decimal("-50.00"), Decimal("-50.00")]
    mv = spark.createDataFrame([(Decimal("5"), "RECEIPT"), (Decimal("5"), "ISSUE"), (Decimal("5"), "SCRAP")], "q decimal(18,4), t string")
    assert [r[0] for r in mv.select(rules.movementSignedQuantity(F.col("q"), F.col("t"))).collect()] == [Decimal("5"), Decimal("-5"), Decimal("-5")]


def test_gl_signed_amount_and_manual_journal(spark):
    df = spark.createDataFrame([(Decimal("100.00"), Decimal("0.00"), "MANUAL"), (Decimal("0.00"), Decimal("40.00"), "AP")], "d decimal(18,2), c decimal(18,2), src string")
    rows = df.select(rules.glSignedAmount(F.col("d"), F.col("c")).alias("s"), rules.isManualJournal(F.col("src")).alias("m")).collect()
    assert [(r["s"], r["m"]) for r in rows] == [(Decimal("100.00"), True), (Decimal("-40.00"), False)]


def test_bot_detection_and_bounce(spark):
    df = spark.createDataFrame([("Mozilla/5.0", 3, 120), ("Googlebot/2.1", 3, 120), ("Mozilla/5.0", 900, 120), ("Mozilla/5.0", 1, 5)], "ua string, pv int, dur int")
    rows = df.select(rules.isBotSession(F.col("ua"), F.col("pv")).alias("bot"), rules.isBounce(F.col("pv"), F.col("dur")).alias("bounce")).collect()
    assert [r["bot"] for r in rows] == [False, True, True, False]
    assert [r["bounce"] for r in rows] == [False, False, False, True]


def test_restocking_fee_is_regional(spark):
    df = spark.createDataFrame(
        [("NA", date(2024, 1, 1), date(2024, 1, 20), Decimal("2"), Decimal("100.00")), ("NA", date(2024, 1, 1), date(2024, 3, 1), Decimal("2"), Decimal("100.00")),
         ("EU", date(2024, 1, 1), date(2024, 1, 10), Decimal("2"), Decimal("100.00")), ("APAC", date(2024, 1, 1), date(2024, 1, 10), Decimal("2"), Decimal("100.00"))],
        "region string, inv date, ret date, qty decimal(18,4), price decimal(18,2)",
    )
    fees = [r[0] for r in df.select(rules.restockingFeeAmount(F.col("region"), F.col("inv"), F.col("ret"), F.col("qty"), F.col("price"))).collect()]
    assert fees[0] == Decimal("0.00")
    assert fees[1] == Decimal("30.00")
    assert fees[2] == Decimal("0.00")
    assert fees[3] == Decimal("5.00")  # APAC flat fee


def test_inventory_snapshot_rules(spark):
    df = spark.createDataFrame([(Decimal("100"), Decimal("60"), Decimal("1000.00"), "EU", Decimal("5")), (Decimal("0"), Decimal("0"), Decimal("0.00"), "NA", None)], "onHand decimal(18,4), aged decimal(18,4), value decimal(18,2), region string, cover decimal(9,2)")
    rows = df.select(
        rules.isSlowMoving(F.col("aged"), F.col("onHand")).alias("slow"),
        rules.stockAgeBucket(F.col("aged"), F.col("onHand")).alias("bucket"),
        rules.obsolescenceProvision(F.col("value"), F.col("aged"), F.col("onHand"), F.col("region")).alias("prov"),
        rules.coverBand(F.col("cover")).alias("band"),
        rules.costingMethodCode(F.col("region")).alias("costing"),
    ).collect()
    assert (rows[0]["slow"], rows[0]["bucket"], rows[0]["prov"], rows[0]["band"], rows[0]["costing"]) == (True, "OBSOLETE", Decimal("180.00"), "CRITICAL", "FIFO")
    assert (rows[1]["slow"], rows[1]["bucket"], rows[1]["prov"], rows[1]["band"]) == (False, "NONE", Decimal("0.00"), "UNKNOWN")
