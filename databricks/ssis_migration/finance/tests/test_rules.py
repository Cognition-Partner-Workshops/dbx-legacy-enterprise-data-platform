from datetime import date

from pyspark.sql import functions as F

from finance import rules


def _one(spark, expr, **cols):
    df = spark.createDataFrame([cols])
    return df.select(expr.alias("v")).collect()[0]["v"]


def test_ssis_aging_buckets(spark):
    df = spark.createDataFrame([(d,) for d in [-5, 0, 1, 30, 31, 60, 61, 90, 91]], "d int")
    got = [r["b"] for r in df.select(rules.ssisAgingBucket(F.col("d")).alias("b")).collect()]
    assert got == ["CURRENT", "CURRENT", "B030", "B030", "B060", "B060", "B090", "B090", "B090P"]


def test_oracle_regional_bucket_families(spark):
    df = spark.createDataFrame([(20, "EU"), (20, "APAC"), (20, "NA"), (200, "APAC")], "d int, r string")
    got = [r["b"] for r in df.select(rules.oracleAgingBucket(F.col("d"), F.col("r")).alias("b")).collect()]
    assert got == ["D01_30", "D16_30", "B1_1_30", "D60_PLUS"]


def test_residual_tolerance_by_region(spark):
    df = spark.createDataFrame([("NA", 1000.0), ("EU", 1000.0), ("APAC", 1000.0)], "r string, rem double")
    got = {
        r["r"]: round(r["t"], 4)
        for r in df.select("r", rules.residualTolerance(F.col("r"), F.col("rem")).alias("t")).collect()
    }
    assert got == {"NA": 0.02, "EU": 0.01, "APAC": 5.0}


def test_payment_status_and_value_date(spark):
    df = spark.createDataFrame(
        [
            ("CLRD", None, "NA", date(2024, 12, 2)),
            ("ISSD", date(2024, 12, 5), "EU", date(2024, 12, 2)),
            ("ISSD", None, "EU", date(2024, 12, 2)),
        ],
        "status string, void_dt date, region string, pay_dt date",
    )
    rows = df.select(
        rules.paymentStatus(F.col("status"), F.col("void_dt")).alias("s"),
        rules.valueDate(F.lit(None).cast("date"), F.col("pay_dt"), F.col("region")).alias("vd"),
    ).collect()
    assert [r["s"] for r in rows] == ["PAID", "VOID", "PEND"]
    assert rows[0]["vd"] == date(2024, 12, 3)  # NA T+1
    assert rows[2]["vd"] == date(2024, 12, 4)  # EU T+2


def test_due_date_regional_snapping(spark):
    # 2024-03-02 is a Saturday: NA rolls forward to Monday, EU rolls back to Friday, APAC snaps to the 15th
    df = spark.createDataFrame([("NA",), ("EU",), ("APAC",)], "r string")
    got = {
        r["r"]: r["d"]
        for r in df.select(
            "r",
            rules.dueDate(
                F.lit(date(2024, 2, 1)), F.lit("NET"), F.lit(30), F.lit(None), F.lit(None), F.col("r")
            ).alias("d"),
        ).collect()
    }
    assert got["NA"] == date(2024, 3, 4)
    assert got["EU"] == date(2024, 3, 1)
    assert got["APAC"] == date(2024, 3, 15)


def test_withholding_three_jurisdictions(spark):
    df = spark.createDataFrame(
        [
            ("NA", 1000.0, "CONS", None, 24.0, None, 0.0),
            ("NA", 1000.0, "ITEM", None, 24.0, None, 0.0),
            ("EU", 1000.0, "SERV", "DE123", 20.0, 10.0, 0.0),
            ("EU", 1000.0, "SERV", None, 20.0, 10.0, 0.0),
            ("APAC", 50.0, "SERV", None, 47.0, None, 75.0),
            ("APAC", 100.0, "SERV", None, 47.0, None, 75.0),
        ],
        "r string, amt double, cat string, reg string, rate double, treaty double, thr double",
    )
    got = [
        round(r["w"], 2)
        for r in df.select(
            rules.withholdingAmount(
                F.col("r"),
                F.col("amt"),
                F.col("cat"),
                F.col("reg"),
                F.col("rate"),
                F.col("treaty"),
                F.col("thr"),
            ).alias("w")
        ).collect()
    ]
    assert got == [240.0, 0.0, 100.0, 200.0, 0.0, 47.0]


def test_fiscal_period_arithmetic(spark):
    df = spark.createDataFrame(
        [(date(2024, 12, 15), "APAC"), (date(2024, 12, 15), "NA"), (date(2024, 3, 31), "APAC")],
        "d date, r string",
    )
    got = [
        r["p"] for r in df.select(rules.fiscalPeriodArithmetic(F.col("d"), F.col("r")).alias("p")).collect()
    ]
    assert got == ["2025-09", "2024-12", "2024-12"]
