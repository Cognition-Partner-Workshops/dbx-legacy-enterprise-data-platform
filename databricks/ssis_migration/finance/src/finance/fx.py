"""Set-based port of WWI_FIN.FN_CONVERT_AMOUNT.

Resolution order (per row): same currency -> direct rate on the date or the latest
within `maxBackDays` -> inverse pair within the window -> USD triangulation within
30 days.  Result is rounded to the target currency's minor unit (default 2).
Where Oracle raised ORA-20031 (no rate) we return NULL and set `<out>_rate_missing`.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RATE_COLS = ["from_curr_cd", "to_curr_cd", "rate_dt", "rate_type_cd", "rate"]


def normalizeRates(rates: DataFrame) -> DataFrame:
    """Lower-case the fx_rate_daily columns and drop superseded rows."""
    lowered = rates.select(*[F.col(c).alias(c.lower()) for c in rates.columns])
    if "superseded_flg" in lowered.columns:
        lowered = lowered.where(F.coalesce(F.col("superseded_flg"), F.lit("N")) != "Y")
    return lowered.select(
        F.upper("from_curr_cd").alias("from_curr_cd"),
        F.upper("to_curr_cd").alias("to_curr_cd"),
        F.to_date("rate_dt").alias("rate_dt"),
        F.upper("rate_type_cd").alias("rate_type_cd"),
        F.col("rate").cast("decimal(18,8)").alias("rate"),
    )


def _latest(pairs: DataFrame, keyCols: list[str], rateCol: str) -> DataFrame:
    w = Window.partitionBy(*keyCols).orderBy(F.col("rate_dt").desc(), F.col(rateCol).desc())
    return pairs.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn", "rate_dt")


def _roundToMinor(value: Column, minor: Column) -> Column:
    rounded = F.round(value, 2)
    for digits in (0, 1, 3, 4):
        rounded = F.when(minor == digits, F.round(value, digits)).otherwise(rounded)
    return rounded


def convertAmounts(
    df: DataFrame,
    rates: DataFrame,
    currencies: DataFrame | None,
    amountCol: str,
    fromCol: str,
    toCol: str,
    dateCol: str,
    outCol: str,
    rateType: str = "SPOT",
    maxBackDays: int = 7,
) -> DataFrame:
    rid = f"_{outCol}_rid"
    base = df.withColumn(rid, F.monotonically_increasing_id())
    keys = base.select(
        rid,
        F.upper(F.col(fromCol)).alias("_from"),
        F.upper(F.col(toCol)).alias("_to"),
        F.to_date(F.col(dateCol)).alias("_dt"),
    )
    r = rates.where(F.col("rate_type_cd") == rateType.upper())

    direct = _latest(
        keys.join(
            r,
            (r.from_curr_cd == keys._from)
            & (r.to_curr_cd == keys._to)
            & (r.rate_dt <= keys._dt)
            & (r.rate_dt >= F.date_sub(keys._dt, maxBackDays)),
        ).select(rid, "rate_dt", F.col("rate").alias("_direct")),
        [rid],
        "_direct",
    )
    inverse = _latest(
        keys.join(
            r,
            (r.from_curr_cd == keys._to)
            & (r.to_curr_cd == keys._from)
            & (r.rate_dt <= keys._dt)
            & (r.rate_dt >= F.date_sub(keys._dt, maxBackDays))
            & (r.rate != 0),
        ).select(rid, "rate_dt", (F.lit(1.0) / F.col("rate")).alias("_inverse")),
        [rid],
        "_inverse",
    )
    legFrom = _latest(
        keys.join(
            r,
            (r.from_curr_cd == keys._from)
            & (r.to_curr_cd == "USD")
            & (r.rate_dt <= keys._dt)
            & (r.rate_dt >= F.date_sub(keys._dt, 30)),
        ).select(rid, "rate_dt", F.col("rate").alias("_leg_from")),
        [rid],
        "_leg_from",
    )
    legTo = _latest(
        keys.join(
            r,
            (r.from_curr_cd == "USD")
            & (r.to_curr_cd == keys._to)
            & (r.rate_dt <= keys._dt)
            & (r.rate_dt >= F.date_sub(keys._dt, 30)),
        ).select(rid, "rate_dt", F.col("rate").alias("_leg_to")),
        [rid],
        "_leg_to",
    )
    resolved = (
        keys.join(direct, rid, "left")
        .join(inverse, rid, "left")
        .join(legFrom, rid, "left")
        .join(legTo, rid, "left")
        .withColumn(
            "_rate",
            F.when(F.col("_from") == F.col("_to"), F.lit(1.0)).otherwise(
                F.coalesce(F.col("_direct"), F.col("_inverse"), F.col("_leg_from") * F.col("_leg_to"))
            ),
        )
        .select(rid, "_rate", "_to")
    )
    if currencies is not None:
        minor = currencies.select(
            F.upper(F.col("curr_cd")).alias("_to"), F.col("minor_unit_digits").cast("int").alias("_minor")
        )
        resolved = resolved.join(minor, "_to", "left")
    else:
        resolved = resolved.withColumn("_minor", F.lit(None).cast("int"))
    resolved = resolved.withColumn("_minor", F.coalesce(F.col("_minor"), F.lit(2))).drop("_to")

    out = base.join(resolved, rid, "left")
    amount = F.col(amountCol).cast("decimal(18,5)")
    return (
        out.withColumn(f"{outCol}_rate", F.col("_rate").cast("decimal(18,8)"))
        .withColumn(
            outCol,
            F.when(amount.isNull(), F.lit(None))
            .when(F.col("_rate").isNull(), F.lit(None))
            .otherwise(_roundToMinor(amount * F.col("_rate"), F.col("_minor")))
            .cast("decimal(18,5)"),
        )
        .withColumn(f"{outCol}_rate_missing", amount.isNotNull() & F.col("_rate").isNull())
        .drop(rid, "_rate", "_minor")
    )
