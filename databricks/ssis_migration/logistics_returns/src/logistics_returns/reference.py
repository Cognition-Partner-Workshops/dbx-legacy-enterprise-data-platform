"""Reference data shared by the staging and fact packages.

The legacy ``ref.*`` tables that the SSIS lookups cache (``ref.Carrier``, ``ref.CodeCrosswalk``, ``ref.Country``,
``ref.FxRateDaily``) are empty / absent on the shared baseline host, so every lookup here first reads the legacy
object through federation and falls back to the code-level reference below when the legacy object yields no rows.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from logistics_returns.common import readLegacy
from logistics_returns.config import RunContext

REGION_NA = "NA"
REGION_EU = "EU"
REGION_APAC = "APAC"
REGION_OTHER = "OTHER"

COUNTRY_REGION = {
    "US": REGION_NA, "CA": REGION_NA, "MX": REGION_NA,
    "GB": REGION_EU, "IE": REGION_EU, "DE": REGION_EU, "FR": REGION_EU, "NL": REGION_EU, "BE": REGION_EU,
    "ES": REGION_EU, "IT": REGION_EU, "PT": REGION_EU, "AT": REGION_EU, "PL": REGION_EU, "SE": REGION_EU,
    "DK": REGION_EU, "FI": REGION_EU, "NO": REGION_EU, "CH": REGION_EU, "CZ": REGION_EU,
    "AU": REGION_APAC, "NZ": REGION_APAC, "SG": REGION_APAC, "JP": REGION_APAC, "KR": REGION_APAC,
    "HK": REGION_APAC, "MY": REGION_APAC, "TH": REGION_APAC, "IN": REGION_APAC, "CN": REGION_APAC,
    "ID": REGION_APAC, "PH": REGION_APAC, "VN": REGION_APAC, "TW": REGION_APAC,
}  # fmt: skip

CURRENCY_REGION = {
    "USD": REGION_NA, "CAD": REGION_NA, "MXN": REGION_NA,
    "EUR": REGION_EU, "GBP": REGION_EU, "CHF": REGION_EU, "SEK": REGION_EU, "DKK": REGION_EU, "NOK": REGION_EU,
    "PLN": REGION_EU, "CZK": REGION_EU,
    "AUD": REGION_APAC, "NZD": REGION_APAC, "SGD": REGION_APAC, "JPY": REGION_APAC, "KRW": REGION_APAC,
    "HKD": REGION_APAC, "MYR": REGION_APAC, "THB": REGION_APAC, "INR": REGION_APAC, "CNY": REGION_APAC,
}  # fmt: skip

# Countries whose statutory / tax regime the packages treat as "EU" for VAT credit-note purposes.
EU_MEMBER_COUNTRIES = {"IE", "DE", "FR", "NL", "BE", "ES", "IT", "PT", "AT", "PL", "SE", "DK", "FI", "CZ"}

# Legacy ref.CodeCrosswalk (CodeDomainCode = 'RETURN_REASON') is empty on the host. The SSIS lookup would therefore
# have rejected every return as an unmapped reason; this fallback maps the raw OLTP reason codes onto the conformed
# codes in Returns.ReturnReasons (README: deliberate deviation). Codes not listed are still rejected.
FALLBACK_RETURN_REASON_CROSSWALK: list[tuple[str, str, str]] = [
    ("DMG", "DAMTR", "DAMAGE"),
    ("DAMG", "DAMTR", "DAMAGE"),
    ("WRNG", "PICKER", "PICKERROR"),
    ("MISM", "PICKER", "PICKERROR"),
    ("QTY", "PICKER", "PICKERROR"),
    ("OVER", "PICKER", "PICKERROR"),
    ("LATE", "LATEEU", "LATE"),
    ("DELY", "LATEEU", "LATE"),
    ("NOR", "COM", "CHANGEOFMIND"),
    ("COM", "COM", "CHANGEOFMIND"),
    ("COOL", "COOL", "CHANGEOFMIND"),
    ("QUAL", "QUAL", "QUALITY"),
    ("CONFORM", "CONFORM", "QUALITY"),
    ("RECALL", "RECALL", "RECALL"),
    ("CUSTAP", "CUSTAP", "OTHER"),
]

CROSSWALK_SCHEMA = T.StructType(
    [
        T.StructField("source_code_value", T.StringType()),
        T.StructField("conformed_code_value", T.StringType()),
        T.StructField("return_reason_group_code", T.StringType()),
    ]
)

CARRIER_SCHEMA = T.StructType(
    [
        T.StructField("carrier_code", T.StringType()),
        T.StructField("carrier_name", T.StringType()),
        T.StructField("carrier_region_code", T.StringType()),
        T.StructField("service_level_list", T.StringType()),
        T.StructField("on_time_target_percent", T.DecimalType(9, 2)),
    ]
)


def mapColumn(col: Column, mapping: dict[str, str], default: str | None = None) -> Column:
    lookup = F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])
    value = lookup[F.upper(F.trim(col.cast("string")))]
    return F.coalesce(value, F.lit(default)) if default is not None else value


def countryRegion(countryCode: Column) -> Column:
    return mapColumn(countryCode, COUNTRY_REGION, REGION_OTHER)


def currencyRegion(currencyCode: Column) -> Column:
    return mapColumn(currencyCode, CURRENCY_REGION)


def regionStatutoryWindowDays(regionCode: Column) -> Column:
    """The Derived Column expression in STG_Load_ReturnAndCredit: EU 14, APAC 7, otherwise 30 days."""
    region = F.upper(F.trim(regionCode.cast("string")))
    return F.when(region == REGION_EU, F.lit(14)).when(region == REGION_APAC, F.lit(7)).otherwise(F.lit(30))


def regionSlaTargetPercent(regionCode: Column) -> Column:
    """usp_RefreshAggregateDeliveryPerformance: NA 96 %, EU 98 %, APAC/other 92 %."""
    region = F.upper(F.trim(regionCode.cast("string")))
    return (F.when(region == REGION_NA, F.lit(96.0)).when(region == REGION_EU, F.lit(98.0)).otherwise(F.lit(92.0))).cast(
        "decimal(9,2)"
    )


def regionStalledThresholdDays(regionCode: Column) -> Column:
    """usp_LoadFactOrderFulfilment: NA 30, EU 45, everywhere else 60 days without a milestone."""
    region = F.upper(F.trim(regionCode.cast("string")))
    return F.when(region == REGION_NA, F.lit(30)).when(region == REGION_EU, F.lit(45)).otherwise(F.lit(60))


def returnReasonCrosswalk(ctx: RunContext) -> DataFrame:
    """ref.CodeCrosswalk (domain RETURN_REASON) when populated, else the code-level fallback."""
    spark = ctx.spark
    try:
        legacy = readLegacy(spark, ctx.legacyStaging, "ref", "CodeCrosswalk")
        legacy = (
            legacy.where(F.upper(F.col("CodeDomainCode")) == "RETURN_REASON")
            .where(F.coalesce(F.col("EffectiveToDate").cast("date") >= F.current_date(), F.lit(True)))
            .select(
                F.upper(F.trim(F.col("SourceCodeValue"))).alias("source_code_value"),
                F.upper(F.trim(F.col("ConformedCodeValue"))).alias("conformed_code_value"),
                F.lit(None).cast("string").alias("return_reason_group_code"),
            )
            .dropDuplicates(["source_code_value"])
        )
        if legacy.limit(1).count() > 0:
            return legacy
    except Exception:  # noqa: BLE001 - a missing legacy object must not stop the load; the fallback is authoritative
        pass
    return spark.createDataFrame(FALLBACK_RETURN_REASON_CROSSWALK, CROSSWALK_SCHEMA)


def carrierReference(ctx: RunContext) -> DataFrame:
    """Active carriers: legacy Dimension.Carrier when populated, else the OLTP Shipping.Carriers master."""
    spark = ctx.spark
    try:
        dim = readLegacy(spark, ctx.legacyDw, "Dimension", "Carrier")
        if "Carrier Code" in dim.columns and dim.limit(1).count() > 0:
            return dim.select(
                F.upper(F.trim(F.col("Carrier Code"))).alias("carrier_code"),
                F.col("Carrier Name").alias("carrier_name"),
                F.col("Region Code").alias("carrier_region_code"),
                F.lit(None).cast("string").alias("service_level_list"),
                F.lit(None).cast("decimal(9,2)").alias("on_time_target_percent"),
            ).dropDuplicates(["carrier_code"])
    except Exception:  # noqa: BLE001 - fall through to the OLTP master
        pass
    try:
        oltp = spark.table(ctx.legacy(ctx.legacyOltp, "Shipping", "Carriers"))
        return oltp.where(F.upper(F.col("CarrierStatus")) == "ACTIVE").select(
            F.upper(F.trim(F.col("CarrierCode"))).alias("carrier_code"),
            F.col("CarrierName").alias("carrier_name"),
            F.upper(F.trim(F.col("RegionCode"))).alias("carrier_region_code"),
            F.col("ServiceLevelList").alias("service_level_list"),
            F.col("OnTimeTargetPercent").cast("decimal(9,2)").alias("on_time_target_percent"),
        )
    except Exception:  # noqa: BLE001
        return spark.createDataFrame([], CARRIER_SCHEMA)


def fxRatesToUsd(ctx: RunContext) -> DataFrame:
    """ref.FxRateDaily rates to USD (empty on the baseline host -> non-USD amounts stay NULL, flagged in dq)."""
    schema = T.StructType(
        [
            T.StructField("from_currency_code", T.StringType()),
            T.StructField("rate_date", T.DateType()),
            T.StructField("conversion_rate", T.DecimalType(18, 8)),
        ]
    )
    try:
        legacy = readLegacy(ctx.spark, ctx.legacyStaging, "ref", "FxRateDaily")
        return legacy.where(F.upper(F.col("ToCurrencyCode")) == "USD").select(
            F.upper(F.trim(F.col("FromCurrencyCode"))).alias("from_currency_code"),
            F.col("RateDate").cast("date").alias("rate_date"),
            F.col("ConversionRate").cast("decimal(18,8)").alias("conversion_rate"),
        )
    except Exception:  # noqa: BLE001
        return ctx.spark.createDataFrame([], schema)


def convertToUsd(df: DataFrame, fx: DataFrame, amountCol: str, currencyCol: str, dateCol: str, outCol: str) -> DataFrame:
    """Join the latest rate on/before the transaction date; USD passes through at 1.0."""
    rates = fx.alias("fx")
    df = df.withColumn("_fx_row_id", F.monotonically_increasing_id())
    joined = df.alias("d").join(
        rates,
        (F.upper(F.col(f"d.{currencyCol}")) == F.col("fx.from_currency_code")) & (F.col("fx.rate_date") <= F.col(f"d.{dateCol}")),
        "left",
    )
    keyCols = [F.col(f"d.{c}") for c in df.columns]
    ranked = joined.withColumn(
        "_fx_rank",
        F.row_number().over(Window.partitionBy(F.col("d._fx_row_id")).orderBy(F.col("fx.rate_date").desc_nulls_last())),
    ).where(F.col("_fx_rank") == 1)
    rate = F.when(F.upper(F.col(f"d.{currencyCol}")) == "USD", F.lit(1.0)).otherwise(F.col("fx.conversion_rate"))
    return ranked.select(*keyCols, (F.col(f"d.{amountCol}") * rate).cast("decimal(19,4)").alias(outCol)).drop("_fx_row_id")


def emptyLike(spark: SparkSession, schema: T.StructType) -> DataFrame:
    return spark.createDataFrame([], schema)
