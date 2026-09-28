"""Silver reference tables for the tax / FX / fiscal rules library.

``run(spark, cfg)`` reads the bronze copies of the Oracle reference tables
and rebuilds:

- ``ref_fx_rate``         from ``oracle_wwi_ref_fx_rate_daily``
- ``ref_tax_rate_na``     from ``oracle_wwi_fin_tax_jurisdiction`` + ``oracle_wwi_fin_tax_rate``
- ``ref_tax_rate_eu``     from the same two tables (REGION_CD = 'EU')
- ``ref_tax_rate_apac``   from the same two tables (REGION_CD = 'APAC')
- ``dim_fiscal_calendar`` generated for the date span of ``oracle_wwi_ref_calendar_fiscal``
- ``dim_date``            one row per date with all three fiscal calendars

Every builder is a pure ``DataFrame -> DataFrame`` function so tests can feed
in-test frames shaped like the Oracle DDL (``oracle/tables/*.sql``). All
writes are full overwrites, so re-runs are idempotent.

Legacy artefacts replaced: ``oracle/reference/02_currency_and_fx_rates.sql``,
``oracle/reference/05_tax_na_sales_and_use.sql``, ``06_tax_eu_vat.sql``,
``07_tax_apac_gst.sql``, ``11_fiscal_calendars_and_periods.sql``,
``Integration.usp_PopulateDateDimension`` and ``Dimension.Date`` /
``Dimension.Fiscal Calendar`` (sqlserver/warehouse/dimensions).
"""
from __future__ import annotations

import datetime as dt

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import overwriteTable, tableExists
from sales_lakehouse.silver.rules.fiscal import (
    CALENDAR_APAC,
    CALENDAR_CODES,
    CALENDAR_EU,
    CALENDAR_NA,
    fiscalParts,
    fiscalPeriodKey,
    fiscalPeriodName,
)

RATE = DecimalType(19, 8)
PCT = DecimalType(9, 5)

BRONZE_FX_RATE_DAILY = "oracle_wwi_ref_fx_rate_daily"
BRONZE_CALENDAR_FISCAL = "oracle_wwi_ref_calendar_fiscal"
BRONZE_TAX_JURISDICTION = "oracle_wwi_fin_tax_jurisdiction"
BRONZE_TAX_RATE = "oracle_wwi_fin_tax_rate"

REF_FX_RATE = "ref_fx_rate"
REF_TAX_RATE_NA = "ref_tax_rate_na"
REF_TAX_RATE_EU = "ref_tax_rate_eu"
REF_TAX_RATE_APAC = "ref_tax_rate_apac"
DIM_FISCAL_CALENDAR = "dim_fiscal_calendar"
DIM_DATE = "dim_date"

# used when the bronze calendar has not been loaded yet (never loaded more
# than two years ahead in the legacy estate either - FN_FISCAL_PERIOD notes)
DEFAULT_SPINE_START = dt.date(2014, 1, 1)
DEFAULT_SPINE_END = dt.date(2027, 12, 31)

_OPEN_ENDED = dt.date(9999, 12, 31)


def _lineage(df: DataFrame, cfg: PipelineConfig) -> DataFrame:
    return df.withColumn("batch_id", F.lit(cfg.batchId).cast("bigint")).withColumn(
        "loaded_at_utc", F.current_timestamp()
    )


def _flag(col: str) -> Column:
    return F.upper(F.trim(F.col(col))) == "Y"


# --------------------------------------------------------------------------- FX
def buildRefFxRate(fxRateDaily: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """One row per currency / rate_date / rate_type / feed region, quoted as
    units of reporting currency per unit of ``currency_code``.

    Derivation order follows ``WWI_REF.PKG_FX.get_rate``: a direct quote to
    the reporting currency, else the inverse of a quote *from* it, else a
    triangulation through whichever currency the feed quotes against.
    ``effective_date`` is the publication date the rate really comes from:
    weekend / holiday rows that the NA and EU feeds fill in
    (``INTERPOLATED_FLG = 'Y'``) carry the date of the last published rate.
    """
    reporting = cfg.reportingCurrency.upper()
    active = fxRateDaily.filter(~_flag("SUPERSEDED_FLG")).select(
        F.upper(F.trim(F.col("FROM_CURR_CD"))).alias("fromCcy"),
        F.upper(F.trim(F.col("TO_CURR_CD"))).alias("toCcy"),
        F.col("RATE_DT").cast("date").alias("rateDate"),
        F.upper(F.trim(F.col("RATE_TYPE_CD"))).alias("rateType"),
        F.col("RATE").cast(RATE).alias("rate"),
        F.col("INVERSE_RATE").cast(RATE).alias("inverseRate"),
        F.col("RATE_SOURCE_CD").alias("sourceCode"),
        F.coalesce(F.upper(F.trim(F.col("FEED_REGION_CD"))), F.lit("GLOBAL")).alias("feedRegion"),
        _flag("INTERPOLATED_FLG").alias("isInterpolated"),
    )
    one = F.lit(1).cast(RATE)
    direct = active.filter(F.col("toCcy") == reporting).select(
        F.col("fromCcy").alias("currency_code"),
        "rateDate",
        "rateType",
        F.col("rate").alias("rate_to_usd"),
        "sourceCode",
        "feedRegion",
        "isInterpolated",
        F.lit("DIRECT").alias("derivation_code"),
        F.lit(1).alias("priority"),
    )
    inverse = active.filter((F.col("fromCcy") == reporting) & (F.col("toCcy") != reporting)).select(
        F.col("toCcy").alias("currency_code"),
        "rateDate",
        "rateType",
        F.coalesce(F.col("inverseRate"), (one / F.col("rate")).cast(RATE)).alias("rate_to_usd"),
        "sourceCode",
        "feedRegion",
        "isInterpolated",
        F.lit("INVERSE").alias("derivation_code"),
        F.lit(2).alias("priority"),
    )
    pivot = direct.unionByName(inverse).select(
        F.col("currency_code").alias("pivotCcy"),
        F.col("rateDate").alias("pivotDate"),
        F.col("rate_to_usd").alias("pivotRate"),
        F.col("priority").alias("pivotPriority"),
    )
    pivotBest = pivot.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("pivotCcy", "pivotDate").orderBy("pivotPriority", "pivotRate"))
    ).filter(F.col("_rn") == 1)
    cross = active.filter((F.col("fromCcy") != reporting) & (F.col("toCcy") != reporting))
    triangulated = cross.join(
        pivotBest, (cross.toCcy == pivotBest.pivotCcy) & (cross.rateDate == pivotBest.pivotDate), "inner"
    ).select(
        F.col("fromCcy").alias("currency_code"),
        "rateDate",
        "rateType",
        (F.col("rate") * F.col("pivotRate")).cast(RATE).alias("rate_to_usd"),
        "sourceCode",
        "feedRegion",
        "isInterpolated",
        F.lit("TRIANGULATED").alias("derivation_code"),
        F.lit(3).alias("priority"),
    )
    allRates = direct.unionByName(inverse).unionByName(triangulated).filter(F.col("currency_code") != reporting)
    grain = Window.partitionBy("currency_code", "rateDate", "rateType", "feedRegion").orderBy("priority", "rate_to_usd")
    deduped = allRates.withColumn("_rn", F.row_number().over(grain)).filter(F.col("_rn") == 1).drop("_rn", "priority")
    history = Window.partitionBy("currency_code", "rateType", "feedRegion").orderBy("rateDate").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    published = F.last(F.when(~F.col("isInterpolated"), F.col("rateDate")), ignorenulls=True).over(history)
    out = deduped.select(
        "currency_code",
        F.col("rateDate").alias("rate_date"),
        F.coalesce(published, F.col("rateDate")).alias("effective_date"),
        "rate_to_usd",
        F.col("sourceCode").alias("rate_source_code"),
        F.col("feedRegion").alias("region_code"),
        F.col("rateType").alias("rate_type_code"),
        F.lit(reporting).alias("reporting_currency_code"),
        F.col("isInterpolated").alias("is_interpolated"),
        "derivation_code",
    )
    return _lineage(out, cfg)


# --------------------------------------------------------------------------- tax
def _jurisdictions(taxJurisdiction: DataFrame, region: str) -> DataFrame:
    return taxJurisdiction.filter(F.upper(F.trim(F.col("REGION_CD"))) == region).select(
        F.col("JURISDICTION_CD").alias("jurisdiction_code"),
        F.col("JURISDICTION_NAME").alias("jurisdiction_name"),
        F.col("COUNTRY_CD").alias("country_code"),
        F.col("STATE_PROV_CD").alias("state_code"),
        F.col("COUNTY_TXT").alias("county_name"),
        F.col("CITY_TXT").alias("city_name"),
        F.col("POSTAL_FROM_CD").alias("postal_from_code"),
        F.col("POSTAL_TO_CD").alias("postal_to_code"),
        F.upper(F.trim(F.col("JURISDICTION_LEVEL_CD"))).alias("jurisdiction_level_code"),
        F.col("TAX_REGIME_CD").alias("jurisdiction_regime_code"),
        _flag("STACKS_WITH_PARENT_FLG").alias("stacks_with_parent"),
        F.col("PARENT_JURISDICTION_CD").alias("parent_jurisdiction_code"),
        F.col("EFFECTIVE_FROM_DT").cast("date").alias("jurisdiction_effective_from"),
        F.col("EFFECTIVE_TO_DT").cast("date").alias("jurisdiction_effective_to"),
        _flag("ACTIVE_FLG").alias("jurisdiction_is_active"),
    )


def _rates(taxRate: DataFrame, region: str) -> DataFrame:
    return taxRate.filter(F.upper(F.trim(F.col("REGION_CD"))) == region).select(
        F.col("JURISDICTION_CD").alias("jurisdiction_code"),
        F.col("TAX_CODE_CD").alias("tax_code"),
        F.upper(F.trim(F.col("TAX_REGIME_CD"))).alias("tax_regime_code"),
        F.upper(F.trim(F.col("RATE_CATEGORY_CD"))).alias("rate_category_code"),
        F.col("RATE_PCT").cast(PCT).alias("tax_rate_pct"),
        (F.col("RATE_PCT").cast(RATE) / F.lit(100).cast(DecimalType(3, 0))).cast(RATE).alias("tax_rate"),
        F.col("PRODUCT_CATEGORY_CD").alias("product_category_code"),
        _flag("REVERSE_CHARGE_FLG").alias("is_reverse_charge"),
        _flag("SELF_ASSESS_FLG").alias("is_self_assessed"),
        F.col("RECOVERABLE_PCT").cast(PCT).alias("recoverable_pct"),
        F.col("EFFECTIVE_FROM_DT").cast("date").alias("effective_from"),
        F.col("EFFECTIVE_TO_DT").cast("date").alias("effective_to"),
        _flag("ACTIVE_FLG").alias("is_active"),
        F.col("RATE_DESC").alias("rate_description"),
    )


_NA_LEVELS: dict[str, str] = {"STATE": "state", "COUNTY": "county", "CITY": "city", "SPEC": "district"}


def buildRefTaxRateNa(taxJurisdiction: DataFrame, taxRate: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """One row per NA jurisdiction / regime / rate category / effective
    segment, with the stacked state, county, city and district components.

    ``PKG_TAX.determine_tax``: "sales tax is additive across state, county and
    city rows in the jurisdiction table; each contributes its own tax line".
    The hierarchy is walked through ``PARENT_JURISDICTION_CD`` (up to four
    levels: STATE > COUNTY > CITY, SPEC districts hanging off any of them) and
    the components are summed into ``combined_tax_rate`` per effective segment
    (a new segment starts wherever any component starts or stops).
    """
    jur = _jurisdictions(taxJurisdiction, "NA")
    parents = jur.select(
        F.col("jurisdiction_code").alias("childCode"), F.col("parent_jurisdiction_code").alias("parentCode")
    )
    chain = jur.select(F.col("jurisdiction_code").alias("leafCode"), F.col("jurisdiction_code").alias("memberCode"))
    frontier = chain
    for _ in range(3):
        frontier = (
            frontier.join(parents, frontier.memberCode == parents.childCode, "inner")
            .filter(F.col("parentCode").isNotNull())
            .select("leafCode", F.col("parentCode").alias("memberCode"))
        )
        chain = chain.unionByName(frontier)
    chain = chain.distinct()

    memberLevels = jur.select(
        F.col("jurisdiction_code").alias("memberCode"), F.col("jurisdiction_level_code").alias("memberLevel")
    )
    components = (
        chain.join(_rates(taxRate, "NA").withColumnRenamed("jurisdiction_code", "memberCode"), "memberCode")
        .join(memberLevels, "memberCode")
        .filter(F.col("is_active"))
    )
    segmentKeys = ["leafCode", "tax_regime_code", "rate_category_code"]
    boundaries = components.select(*segmentKeys, F.col("effective_from").alias("segStart")).unionByName(
        components.filter(F.col("effective_to").isNotNull()).select(
            *segmentKeys, F.date_add(F.col("effective_to"), 1).alias("segStart")
        )
    ).distinct()
    segments = boundaries.withColumn(
        "segEnd", F.date_sub(F.lead("segStart").over(Window.partitionBy(*segmentKeys).orderBy("segStart")), 1)
    )
    comp = components.alias("c")
    seg = segments.alias("s")
    inSegment = (
        (F.col("c.effective_from") <= F.col("s.segStart"))
        & (F.coalesce(F.col("c.effective_to"), F.lit(_OPEN_ENDED)) >= F.col("s.segStart"))
    )
    joined = seg.join(
        comp,
        (F.col("s.leafCode") == F.col("c.leafCode"))
        & (F.col("s.tax_regime_code") == F.col("c.tax_regime_code"))
        & (F.col("s.rate_category_code") == F.col("c.rate_category_code"))
        & inSegment,
        "inner",
    )

    def levelRate(level: str) -> Column:
        return F.coalesce(
            F.sum(F.when(F.col("c.memberLevel") == level, F.col("c.tax_rate"))), F.lit(0).cast(RATE)
        ).cast(RATE)

    def levelCode(level: str) -> Column:
        return F.max(F.when(F.col("c.memberLevel") == level, F.col("c.memberCode")))

    aggregated = joined.groupBy(
        F.col("s.leafCode").alias("jurisdiction_code"),
        F.col("s.tax_regime_code").alias("tax_regime_code"),
        F.col("s.rate_category_code").alias("rate_category_code"),
        F.col("s.segStart").alias("effective_from"),
        F.col("s.segEnd").alias("effective_to"),
    ).agg(
        *[levelCode(level).alias(f"{name}_jurisdiction_code") for level, name in _NA_LEVELS.items()],
        *[levelRate(level).alias(f"{name}_tax_rate") for level, name in _NA_LEVELS.items()],
        F.sum(F.col("c.tax_rate")).cast(RATE).alias("combined_tax_rate"),
        F.concat_ws(",", F.sort_array(F.collect_set(F.col("c.tax_code")))).alias("component_tax_codes"),
        F.count(F.lit(1)).alias("component_count"),
        F.max(F.col("c.is_self_assessed")).alias("is_self_assessed"),
    )
    out = jur.join(aggregated, "jurisdiction_code", "inner").select(
        "jurisdiction_code",
        "jurisdiction_name",
        "jurisdiction_level_code",
        "country_code",
        "state_code",
        "county_name",
        "city_name",
        "postal_from_code",
        "postal_to_code",
        "parent_jurisdiction_code",
        "stacks_with_parent",
        "tax_regime_code",
        "rate_category_code",
        "state_jurisdiction_code",
        "county_jurisdiction_code",
        "city_jurisdiction_code",
        "district_jurisdiction_code",
        "state_tax_rate",
        "county_tax_rate",
        "city_tax_rate",
        "district_tax_rate",
        "combined_tax_rate",
        (F.col("combined_tax_rate") * F.lit(100).cast(DecimalType(3, 0))).cast(PCT).alias("combined_tax_rate_pct"),
        "component_tax_codes",
        "component_count",
        "is_self_assessed",
        "effective_from",
        "effective_to",
        F.lit("NA").alias("region_code"),
    )
    return _lineage(out, cfg)


def buildRefTaxRateEu(taxJurisdiction: DataFrame, taxRate: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """One row per EU country VAT rate (standard, reduced, zero) with
    effective range and reverse-charge eligibility (``REVERSE_CHARGE_FLG``).
    ``country_reverse_charge_eligible`` is true when any rate of the country
    is reverse-charge eligible (Article 138 intra-community supplies).
    """
    jur = _jurisdictions(taxJurisdiction, "EU")
    rates = _rates(taxRate, "EU")
    countryEligible = rates.groupBy("jurisdiction_code").agg(
        F.max(F.col("is_reverse_charge")).alias("country_reverse_charge_eligible")
    )
    out = (
        jur.join(rates, "jurisdiction_code", "inner")
        .join(countryEligible, "jurisdiction_code", "left")
        .select(
            "country_code",
            "jurisdiction_code",
            "jurisdiction_name",
            "tax_code",
            "tax_regime_code",
            "rate_category_code",
            F.col("tax_rate_pct").alias("vat_rate_pct"),
            F.col("tax_rate").alias("vat_rate"),
            (F.col("rate_category_code") == "STD").alias("is_standard_rate"),
            F.col("rate_category_code").startswith("RED").alias("is_reduced_rate"),
            "product_category_code",
            F.col("is_reverse_charge").alias("is_reverse_charge_eligible"),
            F.coalesce(F.col("country_reverse_charge_eligible"), F.lit(False)).alias("country_reverse_charge_eligible"),
            "recoverable_pct",
            "effective_from",
            "effective_to",
            "is_active",
            "rate_description",
            F.lit("EU").alias("region_code"),
        )
    )
    return _lineage(out, cfg)


def buildRefTaxRateApac(taxJurisdiction: DataFrame, taxRate: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """One row per APAC country GST / consumption-tax rate with the inclusive
    flag. APAC list prices are GST-inclusive (SSIS ``FACT_APAC_Load_Sale``:
    "List prices are GST-inclusive in AU/NZ/IN so the tax is backed out of the
    gross"); the Oracle reference carries no inclusive flag, so output-tax
    categories are flagged inclusive and ``INPUT`` credit rows are not.
    """
    jur = _jurisdictions(taxJurisdiction, "APAC")
    rates = _rates(taxRate, "APAC")
    out = jur.join(rates, "jurisdiction_code", "inner").select(
        "country_code",
        "jurisdiction_code",
        "jurisdiction_name",
        "tax_code",
        "tax_regime_code",
        "rate_category_code",
        F.col("tax_rate_pct").alias("gst_rate_pct"),
        F.col("tax_rate").alias("gst_rate"),
        # LEGACY QUIRK: APAC prices are GST-inclusive everywhere the legacy loads
        # look (SSIS FACT_APAC_Load_Sale, PKG_TAX "APAC prices are tax inclusive").
        (F.col("rate_category_code") != "INPUT").alias("is_price_inclusive"),
        (F.col("tax_rate_pct") == 0).alias("is_gst_free"),
        "product_category_code",
        "recoverable_pct",
        "effective_from",
        "effective_to",
        "is_active",
        "rate_description",
        F.lit("APAC").alias("region_code"),
    )
    return _lineage(out, cfg)


# --------------------------------------------------------------------------- calendars
def dateSpine(spark: SparkSession, start: dt.date, end: dt.date) -> DataFrame:
    """One row per calendar date, widened by a week each side so every
    NA445 period touching the range is complete."""
    return spark.range(1).select(
        F.explode(
            F.sequence(F.date_sub(F.lit(start), 7), F.date_add(F.lit(end), 7), F.expr("interval 1 day"))
        ).alias("calendar_date")
    )


def spineRange(calendarFiscal: DataFrame | None) -> tuple[dt.date, dt.date]:
    if calendarFiscal is None:
        return DEFAULT_SPINE_START, DEFAULT_SPINE_END
    row = calendarFiscal.agg(
        F.min(F.col("CALENDAR_DT").cast("date")).alias("lo"), F.max(F.col("CALENDAR_DT").cast("date")).alias("hi")
    ).first()
    if row is None or row["lo"] is None or row["hi"] is None:
        return DEFAULT_SPINE_START, DEFAULT_SPINE_END
    lo, hi = row["lo"], row["hi"]
    return dt.date(lo.year, 1, 1), dt.date(hi.year, 12, 31)


def _spineWithCalendars(spine: DataFrame) -> DataFrame:
    out = spine
    for code in CALENDAR_CODES:
        prefix = code.lower()
        parts = fiscalParts(F.col("calendar_date"), code)
        for name, expr in parts.items():
            out = out.withColumn(f"{prefix}_{name}", expr)
        out = out.withColumn(
            f"{prefix}_fiscal_period_key",
            fiscalPeriodKey(F.col(f"{prefix}_fiscal_year"), F.col(f"{prefix}_fiscal_period")),
        )
    return out


def buildDimFiscalCalendar(spine: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """One row per calendar / fiscal period, derived from the day spine."""
    enriched = _spineWithCalendars(spine)
    pieces = []
    for code in CALENDAR_CODES:
        prefix = code.lower()
        pieces.append(
            enriched.select(
                F.lit(code).alias("calendar_code"),
                F.col(f"{prefix}_fiscal_year").alias("fiscal_year"),
                F.col(f"{prefix}_fiscal_quarter").alias("fiscal_quarter"),
                F.col(f"{prefix}_fiscal_period").alias("fiscal_period"),
                F.col(f"{prefix}_period_start").alias("period_start"),
                F.col(f"{prefix}_period_end").alias("period_end"),
            ).distinct()
        )
    periods = pieces[0]
    for piece in pieces[1:]:
        periods = periods.unionByName(piece)
    regionFor = (
        F.when(F.col("calendar_code") == CALENDAR_NA, "NA")
        .when(F.col("calendar_code") == CALENDAR_EU, "EU")
        .when(F.col("calendar_code") == CALENDAR_APAC, "APAC")
    )
    out = periods.select(
        "calendar_code",
        "fiscal_year",
        "fiscal_quarter",
        "fiscal_period",
        fiscalPeriodKey(F.col("fiscal_year"), F.col("fiscal_period")).alias("fiscal_period_key"),
        "period_start",
        "period_end",
        fiscalPeriodName(F.col("fiscal_year"), F.col("fiscal_period")).alias("period_name"),
        (F.datediff(F.col("period_end"), F.col("period_start")) + 1).cast("int").alias("period_days"),
        (F.col("fiscal_period") == 12).alias("is_year_end_period"),
        regionFor.alias("region_code"),
    )
    return _lineage(out, cfg)


def _isoWeekYear(d):
    """Year the ISO week of ``d`` belongs to (``date_format`` rejects ``YYYY``)."""
    week = F.weekofyear(d)
    month = F.month(d)
    return (
        F.when((month == 1) & (week >= 52), F.year(d) - 1)
        .when((month == 12) & (week == 1), F.year(d) + 1)
        .otherwise(F.year(d))
        .cast("int")
    )


def buildDimDate(spine: DataFrame, calendarFiscal: DataFrame | None, cfg: PipelineConfig) -> DataFrame:
    """``date_key`` yyyymmdd, calendar attributes, and the fiscal period of all
    three calendars side by side (``Dimension.Date``: "Three fiscal calendars
    live side by side on the one row"). Holiday flags come from the Oracle
    calendar where a row exists."""
    enriched = _spineWithCalendars(spine)
    d = F.col("calendar_date")
    out = enriched.select(
        F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),
        d,
        F.dayofmonth(d).cast("int").alias("day_of_month"),
        F.dayofweek(d).cast("int").alias("day_of_week_number"),
        F.date_format(d, "EEEE").alias("day_name"),
        F.dayofyear(d).cast("int").alias("day_of_year"),
        F.month(d).cast("int").alias("calendar_month_number"),
        F.date_format(d, "MMMM").alias("calendar_month_name"),
        F.date_format(d, "yyyy-MM").alias("calendar_month_label"),
        F.quarter(d).cast("int").alias("calendar_quarter_number"),
        F.year(d).cast("int").alias("calendar_year"),
        F.weekofyear(d).cast("int").alias("iso_week_number"),
        _isoWeekYear(d).alias("iso_week_year"),
        F.dayofweek(d).isin(1, 7).alias("is_weekend"),
        (d == F.last_day(d)).alias("is_month_end"),
        *[
            F.col(f"{code.lower()}_{name}").alias(f"{code.lower()}_{name}")
            for code in CALENDAR_CODES
            for name in ("fiscal_year", "fiscal_quarter", "fiscal_period", "fiscal_period_key", "period_start", "period_end")
        ],
    )
    for code in CALENDAR_CODES:
        prefix = code.lower()
        out = out.withColumn(
            f"{prefix}_fiscal_period_name",
            fiscalPeriodName(F.col(f"{prefix}_fiscal_year"), F.col(f"{prefix}_fiscal_period")),
        )
    holidayCols = {"NA": "na445", "EU": "eucal", "APAC": "apacjun"}
    if calendarFiscal is not None:
        holidays = (
            calendarFiscal.select(
                F.col("CALENDAR_DT").cast("date").alias("calendar_date"),
                F.upper(F.trim(F.col("REGION_CD"))).alias("regionCode"),
                _flag("HOLIDAY_FLG").alias("isHoliday"),
                F.col("HOLIDAY_NAME").alias("holidayName"),
            )
            .groupBy("calendar_date")
            .agg(
                *[
                    F.max(F.when(F.col("regionCode") == region, F.col("isHoliday"))).alias(f"{prefix}_is_holiday")
                    for region, prefix in holidayCols.items()
                ],
                *[
                    F.max(F.when((F.col("regionCode") == region) & F.col("isHoliday"), F.col("holidayName"))).alias(
                        f"{prefix}_holiday_name"
                    )
                    for region, prefix in holidayCols.items()
                ],
            )
        )
        out = out.join(holidays, "calendar_date", "left")
    else:
        for prefix in holidayCols.values():
            out = out.withColumn(f"{prefix}_is_holiday", F.lit(None).cast("boolean")).withColumn(
                f"{prefix}_holiday_name", F.lit(None).cast("string")
            )
    for prefix in holidayCols.values():
        out = out.withColumn(f"{prefix}_is_holiday", F.coalesce(F.col(f"{prefix}_is_holiday"), F.lit(False)))
    return _lineage(out, cfg)


# --------------------------------------------------------------------------- entry point
def _readOptional(spark: SparkSession, cfg: PipelineConfig, table: str) -> DataFrame | None:
    fqn = cfg.fqn("bronze", table)
    return spark.table(fqn) if tableExists(spark, fqn) else None


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Rebuild every silver reference table from bronze. Idempotent."""
    fxRateDaily = spark.table(cfg.fqn("bronze", BRONZE_FX_RATE_DAILY))
    taxJurisdiction = spark.table(cfg.fqn("bronze", BRONZE_TAX_JURISDICTION))
    taxRate = spark.table(cfg.fqn("bronze", BRONZE_TAX_RATE))
    calendarFiscal = _readOptional(spark, cfg, BRONZE_CALENDAR_FISCAL)

    overwriteTable(buildRefFxRate(fxRateDaily, cfg), cfg.fqn("silver", REF_FX_RATE))
    overwriteTable(buildRefTaxRateNa(taxJurisdiction, taxRate, cfg), cfg.fqn("silver", REF_TAX_RATE_NA))
    overwriteTable(buildRefTaxRateEu(taxJurisdiction, taxRate, cfg), cfg.fqn("silver", REF_TAX_RATE_EU))
    overwriteTable(buildRefTaxRateApac(taxJurisdiction, taxRate, cfg), cfg.fqn("silver", REF_TAX_RATE_APAC))

    start, end = spineRange(calendarFiscal)
    spine = dateSpine(spark, start, end)
    overwriteTable(buildDimFiscalCalendar(spine, cfg), cfg.fqn("silver", DIM_FISCAL_CALENDAR))
    overwriteTable(buildDimDate(spine, calendarFiscal, cfg), cfg.fqn("silver", DIM_DATE))
