"""REF_Load_* packages: full refresh of the conformed reference dimensions in the DW (gold_dim_*).

Each build* function is a pure DataFrame transformation so it can be tested on local Spark; run* functions
read the silver layer, add the surrogate key + unknown member and overwrite the gold table.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ref_calendar import config, seeds
from ref_calendar.common import readLegacyDw, rowHash, writeTable, yesNo
from ref_calendar.reference import translateCode

WWI_EPOCH = "2013-01-01 00:00:00"
HIGH_TS = "9999-12-31 23:59:59.999"
REGIONS = ["NA", "EU", "APAC"]


def withSurrogateKey(df: DataFrame, keyCol: str, orderCols: list) -> DataFrame:
    return df.withColumn(keyCol, F.row_number().over(Window.orderBy(*orderCols)).cast("int"))


def unknownRow(spark: SparkSession, schema: T.StructType, keyCol: str, nameCols: list[str], idCol: str | None = None) -> DataFrame:
    """WWI-style unknown member: key 0, 'Unknown' in the descriptive columns, valid from the WWI epoch."""
    values = {}
    for f in schema.fields:
        if f.name == keyCol or f.name == idCol:
            values[f.name] = 0
        elif f.name in nameCols:
            values[f.name] = "Unknown"
        elif f.name == "valid_from":
            values[f.name] = WWI_EPOCH
        elif f.name == "valid_to":
            values[f.name] = HIGH_TS
        elif f.name in ("lineage_key", "batch_id"):
            values[f.name] = 0
        elif f.name == "region_code":
            values[f.name] = "GLOBAL"
        elif f.name == "is_active":
            values[f.name] = True
        else:
            values[f.name] = None
    stringSchema = T.StructType([T.StructField(f.name, T.StringType()) for f in schema.fields])
    row = spark.createDataFrame([tuple(str(values[f.name]) if values[f.name] is not None else None for f in schema.fields)], stringSchema)
    return row.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def finaliseDimension(spark: SparkSession, df: DataFrame, keyCol: str, orderCols: list, nameCols: list[str], batchId: int, idCol: str | None = None) -> DataFrame:
    keyed = withSurrogateKey(df, keyCol, orderCols)
    if "valid_from" not in keyed.columns:
        keyed = keyed.withColumn("valid_from", F.to_timestamp(F.lit(WWI_EPOCH))).withColumn("valid_to", F.to_timestamp(F.lit(HIGH_TS)))
    keyed = keyed.withColumn("lineage_key", F.lit(batchId).cast("bigint"))
    ordered = keyed.select(keyCol, *[c for c in keyed.columns if c != keyCol])
    return unknownRow(spark, ordered.schema, keyCol, nameCols, idCol).unionByName(ordered)


# ----------------------------------------------------------------------------- OLTP-sourced dims

def buildDimPaymentMethod(bronze: DataFrame, crosswalk: DataFrame) -> DataFrame:
    """REF_Load_PaymentMethod over Application.PaymentMethods (all system versions): the WWI dimension shape plus
    the conformed code (PAYMENT_METHOD crosswalk), settlement type and electronic flag from the generator."""
    df = bronze.select(
        F.col("payment_method_id").alias("wwi_payment_method_id"),
        F.col("payment_method_name").alias("payment_method"),
        F.col("valid_from"), F.col("valid_to"),
        (F.col("valid_to") < F.to_timestamp(F.lit("9999-01-01"))).alias("is_history"),
        F.upper("payment_method_name").alias("source_code"),
    )
    df = translateCode(df, crosswalk, "PAYMENT_METHOD", config.SOURCE_SYSTEM_OLTP, "source_code", None, "payment_method_code")
    code = F.col("payment_method_code")
    return df.select(
        "wwi_payment_method_id", "payment_method", "valid_from", "valid_to", "is_history", "payment_method_code",
        F.when(code == "CASH", "CASH").when(code.isin("CHQ", "CHECK"), "PAPER").when(code == "CARD", "CARD").otherwise("ELECTRONIC").alias("settlement_type_code"),
        yesNo(~code.isin("CASH", "CHQ", "CHECK")).alias("is_electronic_flag"),
        F.when(code == "CASH", 0).when(code.isin("CHQ", "CHECK"), 5).when(code == "CARD", 2).otherwise(1).cast("int").alias("settlement_days"),
        F.lit("GLOBAL").alias("region_code"),
        F.lit(config.SOURCE_SYSTEM_OLTP).alias("source_system_code"),
    ).withColumn("row_hash_type_1", rowHash("payment_method", "payment_method_code", "settlement_type_code"))


def buildDimTransactionType(bronze: DataFrame, crosswalk: DataFrame) -> DataFrame:
    """REF_Load_TransactionType over Application.TransactionTypes (all system versions) with ledger semantics."""
    name = F.col("transaction_type_name")
    df = bronze.select(
        F.col("transaction_type_id").alias("wwi_transaction_type_id"),
        name.alias("transaction_type"),
        F.col("valid_from"), F.col("valid_to"),
        (F.col("valid_to") < F.to_timestamp(F.lit("9999-01-01"))).alias("is_history"),
        F.col("ledger_side_code"), F.col("is_reversal"),
        F.upper(name).alias("source_code"),
    )
    df = translateCode(df, crosswalk, "TRANSACTION_TYPE", config.SOURCE_SYSTEM_OLTP, "source_code", None, "transaction_type_code")
    name = F.col("transaction_type")
    isCustomer, isSupplier, isStock = name.like("Customer%"), name.like("Supplier%"), name.like("Stock%")
    return df.select(
        "wwi_transaction_type_id", "transaction_type", "valid_from", "valid_to", "is_history", "transaction_type_code",
        F.when(isCustomer, "AR").when(isSupplier, "AP").when(isStock, "INVENTORY").otherwise("OTHER").alias("transaction_category_code"),
        F.when(F.col("ledger_side_code") == "CR", "CREDIT").otherwise("DEBIT").alias("ledger_impact_code"),
        F.when(name.like("%Credit Note%") | name.like("%Refund%") | name.like("%Payment Received%") | name.like("%Issue%") | name.like("%Contra%"), -1).otherwise(1).cast("int").alias("amount_sign"),
        isCustomer.alias("affects_customer_balance"), isSupplier.alias("affects_supplier_balance"), isStock.alias("affects_inventory_value"),
        (F.col("is_reversal") == 1).alias("is_reversal_type"),
        yesNo(isCustomer | isSupplier).alias("requires_tax_analysis"),
        F.lit("GLOBAL").alias("region_code"),
        F.lit(config.SOURCE_SYSTEM_OLTP).alias("source_system_code"),
    ).withColumn("row_hash_type_1", rowHash("transaction_type", "transaction_type_code", "ledger_impact_code", "amount_sign"))


# ----------------------------------------------------------------------------- crosswalk-sourced dims

def crosswalkMembers(crosswalk: DataFrame, domain: str) -> DataFrame:
    """Distinct conformed codes of a crosswalk domain with their source aliases; region NULL means all regions."""
    return (
        crosswalk.where((F.col("code_domain_code") == domain) & F.col("is_active"))
        .groupBy(F.col("conformed_code_value").alias("conformed_code"))
        .agg(
            F.sort_array(F.collect_set(F.concat_ws(":", "source_system_code", "source_code_value"))).alias("source_codes"),
            F.sort_array(F.collect_set(F.coalesce(F.col("region_code"), F.lit("ALL")))).alias("regions"),
        )
        .withColumn("region_code", F.when(F.array_contains("regions", "ALL") | (F.size("regions") > 1), "GLOBAL").otherwise(F.element_at("regions", 1)))
        .withColumn("legacy_source_codes", F.concat_ws(";", "source_codes"))
        .drop("source_codes", "regions")
    )


def titleCase(col):
    return F.initcap(F.lower(F.regexp_replace(col, "_", " ")))


def buildDimCarrier(crosswalk: DataFrame) -> DataFrame:
    m = crosswalkMembers(crosswalk, "CARRIER")
    code = F.col("conformed_code")
    return m.select(
        code.alias("carrier_code"),
        F.when(code == "FDX", "FedEx").when(code == "UPS", "UPS").when(code == "DHL", "DHL").when(code == "OWN", "Own Fleet").when(code == "TOLL", "Toll").otherwise(titleCase(code)).alias("carrier_name"),
        "region_code", "legacy_source_codes",
        (code == "OWN").alias("is_own_fleet"),
        code.isin("DHL", "FDX", "UPS").alias("is_cross_border_capable"),
        F.when(F.col("region_code") == "NA", 3).when(F.col("region_code") == "EU", 2).when(F.col("region_code") == "APAC", 5).otherwise(4).cast("int").alias("target_transit_days"),
        F.when(code == "OWN", "OWN").otherwise("THIRD_PARTY").alias("carrier_type_code"),
        F.lit(True).alias("is_active"),
    ).withColumn("row_hash_type_1", rowHash("carrier_name", "region_code", "is_own_fleet", "target_transit_days"))


def buildDimLoyaltyTier(crosswalk: DataFrame) -> DataFrame:
    m = crosswalkMembers(crosswalk, "LOYALTY_TIER")
    rank = F.regexp_extract(F.col("conformed_code"), r"(\d+)$", 1).cast("int")
    return m.select(
        F.col("conformed_code").alias("loyalty_tier_code"),
        F.coalesce(F.element_at(F.split(F.element_at(F.split("legacy_source_codes", ";"), 1), ":"), 2), F.col("conformed_code")).alias("loyalty_tier_name"),
        F.coalesce(rank, F.lit(0)).alias("tier_rank"),
        "region_code", "legacy_source_codes",
        (F.coalesce(rank, F.lit(0)) * 2.5).cast("decimal(5,2)").alias("na_discount_pct"),
        (F.coalesce(rank, F.lit(0)) * 2.0).cast("decimal(5,2)").alias("eu_discount_pct"),
        (F.coalesce(rank, F.lit(0)) * 3.0).cast("decimal(5,2)").alias("apac_discount_pct"),
        (F.coalesce(rank, F.lit(0)) * 1000).cast("int").alias("qualifying_annual_spend"),
        F.lit(True).alias("is_active"),
    ).withColumn("loyalty_tier_name", F.initcap(F.lower("loyalty_tier_name"))).withColumn("row_hash_type_1", rowHash("loyalty_tier_name", "tier_rank"))


def buildDimSalesChannel(crosswalk: DataFrame) -> DataFrame:
    m = crosswalkMembers(crosswalk, "SALES_CHANNEL")
    code = F.col("conformed_code")
    return m.select(
        code.alias("sales_channel_code"),
        F.when(code == "ONLINE", "Online").when(code == "PARTNER", "Partner / Wholesale").when(code == "DIRECT", "Direct Sales").when(code == "TELE", "Telesales").otherwise(titleCase(code)).alias("sales_channel_name"),
        F.when(code == "ONLINE", "DIGITAL").when(code == "PARTNER", "INDIRECT").otherwise("DIRECT").alias("channel_type_code"),
        (code == "ONLINE").alias("is_self_service"),
        (code == "PARTNER").alias("is_indirect"),
        "region_code", "legacy_source_codes",
        F.lit(True).alias("is_active"),
    ).withColumn("row_hash_type_1", rowHash("sales_channel_name", "channel_type_code", "region_code"))


def buildDimReturnReason(reasonCodes: DataFrame, crosswalk: DataFrame) -> DataFrame:
    """One row per conformed RETURN reason, with the regional restocking-fee schedule."""
    aliases = crosswalkMembers(crosswalk, "RETURN").select(F.col("conformed_code").alias("_code"), "legacy_source_codes")
    r = reasonCodes.where("reason_domain_code = 'RETURN' AND is_active AND conformed_reason_code <> 'UNKNOWN'")
    group = F.col("reason_group_code")
    return r.join(aliases, r.conformed_reason_code == aliases._code, "left").select(
        F.col("conformed_reason_code").alias("return_reason_code"),
        F.col("conformed_reason_name").alias("return_reason_name"),
        group.alias("return_category_code"),
        (F.col("is_supplier_fault") == 1).alias("is_supplier_recoverable"),
        (F.col("is_customer_fault") == 1).alias("is_customer_fault"),
        (F.col("requires_approval") == 1).alias("requires_approval"),
        F.when(F.col("is_customer_fault") == 1, 10.0).otherwise(0.0).cast("decimal(5,2)").alias("na_restocking_fee_pct"),
        F.lit(0.0).cast("decimal(5,2)").alias("eu_restocking_fee_pct"),
        F.when(F.col("is_customer_fault") == 1, 5.0).otherwise(0.0).cast("decimal(5,2)").alias("apac_restocking_fee_pct"),
        F.coalesce(F.col("legacy_source_codes"), F.lit("")).alias("legacy_source_codes"),
        F.lit("GLOBAL").alias("region_code"),
        F.lit(True).alias("is_active"),
    ).withColumn("row_hash_type_1", rowHash("return_reason_name", "return_category_code", "is_supplier_recoverable"))


def buildDimPaymentTerms(stgTerms: DataFrame) -> DataFrame:
    """One row per (conformed terms code, region) with regional net-day caps and early-settlement flag; the
    generator rejects net days outside 0..180 and discounts outside 0..100."""
    valid = stgTerms.where(F.col("net_days").between(0, 180) & F.col("discount_percent").between(0, 100))
    w = Window.partitionBy("conformed_terms_code", "region_code").orderBy(F.col("net_days"), F.col("payment_terms_code"))
    best = valid.withColumn("rn", F.row_number().over(w)).where("rn = 1")
    cap = F.when(F.col("region_code") == "EU", 60).when(F.col("region_code") == "APAC", 90).otherwise(60)
    return best.select(
        F.col("conformed_terms_code").alias("payment_terms_code"),
        F.col("payment_terms_description").alias("payment_terms_description"),
        "region_code", "net_days", "discount_percent", "discount_days",
        F.col("payment_terms_code").alias("legacy_source_code"),
        F.least(F.col("net_days"), cap).cast("int").alias("regional_capped_net_days"),
        (F.col("net_days") > cap).alias("exceeds_regional_cap"),
        (F.col("discount_percent") > 0).alias("early_settlement_flag"),
        F.coalesce(F.col("calculation_basis_cd"), F.lit("NETDAY")).alias("calculation_basis_code"),
        F.lit(True).alias("is_active"),
    ).withColumn("row_hash_type_1", rowHash("payment_terms_description", "net_days", "discount_percent", "discount_days"))


def buildDimCurrency(refCurrency: DataFrame, refFx: DataFrame, asOf=None) -> DataFrame:
    """Active conformed currencies with their latest corporate USD rate, staleness (> 7 days) and status."""
    corp = refFx.where((F.col("rate_type_code") == "CORP") & (F.col("to_currency_code") == "USD"))
    latest = corp.groupBy(F.col("from_currency_code").alias("_ccy")).agg(
        F.max_by("conversion_rate", "rate_date").alias("usd_rate"), F.max("rate_date").alias("usd_rate_date")
    )
    maxDate = corp.agg(F.max("rate_date")).first()
    anchor = F.lit(asOf) if asOf is not None else (F.lit(maxDate[0]) if maxDate and maxDate[0] else F.current_date())
    c = refCurrency.where("is_active").join(latest, refCurrency.currency_code == latest._ccy, "left").drop("_ccy")
    staleness = F.datediff(anchor, F.col("usd_rate_date"))
    return c.select(
        "currency_code", "currency_name", "currency_symbol", "currency_numeric_code", "minor_unit_digits", "rounding_rule_code",
        "primary_country_code", "region_code", "is_transactional_currency", "is_reporting_currency",
        F.when(F.col("currency_code") == "USD", F.lit(1.0)).otherwise(F.col("usd_rate")).cast("decimal(18,8)").alias("usd_conversion_rate"),
        F.when(F.col("currency_code") == "USD", anchor).otherwise(F.col("usd_rate_date")).alias("usd_rate_date"),
        F.when(F.col("currency_code") == "USD", F.lit(False)).otherwise(F.col("usd_rate_date").isNull() | (staleness > 7)).alias("is_rate_stale"),
        F.when(F.col("currency_code") == "USD", "BASE").when(F.col("usd_rate_date").isNull(), "UNRATED").when(staleness > 7, "STALE").otherwise("CURRENT").alias("rate_status_code"),
        F.col("source_system_code"),
        F.lit(True).alias("is_active"),
    ).withColumn("row_hash_type_1", rowHash("currency_name", "minor_unit_digits", "usd_conversion_rate"))


def buildDimGeography(refRegion: DataFrame, refCountry: DataFrame, refTaxJurisdiction: DataFrame) -> DataFrame:
    """One conformed geography per country: region attributes, tax structure and reverse-charge/EU flags."""
    rg = refRegion.select(F.col("region_code").alias("_rg"), "region_name", "tax_regime_code", F.col("default_currency_code").alias("region_currency_code"), "fiscal_year_start_month")
    natTax = refTaxJurisdiction.where("jurisdiction_level_code IN ('COUNTRY','NATIONAL')").groupBy(F.col("country_code").alias("_tc")).agg(
        F.max("standard_rate_pct").alias("national_standard_rate_pct"), F.max(F.col("reverse_charge_eligible").cast("int")).alias("_rc")
    )
    c = refCountry.where("is_active").join(rg, refCountry.region_code == rg._rg, "left").join(natTax, refCountry.country_code == natTax._tc, "left")
    return c.select(
        F.concat_ws("-", "region_code", "country_code").alias("geography_code"),
        "country_code", "country_code_iso3", "country_name", "region_code", "region_name", "sub_region_name",
        F.coalesce(F.col("currency_code"), F.col("region_currency_code")).alias("currency_code"),
        F.coalesce(F.col("tax_regime_code"), F.when(F.col("region_code") == "EU", "VAT").when(F.col("region_code") == "APAC", "GST").otherwise("SALESTAX")).alias("tax_structure_code"),
        F.col("national_standard_rate_pct"),
        F.coalesce(F.col("_rc") == 1, F.col("is_eu_vat_area")).alias("reverse_charge_eligible"),
        F.col("is_eu_member"), F.col("is_eu_vat_area"), F.col("is_sanctioned"),
        F.col("postal_code_required_flag"), F.col("state_province_required_flag"), F.col("postal_format_mask"),
        F.col("fiscal_year_start_month"),
        F.lit(True).alias("is_active"),
    ).withColumn("row_hash_type_1", rowHash("country_name", "region_code", "currency_code", "tax_structure_code"))


def buildDimWarehouseSite(stockMovement: DataFrame, refCountry: DataFrame, postalRules: DataFrame) -> DataFrame:
    """Aggregate stock movement per site, attach the country rule set and standardise the postal code; sites
    without a resolvable country are rejected by the generator (they are simply excluded here)."""
    cols = {c.lower(): c for c in stockMovement.columns}
    site = F.col(cols.get("warehousesitecode", "warehouse_site_code"))
    movementKey = cols.get("stockmovementbusinesskey", cols.get("movement_key"))
    agg = stockMovement.groupBy(site.alias("warehouse_site_code")).agg(
        (F.count(F.col(movementKey)) if movementKey else F.count("*")).alias("movement_count"),
        F.max(F.col(cols.get("countrycode", "country_code"))).alias("country_code"),
        F.max(F.col(cols.get("postalcode", "postal_code"))).alias("postal_code"),
        F.max(F.col(cols.get("sitename", "site_name"))).alias("warehouse_site_name"),
    )
    country = refCountry.select(F.col("country_code").alias("_cc"), "region_code", "country_name")
    rules = postalRules.where("rule_priority = 1").select(F.col("country_code").alias("_pc"), "upper_case_flag", "strip_characters", "truncate_to_length")
    j = agg.join(country, agg.country_code == country._cc, "inner").join(rules, agg.country_code == rules._pc, "left")
    postal = F.regexp_replace(F.col("postal_code"), F.coalesce(F.col("strip_characters"), F.lit("")), "")
    return j.select(
        "warehouse_site_code", F.coalesce("warehouse_site_name", "warehouse_site_code").alias("warehouse_site_name"),
        "country_code", "country_name", "region_code",
        F.when(F.col("upper_case_flag") == 1, F.upper(postal)).otherwise(postal).alias("postcode_standardized"),
        "movement_count",
        F.when(F.col("movement_count") >= 10000, "HUB").when(F.col("movement_count") >= 1000, "REGIONAL").otherwise("SATELLITE").alias("site_type_code"),
        F.lit(True).alias("is_active"),
    ).withColumn("row_hash_type_1", rowHash("warehouse_site_name", "country_code", "site_type_code"))


def warehouseSiteSource(spark: SparkSession) -> DataFrame:
    """The package aggregates stg.StockMovement by WarehouseSiteId/Code/Name/CountryCode/PostalCode, but the
    deployed stg.StockMovement DDL only carries WarehouseCode (and is empty on the host). When the site
    columns are absent the site list comes from OLTP Warehouse.WarehouseSites instead (country = first two
    characters of the site code, exactly as the package derives it), with zero movements per site."""
    stockTable = f"{config.LEGACY_STAGING}.stg.StockMovement"
    if spark.catalog.tableExists(stockTable):
        stock = spark.table(stockTable)
        if "WarehouseSiteCode" in stock.columns and "CountryCode" in stock.columns:
            return stock
    sites = spark.table(f"{config.LEGACY_OLTP}.Warehouse.WarehouseSites")
    return sites.select(
        F.upper(F.trim("SiteCode")).alias("warehouse_site_code"),
        F.substring(F.upper(F.trim("SiteCode")), 1, 2).alias("country_code"),
        F.lit(None).cast("string").alias("postal_code"),
        F.col("SiteName").alias("site_name"),
        F.lit(None).cast("string").alias("movement_key"),
    )


# ----------------------------------------------------------------------------- code translation

def buildCodeTranslation(bronzeTranslation: DataFrame, crosswalk: DataFrame) -> tuple[DataFrame, DataFrame]:
    """REF_Load_CodeTranslation: conform the Oracle translation rows through the crosswalk; source values with no
    conformed mapping are reported with severity/review state instead of failing the load."""
    cols = {c.lower(): c for c in bronzeTranslation.columns}

    def col(*names, default=None):
        for n in names:
            if n in cols:
                return F.col(cols[n])
        return F.lit(default).cast("string")

    df = bronzeTranslation.select(
        col("translation_id", "code_translation_id").cast("long").alias("translation_id"),
        F.upper(F.trim(col("code_set_cd", "code_set"))).alias("code_domain_code"),
        F.upper(F.trim(col("source_sys_cd", "source_system_cd", "source_system"))).alias("source_system_code"),
        F.upper(F.trim(col("source_value_txt", "source_value", "source_cd"))).alias("source_code_value"),
        F.trim(col("target_value_txt", "target_value", "target_cd")).alias("target_code_value"),
        F.upper(F.trim(col("region_cd"))).alias("region_code"),
        col("target_entity", "entity_cd").alias("target_entity"),
        col("description_txt", "source_desc", "source_description").alias("source_description"),
        col("priority_nbr", "priority").cast("int").alias("priority"),
        col("active_flg", default="Y").alias("active_flg"),
        col("effective_from_dt").cast("date").alias("effective_from_date"),
        col("effective_to_dt").cast("date").alias("effective_to_date"),
    )
    xw = crosswalk.where("is_active").select(
        F.col("code_domain_code").alias("_d"), F.col("source_system_code").alias("_s"), F.col("source_code_value").alias("_v"), F.col("conformed_code_value")
    ).dropDuplicates(["_d", "_s", "_v"])
    j = df.join(xw, (df.code_domain_code == xw._d) & (df.source_code_value == xw._v) & (xw._s == F.coalesce(df.source_system_code, F.lit("ORA_ERP"))), "left").drop("_d", "_s", "_v")
    mapped = (
        j.withColumn("is_crosswalk_mapped", F.col("conformed_code_value").isNotNull())
        .withColumn("conformed_code_value", F.coalesce(F.col("conformed_code_value"), F.col("target_code_value")))
        .withColumn("is_active", F.upper(F.trim("active_flg")) == "Y").drop("active_flg")
    )
    unmapped = j.where(F.col("conformed_code_value").isNull() & F.col("target_code_value").isNull()).select(
        "code_domain_code", "source_system_code", "source_code_value", "region_code",
        F.when(F.col("code_domain_code").isin("CURRENCY", "COUNTRY", "REGION", "PAYMENT_METHOD"), "HIGH").otherwise("MEDIUM").alias("severity_code"),
        F.lit("PENDING_REVIEW").alias("review_state_code"),
    )
    return mapped, unmapped


# ----------------------------------------------------------------------------- date dimension

def buildDimDate(spark: SparkSession, startDate: str = config.DATE_DIMENSION_START, endDate: str = config.DATE_DIMENSION_END) -> DataFrame:
    """REF_Load_DateDimension: the WWI calendar (fiscal year starts in November) plus the NA/EU/APAC fiscal
    calendars from ref.Region, and the two reserved members 1900-01-01 (Unknown) / 1900-01-02 (N/A)."""
    regionStart = {r["region_code"]: r["fiscal_year_start_month"] for r in seeds.REGION_GRID}
    spine = spark.sql(f"SELECT explode(sequence(to_date('{startDate}'), to_date('{endDate}'), interval 1 day)) AS date")
    d = F.col("date")
    month, year, dow = F.month(d), F.year(d), F.dayofweek(d)
    wwiStart = config.WWI_FISCAL_YEAR_START_MONTH
    fiscalMonth = F.when(month >= wwiStart, month - wwiStart + 1).otherwise(month + 12 - wwiStart + 1)
    fiscalYear = F.when(month >= wwiStart, year + 1).otherwise(year)
    df = spine.select(
        d.alias("date"),
        F.dayofmonth(d).cast("int").alias("day_number"),
        F.dayofmonth(d).cast("string").alias("day"),
        F.date_format(d, "MMMM").alias("month"),
        F.date_format(d, "MMM").alias("short_month"),
        month.cast("int").alias("calendar_month_number"),
        F.concat(F.lit("CY"), year.cast("string"), F.lit("-"), F.date_format(d, "MMM")).alias("calendar_month_label"),
        year.cast("int").alias("calendar_year"),
        F.concat(F.lit("CY"), year.cast("string")).alias("calendar_year_label"),
        fiscalMonth.cast("int").alias("fiscal_month_number"),
        F.concat(F.lit("FY"), fiscalYear.cast("string"), F.lit("-"), F.date_format(d, "MMM")).alias("fiscal_month_label"),
        fiscalYear.cast("int").alias("fiscal_year"),
        F.concat(F.lit("FY"), fiscalYear.cast("string")).alias("fiscal_year_label"),
        F.weekofyear(d).cast("int").alias("iso_week_number"),
        F.date_format(d, "EEEE").alias("day_of_week"),
        dow.cast("int").alias("day_of_week_number"),
        F.quarter(d).cast("int").alias("calendar_quarter_number"),
        F.lit(False).alias("is_reserved_member"),
        F.lit(None).cast("string").alias("reserved_member_description"),
    )
    for region, start in regionStart.items():
        p = region.lower()
        if region == "APAC":
            fy = F.when(month >= start, year).otherwise(year - 1)
        elif start == 1:
            fy = year
        else:
            fy = F.when(month >= start, year + 1).otherwise(year)
        period = F.when(month >= start, month - start + 1).otherwise(month + 12 - start + 1)
        df = (
            df.withColumn(f"{p}_fiscal_year", fy.cast("int"))
            .withColumn(f"{p}_fiscal_year_label", F.concat(F.lit("FY"), fy.cast("string")))
            .withColumn(f"{p}_fiscal_period", period.cast("int"))
            .withColumn(f"{p}_fiscal_quarter", (((period - 1) / 3).cast("int") + 1).cast("int"))
            .withColumn(f"{p}_fiscal_period_label", F.concat(F.lit("FY"), fy.cast("string"), F.lit("-P"), F.lpad(period.cast("string"), 2, "0")))
            .withColumn(f"{p}_is_period_end", d == F.last_day(d))
            .withColumn(f"{p}_is_year_end", (d == F.last_day(d)) & (period == 12))
            .withColumn(f"is_weekend_{p}", yesNo(dow.isin(1, 7)))
            .withColumn(f"{p}_is_trading_day", yesNo(~dow.isin(1, 7)))
            .withColumn(f"{p}_same_period_last_year_date", F.add_months(d, -12))
        )
    df = df.withColumn("eu_iso_week_number", F.weekofyear(d).cast("int")).withColumn("eu_iso_week_year", F.year(F.date_add(d, 26 - F.dayofweek(d)).cast("date")).cast("int"))
    reserved = spark.createDataFrame(
        [("1900-01-01", 0, "Unknown", "Unknown", "UNK", 0, "Unknown", 1900, "CY1900", 0, "Unknown", 1900, "FY1900", 0, "Unknown", 0, 0, True, "Unknown date"),
         ("1900-01-02", 0, "N/A", "N/A", "N/A", 0, "N/A", 1900, "CY1900", 0, "N/A", 1900, "FY1900", 0, "N/A", 0, 0, True, "Not applicable - no date for this role")],
        ["date", "day_number", "day", "month", "short_month", "calendar_month_number", "calendar_month_label", "calendar_year", "calendar_year_label",
         "fiscal_month_number", "fiscal_month_label", "fiscal_year", "fiscal_year_label", "iso_week_number", "day_of_week", "day_of_week_number",
         "calendar_quarter_number", "is_reserved_member", "reserved_member_description"],
    ).withColumn("date", F.to_date("date"))
    return reserved.unionByName(df, allowMissingColumns=True).orderBy("date")


# ----------------------------------------------------------------------------- unknown members

def buildUnknownMembers(spark: SparkSession, registry: DataFrame) -> DataFrame:
    """REF_Load_UnknownMembers: the reserved member registry every other group keys off. Registered dimensions
    (except the big SCD2/hybrid ones, Date, Time and bridges) get -1 Unknown and -2 Not Applicable valid from
    1900-01-01; City additionally carries 0 Unknown from the WWI epoch; Date carries 1900-01-01/1900-01-02."""
    excluded = ["Customer", "Stock Item", "Supplier", "Employee", "Salesperson", "City", "Date", "Time"]
    reg = registry.select(
        F.regexp_replace(F.col("Dimension Name"), "^Dimension\\.", "").alias("dimension_name"),
        F.col("Key Column Name").alias("key_column_name"), F.col("SCD Pattern").alias("scd_pattern"),
        F.col("Reserved Key Low").cast("int").alias("reserved_key_low"), F.col("Reserved Key High").cast("int").alias("reserved_key_high"),
    ).where(~F.col("dimension_name").isin(excluded) & (F.col("scd_pattern") != "Bridge"))
    keys = spark.createDataFrame([(-1, "Unknown"), (-2, "Not Applicable")], ["member_key", "member_description"])
    generic = reg.crossJoin(keys).select(
        "dimension_name", "key_column_name", "member_key", "member_description",
        F.to_timestamp(F.lit(config.LOW_TIMESTAMP)).alias("valid_from"), F.to_timestamp(F.lit(HIGH_TS)).alias("valid_to"),
        F.lit("GLOBAL").alias("region_code"), "reserved_key_low", "reserved_key_high",
    )
    special = spark.createDataFrame(
        [("City", "City Key", -2, "Not Applicable", config.LOW_TIMESTAMP), ("City", "City Key", -1, "Unknown", config.LOW_TIMESTAMP),
         ("City", "City Key", 0, "Unknown", WWI_EPOCH), ("Date", "Date", 19000101, "Unknown date (1900-01-01)", config.LOW_TIMESTAMP),
         ("Date", "Date", 19000102, "Not applicable - no date for this role (1900-01-02)", config.LOW_TIMESTAMP),
         ("Time", "Time Key", -1, "Unknown time", config.LOW_TIMESTAMP), ("Time", "Time Key", -2, "Not applicable - date grain fact", config.LOW_TIMESTAMP)],
        ["dimension_name", "key_column_name", "member_key", "member_description", "valid_from"],
    ).select(
        "dimension_name", "key_column_name", "member_key", "member_description", F.to_timestamp("valid_from").alias("valid_from"),
        F.to_timestamp(F.lit(HIGH_TS)).alias("valid_to"), F.lit("GLOBAL").alias("region_code"), F.lit(-9).alias("reserved_key_low"), F.lit(0).alias("reserved_key_high"),
    )
    return generic.unionByName(special).orderBy("dimension_name", "member_key")


# ----------------------------------------------------------------------------- entrypoints

def runRefLoadDimensions(spark: SparkSession, batchId: int) -> dict:
    crosswalk = spark.table(config.tbl("silver_ref_code_crosswalk"))
    counts = {}

    def publish(name, df, keyCol, orderCols, nameCols, idCol=None):
        out = finaliseDimension(spark, df, keyCol, orderCols, nameCols, batchId, idCol)
        writeTable(out, config.tbl(name))
        counts[name] = spark.table(config.tbl(name)).count()

    publish("gold_dim_payment_method", buildDimPaymentMethod(spark.table(config.tbl("bronze_sql_payment_methods")), crosswalk), "payment_method_key",
            [F.col("valid_from"), F.col("is_history").desc(), F.col("wwi_payment_method_id")], ["payment_method"], "wwi_payment_method_id")
    publish("gold_dim_transaction_type", buildDimTransactionType(spark.table(config.tbl("bronze_sql_transaction_types")), crosswalk), "transaction_type_key",
            [F.col("valid_from"), F.col("is_history").desc(), F.col("wwi_transaction_type_id")], ["transaction_type"], "wwi_transaction_type_id")
    publish("gold_dim_carrier", buildDimCarrier(crosswalk), "carrier_key", ["carrier_code"], ["carrier_code", "carrier_name"])
    publish("gold_dim_loyalty_tier", buildDimLoyaltyTier(crosswalk), "loyalty_tier_key", ["tier_rank", "loyalty_tier_code"], ["loyalty_tier_code", "loyalty_tier_name"])
    publish("gold_dim_sales_channel", buildDimSalesChannel(crosswalk), "sales_channel_key", ["sales_channel_code"], ["sales_channel_code", "sales_channel_name"])
    publish("gold_dim_return_reason", buildDimReturnReason(spark.table(config.tbl("silver_ref_reason_code")), crosswalk), "return_reason_key", ["return_reason_code"], ["return_reason_code", "return_reason_name"])
    publish("gold_dim_payment_terms", buildDimPaymentTerms(spark.table(config.tbl("silver_stg_payment_terms"))), "payment_terms_key", ["payment_terms_code", "region_code"], ["payment_terms_code", "payment_terms_description"])
    publish("gold_dim_currency", buildDimCurrency(spark.table(config.tbl("silver_ref_currency")), spark.table(config.tbl("silver_ref_fx_rate_daily"))), "currency_key", ["currency_code"], ["currency_code", "currency_name"])
    publish("gold_dim_geography", buildDimGeography(spark.table(config.tbl("silver_ref_region")), spark.table(config.tbl("silver_ref_country")), spark.table(config.tbl("silver_ref_tax_jurisdiction"))),
            "geography_key", ["geography_code"], ["geography_code", "country_name"])
    publish("gold_dim_warehouse_site", buildDimWarehouseSite(warehouseSiteSource(spark), spark.table(config.tbl("silver_ref_country")), spark.table(config.tbl("silver_ref_postal_format_rule"))),
            "warehouse_site_key", ["warehouse_site_code"], ["warehouse_site_code", "warehouse_site_name"])
    mapped, unmapped = buildCodeTranslation(spark.table(config.tbl("bronze_oracle_code_translation")), crosswalk)
    writeTable(mapped.withColumn("batch_id", F.lit(batchId).cast("bigint")), config.tbl("gold_ref_code_translation"))
    writeTable(unmapped.withColumn("batch_id", F.lit(batchId).cast("bigint")), config.tbl("gold_ref_unmapped_source_code"))
    counts["gold_ref_code_translation"] = spark.table(config.tbl("gold_ref_code_translation")).count()
    counts["gold_ref_unmapped_source_code"] = spark.table(config.tbl("gold_ref_unmapped_source_code")).count()
    writeTable(buildDimDate(spark).withColumn("last_load_batch_id", F.lit(batchId).cast("bigint")), config.tbl("gold_dim_date"))
    counts["gold_dim_date"] = spark.table(config.tbl("gold_dim_date")).count()
    registry = readLegacyDw(spark, "Integration", "DimensionKeyRegistry")
    writeTable(buildUnknownMembers(spark, registry).withColumn("last_load_batch_id", F.lit(batchId).cast("bigint")), config.tbl("gold_dim_unknown_member"))
    counts["gold_dim_unknown_member"] = spark.table(config.tbl("gold_dim_unknown_member")).count()
    return counts
