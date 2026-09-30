"""Silver layer: the legacy sale facts and the dimensions the sales packages need.

stg.SaleLine is empty on the legacy host, so the silver sale line is rebuilt from the legacy
``Fact.Sale`` rows plus the Customer / Employee / City / Stock Item dimensions. Regional
attributes that the legacy warehouse never populated (region, territory, currency, fiscal
period, tax treatment) are derived here with the same rules the packages apply downstream.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_performance import config, fiscal
from sales_performance.common import readDw, readOltp, saveTable, snakeCaseColumns, withAudit

SALE_LINE_TABLE = "silver_sale_line"
FACT_SALE_TABLE = "gold_fact_sale"
FACT_ORDER_TABLE = "gold_fact_order"
FACT_PAYMENT_TABLE = "gold_fact_payment"
CUSTOMER_TABLE = "silver_dim_customer"
STOCK_ITEM_TABLE = "silver_dim_stock_item"
EMPLOYEE_TABLE = "silver_dim_employee"
DATE_TABLE = "silver_dim_date"
TERRITORY_TABLE = "silver_sales_territory"
PLAN_TABLE = "silver_commission_plan"
QUOTA_TABLE = "silver_sales_quota"
FX_TABLE = "silver_fx_rate_average"

WESTERN_STATES = (
    "Washington",
    "Oregon",
    "California",
    "Nevada",
    "Idaho",
    "Montana",
    "Wyoming",
    "Utah",
    "Colorado",
    "Arizona",
    "New Mexico",
    "Alaska",
    "Hawaii",
    "Texas",
    "Oklahoma",
    "Kansas",
    "Nebraska",
    "South Dakota",
    "North Dakota",
)


def _literalMap(pairs):
    return F.create_map(*[x for k, v in pairs for x in (F.lit(k), F.lit(v))])


def countryRegionMap():
    return _literalMap([(name, region) for (name, region, _) in config.COUNTRY_MAP.values()])


def countryIso3Map():
    return _literalMap([(name, iso3) for (name, _, iso3) in config.COUNTRY_MAP.values()])


def regionCurrencyMap():
    return _literalMap(list(config.REGION_CURRENCY.items()))


def regionForCountry(countryName):
    return F.coalesce(countryRegionMap()[F.upper(countryName)], F.lit("UNK"))


def territoryForRow(countryName, stateProvince):
    iso3 = countryIso3Map()[F.upper(countryName)]
    return (
        F.when((iso3 == "USA") & F.col(stateProvince).isin(*WESTERN_STATES), "NA-US-WEST")
        .when(iso3 == "USA", "NA-US-EAST")
        .when(iso3 == "CAN", "NA-CA")
        .when(iso3 == "GBR", "EU-UK")
        .when(iso3 == "DEU", "EU-DE")
        .when(iso3 == "NLD", "EU-NL")
        .when(iso3 == "AUS", "AP-AU")
        .when(iso3 == "SGP", "AP-SG")
        .when(iso3 == "JPN", "AP-JP")
        .otherwise(F.lit("UNASSIGNED"))
    )


def buildSaleLine(factSale: DataFrame, customers: DataFrame, employees: DataFrame, cities: DataFrame, stockItems: DataFrame) -> DataFrame:
    """Pure transformation from snake_cased legacy frames to the silver sale line."""
    cust = customers.select(
        F.col("customer_key"),
        F.col("wwi_customer_id").alias("customer_id"),
        F.col("customer").alias("customer_name"),
        F.col("category").alias("customer_category"),
        F.col("buying_group"),
        F.col("postal_code").alias("customer_postal_code"),
    ).dropDuplicates(["customer_key"])
    emp = employees.select(
        F.col("employee_key").alias("salesperson_key"),
        F.col("wwi_employee_id").alias("salesperson_id"),
        F.col("employee").alias("salesperson_name"),
    ).dropDuplicates(["salesperson_key"])
    city = cities.select(F.col("city_key"), F.col("city"), F.col("state_province"), F.col("country").alias("country_name")).dropDuplicates(
        ["city_key"]
    )
    item = stockItems.select(
        F.col("stock_item_key"),
        F.col("wwi_stock_item_id").alias("stock_item_id"),
        F.col("stock_item").alias("stock_item_name"),
        F.col("brand"),
        F.col("barcode"),
    ).dropDuplicates(["stock_item_key"])

    lineWindow = Window.partitionBy("wwi_invoice_id").orderBy("sale_key")
    df = (
        factSale.withColumn("invoice_line_number", F.row_number().over(lineWindow))
        .join(cust, "customer_key", "left")
        .join(emp, "salesperson_key", "left")
        .join(city, "city_key", "left")
        .join(item, "stock_item_key", "left")
    )
    region = regionForCountry(F.col("country_name"))
    calendar = fiscal.calendarForRegion(region)
    extended = F.col("total_excluding_tax").cast("decimal(19,4)")
    tax = F.col("tax_amount").cast("decimal(19,4)")
    including = F.col("total_including_tax").cast("decimal(19,4)")
    return df.select(
        F.col("sale_key").cast("bigint"),
        F.col("wwi_invoice_id").cast("bigint").alias("invoice_number"),
        F.col("invoice_line_number"),
        F.col("invoice_date_key").cast("date").alias("invoice_date"),
        F.col("delivery_date_key").cast("date").alias("delivery_date"),
        F.coalesce(F.col("customer_key"), F.lit(config.UNKNOWN_KEY)).cast("bigint").alias("customer_key"),
        F.col("customer_id").cast("bigint"),
        F.col("customer_name"),
        F.col("customer_category"),
        F.col("buying_group"),
        F.coalesce(F.col("bill_to_customer_key"), F.lit(config.UNKNOWN_KEY)).cast("bigint").alias("bill_to_customer_key"),
        (F.col("bill_to_customer_key") != F.col("customer_key")).alias("is_house_account"),
        F.coalesce(F.col("city_key"), F.lit(config.UNKNOWN_KEY)).cast("bigint").alias("city_key"),
        F.col("state_province"),
        F.col("country_name"),
        F.coalesce(countryIso3Map()[F.upper(F.col("country_name"))], F.lit("UNK")).alias("country_code_iso3"),
        region.alias("region_code"),
        territoryForRow(F.col("country_name"), "state_province").alias("territory_code"),
        F.coalesce(F.col("stock_item_key"), F.lit(config.UNKNOWN_KEY)).cast("bigint").alias("stock_item_key"),
        F.col("stock_item_id").cast("bigint"),
        F.col("stock_item_name"),
        F.col("brand"),
        F.coalesce(F.col("salesperson_key"), F.lit(config.UNKNOWN_KEY)).cast("bigint").alias("salesperson_key"),
        F.col("salesperson_id").cast("bigint"),
        F.col("salesperson_name"),
        F.col("description"),
        F.col("package"),
        F.col("quantity").cast("decimal(18,3)").alias("quantity"),
        F.col("unit_price").cast("decimal(19,4)").alias("unit_price"),
        F.col("tax_rate").cast("decimal(9,4)").alias("tax_rate"),
        extended.alias("extended_price"),
        tax.alias("tax_amount"),
        including.alias("total_including_tax"),
        F.col("profit").cast("decimal(19,4)").alias("profit"),
        (extended - F.col("profit").cast("decimal(19,4)")).alias("cost_amount"),
        F.lit(None).cast("decimal(19,4)").alias("net_amount_reported"),
        extended.alias("net_amount"),
        F.when(region == "EU", tax).alias("vat_amount"),
        F.when(region == "EU", F.col("tax_rate").cast("decimal(9,4)")).alias("vat_rate_percent"),
        F.when(region == "APAC", tax).alias("gst_amount"),
        F.coalesce(regionCurrencyMap()[region], F.lit("USD")).alias("currency_code"),
        F.when(F.col("unit_price") == 0, "SAMPLE").otherwise("STANDARD").alias("line_type_code"),
        F.lit("DIRECT").alias("sales_channel_code"),
        calendar.alias("fiscal_calendar_code"),
        fiscal.fiscalYear(F.col("invoice_date_key"), calendar).alias("fiscal_year"),
        fiscal.fiscalPeriod(F.col("invoice_date_key"), calendar).alias("fiscal_period"),
        fiscal.fiscalPeriodLabel(F.col("invoice_date_key"), calendar).alias("fiscal_period_label"),
        fiscal.calendarMonth(F.col("invoice_date_key")).alias("calendar_month"),
        F.lit(False).alias("is_reversal"),
        F.lit(None).cast("bigint").alias("reverses_sale_key"),
        F.lit(False).alias("is_correction"),
        F.col("lineage_key").cast("bigint").alias("legacy_lineage_key"),
    )


def buildTerritories(territories: DataFrame) -> DataFrame:
    t = snakeCaseColumns(territories)
    return t.select(
        F.col("sales_territory_id").cast("int"),
        F.col("territory_code"),
        F.col("territory_name"),
        F.col("parent_territory_id").cast("int"),
        F.col("territory_level").cast("int"),
        F.trim(F.col("region_code")).alias("region_code"),
        F.col("country_iso3").alias("country_code_iso3"),
        F.col("tax_regime_code"),
        F.col("fiscal_calendar_code"),
        F.col("reporting_currency_code"),
        F.col("is_active").cast("boolean"),
    )


def buildCommissionPlans(plans: DataFrame) -> DataFrame:
    p = snakeCaseColumns(plans)
    return p.select(
        F.col("commission_plan_id").cast("int"),
        F.col("plan_code"),
        F.col("plan_name"),
        F.trim(F.col("region_code")).alias("region_code"),
        F.col("commission_basis"),
        F.col("band1_upper_percent").cast("decimal(9,4)"),
        F.col("band1_rate_percent").cast("decimal(9,4)"),
        F.col("band2_upper_percent").cast("decimal(9,4)"),
        F.col("band2_rate_percent").cast("decimal(9,4)"),
        F.col("band3_upper_percent").cast("decimal(9,4)"),
        F.col("band3_rate_percent").cast("decimal(9,4)"),
        F.col("accelerator_percent").cast("decimal(9,4)"),
        F.col("minimum_margin_percent").cast("decimal(9,4)"),
        F.col("effective_from_date").cast("date"),
        F.col("effective_to_date").cast("date"),
        F.col("plan_code").like("%FIELD%").alias("is_default_plan"),
        F.lit(None).cast("decimal(19,4)").alias("statutory_cap_amount"),
    )


def buildQuotas(quotas: DataFrame, territories: DataFrame) -> DataFrame:
    q = snakeCaseColumns(quotas)
    return q.join(territories.select("sales_territory_id", "territory_code", "region_code"), "sales_territory_id", "left").select(
        F.col("sales_quota_id").cast("bigint"),
        F.col("territory_code"),
        F.col("region_code"),
        F.col("salesperson_person_id").cast("bigint").alias("salesperson_id"),
        F.col("fiscal_calendar_code"),
        F.col("fiscal_period_label"),
        F.col("period_start_date").cast("date"),
        F.col("period_end_date").cast("date"),
        F.col("quota_amount").cast("decimal(19,4)"),
        F.col("quota_currency_code"),
        F.col("stretch_quota_amount").cast("decimal(19,4)"),
        F.col("quota_status"),
    )


def loadSilverLayer(spark: SparkSession, batchId: int):
    """Materialise the silver tables from the legacy warehouse/OLTP through federation."""
    factSale = snakeCaseColumns(readDw(spark, "Fact", "Sale"))
    customers = snakeCaseColumns(readDw(spark, "Dimension", "Customer"))
    employees = snakeCaseColumns(readDw(spark, "Dimension", "Employee")).drop("photo")
    cities = snakeCaseColumns(readDw(spark, "Dimension", "City")).drop("location")
    stockItems = snakeCaseColumns(readDw(spark, "Dimension", "Stock Item")).drop("photo")
    dates = snakeCaseColumns(readDw(spark, "Dimension", "Date"))
    territories = buildTerritories(readOltp(spark, "Sales", "SalesTerritories"))
    plans = buildCommissionPlans(readOltp(spark, "Sales", "CommissionPlans"))
    quotas = buildQuotas(readOltp(spark, "Sales", "SalesQuotas"), territories)

    saveTable(withAudit(customers, "silver", batchId), CUSTOMER_TABLE)
    saveTable(withAudit(employees, "silver", batchId), EMPLOYEE_TABLE)
    saveTable(withAudit(stockItems, "silver", batchId), STOCK_ITEM_TABLE)
    saveTable(withAudit(dates, "silver", batchId), DATE_TABLE)
    saveTable(withAudit(territories, "silver", batchId), TERRITORY_TABLE)
    saveTable(withAudit(plans, "silver", batchId), PLAN_TABLE)
    saveTable(withAudit(quotas, "silver", batchId), QUOTA_TABLE)
    saveTable(withAudit(buildFxRates(spark), "silver", batchId), FX_TABLE)

    saleLine = buildSaleLine(factSale, customers, employees, cities, stockItems)
    saveTable(withAudit(saleLine, "silver", batchId), SALE_LINE_TABLE)
    return spark.table(config.tableName(SALE_LINE_TABLE)).count()


def buildFxRates(spark: SparkSession) -> DataFrame:
    """Average (period) FX rates. The legacy ref.FxRateDaily/stg.FxRate tables are empty, so the
    only rates known to the estate are the identities; anything else is a missing rate."""
    currencies = sorted(set(config.REGION_CURRENCY.values()) | {"CAD", "GBP", "SGD", "JPY"})
    rows = [(c, c, "AVERAGE", 1.0) for c in currencies]
    return spark.createDataFrame(rows, "currency_code string, quote_currency_code string, rate_type_code string, conversion_rate double")


def buildFactOrder(factOrder: DataFrame, customers: DataFrame, employees: DataFrame, cities: DataFrame) -> DataFrame:
    cust = customers.select("customer_key", F.col("wwi_customer_id").alias("customer_id")).dropDuplicates(["customer_key"])
    emp = employees.select(F.col("employee_key").alias("salesperson_key"), F.col("wwi_employee_id").alias("salesperson_id")).dropDuplicates(
        ["salesperson_key"]
    )
    city = cities.select("city_key", "state_province", F.col("country").alias("country_name")).dropDuplicates(["city_key"])
    lineWindow = Window.partitionBy("wwi_order_id").orderBy("order_key")
    df = (
        factOrder.withColumn("order_line_number", F.row_number().over(lineWindow))
        .join(cust, "customer_key", "left")
        .join(emp, "salesperson_key", "left")
        .join(city, "city_key", "left")
    )
    region = regionForCountry(F.col("country_name"))
    return df.select(
        F.col("order_key").cast("bigint"),
        F.col("wwi_order_id").cast("bigint").alias("order_number"),
        "order_line_number",
        F.col("order_date_key").cast("date").alias("order_date"),
        F.coalesce(F.col("customer_key"), F.lit(config.UNKNOWN_KEY)).cast("bigint").alias("customer_key"),
        F.col("customer_id").cast("bigint"),
        F.coalesce(F.col("stock_item_key"), F.lit(config.UNKNOWN_KEY)).cast("bigint").alias("stock_item_key"),
        F.coalesce(F.col("salesperson_key"), F.lit(config.UNKNOWN_KEY)).cast("bigint").alias("salesperson_key"),
        F.col("salesperson_id").cast("bigint"),
        F.coalesce(F.col("city_key"), F.lit(config.UNKNOWN_KEY)).cast("bigint").alias("city_key"),
        region.alias("region_code"),
        territoryForRow(F.col("country_name"), "state_province").alias("territory_code"),
        F.col("quantity").cast("decimal(18,3)"),
        F.col("unit_price").cast("decimal(19,4)"),
        F.col("total_excluding_tax").cast("decimal(19,4)").alias("extended_price"),
        F.col("tax_amount").cast("decimal(19,4)"),
        F.col("total_including_tax").cast("decimal(19,4)"),
        F.lit(False).alias("is_restated"),
        F.col("lineage_key").cast("bigint").alias("legacy_lineage_key"),
    )


def loadGoldFacts(spark: SparkSession, batchId: int, resetCorrections: bool = False):
    """gold_fact_sale / gold_fact_order start as copies of the silver facts; commissions post onto
    them and FACT_Apply_Corrections appends reversal/replacement rows. Rows created by earlier
    corrections survive a reload unless ``resetCorrections`` is set."""
    saleLine = spark.table(config.tableName(SALE_LINE_TABLE)).withColumn("correction_lineage_key", F.lit(None).cast("bigint"))
    fullName = config.tableName(FACT_SALE_TABLE)
    if spark.catalog.tableExists(fullName) and not resetCorrections:
        existing = spark.table(fullName)
        extra = existing.filter(F.col("reverses_sale_key").isNotNull())
        if extra.limit(1).count() > 0:
            reversed_ = extra.select(F.col("reverses_sale_key").alias("sale_key")).distinct()
            saleLine = (
                saleLine.join(reversed_, "sale_key", "left_anti")
                .unionByName(existing.join(reversed_, "sale_key", "inner"), allowMissingColumns=True)
                .unionByName(extra, allowMissingColumns=True)
            )
    saveTable(saleLine, FACT_SALE_TABLE)
    factOrder = snakeCaseColumns(readDw(spark, "Fact", "Order"))
    order = buildFactOrder(
        factOrder,
        spark.table(config.tableName(CUSTOMER_TABLE)),
        spark.table(config.tableName(EMPLOYEE_TABLE)),
        snakeCaseColumns(readDw(spark, "Dimension", "City")).drop("location"),
    )
    order = _keepRestated(
        spark, withAudit(order, "gold", batchId), FACT_ORDER_TABLE, ["order_number", "order_line_number"], resetCorrections
    )
    saveTable(order, FACT_ORDER_TABLE)
    payment = snakeCaseColumns(readDw(spark, "Fact", "Payment")).withColumn("is_restated", F.lit(False))
    payment = _keepRestated(
        spark, withAudit(payment, "gold", batchId), FACT_PAYMENT_TABLE, ["receipt_number", "receipt_line_number"], resetCorrections
    )
    saveTable(payment, FACT_PAYMENT_TABLE)
    return spark.table(fullName).count()


def _keepRestated(spark: SparkSession, fresh: DataFrame, table: str, keys, reset: bool) -> DataFrame:
    """Rows restated in place by FACT_Apply_Corrections win over the legacy copy on reload."""
    fullName = config.tableName(table)
    if reset or not spark.catalog.tableExists(fullName):
        return fresh
    existing = spark.table(fullName)
    if "is_restated" not in existing.columns:
        return fresh
    restated = existing.filter(F.col("is_restated"))
    if restated.limit(1).count() == 0:
        return fresh
    restated = spark.createDataFrame(restated.collect(), restated.schema)
    return fresh.join(restated.select(*keys), keys, "left_anti").unionByName(restated, allowMissingColumns=True)
