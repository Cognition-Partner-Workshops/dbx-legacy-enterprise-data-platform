"""Spark ports of the sqlserver/reference/ref.usp_Load*.sql procedures (the SSIS pre-tasks).

Each function takes the PackageContext of the running notebook, reads bronze/silver Delta tables,
merges into ``silver.ref_*`` with delta_io.mergeReference and logs the same counts the procedure
handed to etl.usp_LogRowCount (SourceRowCount / TargetRowCount / Insert / Update / Reject).

Steward-maintained rows (MaintainedByName different from the loading package) are never updated or
deleted, effective dates are preserved (superseded mappings are closed, never overwritten).
"""
import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException

from wwi_ref import grids
from wwi_ref.delta_io import mergeReference, withSequence

RAW_COLUMNS = {
    "raw.OracleGeography": ["GEOGRAPHY_ID", "COUNTRY_CD", "COUNTRY_NAME", "ISO3_CD", "REGION_CD", "SUB_REGION_NAME",
                            "STATE_PROVINCE_CD", "STATE_PROVINCE_NAME", "CITY_NAME", "POSTAL_CD", "POSTAL_FORMAT_MASK",
                            "TIMEZONE_NAME", "CURRENCY_CD", "TAX_JURISDICTION_CD", "POPULATION_NUM", "LATITUDE",
                            "LONGITUDE", "LAST_UPDATE_DT", "BatchId", "PackageExecutionId", "LoadedAtUtc",
                            "SourceSystemCode", "SourceRowNumber"],
    "raw.OracleCurrency": ["CURRENCY_CD", "CURRENCY_NAME", "CURRENCY_SYMBOL", "MINOR_UNIT_DIGITS", "ISO_NUMERIC_CD",
                           "ACTIVE_FLG", "EURO_LEGACY_FLG", "LEGACY_FIXED_RATE", "LAST_UPDATE_DT", "BatchId",
                           "PackageExecutionId", "LoadedAtUtc", "SourceSystemCode", "SourceRowNumber"],
    "raw.OracleFxRate": ["FROM_CURRENCY_CD", "TO_CURRENCY_CD", "RATE_DT", "RATE_TYPE_CD", "CONVERSION_RATE",
                         "INVERSE_RATE", "RATE_SOURCE_CD", "LEDGER_CD", "LAST_UPDATE_DT", "BatchId",
                         "PackageExecutionId", "LoadedAtUtc", "SourceSystemCode", "SourceRowNumber"],
    "raw.OracleTaxRate": ["TAX_RATE_ID", "TAX_CD", "TAX_REGIME_CD", "TAX_JURISDICTION_CD", "COUNTRY_CD",
                          "STATE_PROVINCE_CD", "TAX_CLASS_CD", "RATE_PCT", "COMPOUND_FLG", "RECOVERABLE_PCT",
                          "REVERSE_CHARGE_FLG", "EFFECTIVE_FROM_DT", "EFFECTIVE_TO_DT", "LAST_UPDATE_DT", "BatchId",
                          "PackageExecutionId", "LoadedAtUtc", "SourceSystemCode", "SourceRowNumber"],
    "raw.OracleProductMaster": ["PRODUCT_ID", "PRODUCT_CD", "PRODUCT_DESC", "BASE_UOM_CD", "SELL_UOM_CD",
                                "UOM_CONVERSION_FACTOR", "LIFECYCLE_STATUS_CD", "WWI_STOCK_ITEM_ID", "LAST_UPDATE_DT",
                                "BatchId", "PackageExecutionId", "LoadedAtUtc", "SourceSystemCode", "SourceRowNumber"],
    "stg.StockMovement": ["StagingStockMovementId", "StockMovementBusinessKey", "SourceSystemCode", "StockItemBusinessKey",
                          "MovementTypeCode", "MovementReasonCode", "MovementDirection", "WarehouseCode", "BinCode",
                          "MovementDate", "Quantity", "BatchId", "PackageExecutionId", "LoadedAtUtc"],
}

REPORTING_CURRENCY_CODE = "USD"
MAX_FILL_FORWARD_DAYS = 5
DEFAULT_EFFECTIVE_FROM = "1900-01-01"


# --------------------------------------------------------------------------- source access
def emptyFrame(spark, columns):
    return spark.createDataFrame([], T.StructType([T.StructField(c, T.StringType()) for c in columns]))


def readOptional(ctx, legacyName, columns=None):
    """Bronze extracts are owned by other bundles; when a source table is not (yet) there the load
    proceeds with zero source rows (the legacy procedure would have found an empty raw table) and a
    warning is written to etl.error_log so the operator can see why nothing changed."""
    tableFqn = ctx.table(legacyName)
    try:
        return ctx.spark.table(tableFqn)
    except AnalysisException as exc:
        ctx.logWarning("source %s is not available: %s" % (tableFqn, str(exc).splitlines()[0]),
                       sourceComponent=ctx.currentStep, errorCode="SOURCE_MISSING")
        return emptyFrame(ctx.spark, columns or RAW_COLUMNS.get(legacyName, ["BatchId"]))


def rawBatch(ctx, legacyName):
    """``WHERE r.BatchId = @BatchId`` of the procedures. When the current batch loaded nothing into the
    extract the most recent extract batch is used instead (stand-alone reference refresh)."""
    df = readOptional(ctx, legacyName)
    if "BatchId" not in df.columns:
        return df
    current = df.where(F.col("BatchId") == ctx.batchId)
    if current.limit(1).count() > 0:
        return current
    latest = df.agg(F.max("BatchId")).first()[0]
    if latest is None:
        return current
    ctx.logWarning("%s has no rows for BatchId %s; using latest extract batch %s" % (legacyName, ctx.batchId, latest),
                   errorCode="SOURCE_BATCH_FALLBACK")
    return df.where(F.col("BatchId") == latest)


def cleanString(col):
    """stg.ufn_CleanString: trim, collapse internal whitespace, empty -> NULL."""
    cleaned = F.trim(F.regexp_replace(F.col(col).cast("string"), r"\s+", " "))
    return F.when(F.length(cleaned) == 0, F.lit(None)).otherwise(cleaned)


def safeDate(col):
    """stg.ufn_SafeDate: tolerant parse of the ISO / US / compact forms the ERP emits, NULL when unparseable."""
    s = F.trim(F.col(col).cast("string"))
    return F.coalesce(
        F.expr("try_to_timestamp(%s, 'yyyy-MM-dd HH:mm:ss')" % col).cast("date"),
        F.expr("try_to_timestamp(%s, 'yyyy-MM-dd')" % col).cast("date"),
        F.expr("try_to_timestamp(%s, 'dd-MMM-yy')" % col).cast("date"),
        F.expr("try_to_timestamp(%s, 'MM/dd/yyyy')" % col).cast("date"),
        F.expr("try_to_timestamp(%s, 'yyyyMMdd')" % col).cast("date"),
        F.when(F.length(s) >= 10, F.expr("try_to_timestamp(substr(%s, 1, 10), 'yyyy-MM-dd')" % col).cast("date")),
    )


def safeDecimal(col, precision=19, scale=8):
    return F.expr("try_cast(%s AS DECIMAL(%d,%d))" % (col, precision, scale))


def flag(col):
    return F.upper(F.trim(F.col(col).cast("string"))).isin("Y", "1", "TRUE", "T")


def gridFrame(spark, columns, rows):
    """Steward grid -> DataFrame with an explicit schema (columns that are NULL on every row cannot be inferred)."""
    fields = []
    for i, name in enumerate(columns):
        sample = next((r[i] for r in rows if r[i] is not None), None)
        if isinstance(sample, bool):
            dataType = T.BooleanType()
        elif isinstance(sample, int):
            dataType = T.IntegerType()
        elif isinstance(sample, float):
            dataType = T.DoubleType()
        else:
            dataType = T.StringType()
        fields.append(T.StructField(name, dataType, True))
    return spark.createDataFrame([tuple(r) for r in rows], T.StructType(fields))


def activeCrosswalk(ctx, domain=None):
    df = ctx.spark.table(ctx.table("ref.CodeCrosswalk")).where(F.col("EffectiveToDate").isNull())
    return df.where(F.col("CodeDomainCode") == domain) if domain else df


def _count(df):
    return df.count()


def _log(ctx, objectName, source, target, counts, rejects=0):
    ctx.addCounts(objectName, sourceRowCount=source, targetRowCount=target, insertRowCount=counts.inserted,
                  updateRowCount=counts.updated, deleteRowCount=counts.deleted, rejectRowCount=rejects)


# --------------------------------------------------------------------------- ref.usp_LoadRegion
def loadRegion(ctx):
    ctx.step("Load Regions")
    spark = ctx.spark
    target = ctx.table("ref.Region")
    regions = gridFrame(spark, grids.REGION_COLUMNS, grids.REGIONS).withColumn("IsActive", F.lit(True))
    counts = mergeReference(spark, target, regions, ["RegionCode"])
    geo = rawBatch(ctx, "raw.OracleGeography")
    misses = (geo.select(F.upper(F.trim(F.col("REGION_CD"))).alias("REGION_CD"), "SourceSystemCode")
              .where(F.col("REGION_CD").isNotNull())
              .join(activeCrosswalk(ctx, "REGION").select(F.col("SourceCodeValue").alias("REGION_CD")), "REGION_CD", "left_anti")
              .join(regions.select(F.col("RegionCode").alias("REGION_CD")), "REGION_CD", "left_anti")
              .groupBy("REGION_CD", "SourceSystemCode").agg(F.count(F.lit(1)).alias("OccurrenceCount")))
    rejected = ctx.rejectLookupFailures(misses, "raw.OracleGeography", "ref.Region", "RegionCode", "REGION_CD",
                                        "REGION_CD", "REF_LOOKUP_MISS", "source region code has no conformed region",
                                        sourceSystemCodeColumn="SourceSystemCode", routedToUnknownMember=True,
                                        occurrenceColumn="OccurrenceCount", payloadColumns=["REGION_CD"])
    _log(ctx, "ref.Region", len(grids.REGIONS), _count(spark.table(target)), counts, rejected)


# --------------------------------------------------------------------------- ref.usp_LoadStatusCode / ReasonCode
def loadStatusCode(ctx, statusDomainCode=None):
    ctx.step("Load Status Codes")
    spark = ctx.spark
    target = ctx.table("ref.StatusCode")
    rows = [r for r in grids.STATUS_CODES if statusDomainCode is None or r[0] == statusDomainCode]
    df = gridFrame(spark, grids.STATUS_CODE_COLUMNS, rows).withColumn("IsActive", F.lit(True))
    counts = mergeReference(spark, target, df, ["StatusDomainCode", "ConformedStatusCode"])
    _log(ctx, "ref.StatusCode", len(rows), _count(spark.table(target)), counts, 0)


def loadReasonCode(ctx, reasonDomainCode=None):
    ctx.step("Load Reason Codes")
    spark = ctx.spark
    target = ctx.table("ref.ReasonCode")
    rows = [r for r in grids.REASON_CODES if reasonDomainCode is None or r[0] == reasonDomainCode]
    df = gridFrame(spark, grids.REASON_CODE_COLUMNS, rows).withColumn("IsActive", F.lit(True))
    counts = mergeReference(spark, target, df, ["ReasonDomainCode", "ConformedReasonCode"])
    _log(ctx, "ref.ReasonCode", len(rows), _count(spark.table(target)), counts, 0)


# --------------------------------------------------------------------------- ref.usp_LoadUnitOfMeasure / UomConversion
def loadUnitOfMeasure(ctx):
    ctx.step("Load Unit Of Measure")
    spark = ctx.spark
    target = ctx.table("ref.UnitOfMeasure")
    uom = (gridFrame(spark, grids.UOM_COLUMNS, grids.UNITS_OF_MEASURE)
           .withColumn("IsBaseUom", F.col("UomCode") == F.col("BaseUomCode"))
           .withColumn("IsActive", F.lit(True)))
    counts = mergeReference(spark, target, uom, ["UomCode"])
    products = rawBatch(ctx, "raw.OracleProductMaster")
    misses = (products.select(F.upper(F.trim(F.col("BASE_UOM_CD"))).alias("BASE_UOM_CD"), "SourceSystemCode")
              .where(F.col("BASE_UOM_CD").isNotNull())
              .join(uom.select(F.col("UomCode").alias("BASE_UOM_CD")), "BASE_UOM_CD", "left_anti")
              .join(activeCrosswalk(ctx, "UOM").select(F.col("SourceCodeValue").alias("BASE_UOM_CD")), "BASE_UOM_CD", "left_anti")
              .groupBy("BASE_UOM_CD", "SourceSystemCode").agg(F.count(F.lit(1)).alias("OccurrenceCount")))
    rejected = ctx.rejectLookupFailures(misses, "raw.OracleProductMaster", "ref.UnitOfMeasure", "UomCode", "BASE_UOM_CD",
                                        "BASE_UOM_CD", "REF_LOOKUP_MISS", "product base unit has no conformed unit of measure",
                                        sourceSystemCodeColumn="SourceSystemCode", routedToUnknownMember=True,
                                        occurrenceColumn="OccurrenceCount", payloadColumns=["BASE_UOM_CD"])
    _log(ctx, "ref.UnitOfMeasure", len(grids.UNITS_OF_MEASURE), _count(spark.table(target)), counts, rejected)


def loadUomConversion(ctx, maintainedByName="REF_Load_TransactionType"):
    ctx.step("Load UOM Conversions")
    spark = ctx.spark
    target = ctx.table("ref.UomConversion")
    uomCodes = spark.table(ctx.table("ref.UnitOfMeasure")).select("UomCode")
    standard = (gridFrame(spark, grids.UOM_CONVERSION_COLUMNS, grids.UOM_CONVERSIONS)
                .withColumn("StockItemBusinessKey", F.lit("*"))
                .withColumn("IsItemSpecific", F.lit(False))
                .withColumn("EffectiveFromDate", F.lit(DEFAULT_EFFECTIVE_FROM).cast("date"))
                .withColumn("MaintainedByName", F.lit(maintainedByName))
                .withColumn("MaintenanceNote", F.lit("standard factor (ref.usp_LoadUomConversion)")))
    products = rawBatch(ctx, "raw.OracleProductMaster")
    item = (products
            .select(F.upper(F.trim(F.col("SELL_UOM_CD"))).alias("FromUomCode"),
                    F.upper(F.trim(F.col("BASE_UOM_CD"))).alias("ToUomCode"),
                    F.concat(F.lit("ORA_ERP|"), F.upper(F.trim(F.col("PRODUCT_CD")))).alias("StockItemBusinessKey"),
                    safeDecimal("UOM_CONVERSION_FACTOR", 18, 8).alias("ConversionFactor"),
                    F.col("UOM_CONVERSION_FACTOR").cast("string").alias("RawFactor"),
                    F.col("PRODUCT_CD"), F.col("SourceSystemCode"))
            .where(F.col("FromUomCode").isNotNull() & F.col("ToUomCode").isNotNull() & (F.col("FromUomCode") != F.col("ToUomCode"))))
    validItem = (item.where(F.col("ConversionFactor") > 0)
                 .join(uomCodes.withColumnRenamed("UomCode", "FromUomCode"), "FromUomCode", "left_semi")
                 .join(uomCodes.withColumnRenamed("UomCode", "ToUomCode"), "ToUomCode", "left_semi")
                 .dropDuplicates(["FromUomCode", "ToUomCode", "StockItemBusinessKey"])
                 .withColumn("IsItemSpecific", F.lit(True))
                 .withColumn("EffectiveFromDate", F.lit(ctx.businessDate or datetime.date.today()).cast("date"))
                 .withColumn("MaintainedByName", F.lit(maintainedByName))
                 .withColumn("MaintenanceNote", F.lit("item factor from raw.OracleProductMaster"))
                 .select(*standard.columns))
    badFactor = item.where(F.col("ConversionFactor").isNull() | (F.col("ConversionFactor") <= 0))
    rejected = ctx.rejectConstraintViolations(
        badFactor, "ref.UomConversion", "PRODUCT_CD", "ConversionFactor", "RawFactor", "REF_CONVERSION_FACTOR_INVALID",
        "item conversion factor is missing, non-numeric or not positive", constraintName="CK_UomConversion_Factor",
        constraintTypeCode="CHECK", payloadColumns=["PRODUCT_CD", "FromUomCode", "ToUomCode", "RawFactor"])
    source = standard.unionByName(validItem)
    sourceCount = source.count()
    preserve = "t.MaintainedByName = %s AND t.IsItemSpecific = false" % ("'%s'" % maintainedByName)
    counts = mergeReference(spark, target, source, ["FromUomCode", "ToUomCode", "StockItemBusinessKey"],
                            updateCols=["ConversionFactor", "IsItemSpecific", "MaintenanceNote"],
                            notMatchedBySourceSql=preserve)
    _log(ctx, "ref.UomConversion", sourceCount, _count(spark.table(target)), counts, rejected)


# --------------------------------------------------------------------------- ref.usp_LoadCurrency
def loadCurrency(ctx, reportingCurrencyCode=REPORTING_CURRENCY_CODE):
    ctx.step("Load Currency Reference")
    spark = ctx.spark
    target = ctx.table("ref.Currency")
    raw = rawBatch(ctx, "raw.OracleCurrency")
    src = (raw.select(F.upper(F.trim(F.col("CURRENCY_CD"))).alias("CurrencyCode"),
                      cleanString("CURRENCY_NAME").alias("CurrencyName"),
                      cleanString("CURRENCY_SYMBOL").alias("CurrencySymbol"),
                      F.expr("try_cast(MINOR_UNIT_DIGITS AS INT)").alias("MinorUnitDigits"),
                      flag("ACTIVE_FLG").alias("IsActive"), flag("EURO_LEGACY_FLG").alias("IsEuroLegacy"),
                      safeDecimal("LEGACY_FIXED_RATE").alias("EuroFixedRate"),
                      safeDate("LAST_UPDATE_DT").alias("LastUpdateDate"),
                      F.col("SourceSystemCode"), F.col("CURRENCY_CD").alias("RawCode"), F.col("SourceRowNumber")))
    valid = (src.where(F.col("CurrencyCode").rlike("^[A-Z]{3}$") & F.col("CurrencyName").isNotNull())
             .withColumn("_rank", F.row_number().over(Window.partitionBy("CurrencyCode").orderBy(F.col("LastUpdateDate").desc_nulls_last(), F.col("SourceRowNumber").desc_nulls_last())))
             .where(F.col("_rank") == 1).drop("_rank"))
    invalid = src.where(~(F.col("CurrencyCode").rlike("^[A-Z]{3}$")) | F.col("CurrencyCode").isNull() | F.col("CurrencyName").isNull())
    rejected = ctx.rejectConstraintViolations(
        invalid, "ref.Currency", "RawCode", "CurrencyCode", "RawCode", "REF_CURRENCY_CODE_INVALID",
        "currency code is not a three letter ISO code or has no name", constraintName="CK_Currency_Code",
        constraintTypeCode="CHECK", payloadColumns=["RawCode", "CurrencyName"])
    shaped = (valid
              .withColumn("MinorUnitDigits", F.coalesce(F.col("MinorUnitDigits"), F.lit(2)))
              .withColumn("RoundingRuleCode", F.when(F.col("MinorUnitDigits") == 0, "UNIT").otherwise("HALF_EVEN"))
              .withColumn("IsReportingCurrency", F.col("CurrencyCode") == reportingCurrencyCode)
              .withColumn("IsEuroLegacy", F.coalesce(F.col("IsEuroLegacy"), F.lit(False)))
              .withColumn("RetiredDate", F.when(F.col("IsEuroLegacy") | ~F.col("IsActive"), F.col("LastUpdateDate")))
              .withColumn("IsActive", F.col("IsActive") & ~F.col("IsEuroLegacy"))
              .select("CurrencyCode", "CurrencyName", "CurrencySymbol", "MinorUnitDigits", "RoundingRuleCode",
                      "IsReportingCurrency", "IsEuroLegacy", "EuroFixedRate", "RetiredDate", "IsActive"))
    sourceCount = src.count()
    counts = mergeReference(spark, target, shaped, ["CurrencyCode"])
    _log(ctx, "ref.Currency", sourceCount, _count(spark.table(target)), counts, rejected)


# --------------------------------------------------------------------------- ref.usp_LoadFxRateDaily
def loadFxRateDaily(ctx, maxFillForwardDays=MAX_FILL_FORWARD_DAYS):
    ctx.step("Load Daily FX Rates")
    spark = ctx.spark
    target = ctx.table("ref.FxRateDaily")
    raw = rawBatch(ctx, "raw.OracleFxRate")
    currencies = spark.table(ctx.table("ref.Currency")).select("CurrencyCode")
    src = (raw.select(F.upper(F.trim(F.col("FROM_CURRENCY_CD"))).alias("FromCurrencyCode"),
                      F.upper(F.trim(F.col("TO_CURRENCY_CD"))).alias("ToCurrencyCode"),
                      safeDate("RATE_DT").alias("RateDate"),
                      F.coalesce(F.upper(F.trim(F.col("RATE_TYPE_CD"))), F.lit("CORPORATE")).alias("RateTypeCode"),
                      safeDecimal("CONVERSION_RATE").alias("ConversionRate"),
                      cleanString("RATE_SOURCE_CD").alias("RateSourceCode"),
                      F.col("CONVERSION_RATE").cast("string").alias("RawRate"),
                      F.col("RATE_DT").cast("string").alias("RawDate"),
                      F.concat_ws("|", F.col("FROM_CURRENCY_CD"), F.col("TO_CURRENCY_CD"), F.col("RATE_DT")).alias("RawKey"),
                      F.col("SourceSystemCode")))
    okRate = F.col("ConversionRate").isNotNull() & (F.col("ConversionRate") > 0) & F.col("RateDate").isNotNull()
    invalid = src.where(~okRate | F.col("ConversionRate").isNull() | F.col("RateDate").isNull())
    rejectedInvalid = ctx.rejectConstraintViolations(
        invalid, "ref.FxRateDaily", "RawKey", "ConversionRate", "RawRate", "REF_FX_RATE_INVALID",
        "rate is not a positive number or the rate date does not parse", constraintName="CK_FxRateDaily_Rate",
        constraintTypeCode="CHECK", payloadColumns=["RawKey", "RawRate", "RawDate"])
    candidate = src.where(okRate)
    known = (candidate
             .join(currencies.withColumnRenamed("CurrencyCode", "FromCurrencyCode"), "FromCurrencyCode", "left_semi")
             .join(currencies.withColumnRenamed("CurrencyCode", "ToCurrencyCode"), "ToCurrencyCode", "left_semi"))
    unknown = candidate.join(known.select("RawKey"), "RawKey", "left_anti")
    rejectedUnknown = ctx.rejectLookupFailures(
        unknown, "raw.OracleFxRate", "ref.Currency", "CurrencyCode", "FromCurrencyCode", "RawKey", "REF_LOOKUP_MISS",
        "FX rate refers to a currency that is not in ref.Currency", sourceSystemCodeColumn="SourceSystemCode",
        payloadColumns=["FromCurrencyCode", "ToCurrencyCode", "RawDate"])
    observed = (known.withColumn("_rank", F.row_number().over(
        Window.partitionBy("FromCurrencyCode", "ToCurrencyCode", "RateDate", "RateTypeCode").orderBy(F.col("RateSourceCode").desc_nulls_last())))
        .where(F.col("_rank") == 1).drop("_rank")
        .select("FromCurrencyCode", "ToCurrencyCode", "RateDate", "RateTypeCode", "ConversionRate", "RateSourceCode"))
    # fill forward: missing calendar days after an observed rate carry the last observed rate for at most
    # @MaxFillForwardDays days (never over an observed day)
    bounds = observed.groupBy("FromCurrencyCode", "ToCurrencyCode", "RateTypeCode").agg(F.min("RateDate").alias("_min"), F.max("RateDate").alias("_max"))
    calendar = bounds.select("FromCurrencyCode", "ToCurrencyCode", "RateTypeCode",
                             F.explode(F.sequence(F.col("_min"), F.col("_max"), F.expr("INTERVAL 1 DAY"))).alias("RateDate"))
    w = Window.partitionBy("FromCurrencyCode", "ToCurrencyCode", "RateTypeCode").orderBy("RateDate")
    filled = (calendar.join(observed, ["FromCurrencyCode", "ToCurrencyCode", "RateTypeCode", "RateDate"], "left")
              .withColumn("_lastRate", F.last("ConversionRate", ignorenulls=True).over(w))
              .withColumn("_lastDate", F.last(F.when(F.col("ConversionRate").isNotNull(), F.col("RateDate")), ignorenulls=True).over(w))
              .withColumn("_gap", F.datediff(F.col("RateDate"), F.col("_lastDate")))
              .where(F.col("ConversionRate").isNotNull() | (F.col("_gap") <= maxFillForwardDays))
              .withColumn("RateSourceCode", F.when(F.col("ConversionRate").isNull(), F.lit("FILL_FORWARD")).otherwise(F.col("RateSourceCode")))
              .withColumn("ConversionRate", F.coalesce(F.col("ConversionRate"), F.col("_lastRate")))
              .drop("_lastRate", "_lastDate", "_gap")
              .withColumn("IsTreasuryOverride", F.lit(False))
              .withColumn("EffectiveFromUtc", F.col("RateDate").cast("timestamp"))
              .withColumn("EffectiveToUtc", F.lit(None).cast("timestamp"))
              .withColumn("LoadedFromBatchId", F.lit(ctx.batchId).cast("bigint")))
    sourceCount = src.count()
    # treasury overrides are steward rows: never replaced by the feed
    counts = mergeReference(spark, target, filled, ["FromCurrencyCode", "ToCurrencyCode", "RateDate", "RateTypeCode"],
                            updateCols=["ConversionRate", "RateSourceCode", "LoadedFromBatchId"])
    _log(ctx, "ref.FxRateDaily", sourceCount, _count(spark.table(target)), counts, rejectedInvalid + rejectedUnknown)


# --------------------------------------------------------------------------- ref.usp_LoadCountry
def loadCountry(ctx):
    ctx.step("Load Countries")
    spark = ctx.spark
    target = ctx.table("ref.Country")
    geo = rawBatch(ctx, "raw.OracleGeography")
    regions = spark.table(ctx.table("ref.Region")).select("RegionCode")
    regionMap = activeCrosswalk(ctx, "REGION").select(F.col("SourceCodeValue").alias("REGION_CD"), F.col("ConformedCodeValue").alias("MappedRegion"))
    eu = gridFrame(spark, grids.EU_MEMBERSHIP_COLUMNS, grids.EU_MEMBERSHIP)
    src = (geo.select(F.upper(F.trim(F.col("COUNTRY_CD"))).alias("CountryCode"),
                      F.upper(F.trim(F.col("ISO3_CD"))).alias("CountryCodeIso3"),
                      cleanString("COUNTRY_NAME").alias("CountryName"),
                      F.upper(F.trim(F.col("REGION_CD"))).alias("REGION_CD"),
                      cleanString("SUB_REGION_NAME").alias("SubRegionName"),
                      F.upper(F.trim(F.col("CURRENCY_CD"))).alias("LocalCurrencyCode"),
                      cleanString("POSTAL_FORMAT_MASK").alias("PostalFormatMask"),
                      cleanString("STATE_PROVINCE_CD").alias("STATE_PROVINCE_CD"),
                      safeDate("LAST_UPDATE_DT").alias("LastUpdateDate"), F.col("SourceSystemCode"), F.col("SourceRowNumber"))
           .where(F.col("CountryCode").rlike("^[A-Z]{2}$")))
    perCountry = (src.groupBy("CountryCode")
                  .agg(F.max_by("CountryCodeIso3", "LastUpdateDate").alias("CountryCodeIso3"),
                       F.max_by("CountryName", "LastUpdateDate").alias("CountryName"),
                       F.max_by("REGION_CD", "LastUpdateDate").alias("REGION_CD"),
                       F.max_by("SubRegionName", "LastUpdateDate").alias("SubRegionName"),
                       F.max_by("LocalCurrencyCode", "LastUpdateDate").alias("LocalCurrencyCode"),
                       F.max_by("PostalFormatMask", "LastUpdateDate").alias("PostalFormatMask"),
                       F.max(F.col("STATE_PROVINCE_CD").isNotNull().cast("int")).alias("HasStateProvince"),
                       F.max("SourceSystemCode").alias("SourceSystemCode"))
                  .join(regionMap, "REGION_CD", "left")
                  .withColumn("RegionCode", F.coalesce(F.col("MappedRegion"), F.col("REGION_CD"))))
    resolved = perCountry.join(regions, "RegionCode", "left_semi")
    misses = perCountry.join(regions, "RegionCode", "left_anti")
    rejected = ctx.rejectLookupFailures(
        misses, "raw.OracleGeography", "ref.Region", "RegionCode", "REGION_CD", "CountryCode", "REF_LOOKUP_MISS",
        "country region code has no conformed region", sourceSystemCodeColumn="SourceSystemCode",
        routedToUnknownMember=True, payloadColumns=["CountryCode", "CountryName", "REGION_CD"])
    shaped = (resolved.join(eu, "CountryCode", "left")
              .withColumn("EuAccessionDate", F.col("EuAccessionDate").cast("date"))
              .withColumn("EuExitDate", F.col("EuExitDate").cast("date"))
              .withColumn("IsEuMemberState", F.col("EuAccessionDate").isNotNull() & F.col("EuExitDate").isNull())
              .withColumn("PostalCodeRequiredFlag", F.col("PostalFormatMask").isNotNull())
              .withColumn("StateProvinceRequiredFlag", F.col("HasStateProvince") == 1)
              .withColumn("AddressLineOrderCode", F.when(F.col("RegionCode") == "NA", "STREET_CITY_STATE_POSTAL")
                          .when(F.col("RegionCode") == "APAC", "STREET_CITY_POSTAL_STATE").otherwise("STREET_POSTAL_CITY"))
              .withColumn("VatRegistrationMask", F.when(F.col("IsEuMemberState") | F.col("EuExitDate").isNotNull(),
                                                        F.concat(F.col("CountryCode"), F.lit("999999999"))))
              .withColumn("IsActive", F.lit(True))
              .select("CountryCode", "CountryCodeIso3", "CountryName", "RegionCode", "SubRegionName", "LocalCurrencyCode",
                      "PostalFormatMask", "PostalCodeRequiredFlag", "StateProvinceRequiredFlag", "AddressLineOrderCode",
                      "VatRegistrationMask", "IsEuMemberState", "EuAccessionDate", "EuExitDate", "IsActive"))
    sourceCount = src.count()
    counts = mergeReference(spark, target, shaped, ["CountryCode"])
    _log(ctx, "ref.Country", sourceCount, _count(spark.table(target)), counts, rejected)


# --------------------------------------------------------------------------- ref.usp_LoadTaxJurisdiction
def loadTaxJurisdiction(ctx):
    ctx.step("Load Tax Jurisdictions")
    spark = ctx.spark
    target = ctx.table("ref.TaxJurisdiction")
    raw = rawBatch(ctx, "raw.OracleTaxRate")
    countries = spark.table(ctx.table("ref.Country")).select("CountryCode", "RegionCode")
    regions = spark.table(ctx.table("ref.Region")).select("RegionCode", "TaxRegimeCode")
    src = (raw.select(F.upper(F.trim(F.col("TAX_JURISDICTION_CD"))).alias("TaxJurisdictionCode"),
                      cleanString("TAX_CD").alias("TaxCode"),
                      F.upper(F.trim(F.col("COUNTRY_CD"))).alias("CountryCode"),
                      cleanString("STATE_PROVINCE_CD").alias("StateProvinceCode"),
                      safeDecimal("RATE_PCT", 9, 4).alias("RatePercent"),
                      F.col("RATE_PCT").cast("string").alias("RawRate"),
                      safeDecimal("RECOVERABLE_PCT", 9, 4).alias("RecoverablePercent"),
                      flag("REVERSE_CHARGE_FLG").alias("ReverseChargeFlag"),
                      F.coalesce(safeDate("EFFECTIVE_FROM_DT"), F.lit(DEFAULT_EFFECTIVE_FROM).cast("date")).alias("EffectiveFromDate"),
                      safeDate("EFFECTIVE_TO_DT").alias("EffectiveToDate"),
                      F.col("TAX_RATE_ID").cast("string").alias("RawKey"), F.col("SourceSystemCode"))
           .where(F.col("TaxJurisdictionCode").isNotNull()))
    okRate = F.col("RatePercent").isNotNull() & (F.col("RatePercent") >= 0) & (F.col("RatePercent") <= 100)
    invalid = src.where(~okRate | F.col("RatePercent").isNull())
    rejectedInvalid = ctx.rejectConstraintViolations(
        invalid, "ref.TaxJurisdiction", "RawKey", "CombinedRatePercent", "RawRate", "REF_TAX_RATE_INVALID",
        "tax rate is not a percentage between 0 and 100", constraintName="CK_TaxJurisdiction_Rate",
        constraintTypeCode="CHECK", payloadColumns=["RawKey", "TaxJurisdictionCode", "CountryCode", "RawRate"])
    candidate = src.where(okRate)
    known = candidate.join(countries, "CountryCode", "inner").join(regions, "RegionCode", "left")
    unknown = candidate.join(countries, "CountryCode", "left_anti")
    rejectedUnknown = ctx.rejectLookupFailures(
        unknown, "raw.OracleTaxRate", "ref.Country", "CountryCode", "CountryCode", "RawKey", "REF_LOOKUP_MISS",
        "tax rate country is not in ref.Country", sourceSystemCodeColumn="SourceSystemCode",
        payloadColumns=["RawKey", "TaxJurisdictionCode", "CountryCode"])
    isNa = F.col("RegionCode") == "NA"
    isEu = F.col("TaxRegimeCode") == "VAT"
    isApac = F.col("TaxRegimeCode") == "GST"
    shaped = (known
              .withColumn("_rank", F.row_number().over(Window.partitionBy("TaxJurisdictionCode", "EffectiveFromDate").orderBy(F.col("RatePercent").desc())))
              .where(F.col("_rank") == 1).drop("_rank")
              .select(
                  "TaxJurisdictionCode",
                  F.coalesce(F.col("TaxCode"), F.col("TaxJurisdictionCode")).alias("TaxJurisdictionName"),
                  "TaxRegimeCode", "CountryCode", "StateProvinceCode",
                  F.lit(None).cast("string").alias("CountyOrDistrictName"), F.lit(None).cast("string").alias("CityName"),
                  F.lit(None).cast("string").alias("PostalCodeLow"), F.lit(None).cast("string").alias("PostalCodeHigh"),
                  F.col("RatePercent").alias("CombinedRatePercent"),
                  F.when(isNa & F.col("StateProvinceCode").isNotNull(), F.col("RatePercent")).otherwise(F.lit(0)).cast("decimal(9,4)").alias("StateRatePercent"),
                  F.lit(0).cast("decimal(9,4)").alias("CountyRatePercent"), F.lit(0).cast("decimal(9,4)").alias("CityRatePercent"),
                  F.lit(0).cast("decimal(9,4)").alias("SpecialDistrictRatePercent"),
                  (isEu & F.col("ReverseChargeFlag")).alias("ReverseChargeEligible"),
                  (isEu | isApac).alias("RegistrationRequiredFlag"),
                  "EffectiveFromDate", "EffectiveToDate"))
    sourceCount = src.count()
    # effective dating: a new (code, EffectiveFromDate) version is inserted, the previous open version of the
    # same code is closed the day before; existing versions are never rewritten.
    existing = spark.table(target).select("TaxJurisdictionCode", F.col("EffectiveFromDate").alias("_existingFrom"), "EffectiveToDate")
    newVersions = shaped.join(existing.select("TaxJurisdictionCode", F.col("_existingFrom").alias("EffectiveFromDate")),
                              ["TaxJurisdictionCode", "EffectiveFromDate"], "left_anti")
    counts = mergeReference(spark, target, newVersions, ["TaxJurisdictionCode", "EffectiveFromDate"], insertOnly=True)
    closeSql = """
        MERGE INTO %s AS t
        USING (SELECT TaxJurisdictionCode, EffectiveFromDate AS NewFrom FROM %s) AS s
        ON t.TaxJurisdictionCode = s.TaxJurisdictionCode AND t.EffectiveFromDate < s.NewFrom
           AND (t.EffectiveToDate IS NULL OR t.EffectiveToDate >= s.NewFrom)
        WHEN MATCHED THEN UPDATE SET t.EffectiveToDate = date_sub(s.NewFrom, 1)
    """
    newVersions.createOrReplaceTempView("wwi_ref_tax_new")
    closed = spark.sql(closeSql % (target, "wwi_ref_tax_new"))
    closedRows = closed.first()["num_updated_rows"] if closed.columns else None
    ctx.addCounts("ref.TaxJurisdiction", sourceRowCount=sourceCount, targetRowCount=_count(spark.table(target)),
                  insertRowCount=counts.inserted, updateRowCount=closedRows, rejectRowCount=rejectedInvalid + rejectedUnknown)


# --------------------------------------------------------------------------- ref.usp_LoadPostalFormatRule
def loadPostalFormatRule(ctx):
    ctx.step("Load Postal Format Rules")
    spark = ctx.spark
    target = ctx.table("ref.PostalFormatRule")
    countries = spark.table(ctx.table("ref.Country")).select("CountryCode", "RegionCode", "PostalFormatMask")
    regions = spark.table(ctx.table("ref.Region")).select("RegionCode", "AddressRuleSetCode")
    grid = gridFrame(spark, grids.POSTAL_RULE_COLUMNS, grids.POSTAL_RULES)
    fallback = (countries.join(grid.select("CountryCode").distinct(), "CountryCode", "left_anti")
                .join(regions, "RegionCode", "left")
                .select("CountryCode", F.coalesce(F.col("AddressRuleSetCode"), F.lit("GENERIC")).alias("RuleSetCode"),
                        F.lit(900).alias("RulePriority"), F.lit(" -").alias("StripCharacters"), F.lit(True).alias("UpperCaseFlag"),
                        F.col("PostalFormatMask").alias("FormatMask"), F.lit(None).cast("int").alias("MinimumLength"),
                        F.lit(None).cast("int").alias("MaximumLength"), F.lit(None).cast("int").alias("TruncateToLength"),
                        F.lit(None).cast("int").alias("InsertSeparatorAt"), F.lit(None).cast("string").alias("SeparatorCharacter"),
                        F.lit("region fallback rule (ref.usp_LoadPostalFormatRule)").alias("RuleNote")))
    source = grid.select(*fallback.columns).unionByName(fallback)
    existing = spark.table(target).select("CountryCode", "RuleSetCode", "RulePriority", "RuleId")
    keyed = source.join(existing, ["CountryCode", "RuleSetCode", "RulePriority"], "left")
    keyed = withSequence(spark, target, keyed, "RuleId", ["CountryCode", "RuleSetCode", "RulePriority"])
    geo = rawBatch(ctx, "raw.OracleGeography")
    misses = (geo.select(F.upper(F.trim(F.col("COUNTRY_CD"))).alias("COUNTRY_CD"), "SourceSystemCode")
              .where(F.col("COUNTRY_CD").isNotNull())
              .join(countries.select(F.col("CountryCode").alias("COUNTRY_CD")), "COUNTRY_CD", "left_anti")
              .groupBy("COUNTRY_CD", "SourceSystemCode").agg(F.count(F.lit(1)).alias("OccurrenceCount")))
    rejected = ctx.rejectLookupFailures(misses, "raw.OracleGeography", "ref.Country", "CountryCode", "COUNTRY_CD", "COUNTRY_CD",
                                        "REF_LOOKUP_MISS", "postal rule country is not in ref.Country",
                                        sourceSystemCodeColumn="SourceSystemCode", occurrenceColumn="OccurrenceCount",
                                        payloadColumns=["COUNTRY_CD"])
    sourceCount = source.count()
    counts = mergeReference(spark, target, keyed, ["CountryCode", "RuleSetCode", "RulePriority"],
                            updateCols=["StripCharacters", "UpperCaseFlag", "FormatMask", "MinimumLength", "MaximumLength",
                                        "TruncateToLength", "InsertSeparatorAt", "SeparatorCharacter", "RuleNote"])
    _log(ctx, "ref.PostalFormatRule", sourceCount, _count(spark.table(target)), counts, rejected)


# --------------------------------------------------------------------------- ref.usp_LoadCodeCrosswalk
def loadCodeCrosswalk(ctx, codeDomainCode=None, effectiveFromDate=None, maintainedByName="REF_Load_CodeTranslation"):
    ctx.step("Load Code Crosswalk %s" % (codeDomainCode or "- All Domains"))
    spark = ctx.spark
    target = ctx.table("ref.CodeCrosswalk")
    effectiveFrom = effectiveFromDate or ctx.businessDate or datetime.date.today()
    rows = [r for r in grids.CODE_CROSSWALK if codeDomainCode is None or r[0] == codeDomainCode]
    grid = (gridFrame(spark, grids.CODE_CROSSWALK_COLUMNS, rows)
            .withColumn("IsDefaultForConformed", F.coalesce(F.col("IsDefaultForConformed").cast("boolean"), F.lit(False))))
    keys = ["CodeDomainCode", "SourceSystemCode", "SourceCodeValue"]
    scope = spark.table(target)
    if codeDomainCode:
        scope = scope.where(F.col("CodeDomainCode") == codeDomainCode)
    openRows = scope.where(F.col("EffectiveToDate").isNull())
    stewardOpen = openRows.where(F.col("MaintainedByName") != maintainedByName)
    ownedOpen = openRows.where((F.col("MaintainedByName") == maintainedByName) | F.col("MaintainedByName").isNull())
    # rows a steward has taken over are left alone
    candidate = grid.join(stewardOpen.select(*keys), keys, "left_anti")
    cmp = candidate.join(ownedOpen.select(*keys, F.col("ConformedCodeValue").alias("_conformed"),
                                          F.col("RegionCode").alias("_region"),
                                          F.col("IsDefaultForConformed").alias("_default"),
                                          F.col("CrosswalkId").alias("_id")), keys, "left")
    unchanged = (F.col("_conformed") == F.col("ConformedCodeValue")) & F.col("_region").eqNullSafe(F.col("RegionCode")) \
        & (F.col("_default") == F.col("IsDefaultForConformed"))
    toInsert = cmp.where(F.col("_id").isNull() | ~unchanged)
    toClose = cmp.where(F.col("_id").isNotNull() & ~unchanged).select(F.col("_id").alias("CrosswalkId"))
    vanished = ownedOpen.join(grid.select(*keys), keys, "left_anti").select("CrosswalkId")
    closeIds = toClose.unionByName(vanished)
    closeIds.createOrReplaceTempView("wwi_ref_xw_close")
    closed = spark.sql("""
        MERGE INTO %s AS t USING wwi_ref_xw_close AS s ON t.CrosswalkId = s.CrosswalkId
        WHEN MATCHED AND t.EffectiveToDate IS NULL THEN UPDATE SET
            t.EffectiveToDate = date_sub(CAST('%s' AS DATE), 1),
            t.MaintenanceNote = concat_ws(' ', t.MaintenanceNote, 'closed by %s')
    """ % (target, effectiveFrom, maintainedByName))
    closedRows = closed.first()["num_updated_rows"] if closed.columns else None
    inserts = (toInsert.select(*grid.columns)
               .withColumn("EffectiveFromDate", F.lit(str(effectiveFrom)).cast("date"))
               .withColumn("EffectiveToDate", F.lit(None).cast("date"))
               .withColumn("MaintainedByName", F.lit(maintainedByName))
               .withColumn("MaintenanceNote", F.lit("steward grid (ref.usp_LoadCodeCrosswalk)"))
               .withColumn("CrosswalkId", F.lit(None).cast("bigint")))
    inserts = withSequence(spark, target, inserts, "CrosswalkId", keys)
    # a same-day re-run finds the version already opened today: update in place instead of a duplicate
    counts = mergeReference(spark, target, inserts, keys + ["EffectiveFromDate"],
                            updateCols=["SourceCodeDescription", "ConformedCodeValue", "RegionCode", "IsDefaultForConformed",
                                        "EffectiveToDate", "MaintenanceNote"])
    ctx.addCounts("ref.CodeCrosswalk" + ("" if codeDomainCode is None else "." + codeDomainCode),
                  sourceRowCount=len(rows), targetRowCount=_count(spark.table(target).where(F.col("EffectiveToDate").isNull())),
                  insertRowCount=counts.inserted, updateRowCount=closedRows, rejectRowCount=0)


# --------------------------------------------------------------------------- ref.usp_LoadSourceKeyCrosswalk
def loadSourceKeyCrosswalk(ctx, maintainedByName="REF_Load_UnknownMembers"):
    ctx.step("Load Source Key Crosswalk")
    spark = ctx.spark
    target = ctx.table("ref.SourceKeyCrosswalk")
    products = rawBatch(ctx, "raw.OracleProductMaster")
    productCode = F.upper(F.trim(F.col("PRODUCT_CD")))
    erp = (products.where(productCode.isNotNull())
           .select(F.lit("PRODUCT").alias("EntityName"), F.lit("ORA_ERP").alias("SourceSystemCode"),
                   productCode.alias("SourceKeyValue"), productCode.alias("ConformedBusinessKey"),
                   F.lit("ERP_MASTER").alias("MatchMethodCode")))
    oltp = (products.where(productCode.isNotNull() & F.col("WWI_STOCK_ITEM_ID").isNotNull())
            .select(F.lit("PRODUCT").alias("EntityName"), F.lit("WWI_OLTP").alias("SourceSystemCode"),
                    F.trim(F.col("WWI_STOCK_ITEM_ID").cast("string")).alias("SourceKeyValue"),
                    productCode.alias("ConformedBusinessKey"), F.lit("ERP_LINK").alias("MatchMethodCode")))
    source = (erp.unionByName(oltp).dropDuplicates(["EntityName", "SourceSystemCode", "SourceKeyValue"])
              .withColumn("SupersededByBusinessKey", F.lit(None).cast("string"))
              .withColumn("IsActive", F.lit(True))
              .withColumn("CreatedAtUtc", F.current_timestamp())
              .withColumn("MaintainedByName", F.lit(maintainedByName))
              .withColumn("CrosswalkId", F.lit(None).cast("bigint")))
    keys = ["EntityName", "SourceSystemCode", "SourceKeyValue"]
    sourceCount = source.count()
    unseen = source.join(spark.table(target).select(*keys), keys, "left_anti")
    unseen = withSequence(spark, target, unseen, "CrosswalkId", keys)
    counts = mergeReference(spark, target, unseen, keys, insertOnly=True)
    deactivated = None
    if sourceCount > 0:
        source.select(*keys).createOrReplaceTempView("wwi_ref_skx_seen")
        result = spark.sql("""
            MERGE INTO %s AS t USING wwi_ref_skx_seen AS s
            ON t.EntityName = s.EntityName AND t.SourceSystemCode = s.SourceSystemCode AND t.SourceKeyValue = s.SourceKeyValue
            WHEN NOT MATCHED BY SOURCE AND t.EntityName = 'PRODUCT' AND t.IsActive = true AND t.MaintainedByName = '%s'
                THEN UPDATE SET t.IsActive = false
        """ % (target, maintainedByName))
        deactivated = result.first()["num_updated_rows"] if result.columns else None
    ctx.addCounts("ref.SourceKeyCrosswalk", sourceRowCount=sourceCount, targetRowCount=counts.inserted,
                  insertRowCount=counts.inserted, updateRowCount=deactivated, rejectRowCount=0)


# --------------------------------------------------------------------------- ref.usp_ReportUnmappedCodes
def reportUnmappedCodes(ctx, codeDomainCode=None, logRejects=True):
    """Sweep the raw extracts of one (or every) domain for source codes without an active mapping and
    register each distinct code once per batch in err.RejectedLookupFailure (+ etl.rejected_record),
    payload {"DOMAIN": ..., "CODE": ...} exactly as ref.vw_UnmappedSourceCode expects."""
    ctx.step("Report Unmapped Codes - %s" % (codeDomainCode or "All Domains"))
    spark = ctx.spark
    active = activeCrosswalk(ctx).select("CodeDomainCode", "SourceSystemCode", F.upper(F.trim(F.col("SourceCodeValue"))).alias("CODE"))
    observedTotal = 0
    unmappedTotal = 0
    for domain, system, legacyTable, column in grids.UNMAPPED_CODE_SOURCES:
        if codeDomainCode and domain != codeDomainCode:
            continue
        raw = readOptional(ctx, legacyTable, [column, "BatchId", "SourceSystemCode"])
        if column not in raw.columns:
            ctx.logWarning("%s has no column %s; domain %s not scanned" % (legacyTable, column, domain), errorCode="SOURCE_COLUMN_MISSING")
            continue
        observed = (raw.select(F.upper(F.trim(F.col(column).cast("string"))).alias("CODE"))
                    .where(F.col("CODE").isNotNull() & (F.col("CODE") != ""))
                    .groupBy("CODE").agg(F.count(F.lit(1)).alias("OccurrenceCount")))
        unmapped = (observed.join(active.where((F.col("CodeDomainCode") == domain) & (F.col("SourceSystemCode") == system)).select("CODE"),
                                  "CODE", "left_anti")
                    .withColumn("DOMAIN", F.lit(domain)).withColumn("SYSTEM", F.lit(system)))
        observedCount = observed.count()
        observedTotal += observedCount
        if logRejects:
            rejected = ctx.rejectLookupFailures(
                unmapped, legacyTable, "ref.CodeCrosswalk", column, "CODE", "CODE", "REF_UNMAPPED_CODE",
                "source code has no active ref.CodeCrosswalk mapping", sourceSystemCodeColumn="SYSTEM",
                routedToUnknownMember=True, occurrenceColumn="OccurrenceCount", payloadColumns=["DOMAIN", "CODE"])
        else:
            rejected = unmapped.count()
        unmappedTotal += rejected
        ctx.addCounts("%s.%s" % (legacyTable, column), sourceRowCount=observedCount, targetRowCount=rejected, rejectRowCount=rejected)
    return observedTotal, unmappedTotal


def unmappedSourceCodes(ctx):
    """Port of ref.vw_UnmappedSourceCode over silver.err_rejected_lookup_failure."""
    spark = ctx.spark
    domain = F.get_json_object("RecordPayload", "$.DOMAIN")
    # the domain only travels in the payload written by ref.usp_ReportUnmappedCodes ({"DOMAIN":..,"CODE":..});
    # rejects without it are not part of the view
    rejects = (spark.table(ctx.table("err.RejectedLookupFailure"))
               .where((F.col("RejectReasonCode") == "REF_UNMAPPED_CODE") & domain.isNotNull()
                      & F.get_json_object("RecordPayload", "$.CODE").isNotNull())
               .select(domain.alias("CodeDomainCode"), F.col("SourceSystemCode"), F.col("LookupValue").alias("SourceCodeValue"),
                       "OccurrenceCount", "RejectedAtUtc", "BatchId"))
    active = activeCrosswalk(ctx).select("CodeDomainCode", "SourceSystemCode", F.upper(F.trim(F.col("SourceCodeValue"))).alias("SourceCodeValue"))
    return (rejects.join(active, ["CodeDomainCode", "SourceSystemCode", "SourceCodeValue"], "left_anti")
            .groupBy("CodeDomainCode", "SourceSystemCode", "SourceCodeValue")
            .agg(F.count(F.lit(1)).alias("ObservationCount"), F.sum("OccurrenceCount").cast("bigint").alias("TotalOccurrenceCount"),
                 F.min("RejectedAtUtc").alias("FirstObservedAtUtc"), F.max("RejectedAtUtc").alias("LastObservedAtUtc"),
                 F.max("BatchId").alias("LatestBatchId")))


# --------------------------------------------------------------------------- REF_Load_WarehouseSite source
def warehouseSiteMovements(ctx):
    """stg.StockMovement joined to the warehouse site attributes (legacy: Warehouse.StockItemTransactions
    joined to the WWI site table). silver.stg_stock_movement is owned by the staging bundle; when it does
    not carry site attributes they are derived from WarehouseCode and the fallback is logged as a warning."""
    df = readOptional(ctx, "stg.StockMovement")
    cols = set(df.columns)
    if "WarehouseSiteId" not in cols:
        if "WarehouseCode" not in cols:
            ctx.logWarning("stg.StockMovement has neither WarehouseSiteId nor WarehouseCode; no sites derived",
                           errorCode="SOURCE_COLUMN_MISSING")
            return emptyFrame(ctx.spark, ["WarehouseSiteId", "WarehouseSiteCode", "WarehouseSiteName", "CountryCode", "PostalCode"])
        ctx.logWarning("stg.StockMovement has no WarehouseSiteId; sites derived from WarehouseCode", errorCode="SOURCE_COLUMN_FALLBACK")
        code = F.upper(F.trim(F.col("WarehouseCode").cast("string")))
        df = (df.where(code.isNotNull())
              .withColumn("WarehouseSiteCode", code)
              .withColumn("WarehouseSiteId", (F.abs(F.xxhash64(code)) % F.lit(2147483647)).cast("int")))
    if "WarehouseSiteCode" not in df.columns:
        df = df.withColumn("WarehouseSiteCode", F.col("WarehouseSiteId").cast("string"))
    if "WarehouseSiteName" not in df.columns:
        df = df.withColumn("WarehouseSiteName", F.col("WarehouseSiteCode"))
    if "CountryCode" not in df.columns:
        default = None
        try:
            from dbx_etl_common import control
            default = control.getConfiguration(ctx.spark, ctx.catalog, "WarehouseSite.DefaultCountryCode", ctx.environmentCode)
        except Exception:
            default = None
        df = df.withColumn("CountryCode", F.lit(default).cast("string"))
    if "PostalCode" not in df.columns:
        df = df.withColumn("PostalCode", F.lit(None).cast("string"))
    return df.select("WarehouseSiteId", "WarehouseSiteCode", "WarehouseSiteName", "CountryCode", "PostalCode")
