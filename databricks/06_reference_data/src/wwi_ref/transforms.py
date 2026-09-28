"""Pure DataFrame transformations: one function per SSIS Data Flow in build_reference_packages.py.

Every Derived Column / Conditional Split / Lookup expression from the generator is ported here as a
``withColumn`` / filter / join so the semantics can be unit-tested with local PySpark without any
Delta table or control framework. Functions take DataFrames and return DataFrames.
"""
from pyspark.sql import functions as F

from wwi_ref.delta_io import changeHash

# SSIS ISNULL(x) ? "" : x  ->  coalesce(x, '')
def _nz(col):
    return F.coalesce(F.col(col).cast("string"), F.lit(""))


def _upperTrim(col):
    return F.upper(F.trim(F.col(col)))


# --------------------------------------------------------------------------- conformed_code_query
def conformedCodeQuery(crosswalkDf, codeDomain, codeAlias, nameAlias):
    """Port of build_reference_packages.conformed_code_query: one row per conformed value and region
    out of the active (EffectiveToDate IS NULL) crosswalk rows of a domain, description taken from the
    mapping flagged IsDefaultForConformed."""
    return (crosswalkDf
            .where((F.col("CodeDomainCode") == codeDomain) & F.col("EffectiveToDate").isNull())
            .groupBy(F.col("ConformedCodeValue").alias(codeAlias),
                     F.coalesce(F.col("RegionCode"), F.lit("ALL")).alias("RegionCode"))
            .agg(F.max(F.when(F.col("IsDefaultForConformed"), F.col("SourceCodeDescription"))).alias(nameAlias),
                 F.min("EffectiveFromDate").alias("EffectiveFromDate"),
                 F.count(F.lit(1)).alias("SourceCodeCount"))
            .withColumn(nameAlias, F.coalesce(F.col(nameAlias), F.col(codeAlias))))


# --------------------------------------------------------------------------- REF_Load_Currency
def currencyDimension(currencyDf, fxDf, asOfDate=None):
    """REF Currency Conformed + Shape Currency Dimension + Screen Currency."""
    latest = (fxDf.where((F.col("ToCurrencyCode") == "USD") & (F.col("RateTypeCode") == "CORPORATE"))
              .groupBy(F.col("FromCurrencyCode").alias("CurrencyCode"))
              .agg(F.max_by("ConversionRate", "RateDate").alias("LatestRateToUsd"),
                   F.max("RateDate").alias("LatestRateDate")))
    today = F.lit(asOfDate).cast("date") if asOfDate is not None else F.current_date()
    df = (currencyDf.where(F.col("IsActive") == True)  # noqa: E712
          .select("CurrencyCode", "CurrencyName", "CurrencySymbol", "MinorUnitDigits", "RoundingRuleCode",
                  "IsReportingCurrency", "IsEuroLegacy", "RetiredDate")
          .join(latest, "CurrencyCode", "left")
          .withColumn("CurrencyCode", _upperTrim("CurrencyCode"))
          .withColumn("CurrencyName", F.trim("CurrencyName"))
          .withColumn("CurrencyStatusCode", F.when(F.col("IsEuroLegacy") == True, "LEGACY")  # noqa: E712
                      .when(F.col("RetiredDate").isNull(), "ACTIVE").otherwise("RETIRED"))
          .withColumn("RateStalenessDays", F.when(F.col("LatestRateDate").isNull(), F.lit(9999))
                      .otherwise(F.datediff(today, F.col("LatestRateDate"))).cast("int"))
          .withColumn("RateStatusCode", F.when(F.col("RateStalenessDays") <= 30, "RATED")
                      .when(F.col("RateStalenessDays") < 9999, "STALE").otherwise("UNRATED"))
          .withColumn("ChangeHash", changeHash(["CurrencyCode", "CurrencyName", "MinorUnitDigits"])))
    return df


# --------------------------------------------------------------------------- REF_Load_PaymentMethod
def paymentMethodDimension(crosswalkDf):
    df = conformedCodeQuery(crosswalkDf, "PAYMENT_METHOD", "PaymentMethodCode", "PaymentMethodName")
    return (df
            .withColumn("PaymentMethodCode", _upperTrim("PaymentMethodCode"))
            .withColumn("SettlementTypeCode", F.when(F.col("RegionCode") == "EU", "SEPA")
                        .when(F.col("RegionCode") == "APAC", "RTGS").otherwise("ACH"))
            .withColumn("ElectronicFlag", F.when(F.col("PaymentMethodCode").isin("CHQ", "CASH"), "N").otherwise("Y"))
            .withColumn("ChangeHash", changeHash(["PaymentMethodCode", "PaymentMethodName", "RegionCode"])))


def screenPaymentMethod(df):
    ok = (F.col("PaymentMethodCode") != "") & (F.col("PaymentMethodCode") != "UNKNOWN")
    return df.where(ok), df.where(~ok)


# --------------------------------------------------------------------------- REF_Load_TransactionType
def transactionTypeDimension(crosswalkDf):
    df = conformedCodeQuery(crosswalkDf, "TRANSACTION_TYPE", "TransactionTypeCode", "TransactionTypeName")
    code = F.col("TransactionTypeCode")
    return (df
            .withColumn("TransactionTypeCode", _upperTrim("TransactionTypeCode"))
            .withColumn("MovementDirectionCode", F.when(code.isin("PURCH", "RET"), "IN").otherwise("OUT"))
            .withColumn("MovementSign", F.when(code.isin("PURCH", "RET"), 1).otherwise(-1).cast("int"))
            .withColumn("AffectsLedgerFlag", F.when(code.isin("SALE", "PURCH"), "Y").otherwise("N"))
            .withColumn("ReversalAllowedFlag", F.when(code.isin("ADJ", "RET"), "Y").otherwise("N"))
            .withColumn("ChangeHash", changeHash(["TransactionTypeCode", "TransactionTypeName", "RegionCode"])))


def screenTransactionType(df):
    ok = (F.col("SourceCodeCount") > 0) & (F.col("TransactionTypeCode") != "UNKNOWN")
    return df.where(ok), df.where(~ok)


def unitOfMeasureDimension(uomDf, conversionDf):
    std = conversionDf.where(F.col("StockItemBusinessKey") == "*").select(
        F.col("FromUomCode").alias("UomCode"), F.col("ToUomCode").alias("BaseUomCode"), "ConversionFactor")
    return (uomDf.where(F.col("IsActive") == True)  # noqa: E712
            .select("UomCode", "UomName", "UomClassCode", "BaseUomCode", "DecimalPrecision")
            .join(std, ["UomCode", "BaseUomCode"], "left")
            .withColumn("ConversionFactor", F.coalesce(F.col("ConversionFactor"), F.lit(1).cast("decimal(18,8)")))
            .withColumn("UomCode", _upperTrim("UomCode"))
            .withColumn("BaseFactorText", F.col("ConversionFactor").cast("string"))
            .withColumn("ChangeHash", changeHash(["UomCode", "UomName", "BaseUomCode", "ConversionFactor"])))


# --------------------------------------------------------------------------- REF_Load_PaymentTerms
def paymentTermsDimension(crosswalkDf):
    df = conformedCodeQuery(crosswalkDf, "PAYMENT_TERMS", "PaymentTermsCode", "PaymentTermsName")
    code = F.upper(F.trim(F.col("PaymentTermsCode")))
    suffix = F.regexp_extract(code, r"(\d+)$", 1)
    netDays = (F.when(code == "EOM", 30).when(code == "DISC210", 30)
               .when(suffix != "", suffix.cast("int")).otherwise(F.lit(None).cast("int")))
    df = (df.withColumn("PaymentTermsCode", code)
          .withColumn("NetDays", netDays.cast("int"))
          .withColumn("DiscountDays", F.when(code == "DISC210", 10).otherwise(0).cast("int"))
          .withColumn("DiscountPercent", F.when(code == "DISC210", 2).otherwise(0).cast("decimal(9,4)")))
    capped = (F.when((F.col("RegionCode") == "EU") & (F.col("NetDays") > 60), 60)
              .when((F.col("RegionCode") == "APAC") & (F.col("NetDays") > 90), 90)
              .otherwise(F.col("NetDays")))
    return (df.withColumn("NetDaysCapped", capped.cast("int"))
            .withColumn("EarlySettlementFlag", F.when(F.col("DiscountDays") > 0, "Y").otherwise("N"))
            .withColumn("ChangeHash", changeHash(["PaymentTermsCode", "PaymentTermsName", "RegionCode", "NetDaysCapped"])))


def screenPaymentTerms(df):
    ok = (F.col("NetDaysCapped") > 0) & (F.col("NetDaysCapped") <= 365)
    return df.where(ok), df.where(~ok | F.col("NetDaysCapped").isNull())


# --------------------------------------------------------------------------- REF_Load_ReturnReason
def returnReasonDimension(reasonCodeDf, crosswalkDf):
    counts = (crosswalkDf.where((F.col("CodeDomainCode") == "RETURN") & F.col("EffectiveToDate").isNull())
              .groupBy(F.col("ConformedCodeValue").alias("ReturnReasonCode"))
              .agg(F.count(F.lit(1)).alias("SourceCodeCount")))
    df = (reasonCodeDf.where((F.col("ReasonDomainCode") == "RETURN") & (F.col("IsActive") == True))  # noqa: E712
          .select(F.col("ConformedReasonCode").alias("ReturnReasonCode"),
                  F.col("ConformedReasonName").alias("ReturnReasonName"),
                  "ReasonGroupCode", "IsCustomerFault", "IsSupplierFault", "RequiresApproval")
          .join(counts, "ReturnReasonCode", "left")
          .withColumn("SourceCodeCount", F.coalesce(F.col("SourceCodeCount"), F.lit(0)).cast("bigint")))
    group = F.upper(F.coalesce(F.col("ReasonGroupCode"), F.lit("")))
    code = F.upper(F.trim(F.col("ReturnReasonCode")))
    return (df
            .withColumn("ReturnReasonCode", code)
            .withColumn("ReasonCategoryCode", F.when(group == "QUALITY", "QUALITY").when(group == "FULFILMENT", "SERVICE")
                        .when(group == "COMMERCIAL", "CUSTOMER").otherwise("OTHER"))
            .withColumn("SupplierRecoverableFlag", F.when(F.col("IsSupplierFault") == True, "Y").otherwise("N"))  # noqa: E712
            .withColumn("RestockingFeePercentEu", F.lit(0).cast("decimal(9,4)"))
            .withColumn("RestockingFeePercentNa", F.when(code == "NOTNEEDED", 15).otherwise(0).cast("decimal(9,4)"))
            .withColumn("RestockingFeePercentApac", F.when(code == "NOTNEEDED", 15).otherwise(0).cast("decimal(9,4)"))
            .withColumn("ChangeHash", changeHash(["ReturnReasonCode", "ReturnReasonName", "ReasonGroupCode"])))


def screenSourced(df):
    ok = F.col("SourceCodeCount") > 0
    return df.where(ok), df.where(~ok | F.col("SourceCodeCount").isNull())


# --------------------------------------------------------------------------- REF_Load_LoyaltyTier
def loyaltyTierDimension(crosswalkDf):
    df = conformedCodeQuery(crosswalkDf, "LOYALTY_TIER", "LoyaltyTierCode", "TierName")
    code = F.upper(F.trim(F.col("LoyaltyTierCode")))
    rank = F.when(code == "TIER4", 1).when(code == "TIER3", 2).when(code == "TIER2", 3).otherwise(4)
    df = df.withColumn("LoyaltyTierCode", code).withColumn("TierRank", rank.cast("int"))
    discount = (F.when(F.col("RegionCode") == "APAC", F.lit(12) - F.lit(2) * F.col("TierRank"))
                .when(F.col("RegionCode") == "EU", F.least(F.lit(5), F.lit(10) - F.lit(2) * F.col("TierRank")))
                .otherwise(F.lit(10) - F.lit(2) * F.col("TierRank")))
    return (df.withColumn("DiscountPercent", discount.cast("decimal(9,4)"))
            .withColumn("PointsMultiplier", F.when(F.col("RegionCode") == "APAC", 1.5).otherwise(1.0).cast("decimal(18,2)"))
            .withColumn("ChangeHash", changeHash(["LoyaltyTierCode", "TierName", "RegionCode", "TierRank"])))


# --------------------------------------------------------------------------- REF_Load_SalesChannel
def salesChannelDimension(crosswalkDf):
    df = conformedCodeQuery(crosswalkDf, "SALES_CHANNEL", "ChannelCode", "ChannelName")
    code = F.upper(F.trim(F.col("ChannelCode")))
    return (df.withColumn("ChannelCode", code)
            .withColumn("ChannelGroupCode", F.when(code == "ONLINE", "DIGITAL").when(code == "PARTNER", "PARTNER").otherwise("DIRECT"))
            .withColumn("DigitalFlag", F.when(code == "ONLINE", "Y").otherwise("N"))
            .withColumn("ChangeHash", changeHash(["ChannelCode", "ChannelName", "RegionCode"])))


# --------------------------------------------------------------------------- REF_Load_Carrier
def carrierDimension(crosswalkDf):
    df = conformedCodeQuery(crosswalkDf, "CARRIER", "CarrierCode", "CarrierName")
    code = F.upper(F.trim(F.col("CarrierCode")))
    return (df.withColumn("CarrierCode", code)
            .withColumn("CrossBorderFlag", F.when(F.col("RegionCode") == "ALL", "Y").otherwise("N"))
            .withColumn("OnTimeTargetDays", F.when(F.col("RegionCode") == "APAC", 5).when(F.col("RegionCode") == "EU", 3)
                        .otherwise(4).cast("decimal(9,2)"))
            .withColumn("OwnFleetFlag", F.when(code == "OWN", "Y").otherwise("N"))
            .withColumn("ChangeHash", changeHash(["CarrierCode", "CarrierName", "RegionCode"])))


# --------------------------------------------------------------------------- REF_Load_WarehouseSite
def standardizePostalCode(postalCol, truncateCol):
    stripped = F.regexp_replace(F.upper(F.trim(F.coalesce(F.col(postalCol), F.lit("")))), r"[ \-]", "")
    return F.when(F.col(truncateCol) > 0, F.substring(stripped, 1, 20).substr(F.lit(1), F.col(truncateCol))).otherwise(stripped)


def warehouseSiteDimension(siteMovementsDf, countryDf, postalRuleDf, regionDf):
    """REF Warehouse Site Conformed (aggregate over stock movements) + Standardize Site + Lookup Site Region.

    ``siteMovementsDf`` must carry WarehouseSiteId, WarehouseSiteCode, WarehouseSiteName, CountryCode, PostalCode
    (one row per movement). Returns (published, unknownRegion) where unknownRegion is the lookup no-match output.
    """
    country = countryDf.select(F.col("CountryCode").alias("_CountryCode"), F.col("RegionCode").alias("_RegionCode"))
    rule = (postalRuleDf.where(F.col("RulePriority") == 1)
            .select(F.col("CountryCode").alias("_RuleCountry"), "FormatMask", "StripCharacters", "UpperCaseFlag", "TruncateToLength"))
    grouped = (siteMovementsDf
               .withColumn("_CountryKey", F.substring(F.upper(F.coalesce(F.col("CountryCode"), F.lit(""))), 1, 2))
               .join(country, F.col("_CountryKey") == F.col("_CountryCode"), "left")
               .join(rule, F.col("_CountryCode") == F.col("_RuleCountry"), "left")
               .groupBy("WarehouseSiteId", F.col("_CountryCode").alias("CountryCode"), F.col("_RegionCode").alias("RegionCode"))
               .agg(F.max("WarehouseSiteCode").alias("WarehouseSiteCode"),
                    F.max("WarehouseSiteName").alias("WarehouseSiteName"),
                    F.max("PostalCode").alias("PostalCode"),
                    F.max("FormatMask").alias("FormatMask"),
                    F.max("StripCharacters").alias("StripCharacters"),
                    F.max(F.col("UpperCaseFlag").cast("int")).alias("UpperCaseFlag"),
                    F.max(F.col("TruncateToLength").cast("int")).alias("TruncateToLength"),
                    F.count(F.lit(1)).alias("MovementCount")))
    shaped = (grouped
              .withColumn("WarehouseSiteCode", _upperTrim("WarehouseSiteCode"))
              .withColumn("PostalCodeStandardized", standardizePostalCode("PostalCode", "TruncateToLength"))
              .withColumn("SiteTypeCode", F.when(F.col("MovementCount") > 100000, "DC")
                          .when(F.col("MovementCount") > 10000, "REGIONAL").otherwise("SATELLITE"))
              .withColumn("ChangeHash", changeHash(["WarehouseSiteCode", "WarehouseSiteName", "CountryCode", "PostalCode"])))
    region = regionDf.select("RegionCode", "RegionName", "AddressRuleSetCode", "WeightUomCode")
    joined = shaped.join(region, "RegionCode", "left")
    matched = joined.where(F.col("RegionName").isNotNull())
    noMatch = joined.where(F.col("RegionName").isNull())
    return matched, noMatch


def screenWarehouseSite(df):
    ok = F.length(F.col("PostalCodeStandardized")) > 2
    return df.where(ok), df.where(~ok)


# --------------------------------------------------------------------------- REF_Load_CostCenter
def costCenterDimension(crosswalkDf):
    df = conformedCodeQuery(crosswalkDf, "COST_CENTER", "CostCenterCode", "CostCenterName")
    code = F.upper(F.trim(F.col("CostCenterCode")))
    return (df.withColumn("CostCenterCode", code)
            .withColumn("ParentCostCenterCode", F.when(code == "CORP", "ROOT").otherwise("CORP"))
            .withColumn("CompanyCode", F.lit("0001"))
            .withColumn("FunctionCode", F.substring(code, 1, 2))
            .withColumn("SuspenseFlag", F.when(code == "SUSP", "Y").otherwise("N"))
            .withColumn("RowHashType2", changeHash(["CostCenterCode", "CostCenterName", "RegionCode", "ParentCostCenterCode"]))
            .orderBy("ParentCostCenterCode", "CostCenterCode"))


def screenCostCenter(df):
    selfRef = F.col("CostCenterCode") == F.col("ParentCostCenterCode")
    ok = (F.col("SourceCodeCount") > 0) & ~selfRef
    return df.where(ok), df.where(~ok)


# --------------------------------------------------------------------------- REF_Load_Geography
def geographyDimension(countryDf, regionDf, taxDf, currencyDf, asOfDate=None):
    """REF Geography Conformed + Standardize Geography + Lookup Country Currency.
    Returns (matched, currencyNoMatch)."""
    today = F.lit(asOfDate).cast("date") if asOfDate is not None else F.current_date()
    latestTax = (taxDf.where(F.col("StateProvinceCode").isNull()
                             & (F.col("EffectiveToDate").isNull() | (F.col("EffectiveToDate") >= today)))
                 .groupBy("CountryCode")
                 .agg(F.max_by("TaxJurisdictionCode", "EffectiveFromDate").alias("TaxJurisdictionCode"),
                      F.max_by("CombinedRatePercent", "EffectiveFromDate").alias("CombinedRatePercent"),
                      F.max_by("ReverseChargeEligible", "EffectiveFromDate").alias("ReverseChargeEligible")))
    region = regionDf.select("RegionCode", "TaxRegimeCode")
    df = (countryDf.where(F.col("IsActive") == True)  # noqa: E712
          .select("CountryCode", "CountryCodeIso3", "CountryName", "RegionCode", "SubRegionName", "LocalCurrencyCode",
                  "IsEuMemberState", "EuExitDate", "PostalFormatMask")
          .join(region, "RegionCode", "inner")
          .join(latestTax, "CountryCode", "left"))
    regime = F.col("TaxRegimeCode")
    df = (df.withColumn("CountryCode", _upperTrim("CountryCode"))
          .withColumn("GeographyCode", F.concat_ws("|", _upperTrim("CountryCode"), _upperTrim("RegionCode")))
          .withColumn("GeographyName", F.trim("CountryName"))
          .withColumn("TaxStructureCode", F.when(regime == "VAT", "EU_VAT").when(regime == "GST", "APAC_GST").otherwise("NA_SALESTAX"))
          .withColumn("ReverseChargeFlag", F.when((regime == "VAT") & (F.col("ReverseChargeEligible") == True), "Y").otherwise("N"))  # noqa: E712
          .withColumn("EuStatusCode", F.when(F.col("IsEuMemberState") == True, "MEMBER")  # noqa: E712
                      .when(F.col("EuExitDate").isNull(), "NONMEMBER").otherwise("EXITED"))
          .withColumn("ChangeHash", changeHash(["CountryCode", "CountryName", "RegionCode", "TaxJurisdictionCode"])))
    currency = currencyDf.select(F.col("CurrencyCode").alias("LocalCurrencyCode"), "CurrencyName", "MinorUnitDigits")
    joined = df.join(currency, "LocalCurrencyCode", "left")
    return joined.where(F.col("CurrencyName").isNotNull()), joined.where(F.col("CurrencyName").isNull())


def screenGeography(df):
    ok = F.length(F.trim(F.coalesce(F.col("TaxJurisdictionCode"), F.lit("")))) > 0
    return df.where(ok), df.where(~ok)


# --------------------------------------------------------------------------- REF_Load_UnknownMembers
def unknownMemberRows(statusDf, reasonDf):
    status = statusDf.where(F.col("ConformedStatusCode") == "UNKNOWN").select(
        F.lit("ref.StatusCode").alias("ReferenceTableName"), F.col("ConformedStatusCode").alias("UnknownCodeValue"),
        F.col("ConformedStatusName").alias("UnknownDescription"), F.col("StatusDomainCode").alias("DomainCode"))
    reason = reasonDf.where(F.col("ConformedReasonCode") == "UNKNOWN").select(
        F.lit("ref.ReasonCode").alias("ReferenceTableName"), F.col("ConformedReasonCode").alias("UnknownCodeValue"),
        F.col("ConformedReasonName").alias("UnknownDescription"), F.col("ReasonDomainCode").alias("DomainCode"))
    return (status.unionByName(reason)
            .withColumn("UnknownMemberKey", F.lit(-1).cast("int"))
            .withColumn("NotApplicableKey", F.lit(-2).cast("int"))
            .withColumn("IsUnknownMemberFlag", F.lit("Y"))
            .withColumn("ChangeHash", changeHash(["ReferenceTableName", "DomainCode", "UnknownCodeValue", "UnknownDescription"])))


def screenUnknownMember(df):
    ok = F.length(F.trim(F.coalesce(F.col("DomainCode"), F.lit("")))) > 0
    return df.where(ok), df.where(~ok)


# --------------------------------------------------------------------------- REF_Load_CodeTranslation
def codeSetConfiguration(crosswalkDf):
    """Shape Configuration Row + Summarize Mapping Coverage -> one etl.configuration row per domain/system."""
    return (crosswalkDf.where(F.col("EffectiveToDate").isNull())
            .withColumn("ConfigurationKey", F.concat(F.lit("CodeSetVersion."), _upperTrim("CodeDomainCode")))
            .withColumn("ConfigurationValue", F.concat_ws("|", _upperTrim("SourceSystemCode"), _upperTrim("ConformedCodeValue")))
            .groupBy("CodeDomainCode", "SourceSystemCode", "ConfigurationKey")
            .agg(F.count("SourceCodeValue").alias("MappingCount"),
                 F.max("ConfigurationValue").alias("ConfigurationValue")))


def tagUnmappedCodes(unmappedDf):
    return (unmappedDf
            .withColumn("CoverageStatusCode", F.lit("UNMAPPED"))
            .withColumn("SeverityCode", F.when(F.col("TotalOccurrenceCount") > 1000, "HIGH").otherwise("LOW"))
            .withColumn("ReviewedFlag", F.lit("N")))


def screenUnmappedCode(df):
    ok = F.length(F.trim(F.coalesce(F.col("SourceCodeValue"), F.lit("")))) > 0
    return df.where(ok), df.where(~ok)
