"""Pure DataFrame ports of the STG_Load_* Data Flow transformations.

Every function takes DataFrames in and hands DataFrames back, so the notebooks
stay thin (read bronze -> transform -> write silver / route rejects) and the
expressions can be unit tested with local Spark. Derived Column expressions are
translated one-to-one from ssis/04_staging/build_staging_packages.py; the
regional divergences documented in sqlserver/staging/procedures are folded in
where the package calls the procedure after the Data Flow.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from stg_common import expressions as X

FAR_FUTURE = "9999-12-31"
EPOCH = "1900-01-01"


def _up(col):
    return F.upper(F.trim(F.col(col)))


def _upDefault(col, default):
    """UPPER(TRIM(ISNULL(col) ? default : col))"""
    return F.upper(F.trim(F.coalesce(F.col(col), F.lit(default))))


def _dec(col, precision, scale, default=None):
    c = F.col(col) if isinstance(col, str) else col
    if default is not None:
        c = F.coalesce(c, F.lit(default))
    return c.cast("decimal(%d,%d)" % (precision, scale))


def _regionCode(col="REGION_CD", default="NA"):
    return _upDefault(col, default)


def _dateOrFarFuture(col):
    return F.coalesce(F.col(col).cast("date"), F.lit(FAR_FUTURE).cast("date"))


# ----------------------------------------------------------------------------- master data


def cleanseCostCenter(df):
    return df.withColumns(
        {
            "CostCenterCode": F.upper(F.regexp_replace(F.trim(F.col("CC_CODE")), " ", "")),
            "CostCenterName": F.trim(F.col("CC_NAME")),
            "ParentCostCenterCode": F.when(X.isBlank("PARENT_CC_CODE"), F.lit("ROOT")).otherwise(_up("PARENT_CC_CODE")),
            "CompanyCode": _upDefault("COMPANY_CD", "1000"),
            "RegionCode": _regionCode(),
            "FunctionCode": _upDefault("FUNCTION_CD", "GEN"),
            "IsActiveFlag": _upDefault("ACTIVE_FLG", "Y"),
        }
    )


def deriveCostCenterHierarchy(df):
    """Parent lookup output (ParentCostCenterName) must already be joined."""
    orphan = F.col("ParentCostCenterName").isNull()
    return df.withColumns(
        {
            "HierarchyPath": F.concat(
                F.col("CompanyCode"),
                F.lit("/"),
                F.when(orphan, F.lit("ORPHAN")).otherwise(F.col("ParentCostCenterCode")),
                F.lit("/"),
                F.col("CostCenterCode"),
            ),
            "HierarchyLevel": F.when(F.col("ParentCostCenterCode") == "ROOT", F.lit(1)).otherwise(F.when(orphan, F.lit(3)).otherwise(F.lit(2))),
            "ChangeHash": X.changeHash("CostCenterName", "ParentCostCenterCode", "FunctionCode", "IsActiveFlag"),
        }
    )


def splitCostCenter(df):
    """(active, closed, orphan) per 'Apply Cost Center Rules'."""
    active = df.where(F.col("IsActiveFlag") == "Y")
    closed = df.where((F.col("IsActiveFlag") == "N") & (F.col("HierarchyLevel") > 1))
    orphan = df.where(~((F.col("IsActiveFlag") == "Y") | ((F.col("IsActiveFlag") == "N") & (F.col("HierarchyLevel") > 1))))
    return active, closed, orphan


def cleanseCurrency(df):
    return df.withColumns(
        {
            "CurrencyCode": _up("CCY_CODE"),
            "CurrencyName": F.trim(F.col("CCY_NAME")),
            "MinorUnits": F.coalesce(F.col("MINOR_UNITS"), F.lit(2)).cast("int"),
            "IsActiveFlag": _upDefault("ACTIVE_FLG", "Y"),
        }
    )


def normaliseFxRate(df):
    rate = F.col("RATE").cast("decimal(18,8)")
    return df.withColumns(
        {
            "FromCurrencyCode": _up("FROM_CCY"),
            "ToCurrencyCode": _up("TO_CCY"),
            "RateTypeCode": _upDefault("RATE_TYPE_CD", "SPOT"),
            "EffectiveFromDate": F.col("EFF_FROM_DT").cast("date"),
            "EffectiveToDate": _dateOrFarFuture("EFF_TO_DT"),
            "ExchangeRate": rate,
            "InverseRate": F.when(rate == 0, F.lit(0)).otherwise(F.lit(1) / rate).cast("decimal(18,8)"),
            "RateSourceCode": _upDefault("SRC_SYSTEM_CD", "ORA_ERP"),
        }
    )


def dedupeFxByPairAndDate(df):
    """Sort with duplicate removal on (From, To, EffectiveFrom); the file override
    (RateSourceCode <> ORA_ERP, unioned after the Oracle rows) wins."""
    w = Window.partitionBy("FromCurrencyCode", "ToCurrencyCode", "EffectiveFromDate").orderBy(
        F.when(F.col("RateSourceCode") == "ORA_ERP", F.lit(1)).otherwise(F.lit(0)), F.col("EffectiveToDate").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def triangulateThroughUsd(df, usdCross):
    """usdCross: (ToCurrencyCode, UsdCrossRate) — current SPOT rates to USD."""
    joined = df.join(usdCross.dropDuplicates(["ToCurrencyCode"]), "ToCurrencyCode", "left")
    return joined.withColumn(
        "UsdEquivalentRate",
        F.when(F.col("UsdCrossRate").isNull(), F.col("ExchangeRate")).otherwise(F.col("ExchangeRate") * F.col("UsdCrossRate")).cast("decimal(18,8)"),
    )


def cleanseCustomer(df):
    regionRaw = F.upper(F.trim(F.col("REGION_CD")))
    consent = F.col("CONSENT_FLAG")
    return df.withColumns(
        {
            "CustomerCode": _up("CUST_CODE"),
            "CustomerName": F.trim(F.regexp_replace(F.col("CUST_NAME"), "  ", " ")),
            "TradingName": F.when(F.trim(F.col("CUST_NAME")) == F.trim(F.coalesce(F.col("TRADING_NAME"), F.lit(""))), F.lit(None).cast("string")).otherwise(F.trim(F.col("TRADING_NAME"))),
            "CustomerClassCode": F.when(X.isBlank("CUST_CLASS_CD"), F.lit("UNCL")).otherwise(_up("CUST_CLASS_CD")),
            "CreditStatusCode": _upDefault("CREDIT_STATUS_CD", "NEW"),
            "CountryCode": _up("COUNTRY_CD"),
            "RegionCode": _regionCode(),
            "TaxRegistrationNumber": F.upper(F.regexp_replace(F.regexp_replace(F.trim(F.col("TAX_REG_NBR")), " ", ""), "-", "")),
            "MarketingConsentFlag": F.when(
                regionRaw == "EU",
                F.when(F.upper(F.trim(F.coalesce(consent, F.lit("N")))) == "Y", F.lit("Y")).otherwise(F.lit("N")),
            ).otherwise(F.when(F.upper(F.trim(F.coalesce(consent, F.lit("Y")))) == "N", F.lit("N")).otherwise(F.lit("Y"))),
            "RetentionMonths": F.when(regionRaw == "EU", F.lit(24)).when(regionRaw == "APAC", F.lit(60)).otherwise(F.lit(84)),
        }
    )


LEGAL_SUFFIXES = ("PTY LTD", "PTE LTD", "INC", "LLC", "LTD", "GMBH", "SARL", "BV", "KK")


def customerNameStandardized(nameCol):
    """usp_NormalizeCustomer: upper case, punctuation removed, trailing legal suffix stripped."""
    name = F.upper(F.trim(F.regexp_replace(F.coalesce(F.col(nameCol), F.lit("")), "[^A-Za-z0-9 ]", "")))
    name = F.regexp_replace(name, " +", " ")
    pattern = " (%s)$" % "|".join(LEGAL_SUFFIXES)
    return F.trim(F.regexp_replace(name, pattern, ""))


def deriveCustomerHash(df):
    """After the Country lookup (DefaultCurrencyCode in scope); adds the
    usp_NormalizeCustomer outputs (CustomerNameStandardized, RetentionExpiryDate, RowHash)."""
    out = df.withColumns(
        {
            "CreditLimitAmount": _dec("CREDIT_LIMIT_AMT", 18, 2, 0),
            "CreditCurrencyCode": F.when(F.col("CREDIT_CCY").isNull(), F.col("DefaultCurrencyCode")).otherwise(_up("CREDIT_CCY")),
            "SourceCreatedDate": F.coalesce(F.col("CREATED_DT").cast("timestamp"), F.lit(EPOCH).cast("timestamp")),
            "SourceModifiedDate": F.coalesce(F.col("LAST_UPD_DT"), F.col("CREATED_DT")).cast("timestamp"),
        }
    )
    out = out.withColumns(
        {
            "ChangeHash": X.changeHash("CustomerName", "CustomerClassCode", "CreditStatusCode", "CountryCode", "TaxRegistrationNumber", "CreditLimitAmount"),
            "CustomerNameStandardized": customerNameStandardized("CustomerName"),
            "RetentionExpiryDate": F.add_months(F.col("SourceModifiedDate").cast("date"), F.col("RetentionMonths")),
        }
    )
    return out.withColumn(
        "RowHash",
        X.rowHash("CustomerCode", "CustomerName", "TradingName", "CustomerClassCode", "CreditStatusCode", "CountryCode", "RegionCode", "TaxRegistrationNumber", "MarketingConsentFlag", "CreditLimitAmount", "CreditCurrencyCode"),
    )


def splitCustomer(df):
    """(valid, missingName, malformedCode) per 'Apply Customer Business Rules'."""
    missing = F.col("CustomerName").isNull() | (F.trim(F.col("CustomerName")) == "")
    valid = ~missing & (F.length(F.col("CustomerCode")) >= 4)
    return df.where(valid), df.where(missing), df.where(~missing & ~F.coalesce(valid, F.lit(False)))


def standardizeAddress(df, regionCode):
    """The three region-specific 'Standardize <region> Address' Derived Columns."""
    base = {
        "CustomerCode": _up("CUST_CODE"),
        "AddressTypeCode": _upDefault("ADDR_TYPE_CD", "MAIN"),
        "EffectiveFromDate": F.coalesce(F.col("EFF_FROM_DT").cast("timestamp"), F.lit(EPOCH).cast("timestamp")),
        "RegionCode": F.lit(regionCode),
    }
    if regionCode == "NA":
        base.update(
            {
                "AddressLine1": F.upper(F.regexp_replace(F.regexp_replace(F.regexp_replace(F.trim(F.col("ADDR_LINE_1")), "Street", "ST"), "Avenue", "AVE"), "Suite", "STE")),
                "AddressLine2": F.upper(F.trim(F.coalesce(F.col("ADDR_LINE_2"), F.lit("")))),
                "CityName": _up("CITY_NAME"),
                "StateProvinceCode": F.upper(F.substring(F.trim(F.col("STATE_PROV_CD")), 1, 2)),
                "PostalCode": F.substring(F.regexp_replace(F.trim(F.col("POSTAL_CD")), "-", ""), 1, 5),
                "PostalStandard": F.lit("USPS"),
                "CountryCode": _upDefault("COUNTRY_CD", "USA"),
            }
        )
    elif regionCode == "EU":
        base.update(
            {
                "AddressLine1": F.trim(F.col("ADDR_LINE_1")),
                "AddressLine2": F.trim(F.coalesce(F.col("ADDR_LINE_2"), F.lit(""))),
                "CityName": F.trim(F.col("CITY_NAME")),
                "StateProvinceCode": F.lit(""),
                "PostalCode": F.concat(_up("COUNTRY_CD"), F.lit("-"), F.upper(F.regexp_replace(F.trim(F.col("POSTAL_CD")), " ", ""))),
                "PostalStandard": F.lit("UPU"),
                "CountryCode": _up("COUNTRY_CD"),
            }
        )
    else:
        base.update(
            {
                "AddressLine1": F.trim(F.col("ADDR_LINE_1")),
                "AddressLine2": F.trim(F.coalesce(F.col("ADDR_LINE_2"), F.lit(""))),
                "CityName": _up("CITY_NAME"),
                "StateProvinceCode": _upDefault("STATE_PROV_CD", "XX"),
                "PostalCode": F.regexp_replace(F.regexp_replace(F.trim(F.col("POSTAL_CD")), "-", ""), " ", ""),
                "PostalStandard": F.lit("APAC-LOCAL"),
                "CountryCode": _up("COUNTRY_CD"),
            }
        )
    return df.withColumns(base)


def postalCodeValid(regionCode):
    pc = F.col("PostalCode")
    if regionCode == "NA":
        return (F.length(pc) == 5) & pc.rlike("^[0-9]+$")
    if regionCode == "EU":
        return (F.length(pc) >= 6) & (F.instr(pc, "-") > 0)
    return (F.length(pc) >= 3) & pc.rlike("^[0-9]+$")


def splitEmployeeNames(df):
    full = F.trim(F.col("FullName"))
    space = F.instr(full, " ")
    region = _upDefault("RegionCode", "NA")
    return df.withColumns(
        {
            "EmployeeId": F.col("SalespersonPersonID").cast("int"),
            "FullName": full,
            "GivenName": F.when(space > 0, F.substring(full, 1, 1000).substr(F.lit(1), space - 1)).otherwise(full),
            "FamilyName": F.when(space > 0, full.substr(space + 1, F.lit(50))).otherwise(F.lit("")),
            "PreferredName": F.coalesce(F.trim(F.col("PreferredName")), full),
            "RegionCode": region,
            "EmailAddress": F.when(region == "EU", F.lit("masked@example.invalid")).otherwise(F.lower(F.trim(F.coalesce(F.col("EmailAddress"), F.lit(""))))),
            "PhoneNumberLast4": F.when(F.col("PhoneNumber").isNull(), F.lit("0000")).otherwise(F.substring(F.regexp_replace(F.regexp_replace(F.trim(F.col("PhoneNumber")), "-", ""), " ", ""), -4, 4)),
            "IsCurrentFlag": F.when(F.col("ValidTo").isNull() | (F.col("ValidTo") > F.current_timestamp()), F.lit("Y")).otherwise(F.lit("N")),
        }
    )


def survivorshipDedupe(df, keys, orderCols):
    """Sort transform with 'remove duplicates': first row per key in sort order."""
    w = Window.partitionBy(*keys).orderBy(*orderCols)
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def prepareQuota(df):
    return df.withColumns(
        {
            "EmployeeId": F.col("SalespersonPersonID").cast("int"),
            "SalesTerritoryCode": _upDefault("SalesTerritoryCode", "UNASSIGNED"),
            "QuotaCurrencyCode": _upDefault("QuotaCurrencyCode", "USD"),
            "QuotaAmount": _dec("QuotaAmount", 18, 2, 0),
        }
    )


def convertQuota(df):
    return df.withColumn("QuotaAmountUsd", (F.col("QuotaAmount") * F.coalesce(F.col("AverageRate"), F.lit(1))).cast("decimal(18,2)"))


def cleanseGeography(df):
    return df.withColumns(
        {
            "GeographyCode": _up("GEO_CODE"),
            "CityName": F.trim(F.regexp_replace(F.col("CITY_NAME"), "  ", " ")),
            "StateProvinceCode": _upDefault("STATE_PROV_CD", ""),
            "StateProvinceName": F.trim(F.coalesce(F.col("STATE_PROV_NAME"), F.lit(""))),
            "CountryCode": _up("COUNTRY_CD"),
            "RegionCode": _regionCode(),
            "Latitude": _dec("LATITUDE", 9, 6, 0),
            "Longitude": _dec("LONGITUDE", 9, 6, 0),
            "PopulationCount": F.coalesce(F.col("POPULATION"), F.lit(0)).cast("long"),
            "SalesTerritoryCode": _upDefault("SALES_TERR_CD", "UNASSIGNED"),
            "CityStateKey": F.concat(_up("CITY_NAME"), F.lit("|"), _upDefault("STATE_PROV_CD", "")),
        }
    ).withColumn("ChangeHash", X.changeHash("CityName", "StateProvinceCode", "CountryCode", "SalesTerritoryCode", "PopulationCount"))


def coordinatesPlausible():
    return F.col("Latitude").between(-90, 90) & F.col("Longitude").between(-180, 180)


def cleanseProduct(df):
    return df.withColumns(
        {
            "ProductCode": _up("PROD_CODE"),
            "ProductDescription": F.trim(F.regexp_replace(F.regexp_replace(F.col("PROD_DESC"), "\t", " "), "  ", " ")),
            "ProductFamilyCode": _upDefault("PROD_FAMILY_CD", "UNCLASS"),
            "BaseUomCode": _upDefault("BASE_UOM_CD", "EA"),
            "PackQuantity": F.when(F.col("PACK_QTY").isNull() | (F.col("PACK_QTY") <= 0), F.lit(1)).otherwise(F.col("PACK_QTY")).cast("decimal(18,4)"),
            "HazardousFlag": F.when(_upDefault("HAZMAT_FLG", "N") == "Y", F.lit("Y")).otherwise(F.lit("N")),
            "DiscontinuedFlag": _upDefault("DISCONTINUED_FLG", "N"),
            "WeightUomCode": _upDefault("WEIGHT_UOM_CD", "KG"),
        }
    )


def convertProductUnits(df):
    """After the UoM lookups (ConversionFactor, WeightFactorKg in scope)."""
    return df.withColumns(
        {
            "EachesPerPack": F.when(F.col("ConversionFactor").isNull(), F.col("PackQuantity")).otherwise(F.col("PackQuantity") * F.col("ConversionFactor")).cast("decimal(18,4)"),
            "NetWeightKg": F.when(F.col("NET_WEIGHT").isNull(), F.lit(0)).otherwise(F.col("NET_WEIGHT") * F.coalesce(F.col("WeightFactorKg"), F.lit(1))).cast("decimal(18,4)"),
            "ListPriceAmount": _dec("LIST_PRICE_AMT", 18, 2, 0),
            "ListPriceCurrencyCode": _upDefault("LIST_PRICE_CCY", "USD"),
        }
    ).withColumn("ChangeHash", X.changeHash("ProductDescription", "ProductFamilyCode", "BaseUomCode", "ListPriceAmount", "DiscontinuedFlag"))


def splitProduct(df):
    """(sellable, discontinued, priceless)."""
    sellable = (F.col("DiscontinuedFlag") == "N") & (F.col("ListPriceAmount") > 0)
    disc = F.col("DiscontinuedFlag") == "Y"
    return df.where(sellable), df.where(~sellable & disc), df.where(~sellable & ~disc)


def typePromotions(df):
    pct = F.col("DiscountPercent")
    return df.withColumns(
        {
            "PromotionCode": _up("PromotionCode"),
            "PromotionName": F.trim(F.col("PromotionName")),
            "PromotionTypeCode": F.when(pct.isNull() | (pct == 0), F.lit("AMOUNT")).otherwise(F.lit("PERCENT")),
            "RegionCode": _upDefault("RegionCode", "NA"),
            "DiscountPercent": _dec("DiscountPercent", 9, 4, 0),
            "DiscountAmount": _dec("DiscountAmount", 18, 2, 0),
            "ValidToDate": F.coalesce(F.col("ValidTo").cast("timestamp"), F.lit(FAR_FUTURE).cast("timestamp")),
        }
    )


def standardizeTerritory(df):
    region = _upDefault("RegionCode", "NA")
    return df.withColumns(
        {
            "SalesTerritoryCode": _up("SalesTerritoryCode"),
            "SalesTerritoryName": F.trim(F.col("SalesTerritoryName")),
            "RegionCode": region,
            "CountryCode": _upDefault("CountryCode", "USA"),
            "ParentTerritoryCode": F.when(F.col("ParentTerritoryCode").isNull(), region).otherwise(_up("ParentTerritoryCode")),
        }
    )


def cleanseStockItem(df):
    price = F.col("UnitPrice")
    return df.withColumns(
        {
            "StockItemId": F.col("StockItemID").cast("int"),
            "StockItemName": F.trim(F.col("StockItemName")),
            "BrandName": F.when(F.col("Brand").isNull(), F.lit("UNBRANDED")).otherwise(_up("Brand")),
            "SizeText": F.when(F.col("Size").isNull(), F.lit("N/A")).otherwise(_up("Size")),
            "Barcode": F.regexp_replace(F.trim(F.col("Barcode")), " ", ""),
            "SupplierId": F.coalesce(F.col("SupplierID"), F.lit(-1)).cast("int"),
            "UnitPriceAmount": _dec("UnitPrice", 18, 2, 0),
            "TypicalWeightGrams": F.when(F.col("TypicalWeightPerUnit").isNull(), F.lit(0)).otherwise(F.col("TypicalWeightPerUnit") * 1000).cast("decimal(18,4)"),
            "PriceBandCode": F.when(price.isNull(), F.lit("UNK")).when(price < 10, F.lit("LOW")).when(price < 100, F.lit("MID")).otherwise(F.lit("HGH")),
            "ChillerFlag": F.when(F.col("IsChillerStock").cast("boolean"), F.lit("Y")).otherwise(F.lit("N")),
            "MarketingText": F.substring(F.trim(F.col("MarketingComments")), 1, 400),
        }
    ).withColumn("ChangeHash", X.changeHash("StockItemName", "BrandName", "UnitPriceAmount", "PriceBandCode", "ChillerFlag"))


def splitStockItem(df):
    """(valid, missingName, negativePrice)."""
    missing = F.length(F.col("StockItemName")) <= 2
    valid = ~F.coalesce(missing, F.lit(True)) & (F.col("UnitPriceAmount") >= 0)
    return df.where(valid), df.where(F.coalesce(missing, F.lit(True))), df.where(~F.coalesce(missing, F.lit(True)) & ~F.coalesce(valid, F.lit(False)))


def cleanseSupplier(df):
    return df.withColumns(
        {
            "SupplierCode": _up("SUPP_CODE"),
            "SupplierName": F.trim(F.col("SUPP_NAME")),
            "TaxIdentifier": F.upper(F.regexp_replace(F.regexp_replace(F.trim(F.coalesce(F.col("TAX_ID"), F.lit(""))), " ", ""), "\\.", "")),
            "SupplierStatusCode": _upDefault("SUPP_STATUS_CD", "PEND"),
            "CountryCode": _up("COUNTRY_CD"),
            "RegionCode": _regionCode(),
            "DefaultCurrencyCode": _upDefault("DEFAULT_CCY", "USD"),
            "LeadTimeDays": F.when(F.col("LEAD_TIME_DAYS").isNull() | (F.col("LEAD_TIME_DAYS") <= 0), F.lit(14)).otherwise(F.col("LEAD_TIME_DAYS")).cast("int"),
            "PaymentTermsCode": _upDefault("PAY_TERMS_CD", "NET30"),
        }
    )


def supplierTaxIdentifierType(regionCol="RegionCode", taxCol="TaxIdentifier"):
    """usp_NormalizeSupplier: EIN / VATIN / GSTIN / ABN inference by region."""
    tax = F.col(taxCol)
    region = F.col(regionCol)
    return (
        F.when(F.length(tax) == 0, F.lit(None).cast("string"))
        .when((region == "NA") & tax.rlike("^[0-9]{2}-?[0-9]{7}$"), F.lit("EIN"))
        .when((region == "EU") & tax.rlike("^[A-Z]{2}[A-Z0-9]{8,12}$"), F.lit("VATIN"))
        .when((region == "APAC") & (F.length(tax) == 15), F.lit("GSTIN"))
        .when((region == "APAC") & tax.rlike("^[0-9]{11}$"), F.lit("ABN"))
        .otherwise(F.lit("UNKNOWN"))
    )


def deriveSupplierHash(df):
    return df.withColumns(
        {
            "WithholdingApplicableFlag": F.when((F.col("RegionCode") == "APAC") & (F.length(F.col("TaxIdentifier")) == 0), F.lit("Y")).otherwise(F.lit("N")),
            "TaxIdentifierTypeCode": supplierTaxIdentifierType(),
            "ChangeHash": X.changeHash("SupplierName", "TaxIdentifier", "PaymentTermsCode", "SupplierStatusCode", "LeadTimeDays"),
        }
    ).withColumn(
        "GstRegisteredFlag", F.when(F.col("TaxIdentifierTypeCode").isin("GSTIN", "ABN"), F.lit("Y")).otherwise(F.lit("N"))
    )


def splitSupplier(df):
    """(valid, missingTaxId, suspect)."""
    noTax = F.length(F.col("TaxIdentifier")) == 0
    valid = ~noTax | (F.col("SupplierStatusCode") == "PEND")
    return df.where(valid), df.where(~valid & noTax), df.where(~valid & ~noTax)


def deriveTaxRate(df):
    region = _regionCode()
    return df.withColumns(
        {
            "TaxCode": _up("TAX_CODE"),
            "RegionCode": region,
            "TaxTypeCode": F.when(region == "EU", F.lit("VAT")).when(region == "APAC", F.lit("GST")).otherwise(F.lit("SALESTAX")),
            "JurisdictionCode": F.upper(F.trim(F.coalesce(F.col("JURISDICTION_CD"), F.col("COUNTRY_CD"), F.lit("UNKNOWN")))),
            "RatePercent": _dec("RATE_PCT", 9, 4, 0),
            "IsRecoverableFlag": F.when(region == "EU", F.lit("Y")).otherwise(_upDefault("RECOVERABLE_FLG", "N")),
            "EffectiveFromDate": F.col("EFF_FROM_DT").cast("date"),
            "EffectiveToDate": _dateOrFarFuture("EFF_TO_DT"),
        }
    )


def taxRatePlausible():
    r = F.col("RatePercent")
    region = F.col("RegionCode")
    return ((region == "NA") & (r >= 0) & (r < 20)) | ((region == "EU") & (r > 0) & (r <= 27)) | ((region == "APAC") & (r > 0) & (r <= 15))


def cleansePaymentTerms(df):
    return df.withColumns(
        {
            "PaymentTermsCode": F.upper(F.regexp_replace(F.trim(F.col("TERMS_CODE")), " ", "")),
            "PaymentTermsDescription": F.trim(F.coalesce(F.col("TERMS_DESC"), F.lit(""))),
            "NetDays": F.when(F.col("NET_DAYS").isNull() | (F.col("NET_DAYS") < 0), F.lit(30)).otherwise(F.col("NET_DAYS")).cast("int"),
            "DiscountPercent": _dec("DISC_PCT", 9, 4, 0),
            "DiscountDays": F.coalesce(F.col("DISC_DAYS"), F.lit(0)).cast("int"),
            "RegionCode": _regionCode(),
            "IsCurrent": F.lit(True),
        }
    )


def cleanseVendorContract(df):
    return df.withColumns(
        {
            "ContractNumber": _up("CONTRACT_NBR"),
            "SupplierCode": _up("SUPP_CODE"),
            "ContractTypeCode": _upDefault("CONTRACT_TYPE_CD", "STD"),
            "CommitCurrencyCode": _upDefault("COMMIT_CCY", "USD"),
            "StartDate": F.col("START_DT").cast("date"),
            "EndDate": _dateOrFarFuture("END_DT"),
            "CommitAmount": _dec("COMMIT_AMT", 18, 2, 0),
            "RegionCode": _regionCode(),
        }
    )


def bandVendorContract(df):
    usd = F.col("CommitAmount") * F.col("ContractFxRate")
    return df.withColumns(
        {
            "CommitAmountUsd": usd.cast("decimal(18,2)"),
            "ContractBandCode": F.when(usd >= 1000000, F.lit("STRATEGIC")).when(usd >= 100000, F.lit("MAJOR")).otherwise(F.lit("TACTICAL")),
            "DiscountPercent": _dec("DISC_PCT", 9, 4, 0),
            "StatusCode": _upDefault("STATUS_CD", "DRFT"),
        }
    )


# ----------------------------------------------------------------------------- transactional (watermarked)


def deriveApInvoiceHeader(df):
    region = _up("REGION_CD")
    gross = F.coalesce(F.col("GROSS_AMT"), F.lit(0)).cast("decimal(18,2)")
    tax = F.coalesce(F.col("TAX_AMT"), F.lit(0)).cast("decimal(18,2)")
    return df.withColumns(
        {
            "InvoiceNumber": _up("INVOICE_NBR"),
            "SupplierCode": _up("VENDOR_CODE"),
            "RegionCode": region,
            "TaxRegimeCode": F.when(region == "EU", F.lit("VAT")).when(region == "APAC", F.lit("GST")).otherwise(F.lit("SUT")),
            "TaxRecoverableFlag": F.when(region == "NA", F.lit("N")).otherwise(F.lit("Y")),
            "GrossAmount": gross,
            "TaxAmount": tax,
            "NetAmount": (gross - tax).cast("decimal(18,2)"),
            "InvoiceCurrencyCode": _up("INV_CCY"),
            "PaymentTermsCode": _upDefault("TERMS_CD", "NET30"),
            "OnHoldFlag": _upDefault("HOLD_FLAG", "N"),
            "InvoiceDate": F.col("INVOICE_DT").cast("timestamp"),
            "FxEffectiveDate": F.col("INVOICE_DT").cast("date"),
        }
    )


def apInvoiceHeaderFx(df):
    """After the PaymentTerms (NetDays, DiscountDays) and FX (ConversionRate) lookups."""
    inv = F.col("INVOICE_DT").cast("date")
    return df.withColumns(
        {
            "GrossAmountUsd": (F.col("GrossAmount") * F.col("ConversionRate")).cast("decimal(18,2)"),
            "DueDate": F.coalesce(F.col("DUE_DT").cast("timestamp"), F.date_add(inv, F.col("NetDays").cast("int")).cast("timestamp")),
            "DiscountDueDate": F.date_add(inv, F.col("DiscountDays").cast("int")).cast("timestamp"),
            "ChangeHash": X.changeHash("InvoiceNumber", "SupplierCode", "GrossAmount", "TaxAmount", "OnHoldFlag"),
        }
    )


def splitApInvoice(df):
    """(valid, taxExceedsGross, nonPositiveGross)."""
    exceeds = F.col("TaxAmount") > F.col("GrossAmount")
    valid = (F.col("GrossAmount") > 0) & ~exceeds
    return df.where(valid), df.where(exceeds), df.where(~exceeds & ~F.coalesce(valid, F.lit(False)))


def deriveApInvoiceLine(df):
    return df.withColumns(
        {
            "InvoiceNumber": _up("INVOICE_NBR"),
            "LineNumber": F.col("INV_LINE_NBR").cast("int"),
            "CostCenterCode": _upDefault("COST_CENTER_CD", "UNALLOC"),
            "GlAccountCode": F.regexp_replace(_up("GL_ACCOUNT_CD"), " ", ""),
            "TaxCode": _upDefault("TAX_CODE", "NONE"),
            "LineAmount": _dec("LINE_AMT", 18, 2, 0),
            "PurchaseOrderNumber": _up("PO_NBR"),
        }
    )


def apInvoiceLineTax(df):
    """After the Cost Center and Tax Rate (TaxRatePercent, ignore-miss) lookups."""
    return df.withColumn(
        "LineTaxAmount",
        F.when(F.col("TaxRatePercent").isNull(), F.lit(0)).otherwise(F.col("LineAmount") * F.col("TaxRatePercent") / 100).cast("decimal(18,2)"),
    )


def deriveGlJournal(df):
    region = _up("REGION_CD")
    debit = F.coalesce(F.col("DEBIT_AMT"), F.lit(0)).cast("decimal(18,2)")
    credit = F.coalesce(F.col("CREDIT_AMT"), F.lit(0)).cast("decimal(18,2)")
    acct = F.col("ACCOUNTING_DT").cast("timestamp")
    month = F.month(acct)
    year = F.year(acct)
    return df.withColumns(
        {
            "JournalId": _up("JOURNAL_ID"),
            "JournalLineNumber": F.col("JOURNAL_LINE_NBR").cast("int"),
            "LedgerCode": _up("LEDGER_CD"),
            "GlAccountCode": F.regexp_replace(_up("GL_ACCOUNT_CD"), "-", ""),
            "CostCenterCode": _upDefault("COST_CENTER_CD", "UNALLOC"),
            "RegionCode": region,
            "AccountingDate": acct,
            "PeriodName": F.col("PERIOD_NAME"),
            "FiscalYear": F.when(region == "NA", F.when(month >= 7, year + 1).otherwise(year))
            .when(region == "APAC", F.when(month >= 4, year + 1).otherwise(year))
            .otherwise(year)
            .cast("int"),
            "FiscalPeriod": F.when(region == "NA", ((month + 5) % 12) + 1).when(region == "APAC", ((month + 8) % 12) + 1).otherwise(month).cast("int"),
            "DebitAmount": debit,
            "CreditAmount": credit,
            "SignedAmount": (debit - credit).cast("decimal(18,2)"),
            "JournalCurrencyCode": _up("JRNL_CCY"),
            "SourceCode": _upDefault("SOURCE_CD", "MANUAL"),
        }
    )


def splitGlJournal(df):
    """(valid, bothSides, zeroValue)."""
    debit = F.col("DebitAmount")
    credit = F.col("CreditAmount")
    valid = ((debit > 0) & (credit == 0)) | ((credit > 0) & (debit == 0))
    both = (debit > 0) & (credit > 0)
    return df.where(valid), df.where(both), df.where(~valid & ~both)


def deriveLoyaltyLedger(df):
    region = _upDefault("RegionCode", "NA")
    delta = F.col("PointsDelta")
    entry = F.col("EntryDate").cast("timestamp")
    return df.withColumns(
        {
            "LoyaltyEntryId": F.col("LoyaltyEntryID").cast("long"),
            "CustomerId": F.col("CustomerID").cast("int"),
            "EntryTypeCode": _upDefault("EntryTypeCode", "ADJ"),
            "ProgramCode": _upDefault("ProgramCode", "BASE"),
            "RegionCode": region,
            "PointsDelta": F.coalesce(delta, F.lit(0)).cast("int"),
            "PointsEarned": F.when(delta.isNull() | (delta < 0), F.lit(0)).otherwise(delta).cast("int"),
            "PointsRedeemed": F.when(delta.isNull() | (delta > 0), F.lit(0)).otherwise(-delta).cast("int"),
            "EntryDate": entry,
            "ExpiryDate": F.coalesce(
                F.col("ExpiryDate").cast("timestamp"),
                F.when(region == "EU", F.add_months(entry, 12)).when(region == "APAC", F.add_months(entry, 18)).otherwise(F.add_months(entry, 24)).cast("timestamp"),
            ),
        }
    )


def loyaltyTier(netPointsCol):
    p = F.col(netPointsCol)
    return F.when(p >= 50000, F.lit("PLT")).when(p >= 20000, F.lit("GLD")).when(p >= 5000, F.lit("SLV")).otherwise(F.lit("BRZ"))


def aggregateLoyalty(df):
    agg = df.groupBy("CustomerId", "ProgramCode", "RegionCode").agg(
        F.sum("PointsEarned").cast("int").alias("TotalPointsEarned"),
        F.sum("PointsRedeemed").cast("int").alias("TotalPointsRedeemed"),
        F.count("LoyaltyEntryId").cast("int").alias("EntryCount"),
        F.max("EntryDate").alias("LastEntryDate"),
        F.min("ExpiryDate").alias("EarliestExpiryDate"),
    )
    agg = agg.withColumn("NetPointsBalance", (F.col("TotalPointsEarned") - F.col("TotalPointsRedeemed")).cast("int"))
    return agg.withColumn("LoyaltyTierCode", loyaltyTier("NetPointsBalance"))


def deriveOrderHeader(df):
    orderDate = F.col("OrderDate").cast("timestamp")
    return df.withColumns(
        {
            "OrderId": F.col("OrderID").cast("int"),
            "CustomerId": F.col("CustomerID").cast("int"),
            "SalespersonId": F.coalesce(F.col("SalespersonPersonID"), F.lit(-1)).cast("int"),
            "OrderDate": orderDate,
            "CustomerPoNumber": F.when(F.col("CustomerPurchaseOrderNumber").isNull(), F.lit("")).otherwise(_up("CustomerPurchaseOrderNumber")),
            "BackorderFlag": F.when(F.col("IsUndersupplyBackordered").cast("boolean"), F.lit("Y")).otherwise(F.lit("N")),
            "OrderComments": F.substring(F.trim(F.regexp_replace(F.regexp_replace(F.col("Comments"), "\r", " "), "\n", " ")), 1, 400),
            "ExpectedDeliveryDate": F.coalesce(F.col("ExpectedDeliveryDate").cast("timestamp"), F.date_add(orderDate.cast("date"), 3).cast("timestamp")),
            "LastEditedWhen": F.col("LastEditedWhen").cast("timestamp"),
        }
    ).withColumn("ChangeHash", X.changeHash("OrderId", "CustomerId", "BackorderFlag", "ExpectedDeliveryDate"))


def deriveOrderLine(df):
    qty = F.col("Quantity")
    price = F.coalesce(F.col("UnitPrice"), F.lit(0)).cast("decimal(18,2)")
    taxRate = F.coalesce(F.col("TaxRate"), F.lit(0)).cast("decimal(9,3)")
    return df.withColumns(
        {
            "OrderLineId": F.col("OrderLineID").cast("int"),
            "OrderId": F.col("OrderID").cast("int"),
            "StockItemId": F.col("StockItemID").cast("int"),
            "LineDescription": F.trim(F.col("Description")),
            "Quantity": qty.cast("int"),
            "UnitPriceAmount": price,
            "ExtendedAmount": (qty * price).cast("decimal(18,2)"),
            "LineTaxAmount": (qty * price * taxRate / 100).cast("decimal(18,2)"),
            "PickedFlag": F.when(F.col("PickingCompletedWhen").isNull(), F.lit("N")).otherwise(F.lit("Y")),
            "PackageTypeCode": _upDefault("PackageTypeCode", "EACH"),
        }
    )


def splitOrderLine(df):
    """(valid, invalidQuantity, negativePrice)."""
    badQty = F.col("Quantity") <= 0
    valid = (F.col("Quantity") > 0) & (F.col("UnitPriceAmount") >= 0)
    return df.where(valid), df.where(badQty), df.where(~F.coalesce(badQty, F.lit(False)) & ~F.coalesce(valid, F.lit(False)))


def derivePartnerSale(df):
    text = F.trim(F.col("SaleDateText"))
    saleDateIso = F.when(F.instr(F.col("SaleDateText"), "/") > 0, F.concat(F.substring(text, -4, 4), F.lit("-"), F.substring(text, 4, 2), F.lit("-"), F.substring(text, 1, 2))).otherwise(F.substring(text, 1, 10))
    qtyText = F.regexp_replace(F.trim(F.col("QuantityText")), ",", "")
    amtText = F.regexp_replace(F.regexp_replace(F.trim(F.col("AmountText")), ",", ""), "\\$", "")
    return df.withColumns(
        {
            "PartnerCode": _up("PartnerCode"),
            "PartnerOrderRef": _up("PartnerOrderRef"),
            "CustomerRef": _up("CustomerRef"),
            "ItemRef": F.upper(F.regexp_replace(F.trim(F.col("ItemRef")), " ", "")),
            "SaleDateIso": saleDateIso,
            "SaleDate": F.to_date(saleDateIso, "yyyy-MM-dd"),
            "QuantityText": qtyText,
            "AmountText": amtText,
            "Quantity": X.safeDecimal(qtyText, precision=18, scale=3),
            "GrossAmount": X.safeDecimal(amtText, precision=18, scale=2),
            "PartnerCurrencyCode": F.upper(F.substring(F.trim(F.coalesce(F.col("CurrencyText"), F.lit("USD"))), 1, 3)),
            "CountryName": _up("CountryText"),
        }
    )


def splitPartnerSale(df):
    """(valid, unparsableAmount, missingOrderRef)."""
    badAmount = F.coalesce(F.col("GrossAmount"), F.lit(0)) <= 0
    valid = (F.col("Quantity") > 0) & (F.col("GrossAmount") > 0) & (F.length(F.col("PartnerOrderRef")) > 0)
    return df.where(valid), df.where(badAmount), df.where(~badAmount & ~F.coalesce(valid, F.lit(False)))


def derivePayment(df):
    region = _up("REGION_CD")
    status = _up("PAY_STATUS_CD")
    payDate = F.col("PAY_DT").cast("timestamp")
    return df.withColumns(
        {
            "PaymentNumber": _up("PAYMENT_NBR"),
            "SupplierCode": _up("VENDOR_CODE"),
            "SourcePaymentMethodCode": _up("PAY_METHOD_CD"),
            "BankAccountCode": F.substring(F.trim(F.col("BANK_ACCT_CD")), -4, 4),
            "PaymentCurrencyCode": _up("PAY_CCY"),
            "PaymentAmount": F.col("PAY_AMT").cast("decimal(18,2)"),
            "PaymentDate": payDate,
            "PaymentStatusCode": F.when(status == "P", F.lit("PAID")).when(status == "V", F.lit("VOID")).otherwise(F.lit("PEND")),
            "RegionCode": region,
            "ValueDate": F.coalesce(
                F.col("VALUE_DT").cast("timestamp"),
                F.when(region == "EU", F.date_add(payDate.cast("date"), 2)).otherwise(F.date_add(payDate.cast("date"), 1)).cast("timestamp"),
            ),
        }
    ).withColumn("ChangeHash", X.changeHash("PaymentNumber", "SupplierCode", "PaymentAmount", "PaymentStatusCode"))


def regionalPaymentMethod(regionCol="RegionCode", sourceCol="SourcePaymentMethodCode", crosswalkCol="PaymentMethodCode"):
    """usp_AppendIncremental_Payment regional conformance layered on the crosswalk:
    EU SEPA/BACS conform to SEPA, APAC BPAY stays BPAY and other APAC non-cheque
    methods default to WIRE when the crosswalk is silent; otherwise the crosswalk value."""
    src = F.col(sourceCol)
    region = F.col(regionCol)
    cx = F.col(crosswalkCol)
    return (
        F.when((region == "EU") & src.isin("SEPA", "BACS"), F.lit("SEPA"))
        .when((region == "APAC") & (src == "BPAY"), F.lit("BPAY"))
        .when((region == "APAC") & cx.isNull() & (src != "CHK"), F.lit("WIRE"))
        .otherwise(F.coalesce(cx, src))
    )


def splitPayment(df):
    """(valid, futureDated, nonPositive)."""
    future = F.col("PaymentDate") > F.current_timestamp()
    valid = (F.col("PaymentAmount") > 0) & ~future
    return df.where(valid), df.where(future), df.where(~F.coalesce(future, F.lit(False)) & ~F.coalesce(valid, F.lit(False)))


def derivePurchaseOrderHeader(df):
    status = _up("PO_STATUS_CD")
    poDate = F.col("PO_DT").cast("timestamp")
    return df.withColumns(
        {
            "PurchaseOrderNumber": _up("PO_NBR"),
            "SupplierCode": _up("VENDOR_CODE"),
            "OrderStatusCode": F.when(status == "OP", F.lit("OPEN")).when(status == "CL", F.lit("CLSD")).when(status == "CN", F.lit("CANC")).otherwise(F.lit("UNKN")),
            "BuyingOrgCode": _up("BUY_ORG_CD"),
            "RegionCode": _up("REGION_CD"),
            "OrderCurrencyCode": _up("PO_CCY"),
            "OrderDate": poDate,
            "FxEffectiveDate": F.col("PO_DT").cast("date"),
            "OrderTotalAmount": _dec("PO_TOTAL_AMT", 18, 2, 0),
            "PromisedDate": F.coalesce(F.col("PROMISED_DT").cast("timestamp"), poDate),
            "LastUpdatedDate": F.col("LAST_UPD_DT").cast("timestamp"),
        }
    )


def purchaseOrderHeaderFx(df):
    return df.withColumn("OrderTotalAmountUsd", (F.col("OrderTotalAmount") * F.col("ConversionRate")).cast("decimal(18,2)")).withColumn(
        "ChangeHash", X.changeHash("PurchaseOrderNumber", "OrderStatusCode", "OrderTotalAmount", "PromisedDate")
    )


def splitPurchaseOrderHeader(df):
    """(valid, negativeTotal, unknownStatus)."""
    negative = F.col("OrderTotalAmount") < 0
    valid = (F.length(F.col("PurchaseOrderNumber")) > 0) & (F.col("OrderTotalAmount") >= 0) & (F.col("OrderStatusCode") != "UNKN")
    return df.where(valid), df.where(negative), df.where(~F.coalesce(negative, F.lit(False)) & ~F.coalesce(valid, F.lit(False)))


def derivePurchaseOrderLine(df):
    price = F.coalesce(F.col("UNIT_PRICE_AMT"), F.lit(0)).cast("decimal(18,2)")
    return df.withColumns(
        {
            "PurchaseOrderNumber": _up("PO_NBR"),
            "LineNumber": F.col("PO_LINE_NBR").cast("int"),
            "SourceItemCode": _up("ITEM_CODE"),
            "SourceUomCode": _upDefault("UOM_CD", "EA"),
            "OrderQuantity": F.col("ORDER_QTY").cast("decimal(18,4)"),
            "UnitPriceAmount": price,
            "ExtendedAmount": (F.col("ORDER_QTY") * price).cast("decimal(18,2)"),
            "TaxCode": _upDefault("TAX_CODE", "NONE"),
            "NeedByDate": F.coalesce(F.col("NEED_BY_DT").cast("timestamp"), F.lit(EPOCH).cast("timestamp")),
        }
    )


def purchaseOrderLineUom(df):
    """After the UoM (ConversionFactor) lookup."""
    return df.withColumn("OrderQuantityBase", (F.col("ORDER_QTY") * F.col("ConversionFactor")).cast("decimal(18,4)"))


def splitPurchaseOrderLine(df):
    """(valid, zeroQuantity, negativePrice)."""
    zero = F.col("OrderQuantityBase") <= 0
    valid = (F.col("OrderQuantityBase") > 0) & (F.col("UnitPriceAmount") >= 0)
    return df.where(valid), df.where(zero), df.where(~F.coalesce(zero, F.lit(False)) & ~F.coalesce(valid, F.lit(False)))


def deriveReturn(df):
    region = _upDefault("RegionCode", "NA")
    return df.withColumns(
        {
            "ReturnLineId": F.col("ReturnLineID").cast("int"),
            "InvoiceId": F.col("InvoiceID").cast("int"),
            "StockItemId": F.col("StockItemID").cast("int"),
            "SourceReturnReasonCode": _upDefault("ReturnReasonCode", "UNSTATED"),
            "RegionCode": region,
            "ReturnWindowDays": F.when(region == "EU", F.lit(14)).when(region == "APAC", F.lit(7)).otherwise(F.lit(30)).cast("int"),
            "QuantityReturned": F.col("QuantityReturned").cast("int"),
            "ReturnedWhen": F.col("ReturnedWhen").cast("timestamp"),
        }
    )


def deriveCreditNote(df):
    amount = F.col("CreditAmount")
    return df.withColumns(
        {
            "CreditNoteId": F.col("CreditNoteID").cast("int"),
            "InvoiceId": F.col("InvoiceID").cast("int"),
            "CreditReasonCode": _upDefault("CreditReasonCode", "UNSTATED"),
            "CreditAmount": _dec("CreditAmount", 18, 2, 0),
            "CurrencyCode": F.col("CurrencyCode"),
            "IssuedWhen": F.col("IssuedWhen").cast("timestamp"),
            "ApprovalBandCode": F.when(amount.isNull(), F.lit("NONE")).when(amount < 500, F.lit("AUTO")).when(amount < 5000, F.lit("MGR")).otherwise(F.lit("FIN")),
            "ApprovedFlag": F.when(X.isBlank("ApprovedBy"), F.lit("N")).otherwise(F.lit("Y")),
        }
    )


def splitCreditNote(df):
    """(approved, unapproved, zeroCredit)."""
    unapproved = F.col("ApprovedFlag") == "N"
    approved = (F.col("ApprovedFlag") == "Y") & (F.col("CreditAmount") > 0)
    return df.where(approved), df.where(unapproved), df.where(~unapproved & ~approved)


def tagSaleRegion(df):
    region = _upDefault("BillToRegionCode", "NA")
    return df.withColumns(
        {
            "InvoiceId": F.col("InvoiceID").cast("int"),
            "CustomerId": F.col("CustomerID").cast("int"),
            "OrderId": F.coalesce(F.col("OrderID"), F.lit(-1)).cast("int"),
            "RegionCode": region,
            "TaxRegimeCode": F.when(region == "EU", F.lit("VAT")).when(region == "APAC", F.lit("GST")).otherwise(F.lit("SUT")),
            "DeliveryMethodCode": _upDefault("DeliveryMethodCode", "UNKNOWN"),
            "SaleCurrencyCode": _upDefault("CurrencyCode", "USD"),
            "DeliveryConfirmedFlag": F.when(F.col("ConfirmedDeliveryTime").isNull(), F.lit("N")).otherwise(F.lit("Y")),
            "InvoiceDate": F.col("InvoiceDate").cast("timestamp"),
            "FxEffectiveDate": F.col("InvoiceDate").cast("date"),
        }
    )


def defaultMissingFx(df):
    return df.withColumns(
        {
            "EffectiveConversionRate": F.coalesce(F.col("ConversionRate"), F.lit(1)).cast("decimal(18,6)"),
            "FxImputedFlag": F.when(F.col("ConversionRate").isNull(), F.lit("Y")).otherwise(F.lit("N")),
            "ChangeHash": X.changeHash("InvoiceId", "CustomerId", "DeliveryMethodCode", "DeliveryConfirmedFlag"),
        }
    )


def recomputeSaleLineTax(df):
    net = F.col("Quantity") * F.col("UnitPrice")
    tax = net * F.col("TaxRate") / 100
    return df.withColumns(
        {
            "InvoiceLineId": F.col("InvoiceLineID").cast("int"),
            "InvoiceId": F.col("InvoiceID").cast("int"),
            "StockItemId": F.col("StockItemID").cast("int"),
            "NetAmount": net.cast("decimal(18,2)"),
            "RecomputedTaxAmount": tax.cast("decimal(18,2)"),
            "TaxVarianceAmount": F.abs(F.coalesce(F.col("TaxAmount"), F.lit(0)) - tax).cast("decimal(18,2)"),
            "LineProfitAmount": _dec("LineProfit", 18, 2, 0),
        }
    )


def splitSaleLine(df, tolerance=0.02):
    """(valid, taxMismatch, zeroQuantity)."""
    tol = F.lit(tolerance).cast("decimal(18,2)")
    mismatch = F.col("TaxVarianceAmount") > tol
    valid = ~mismatch & (F.col("Quantity") != 0)
    return df.where(valid), df.where(mismatch), df.where(~F.coalesce(mismatch, F.lit(False)) & ~F.coalesce(valid, F.lit(False)))


def standardizeShipment(df):
    country = _up("DestinationCountryCode")
    postal = F.trim(F.col("DestinationPostalCode"))
    return df.withColumns(
        {
            "ShipmentId": F.col("ShipmentID").cast("int"),
            "InvoiceId": F.col("InvoiceID").cast("int"),
            "CarrierCode": _upDefault("CarrierCode", "UNKN"),
            "TrackingNumber": F.upper(F.regexp_replace(F.trim(F.coalesce(F.col("TrackingNumber"), F.lit(""))), " ", "")),
            "DestinationCountryCode": country,
            "DestinationPostalCode": F.when(country.isin("USA", "CAN"), F.substring(F.regexp_replace(postal, " ", ""), 1, 5))
            .when(country.isin("JPN", "AUS"), F.regexp_replace(F.regexp_replace(postal, "-", ""), " ", ""))
            .otherwise(F.upper(postal)),
            "GrossWeightGrams": F.when(F.col("GrossWeightKg").isNull(), F.lit(0)).otherwise(F.col("GrossWeightKg") * 1000).cast("decimal(18,3)"),
            "DeliveredFlag": F.when(F.col("DeliveredWhen").isNull(), F.lit("N")).otherwise(F.lit("Y")),
            "TransitDays": F.when(F.col("DeliveredWhen").isNull(), F.lit(-1)).otherwise(F.datediff(F.col("DeliveredWhen").cast("date"), F.col("DespatchedWhen").cast("date"))).cast("int"),
            "DespatchedWhen": F.col("DespatchedWhen").cast("timestamp"),
            "DeliveredWhen": F.col("DeliveredWhen").cast("timestamp"),
        }
    )


def rebaseShipmentLine(df):
    return df.withColumns(
        {
            "ShipmentLineId": F.col("ShipmentLineID").cast("int"),
            "ShipmentId": F.col("ShipmentID").cast("int"),
            "StockItemId": F.col("StockItemID").cast("int"),
            "QuantityShipped": F.col("QuantityShipped").cast("int"),
            "LineWeightGrams": F.when(F.col("LineWeightKg").isNull(), F.lit(0)).otherwise(F.col("LineWeightKg") * 1000).cast("decimal(18,3)"),
        }
    )


def applyMovementSign(df):
    """After the TransactionType (MovementSign) and UoM (MovementConversionFactor, ignore-miss) lookups."""
    signed = F.col("Quantity") * F.col("MovementSign") * F.coalesce(F.col("MovementConversionFactor"), F.lit(1))
    return df.withColumns(
        {
            "StockMovementId": F.col("StockItemTransactionID").cast("long"),
            "StockItemId": F.col("StockItemID").cast("int"),
            "CounterpartyTypeCode": F.when(F.col("CustomerID").isNull(), F.when(F.col("SupplierID").isNull(), F.lit("INTERNAL")).otherwise(F.lit("SUPPLIER"))).otherwise(F.lit("CUSTOMER")),
            "SignedQuantity": signed.cast("decimal(18,3)"),
            "MovementDate": F.col("TransactionOccurredWhen").cast("date"),
            "TransactionOccurredWhen": F.col("TransactionOccurredWhen").cast("timestamp"),
        }
    )


def deriveWebSession(df):
    """After the Country -> Region lookup (RegionCode, ignore-miss)."""
    region = F.coalesce(F.col("RegionCode"), F.lit("NA"))
    consent = F.upper(F.trim(F.coalesce(F.col("ConsentFlag"), F.lit("N"))))
    url = F.col("LandingPageUrl")
    # TOKEN(REPLACE(url, "://", " "), " ", 2): the part after the scheme separator
    afterScheme = F.when(F.instr(url, "://") > 0, F.element_at(F.split(F.regexp_replace(url, "://", " "), " "), 2)).otherwise(F.lit(None).cast("string"))
    return df.withColumns(
        {
            "SessionGuid": _up("SessionGuid"),
            "RegionCode": region,
            "ConsentGivenFlag": F.when(consent == "Y", F.lit("Y")).otherwise(F.lit("N")),
            "CustomerId": F.when((region == "EU") & (consent != "Y"), F.lit(-1)).otherwise(F.coalesce(F.col("CustomerID"), F.lit(-1))).cast("int"),
            "UserAgentFamily": F.when(consent != "Y", F.lit("SUPPRESSED")).otherwise(F.substring(F.trim(F.coalesce(F.col("UserAgentText"), F.lit("UNKNOWN"))), 1, 40)),
            "LandingPagePath": F.when(url.isNull(), F.lit("/")).otherwise(F.lower(F.substring(afterScheme, 1, 200))),
            "ChannelCode": _upDefault("ChannelCode", "WEB"),
            "DeviceTypeCode": _upDefault("DeviceTypeCode", "UNKNOWN"),
            "PageViewCount": F.coalesce(F.col("PageViewCount"), F.lit(0)).cast("int"),
            "DurationSeconds": F.when(F.col("DurationSeconds").isNull() | (F.col("DurationSeconds") < 0), F.lit(0)).otherwise(F.col("DurationSeconds")).cast("int"),
            "BounceFlag": F.when(F.col("PageViewCount").isNull() | (F.col("PageViewCount") <= 1), F.lit("Y")).otherwise(F.lit("N")),
            "SessionStartWhen": F.col("SessionStartWhen").cast("timestamp"),
        }
    )


def splitWebSession(df):
    """(valid, implausibleDuration, malformedKey)."""
    tooLong = F.col("DurationSeconds") > 86400
    valid = (F.length(F.col("SessionGuid")) == 36) & ~tooLong
    return df.where(valid), df.where(tooLong), df.where(~F.coalesce(tooLong, F.lit(False)) & ~F.coalesce(valid, F.lit(False)))
