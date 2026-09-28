"""Ports of the STG_Work_* Data Flows and of the work-table procedures they call
(stg.usp_DeduplicateCustomer, work.usp_BuildProductCrosswalk,
work.usp_BuildInventoryPositionDaily, work.usp_MatchPaymentsToInvoices).

The T-SQL cursors / WHILE loops become window functions: the survivorship
ranking is a ROW_NUMBER over the duplicate group, the daily roll-forward is a
running SUM over the position date, and the oldest-first residual allocation
is a cumulative SUM of open invoice amounts compared with the payment amount.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from stg_common import expressions as X

SOURCE_RANK = {"ORA_ERP": 30, "WWI_OLTP": 20, "WWI_WEB": 10}
FUZZY_PREFIX_LENGTH = 12
NAME_OVERLAP_MINIMUM = 80.0


def _blank(col):
    return F.col(col).isNull() | (F.trim(F.col(col)) == "")


def _sourceRank(col="SourceSystemCode"):
    expr = F.lit(5)
    for code, rank in SOURCE_RANK.items():
        expr = F.when(F.col(col) == code, F.lit(rank)).otherwise(expr)
    return expr


# ----------------------------------------------------------------- CustomerDedup
def buildCustomerBlockingKey(df):
    """DERIVED[Build Blocking Key]: NormalizedName / BlockingKey / TaxKey."""
    name = F.trim(F.col("CustomerName"))
    postal = F.coalesce(F.col("PostalCode"), F.lit("00000"))
    return df.withColumns(
        {
            "NormalizedName": F.upper(F.regexp_replace(name, r"[ .,]", "")),
            "BlockingKey": F.concat_ws("|", F.substring(F.upper(F.regexp_replace(name, " ", "")), 1, 8), F.upper(F.trim(F.col("CountryCode"))), F.substring(F.upper(F.regexp_replace(F.trim(postal), " ", "")), 1, 5)),
            "TaxKey": F.when(_blank("TaxRegistrationNumber"), F.lit("NONE")).otherwise(F.upper(F.regexp_replace(F.trim(F.col("TaxRegistrationNumber")), "-", ""))),
        }
    )


def scoreCustomerSurvivorship(df, sourceSystemCode="ORA_ERP", fuzzyPrefixLength=FUZZY_PREFIX_LENGTH):
    """stg.usp_DeduplicateCustomer: rule grouping, survivorship score and winner per group."""
    src = df.withColumns(
        {
            "CandidateCustomerBusinessKey": X.sourceSystemKey(F.lit(sourceSystemCode), "CustomerCode"),
            "SourceSystemCode": F.coalesce(F.col("SourceSystemCode"), F.lit(sourceSystemCode)) if "SourceSystemCode" in df.columns else F.lit(sourceSystemCode),
            "SourceCustomerId": F.col("CustomerCode"),
            "MatchKeyName": F.coalesce(F.col("CustomerNameStandardized"), F.col("NormalizedName")),
            "MatchKeyTaxNumber": F.when(F.col("TaxKey") == "NONE", F.lit(None).cast("string")).otherwise(F.regexp_replace(F.col("TaxKey"), " ", "")),
            "MatchKeyPostal": F.col("PostalCode"),
            "MatchKeyCountry": F.col("CountryCode"),
            "SourceModifiedDate": F.col("SourceModifiedDate").cast("timestamp"),
            "SourceRank": _sourceRank(),
            "AttributeCompleteness": sum(
                F.when(F.col(c).isNotNull(), F.lit(2)).otherwise(F.lit(0))
                for c in ["TradingName", "TaxRegistrationNumber", "CreditLimitAmount", "CreditCurrencyCode", "CustomerClassCode", "PostalCode", "SourceCreatedDate"]
            ),
        }
    )
    taxRule = F.col("MatchKeyTaxNumber").isNotNull()
    postalRule = F.col("MatchKeyName").isNotNull() & F.col("MatchKeyPostal").isNotNull()
    grouped = src.withColumns(
        {
            "MatchRuleCode": F.when(taxRule, F.lit("EXACT_TAXNUM")).when(postalRule, F.lit("NAME_POSTAL")).otherwise(F.lit("NAME_FUZZY")),
            "GroupKey": F.when(taxRule, F.concat(F.lit("TAX|"), F.col("MatchKeyTaxNumber")))
            .when(postalRule, F.concat_ws("|", F.lit("NP"), F.col("MatchKeyName"), F.col("MatchKeyPostal")))
            .otherwise(F.concat_ws("|", F.lit("NF"), F.substring(F.coalesce(F.col("MatchKeyName"), F.lit("?")), 1, fuzzyPrefixLength), F.coalesce(F.col("MatchKeyCountry"), F.lit("??")))),
        }
    )
    grouped = grouped.withColumn("DuplicateGroupId", F.dense_rank().over(Window.orderBy("MatchRuleCode", "GroupKey")).cast("long"))
    group = Window.partitionBy("DuplicateGroupId")
    consent = (F.col("RegionCode") == "EU") & (F.upper(F.coalesce(F.col("MarketingConsentFlag").cast("string"), F.lit("N"))).isin("Y", "1", "TRUE"))
    scored = grouped.withColumn(
        "SurvivorshipScore",
        (F.col("SourceRank") + F.col("AttributeCompleteness") + F.when(F.col("SourceModifiedDate") == F.max("SourceModifiedDate").over(group), F.lit(10)).otherwise(F.lit(0)) + F.when(consent, F.lit(50)).otherwise(F.lit(0))).cast("decimal(9,4)"),
    )
    ordered = group.orderBy(F.col("SurvivorshipScore").desc(), F.col("CandidateCustomerBusinessKey"))
    ranked = scored.withColumns(
        {
            "SurvivorRank": F.row_number().over(ordered),
            "GroupSize": F.count("*").over(group),
            "WinnerBusinessKey": F.first("CandidateCustomerBusinessKey").over(ordered.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        }
    )
    return ranked.withColumns(
        {
            "MatchScore": F.when(F.col("MatchRuleCode") == "EXACT_TAXNUM", F.lit(100.00)).when(F.col("MatchRuleCode") == "NAME_POSTAL", F.lit(85.00)).otherwise(F.lit(60.00)).cast("decimal(5,2)"),
            "IsSelectedSurvivor": F.col("SurvivorRank") == 1,
            "LosesToBusinessKey": F.when(F.col("SurvivorRank") == 1, F.lit(None).cast("string")).otherwise(F.col("WinnerBusinessKey")),
            "DecisionNote": F.when(F.col("GroupSize") == 1, F.lit("singleton; no merge"))
            .when(F.col("SurvivorRank") == 1, F.concat(F.lit("survivor by "), F.col("MatchRuleCode"), F.lit("; score "), F.col("SurvivorshipScore").cast("string")))
            .otherwise(F.concat(F.lit("retired in favour of "), F.col("WinnerBusinessKey"), F.lit("; score "), F.col("SurvivorshipScore").cast("string"))),
        }
    )


def standardizeCustomerAddressByRegion(df, sourceSystemCode="ORA_ERP"):
    """DERIVED[Standardize By Region] into the work.CustomerAddressStandardized shape."""
    region = F.upper(F.trim(F.col("RegionCode")))
    line1 = F.trim(F.col("AddressLine1"))
    postal = F.trim(F.col("PostalCode"))
    naLine = F.upper(F.regexp_replace(F.regexp_replace(F.regexp_replace(line1, "Street", "ST"), "Avenue", "AVE"), "Suite", "STE"))
    outLine = F.when(region == "NA", naLine).when(region == "EU", line1).otherwise(F.upper(line1))
    outPostal = (
        F.when(region == "NA", F.substring(F.regexp_replace(postal, " ", ""), 1, 5))
        .when(region == "EU", F.concat(F.upper(F.trim(F.col("CountryCode"))), F.lit("-"), F.regexp_replace(postal, " ", "")))
        .otherwise(F.regexp_replace(postal, "[- ]", ""))
    )
    return df.withColumns(
        {
            "CustomerBusinessKey": X.sourceSystemKey(F.lit(sourceSystemCode), "CustomerCode"),
            "AddressBusinessKey": F.concat_ws("|", X.sourceSystemKey(F.lit(sourceSystemCode), "CustomerCode"), F.col("AddressTypeCode")),
            "RuleSetCode": F.when(region.isin("NA", "EU"), region).otherwise(F.lit("ROW")),
            "InputAddressLine1": F.col("AddressLine1"),
            "InputCityName": F.col("CityName"),
            "InputPostalCode": F.col("PostalCode"),
            "InputCountryCode": F.col("CountryCode"),
            "OutputAddressLine1": outLine,
            "OutputCityName": F.upper(F.trim(F.col("CityName"))),
            "OutputStateProvinceCode": F.upper(F.trim(F.col("StateProvinceCode"))),
            "OutputPostalCode": outPostal,
            "OutputCountryCode": F.upper(F.trim(F.col("CountryCode"))),
            "AddressQualityCode": F.when(_blank("AddressLine1"), F.lit("MISSING")).when(_blank("PostalCode"), F.lit("NOPOST")).otherwise(F.lit("OK")),
            "GeographyBusinessKey": F.col("GeographyKey").cast("string"),
        }
    ).withColumn("StandardizationStatusCode", F.col("AddressQualityCode"))


def splitAddressQuality(df):
    """SPLIT[Screen Address Quality]: (usable, missingPostal, missingStreet)."""
    return df.where(F.col("AddressQualityCode") == "OK"), df.where(F.col("AddressQualityCode") == "NOPOST"), df.where(F.col("AddressQualityCode") == "MISSING")


# --------------------------------------------------------------- ProductCrosswalk
def _matchKey(codeCol, nameCol):
    return F.when(_blank(codeCol), F.upper(F.regexp_replace(F.trim(F.col(nameCol)), " ", ""))).otherwise(F.trim(F.col(codeCol)))


def shapeErpCrosswalkSide(products, sourceSystemCode="ORA_ERP"):
    """DERIVED[Shape Oracle Side] plus the #Erp columns of work.usp_BuildProductCrosswalk."""
    gtin = "Gtin" if "Gtin" in products.columns else "Barcode"
    df = products if gtin in products.columns else products.withColumn(gtin, F.lit(None).cast("string"))
    return df.select(
        F.lit(sourceSystemCode).alias("SourceSystemCode"),
        F.upper(F.trim(F.col("ProductCode"))).alias("SourceItemCode"),
        _matchKey(gtin, "ProductDescription").alias("MatchKey"),
        F.when(_blank(gtin), F.lit("NAME")).otherwise(F.lit("GTIN")).alias("MatchRuleCode"),
        F.lit(-1).alias("StockItemId"),
        F.trim(F.col("ProductCode")).alias("ErpProductCode"),
        X.sourceSystemKey(F.lit(sourceSystemCode), "ProductCode").alias("ErpProductBusinessKey"),
        F.when(_blank(gtin), F.lit(None).cast("string")).otherwise(F.regexp_replace(F.trim(F.col(gtin)), " ", "")).alias("Barcode"),
        X.cleanString(F.substring(F.col("ProductDescription"), 1, 200), upper=True).alias("NormalizedName"),
        F.trim(F.coalesce(F.col("ProductFamilyCode"), F.lit(""))).alias("BrandCode"),
        F.col("ProductKey") if "ProductKey" in products.columns else F.lit(None).cast("int").alias("ProductKey"),
    )


def shapeOltpCrosswalkSide(stockItems):
    """DERIVED[Shape OLTP Side] plus the #Oltp columns of work.usp_BuildProductCrosswalk."""
    return stockItems.select(
        F.lit("WWI_OLTP").alias("SourceSystemCode"),
        F.col("StockItemId").cast("string").alias("SourceItemCode"),
        _matchKey("Barcode", "StockItemName").alias("MatchKey"),
        F.when(_blank("Barcode"), F.lit("NAME")).otherwise(F.lit("GTIN")).alias("MatchRuleCode"),
        F.col("StockItemId").cast("int").alias("StockItemId"),
        F.col("StockItemId").cast("int").alias("OltpStockItemId"),
        X.sourceSystemKey(F.lit("WWI_OLTP"), "StockItemId").alias("StockItemBusinessKey"),
        F.when(_blank("Barcode"), F.lit(None).cast("string")).otherwise(F.regexp_replace(F.trim(F.col("Barcode")), " ", "")).alias("Barcode"),
        X.cleanString(F.col("StockItemName"), upper=True).alias("NormalizedName"),
        F.trim(F.coalesce(F.col("BrandName"), F.lit(""))).alias("BrandCode"),
    )


def splitCrosswalkCandidates(df):
    """SPLIT[Survivorship Rule]: (preferred GTIN, name match with >= 8 chars, unmatchable)."""
    preferred = F.col("MatchRuleCode") == "GTIN"
    nameMatch = (F.col("MatchRuleCode") == "NAME") & (F.length(F.col("MatchKey")) >= 8)
    return df.where(preferred), df.where(nameMatch), df.where(~F.coalesce(preferred | nameMatch, F.lit(False)))


def _tokenOverlap(erp, oltp):
    """Pass 3 NAME: share of the ERP name tokens (longer than 2 chars) found in the OLTP name, per brand."""
    erpTokens = erp.select("ErpProductBusinessKey", "BrandCode", F.explode(F.split(F.col("NormalizedName"), " ")).alias("Token")).where(F.length("Token") > 2)
    tokenCount = erp.select("ErpProductBusinessKey", F.size(F.split(F.col("NormalizedName"), " ")).alias("TokenCount"))
    oltpTokens = oltp.select("OltpStockItemId", "StockItemBusinessKey", F.col("BrandCode").alias("OltpBrandCode"), F.explode(F.split(F.col("NormalizedName"), " ")).alias("Token")).dropDuplicates()
    shared = (
        erpTokens.join(oltpTokens, (erpTokens.Token == oltpTokens.Token) & (erpTokens.BrandCode == oltpTokens.OltpBrandCode))
        .groupBy("ErpProductBusinessKey", "OltpStockItemId", "StockItemBusinessKey")
        .agg(F.count("*").alias("SharedTokens"))
        .join(tokenCount, "ErpProductBusinessKey")
        .withColumn("OverlapPercent", (F.lit(100.0) * F.col("SharedTokens") / F.col("TokenCount")).cast("decimal(5,2)"))
    )
    return shared


def resolveProductCrosswalk(erp, oltp, manualXref=None, supplierCatalog=None, nameOverlapMinimum=NAME_OVERLAP_MINIMUM):
    """work.usp_BuildProductCrosswalk: MANUAL_XREF > BARCODE > NAME > UNMATCHED, one row per ERP product."""
    erpBase = erp.select("SourceSystemCode", "SourceItemCode", "MatchKey", "MatchRuleCode", "ErpProductCode", "ErpProductBusinessKey", "Barcode", "NormalizedName", "BrandCode", "ProductKey")
    if supplierCatalog is not None:
        catalog = (
            supplierCatalog.select(F.trim(F.col("SupplierItemCode")).alias("ErpProductCode"), F.regexp_replace(F.trim(F.col("EanBarcode")), " ", "").alias("CatalogBarcode"), "SourceRowNumber")
            .withColumn("_rn", F.row_number().over(Window.partitionBy("ErpProductCode").orderBy(F.col("SourceRowNumber").desc())))
            .where(F.col("_rn") == 1)
            .drop("_rn", "SourceRowNumber")
        )
        erpBase = erpBase.join(catalog, "ErpProductCode", "left").withColumn("Barcode", F.coalesce(F.when(F.col("CatalogBarcode") != "", F.col("CatalogBarcode")), F.col("Barcode"))).drop("CatalogBarcode")
        partner = supplierCatalog.select(F.trim(F.col("SupplierItemCode")).alias("ErpProductCode"), F.substring(F.trim(F.col("SupplierItemCode")), 1, 60).alias("PartnerProductCode"), "SourceRowNumber")
        partner = partner.withColumn("_rn", F.row_number().over(Window.partitionBy("ErpProductCode").orderBy("SourceRowNumber"))).where(F.col("_rn") == 1).drop("_rn", "SourceRowNumber")
    else:
        partner = None
    oltpBase = oltp.select("OltpStockItemId", "StockItemBusinessKey", F.col("Barcode").alias("OltpBarcode"), F.col("NormalizedName").alias("OltpName"), F.col("BrandCode").alias("OltpBrandCode"))

    # pass 1: MANUAL_XREF via ref.SourceKeyCrosswalk (EntityName = 'Product', MatchMethodCode = 'MANUAL', IsActive)
    if manualXref is not None:
        xref = manualXref.where((F.col("EntityName") == "Product") & (F.col("MatchMethodCode") == "MANUAL") & (F.col("IsActive") == F.lit(True))).select(
            "SourceSystemCode", F.col("SourceKeyValue").alias("ErpProductCode"), F.col("ConformedBusinessKey").alias("StockItemBusinessKey"), F.col("MaintainedByName").alias("ReviewedByName")
        )
        manual = (
            erpBase.join(xref, ["SourceSystemCode", "ErpProductCode"])
            .join(oltpBase.select("OltpStockItemId", "StockItemBusinessKey"), "StockItemBusinessKey")
            .withColumn("_rn", F.row_number().over(Window.partitionBy("ErpProductBusinessKey").orderBy("OltpStockItemId")))
            .where(F.col("_rn") == 1)
            .drop("_rn")
            .withColumns({"MatchMethodCode": F.lit("MANUAL_XREF"), "MatchConfidence": F.lit(100.00).cast("decimal(5,2)"), "NameTokenOverlapPercent": F.lit(None).cast("decimal(5,2)"), "IsAmbiguous": F.lit(False), "CandidateCount": F.lit(1), "ResolvedFlag": F.lit(True)})
        )
    else:
        manual = None

    def remaining(resolvedSoFar):
        return erpBase if resolvedSoFar is None else erpBase.join(resolvedSoFar.select("ErpProductBusinessKey"), "ErpProductBusinessKey", "left_anti")

    # pass 2: BARCODE (ambiguous when several stock items share the barcode)
    barcodeHits = oltpBase.where(F.col("OltpBarcode").isNotNull()).groupBy(F.col("OltpBarcode").alias("Barcode")).agg(F.count("*").alias("CandidateCount"), F.min("OltpStockItemId").alias("OltpStockItemId"), F.min("StockItemBusinessKey").alias("StockItemBusinessKey"))
    barcode = remaining(manual).where(F.col("Barcode").isNotNull()).join(barcodeHits, "Barcode").withColumns(
        {
            "OltpStockItemId": F.when(F.col("CandidateCount") == 1, F.col("OltpStockItemId")),
            "StockItemBusinessKey": F.when(F.col("CandidateCount") == 1, F.col("StockItemBusinessKey")),
            "MatchMethodCode": F.lit("BARCODE"),
            "MatchConfidence": F.lit(95.00).cast("decimal(5,2)"),
            "NameTokenOverlapPercent": F.lit(None).cast("decimal(5,2)"),
            "IsAmbiguous": F.col("CandidateCount") > 1,
            "ResolvedFlag": F.col("CandidateCount") == 1,
            "ReviewedByName": F.lit(None).cast("string"),
        }
    )
    resolved = barcode if manual is None else manual.unionByName(barcode, allowMissingColumns=True)

    # pass 3: NAME token overlap within brand
    left = remaining(resolved)
    overlap = _tokenOverlap(left, oltp).where(F.col("OverlapPercent") >= F.lit(nameOverlapMinimum))
    best = Window.partitionBy("ErpProductBusinessKey").orderBy(F.col("OverlapPercent").desc(), F.col("OltpStockItemId"))
    nameMatch = (
        overlap.withColumn("TieCount", F.count("*").over(Window.partitionBy("ErpProductBusinessKey")))
        .withColumn("_rn", F.row_number().over(best))
        .where(F.col("_rn") == 1)
        .drop("_rn", "SharedTokens", "TokenCount")
    )
    named = left.join(nameMatch, "ErpProductBusinessKey").withColumns(
        {
            "MatchMethodCode": F.lit("NAME"),
            "MatchConfidence": F.col("OverlapPercent"),
            "NameTokenOverlapPercent": F.col("OverlapPercent"),
            "IsAmbiguous": F.col("TieCount") > 1,
            "CandidateCount": F.col("TieCount"),
            "ResolvedFlag": F.col("TieCount") == 1,
            "ReviewedByName": F.lit(None).cast("string"),
        }
    ).drop("OverlapPercent", "TieCount")
    resolved = resolved.unionByName(named, allowMissingColumns=True)

    # pass 4: UNMATCHED
    unmatched = remaining(resolved).withColumns(
        {
            "OltpStockItemId": F.lit(None).cast("int"),
            "StockItemBusinessKey": F.lit(None).cast("string"),
            "MatchMethodCode": F.lit("UNMATCHED"),
            "MatchConfidence": F.lit(0.00).cast("decimal(5,2)"),
            "NameTokenOverlapPercent": F.lit(None).cast("decimal(5,2)"),
            "IsAmbiguous": F.lit(False),
            "CandidateCount": F.lit(0),
            "ResolvedFlag": F.lit(False),
            "ReviewedByName": F.lit(None).cast("string"),
        }
    )
    out = resolved.unionByName(unmatched, allowMissingColumns=True).withColumn("StockItemId", F.col("OltpStockItemId"))
    if partner is not None:
        out = out.join(partner, "ErpProductCode", "left")
    else:
        out = out.withColumn("PartnerProductCode", F.lit(None).cast("string"))
    return out.withColumn("CandidateCount", F.col("CandidateCount").cast("int"))


# -------------------------------------------------------------- InventoryPosition
def aggregateDailyPosition(movements):
    """AGG[Aggregate Daily Position] plus the per-type sums of work.usp_BuildInventoryPositionDaily."""
    qty = F.col("SignedQuantity")
    typeCode = F.upper(F.coalesce(F.col("TransactionTypeCode"), F.lit("")))
    receipt = typeCode.rlike("RECEIPT|RECV|PURCH")
    issue = typeCode.rlike("ISSUE|SALE|CUST")
    transfer = typeCode.rlike("TRANSFER|XFER")
    adjust = ~(receipt | issue | transfer)
    return movements.groupBy(F.col("StockItemId"), F.coalesce(F.col("WarehouseCode"), F.lit("UNKNOWN")).alias("WarehouseCode"), F.col("MovementDate").cast("date").alias("PositionDate")).agg(
        F.sum(qty).cast("decimal(18,3)").alias("NetQuantity"),
        F.max(qty).cast("decimal(18,3)").alias("MaxSingleMovement"),
        F.min(qty).cast("decimal(18,3)").alias("MinSingleMovement"),
        F.count("StockItemId").cast("int").alias("MovementCount"),
        F.sum(F.when(receipt, F.abs(qty)).otherwise(F.lit(0))).cast("decimal(18,3)").alias("ReceiptQuantity"),
        F.sum(F.when(issue, F.abs(qty)).otherwise(F.lit(0))).cast("decimal(18,3)").alias("IssueQuantity"),
        F.sum(F.when(adjust, qty).otherwise(F.lit(0))).cast("decimal(18,3)").alias("AdjustmentQuantity"),
        F.sum(F.when(transfer & (qty > 0), F.abs(qty)).otherwise(F.lit(0))).cast("decimal(18,3)").alias("TransferInQuantity"),
        F.sum(F.when(transfer & (qty < 0), F.abs(qty)).otherwise(F.lit(0))).cast("decimal(18,3)").alias("TransferOutQuantity"),
        F.sum(F.when(receipt, F.abs(qty) * F.coalesce(F.col("UnitCostAmount"), F.lit(0))).otherwise(F.lit(0))).alias("_receiptCost"),
    ).withColumn(
        "DayAverageUnitCostUsd", F.when(F.col("ReceiptQuantity") == 0, F.lit(None)).otherwise(F.col("_receiptCost") / F.col("ReceiptQuantity")).cast("decimal(19,6)")
    ).drop("_receiptCost")


def classifyPosition(df):
    """DERIVED[Classify Position]."""
    net = F.col("NetQuantity")
    return df.withColumns(
        {
            "StockPositionCode": F.when(net < 0, F.lit("NEGATIVE")).when(net == 0, F.lit("ZERO")).otherwise(F.lit("POSITIVE")),
            "HighChurnFlag": F.when(F.col("MovementCount") > 50, F.lit("Y")).otherwise(F.lit("N")),
        }
    )


def rollForwardInventory(daily, sourceSystemCode="SQL_WWI", daysOfCoverWindow=28):
    """work.usp_BuildInventoryPositionDaily WHILE loop as window functions.

    closing = opening + receipts - issues + adjustments + transfers-in - transfers-out;
    the opening is the previous position day's closing (seeded at 0), the average
    cost is the receipt-weighted day cost carried forward, days of cover divides
    closing by the trailing average daily issue. RollForwardBrokenFlag marks a key
    whose previous calendar day has no position row (a gap in the extract)."""
    key = Window.partitionBy("StockItemId", "WarehouseCode").orderBy("PositionDate")
    dayDelta = F.col("ReceiptQuantity") - F.col("IssueQuantity") + F.col("AdjustmentQuantity") + F.col("TransferInQuantity") - F.col("TransferOutQuantity")
    df = daily.withColumn("_delta", dayDelta)
    df = df.withColumns(
        {
            "ClosingQuantity": F.sum("_delta").over(key.rowsBetween(Window.unboundedPreceding, Window.currentRow)).cast("decimal(18,3)"),
            "AverageUnitCostUsd": F.last("DayAverageUnitCostUsd", ignorenulls=True).over(key.rowsBetween(Window.unboundedPreceding, Window.currentRow)).cast("decimal(19,6)"),
            "_prevDate": F.lag("PositionDate").over(key),
            "_firstDate": F.min("PositionDate").over(Window.partitionBy(F.lit(1))),
        }
    )
    df = df.withColumn("OpeningQuantity", (F.col("ClosingQuantity") - F.col("_delta")).cast("decimal(18,3)"))
    df = df.withColumn("_dayNumber", F.datediff(F.col("PositionDate"), F.lit("1900-01-01").cast("date")))
    coverKey = Window.partitionBy("StockItemId", "WarehouseCode").orderBy("_dayNumber").rangeBetween(-daysOfCoverWindow, 0)
    df = df.withColumn("_avgDailyIssue", (F.sum("IssueQuantity").over(coverKey) / F.lit(daysOfCoverWindow)).cast("decimal(18,4)"))
    return df.withColumns(
        {
            "StockItemBusinessKey": X.sourceSystemKey(F.lit(sourceSystemCode), "StockItemId"),
            "ClosingValueUsd": (F.col("ClosingQuantity") * F.col("AverageUnitCostUsd")).cast("decimal(19,4)"),
            "DaysOfCoverEstimate": F.when(F.col("_avgDailyIssue").isNull() | (F.col("_avgDailyIssue") == 0), F.lit(None)).otherwise(F.col("ClosingQuantity") / F.col("_avgDailyIssue")).cast("decimal(9,2)"),
            "NegativeBalanceFlag": F.col("ClosingQuantity") < 0,
            "RollForwardBrokenFlag": (F.col("PositionDate") > F.col("_firstDate")) & (F.col("_prevDate").isNull() | (F.datediff(F.col("PositionDate"), F.col("_prevDate")) > 1)),
        }
    ).drop("_delta", "_prevDate", "_firstDate", "_dayNumber", "_avgDailyIssue")


# ------------------------------------------------------------------ PaymentMatch
def bandPaymentCandidates(payments, invoices):
    """DFT Match Payments To Invoices: supplier + currency join and EXACT / TOLERANCE / VARIANCE banding."""
    inv = invoices.select("SupplierCode", F.col("InvoiceCurrencyCode").alias("PaymentCurrencyCode"), "InvoiceNumber", "GrossAmount", "DueDate")
    joined = payments.select("PaymentNumber", "SupplierCode", "PaymentAmount", "PaymentCurrencyCode", "PaymentDate").join(inv, ["SupplierCode", "PaymentCurrencyCode"], "left")
    diff = F.abs(F.col("PaymentAmount") - F.col("GrossAmount"))
    return joined.withColumns(
        {
            "AmountVariance": F.when(F.col("GrossAmount").isNull(), F.lit(0)).otherwise(diff).cast("decimal(18,2)"),
            "MatchTypeCode": F.when(F.col("InvoiceNumber").isNull(), F.lit("UNMATCHED")).when(diff <= 0.01, F.lit("EXACT")).when(diff <= F.col("GrossAmount") * 0.02, F.lit("TOLERANCE")).otherwise(F.lit("VARIANCE")),
            "DaysLate": F.when(F.col("DueDate").isNull(), F.lit(0)).otherwise(F.datediff(F.col("PaymentDate").cast("date"), F.col("DueDate").cast("date"))).cast("int"),
            "LatePaymentFlag": F.when(F.col("DueDate").isNull(), F.lit("N")).when(F.col("PaymentDate") > F.col("DueDate"), F.lit("Y")).otherwise(F.lit("N")),
        }
    )


def splitPaymentBand(df):
    """SPLIT[Route Match Outcome]: (matched, variance, unmatched)."""
    return df.where(F.col("MatchTypeCode").isin("EXACT", "TOLERANCE")), df.where(F.col("MatchTypeCode") == "VARIANCE"), df.where(~F.col("MatchTypeCode").isin("EXACT", "TOLERANCE", "VARIANCE"))


def _regionalTolerance(regionCol, remainingCol):
    return F.when(F.col(regionCol) == "EU", F.lit(0.01)).when(F.col(regionCol) == "APAC", F.col(remainingCol) * 0.005).otherwise(F.lit(0.02)).cast("decimal(19,4)")


def allocatePayments(payments, invoices, sourceSystemCode="ORA_ERP"):
    """work.usp_MatchPaymentsToInvoices passes 2 (EXACT_AMT) and 3 (RESIDUAL / UNAPPLIED).

    Pass 1 (REMIT_REF) needs stg.Payment.RemittanceReference, which the staging
    package does not carry, so it never fires here. Pass 3's nested cursors are
    replaced by a cumulative sum of open amounts per supplier ordered by invoice
    date: an invoice is touched while the running total before it is below the
    payment amount, and the applied amount is the remaining cash capped by the
    open amount."""
    pay = payments.select(
        X.sourceSystemKey(F.lit(sourceSystemCode), "PaymentNumber").alias("PaymentBusinessKey"), "PaymentNumber", X.sourceSystemKey(F.lit(sourceSystemCode), "SupplierCode").alias("SupplierBusinessKey"),
        "SupplierCode", F.col("PaymentAmount").cast("decimal(19,4)").alias("PaymentAmount"), "PaymentDate", F.coalesce(F.col("RegionCode"), F.lit("NA")).alias("RegionCode"),
    )
    inv = invoices.select(
        X.sourceSystemKey(F.lit(sourceSystemCode), "InvoiceNumber").alias("ApInvoiceBusinessKey"), "InvoiceNumber", "SupplierCode", F.col("GrossAmount").cast("decimal(19,4)").alias("OpenAmount"),
        "InvoiceDate", "DiscountDueDate", F.coalesce(F.col("OnHoldFlag"), F.lit("N")).alias("OnHoldFlag"),
    ).where(F.col("OpenAmount") > 0)
    banded = bandPaymentCandidates(payments, invoices).select("PaymentNumber", "InvoiceNumber", "MatchTypeCode", "AmountVariance", "DaysLate", "LatePaymentFlag")

    # pass 2: exact open amount, or early-payment discount between 0.01 and 5%
    discountWindow = F.col("DiscountDueDate").isNotNull() & (F.col("PaymentDate") <= F.col("DiscountDueDate"))
    gap = F.col("OpenAmount") - F.col("PaymentAmount")
    exactCandidates = pay.join(inv, "SupplierCode").where((F.col("OpenAmount") == F.col("PaymentAmount")) | (discountWindow & (gap >= 0.01) & (gap <= F.col("OpenAmount") * 0.05)))
    exact = (
        exactCandidates.withColumn("_rn", F.row_number().over(Window.partitionBy("PaymentBusinessKey").orderBy("InvoiceDate", "ApInvoiceBusinessKey")))
        .where(F.col("_rn") <= 2)
        .drop("_rn")
        .withColumns(
            {
                "MatchPassNumber": F.lit(2),
                "MatchRuleCode": F.lit("EXACT_AMT"),
                "AppliedAmount": F.col("PaymentAmount"),
                "DiscountTakenAmount": F.when(discountWindow, gap).otherwise(F.lit(0.0)).cast("decimal(19,4)"),
                "ResidualAmount": F.lit(0.0).cast("decimal(19,4)"),
                "WithinToleranceFlag": F.lit(True),
                "MatchConfidence": F.lit(90.00).cast("decimal(5,2)"),
                "IsFinalAllocation": F.lit(True),
                "UnmatchedReasonCode": F.lit(None).cast("string"),
            }
        )
    )

    # pass 3: residual, oldest open invoice first, not on hold
    rest = pay.join(exact.select("PaymentBusinessKey").distinct(), "PaymentBusinessKey", "left_anti").where(F.col("PaymentAmount") > 0)
    openInv = inv.where(F.col("OnHoldFlag") != "Y")
    order = Window.partitionBy("PaymentBusinessKey").orderBy("InvoiceDate", "ApInvoiceBusinessKey")
    residualCandidates = rest.join(openInv, "SupplierCode").withColumn("_before", F.coalesce(F.sum("OpenAmount").over(order.rowsBetween(Window.unboundedPreceding, -1)), F.lit(0)))
    residual = (
        residualCandidates.where(F.col("_before") < F.col("PaymentAmount"))
        .withColumn("_remaining", (F.col("PaymentAmount") - F.col("_before")).cast("decimal(19,4)"))
        .withColumn("AppliedAmount", F.least(F.col("_remaining"), F.col("OpenAmount")).cast("decimal(19,4)"))
        .withColumn("_tolerance", _regionalTolerance("RegionCode", "PaymentAmount"))
        .withColumns(
            {
                "MatchPassNumber": F.lit(3),
                "MatchRuleCode": F.lit("RESIDUAL"),
                "DiscountTakenAmount": F.lit(None).cast("decimal(19,4)"),
                "ResidualAmount": (F.col("OpenAmount") - F.col("AppliedAmount")).cast("decimal(19,4)"),
                "WithinToleranceFlag": F.abs(F.col("OpenAmount") - F.col("AppliedAmount")) <= F.col("_tolerance"),
                "MatchConfidence": F.lit(65.00).cast("decimal(5,2)"),
                "IsFinalAllocation": (F.col("_remaining") - F.col("AppliedAmount")) <= F.col("_tolerance"),
                "UnmatchedReasonCode": F.lit(None).cast("string"),
            }
        )
        .drop("_before", "_remaining", "_tolerance")
    )
    applied = residual.groupBy("PaymentBusinessKey").agg(F.sum("AppliedAmount").alias("_applied"))
    unappliedRows = rest.join(applied, "PaymentBusinessKey", "left").withColumn("_remaining", (F.col("PaymentAmount") - F.coalesce(F.col("_applied"), F.lit(0))).cast("decimal(19,4)"))
    unapplied = (
        unappliedRows.withColumn("_tolerance", _regionalTolerance("RegionCode", "PaymentAmount"))
        .where(F.col("_remaining") > F.col("_tolerance"))
        .withColumns(
            {
                "ApInvoiceBusinessKey": F.lit(None).cast("string"),
                "InvoiceNumber": F.lit(None).cast("string"),
                "MatchPassNumber": F.lit(3),
                "MatchRuleCode": F.lit("UNAPPLIED"),
                "AppliedAmount": F.lit(0.0).cast("decimal(19,4)"),
                "DiscountTakenAmount": F.lit(None).cast("decimal(19,4)"),
                "ResidualAmount": F.col("_remaining"),
                "WithinToleranceFlag": F.lit(False),
                "MatchConfidence": F.lit(0.00).cast("decimal(5,2)"),
                "IsFinalAllocation": F.lit(True),
                "UnmatchedReasonCode": F.lit("NO_OPEN_INVOICE"),
            }
        )
        .drop("_applied", "_remaining", "_tolerance")
    )
    out = exact.unionByName(residual, allowMissingColumns=True).unionByName(unapplied, allowMissingColumns=True)
    out = out.withColumns({"AppliedAmountUsd": F.lit(None).cast("decimal(19,4)"), "FxDifferenceUsd": F.lit(None).cast("decimal(19,4)")})
    return out.join(banded, ["PaymentNumber", "InvoiceNumber"], "left").select(
        "PaymentBusinessKey", "PaymentNumber", "ApInvoiceBusinessKey", "InvoiceNumber", "SupplierBusinessKey", "SupplierCode", "MatchPassNumber", "MatchRuleCode", "MatchTypeCode",
        "AmountVariance", "DaysLate", "LatePaymentFlag", "AppliedAmount", "AppliedAmountUsd", "DiscountTakenAmount", "FxDifferenceUsd", "ResidualAmount", "WithinToleranceFlag",
        "MatchConfidence", "IsFinalAllocation", "UnmatchedReasonCode",
    )


def paymentMatchOutcome(allocations):
    """The stg.Payment MatchStatusCode / AppliedInvoiceCount / UnappliedAmount update."""
    return allocations.groupBy("PaymentNumber").agg(
        F.count(F.when(F.col("ApInvoiceBusinessKey").isNotNull(), F.lit(1))).cast("int").alias("AppliedInvoiceCount"),
        F.sum(F.when(F.col("MatchRuleCode") == "UNAPPLIED", F.col("ResidualAmount")).otherwise(F.lit(0))).cast("decimal(19,4)").alias("UnappliedAmount"),
        F.max(F.when(F.col("MatchRuleCode") == "UNAPPLIED", F.lit(1)).otherwise(F.lit(0))).alias("_hasUnapplied"),
        F.min("MatchPassNumber").alias("_minPass"),
        F.count("*").alias("_rows"),
    ).withColumn(
        "MatchStatusCode",
        F.when(F.col("_rows") == 0, F.lit("UNMATCHED")).when(F.col("_hasUnapplied") == 1, F.lit("PARTIAL")).when(F.col("_minPass") == 1, F.lit("MATCHED_REF")).when(F.col("_minPass") == 2, F.lit("MATCHED_AMT")).otherwise(F.lit("MATCHED_RESIDUAL")),
    ).drop("_hasUnapplied", "_minPass", "_rows")




PRODUCT_CROSSWALK_SCHEMA = (
    "ErpProductCode string, ErpProductBusinessKey string, SourceSystemCode string, SourceItemCode string, OltpStockItemId int, "
    "StockItemId int, ProductKey bigint, StockItemBusinessKey string, PartnerProductCode string, Barcode string, MatchKey string, "
    "MatchRuleCode string, MatchMethodCode string, MatchConfidence decimal(5,2), NormalizedName string, NameTokenOverlapPercent decimal(5,2), "
    "IsAmbiguous boolean, CandidateCount int, ResolvedFlag boolean, ReviewedByName string, BatchId bigint, PackageExecutionId bigint"
)


def emptyProductCrosswalk(spark):
    """Shape of work.ProductCrosswalk for consumers running before the first crosswalk build."""
    return spark.createDataFrame([], PRODUCT_CROSSWALK_SCHEMA)
