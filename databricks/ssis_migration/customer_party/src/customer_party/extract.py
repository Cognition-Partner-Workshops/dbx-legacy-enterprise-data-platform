"""EXT_ORA_* and EXT_SQL_* packages: land legacy sources into bronze Delta tables.

Sources are read through Lakehouse Federation. The two Oracle extracts are
incremental (UPDATED_DT watermark with a lookback window, plus a delete-detection
pass over WWI_AUDIT.CHANGE_LOG); the four SQL Server extracts are full reloads
that share one raw landing table keyed by `record_kind`, exactly like the legacy
`raw.SqlOrder` table the packages wrote to.
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from customer_party.config import LEGACY_OLTP, LEGACY_ORACLE, PipelineConfig, utcNow
from customer_party.tables import replacePartition, withLoadMetadata
from customer_party.watermark import getWatermark, logExtractWindow, setWatermark

BRONZE_CUSTOMER_MASTER = "bronze_raw_oracle_customer_master"
BRONZE_CUSTOMER_ADDRESS = "bronze_raw_oracle_customer_address"
BRONZE_SQL_ORDER = "bronze_raw_sql_order"

ORACLE_SOURCE_SYSTEM = "ORA_ERP"
SQL_SOURCE_SYSTEM = "WWI_OLTP"

RECORD_KIND_SEGMENT = "SEGMENT"
RECORD_KIND_PERSON = "PERSON"
RECORD_KIND_TERRITORY = "TERRITORY"
RECORD_KIND_PROMOTION = "PROMOTION"


def normalizeName(col: Column) -> Column:
    """Databricks equivalent of `WWI_MDM.FN_NORMALIZE_NAME`: upper, punctuation to space, collapsed."""
    cleaned = F.regexp_replace(F.upper(F.trim(col)), r"[.,'\-&/()]", " ")
    return F.trim(F.regexp_replace(cleaned, r"\s+", " "))


def deriveCustomerStatus(statusCol: Column, creditHoldCol: Column, deletedCol: Column) -> Column:
    """`WWI_MDM.FN_CUSTOMER_STATUS`: deleted and credit-hold override the stored status."""
    return (
        F.when(deletedCol == "Y", F.lit("CL"))
        .when(creditHoldCol == "Y", F.lit("HD"))
        .otherwise(F.upper(F.trim(statusCol)))
    )


def transformCustomerMaster(
    custDf: DataFrame,
    classDf: DataFrame,
    creditDf: DataFrame,
    windowFrom: datetime,
    windowTo: datetime,
    asOf: datetime,
) -> DataFrame:
    """The package's OLE DB source query, denormalised over the current classification and credit profile."""
    activeClass = (
        classDf.where(F.col("EXPIRY_DT").isNull() & (F.col("CLASS_SCHEME_CD") == "WWI"))
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("CUST_ID").orderBy(F.col("EFFECTIVE_DT").desc_nulls_last(), F.col("CUST_CLASS_ID").desc())
            ),
        )
        .where(F.col("rn") == 1)
        .select(
            F.col("CUST_ID").alias("cl_cust_id"),
            F.col("CLASS_CD").alias("classification_cd"),
            F.col("CLASS_DESC").alias("classification_desc"),
        )
    )
    currentCredit = creditDf.where(F.col("REVIEW_STATUS_CD") == "CUR").select(
        F.col("CUST_ID").alias("cp_cust_id"),
        F.col("CREDIT_LIMIT_AMT").alias("credit_limit_amt"),
        F.col("CREDIT_LIMIT_CURR_CD").alias("credit_ccy"),
        F.col("RISK_CLASS_CD").alias("credit_rating_cd"),
    )
    consentCutoff = F.add_months(F.lit(asOf).cast("timestamp"), -24)
    return (
        custDf.where(
            (F.col("UPDATED_DT") >= F.lit(windowFrom).cast("timestamp"))
            & (F.col("UPDATED_DT") < F.lit(windowTo).cast("timestamp"))
            & (F.coalesce(F.col("DELETED_FLG"), F.lit("N")) != "Y")
        )
        .join(activeClass, F.col("CUST_ID") == F.col("cl_cust_id"), "left")
        .join(currentCredit, F.col("CUST_ID") == F.col("cp_cust_id"), "left")
        .select(
            F.col("CUST_ID").cast("bigint").alias("cust_id"),
            F.col("CUST_NBR").alias("cust_nbr"),
            F.col("CUST_NAME").alias("cust_name"),
            normalizeName(F.col("CUST_NAME")).alias("cust_name_norm"),
            F.col("TRADING_NAME").alias("trading_name"),
            F.col("CUST_TYPE_CD").alias("legal_entity_cd"),
            F.col("REGION_CD").alias("region_cd"),
            F.col("COUNTRY_CD").alias("country_cd"),
            deriveCustomerStatus(F.col("CUST_STATUS_CD"), F.col("CREDIT_HOLD_FLG"), F.col("DELETED_FLG")).alias(
                "cust_status_cd"
            ),
            F.col("classification_cd"),
            F.col("classification_desc"),
            F.col("BUYING_GROUP_CD").alias("buying_group_cd"),
            F.col("PRICE_LIST_CD").alias("price_list_cd"),
            F.col("credit_limit_amt").cast("decimal(18,2)"),
            F.col("credit_ccy"),
            F.col("credit_rating_cd"),
            F.col("PAYMENT_TERMS_CD").alias("payment_terms_cd"),
            F.col("PRIMARY_CURR_CD").alias("currency_cd"),
            F.col("TAX_REG_NBR").alias("tax_registration_nbr"),
            F.col("VAT_REG_NBR").alias("vat_registration_nbr"),
            F.col("GST_REG_NBR").alias("gst_registration_nbr"),
            F.col("TAX_EXEMPT_FLG").alias("tax_exempt_flg"),
            F.col("TAX_EXEMPT_CERT_NBR").alias("tax_exempt_cert_nbr"),
            F.col("CREDIT_HOLD_FLG").alias("credit_hold_flg"),
            F.col("ACCT_MANAGER_CD").alias("acct_manager_cd"),
            F.when(
                (F.col("REGION_CD") == "EU") & (F.col("CONSENT_CAPTURED_DT") < consentCutoff), F.lit(None)
            )
            .otherwise(F.col("CONSENT_MARKETING_FLG"))
            .alias("marketing_consent_flg"),
            F.col("CONSENT_CAPTURED_DT").alias("consent_captured_dt"),
            F.col("CONSENT_SOURCE_CD").alias("consent_source_cd"),
            F.col("RETENTION_UNTIL_DT").alias("retention_until_dt"),
            F.col("FIRST_ORDER_DT").alias("first_order_dt"),
            F.col("LAST_ORDER_DT").alias("last_order_dt"),
            F.col("SOURCE_SYS").alias("source_sys"),
            F.col("CREATED_DT").alias("created_dt"),
            F.col("UPDATED_DT").alias("last_update_dt"),
            F.col("UPDATED_BY").alias("last_update_user"),
            F.lit("N").alias("delete_flag"),
        )
    )


def detectDeletes(changeLogDf: DataFrame, tableName: str, windowFrom: datetime, windowTo: datetime) -> DataFrame:
    """Delete-detection pass over WWI_AUDIT.CHANGE_LOG: one row per deleted primary key."""
    return (
        changeLogDf.where(
            (F.upper(F.col("TABLE_NAME")) == tableName)
            & (F.col("OPERATION_CD") == "D")
            & (F.col("CHANGE_TS") >= F.lit(windowFrom).cast("timestamp"))
            & (F.col("CHANGE_TS") < F.lit(windowTo).cast("timestamp"))
        )
        .groupBy(F.col("PK_VALUE_TXT").alias("pk_value"))
        .agg(F.max("CHANGE_TS").alias("last_update_dt"), F.max("CHANGED_BY").alias("last_update_user"))
    )


def extractOracleCustomerMaster(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """EXT_ORA_CustomerMaster."""
    windowFrom, windowTo = getWatermark(spark, cfg, ORACLE_SOURCE_SYSTEM, "WWI_MDM.CUST_MASTER")
    cust = spark.table(f"{LEGACY_ORACLE}.wwi_mdm.cust_master")
    classification = spark.table(f"{LEGACY_ORACLE}.wwi_mdm.cust_classification")
    credit = spark.table(f"{LEGACY_ORACLE}.wwi_mdm.cust_credit_profile")
    changeLog = spark.table(f"{LEGACY_ORACLE}.wwi_audit.change_log")

    rows = transformCustomerMaster(cust, classification, credit, windowFrom, windowTo, utcNow())
    deletes = detectDeletes(changeLog, "CUST_MASTER", windowFrom, windowTo).select(
        F.col("pk_value").cast("bigint").alias("cust_id"),
        F.col("last_update_dt"),
        F.col("last_update_user"),
        F.lit("Y").alias("delete_flag"),
    )
    landed = withLoadMetadata(
        rows.unionByName(deletes, allowMissingColumns=True).withColumn(
            "source_system_code", F.lit(ORACLE_SOURCE_SYSTEM)
        ),
        cfg,
    )
    replacePartition(landed, cfg.fqn(BRONZE_CUSTOMER_MASTER), "batch_id", str(cfg.batchId), spark)
    batch = spark.table(cfg.fqn(BRONZE_CUSTOMER_MASTER)).where(F.col("batch_id") == cfg.batchId)
    logExtractWindow(spark, cfg, ORACLE_SOURCE_SYSTEM, "WWI_MDM.CUST_MASTER", windowFrom, windowTo, batch.count())
    setWatermark(spark, cfg, ORACLE_SOURCE_SYSTEM, "WWI_MDM.CUST_MASTER", windowTo)
    return batch


def standardizePostalCode(regionCol: Column, postalCol: Column, zip4Col: Column) -> Column:
    """Region-aware postal normalisation from the address extract's derived column."""
    digits = F.regexp_replace(F.coalesce(postalCol, F.lit("")), r"[^0-9]", "")
    naZip = (
        F.when(F.length(digits) == 9, F.concat(F.substring(digits, 1, 5), F.lit("-"), F.substring(digits, 6, 4)))
        .when((F.length(digits) == 5) & zip4Col.isNotNull() & (F.length(F.trim(zip4Col)) == 4),
              F.concat(digits, F.lit("-"), F.trim(zip4Col)))
        .otherwise(F.substring(digits, 1, 5))
    )
    return (
        F.when(regionCol == "NA", naZip)
        .when(regionCol == "EU", F.upper(F.regexp_replace(F.coalesce(postalCol, F.lit("")), r"\s", "")))
        .otherwise(F.trim(F.coalesce(postalCol, F.lit(""))))
    )


def transformCustomerAddress(addrDf: DataFrame, windowFrom: datetime, windowTo: datetime) -> DataFrame:
    region = F.upper(F.trim(F.col("REGION_CD")))
    return addrDf.where(
        (F.col("UPDATED_DT") >= F.lit(windowFrom).cast("timestamp"))
        & (F.col("UPDATED_DT") < F.lit(windowTo).cast("timestamp"))
        & (F.coalesce(F.col("DELETED_FLG"), F.lit("N")) != "Y")
    ).select(
        F.col("CUST_ADDR_ID").cast("bigint").alias("cust_addr_id"),
        F.col("CUST_ID").cast("bigint").alias("cust_id"),
        F.col("ADDR_TYPE_CD").alias("addr_type_cd"),
        F.col("ADDR_SEQ_NBR").cast("int").alias("addr_seq_nbr"),
        F.col("ADDR_LINE_1").alias("addr_line_1"),
        F.col("ADDR_LINE_2").alias("addr_line_2"),
        F.col("ADDR_LINE_3").alias("addr_line_3"),
        F.col("CITY_TXT").alias("city_txt"),
        F.col("COUNTY_TXT").alias("county_txt"),
        F.col("STATE_PROV_CD").alias("state_prov_cd"),
        F.col("PREFECTURE_TXT").alias("prefecture_txt"),
        F.col("POSTAL_CD").alias("postal_cd"),
        F.col("ZIP4_CD").alias("zip4_cd"),
        standardizePostalCode(region, F.col("POSTAL_CD"), F.col("ZIP4_CD")).alias("postal_cd_std"),
        F.upper(F.trim(F.col("ADDR_LINE_1"))).alias("addr_line_1_std"),
        F.upper(F.trim(F.col("CITY_TXT"))).alias("city_std"),
        F.col("COUNTRY_CD").alias("country_cd"),
        region.alias("region_cd"),
        F.col("GEO_LAT").alias("geo_lat"),
        F.col("GEO_LON").alias("geo_lon"),
        F.col("ADDR_VERIFIED_FLG").alias("addr_verified_flg"),
        F.col("PRIMARY_FLG").alias("primary_flg"),
        F.col("VALID_FROM_DT").alias("valid_from_dt"),
        F.col("VALID_TO_DT").alias("valid_to_dt"),
        F.col("SOURCE_SYS").alias("source_sys"),
        F.col("UPDATED_DT").alias("last_update_dt"),
        F.lit("N").alias("delete_flag"),
    )


def withGeographyLookup(rows: DataFrame, geoKeys: DataFrame) -> DataFrame:
    """Left-join the stg.Geography (country, postal) keys and record MATCHED/UNMATCHED."""
    return rows.join(
        geoKeys,
        (F.upper(F.col("country_cd")) == F.col("geo_country")) & (F.col("postal_cd_std") == F.col("geo_postal")),
        "left",
    ).withColumn(
        "geography_lookup_status", F.when(F.col("geo_country").isNotNull(), F.lit("MATCHED")).otherwise(F.lit("UNMATCHED"))
    ).drop("geo_country", "geo_postal")


def extractOracleCustomerAddress(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """EXT_ORA_CustomerAddress.

    The SSIS geography lookup (stg.Geography) is reproduced as a left join that
    records `geography_lookup_status`; the legacy lookup table is empty on the
    baseline so misses are kept (status = UNMATCHED) instead of being redirected
    to the error output, which would have discarded every row.
    """
    windowFrom, windowTo = getWatermark(spark, cfg, ORACLE_SOURCE_SYSTEM, "WWI_MDM.CUST_ADDRESS")
    addr = spark.table(f"{LEGACY_ORACLE}.wwi_mdm.cust_address")
    changeLog = spark.table(f"{LEGACY_ORACLE}.wwi_audit.change_log")
    geography = spark.table("wwi_legacy_staging.stg.geography")
    geoKeys = geography.select(
        F.upper(F.trim(F.col("CountryCode"))).alias("geo_country"),
        F.upper(F.trim(F.col("PostalCode"))).alias("geo_postal"),
    ).distinct() if {"CountryCode", "PostalCode"} <= set(geography.columns) else spark.createDataFrame(
        [], "geo_country string, geo_postal string"
    )

    rows = withGeographyLookup(transformCustomerAddress(addr, windowFrom, windowTo), geoKeys)
    deletes = detectDeletes(changeLog, "CUST_ADDRESS", windowFrom, windowTo).select(
        F.col("pk_value").cast("bigint").alias("cust_addr_id"),
        F.col("last_update_dt"),
        F.lit("Y").alias("delete_flag"),
    )
    landed = withLoadMetadata(
        rows.unionByName(deletes, allowMissingColumns=True).withColumn(
            "source_system_code", F.lit(ORACLE_SOURCE_SYSTEM)
        ),
        cfg,
    )
    replacePartition(landed, cfg.fqn(BRONZE_CUSTOMER_ADDRESS), "batch_id", str(cfg.batchId), spark)
    batch = spark.table(cfg.fqn(BRONZE_CUSTOMER_ADDRESS)).where(F.col("batch_id") == cfg.batchId)
    logExtractWindow(spark, cfg, ORACLE_SOURCE_SYSTEM, "WWI_MDM.CUST_ADDRESS", windowFrom, windowTo, batch.count())
    setWatermark(spark, cfg, ORACLE_SOURCE_SYSTEM, "WWI_MDM.CUST_ADDRESS", windowTo)
    return batch


def toRawSqlOrderRows(df: DataFrame, recordKind: str, sourceKeyCol: str) -> DataFrame:
    """Shape a typed extract into the shared raw landing table (`raw.SqlOrder` + RecordKind).

    The legacy table only had string columns, so every attribute is carried in a
    string map; staging casts them back.
    """
    payloadCols = [c for c in df.columns if c != sourceKeyCol]
    payload = F.map_from_arrays(
        F.array(*[F.lit(c) for c in payloadCols]),
        F.array(*[F.col(c).cast("string") for c in payloadCols]),
    )
    return df.select(
        F.lit(recordKind).alias("record_kind"),
        F.col(sourceKeyCol).cast("string").alias("source_key"),
        payload.alias("payload"),
        F.lit(SQL_SOURCE_SYSTEM).alias("source_system_code"),
        F.current_timestamp().alias("extracted_at_utc"),
    )


def deriveMarketableFlag(regionCol: Column, consentStatusCol: Column) -> Column:
    """EU is opt-in, everywhere else is opt-out."""
    return F.when(
        F.trim(regionCol) == "EU", F.when(consentStatusCol == "OPTIN", F.lit("Y")).otherwise(F.lit("N"))
    ).otherwise(F.when(consentStatusCol == "OPTOUT", F.lit("N")).otherwise(F.lit("Y")))


def transformCustomerSegments(assignDf: DataFrame, segmentDf: DataFrame, asOf: datetime) -> DataFrame:
    """Current assignments joined to their segment; EU rows past retention lose their scoring attributes."""
    now = F.lit(asOf).cast("timestamp")
    seg = segmentDf.select(
        F.col("CustomerSegmentID").alias("seg_id"),
        F.col("SegmentCode"),
        F.col("SegmentName"),
        F.col("SegmentFamily"),
        F.trim(F.col("RegionCode")).alias("RegionCode"),
        F.col("ConsentRequired"),
        F.col("ConsentBasisCode"),
        F.col("RetentionMonths"),
    )
    joined = (
        assignDf.where(
            F.col("IsCurrentRow") & (F.coalesce(F.col("ValidToDate").cast("timestamp"), F.lit("9999-12-31").cast("timestamp")) > now)
        )
        .join(seg, F.col("CustomerSegmentID") == F.col("seg_id"), "inner")
        .withColumn(
            "ConsentStatusCode",
            F.when(F.col("ConsentCapturedWhen").isNotNull(), F.lit("OPTIN")).otherwise(F.lit("UNKNOWN")),
        )
    )
    retentionExpired = (F.col("RegionCode") == "EU") & (
        F.col("ValidFromDate").cast("timestamp") < F.add_months(now, -24)
    )
    return joined.select(
        F.col("CustomerSegmentAssignmentID"),
        F.col("CustomerID"),
        F.col("CustomerSegmentID"),
        F.col("SegmentCode"),
        F.col("SegmentName"),
        F.col("SegmentFamily"),
        F.col("RegionCode"),
        F.when(retentionExpired, F.lit(None)).otherwise(F.col("ScoreValue")).alias("ScoreValue"),
        F.when(retentionExpired, F.lit(None)).otherwise(F.col("AssignmentReason")).alias("AssignmentReason"),
        F.col("ConsentStatusCode"),
        F.col("ConsentBasisCode"),
        F.col("RetentionMonths"),
        deriveMarketableFlag(F.col("RegionCode"), F.col("ConsentStatusCode")).alias("MarketableFlag"),
        retentionExpired.alias("ScoringSuppressed"),
        F.col("ValidFromDate"),
        F.col("ValidToDate"),
        F.col("LastEditedWhen"),
    )


def extractSqlCustomerSegments(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """EXT_SQL_CustomerSegments (full reload of record_kind = SEGMENT)."""
    assignments = spark.table(f"{LEGACY_OLTP}.Sales.CustomerSegmentAssignments")
    segments = spark.table(f"{LEGACY_OLTP}.Sales.CustomerSegments")
    rows = transformCustomerSegments(assignments, segments, utcNow())
    return landSqlRecordKind(spark, cfg, rows, RECORD_KIND_SEGMENT, "CustomerSegmentAssignmentID")


def transformPeople(peopleDf: DataFrame) -> DataFrame:
    """Employees and salespeople only; logon, password and photo columns are never extracted."""
    return peopleDf.where(F.col("IsEmployee") | F.col("IsSalesperson")).select(
        F.col("PersonID"),
        F.col("FullName"),
        F.col("PreferredName"),
        F.col("SearchName"),
        F.col("IsEmployee"),
        F.col("IsSalesperson"),
        F.col("PhoneNumber"),
        F.col("FaxNumber"),
        F.col("EmailAddress"),
        F.when(F.col("IsSalesperson"), F.lit("SALES")).otherwise(F.lit("EMP")).alias("RoleCode"),
        F.col("ValidFrom"),
        F.col("ValidTo"),
    )


def extractSqlPeople(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """EXT_SQL_People (full reload of record_kind = PERSON)."""
    people = spark.table(f"{LEGACY_OLTP}.Application.People")
    return landSqlRecordKind(spark, cfg, transformPeople(people), RECORD_KIND_PERSON, "PersonID")


def deriveFiscalCalendarCode(regionCol: Column) -> Column:
    return (
        F.when(F.trim(regionCol) == "NA", F.lit("445"))
        .when(F.trim(regionCol) == "EU", F.lit("CAL"))
        .when(F.trim(regionCol) == "APAC", F.lit("APR_MAR"))
        .otherwise(F.lit("CAL"))
    )


def transformSalesTerritories(territoryDf: DataFrame, quotaDf: DataFrame, planDf: DataFrame, asOf: datetime) -> DataFrame:
    today = F.lit(asOf.date()).cast("date")
    parent = territoryDf.select(
        F.col("SalesTerritoryID").alias("parent_id"), F.col("TerritoryCode").alias("ParentTerritoryCode")
    )
    currentQuota = (
        quotaDf.where((F.col("PeriodStartDate") <= today) & (F.col("PeriodEndDate") >= today))
        .groupBy(F.col("SalesTerritoryID").alias("q_territory_id"))
        .agg(F.sum("QuotaAmount").alias("CurrentQuotaAmount"), F.max("QuotaCurrencyCode").alias("QuotaCurrencyCode"))
    )
    currentPlan = (
        planDf.where((F.col("EffectiveFromDate") <= today) & (F.col("EffectiveToDate").isNull() | (F.col("EffectiveToDate") >= today)))
        .groupBy(F.trim(F.col("RegionCode")).alias("plan_region"))
        .agg(F.max("PlanCode").alias("CommissionPlanCode"), F.max("Band1RatePercent").alias("CommissionRatePercent"))
    )
    return (
        territoryDf.join(parent, F.col("ParentTerritoryID") == F.col("parent_id"), "left")
        .join(currentQuota, F.col("SalesTerritoryID") == F.col("q_territory_id"), "left")
        .join(currentPlan, F.trim(F.col("RegionCode")) == F.col("plan_region"), "left")
        .select(
            F.col("SalesTerritoryID"),
            F.col("TerritoryCode"),
            F.col("TerritoryName"),
            F.col("ParentTerritoryID"),
            F.col("ParentTerritoryCode"),
            F.col("TerritoryLevel"),
            F.trim(F.col("RegionCode")).alias("RegionCode"),
            F.col("CountryISO3"),
            F.col("TaxRegimeCode"),
            F.col("FiscalCalendarCode").alias("SourceFiscalCalendarCode"),
            deriveFiscalCalendarCode(F.col("RegionCode")).alias("FiscalCalendarCode"),
            F.col("ReportingCurrencyCode"),
            F.col("PostalStandardCode"),
            F.col("ManagerPersonID"),
            F.col("IsActive"),
            F.col("CurrentQuotaAmount"),
            F.col("QuotaCurrencyCode"),
            F.col("CommissionPlanCode"),
            F.col("CommissionRatePercent"),
            F.col("LastEditedWhen"),
        )
    )


def extractSqlSalesTerritories(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """EXT_SQL_SalesTerritories (full reload of record_kind = TERRITORY)."""
    territories = spark.table(f"{LEGACY_OLTP}.Sales.SalesTerritories")
    quotas = spark.table(f"{LEGACY_OLTP}.Sales.SalesQuotas")
    plans = spark.table(f"{LEGACY_OLTP}.Sales.CommissionPlans")
    rows = transformSalesTerritories(territories, quotas, plans, utcNow())
    return landSqlRecordKind(spark, cfg, rows, RECORD_KIND_TERRITORY, "SalesTerritoryID")


def transformPromotions(promoDf: DataFrame, lineDf: DataFrame, redemptionDf: DataFrame) -> DataFrame:
    lines = lineDf.groupBy(F.col("PromotionID").alias("l_promo")).agg(
        F.count("*").alias("PromotionLineCount"),
        F.max("DiscountPercent").alias("DiscountPercent"),
        F.max("DiscountAmount").alias("DiscountAmount"),
    )
    redemptions = redemptionDf.groupBy(F.col("PromotionID").alias("r_promo")).agg(
        F.count("*").alias("RedemptionCount"), F.sum("RedeemedValue").alias("RedeemedValue")
    ) if "RedeemedValue" in redemptionDf.columns else redemptionDf.groupBy(F.col("PromotionID").alias("r_promo")).agg(
        F.count("*").alias("RedemptionCount"), F.sum("DiscountValue").alias("RedeemedValue")
    )
    promoBase = promoDf.drop("RedemptionCount", "RedeemedValue")
    return (
        promoBase.join(lines, F.col("PromotionID") == F.col("l_promo"), "left")
        .join(redemptions, F.col("PromotionID") == F.col("r_promo"), "left")
        .select(
            F.col("PromotionID"),
            F.col("PromotionCode"),
            F.col("PromotionName"),
            F.trim(F.col("RegionCode")).alias("RegionCode"),
            F.col("PromotionType").alias("MechanicCode"),
            F.col("CampaignReference"),
            F.col("StartDate"),
            F.col("EndDate"),
            F.col("BudgetAmount"),
            F.col("BudgetCurrencyCode"),
            F.col("SupplierFundedPercent"),
            F.col("MaximumRedemptionsPerCustomer"),
            F.col("RequiresCouponCode"),
            F.col("IsStackable"),
            F.col("PromotionStatus"),
            F.coalesce(F.col("PromotionLineCount"), F.lit(0)).alias("PromotionLineCount"),
            F.col("DiscountPercent"),
            F.col("DiscountAmount"),
            F.coalesce(F.col("RedemptionCount"), F.lit(0)).alias("RedemptionCount"),
            F.coalesce(F.col("RedeemedValue"), F.lit(0)).cast("decimal(18,2)").alias("RedeemedValue"),
            (F.coalesce(F.col("RedeemedValue"), F.lit(0)) / F.when(F.col("BudgetAmount") > 0, F.col("BudgetAmount")))
            .cast("decimal(9,4)")
            .alias("RedemptionRate"),
            F.col("LastEditedWhen"),
        )
    )


def extractSqlPromotions(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """EXT_SQL_Promotions (full reload of record_kind = PROMOTION)."""
    promotions = spark.table(f"{LEGACY_OLTP}.Sales.Promotions")
    lines = spark.table(f"{LEGACY_OLTP}.Sales.PromotionLines")
    redemptions = spark.table(f"{LEGACY_OLTP}.Sales.PromotionRedemptions")
    rows = transformPromotions(promotions, lines, redemptions)
    return landSqlRecordKind(spark, cfg, rows, RECORD_KIND_PROMOTION, "PromotionID")


def landSqlRecordKind(
    spark: SparkSession, cfg: PipelineConfig, rows: DataFrame, recordKind: str, sourceKeyCol: str
) -> DataFrame:
    """`DELETE FROM raw.SqlOrder WHERE RecordKind = ?` + fast load, as one replaceWhere."""
    landed = withLoadMetadata(toRawSqlOrderRows(rows, recordKind, sourceKeyCol), cfg)
    replacePartition(landed, cfg.fqn(BRONZE_SQL_ORDER), "record_kind", recordKind, spark)
    return spark.table(cfg.fqn(BRONZE_SQL_ORDER)).where(F.col("record_kind") == recordKind)


def readRawRecordKind(spark: SparkSession, cfg: PipelineConfig, recordKind: str) -> DataFrame:
    return spark.table(cfg.fqn(BRONZE_SQL_ORDER)).where(F.col("record_kind") == recordKind)
