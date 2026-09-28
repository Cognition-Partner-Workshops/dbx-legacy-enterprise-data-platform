"""Conformed customer (``sales_silver.customer``) and the hybrid SCD2 customer
dimension (``sales_silver.dim_customer``).

Legacy artefacts replaced (see the PR description for the full map):

* ``stg.usp_TruncateAndReload_Customer`` - screens (missing name / bad region /
  no EU consent -> reject; stale account -> WARN) and regional consent model.
* ``stg.usp_NormalizeCustomer`` - name / tax-number standardisation, regional
  default currency.
* ``stg.usp_DeduplicateCustomer`` - duplicate grouping on the normalised tax
  number, then on standardised name + postal code; survivorship score;
  losers stay as WARN rows with ``is_survivor_row = false``.
* ``stg.usp_TranslateSourceCodes`` / ``WWI_REF.PKG_CODE_TRANSLATION`` - code
  translation with the untranslated code passing through unchanged.
* ``stg.vw_CustomerReadyForDimension`` + ``Integration.usp_MigrateStagedCustomerDataV2``
  + ``Dimension.Customer`` - hybrid SCD: Type 2 on trading name, bill-to,
  category, buying group, postal code, country, credit limit, tax
  registration and account status; Type 1 (overwritten on every version) on
  contact, phone, website, discount and the consent block.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_lakehouse.common.config import REGION_CODES, PipelineConfig
from sales_lakehouse.common.quality import quarantine
from sales_lakehouse.common.tables import overwriteTable, readTable, tableExists
from sales_lakehouse.silver.party_resolution import (
    PARTY_RESOLUTION_TABLE,
    STATUS_MISSING_XREF,
    UNKNOWN_PARTY_ID,
)

CUSTOMER_TABLE = "customer"
DIM_CUSTOMER_TABLE = "dim_customer"
UNKNOWN_MEMBER_KEY = -1
HIGH_DATE = "9999-12-31 23:59:59"
SOURCE_SYSTEM_CODE = "SQLSERVER_WWI_OLTP"
ERP_SOURCE_SYSTEM_CODE = "ORA_ERP"

BRONZE_CUSTOMERS = "sqlserver_sales_customers"
BRONZE_CUSTOMER_CATEGORIES = "sqlserver_sales_customer_categories"
BRONZE_BUYING_GROUPS = "sqlserver_sales_buying_groups"
BRONZE_SALES_TERRITORIES = "sqlserver_sales_sales_territories"
BRONZE_PEOPLE = "sqlserver_application_people"
BRONZE_CUST_MASTER = "oracle_wwi_mdm_cust_master"
BRONZE_CODE_TRANSLATION = "oracle_wwi_ref_code_translation"

CODE_SET_CUSTOMER_STATUS = "CUST_STATUS"
CODE_SET_PAYMENT_TERMS = "PAYMENT_TERMS"

# ref.Region.DefaultCurrencyCode as seeded by the staging reference tables.
REGION_DEFAULT_CURRENCY: dict[str, str] = {"NA": "USD", "EU": "EUR", "APAC": "SGD"}
# LEGACY QUIRK: APAC follows the EU opt-in consent model only for JP and AU and
# the NA opt-out model everywhere else (stg.usp_TruncateAndReload_Customer:
# "a compromise nobody has ever revisited").
APAC_OPT_IN_COUNTRIES: tuple[str, ...] = ("JP", "AU")

TYPE2_COLUMNS: tuple[str, ...] = (
    "customer_name",
    "bill_to_customer_name",
    "customer_category_name",
    "buying_group_name",
    "postal_postal_code",
    "primary_country_code",
    "credit_limit_amount",
    "tax_registration_number",
    "customer_status_code",
)
TYPE1_COLUMNS: tuple[str, ...] = (
    "primary_contact_name",
    "phone_number",
    "website_url",
    "standard_discount_percentage",
    "marketing_consent_flag",
    "consent_captured_when",
    "retention_expiry_date",
    "suppress_marketing_attributes_flag",
)
DIM_ATTRIBUTE_COLUMNS: tuple[str, ...] = (
    (
        "wwi_customer_id",
        "customer_business_key",
        "erp_party_id",
        "erp_customer_number",
        "source_system_code",
        "region_code",
        "customer_category_id",
        "buying_group_id",
        "bill_to_customer_id",
        "sales_territory_id",
        "credit_limit_currency_code",
        "payment_terms_code",
        "is_on_credit_hold",
        "account_opened_date",
        "dq_status_code",
    )
    + TYPE2_COLUMNS
    + TYPE1_COLUMNS
)


def hashColumns(columns: tuple[str, ...]):
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]), 256)


def standardizeName(col):
    """``stg.usp_NormalizeCustomer``: upper, trimmed, punctuation and legal-form
    suffixes removed, single spaces."""
    cleaned = F.upper(F.trim(F.regexp_replace(col, r"[^A-Za-z0-9 ]", " ")))
    noSuffix = F.regexp_replace(cleaned, r"\b(LTD|LIMITED|INC|LLC|GMBH|PTY|CO|CORP|SA|BV|PLC)\b", "")
    return F.nullif(F.trim(F.regexp_replace(noSuffix, r"\s+", " ")), F.lit(""))


def normalizeTaxNumber(col):
    return F.nullif(F.upper(F.regexp_replace(col, r"[^A-Za-z0-9]", "")), F.lit(""))


def latestSourceVersion(customers: DataFrame, keyColumn: str = "CustomerID") -> DataFrame:
    """One row per ``keyColumn``: the latest temporal version (``ValidFrom``
    descending, then ``ValidTo`` descending). Reproduces the incremental-extract
    de-duplication of ``stg.usp_DeduplicateCustomer`` step 0."""
    ordering = Window.partitionBy(keyColumn).orderBy(
        F.col("ValidFrom").desc_nulls_last(), F.col("ValidTo").desc_nulls_last()
    )
    return (
        customers.withColumn("versionRank", F.row_number().over(ordering))
        .filter(F.col("versionRank") == 1)
        .drop("versionRank")
    )


def translateCodes(
    df: DataFrame,
    codeTranslation: DataFrame,
    codeSet: str,
    sourceCol: str,
    targetCol: str,
    sourceSystemCode: str = ERP_SOURCE_SYSTEM_CODE,
    regionCol: str = "region_code",
) -> DataFrame:
    """``WWI_REF.PKG_CODE_TRANSLATION.translate`` on a DataFrame.

    Region-specific mappings beat the generic (NULL region) row, the latest
    ``EFFECTIVE_FROM_DT`` wins among those, and only active rows apply. Adds
    ``<targetCol>`` and ``<targetCol>_translated_flag``.
    """
    mappings = codeTranslation.filter(
        (F.col("CODE_SET_CD") == codeSet)
        & (F.col("SOURCE_SYS_CD") == sourceSystemCode)
        & (F.upper(F.coalesce(F.col("ACTIVE_FLG"), F.lit("Y"))) == "Y")
    ).select(
        F.col("SOURCE_VALUE_TXT").alias("xlatSourceValue"),
        F.col("TARGET_VALUE_TXT").alias("xlatTargetValue"),
        F.col("REGION_CD").alias("xlatRegion"),
        F.col("EFFECTIVE_FROM_DT").cast("timestamp").alias("xlatEffectiveFrom"),
        F.col("EFFECTIVE_TO_DT").cast("timestamp").alias("xlatEffectiveTo"),
    )
    joined = df.join(
        mappings,
        (F.col(sourceCol) == F.col("xlatSourceValue"))
        & (F.col("xlatRegion").isNull() | (F.col("xlatRegion") == F.col(regionCol)))
        & (F.col("xlatEffectiveTo").isNull() | (F.col("xlatEffectiveTo") >= F.current_timestamp())),
        "left",
    )
    ordering = Window.partitionBy("wwi_customer_id").orderBy(
        F.when(F.col("xlatRegion").isNotNull(), 0).otherwise(1),
        F.col("xlatEffectiveFrom").desc_nulls_last(),
    )
    picked = joined.withColumn("xlatRank", F.row_number().over(ordering)).filter(F.col("xlatRank") == 1)
    # LEGACY QUIRK: an unmapped code is passed through unchanged rather than
    # failing the extract (PKG_CODE_TRANSLATION.translate "no mapping, code
    # passed through"; stg.usp_TranslateSourceCodes @UnmappedAction = 'LEAVE').
    # The miss is recorded on the row via the *_translated_flag, not rejected.
    return (
        picked.withColumn(
            f"{targetCol}_translated_flag", F.col("xlatTargetValue").isNotNull() & F.col(sourceCol).isNotNull()
        )
        .withColumn(targetCol, F.coalesce(F.col("xlatTargetValue"), F.col(sourceCol)))
        .drop("xlatSourceValue", "xlatTargetValue", "xlatRegion", "xlatEffectiveFrom", "xlatEffectiveTo", "xlatRank")
    )


def conformCustomers(
    customers: DataFrame,
    categories: DataFrame,
    buyingGroups: DataFrame,
    territories: DataFrame,
    people: DataFrame,
    custMaster: DataFrame,
    partyResolution: DataFrame,
    codeTranslation: DataFrame,
    batchId: int,
) -> DataFrame:
    """Pure conformation: one row per OLTP customer, screens expressed as
    ``dq_status_code`` / ``dq_reason_code`` (the caller quarantines FAIL rows)."""
    latest = latestSourceVersion(customers)
    cat = latestSourceVersion(categories, "CustomerCategoryID").select(
        F.col("CustomerCategoryID").alias("customer_category_id"),
        F.col("CustomerCategoryName").alias("customer_category_name"),
    )
    grp = latestSourceVersion(buyingGroups, "BuyingGroupID").select(
        F.col("BuyingGroupID").alias("buying_group_id"), F.col("BuyingGroupName").alias("buying_group_name")
    )
    billTo = latest.select(
        F.col("CustomerID").alias("bill_to_customer_id"), F.col("CustomerName").alias("bill_to_customer_name")
    )
    terr = territories.select(
        F.col("SalesTerritoryID").alias("sales_territory_id"),
        F.upper(F.trim(F.col("TerritoryCode"))).alias("sales_territory_code"),
        F.upper(F.trim(F.col("RegionCode"))).alias("territoryRegionCode"),
        F.col("ReportingCurrencyCode").alias("territoryCurrencyCode"),
    )
    contact = latestSourceVersion(people, "PersonID").select(
        F.col("PersonID").alias("primary_contact_person_id"),
        F.col("FullName").alias("primary_contact_name"),
        F.col("EmailAddress").alias("primary_contact_email"),
    )
    party = partyResolution.select(
        "wwi_customer_id",
        F.col("raw_party_id"),
        F.col("resolved_party_id").alias("erp_party_id"),
        F.col("resolution_status_code").alias("party_resolution_status_code"),
    )
    erp = custMaster.select(
        F.col("CUST_ID").cast("bigint").alias("erp_party_id"),
        F.col("CUST_NBR").alias("erp_customer_number"),
        F.upper(F.trim(F.col("REGION_CD"))).alias("erpRegionCode"),
        F.upper(F.trim(F.col("COUNTRY_CD"))).alias("erpCountryCode"),
        F.col("CUST_STATUS_CD").alias("erpStatusCode"),
        F.col("PAYMENT_TERMS_CD").alias("erpPaymentTermsCode"),
        F.col("PRIMARY_CURR_CD").alias("erpCurrencyCode"),
        F.coalesce(F.col("VAT_REG_NBR"), F.col("TAX_REG_NBR"), F.col("GST_REG_NBR")).alias("erpTaxRegistrationNumber"),
    )

    base = (
        latest.select(
            F.col("CustomerID").cast("int").alias("wwi_customer_id"),
            F.col("CustomerName").alias("customer_name"),
            F.col("BillToCustomerID").cast("int").alias("bill_to_customer_id"),
            F.col("CustomerCategoryID").cast("int").alias("customer_category_id"),
            F.col("BuyingGroupID").cast("int").alias("buying_group_id"),
            F.col("PrimaryContactPersonID").cast("int").alias("primary_contact_person_id"),
            F.col("DeliveryCityID").cast("int").alias("delivery_city_id"),
            F.col("PostalCityID").cast("int").alias("postal_city_id"),
            F.col("CreditLimit").cast("decimal(19,4)").alias("credit_limit_amount"),
            F.col("AccountOpenedDate").cast("date").alias("account_opened_date"),
            F.col("StandardDiscountPercentage").cast("decimal(9,4)").alias("standard_discount_percentage"),
            F.col("IsOnCreditHold").cast("boolean").alias("is_on_credit_hold"),
            F.col("PaymentDays").cast("int").alias("payment_days"),
            F.col("PhoneNumber").alias("phone_number"),
            F.col("WebsiteURL").alias("website_url"),
            F.col("DeliveryPostalCode").alias("delivery_postal_code"),
            F.col("PostalPostalCode").alias("postal_postal_code"),
            F.col("SalesTerritoryID").cast("int").alias("sales_territory_id"),
            F.upper(F.trim(F.col("RegionCode"))).alias("sourceRegionCode"),
            F.col("TaxRegistrationNumber").alias("tax_registration_number_raw"),
            F.col("MarketingConsentFlag").cast("boolean").alias("marketing_consent_flag"),
            F.col("ConsentCapturedWhen").cast("timestamp").alias("consent_captured_when"),
            F.col("DataRetentionExpiresOn").cast("date").alias("retention_expiry_date"),
            F.col("ValidFrom").cast("timestamp").alias("source_modified_at"),
        )
        .join(cat, "customer_category_id", "left")
        .join(grp, "buying_group_id", "left")
        .join(billTo, "bill_to_customer_id", "left")
        .join(terr, "sales_territory_id", "left")
        .join(contact, "primary_contact_person_id", "left")
        .join(party, "wwi_customer_id", "left")
        .join(erp, "erp_party_id", "left")
    )

    regionCode = F.coalesce(F.col("sourceRegionCode"), F.col("territoryRegionCode"), F.col("erpRegionCode"))
    conformed = (
        base.withColumn("region_code", regionCode)
        .withColumn("primary_country_code", F.col("erpCountryCode"))
        .withColumn(
            "erp_party_id",
            F.coalesce(F.col("erp_party_id"), F.lit(UNKNOWN_PARTY_ID).cast("bigint")),
        )
        .withColumn(
            "party_resolution_status_code",
            F.coalesce(F.col("party_resolution_status_code"), F.lit(STATUS_MISSING_XREF)),
        )
        .withColumn(
            "merged_into_party_id",
            F.when(
                F.col("raw_party_id").isNotNull() & (F.col("raw_party_id") != F.col("erp_party_id")),
                F.col("erp_party_id"),
            ),
        )
        .withColumn("customer_business_key", F.concat_ws("|", F.lit("WWI_OLTP"), F.col("wwi_customer_id")))
        .withColumn("source_system_code", F.lit(SOURCE_SYSTEM_CODE))
        .withColumn("customer_name_standardized", standardizeName(F.col("customer_name")))
        .withColumn(
            "tax_registration_number",
            normalizeTaxNumber(F.coalesce(F.col("tax_registration_number_raw"), F.col("erpTaxRegistrationNumber"))),
        )
        .withColumn(
            "credit_limit_currency_code",
            F.coalesce(
                F.col("erpCurrencyCode"),
                F.col("territoryCurrencyCode"),
                *[F.when(regionCode == r, F.lit(c)) for r, c in REGION_DEFAULT_CURRENCY.items()],
            ),
        )
    )

    conformed = translateCodes(
        conformed, codeTranslation, CODE_SET_CUSTOMER_STATUS, "erpStatusCode", "customer_status_code"
    )
    conformed = translateCodes(
        conformed, codeTranslation, CODE_SET_PAYMENT_TERMS, "erpPaymentTermsCode", "payment_terms_code"
    )

    optIn = (F.col("region_code") == "EU") | (
        (F.col("region_code") == "APAC") & F.col("primary_country_code").isin(*APAC_OPT_IN_COUNTRIES)
    )
    # LEGACY QUIRK: NA consent is opt-out, so a NULL flag is consentable; opt-in
    # regions require the flag AND a captured timestamp (2018 privacy rule).
    consentOk = F.when(
        optIn,
        (F.col("marketing_consent_flag") == True) & F.col("consent_captured_when").isNotNull(),  # noqa: E712
    ).otherwise(F.lit(True))
    nameMissing = F.col("customer_name_standardized").isNull()
    regionBad = F.col("region_code").isNull() | ~F.col("region_code").isin(*REGION_CODES)
    creditBad = F.col("credit_limit_amount") < 0
    stale = F.col("retention_expiry_date").isNotNull() & (F.col("retention_expiry_date") < F.current_date())

    screened = conformed.withColumn(
        "dq_reason_code",
        F.when(nameMissing, "MISSING_NAME")
        .when(regionBad, "BAD_REGION")
        .when(creditBad, "BAD_CREDIT")
        .when(~consentOk, "NO_CONSENT")
        .when(stale, "STALE_ACCOUNT"),
    ).withColumn(
        "dq_status_code",
        F.when(F.col("dq_reason_code").isin("MISSING_NAME", "BAD_REGION", "BAD_CREDIT", "NO_CONSENT"), "FAIL")
        .when(F.col("dq_reason_code").isNotNull(), "WARN")
        .otherwise("PASS"),
    )

    # stg.vw_CustomerReadyForDimension: EU rows without marketing consent and
    # rows past their retention expiry have marketing attributes suppressed.
    screened = screened.withColumn(
        "suppress_marketing_attributes_flag",
        ((F.col("region_code") == "EU") & ~F.coalesce(F.col("marketing_consent_flag"), F.lit(False))) | stale,
    )

    deduped = applySurvivorship(screened)
    return (
        deduped.withColumn("row_hash", hashColumns(TYPE2_COLUMNS + TYPE1_COLUMNS))
        .withColumn("change_hash", hashColumns(TYPE2_COLUMNS))
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("loaded_at_utc", F.current_timestamp())
        .drop(
            "sourceRegionCode",
            "territoryRegionCode",
            "erpRegionCode",
            "erpCountryCode",
            "erpStatusCode",
            "erpPaymentTermsCode",
            "erpCurrencyCode",
            "erpTaxRegistrationNumber",
            "territoryCurrencyCode",
            "tax_registration_number_raw",
        )
    )


def applySurvivorship(df: DataFrame) -> DataFrame:
    """``stg.usp_DeduplicateCustomer``: group on the normalised tax number
    (rule ``TAX_EXACT``), otherwise on standardised name + postal code
    (``NAME_POSTAL``). Score = 2 per populated significant attribute + 10 if the
    row carries the group's latest source modification; ties resolve on the
    lowest business key. Losers stay in the table as WARN, ``is_survivor_row``
    false. Rows that already FAIL a screen do not compete."""
    eligible = F.col("dq_status_code") != "FAIL"
    taxGroup = F.when(
        eligible & F.col("tax_registration_number").isNotNull(),
        F.concat(F.lit("TAX|"), F.col("tax_registration_number")),
    )
    namePostal = F.when(
        eligible & F.col("customer_name_standardized").isNotNull() & F.col("postal_postal_code").isNotNull(),
        F.concat(
            F.lit("NAME|"),
            F.col("customer_name_standardized"),
            F.lit("|"),
            F.upper(F.regexp_replace(F.col("postal_postal_code"), r"\s", "")),
        ),
    )
    keyed = df.withColumn("dedupKey", F.coalesce(taxGroup, namePostal)).withColumn(
        "dedupRule", F.when(taxGroup.isNotNull(), "TAX_EXACT").when(namePostal.isNotNull(), "NAME_POSTAL")
    )
    byGroup = Window.partitionBy("dedupKey")
    populated = sum(
        F.when(F.col(c).isNotNull(), 2).otherwise(0)
        for c in (
            "phone_number",
            "website_url",
            "tax_registration_number",
            "postal_postal_code",
            "credit_limit_amount",
            "primary_contact_name",
        )
    )
    scored = (
        keyed.withColumn("groupSize", F.when(F.col("dedupKey").isNotNull(), F.count("*").over(byGroup)).otherwise(1))
        .withColumn("groupMaxModified", F.max("source_modified_at").over(byGroup))
        .withColumn(
            "survivorshipScore",
            populated + F.when(F.col("source_modified_at") == F.col("groupMaxModified"), 10).otherwise(0),
        )
    )
    ranking = byGroup.orderBy(F.col("survivorshipScore").desc(), F.col("customer_business_key").asc())
    ranked = scored.withColumn(
        "survivorRank", F.when(F.col("groupSize") > 1, F.row_number().over(ranking)).otherwise(1)
    )
    return (
        ranked.withColumn("duplicate_group_id", F.when(F.col("groupSize") > 1, F.xxhash64(F.col("dedupKey"))))
        .withColumn("survivorship_rule_applied", F.when(F.col("groupSize") > 1, F.col("dedupRule")))
        .withColumn("is_survivor_row", F.col("survivorRank") == 1)
        .withColumn(
            "dq_status_code",
            F.when(~F.col("is_survivor_row") & (F.col("dq_status_code") == "PASS"), "WARN").otherwise(
                F.col("dq_status_code")
            ),
        )
        .withColumn(
            "dq_reason_code",
            F.when(~F.col("is_survivor_row") & F.col("dq_reason_code").isNull(), "DUPLICATE_LOSER").otherwise(
                F.col("dq_reason_code")
            ),
        )
        .drop("dedupKey", "dedupRule", "groupSize", "groupMaxModified", "survivorshipScore", "survivorRank")
    )


def quarantineCustomers(spark: SparkSession, cfg: PipelineConfig, conformed: DataFrame) -> DataFrame:
    passing = conformed
    for reason, text in (
        ("MISSING_NAME", "CUST_NAME empty after cleaning"),
        ("BAD_REGION", "RegionCode could not be derived or is not NA/EU/APAC"),
        ("BAD_CREDIT", "credit limit is negative"),
        ("NO_CONSENT", "opt-in region row without explicit marketing consent and consent date"),
    ):
        passing = quarantine(
            spark,
            cfg,
            passing,
            f"CUSTOMER_{reason}",
            CUSTOMER_TABLE,
            (F.col("dq_status_code") == "FAIL") & (F.col("dq_reason_code") == reason),
            text,
        )
    return passing


def unknownMemberRow(spark: SparkSession, template: DataFrame) -> DataFrame:
    """``sqlserver/warehouse/dimensions/90_unknown_members.sql``: customer key -1."""
    values = {
        "customer_key": F.lit(UNKNOWN_MEMBER_KEY).cast("bigint"),
        "wwi_customer_id": F.lit(0).cast("int"),
        "customer_business_key": F.lit("UNKNOWN"),
        "customer_name": F.lit("Unknown"),
        "bill_to_customer_name": F.lit("Unknown"),
        "customer_category_name": F.lit("Unknown"),
        "buying_group_name": F.lit("Unknown"),
        "primary_contact_name": F.lit("Unknown"),
        "region_code": F.lit("NA"),
        "dq_status_code": F.lit("PASS"),
        "valid_from": F.lit("1900-01-01 00:00:00").cast("timestamp"),
        "valid_to": F.lit(HIGH_DATE).cast("timestamp"),
        "is_current": F.lit(True),
        "version_number": F.lit(1),
    }
    row = spark.range(1).select(
        *[values.get(f.name, F.lit(None).cast(f.dataType)).alias(f.name) for f in template.schema.fields]
    )
    return row


def prepareDimensionSource(customer: DataFrame, batchId: int) -> DataFrame:
    ready = customer.filter(F.col("is_survivor_row") & F.col("dq_status_code").isin("PASS", "WARN"))
    suppress = F.col("suppress_marketing_attributes_flag")
    source = (
        ready.withColumn("phone_number", F.when(suppress, F.lit(None)).otherwise(F.col("phone_number")))
        .withColumn("website_url", F.when(suppress, F.lit(None)).otherwise(F.col("website_url")))
        .withColumn("primary_contact_name", F.when(suppress, F.lit(None)).otherwise(F.col("primary_contact_name")))
    )
    return source.select(
        *DIM_ATTRIBUTE_COLUMNS,
        F.coalesce(F.col("source_modified_at"), F.current_timestamp()).alias("valid_from"),
        hashColumns(TYPE2_COLUMNS).alias("row_hash_type2"),
        hashColumns(TYPE1_COLUMNS).alias("row_hash_type1"),
        F.lit(batchId).cast("bigint").alias("batch_id"),
    )


def buildDimensionRows(source: DataFrame, startKey: int, versionNumber) -> DataFrame:
    keyed = source.withColumn(
        "customer_key",
        (F.lit(startKey) + F.row_number().over(Window.orderBy("wwi_customer_id", "valid_from"))).cast("bigint"),
    )
    return keyed.select(
        "customer_key",
        *DIM_ATTRIBUTE_COLUMNS,
        "valid_from",
        F.lit(HIGH_DATE).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
        versionNumber.cast("int").alias("version_number"),
        "row_hash_type2",
        "row_hash_type1",
        "batch_id",
        F.current_timestamp().alias("loaded_at_utc"),
    )


def loadDimCustomer(spark: SparkSession, cfg: PipelineConfig, customer: DataFrame) -> None:
    """Hybrid SCD2 load of ``dim_customer`` via ``DeltaTable.merge``.

    * new customer -> insert version 1
    * Type 2 hash changed -> close the current row (``valid_to`` = new
      ``valid_from``, ``is_current`` false) and insert the next version
    * Type 1 hash changed -> overwrite the Type 1 columns on EVERY version
    * nothing changed -> no-op, so re-running a batch creates no versions
    """
    fqn = cfg.fqn("silver", DIM_CUSTOMER_TABLE)
    source = prepareDimensionSource(customer, cfg.batchId)

    if not tableExists(spark, fqn):
        initial = buildDimensionRows(source, 0, F.lit(1))
        overwriteTable(initial.unionByName(unknownMemberRow(spark, initial)), fqn)
        return

    target = DeltaTable.forName(spark, fqn)
    current = spark.table(fqn).filter(F.col("is_current") & (F.col("customer_key") != UNKNOWN_MEMBER_KEY))
    currentSlim = current.select(
        F.col("wwi_customer_id").alias("curCustomerId"),
        F.col("row_hash_type2").alias("curHashType2"),
        F.col("version_number").alias("curVersion"),
    )
    # materialise the comparison before the merges mutate the table it reads
    compared = source.join(
        currentSlim, source["wwi_customer_id"] == currentSlim["curCustomerId"], "left"
    ).localCheckpoint(eager=True)
    newRows = compared.filter(F.col("curCustomerId").isNull())
    changedRows = compared.filter(
        F.col("curCustomerId").isNotNull() & (F.col("curHashType2") != F.col("row_hash_type2"))
    )

    # Type 1: overwrite on every version where the Type 1 hash differs
    # (Integration.usp_MigrateStagedCustomerDataV2 "Type 1: overwrite on EVERY version").
    type1Updates = {c: f"src.{c}" for c in TYPE1_COLUMNS}
    type1Updates["row_hash_type1"] = "src.row_hash_type1"
    type1Updates["batch_id"] = "src.batch_id"
    (
        target.alias("tgt")
        .merge(source.alias("src"), "tgt.wwi_customer_id = src.wwi_customer_id AND tgt.customer_key <> -1")
        .whenMatchedUpdate(condition="tgt.row_hash_type1 <> src.row_hash_type1", set=type1Updates)
        .execute()
    )

    # Type 2: close the current version at the new valid_from.
    closing = changedRows.select("wwi_customer_id", F.col("valid_from").alias("newValidFrom"), "batch_id")
    (
        target.alias("tgt")
        .merge(closing.alias("src"), "tgt.wwi_customer_id = src.wwi_customer_id AND tgt.is_current = true")
        .whenMatchedUpdate(set={"is_current": "false", "valid_to": "src.newValidFrom", "batch_id": "src.batch_id"})
        .execute()
    )

    maxKey = spark.table(fqn).agg(F.max("customer_key")).collect()[0][0] or 0
    inserts = newRows.withColumn("nextVersion", F.lit(1)).unionByName(
        changedRows.withColumn("nextVersion", F.col("curVersion") + 1)
    )
    toInsert = buildDimensionRows(
        inserts.drop("curCustomerId", "curHashType2", "curVersion"), int(maxKey), F.col("nextVersion")
    ).drop("nextVersion")
    if toInsert.limit(1).count() > 0:
        toInsert.write.format("delta").mode("append").saveAsTable(fqn)


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Build ``customer`` (overwrite) and merge ``dim_customer`` (SCD2)."""
    conformed = conformCustomers(
        readTable(spark, cfg, "bronze", BRONZE_CUSTOMERS),
        readTable(spark, cfg, "bronze", BRONZE_CUSTOMER_CATEGORIES),
        readTable(spark, cfg, "bronze", BRONZE_BUYING_GROUPS),
        readTable(spark, cfg, "bronze", BRONZE_SALES_TERRITORIES),
        readTable(spark, cfg, "bronze", BRONZE_PEOPLE),
        readTable(spark, cfg, "bronze", BRONZE_CUST_MASTER),
        readTable(spark, cfg, "silver", PARTY_RESOLUTION_TABLE),
        readTable(spark, cfg, "bronze", BRONZE_CODE_TRANSLATION),
        cfg.batchId,
    )
    passing = quarantineCustomers(spark, cfg, conformed)
    overwriteTable(passing, cfg.fqn("silver", CUSTOMER_TABLE))
    loadDimCustomer(spark, cfg, spark.table(cfg.fqn("silver", CUSTOMER_TABLE)))
