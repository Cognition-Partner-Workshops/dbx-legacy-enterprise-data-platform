"""Sales dimensions: ``dim_sales_channel``, ``dim_sales_territory``,
``dim_salesperson`` (SCD2), ``dim_buying_group`` and
``bridge_customer_buying_group``.

Legacy artefacts replaced:

* ``Integration.usp_MigrateStagedSalesChannelData`` + ``Dimension.Sales Channel``
  (Type 1) and the ``Sales.SalesChannels`` header documenting the
  ``ChannelStatus`` overload.
* ``Integration.usp_MigrateStagedSalesTerritoryData`` + ``Dimension.Sales Territory``
  (Type 1 with retire-not-delete and the parent-code-to-NULL quirk).
* ``Integration.usp_MigrateStagedSalespersonData`` + ``Dimension.Salesperson``
  (Type 2 on the commission-relevant attributes, regional payout defaults).
* ``Integration.usp_MigrateStagedBuyingGroupData`` + ``Dimension.Buying Group``.
* ``Integration.usp_LoadBridgeCustomerBuyingGroup`` + ``Dimension.Customer Buying
  Group Bridge`` (allocation-factor normalisation to DECIMAL(9,6)).
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.quality import quarantine
from sales_lakehouse.common.tables import overwriteTable, readTable, tableExists
from sales_lakehouse.silver.customers import CUSTOMER_TABLE, HIGH_DATE, UNKNOWN_MEMBER_KEY, hashColumns

DIM_SALES_CHANNEL_TABLE = "dim_sales_channel"
DIM_SALES_TERRITORY_TABLE = "dim_sales_territory"
DIM_SALESPERSON_TABLE = "dim_salesperson"
DIM_BUYING_GROUP_TABLE = "dim_buying_group"
BRIDGE_CUSTOMER_BUYING_GROUP_TABLE = "bridge_customer_buying_group"

BRONZE_SALES_CHANNELS = "sqlserver_sales_sales_channels"
BRONZE_SALES_TERRITORIES = "sqlserver_sales_sales_territories"
BRONZE_PEOPLE = "sqlserver_application_people"
BRONZE_SALES_TEAMS = "sqlserver_application_sales_teams"
BRONZE_SALES_TEAM_MEMBERS = "sqlserver_application_sales_team_members"
BRONZE_BUYING_GROUPS = "sqlserver_sales_buying_groups"
BRONZE_BUYING_GROUP_MEMBERSHIP = "sqlserver_ref_buying_group_membership"

SOURCE_SYSTEM_CODE = "SQLSERVER_WWI_OLTP"
HIGH_DATE_DAY = "9999-12-31"
ALLOCATION_TOLERANCE = 0.000001
CHANNEL_STATUS_PILOT = "PILOT"
CHANNEL_STATUS_CLOSED = "CLOSED"

SALESPERSON_TYPE2_COLUMNS: tuple[str, ...] = (
    "sales_role_code",
    "sales_territory_code",
    "commission_plan_id",
    "quota_share_percent",
    "manager_person_id",
    "channel_responsibility_code",
    "sales_end_date",
)


def withUnknownMember(spark: SparkSession, df: DataFrame, keyColumn: str, labels: dict[str, str]) -> DataFrame:
    """Append the ``-1`` unknown member (``90_unknown_members.sql``)."""
    values = {keyColumn: F.lit(UNKNOWN_MEMBER_KEY).cast("bigint")}
    for name, text in labels.items():
        values[name] = F.lit(text)
    unknown = spark.range(1).select(
        *[values.get(f.name, F.lit(None).cast(f.dataType)).alias(f.name) for f in df.schema.fields]
    )
    return df.unionByName(unknown)


def surrogateKey(orderBy: list[str]) -> F.Column:
    return F.row_number().over(Window.orderBy(*orderBy)).cast("bigint")


def latestVersion(df: DataFrame, keyColumn: str, orderColumn: str = "LastEditedWhen") -> DataFrame:
    ordering = Window.partitionBy(keyColumn).orderBy(F.col(orderColumn).desc_nulls_last())
    return (
        df.withColumn("versionRank", F.row_number().over(ordering))
        .filter(F.col("versionRank") == 1)
        .drop("versionRank")
    )


# --------------------------------------------------------------------------- channel
def buildDimSalesChannel(channels: DataFrame, batchId: int) -> DataFrame:
    latest = latestVersion(channels, "SalesChannelID")
    status = F.upper(F.trim(F.col("ChannelStatus")))
    # LEGACY QUIRK: Sales.SalesChannels.ChannelStatus is overloaded across two
    # processes (see the table header). Order entry treats anything other
    # than 'CLOSED' as orderable while the nightly commission run treats
    # 'PILOT' as non-commissionable. There is no IsCommissionable flag in the
    # source and there never was, so both flags are derived from the one code,
    # which is carried verbatim as channel_status_code.
    df = latest.select(
        F.col("SalesChannelID").cast("int").alias("wwi_sales_channel_id"),
        F.upper(F.trim(F.col("ChannelCode"))).alias("sales_channel_code"),
        F.col("ChannelName").alias("sales_channel_name"),
        F.upper(F.trim(F.col("ChannelClass"))).alias("channel_class_code"),
        F.upper(F.trim(F.col("RegionCode"))).alias("region_code"),
        F.col("ChannelStatus").alias("channel_status_code"),
        (status != CHANNEL_STATUS_PILOT).alias("is_commissionable"),
        (status != CHANNEL_STATUS_CLOSED).alias("is_orderable"),
        F.col("PartnerIdentifier").alias("partner_identifier"),
        F.col("DefaultPriceListCode").alias("default_price_list_code"),
        F.col("CommissionModifierPercent").cast("decimal(5,2)").alias("commission_modifier_percent"),
        F.col("RequiresManualApproval").cast("boolean").alias("requires_manual_approval"),
        F.col("OrderPrefix").alias("order_number_prefix"),
        F.col("ValidFromDate").cast("date").alias("launched_on"),
        F.col("ValidToDate").cast("date").alias("retired_on"),
        (
            (status != CHANNEL_STATUS_CLOSED)
            & (F.col("ValidToDate").isNull() | (F.col("ValidToDate").cast("date") >= F.current_date()))
        ).alias("is_active"),
        F.lit(SOURCE_SYSTEM_CODE).alias("source_system_code"),
    )
    # usp_MigrateStagedSalesChannelData logs MARKETPLACE_NO_COMMISSION but still
    # loads the row: surfaced here as WARN, not quarantined.
    marketplaceNoCommission = (F.col("channel_class_code").isin("MKTPL", "MARKETPLACE")) & (
        F.coalesce(F.col("commission_modifier_percent"), F.lit(0)) == 0
    )
    df = df.withColumn("dq_status_code", F.when(marketplaceNoCommission, "WARN").otherwise("PASS"))
    return finishDimension(df, "sales_channel_key", ["region_code", "sales_channel_code"], batchId)


def finishDimension(df: DataFrame, keyColumn: str, orderBy: list[str], batchId: int) -> DataFrame:
    attributeColumns = tuple(c for c in df.columns if c != "dq_status_code")
    return df.select(
        surrogateKey(orderBy).alias(keyColumn),
        "*",
        hashColumns(attributeColumns).alias("row_hash"),
        F.lit(batchId).cast("bigint").alias("batch_id"),
        F.current_timestamp().alias("loaded_at_utc"),
    )


# --------------------------------------------------------------------------- territory
def buildDimSalesTerritory(territories: DataFrame, batchId: int) -> DataFrame:
    latest = latestVersion(territories, "SalesTerritoryID")
    codes = latest.select(
        F.col("SalesTerritoryID").alias("parentId"), F.upper(F.trim(F.col("TerritoryCode"))).alias("parentCode")
    )
    df = latest.join(codes, latest["ParentTerritoryID"] == codes["parentId"], "left").select(
        F.col("SalesTerritoryID").cast("int").alias("wwi_sales_territory_id"),
        F.upper(F.trim(F.col("TerritoryCode"))).alias("sales_territory_code"),
        F.coalesce(F.col("TerritoryName"), F.upper(F.trim(F.col("TerritoryCode")))).alias("sales_territory_name"),
        F.upper(F.coalesce(F.trim(F.col("RegionCode")), F.lit("GLOBAL"))).alias("region_code"),
        F.col("ParentTerritoryID").cast("int").alias("parent_territory_id"),
        # LEGACY QUIRK: a parent code that is not in the alignment is dropped to
        # NULL so the territory reports at region level, rather than rejected
        # (usp_MigrateStagedSalesTerritoryData: "the 2013 author left a note
        # asking for it to be rejected instead; it never was").
        F.col("parentCode").alias("parent_territory_code"),
        F.coalesce(F.col("TerritoryLevel").cast("int"), F.lit(0)).alias("territory_level"),
        F.lit("TERR").alias("territory_level_code"),
        F.lit("DIRECT").alias("coverage_model_code"),
        F.upper(F.trim(F.col("CountryISO3"))).alias("country_iso3_code"),
        F.col("TaxRegimeCode").alias("tax_regime_code"),
        F.col("FiscalCalendarCode").alias("fiscal_calendar_code"),
        F.col("ReportingCurrencyCode").alias("reporting_currency_code"),
        F.col("PostalStandardCode").alias("postal_standard_code"),
        F.col("ManagerPersonID").cast("int").alias("manager_person_id"),
        F.coalesce(F.col("IsActive").cast("boolean"), F.lit(True)).alias("is_active"),
        F.when(~F.coalesce(F.col("IsActive").cast("boolean"), F.lit(True)), F.current_date()).alias("retired_on"),
        F.lit(SOURCE_SYSTEM_CODE).alias("source_system_code"),
        F.lit("PASS").alias("dq_status_code"),
    )
    return finishDimension(df, "sales_territory_key", ["sales_territory_code"], batchId)


# --------------------------------------------------------------------------- salesperson
def buildSalespersonSource(
    people: DataFrame, salesTeams: DataFrame, salesTeamMembers: DataFrame, territories: DataFrame, batchId: int
) -> DataFrame:
    salespeople = latestVersion(
        people.filter(F.col("IsSalesperson").cast("boolean") | F.col("IsEmployee").cast("boolean")),
        "PersonID",
        "ValidFrom",
    ).filter(F.col("IsSalesperson").cast("boolean"))
    memberOrdering = Window.partitionBy("PersonID").orderBy(
        F.when(F.coalesce(F.col("IsPrimaryAssignment").cast("boolean"), F.lit(False)), 0).otherwise(1),
        F.when(F.col("ValidTo").isNull() | (F.col("ValidTo").cast("date") >= F.current_date()), 0).otherwise(1),
        F.col("ValidFrom").desc(),
    )
    membership = (
        salesTeamMembers.withColumn("memberRank", F.row_number().over(memberOrdering))
        .filter(F.col("memberRank") == 1)
        .select(
            F.col("PersonID").alias("memberPersonId"),
            F.col("SalesTeamID").alias("memberTeamId"),
            F.upper(F.trim(F.col("RoleCode"))).alias("sales_role_code"),
            F.col("CommissionPlanID").cast("int").alias("commission_plan_id"),
            F.col("QuotaSharePercent").cast("decimal(5,2)").alias("quota_share_percent"),
            F.col("ValidFrom").cast("date").alias("sales_start_date"),
            F.col("ValidTo").cast("date").alias("sales_end_date"),
        )
    )
    teams = latestVersion(salesTeams, "SalesTeamID").select(
        F.col("SalesTeamID").alias("teamId"),
        F.upper(F.trim(F.col("TeamCode"))).alias("sales_team_code"),
        F.col("SalesTerritoryID").alias("teamTerritoryId"),
        F.upper(F.trim(F.col("RegionCode"))).alias("region_code"),
        F.upper(F.trim(F.col("TeamType"))).alias("channel_responsibility_code"),
        F.col("ManagerPersonID").cast("int").alias("manager_person_id"),
    )
    terr = latestVersion(territories, "SalesTerritoryID").select(
        F.col("SalesTerritoryID").alias("terrId"),
        F.upper(F.trim(F.col("TerritoryCode"))).alias("sales_territory_code"),
        F.upper(F.trim(F.col("CountryISO3"))).alias("country_iso3_code"),
    )
    joined = (
        salespeople.join(membership, salespeople["PersonID"] == membership["memberPersonId"], "left")
        .join(teams, F.col("memberTeamId") == F.col("teamId"), "left")
        .join(terr, F.col("teamTerritoryId") == F.col("terrId"), "left")
    )
    region = F.col("region_code")
    # Regional payout conditioning from usp_MigrateStagedSalespersonData,
    # applied before hashing: NA accelerator threshold defaults to 100%, EU
    # works-agreement cap defaults to 40%, APAC budget FX rate defaults to 1.0
    # and payout frequency to QTR for JPN/KOR else MTH.
    return joined.select(
        F.col("PersonID").cast("int").alias("wwi_employee_id"),
        F.concat(F.lit("EMP-"), F.lpad(F.col("PersonID").cast("string"), 6, "0")).alias("salesperson_code"),
        F.col("FullName").alias("salesperson_name"),
        F.col("PreferredName").alias("preferred_name"),
        F.col("EmailAddress").alias("email_address"),
        region.alias("region_code"),
        F.col("country_iso3_code"),
        F.col("sales_team_code"),
        F.col("teamTerritoryId").cast("int").alias("sales_territory_id"),
        F.col("sales_territory_code"),
        F.col("sales_role_code"),
        F.col("channel_responsibility_code"),
        F.col("commission_plan_id"),
        F.col("quota_share_percent"),
        F.col("manager_person_id"),
        F.col("sales_start_date"),
        F.col("sales_end_date"),
        F.when(region == "NA", F.lit(100.00)).cast("decimal(9,4)").alias("na_accelerator_threshold_pct"),
        F.when(region == "EU", F.lit(40.00)).cast("decimal(9,4)").alias("eu_commission_cap_pct"),
        F.when(region == "APAC", F.lit(1.0)).cast("decimal(18,6)").alias("apac_budget_fx_rate"),
        F.when(region == "APAC", F.when(F.col("country_iso3_code").isin("JPN", "KOR"), "QTR").otherwise("MTH")).alias(
            "apac_payout_frequency_code"
        ),
        (F.col("sales_end_date").isNull() | (F.col("sales_end_date") >= F.current_date())).alias("is_active"),
        F.lit(SOURCE_SYSTEM_CODE).alias("source_system_code"),
        F.lit("PASS").alias("dq_status_code"),
        F.coalesce(F.col("ValidFrom").cast("timestamp"), F.current_timestamp()).alias("valid_from"),
        hashColumns(SALESPERSON_TYPE2_COLUMNS).alias("row_hash_type2"),
        F.lit(batchId).cast("bigint").alias("batch_id"),
    )


def mergeScd2(
    spark: SparkSession,
    fqn: str,
    source: DataFrame,
    keyColumn: str,
    naturalKey: str,
    unknownLabels: dict[str, str],
) -> None:
    """Generic Type 2 merge: close the current row on hash change, insert the
    new version, no-op when unchanged. ``source`` carries ``valid_from`` and
    ``row_hash_type2``."""
    attributeColumns = [c for c in source.columns if c not in ("valid_from", "row_hash_type2", "batch_id")]

    def versionRows(df: DataFrame, startKey: int, version) -> DataFrame:
        return df.select(
            (F.lit(startKey) + F.row_number().over(Window.orderBy(naturalKey, "valid_from")))
            .cast("bigint")
            .alias(keyColumn),
            *attributeColumns,
            "valid_from",
            F.lit(HIGH_DATE).cast("timestamp").alias("valid_to"),
            F.lit(True).alias("is_current"),
            version.cast("int").alias("version_number"),
            "row_hash_type2",
            "batch_id",
            F.current_timestamp().alias("loaded_at_utc"),
        )

    if not tableExists(spark, fqn):
        initial = versionRows(source, 0, F.lit(1))
        labels = dict(unknownLabels)
        overwriteTable(withUnknownMember(spark, initial, keyColumn, labels), fqn)
        return

    current = (
        spark.table(fqn)
        .filter(F.col("is_current") & (F.col(keyColumn) != UNKNOWN_MEMBER_KEY))
        .select(
            F.col(naturalKey).alias("curKey"),
            F.col("row_hash_type2").alias("curHash"),
            F.col("version_number").alias("curVersion"),
        )
    )
    compared = source.join(current, source[naturalKey] == current["curKey"], "left").localCheckpoint(eager=True)
    newRows = compared.filter(F.col("curKey").isNull())
    changedRows = compared.filter(F.col("curKey").isNotNull() & (F.col("curHash") != F.col("row_hash_type2")))

    closing = changedRows.select(
        F.col(naturalKey).alias("srcKey"), F.col("valid_from").alias("newValidFrom"), "batch_id"
    )
    (
        DeltaTable.forName(spark, fqn)
        .alias("tgt")
        .merge(closing.alias("src"), f"tgt.{naturalKey} = src.srcKey AND tgt.is_current = true")
        .whenMatchedUpdate(set={"is_current": "false", "valid_to": "src.newValidFrom", "batch_id": "src.batch_id"})
        .execute()
    )
    maxKey = spark.table(fqn).agg(F.max(keyColumn)).collect()[0][0] or 0
    inserts = newRows.withColumn("nextVersion", F.lit(1)).unionByName(
        changedRows.withColumn("nextVersion", F.col("curVersion") + 1)
    )
    toInsert = versionRows(inserts.drop("curKey", "curHash", "curVersion"), int(maxKey), F.col("nextVersion")).drop(
        "nextVersion"
    )
    if toInsert.limit(1).count() > 0:
        toInsert.write.format("delta").mode("append").saveAsTable(fqn)


# --------------------------------------------------------------------------- buying group
def buyingGroupCode(nameCol: F.Column) -> F.Column:
    """The consortium feed keys memberships on a code the OLTP table does not
    carry; the legacy staging derived it from the upper-cased name."""
    return F.upper(F.trim(F.regexp_replace(nameCol, r"\s+", "_")))


def buildDimBuyingGroup(buyingGroups: DataFrame, batchId: int) -> DataFrame:
    latest = latestVersion(buyingGroups, "BuyingGroupID", "ValidFrom")
    df = latest.select(
        F.col("BuyingGroupID").cast("int").alias("wwi_buying_group_id"),
        buyingGroupCode(F.col("BuyingGroupName")).alias("buying_group_code"),
        F.col("BuyingGroupName").alias("buying_group_name"),
        (F.col("ValidTo").cast("timestamp") < F.current_timestamp()).alias("is_dissolved"),
        F.lit(SOURCE_SYSTEM_CODE).alias("source_system_code"),
        F.lit("PASS").alias("dq_status_code"),
    )
    return finishDimension(df, "buying_group_key", ["buying_group_code"], batchId)


# --------------------------------------------------------------------------- bridge
def buildMembershipRows(membership: DataFrame | None, customer: DataFrame, dimBuyingGroup: DataFrame) -> DataFrame:
    """``usp_LoadBridgeCustomerBuyingGroup`` staging: explicit consortium
    memberships plus an implicit 1.0 row for every customer with a buying group
    but no membership row ("without this the bridge under-reports every direct
    customer")."""
    highDate = F.lit(HIGH_DATE_DAY).cast("date")
    groups = dimBuyingGroup.select(
        F.col("wwi_buying_group_id").alias("groupId"), F.col("buying_group_code").alias("groupCode")
    )
    implicit = (
        customer.filter(F.col("buying_group_id").isNotNull())
        .join(groups, F.col("buying_group_id") == F.col("groupId"), "left")
        .select(
            F.col("wwi_customer_id"),
            F.coalesce(F.col("groupCode"), F.concat(F.lit("ID_"), F.col("buying_group_id"))).alias("buying_group_code"),
            F.coalesce(F.col("account_opened_date"), F.lit("1900-01-01").cast("date")).alias("membership_from"),
            highDate.alias("membership_to"),
            F.lit(True).alias("is_primary_affiliation"),
            F.lit(1.0).cast("decimal(9,6)").alias("allocation_factor"),
            F.lit("EQUAL").alias("allocation_basis_code"),
            F.lit(None).cast("string").alias("allocation_category_scope"),
            F.lit(None).cast("date").alias("allocation_reviewed_on"),
            F.lit(None).cast("string").alias("allocation_reviewed_by"),
            F.lit(False).alias("rebate_eligible"),
            F.lit(None).cast("string").alias("rebate_agreement_reference"),
            F.upper(F.coalesce(F.col("region_code"), F.lit("NA"))).alias("region_code"),
            F.lit("(implicit)").alias("source_membership_reference"),
        )
    )
    if membership is None:
        return implicit
    explicit = membership.filter(F.coalesce(F.col("MembershipTo").cast("date"), highDate) >= F.current_date()).select(
        F.col("WWICustomerID").cast("int").alias("wwi_customer_id"),
        F.upper(F.trim(F.col("BuyingGroupCode"))).alias("buying_group_code"),
        F.coalesce(F.col("MembershipFrom").cast("date"), F.lit("1900-01-01").cast("date")).alias("membership_from"),
        F.coalesce(F.col("MembershipTo").cast("date"), highDate).alias("membership_to"),
        F.coalesce(F.col("IsPrimaryAffiliation").cast("boolean"), F.lit(False)).alias("is_primary_affiliation"),
        F.coalesce(F.col("AllocationFactor"), F.lit(0)).cast("decimal(9,6)").alias("allocation_factor"),
        F.upper(F.nullif(F.trim(F.col("AllocationBasisCode")), F.lit(""))).alias("allocation_basis_code"),
        F.nullif(F.trim(F.col("AllocationCategoryScope")), F.lit("")).alias("allocation_category_scope"),
        F.col("AllocationReviewedOn").cast("date").alias("allocation_reviewed_on"),
        F.nullif(F.trim(F.col("AllocationReviewedBy")), F.lit("")).alias("allocation_reviewed_by"),
        F.coalesce(F.col("RebateEligible").cast("boolean"), F.lit(False)).alias("rebate_eligible"),
        F.nullif(F.trim(F.col("RebateAgreementReference")), F.lit("")).alias("rebate_agreement_reference"),
        F.upper(F.coalesce(F.col("RegionCode"), F.lit("NA"))).alias("region_code"),
        F.col("SourceMembershipReference").alias("source_membership_reference"),
    )
    explicitCustomers = explicit.select("wwi_customer_id").distinct()
    return explicit.unionByName(implicit.join(explicitCustomers, "wwi_customer_id", "left_anti"))


def buildBridge(
    spark: SparkSession,
    cfg: PipelineConfig,
    membership: DataFrame | None,
    customer: DataFrame,
    dimBuyingGroup: DataFrame,
    dimCustomer: DataFrame | None,
) -> DataFrame:
    rows = buildMembershipRows(membership, customer, dimBuyingGroup)
    sourceTable = BRIDGE_CUSTOMER_BUYING_GROUP_TABLE
    rows = quarantine(
        spark,
        cfg,
        rows,
        "BRIDGE_ALLOCATION_RANGE",
        sourceTable,
        (F.col("allocation_factor") < 0) | (F.col("allocation_factor") > 1),
        "Allocation factor is negative or greater than one.",
    )
    rows = quarantine(
        spark,
        cfg,
        rows,
        "BRIDGE_PERIOD_REVERSED",
        sourceTable,
        F.col("membership_to") < F.col("membership_from"),
        "Membership period is reversed.",
    )
    byCustomer = Window.partitionBy("wwi_customer_id")
    rows = rows.withColumn("customerTotal", F.sum("allocation_factor").over(byCustomer))
    rows = quarantine(
        spark,
        cfg,
        rows,
        "BRIDGE_ALLOCATION_ZERO",
        sourceTable,
        F.col("customerTotal") == 0,
        "Every allocation factor for this customer is zero; nothing to normalise.",
    )
    knownCodes = dimBuyingGroup.filter(F.col("buying_group_key") > 0).select(
        F.col("buying_group_code").alias("knownCode"), F.col("buying_group_key")
    )
    rows = rows.join(knownCodes, rows["buying_group_code"] == knownCodes["knownCode"], "left")
    rows = quarantine(
        spark,
        cfg,
        rows,
        "BRIDGE_UNKNOWN_GROUP",
        sourceTable,
        F.col("knownCode").isNull(),
        "Buying group code is not present in dim_buying_group.",
    ).drop("knownCode")

    # LEGACY QUIRK: normalisation only touches customers whose factors are off
    # by more than 0.000001 and re-casts to DECIMAL(9,6) row by row, so a
    # three-way equal split lands on 0.999999 and fails the post-check below
    # exactly as the legacy load did.
    rows = rows.withColumn("customerTotal", F.sum("allocation_factor").over(byCustomer))
    needsNormalising = F.abs(F.col("customerTotal") - 1.0) > ALLOCATION_TOLERANCE
    rows = rows.withColumn(
        "allocation_factor",
        F.when(needsNormalising, (F.col("allocation_factor") / F.col("customerTotal")).cast("decimal(9,6)")).otherwise(
            F.col("allocation_factor")
        ),
    ).withColumn("allocation_basis_code", F.coalesce(F.col("allocation_basis_code"), F.lit("MANUAL")))
    rows = rows.withColumn("customerTotal", F.sum("allocation_factor").over(byCustomer))
    rows = quarantine(
        spark,
        cfg,
        rows,
        "BRIDGE_ALLOCATION_SUM",
        sourceTable,
        F.abs(F.col("customerTotal") - 1.0) > ALLOCATION_TOLERANCE,
        "Allocation factors for this customer do not sum to 1.0 after normalisation.",
    ).drop("customerTotal")

    # EU non-primary affiliations are never rebate eligible (competition rules).
    rows = rows.withColumn(
        "rebate_eligible",
        F.when((F.col("region_code") == "EU") & ~F.col("is_primary_affiliation"), F.lit(False)).otherwise(
            F.col("rebate_eligible")
        ),
    )
    if dimCustomer is not None:
        currentKeys = dimCustomer.filter(F.col("is_current") & (F.col("customer_key") > 0)).select(
            F.col("wwi_customer_id").alias("dimCustomerId"), "customer_key"
        )
        rows = rows.join(currentKeys, rows["wwi_customer_id"] == currentKeys["dimCustomerId"], "left").drop(
            "dimCustomerId"
        )
    else:
        rows = rows.withColumn("customer_key", F.lit(None).cast("bigint"))
    return rows.select(
        surrogateKey(["wwi_customer_id", "buying_group_code", "membership_from"]).alias(
            "customer_buying_group_bridge_key"
        ),
        "wwi_customer_id",
        "buying_group_code",
        F.coalesce(F.col("customer_key"), F.lit(UNKNOWN_MEMBER_KEY)).cast("bigint").alias("customer_key"),
        F.coalesce(F.col("buying_group_key"), F.lit(UNKNOWN_MEMBER_KEY)).cast("bigint").alias("buying_group_key"),
        "membership_from",
        "membership_to",
        (F.col("membership_to") >= F.current_date()).alias("is_current_membership"),
        "is_primary_affiliation",
        "allocation_factor",
        "allocation_basis_code",
        "allocation_category_scope",
        "allocation_reviewed_on",
        "allocation_reviewed_by",
        "rebate_eligible",
        "rebate_agreement_reference",
        "region_code",
        F.lit(SOURCE_SYSTEM_CODE).alias("source_system_code"),
        "source_membership_reference",
        F.lit("PASS").alias("dq_status_code"),
        hashColumns(
            ("wwi_customer_id", "buying_group_code", "membership_from", "membership_to", "allocation_factor")
        ).alias("row_hash"),
        F.lit(cfg.batchId).cast("bigint").alias("batch_id"),
        F.current_timestamp().alias("loaded_at_utc"),
    )


def optionalBronze(spark: SparkSession, cfg: PipelineConfig, table: str) -> DataFrame | None:
    fqn = cfg.fqn("bronze", table)
    return spark.table(fqn) if tableExists(spark, fqn) else None


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    batchId = cfg.batchId
    territories = readTable(spark, cfg, "bronze", BRONZE_SALES_TERRITORIES)

    channel = buildDimSalesChannel(readTable(spark, cfg, "bronze", BRONZE_SALES_CHANNELS), batchId)
    overwriteTable(
        withUnknownMember(
            spark,
            channel,
            "sales_channel_key",
            {
                "sales_channel_code": "UNKNOWN",
                "sales_channel_name": "Unknown",
                "region_code": "NA",
                "channel_status_code": "UNKNOWN",
            },
        ),
        cfg.fqn("silver", DIM_SALES_CHANNEL_TABLE),
    )

    territory = buildDimSalesTerritory(territories, batchId)
    overwriteTable(
        withUnknownMember(
            spark,
            territory,
            "sales_territory_key",
            {"sales_territory_code": "UNKNOWN", "sales_territory_name": "Unknown", "region_code": "GLOBAL"},
        ),
        cfg.fqn("silver", DIM_SALES_TERRITORY_TABLE),
    )

    salesperson = buildSalespersonSource(
        readTable(spark, cfg, "bronze", BRONZE_PEOPLE),
        readTable(spark, cfg, "bronze", BRONZE_SALES_TEAMS),
        readTable(spark, cfg, "bronze", BRONZE_SALES_TEAM_MEMBERS),
        territories,
        batchId,
    )
    mergeScd2(
        spark,
        cfg.fqn("silver", DIM_SALESPERSON_TABLE),
        salesperson,
        "salesperson_key",
        "wwi_employee_id",
        {"salesperson_code": "UNKNOWN", "salesperson_name": "Unknown"},
    )

    buyingGroup = buildDimBuyingGroup(readTable(spark, cfg, "bronze", BRONZE_BUYING_GROUPS), batchId)
    dimBuyingGroupFqn = cfg.fqn("silver", DIM_BUYING_GROUP_TABLE)
    overwriteTable(
        withUnknownMember(
            spark, buyingGroup, "buying_group_key", {"buying_group_code": "UNKNOWN", "buying_group_name": "Unknown"}
        ),
        dimBuyingGroupFqn,
    )

    dimCustomerFqn = cfg.fqn("silver", "dim_customer")
    bridge = buildBridge(
        spark,
        cfg,
        optionalBronze(spark, cfg, BRONZE_BUYING_GROUP_MEMBERSHIP),
        readTable(spark, cfg, "silver", CUSTOMER_TABLE),
        spark.table(dimBuyingGroupFqn),
        spark.table(dimCustomerFqn) if tableExists(spark, dimCustomerFqn) else None,
    )
    overwriteTable(bridge, cfg.fqn("silver", BRIDGE_CUSTOMER_BUYING_GROUP_TABLE))
