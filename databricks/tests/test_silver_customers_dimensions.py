"""Sales dimensions: PILOT commissionability, territory parent quirk,
salesperson SCD2, buying-group bridge allocation normalisation + quarantine."""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

from sales_lakehouse.common.quality import REJECTED_ROWS_TABLE
from sales_lakehouse.silver import dimensions as dm

HIGH = datetime(9999, 12, 31, 23, 59, 59)
T0 = datetime(2020, 1, 1)


def channelRows(spark):
    schema = (
        "SalesChannelID int, ChannelCode string, ChannelName string, ChannelClass string, RegionCode string, "
        "ChannelStatus string, PartnerIdentifier string, DefaultPriceListCode string, CommissionModifierPercent decimal(5,2), "
        "RequiresManualApproval boolean, OrderPrefix string, ValidFromDate date, ValidToDate date, LastEditedBy int, LastEditedWhen timestamp"
    )
    d = Decimal
    return spark.createDataFrame(
        [
            (
                1,
                "web",
                "Web Store",
                "DIGITAL",
                "NA",
                "ACTIVE",
                None,
                None,
                d("0.00"),
                False,
                "WEB",
                date(2013, 1, 1),
                None,
                1,
                T0,
            ),
            (
                2,
                "mkt-au",
                "Marketplace AU",
                "MKTPL",
                "APAC",
                "PILOT",
                "PARTNER-1",
                None,
                d("0.00"),
                False,
                "MKT",
                date(2019, 1, 1),
                None,
                1,
                T0,
            ),
            (
                3,
                "fax",
                "Fax Orders",
                "DIRECT",
                "NA",
                "CLOSED",
                None,
                None,
                d("0.00"),
                False,
                "FAX",
                date(2013, 1, 1),
                date(2015, 1, 1),
                1,
                T0,
            ),
            (
                3,
                "fax",
                "Fax Orders (old name)",
                "DIRECT",
                "NA",
                "ACTIVE",
                None,
                None,
                d("0.00"),
                False,
                "FAX",
                date(2013, 1, 1),
                None,
                1,
                datetime(2014, 1, 1),
            ),
        ],
        schema,
    )


def test_dim_sales_channel_status_overload(spark):
    out = {r.sales_channel_code: r for r in dm.buildDimSalesChannel(channelRows(spark), 1).collect()}
    assert set(out) == {"WEB", "MKT-AU", "FAX"}
    web, pilot, closed = out["WEB"], out["MKT-AU"], out["FAX"]
    assert web.channel_status_code == "ACTIVE" and web.is_commissionable is True and web.is_orderable is True
    # LEGACY QUIRK: PILOT is orderable but not commissionable; status carried verbatim
    assert pilot.channel_status_code == "PILOT" and pilot.is_commissionable is False and pilot.is_orderable is True
    assert pilot.dq_status_code == "WARN"  # marketplace without commission percentage
    assert closed.is_orderable is False and closed.is_commissionable is True and closed.is_active is False
    assert closed.sales_channel_name == "Fax Orders"  # latest LastEditedWhen wins
    assert len({r.sales_channel_key for r in out.values()}) == 3


def territoryRows(spark):
    schema = (
        "SalesTerritoryID int, TerritoryCode string, TerritoryName string, ParentTerritoryID int, TerritoryLevel int, RegionCode string, "
        "CountryISO3 string, TaxRegimeCode string, FiscalCalendarCode string, ReportingCurrencyCode string, PostalStandardCode string, "
        "ManagerPersonID int, IsActive boolean, LastEditedBy int, LastEditedWhen timestamp"
    )
    return spark.createDataFrame(
        [
            (1, " na ", "North America", None, 1, "NA", "USA", "US_SALES", "FY_JUL", "USD", "ZIP5", 7, True, 1, T0),
            (2, "na-west", "NA West", 1, 2, "NA", "USA", "US_SALES", "FY_JUL", "USD", "ZIP5", 7, True, 1, T0),
            (3, "eu-orphan", "Orphan", 999, 2, "EU", "DEU", "EU_VAT", "FY_JAN", "EUR", "DE5", None, False, 1, T0),
        ],
        schema,
    )


def test_dim_sales_territory_parent_quirk_and_retire(spark):
    out = {r.sales_territory_code: r for r in dm.buildDimSalesTerritory(territoryRows(spark), 1).collect()}
    assert out["NA-WEST"].parent_territory_code == "NA"
    # LEGACY QUIRK: unknown parent -> NULL, not a reject
    assert out["EU-ORPHAN"].parent_territory_code is None and out["EU-ORPHAN"].parent_territory_id == 999
    assert out["EU-ORPHAN"].is_active is False and out["EU-ORPHAN"].retired_on is not None
    assert out["NA"].retired_on is None and out["NA"].territory_level_code == "TERR"


def peopleRows(spark, role="AE"):
    people = spark.createDataFrame(
        [
            (1, "Amy Trefl", "Amy", True, True, "amy@example.test", T0, HIGH),
            (2, "Not Sales", "Nope", True, False, "n@example.test", T0, HIGH),
            (3, "Hiroshi Sato", "Hiro", True, True, "h@example.test", T0, HIGH),
        ],
        "PersonID int, FullName string, PreferredName string, IsEmployee boolean, IsSalesperson boolean, EmailAddress string, ValidFrom timestamp, ValidTo timestamp",
    )
    teams = spark.createDataFrame(
        [
            (10, "na-west-1", "NA West Team", None, 2, "NA", "FIELD", 7, None, date(2013, 1, 1), None, True, 1, T0),
            (11, "jp-1", "Japan Team", None, 4, "APAC", "INSIDE", 8, None, date(2013, 1, 1), None, True, 1, T0),
        ],
        "SalesTeamID int, TeamCode string, TeamName string, ParentSalesTeamID int, SalesTerritoryID int, RegionCode string, TeamType string, "
        "ManagerPersonID int, CostCentreCode string, FormedOnDate date, DisbandedOnDate date, IsActive boolean, LastEditedBy int, LastEditedWhen timestamp",
    )
    members = spark.createDataFrame(
        [
            (100, 10, 1, role, 5, Decimal("100.00"), date(2019, 1, 1), None, True, None, None, 1, T0),
            (101, 11, 3, "KAM", 6, Decimal("50.00"), date(2019, 1, 1), None, True, None, None, 1, T0),
        ],
        "SalesTeamMemberID long, SalesTeamID int, PersonID int, RoleCode string, CommissionPlanID int, QuotaSharePercent decimal(5,2), "
        "ValidFrom date, ValidTo date, IsPrimaryAssignment boolean, ReplacedBySalesTeamMemberID long, ChangeReasonCode string, LastEditedBy int, LastEditedWhen timestamp",
    )
    territories = territoryRows(spark).unionByName(
        spark.createDataFrame(
            [(4, "jp", "Japan", None, 1, "APAC", "JPN", "JP_CT", "FY_APR", "JPY", "JP7", 8, True, 1, T0)],
            territoryRows(spark).schema,
        )
    )
    return people, teams, members, territories


def test_dim_salesperson_scd2(spark, cfg):
    fqn = cfg.fqn("silver", dm.DIM_SALESPERSON_TABLE)
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")
    src = dm.buildSalespersonSource(*peopleRows(spark), batchId=1)
    rows = {r.wwi_employee_id: r for r in src.collect()}
    assert set(rows) == {1, 3}  # IsSalesperson only
    assert rows[1].region_code == "NA" and rows[1].na_accelerator_threshold_pct == Decimal("100.0000")
    assert rows[1].eu_commission_cap_pct is None and rows[1].sales_territory_code == "NA-WEST"
    assert rows[3].apac_payout_frequency_code == "QTR" and rows[3].apac_budget_fx_rate == Decimal("1.000000")

    dm.mergeScd2(spark, fqn, src, "salesperson_key", "wwi_employee_id", {"salesperson_name": "Unknown"})
    dm.mergeScd2(spark, fqn, src, "salesperson_key", "wwi_employee_id", {"salesperson_name": "Unknown"})
    d = spark.table(fqn)
    assert d.filter("salesperson_key = -1").count() == 1
    assert d.filter("salesperson_key > 0").count() == 2

    # role change on employee 1 -> new version, old closed
    changed = dm.buildSalespersonSource(*peopleRows(spark, role="KAM"), batchId=2).withColumn(
        "valid_from", F.lit(datetime(2021, 1, 1)).cast("timestamp")
    )
    dm.mergeScd2(spark, fqn, changed, "salesperson_key", "wwi_employee_id", {"salesperson_name": "Unknown"})
    versions = {r.version_number: r for r in spark.table(fqn).filter("wwi_employee_id = 1").collect()}
    assert versions[1].is_current is False and versions[1].valid_to == datetime(2021, 1, 1)
    assert versions[2].is_current is True and versions[2].sales_role_code == "KAM"
    assert spark.table(fqn).filter("wwi_employee_id = 3").count() == 1


def membershipRows(spark, rows):
    schema = (
        "WWICustomerID int, BuyingGroupCode string, MembershipFrom date, MembershipTo date, IsPrimaryAffiliation boolean, "
        "AllocationFactor decimal(9,6), AllocationBasisCode string, AllocationCategoryScope string, AllocationReviewedOn date, "
        "AllocationReviewedBy string, RebateEligible boolean, RebateAgreementReference string, RegionCode string, SourceMembershipReference string"
    )
    return spark.createDataFrame(rows, schema)


def member(cid, code, factor, primary=False, region="NA", frm=date(2019, 1, 1), to=None, rebate=True):
    return (
        cid,
        code,
        frm,
        to,
        primary,
        Decimal(factor) if factor is not None else None,
        "CONTRACT",
        None,
        None,
        None,
        rebate,
        None,
        region,
        f"M-{cid}-{code}",
    )


def test_bridge_allocation_normalisation_and_quarantine(spark, cfg):
    groups = spark.createDataFrame(
        [(1, "Tailspin Toys", T0, HIGH), (2, "Wingtip Toys", T0, HIGH), (3, "Third Group", T0, HIGH)],
        "BuyingGroupID int, BuyingGroupName string, ValidFrom timestamp, ValidTo timestamp",
    )
    dimGroup = dm.buildDimBuyingGroup(groups, 1)
    assert {r.buying_group_code for r in dimGroup.collect()} == {"TAILSPIN_TOYS", "WINGTIP_TOYS", "THIRD_GROUP"}
    customer = spark.createDataFrame(
        [
            (1, 1, date(2013, 1, 1), "NA"),
            (2, None, date(2013, 1, 1), "NA"),
            (3, 2, date(2013, 1, 1), "EU"),
            (4, None, date(2013, 1, 1), "NA"),
            (5, None, date(2013, 1, 1), "NA"),
            (6, None, date(2013, 1, 1), "NA"),
            (7, 1, date(2013, 1, 1), "NA"),
        ],
        "wwi_customer_id int, buying_group_id int, account_opened_date date, region_code string",
    )
    membership = membershipRows(
        spark,
        [
            # customer 3: 0.6 / 0.6 -> normalised to 0.5 / 0.5; EU non-primary loses rebate
            member(3, "TAILSPIN_TOYS", "0.600000", primary=True, region="EU"),
            member(3, "WINGTIP_TOYS", "0.600000", region="EU"),
            # customer 4: all zero -> BRIDGE_ALLOCATION_ZERO
            member(4, "TAILSPIN_TOYS", "0"),
            member(4, "WINGTIP_TOYS", "0"),
            # customer 5: three-way equal split cannot be represented in decimal(9,6) -> BRIDGE_ALLOCATION_SUM
            member(5, "TAILSPIN_TOYS", "1"),
            member(5, "WINGTIP_TOYS", "1"),
            member(5, "THIRD_GROUP", "1"),
            # customer 6: out-of-range factor and unknown group
            member(6, "TAILSPIN_TOYS", "1.5"),
            member(6, "NOBODY", "1"),
            # customer 7 has an explicit row so no implicit one is added
            member(7, "TAILSPIN_TOYS", "1", primary=True),
        ],
    )
    rejectedFqn = cfg.fqn("quality", REJECTED_ROWS_TABLE)
    before = (
        spark.table(rejectedFqn).filter(F.col("source_table") == dm.BRIDGE_CUSTOMER_BUYING_GROUP_TABLE).count()
        if spark.catalog.tableExists(rejectedFqn)
        else 0
    )

    bridge = dm.buildBridge(spark, cfg, membership, customer, dimGroup, None)
    rows = bridge.collect()
    byCustomer = {}
    for r in rows:
        byCustomer.setdefault(r.wwi_customer_id, []).append(r)
    assert set(byCustomer) == {1, 3, 7}
    assert (
        byCustomer[1][0].allocation_factor == Decimal("1.000000")
        and byCustomer[1][0].source_membership_reference == "(implicit)"
    )
    assert byCustomer[1][0].buying_group_key > 0
    c3 = {r.buying_group_code: r for r in byCustomer[3]}
    assert c3["TAILSPIN_TOYS"].allocation_factor == Decimal("0.500000") and c3[
        "WINGTIP_TOYS"
    ].allocation_factor == Decimal("0.500000")
    assert c3["TAILSPIN_TOYS"].rebate_eligible is True and c3["WINGTIP_TOYS"].rebate_eligible is False
    assert len(byCustomer[7]) == 1
    for r in rows:
        assert (
            Decimal("0.999999")
            <= sum(x.allocation_factor for x in byCustomer[r.wwi_customer_id])
            <= Decimal("1.000001")
        )

    rejected = spark.table(rejectedFqn).filter(F.col("source_table") == dm.BRIDGE_CUSTOMER_BUYING_GROUP_TABLE)
    assert rejected.count() - before == 2 + 3 + 1 + 1
    codes = {r.rule_code for r in rejected.collect()}
    assert {
        "BRIDGE_ALLOCATION_ZERO",
        "BRIDGE_ALLOCATION_SUM",
        "BRIDGE_ALLOCATION_RANGE",
        "BRIDGE_UNKNOWN_GROUP",
    } <= codes
    sumRejects = rejected.filter("rule_code = 'BRIDGE_ALLOCATION_SUM'").collect()
    assert all('"wwi_customer_id":5' in r.row_json for r in sumRejects)
