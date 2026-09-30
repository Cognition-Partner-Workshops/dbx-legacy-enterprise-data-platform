"""Run every pure transformation over empty frames that carry the real federated source schemas.

This guards the column-name and type assumptions of the library against the
legacy objects as exposed through Lakehouse Federation (see fixtures/source_schemas.json).
"""
from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F

from customer_party import (
    dedup,
    dim_customer,
    dim_supporting,
    extract,
    quality,
    scd,
    staging,
)
from customer_party.config import LOW_DATE

NOW = datetime(2026, 9, 30, tzinfo=timezone.utc)
ORA = "wwi_legacy_oracle.wwi_mdm."
OLTP = "wwi_legacy_oltp."


@pytest.fixture(scope="module")
def landed(spark, emptySource):
    """Bronze frames exactly as the EXT_* packages land them."""
    master = extract.transformCustomerMaster(
        emptySource(ORA + "cust_master"), emptySource(ORA + "cust_classification"), emptySource(ORA + "cust_credit_profile"), LOW_DATE, NOW, NOW
    ).withColumn("source_system_code", F.lit("ORA_ERP"))
    address = extract.withGeographyLookup(
        extract.transformCustomerAddress(emptySource(ORA + "cust_address"), LOW_DATE, NOW),
        spark.createDataFrame([], "geo_country string, geo_postal string"),
    ).withColumn("source_system_code", F.lit("ORA_ERP"))
    deletes = extract.detectDeletes(emptySource("wwi_legacy_oracle.wwi_audit.change_log"), "CUST_MASTER", LOW_DATE, NOW)
    segments = extract.toRawSqlOrderRows(
        extract.transformCustomerSegments(emptySource(OLTP + "Sales.CustomerSegmentAssignments"), emptySource(OLTP + "Sales.CustomerSegments"), NOW),
        "SEGMENT", "CustomerSegmentAssignmentID",
    )
    people = extract.toRawSqlOrderRows(extract.transformPeople(emptySource(OLTP + "Application.People")), "PERSON", "PersonID")
    territories = extract.toRawSqlOrderRows(
        extract.transformSalesTerritories(
            emptySource(OLTP + "Sales.SalesTerritories"), emptySource(OLTP + "Sales.SalesQuotas"), emptySource(OLTP + "Sales.CommissionPlans"), NOW
        ),
        "TERRITORY", "SalesTerritoryID",
    )
    promotions = extract.toRawSqlOrderRows(
        extract.transformPromotions(emptySource(OLTP + "Sales.Promotions"), emptySource(OLTP + "Sales.PromotionLines"), emptySource(OLTP + "Sales.PromotionRedemptions")),
        "PROMOTION", "PromotionID",
    )
    for df in (master, address, deletes, segments, people, territories, promotions):
        assert df.count() == 0
    return {"master": master, "address": address, "segments": segments, "people": people, "territories": territories, "promotions": promotions}


@pytest.fixture(scope="module")
def staged(spark, emptySource, landed):
    customers, _ = staging.splitValidCustomers(staging.transformStagedCustomer(landed["master"]))
    addresses, _ = staging.splitValidAddresses(staging.transformStagedAddress(landed["address"]))
    employees = staging.transformStagedEmployee(landed["people"], NOW)
    salespeople = staging.transformStagedSalesperson(
        landed["people"], emptySource(OLTP + "Sales.SalesQuotas"), spark.createDataFrame([], "currency_code string, average_rate decimal(18,6)"), NOW
    )
    promotions = staging.transformStagedPromotion(landed["promotions"])
    territories = staging.transformStagedTerritory(landed["territories"], emptySource("wwi_legacy_staging.ref.Country"))
    return {
        "customers": customers, "addresses": addresses, "employees": employees, "salespeople": salespeople,
        "promotions": promotions, "territories": territories,
    }


def test_customer_dedup_and_dq_chain(staged):
    dedupDf, standardized = dedup.deduplicateCustomers(staged["customers"], staged["addresses"])
    updated = dedup.applyDedupToStaging(staged["customers"], dedupDf)
    ruleResults = quality.evaluateRules(updated, F.lit(NOW).cast("timestamp"))
    screened = quality.applyDqOutcome(updated, ruleResults)
    assert screened.count() == 0 and standardized.count() == 0
    for region in dim_customer.REGIONS:
        candidates, rejected = dim_customer.buildRegionalCandidates(screened, standardized, region, NOW)
        assert list(candidates.columns) == list(dim_customer.DIM_COLUMNS)
        assert candidates.count() == 0 and rejected.count() == 0


def test_supporting_dimension_chain(spark, emptySource, landed, staged):
    categories = dim_supporting.buildCategoryCandidates(
        dim_supporting.stageCustomerCategories(emptySource(OLTP + "Sales.CustomerCategories"), emptySource(OLTP + "Sales.Customers"))
    )
    segments = dim_supporting.buildSegmentCandidates(dim_supporting.stageCustomerSegments(emptySource(OLTP + "Sales.CustomerSegments"), landed["segments"]))
    employees = dim_supporting.buildEmployeeCandidates(staged["employees"], NOW)
    territories = dim_supporting.buildTerritoryCandidates(staged["territories"])
    territoryDim = dim_supporting.reparentOrphans(scd.applyScd1(None, territories, dim_supporting.TERRITORY_SPEC))
    salespeople = dim_supporting.buildSalespersonCandidates(staged["salespeople"], territoryDim)
    promotions, rejectedPromotions = dim_supporting.buildPromotionCandidates(staged["promotions"], NOW)
    ts = F.lit(NOW).cast("timestamp")
    dims = {
        "category": scd.applyScd1(None, categories, dim_supporting.CATEGORY_SPEC),
        "segment": scd.applyScd2(None, segments, dim_supporting.SEGMENT_SPEC, ts),
        "employee": dim_supporting.repairManagerKeys(scd.applyScd2(None, employees, dim_supporting.EMPLOYEE_SPEC, ts)),
        "territory": territoryDim,
        "salesperson": scd.applyScd2(None, salespeople, dim_supporting.SALESPERSON_SPEC, ts),
        "promotion": dim_supporting.flagOverlappingCampaigns(scd.applyScd2(None, promotions, dim_supporting.PROMOTION_SPEC, ts)),
    }
    for name, df in dims.items():
        assert df.count() == 0, name
    assert rejectedPromotions.count() == 0
    bridge = dim_supporting.buildTerritoryBridge(dims["salesperson"], NOW)
    assert bridge.count() == 0
