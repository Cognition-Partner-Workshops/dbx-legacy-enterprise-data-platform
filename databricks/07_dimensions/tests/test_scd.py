from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from wwi_dimensions import keys, scd, specs, tables, unknown

T0 = datetime(2026, 1, 10, 3, 0, 0)
T1 = datetime(2026, 1, 11, 3, 0, 0)


def _supplier(spark, rows):
    return spark.createDataFrame(
        rows,
        "SupplierBusinessKey STRING, Supplier STRING, CategoryCode STRING, PaymentTermsCode STRING, PaymentDays INT, "
        "PrimaryContact STRING, QualityRating DECIMAL(5,2), SourceModifiedDate TIMESTAMP",
    )


def _current(spark, catalog, spec):
    return {r[spec.businessKeyColumn]: r for r in spark.table(spec.fullTableName(catalog)).where("IsCurrentRow AND SupplierKey > 0").collect()}


@pytest.mark.usefixtures("cleanGold")
def test_hybrid_scd_new_type2_type1_and_idempotent(spark, catalog):
    spec = specs.SUPPLIER
    src = _supplier(spark, [
        ("SUP-1", "Acme", "PKG", "N30", 30, "Ann", Decimal("4.50"), T0),
        ("SUP-2", "Bolt", "RAW", "N60", 60, "Bob", Decimal("3.00"), T0),
        ("SUP-2", "Bolt Ltd", "RAW", "N60", 60, "Bob", Decimal("3.00"), datetime(2026, 1, 9)),  # older duplicate: dropped
    ])
    r = scd.applyScd(spark, catalog, spec, src, batchId=1, packageExecutionId=11, loadTimestamp=T0)
    assert (r.rowsRead, r.rowsInserted, r.rowsType2Versioned, r.rowsType1Updated) == (3, 2, 0, 0)
    cur = _current(spark, catalog, spec)
    assert cur["SUP-2"]["Supplier"] == "Bolt"                      # latest SourceModifiedDate wins
    assert {cur["SUP-1"]["SupplierKey"], cur["SUP-2"]["SupplierKey"]} == {1, 2}   # positive keys from 1 (reserved -9..0 untouched)
    assert cur["SUP-1"]["VersionNumber"] == 1 and cur["SUP-1"]["LineageKey"] == 11

    # Rerun of the same batch: nothing changes
    r2 = scd.applyScd(spark, catalog, spec, src, batchId=1, packageExecutionId=11, loadTimestamp=T0)
    assert r2.rowsWritten == 0 and r2.rowsUpdated == 0 and r2.rowsUnchanged == 2
    assert spark.table(spec.fullTableName(catalog)).count() == 2

    # Type 2 (PaymentTermsCode) on SUP-1, Type 1 (PrimaryContact) on SUP-2
    src2 = _supplier(spark, [
        ("SUP-1", "Acme", "PKG", "N60", 60, "Ann", Decimal("4.50"), T1),
        ("SUP-2", "Bolt", "RAW", "N60", 60, "Bobby", Decimal("3.00"), T1),
    ])
    r3 = scd.applyScd(spark, catalog, spec, src2, batchId=2, packageExecutionId=12, loadTimestamp=T1)
    assert (r3.rowsType2Versioned, r3.rowsClosedOut, r3.rowsType1Updated, r3.rowsInserted) == (1, 1, 1, 0)
    rows = spark.table(spec.fullTableName(catalog)).where("SupplierBusinessKey = 'SUP-1'").orderBy("VersionNumber").collect()
    assert [x["VersionNumber"] for x in rows] == [1, 2]
    assert rows[0]["IsCurrentRow"] is False and rows[0]["ValidTo"] == T1 and rows[0]["EffectiveTo"] == T1
    assert rows[1]["IsCurrentRow"] is True and rows[1]["ValidFrom"] == T1 and rows[1]["SupplierKey"] == 3
    assert rows[1]["ValidTo"].year == 9999
    cur = _current(spark, catalog, spec)
    assert cur["SUP-2"]["PrimaryContact"] == "Bobby" and cur["SUP-2"]["VersionNumber"] == 1


@pytest.mark.usefixtures("cleanGold")
def test_type1_writes_through_history_and_same_day_sequence(spark, catalog):
    spec = specs.SUPPLIER
    scd.applyScd(spark, catalog, spec, _supplier(spark, [("SUP-1", "Acme", "PKG", "N30", 30, "Ann", Decimal("4.50"), T0)]), 1, 11, loadTimestamp=T0)
    # type 2 change dated same day -> EffectiveSequence 2
    sameDay = datetime(2026, 1, 10, 9, 0, 0)
    scd.applyScd(spark, catalog, spec, _supplier(spark, [("SUP-1", "Acme", "PKG", "N45", 45, "Ann", Decimal("4.50"), sameDay)]), 2, 12, loadTimestamp=sameDay)
    # type 1 change: contact renamed -> both versions updated
    scd.applyScd(spark, catalog, spec, _supplier(spark, [("SUP-1", "Acme", "PKG", "N45", 45, "Annie", Decimal("4.50"), T1)]), 3, 13, loadTimestamp=T1)
    rows = spark.table(spec.fullTableName(catalog)).where("SupplierKey > 0").orderBy("VersionNumber").collect()
    assert [x["PrimaryContact"] for x in rows] == ["Annie", "Annie"]
    assert [x["EffectiveSequence"] for x in rows] == [1, 2]
    assert [x["IsCurrentRow"] for x in rows] == [False, True]
    assert rows[0]["EffectiveTo"] == sameDay and rows[1]["EffectiveFrom"] == sameDay


@pytest.mark.usefixtures("cleanGold")
def test_scd1_overwrites_single_row(spark, catalog):
    spec = specs.CUSTOMER_CATEGORY
    df = spark.createDataFrame([("RET", "Retail", "B2C"), ("WHL", "Wholesale", "B2B")],
                               "CategoryCode STRING, CustomerCategory STRING, CategoryGroup STRING")
    r = scd.applyScd(spark, catalog, spec, df, 1, 11, loadTimestamp=T0)
    assert r.rowsInserted == 2
    df2 = spark.createDataFrame([("RET", "Retail Trade", "B2C"), ("WHL", "Wholesale", "B2B")],
                                "CategoryCode STRING, CustomerCategory STRING, CategoryGroup STRING")
    r2 = scd.applyScd(spark, catalog, spec, df2, 2, 12, loadTimestamp=T1)
    assert (r2.rowsType1Updated, r2.rowsUnchanged, r2.rowsInserted) == (1, 1, 0)
    rows = {x["CategoryCode"]: x for x in spark.table(spec.fullTableName(catalog)).collect()}
    assert len(rows) == 2 and rows["RET"]["CustomerCategory"] == "Retail Trade" and rows["RET"]["LastLoadBatchId"] == 2


@pytest.mark.usefixtures("cleanGold")
def test_unknown_members_and_key_registry(spark, catalog):
    spec = specs.CUSTOMER
    tables.ensureDimensionTable(spark, catalog, spec)
    assert unknown.ensureUnknownMembers(spark, catalog, spec) == 5      # -1 -2 -3 -4 -9 (supports inferred)
    assert unknown.ensureUnknownMembers(spark, catalog, spec) == 0      # rerunnable
    rows = {r["CustomerKey"]: r for r in spark.table(spec.fullTableName(catalog)).collect()}
    assert set(rows) == {-1, -2, -3, -4, -9}
    assert rows[-1]["Customer"] == "Unknown" and rows[-1]["PostalCode"] == "N/A" and rows[-1]["RegionCode"] == "GLOBAL"
    assert rows[-1]["WWICustomerID"] == -1 and rows[-1]["LineageKey"] == 0 and rows[-1]["IsCurrentRow"] is True
    assert rows[-2]["Customer"] == "Not Applicable" and rows[-9]["Customer"] == "Error"
    assert rows[-1]["ValidFrom"].year == 1900 and rows[-1]["ValidTo"].year == 9999

    tables.ensureDimensionTable(spark, catalog, specs.EMPLOYEE)
    assert unknown.ensureUnknownMembers(spark, catalog, specs.EMPLOYEE) == 4  # no -4 without inferred support

    keys.ensureKeyRegistry(spark, catalog)
    first, last = keys.allocateKeyRange(spark, catalog, "Customer", 3, "test")
    assert (first, last) == (1, 3)
    assert keys.allocateKeyRange(spark, catalog, "Customer", 0, "test") == (4, 4)
    reg = spark.table(keys.registryTableName(catalog)).where("DimensionName = 'Customer'").collect()[0]
    assert reg["NextKey"] == 5 and reg["ReservedKeyLow"] == -9 and reg["ReservedKeyHigh"] == 0 and reg["SupportsInferred"] is True


@pytest.mark.usefixtures("cleanGold")
def test_inferred_member_then_enrichment(spark, catalog):
    spec = specs.SUPPLIER
    tables.ensureDimensionTable(spark, catalog, spec)
    unknown.ensureUnknownMembers(spark, catalog, spec)
    stubs = scd.insertInferredMembers(spark, catalog, spec, spark.createDataFrame([("SUP-9",)], "SupplierBusinessKey STRING"), 1, 11, loadTimestamp=T0)
    stub = stubs.collect()[0]
    assert stub["SupplierKey"] == 1 and stub["IsInferredMember"] is True
    row = spark.table(spec.fullTableName(catalog)).where("SupplierKey = 1").collect()[0]
    assert row["Supplier"] == "Inferred: SUP-9" and row["InferredCreatedOn"] == T0
    # second call for the same key does not create another stub
    assert scd.insertInferredMembers(spark, catalog, spec, spark.createDataFrame([("SUP-9",)], "SupplierBusinessKey STRING"), 1, 11, loadTimestamp=T0).count() == 1
    assert spark.table(spec.fullTableName(catalog)).where("SupplierKey > 0").count() == 1

    r = scd.applyScd(spark, catalog, spec, _supplier(spark, [("SUP-9", "Zed", "PKG", "N30", 30, "Zoe", Decimal("1.00"), T1)]), 2, 12, loadTimestamp=T1)
    assert r.rowsInferredEnriched == 1 and r.rowsWritten == 0
    row = spark.table(spec.fullTableName(catalog)).where("SupplierKey = 1").collect()[0]
    assert row["Supplier"] == "Zed" and row["IsInferredMember"] is False and row["EnrichedOn"] == T1 and row["VersionNumber"] == 1


@pytest.mark.usefixtures("cleanGold")
def test_expire_and_overwrite_helpers(spark, catalog):
    spec = specs.VENDOR_CONTRACT
    df = spark.createDataFrame(
        [("C-1", "C-1", 0, "Old deal", "2025-01-01", "2025-12-31", "ACTIVE"), ("C-2", "C-2", 0, "Live deal", "2026-01-01", "2027-12-31", "ACTIVE")],
        "ContractBusinessKey STRING, ContractNumber STRING, AmendmentNumber INT, ContractTitle STRING, ContractStartDate STRING, ContractEndDate STRING, ContractStatusCode STRING",
    ).withColumn("ContractStartDate", F.to_date("ContractStartDate")).withColumn("ContractEndDate", F.to_date("ContractEndDate"))
    scd.applyScd(spark, catalog, spec, df, 1, 11, loadTimestamp=T0)
    n = scd.expireCurrentRows(spark, catalog, spec, "t.ContractEndDate < DATE '2026-01-10'", T0, 1, 11,
                              extraSetSql="t.ContractStatusCode = 'EXPIRED', t.ClosedByLineageKey = 11")
    assert n == 1
    rows = {r["ContractBusinessKey"]: r for r in spark.table(spec.fullTableName(catalog)).collect()}
    assert rows["C-1"]["IsCurrentRow"] is False and rows["C-1"]["ContractStatusCode"] == "EXPIRED" and rows["C-1"]["ValidTo"] == T0
    assert rows["C-2"]["IsCurrentRow"] is True
    n2 = scd.applyType1Overwrite(spark, catalog, spec, spark.createDataFrame([("C-2", "Renamed")], "ContractBusinessKey STRING, ContractTitle STRING"), ["ContractTitle"], 2, 12)
    assert n2 == 1
    assert spark.table(spec.fullTableName(catalog)).where("ContractBusinessKey = 'C-2'").collect()[0]["ContractTitle"] == "Renamed"


def test_row_hash_matches_tsql_shape(spark):
    df = spark.createDataFrame([("a", None, 3)], "x STRING, y STRING, z INT").withColumn("h", scd.rowHash(["x", "y", "z"]))
    expected = spark.sql("SELECT sha2('a||3', 256) AS h").collect()[0]["h"]
    assert df.collect()[0]["h"] == expected
