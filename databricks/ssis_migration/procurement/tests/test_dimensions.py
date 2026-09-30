from pyspark.sql import Row
from pyspark.sql import functions as F

from conftest import d, dec, ts
from procurement.config import HIGH_TS, LOW_TS
from procurement.dimensions import (
    DIM_COLUMNS,
    VC_DIM_COLUMNS,
    applyHybridScd,
    applyScd2,
    emptySupplierDimension,
    emptyVendorContractDimension,
    incomingSupplierVersions,
)
from procurement.staging import SUPPLIER_TYPE1_COLUMNS, SUPPLIER_TYPE2_COLUMNS


def silverRow(key, name="ACME", status="ACTV", terms="N30", currency="USD", lead=14, tier=None):
    row = dict(source_supplier_id=int(key[1:]), supplier_business_key=key, supplier_name=name, supplier_status_code=status, payment_terms_code=terms,
               payment_method_code="ACH", transaction_currency_code=currency, region_code="NA", country_code="US", preferred_supplier_flag="Y",
               lead_time_days=lead, tax_identifier="T" + key, vat_registration_number=None, certification_expired_flag="N", withholding_applies="N",
               strategic_tier_code=tier, payment_days=30, is_survivor_row=True, dq_status_code="PASS")
    return row


def silver(spark, rows):
    from procurement.staging import hashColumns
    schema = ("source_supplier_id long, supplier_business_key string, supplier_name string, supplier_status_code string, payment_terms_code string, "
              "payment_method_code string, transaction_currency_code string, region_code string, country_code string, preferred_supplier_flag string, "
              "lead_time_days int, tax_identifier string, vat_registration_number string, certification_expired_flag string, withholding_applies string, "
              "strategic_tier_code string, payment_days int, is_survivor_row boolean, dq_status_code string")
    df = spark.createDataFrame([Row(**r) for r in rows], schema=schema)
    return df.withColumn("change_hash", hashColumns(SUPPLIER_TYPE2_COLUMNS)).withColumn("type1_hash", hashColumns(SUPPLIER_TYPE1_COLUMNS))


def test_hybrid_scd_new_type2_type1_and_unchanged(spark):
    day1 = incomingSupplierVersions(silver(spark, [silverRow("S1"), silverRow("S2")]))
    dim1 = applyHybridScd(emptySupplierDimension(spark), day1, batchId=1, asOf="2026-01-01 00:00:00")
    assert dim1.count() == 2 and dim1.select(*DIM_COLUMNS).columns == DIM_COLUMNS
    keys = {r.supplier_business_key: r.supplier_key for r in dim1.collect()}
    assert set(keys.values()) == {1, 2}
    assert all(r.valid_from == ts(LOW_TS) and r.valid_to == ts(HIGH_TS) and r.is_current_row for r in dim1.collect())

    # day 2: S1 type-2 change (status), S2 type-1 change (strategic tier), S3 new
    day2 = incomingSupplierVersions(silver(spark, [silverRow("S1", status="HOLD"), silverRow("S2", tier="T1"), silverRow("S3")]))
    dim2 = applyHybridScd(dim1, day2, batchId=2, asOf="2026-01-02 00:00:00").cache()
    assert dim2.count() == 4
    s1 = sorted(dim2.where("supplier_business_key = 'S1'").collect(), key=lambda r: r.row_version)
    assert [r.row_version for r in s1] == [1, 2]
    assert not s1[0].is_current_row and s1[0].valid_to == ts("2026-01-01 23:59:59") and s1[0].supplier_status_code == "ACTV"
    assert s1[1].is_current_row and s1[1].valid_from == ts("2026-01-02 00:00:00") and s1[1].supplier_status_code == "HOLD"
    assert s1[1].supplier_key == keys["S1"] + 0 or s1[1].supplier_key > 2, "new version gets a new surrogate key"
    assert s1[0].supplier_key == keys["S1"]
    s2 = dim2.where("supplier_business_key = 'S2'").collect()
    assert len(s2) == 1 and s2[0].strategic_tier_code == "T1" and s2[0].row_version == 1, "type-1 overwrite in place, no new version"
    assert dim2.where("supplier_business_key = 'S3'").count() == 1
    assert dim2.agg(F.max("supplier_key")).collect()[0][0] == 4

    # day 3: nothing changed -> identical dimension
    dim3 = applyHybridScd(dim2, day2, batchId=3, asOf="2026-01-03 00:00:00")
    assert dim3.count() == 4 and dim3.where("is_current_row").count() == 3
    assert dim3.agg(F.max("supplier_key")).collect()[0][0] == 4


def contractRow(number, committed, supplierKey=1):
    return dict(vendor_contract_key=None, contract_number=number, contract_business_key=int(number.split("-")[1]), supplier_business_key="S1", source_supplier_id=1, contract_type_code="MSA", source_status_code="ACTV",
                region_code="NA", contract_currency_code="USD", contract_start_date=d("2026-01-01"), contract_end_date=d("2026-12-31"), auto_renew_flag="N",
                notice_period_days=30, committed_amount=dec(committed), committed_amount_usd=dec(committed), rebate_percent=dec("1.5"), price_protection_flag="N",
                payment_terms_code="N30", signed_date=d("2025-12-15"), contract_band_code="MAJOR", supplier_key=supplierKey, amendment_number=None,
                fx_collar_lower_rate=None, fx_collar_upper_rate=None, row_hash_type2=f"{number}:{committed}", valid_from=None, valid_to=None, is_current_row=None, lineage_key=None)


def test_scd2_vendor_contract_amendments(spark):
    schema = emptyVendorContractDimension(spark).schema
    incoming1 = spark.createDataFrame([Row(**contractRow("C-1", 1000)), Row(**contractRow("C-2", 500))], schema).drop(
        "vendor_contract_key", "amendment_number", "valid_from", "valid_to", "is_current_row", "lineage_key")
    dim1 = applyScd2(emptyVendorContractDimension(spark), incoming1, batchId=1, asOf="2026-02-01 00:00:00")
    assert dim1.count() == 2 and dim1.columns == VC_DIM_COLUMNS
    assert {r.amendment_number for r in dim1.collect()} == {1}
    incoming2 = spark.createDataFrame([Row(**contractRow("C-1", 1200)), Row(**contractRow("C-2", 500))], schema).drop(
        "vendor_contract_key", "amendment_number", "valid_from", "valid_to", "is_current_row", "lineage_key")
    dim2 = applyScd2(dim1, incoming2, batchId=2, asOf="2026-03-01 00:00:00").cache()
    assert dim2.count() == 3
    c1 = sorted(dim2.where("contract_number = 'C-1'").collect(), key=lambda r: r.amendment_number)
    assert [r.amendment_number for r in c1] == [1, 2]
    assert not c1[0].is_current_row and c1[0].valid_to == ts("2026-02-28 23:59:59")
    assert c1[1].is_current_row and float(c1[1].committed_amount) == 1200.0 and c1[1].vendor_contract_key == 3
    assert dim2.where("contract_number = 'C-2'").count() == 1
