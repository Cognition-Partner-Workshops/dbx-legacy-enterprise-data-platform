from pyspark.sql import Row
from pyspark.sql import functions as F

from conftest import dec, ts
from procurement.quality import screenSuppliers
from procurement.staging import normalizeTaxId, transformSupplier

BRONZE_SUPPLIER_SCHEMA = (
    "supp_id long, supp_nbr string, supp_name string, supp_status_cd string, approval_status_cd string, tax_id_nbr string, vat_reg_nbr string, "
    "payment_terms_cd string, payment_method_cd string, default_curr_cd string, lead_time_days int, region_cd string, country_cd string, "
    "preferred_supplier_flg string, strategic_tier_cd string, certification_expired_flag string, withholding_applies string, quality_cert_cd string, "
    "cert_expiry_dt timestamp, updated_dt timestamp, package_execution_id long"
)


def bronzeSupplier(spark, rows):
    base = dict(supp_id=1, supp_nbr="S1", supp_name=" acme ", supp_status_cd="AC", approval_status_cd="APPR", tax_id_nbr="12-345 6789", vat_reg_nbr=None,
                payment_terms_cd="N30", payment_method_cd="ACH", default_curr_cd=None, lead_time_days=None, region_cd=None, country_cd="US",
                preferred_supplier_flg="Y", strategic_tier_cd=None, certification_expired_flag="N", withholding_applies="N", quality_cert_cd=None,
                cert_expiry_dt=None, updated_dt=ts("2026-01-01 00:00:00"), package_execution_id=1)
    return spark.createDataFrame([Row(**{**base, **r}) for r in rows], schema=BRONZE_SUPPLIER_SCHEMA)


def paymentTerms(spark):
    return spark.createDataFrame([Row(PAYMENT_TERMS_CD="N30", TERMS_DESC="Net 30", NET_DAYS=30), Row(PAYMENT_TERMS_CD="NET30", TERMS_DESC="Net 30", NET_DAYS=30)])


def test_supplier_normalisation_defaults_and_survivorship(spark):
    bronze = bronzeSupplier(spark, [
        dict(supp_id=1, supp_nbr="s1", updated_dt=ts("2026-01-01 00:00:00"), supp_name="old name"),
        dict(supp_id=1, supp_nbr="S1 ", updated_dt=ts("2026-01-02 00:00:00"), supp_name=" New Name "),
        dict(supp_id=2, supp_nbr="S2", payment_terms_cd="XX"),
    ])
    good, rejects = transformSupplier(bronze, paymentTerms(spark), batchId=1)
    survivors = {r.supplier_business_key: r for r in good.where("is_survivor_row").collect()}
    assert set(survivors) == {"S1"}, "unknown payment terms are a lookup reject (SSIS NoMatchBehavior=error), not a survivor"
    assert survivors["S1"].supplier_name == "NEW NAME", "latest source_modified_date wins"
    assert survivors["S1"].tax_identifier == "123456789"
    assert survivors["S1"].transaction_currency_code == "USD" and survivors["S1"].region_code == "NA" and survivors["S1"].lead_time_days == 14
    assert survivors["S1"].payment_days == 30
    assert good.where("supplier_business_key = 'S1'").count() == 2
    assert rejects.count() == 1 and rejects.collect()[0].reject_reason_code == "UNKNOWN_PAYMENT_TERMS"


def test_normalize_tax_id_missing_becomes_none(spark):
    df = spark.createDataFrame([Row(t=None), Row(t="  "), Row(t="de-12 3")], schema="t string").select(normalizeTaxId(F.col("t")).alias("n"))
    assert [r.n for r in df.collect()] == ["NONE", "NONE", "DE123"]


def test_supplier_screen_duplicate_and_missing_tax_id(spark):
    bronze = bronzeSupplier(spark, [
        dict(supp_id=1, supp_nbr="S1", tax_id_nbr="DE123456789", country_cd="DE", updated_dt=ts("2026-01-01 00:00:00")),
        dict(supp_id=2, supp_nbr="S2", tax_id_nbr="de 123456789", country_cd="DE", updated_dt=ts("2026-01-02 00:00:00")),  # duplicate (later) -> reject
        dict(supp_id=3, supp_nbr="S3", tax_id_nbr=None, supp_status_cd="AC"),  # missing tax id -> reject
        dict(supp_id=4, supp_nbr="S4", tax_id_nbr=None, supp_status_cd="PEND"),  # pending -> allowed
        dict(supp_id=5, supp_nbr="S5", tax_id_nbr="12", country_cd="FR", payment_terms_cd=None),  # EU tax shape invalid (null terms default to NET30) -> WARN
    ])
    good, stagingRejects = transformSupplier(bronze, paymentTerms(spark), batchId=1)
    assert {r.supplier_business_key: r.reject_reason_code for r in stagingRejects.collect()} == {"S3": "MISSING_TAX_ID"}
    passed, rejected, result = screenSuppliers(good, batchId=1)
    rejectedMap = {r.supplier_business_key: r.dq_reject_reason_code for r in rejected.collect()}
    assert rejectedMap == {"S2": "DUPLICATE_TAX_ID"}
    status = {r.supplier_business_key: r.dq_status_code for r in result.collect()}
    assert status == {"S1": "PASS", "S4": "PASS", "S5": "WARN"}
    assert dec("1") == dec(1)
