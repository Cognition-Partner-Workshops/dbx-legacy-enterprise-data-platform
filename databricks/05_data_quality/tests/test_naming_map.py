import pytest

from dq_quality import naming_map


@pytest.mark.parametrize("legacy, expected", [
    ("stg.Customer", ("silver", "stg_customer")),
    ("raw.FilePartnerSales", ("bronze", "raw_file_partner_sales")),
    ("err.RejectedLookupFailure", ("silver", "err_rejected_lookup_failure")),
    ("[err].[RejectedFileRow]", ("silver", "err_rejected_file_row")),
    ("ref.Country", ("silver", "ref_country")),
    ("work.PaymentMatched", ("silver", "work_payment_matched")),
    ("Dimension.Customer", ("gold", "dim_customer")),
    ("Fact.Sale", ("gold", "fact_sale")),
    ("etl.DataQualityResult", ("etl", "data_quality_result")),
    ("etl.RowCountAudit", ("etl", "row_count_audit")),
])
def test_legacy_to_delta(legacy, expected):
    assert naming_map.legacyToDelta(legacy) == expected


def test_delta_table_uses_catalog():
    assert naming_map.deltaTable("wwi_dev", "stg.OrderLine") == "wwi_dev.silver.stg_order_line"
    assert naming_map.controlTable("wwi_prod", "data_quality_rule") == "wwi_prod.etl.data_quality_rule"


def test_unknown_schema_raises():
    with pytest.raises(ValueError):
        naming_map.legacyToDelta("dbo.Whatever")
