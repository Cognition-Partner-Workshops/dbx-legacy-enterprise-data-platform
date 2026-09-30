import os

from pyspark.sql import functions as F

from sales_performance.partner_files import FEEDS, processFeedFile, pythonParseFile

SAMPLES = os.path.join(os.path.dirname(__file__), "..", "samples")


def _sample(region, seq):
    spec = FEEDS[region]
    ext = "txt" if region == "APAC" else "csv"
    return spec, os.path.join(SAMPLES, spec.folder, f"{spec.filePattern}20160531_{seq}.{ext}")


def test_na_feed_parses_quantities_taxes_and_reconciles_footer(spark):
    spec, path = _sample("NA", "001")
    valid, rejects, summary = processFeedFile(spark, spec, path, 1)
    assert summary["controls_match"] and summary["detail_rows"] == 6 and summary["valid_rows"] == 5
    rows = {r["partner_order_ref"]: r for r in valid.collect()}
    assert str(rows["TX100002"]["quantity"]) == "1200.000" and str(rows["TX100002"]["gross_amount"]) == "600.0000"
    assert str(rows["TX100001"]["tax_amount"]) == "8.0000" and rows["TX100001"]["tax_treatment_code"] == "SALESTAX"
    assert rows["TX100001"]["transaction_date"].isoformat() == "2016-05-30"
    assert rows["TX100006"]["country_text"] == "CANADA"
    assert rejects.filter(F.col("reject_reason_code") == "MALFORMED").count() == 1  # negative quantity


def test_na_footer_mismatch_quarantines_file(spark):
    spec, path = _sample("NA", "002")
    _, _, summary = processFeedFile(spark, spec, path, 1)
    assert not summary["controls_match"] and summary["control_value"] == 5 and summary["landed_value"] == 2


def test_eu_feed_backs_out_vat_and_maps_consent(spark):
    spec, path = _sample("EU", "001")
    valid, rejects, summary = processFeedFile(spark, spec, path, 1)
    assert summary["controls_match"] and summary["valid_rows"] == 4
    rows = {r["partner_order_ref"]: r for r in valid.collect()}
    assert str(rows["R-5001"]["gross_amount"]) == "119.0000" and str(rows["R-5001"]["net_amount"]) == "100.0000"
    assert str(rows["R-5001"]["tax_amount"]) == "19.0000" and rows["R-5001"]["marketable_flag"] == "Y"
    assert rows["R-5002"]["marketable_flag"] == "N" and str(rows["R-5002"]["gross_amount"]) == "1210.0000"
    assert rows["R-5003"]["marketable_flag"] == "Y" and rows["R-5003"]["country_text"] == "UNITED KINGDOM"
    assert rejects.filter(F.col("source_row_number") == 6).count() == 1  # VAT number too short


def test_eu_unknown_record_type_quarantines_file(spark):
    spec, path = _sample("EU", "002")
    _, rejects, summary = processFeedFile(spark, spec, path, 1)
    assert not summary["controls_match"] and summary["unknown_rows"] == 1
    assert rejects.filter(F.col("reject_reason_code") == "UNKNOWN_RECORD_TYPE").count() == 1


def test_apac_feed_gst_postal_padding_and_latin1(spark):
    spec, path = _sample("APAC", "001")
    valid, rejects, summary = processFeedFile(spark, spec, path, 1)
    assert summary["controls_match"] and summary["detail_rows"] == 5 and summary["valid_rows"] == 4
    rows = {r["partner_order_ref"]: r for r in valid.collect()}
    assert str(rows["SL-7001"]["gross_amount"]) == "220.0000" and rows["SL-7001"]["tax_treatment_code"] == "GST"
    assert rows["SL-7002"]["postal_code"] == "000023" and rows["SL-7003"]["postal_code"] == "1040061"
    assert rows["SL-7001"]["outlet_name"] == "Sydney Café"
    assert rejects.count() == 1  # missing slip number


def test_apac_total_mismatch_quarantines_file(spark):
    spec, path = _sample("APAC", "002")
    _, _, summary = processFeedFile(spark, spec, path, 1)
    assert not summary["controls_match"]


def test_python_reparse_matches_spark_parse_for_every_sample(spark):
    for region in ("NA", "EU", "APAC"):
        for seq in ("001", "002"):
            spec, path = _sample(region, seq)
            valid, _, summary = processFeedFile(spark, spec, path, 1)
            expected = pythonParseFile(spec, path)
            if not summary["controls_match"]:
                assert expected == []
                continue
            got = sorted((r["partner_order_ref"], str(r["gross_amount"])) for r in valid.collect())
            assert got == sorted((e[2], f"{e[5]:.4f}") for e in expected)
