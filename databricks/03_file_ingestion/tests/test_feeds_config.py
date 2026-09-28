"""The feed specifications must agree with config/landing-zone.yaml (the DTSX and YAML, not the generator docstrings)."""

import os

import yaml

from wwi_file_ingestion import feeds

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def loadLanding():
    with open(os.path.join(REPO_ROOT, "config", "landing-zone.yaml"), encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_feed_specs_match_landing_zone_yaml():
    landing = loadLanding()
    entries = {e["consumed_by"]: e for e in landing["directories"]["inbound"]["subdirectories"]}
    for spec in feeds.FEEDS.values():
        if spec.packageName == feeds.QUARANTINE_MALFORMED.packageName:
            continue
        entry = entries[spec.packageName]
        assert spec.landingPath == entry["path"]
        assert spec.filePattern == entry["pattern"]
        assert spec.encoding == entry["encoding"]
        assert spec.delimiter == entry["delimiter"]
        assert spec.header == entry["header"]
        assert spec.legacyObjectName == entry["raw_table"]


def test_legacy_to_delta_naming():
    assert feeds.PARTNER_SALES_NA.bronzeObjectName() == "bronze.raw_file_partner_sales"
    assert feeds.CARRIER_SCAN.bronzeObjectName() == "bronze.raw_file_carrier_scan"
    assert feeds.SUPPLIER_CATALOG.bronzeObjectName() == "bronze.raw_file_supplier_catalog"
    assert feeds.FX_OVERRIDE.bronzeObjectName() == "bronze.raw_file_fx_override"
    assert feeds.QUARANTINE_MALFORMED.bronzeObjectName() == "silver.err_rejected_file_row"


def test_volume_paths_are_catalog_parameterised():
    assert feeds.volumePath("wwi_dev", feeds.VOLUME_INBOUND, "partner/na") == "/Volumes/wwi_dev/bronze/inbound/partner/na"
    assert feeds.volumePath("wwi_prod", feeds.VOLUME_QUARANTINE) == "/Volumes/wwi_prod/bronze/quarantine"
    assert feeds.PARTNER_SALES_APAC.regionCode == "APAC"
    assert feeds.PARTNER_SALES_EU.regionCode == "EU"
    assert feeds.PARTNER_SALES_NA.regionCode == "NA"


def test_python_codecs_decode_the_declared_encodings():
    assert "caf\xe9".encode("cp1252").decode(feeds.PARTNER_SALES_NA.pythonCodec) == "caf\xe9"
    assert "\u00fc".encode("latin-1").decode(feeds.PARTNER_SALES_APAC.pythonCodec) == "\u00fc"
    assert feeds.PARTNER_SALES_EU.pythonCodec.lower().replace("-", "") == "utf8"
