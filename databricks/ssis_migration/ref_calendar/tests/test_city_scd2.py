from datetime import datetime

from pyspark.sql import functions as F

from ref_calendar import city_scd2

HIGH = datetime.fromisoformat("9999-12-31 23:59:59.999")


def geoRow(cityId, name, population, validFrom, validTo=None, rowNo=1, state="Washington", country="USA"):
    return {
        "record_kind": "OLTPCITY", "geography_id": cityId, "city_name": name, "state_province_cd": "WA", "state_province_name": state,
        "country_cd": country, "iso3_cd": country, "country_name": "United States", "continent": "North America", "sales_territory": "Far West",
        "region_name": "Americas", "sub_region_name": "Northern America", "population_num": population, "region_cd": "NA", "tax_jurisdiction_cd": None,
        "postal_format_code": "US5", "timezone_name": "America/Los_Angeles", "postal_cd": None,
        "valid_from": validFrom, "valid_to": validTo, "source_row_number": rowNo,
    }


def refCountry(spark):
    return spark.createDataFrame([("USA", "US", "NA")], "country_code_iso3 string, country_code string, region_code string")


GEO_SCHEMA = (
    "record_kind string, geography_id int, city_name string, state_province_cd string, state_province_name string, country_cd string, "
    "iso3_cd string, country_name string, continent string, sales_territory string, region_name string, sub_region_name string, "
    "population_num bigint, region_cd string, tax_jurisdiction_cd string, postal_format_code string, timezone_name string, postal_cd string, "
    "valid_from timestamp, valid_to timestamp, source_row_number int"
)


def stage(spark, rows):
    df = spark.createDataFrame([tuple(r[f] for f in [x.split()[0] for x in GEO_SCHEMA.split(", ")]) for r in rows], GEO_SCHEMA)
    return city_scd2.conformCityForDimension(df, refCountry(spark))


def test_reserved_members_and_initial_load(spark):
    staged = stage(spark, [geoRow(1, "Aaronsburg", 613, datetime(2013, 1, 1))])
    dim = city_scd2.applyCityScd2(spark, None, staged, batchId=1)
    keys = [r.city_key for r in dim.select("city_key").orderBy("city_key").collect()]
    assert keys == [-2, -1, 0, 1]
    row = dim.where("city_key = 1").first()
    assert row.wwi_city_id == 1 and row.is_current_row and row.version_number == 1 and row.country_code == "US"
    reserved = {r.city_key: r.city for r in dim.where("city_key <= 0").collect()}
    assert reserved == {-2: "Not Applicable", -1: "Unknown", 0: "Unknown"}


def test_dedup_keeps_most_populous_version(spark):
    staged = stage(spark, [geoRow(1, "Aaronsburg", 100, datetime(2013, 1, 1), rowNo=1), geoRow(1, "Aaronsburg", 900, datetime(2013, 1, 1), rowNo=2)])
    assert staged.count() == 1
    assert staged.first().latest_recorded_population == 900


def test_type2_change_closes_current_and_versions(spark):
    first = city_scd2.applyCityScd2(spark, None, stage(spark, [geoRow(1, "Aaronsburg", 613, datetime(2013, 1, 1))]), batchId=1)
    changed = stage(spark, [geoRow(1, "Aaronsburg", 700, datetime(2014, 7, 1, 16))])
    second = city_scd2.applyCityScd2(spark, first, changed, batchId=2)
    versions = second.where("wwi_city_id = 1").orderBy("version_number").collect()
    assert [v.version_number for v in versions] == [1, 2]
    assert versions[0].is_current_row is False and versions[0].valid_to == datetime(2014, 7, 1, 16)
    assert versions[1].is_current_row and versions[1].city_key == 2 and versions[1].latest_recorded_population == 700
    assert versions[1].valid_from == datetime(2014, 7, 1, 16)


def test_unchanged_row_is_left_alone(spark):
    staged = stage(spark, [geoRow(1, "Aaronsburg", 613, datetime(2013, 1, 1))])
    first = city_scd2.applyCityScd2(spark, None, staged, batchId=1)
    second = city_scd2.applyCityScd2(spark, first, staged, batchId=2)
    assert second.where("city_key > 0").count() == 1
    assert second.where("city_key = 1").first().last_load_batch_id == 1


def test_late_arriving_inferred_member_is_promoted_in_place(spark):
    first = city_scd2.applyCityScd2(spark, None, stage(spark, [geoRow(1, "Aaronsburg", 613, datetime(2013, 1, 1))]), batchId=1)
    withInferred = city_scd2.insertInferredCityMembers(spark, first, [1, 42], batchId=2, now=datetime(2015, 1, 1))
    inferred = withInferred.where("wwi_city_id = 42").first()
    assert inferred.is_inferred_member and inferred.city == "Unknown" and inferred.city_key == 2
    promoted = city_scd2.applyCityScd2(spark, withInferred, stage(spark, [geoRow(42, "Abbeville", 2688, datetime(2013, 1, 1))]), batchId=3)
    row = promoted.where("wwi_city_id = 42").collect()
    assert len(row) == 1 and row[0].city_key == 2 and row[0].city == "Abbeville" and row[0].is_inferred_member is False and row[0].is_current_row


def test_historical_versions_backfilled_as_closed_rows(spark):
    staged = stage(spark, [
        geoRow(1, "Aaronsburg", 613, datetime(2013, 1, 1), datetime(2014, 7, 1, 16)),
        geoRow(1, "Aaronsburg", 700, datetime(2014, 7, 1, 16)),
    ])
    dim = city_scd2.applyCityScd2(spark, None, staged, batchId=1)
    rows = dim.where("wwi_city_id = 1").orderBy("valid_from").collect()
    assert [r.is_current_row for r in rows] == [False, True]
    assert [r.version_number for r in rows] == [1, 2]
    assert dim.where("city_key > 0").agg(F.max("city_key")).first()[0] == 2
