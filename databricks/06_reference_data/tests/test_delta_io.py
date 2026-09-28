import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T

from wwi_ref import delta_io, reserved, schemas


def _dim(spark, catalog, legacyName):
    schemas.ensureDimension(spark, catalog, legacyName)
    target = schemas.fqn(catalog, legacyName)
    spark.sql("DELETE FROM %s" % target)
    return target


def _carriers(spark, rows):
    schema = T.StructType([
        T.StructField("CarrierCode", T.StringType()), T.StructField("CarrierName", T.StringType()),
        T.StructField("RegionCode", T.StringType()), T.StructField("CrossBorderFlag", T.StringType()),
        T.StructField("OnTimeTargetDays", T.IntegerType()), T.StructField("OwnFleetFlag", T.StringType()),
        T.StructField("ChangeHash", T.StringType())])
    return spark.createDataFrame(rows, schema)


def test_scd1_merge_inserts_updates_deletes_and_keeps_keys(spark, catalog):
    target = _dim(spark, catalog, "Dimension.Carrier")
    assert reserved.seedReserved(spark, target, "CarrierKey", "REF_Load_Carrier") == 4
    first = _carriers(spark, [("DHL", "DHL", "EU", "Y", 2, "N", "h1"), ("UPS", "UPS", "NA", "N", 5, "N", "h2")])
    c1 = delta_io.publishScd1(spark, target, first, "CarrierKey", ["CarrierCode", "RegionCode"], "REF_Load_Carrier", 1)
    assert (c1.inserted, c1.updated, c1.deleted) == (2, 0, 0)
    keys = {r["CarrierCode"]: r["CarrierKey"] for r in spark.table(target).where("CarrierKey > 0").collect()}
    assert set(keys.values()) == {1, 2}

    second = _carriers(spark, [("DHL", "DHL Express", "EU", "Y", 1, "N", "h1b"), ("FDX", "FedEx", "NA", "Y", 2, "N", "h3")])
    c2 = delta_io.publishScd1(spark, target, second, "CarrierKey", ["CarrierCode", "RegionCode"], "REF_Load_Carrier", 2)
    assert (c2.inserted, c2.updated, c2.deleted) == (1, 1, 1)
    rows = {r["CarrierCode"]: r for r in spark.table(target).where("CarrierKey > 0").collect()}
    assert set(rows) == {"DHL", "FDX"}
    assert rows["DHL"]["CarrierKey"] == keys["DHL"] and rows["DHL"]["CarrierName"] == "DHL Express" and rows["DHL"]["LastLoadBatchId"] == 2
    assert rows["FDX"]["CarrierKey"] == 3
    assert {r["CarrierKey"] for r in spark.table(target).where("CarrierKey < 0").collect()} == {-1, -2, -3, -9}

    c3 = delta_io.publishScd1(spark, target, second, "CarrierKey", ["CarrierCode", "RegionCode"], "REF_Load_Carrier", 3)
    assert (c3.inserted, c3.updated, c3.deleted) == (0, 0, 0)
    assert reserved.seedReserved(spark, target, "CarrierKey", "REF_Load_Carrier") == 0


def test_scd2_versions_only_on_hash_change(spark, catalog):
    target = _dim(spark, catalog, "Dimension.Cost Center")
    cols = ["CostCenterCode", "CostCenterName", "RegionCode", "ParentCostCenterCode", "CompanyCode", "FunctionCode", "SuspenseFlag", "RowHashType2"]
    v1 = spark.createDataFrame([("A100", "Alpha", "NA", "CORP", "0001", "A1", "N", "h1")], cols)
    c1 = delta_io.publishScd2(spark, target, v1, "CostCenterKey", ["CostCenterCode", "RegionCode"], "REF_Load_CostCenter", 1,
                              datetime.date(2024, 1, 1))
    assert (c1.inserted, c1.updated) == (1, 0)
    same = delta_io.publishScd2(spark, target, v1, "CostCenterKey", ["CostCenterCode", "RegionCode"], "REF_Load_CostCenter", 2,
                                datetime.date(2024, 1, 8))
    assert (same.inserted, same.updated) == (0, 0)
    v2 = spark.createDataFrame([("A100", "Alpha Renamed", "NA", "CORP", "0001", "A1", "N", "h2")], cols)
    c2 = delta_io.publishScd2(spark, target, v2, "CostCenterKey", ["CostCenterCode", "RegionCode"], "REF_Load_CostCenter", 3,
                              datetime.date(2024, 1, 15))
    assert (c2.inserted, c2.updated) == (1, 1)
    rows = spark.table(target).orderBy("VersionNumber").collect()
    assert [r["VersionNumber"] for r in rows] == [1, 2]
    assert rows[0]["IsCurrentRow"] is False and str(rows[0]["EffectiveTo"]) == "2024-01-14 23:59:59"
    assert rows[1]["IsCurrentRow"] is True and str(rows[1]["EffectiveFrom"]) == "2024-01-15 00:00:00"
    assert rows[1]["CostCenterKey"] != rows[0]["CostCenterKey"] and rows[1]["CostCenterName"] == "Alpha Renamed"

    gone = delta_io.publishScd2(spark, target, v2.where("1 = 0"), "CostCenterKey", ["CostCenterCode", "RegionCode"], "REF_Load_CostCenter", 4,
                                datetime.date(2024, 2, 1))
    assert (gone.inserted, gone.updated) == (0, 1)
    assert spark.table(target).where("IsCurrentRow").count() == 0


def test_merge_reference_is_idempotent(spark, catalog):
    target = "%s.silver.tmp_merge_ref" % catalog
    spark.sql("DROP TABLE IF EXISTS %s" % target)
    spark.sql("CREATE TABLE %s (K STRING, V STRING, N INT, Owner STRING) USING DELTA" % target)
    df = spark.createDataFrame([("a", "1", 1, "grid"), ("b", "2", 2, "grid"), ("s", "9", 9, "steward")], ["K", "V", "N", "Owner"])
    c1 = delta_io.mergeReference(spark, target, df, ["K"])
    c2 = delta_io.mergeReference(spark, target, df.withColumn("V", F.lit("x")), ["K"], updateCols=["V"])
    assert (c1.inserted, c2.inserted, c2.updated) == (3, 0, 3)
    assert {r["V"] for r in spark.table(target).collect()} == {"x"}
    c3 = delta_io.mergeReference(spark, target, df, ["K"], insertOnly=True)
    assert (c3.inserted, c3.updated) == (0, 0)
    c4 = delta_io.mergeReference(spark, target, df.where("K = 'a'"), ["K"], notMatchedBySourceSql="t.Owner = 'grid'")
    assert c4.deleted == 1
    assert {r["K"] for r in spark.table(target).collect()} == {"a", "s"}
    assert delta_io.withSequence(spark, target, df.select(F.lit(None).cast("bigint").alias("N"), "K"), "N", ["K"]).agg(F.min("N")).first()[0] == 10
