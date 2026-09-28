from pyspark.sql import functions as F


def test_delta_merge_available(spark, catalog):
    spark.sql("CREATE TABLE IF NOT EXISTS %s.silver.tmp_smoke (k INT, v STRING) USING DELTA" % catalog)
    spark.createDataFrame([(1, "a")], ["k", "v"]).createOrReplaceTempView("smoke_src")
    spark.sql("MERGE INTO %s.silver.tmp_smoke t USING smoke_src s ON t.k = s.k WHEN NOT MATCHED THEN INSERT *" % catalog)
    assert spark.table("%s.silver.tmp_smoke" % catalog).count() == 1
    assert spark.table("%s.silver.ref_region" % catalog).columns[0] == "RegionCode"
    assert "DateKey" in spark.table("%s.gold.dim_date" % catalog).columns
