from customer_engagement import PACKAGES, recon


def test_everyPackageHasExactlyOneReconSpec():
    units = [s.package for s in recon.SPECS]
    assert sorted(units) == sorted(PACKAGES) and len(units) == len(set(units)) == 13


def test_checksumIsOrderIndependent(spark):
    a = spark.createDataFrame([(1, "x"), (2, "y")], "k int, v string")
    b = spark.createDataFrame([(2, "y"), (1, "x")], "k int, v string")
    a.createOrReplaceTempView("recon_a")
    b.createOrReplaceTempView("recon_b")
    assert recon.countAndChecksum(spark, "recon_a", ["k", "v"]) == recon.countAndChecksum(spark, "recon_b", ["k", "v"])
    assert recon.countAndChecksum(spark, "recon_a", ["k", "v"], "k = 1")[0] == 1
