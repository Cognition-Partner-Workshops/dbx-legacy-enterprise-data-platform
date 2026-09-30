from ref_calendar import dimensions


def test_unknown_members_follow_registry_conventions(spark):
    registry = spark.createDataFrame(
        [("Dimension.Payment Method", "Payment Method Key", "Type1", -2, 0), ("Dimension.City", "City Key", "Type2", -2, 0), ("Dimension.Date", "Date", "Static", 0, 0), ("Dimension.Customer", "Customer Key", "Type2", -2, 0)],
        "`Dimension Name` string, `Key Column Name` string, `SCD Pattern` string, `Reserved Key Low` int, `Reserved Key High` int",
    )
    out = dimensions.buildUnknownMembers(spark, registry)
    byDim = {}
    for r in out.collect():
        byDim.setdefault(r.dimension_name, {})[r.member_key] = r.member_description
    assert byDim["Payment Method"] == {-1: "Unknown", -2: "Not Applicable"}
    assert set(byDim["City"]) == {-2, -1, 0}
    assert set(byDim["Date"]) == {19000101, 19000102}
    assert "Customer" not in byDim


def test_finalise_dimension_adds_key_zero_unknown_row(spark):
    df = spark.createDataFrame([("A", "Alpha"), ("B", "Beta")], "carrier_code string, carrier_name string")
    out = dimensions.finaliseDimension(spark, df, "carrier_key", ["carrier_code"], ["carrier_name"], batchId=1)
    rows = {r.carrier_key: r for r in out.collect()}
    assert sorted(rows) == [0, 1, 2]
    assert rows[0].carrier_name == "Unknown" and rows[1].carrier_code == "A"
