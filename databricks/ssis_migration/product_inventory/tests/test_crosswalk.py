from pyspark.sql import functions as F

from product_inventory.silver import buildProductCrosswalk


def test_gtin_match_takes_precedence_over_name_match(spark):
    stockItems = spark.createDataFrame(
        [
            (1, "Blue Widget Large", "5000000000001"),
            (2, "Red Widget Small", None),
            (3, "Tiny", None),
            (4, "Nothing Matches Here", ""),
        ],
        "stock_item_id int, stock_item_name string, barcode string",
    )
    products = spark.createDataFrame(
        [
            (100, "P100", "Blue Widget Large", "5000000000001"),
            (101, "P101", "Blue Widget Large", None),
            (102, "P102", "red widget   small", None),
            (103, "P103", "Tiny", None),
        ],
        "product_id long, product_code string, product_description string, gtin string",
    )
    result = buildProductCrosswalk(stockItems, products)
    rows = {r["stock_item_id"]: r for r in result.where(F.col("is_preferred_match") | F.col("product_id").isNull()).collect()}

    # GTIN wins even though a NAME candidate (101) also exists for the same description
    assert rows[1]["product_id"] == 100
    assert rows[1]["match_rule_code"] == "GTIN"
    assert rows[1]["survivorship_rank"] == 1
    # NAME match on the normalised key (upper-cased, whitespace removed)
    assert rows[2]["product_id"] == 102
    assert rows[2]["match_rule_code"] == "NAME"
    assert rows[2]["match_key"] == "REDWIDGETSMALL"
    # names shorter than 8 normalised characters are unmatchable
    assert rows[3]["match_status"] == "UNMATCHABLE"
    assert rows[3]["product_id"] is None
    # no candidate at all -> UNMATCHED reject
    assert rows[4]["match_status"] == "UNMATCHED"
