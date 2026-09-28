from decimal import Decimal

from inv_common import transforms


def _demand(spark):
    cols = "StockItemId int, WarehouseSiteCode string, AverageDailyDemand decimal(18,4)"
    return spark.createDataFrame([(1, "LDN", Decimal("10.0")), (3, "SYD", Decimal("2.5")), (2, "LDN", Decimal("0.0"))], cols)


def test_source_safety_factor_and_discontinued_filter(currentPosition, stockItem, spark):
    src = {(r["StockItemId"], r["WarehouseSiteCode"]): r for r in transforms.buildReplenishmentSource(stockItem, currentPosition, _demand(spark)).collect()}
    assert (6, "LDN") not in {k for k in src}, "discontinued items excluded"
    assert float(src[(1, "LDN")]["SafetyFactor"]) == 1.1     # EU
    assert float(src[(3, "SYD")]["SafetyFactor"]) == 1.5     # APAC
    assert float(src[(2, "LDN")]["SafetyFactor"]) == 1.25    # everything else
    assert float(src[(5, "LDN")]["AverageDailyDemand"]) == 0.0, "missing demand -> 0"


def test_derived_quantities_match_ssis_expressions(currentPosition, stockItem, spark):
    df = transforms.deriveReplenishment(transforms.buildReplenishmentSource(stockItem, currentPosition, _demand(spark)), coverDays=21)
    rows = {(r["StockItemId"], r["WarehouseSiteCode"]): r for r in df.collect()}
    r1 = rows[(1, "LDN")]  # demand 10, lead 7, sf 1.1, onhand 100, onorder 10, alloc 20, outer 12
    assert r1["ReorderPoint"] == 77                     # (DT_I4)(10*7*1.1) = 77
    assert r1["ProjectedAvailable"] == 90
    assert float(r1["DaysOfCover"]) == 8.0              # (100-20)/10
    assert r1["RawSuggestedQuantity"] == 120            # 210 - 90
    assert r1["SuggestedQuantity"] == 132               # (120 div 12 + 1) * 12
    assert r1["IsStockoutRisk"] is False                # 90 <= 70 ? no
    r3 = rows[(3, "SYD")]  # demand 2.5, lead 10, onhand 40, alloc 0, onorder 0, outer 6
    assert r3["RawSuggestedQuantity"] == 12             # (DT_I4)(52.5)=52 - 40
    assert r3["SuggestedQuantity"] == 18                # (12 div 6 + 1) * 6
    assert r3["IsStockoutRisk"] is False                # 40 <= 25 ? no
    r2 = rows[(2, "LDN")]  # zero demand
    assert float(r2["DaysOfCover"]) == 999.0
    assert r2["RawSuggestedQuantity"] == 5 and r2["SuggestedQuantity"] == 5, "0 - (-5) with outer 1"
    assert r2["IsStockoutRisk"] is True   # -5 <= 0


def test_suggestions_filter_and_chiller_suppression(currentPosition, stockItem, spark):
    all_ = transforms.buildReplenishmentSuggestions(stockItem, currentPosition, _demand(spark), 21, batchId=3)
    assert {r["StockItemId"] for r in all_.collect()} == {1, 2, 3}
    noChiller = transforms.buildReplenishmentSuggestions(stockItem, currentPosition, _demand(spark), 21, batchId=3, suppressChiller=True)
    assert {r["StockItemId"] for r in noChiller.collect()} == {2, 3}


def test_inventory_health_aggregate(currentPosition, stockItem, spark, dimWarehouseSite):
    sugg = transforms.buildReplenishmentSuggestions(stockItem, currentPosition, _demand(spark), 21, batchId=3)
    health = {r["WarehouseSiteCode"]: r for r in transforms.aggregateInventoryHealth(sugg, "2024-03-15", 3, dimWarehouseSite).collect()}
    assert health["LDN"]["SuggestionCount"] == 2 and health["LDN"]["SuggestedUnits"] == 137 and health["LDN"]["WarehouseSiteKey"] == 1
    assert health["SYD"]["SuggestedUnits"] == 18 and health["SYD"]["RegionCode"] == "APAC"
    assert health["LDN"]["StockoutRiskCount"] == 1 and health["LDN"]["RegionCode"] == "EU", "site region, not item region" and health["LDN"]["RefreshBatchId"] == 3
