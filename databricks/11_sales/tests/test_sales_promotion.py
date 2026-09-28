from datetime import date
from decimal import Decimal

import sales_promotion as promo


def _money(v):
    return Decimal(str(v)).quantize(Decimal("0.01"))


def _inputs(spark):
    promotions = promo.legacyPromotions(spark.createDataFrame(
        [("P-NA", "NA10", "NA promo", "NA", date(2024, 3, 1), date(2024, 3, 31), "PCT", _money(10), _money(50), 7),
         ("P-AP", "AP5", "APAC promo", "APAC", date(2024, 3, 1), date(2024, 3, 31), "FIXED", _money(5), _money(100), 7),
         ("P-EU", "EU20", "EU promo", "EU", date(2024, 3, 1), date(2024, 3, 31), "PCT", _money(20), _money(1000), 7)],
        "PromotionId string, PromotionCode string, PromotionName string, RegionCode string, StartDate date, EndDate date, "
        "DiscountTypeCode string, DiscountValue decimal(18,2), BudgetAmount decimal(18,2), BatchId long"))
    redemptions = promo.legacyRedemptions(spark.createDataFrame(
        [("P-NA", "L1", date(2024, 3, 15), "C1", "S1", _money(300), "WEB"),
         ("P-NA", "L2", date(2024, 4, 20), "C1", "S1", _money(400), "WEB"),   # +30 days window -> attributed (REGIONAL)
         ("P-NA", "L3", date(2024, 5, 5), "C2", "S1", _money(100), "WEB"),    # outside -> spill
         ("P-AP", "L4", date(2024, 4, 10), "C3", "S2", _money(50), "STORE"),  # +14 days -> attributed
         ("P-AP", "L5", date(2024, 4, 20), "C3", "S2", _money(50), "STORE"),  # spill
         ("P-EU", "L6", date(2024, 4, 1), "C4", "S3", _money(50), "WEB")],    # EU no extension -> spill
        "PromotionId string, SaleLineId string, InvoiceDate date, CustomerId string, StockItemId string, "
        "RedeemedAmount decimal(18,2), RedemptionChannelCode string"))
    return promotions, redemptions


def test_regional_attribution_windows_and_budget_flag(spark):
    promotions, redemptions = _inputs(spark)
    classified = promo.classifyAttribution(promo.joinRedemptions(promotions, redemptions), "REGIONAL")
    rows = {r["SaleLineId"]: r for r in classified.collect()}
    assert rows["L1"]["AttributionEndDate"] == date(2024, 4, 30) and rows["L4"]["AttributionEndDate"] == date(2024, 4, 14)
    assert rows["L6"]["AttributionEndDate"] == date(2024, 3, 31)
    assert rows["L1"]["DiscountCostAmount"] == Decimal("30.00")   # PCT: 300 * 10 / 100
    assert rows["L4"]["DiscountCostAmount"] == Decimal("5.00")    # FIXED: DiscountValue
    attributed, spill = promo.splitSpill(classified)
    assert sorted(r["SaleLineId"] for r in attributed.collect()) == ["L1", "L2", "L4"]
    assert sorted(r["SaleLineId"] for r in spill.collect()) == ["L3", "L5", "L6"]
    summary = promo.flagOverBudget(promo.summarisePromotion(attributed), promotions)
    out = {r["PromotionId"]: r for r in summary.collect()}
    assert out["P-NA"]["RedeemedAmount"] == Decimal("700.00") and out["P-NA"]["DiscountCostAmount"] == Decimal("70.00")
    assert out["P-NA"]["RedemptionCount"] == 2 and out["P-NA"]["RedeemingCustomerCount"] == 1
    assert out["P-NA"]["BudgetStatus"] == "OVER"      # 70 > budget 50
    assert out["P-AP"]["BudgetStatus"] is None        # 5 <= 100
    assert "P-EU" not in out


def test_strict_mode_uses_promotion_window_only(spark):
    promotions, redemptions = _inputs(spark)
    classified = promo.classifyAttribution(promo.joinRedemptions(promotions, redemptions), "STRICT")
    attributed, _ = promo.splitSpill(classified)
    assert sorted(r["SaleLineId"] for r in attributed.collect()) == ["L1"]
