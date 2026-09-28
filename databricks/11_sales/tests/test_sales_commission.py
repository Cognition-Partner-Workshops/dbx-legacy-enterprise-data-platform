from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

import sales_commission as com
import sales_common as sc

BATCH = 7


def _lines(spark, saleLineRows, region):
    df = com.legacySaleLines(spark.createDataFrame(saleLineRows), region)
    return com.regionSaleLines(sc.batchFilter(df, "LoadBatchId", BATCH, False), region)


def test_region_filter_drops_samples_non_usd_and_other_batches(spark, saleLineRows):
    na = _lines(spark, saleLineRows, "NA")
    assert sorted(r["SaleLineId"] for r in na.collect()) == ["NA-1", "NA-2"]


def test_na_extended_price_is_tax_exclusive_but_eu_apac_gross(spark, saleLineRows):
    na = com.legacySaleLines(spark.createDataFrame(saleLineRows), "NA").where("SaleLineId = 'NA-1'").first()
    eu = com.legacySaleLines(spark.createDataFrame(saleLineRows), "EU").where("SaleLineId = 'EU-1'").first()
    assert na["ExtendedPrice"] == Decimal("1000.00") and eu["ExtendedPrice"] == Decimal("1190.00")
    assert eu["VatAmount"] == Decimal("190.00") and eu["NetAmount"] == Decimal("1000.00")


def test_na_commission_accelerator_and_house_account(spark, saleLineRows, commissionPlanRows):
    plans = com.legacyCommissionPlans(spark.createDataFrame(commissionPlanRows))
    planned = com.joinCommissionPlans(_lines(spark, saleLineRows, "NA"), plans, "NA")
    house = spark.createDataFrame([("C1", True, None)], "CustomerId string, IsHouseAccount boolean, ValidTo timestamp")
    houseFlags = com.currentHouseAccountFlags(house)
    out = {r["SaleLineId"]: r for r in com.computeNaCommission(planned, houseFlags, 50).collect()}
    # NA-1: commissionable 1080 (1000 + 80 tax); base 5% = 54; accelerator 2% above 500 = 11.60; house account 50%
    assert out["NA-1"]["CommissionableAmount"] == Decimal("1080.00")
    assert out["NA-1"]["BaseCommissionAmount"] == Decimal("54.00")
    assert out["NA-1"]["AcceleratorCommissionAmount"] == Decimal("11.60")
    assert out["NA-1"]["HouseAccountFactor"] == Decimal("0.50")
    assert out["NA-1"]["CommissionAmount"] == Decimal("32.80")
    assert out["NA-1"]["CommissionPeriod"] == "2024-03"
    # NA-2: 108 commissionable, below threshold, not a house account
    assert out["NA-2"]["AcceleratorCommissionAmount"] == Decimal("0.00")
    assert out["NA-2"]["CommissionAmount"] == Decimal("5.40")
    assert out["NA-2"]["IsHouseAccount"] is None and out["NA-2"]["HouseAccountFactor"] == Decimal("1.00")


def test_na_house_account_lookup_absent_column_returns_none(spark):
    dim = spark.createDataFrame([("C1",)], "WWICustomerID string")
    assert com.currentHouseAccountFlags(dim) is None


def test_unplanned_reps_counted(spark, saleLineRows, commissionPlanRows):
    lines = sc.batchFilter(com.legacySaleLines(spark.createDataFrame(saleLineRows), "NA"), "LoadBatchId", BATCH, False)
    plans = com.legacyCommissionPlans(spark.createDataFrame(commissionPlanRows))
    assert com.countUnplannedReps(lines, plans, "NA") == 1  # rep 11


def test_eu_commission_vat_backout_fx_cap_and_cash_basis(spark, saleLineRows, commissionPlanRows, fxRateRows):
    plans = com.legacyCommissionPlans(spark.createDataFrame(commissionPlanRows))
    fx = com.legacyFxRates(spark.createDataFrame(fxRateRows))
    planned = com.joinCommissionPlans(_lines(spark, saleLineRows, "EU"), plans, "EU")
    eu = com.computeEuCommission(planned, fx, ["DE", "AT"])
    out = {r["SaleLineId"]: r for r in eu.collect()}
    # EU-1 (DE, EUR): NetAmount 1000 used directly, no FX row for EUR -> rate null -> 1000 EUR; 10% = 100 capped to 90
    assert out["EU-1"]["NetCommissionableAmount"] == Decimal("1000.00")
    assert out["EU-1"]["EurConversionRate"] is None and out["EU-1"]["NetAmountEur"] == Decimal("1000.00")
    assert out["EU-1"]["RawCommissionAmount"] == Decimal("100.00") and out["EU-1"]["CommissionAmount"] == Decimal("90.00")
    assert out["EU-1"]["IsCashBasisCountry"] is True
    # EU-2 (FR, GBP): no NetAmount -> 1200 / 1.20 = 1000; AVERAGE month-end rate 1.2 -> 1200 EUR; 10% = 120 capped 90
    assert out["EU-2"]["NetCommissionableAmount"] == Decimal("1000.00")
    assert out["EU-2"]["EurConversionRate"] == Decimal("1.20000000")
    assert out["EU-2"]["NetAmountEur"] == Decimal("1200.00")
    assert out["EU-2"]["CommissionAmount"] == Decimal("90.00")
    assert out["EU-2"]["IsCashBasisCountry"] is False
    accrual, held = com.splitCashBasis(eu)
    assert [r["SaleLineId"] for r in accrual.collect()] == ["EU-2"]
    assert [r["SaleLineId"] for r in held.collect()] == ["EU-1"]
    assert com.countCappedReps(eu) == 1


def test_eu_release_on_cleared_payment_uses_payment_month(spark, saleLineRows, commissionPlanRows, fxRateRows):
    plans = com.legacyCommissionPlans(spark.createDataFrame(commissionPlanRows))
    fx = com.legacyFxRates(spark.createDataFrame(fxRateRows))
    eu = com.computeEuCommission(com.joinCommissionPlans(_lines(spark, saleLineRows, "EU"), plans, "EU"), fx, ["DE"])
    _, held = com.splitCashBasis(eu)
    payments = com.legacyCustomerPayments(spark.createDataFrame(
        [("INV-EU-1", date(2024, 5, 2), "CLEARED"), ("INV-EU-1", date(2024, 4, 1), "PENDING")],
        "InvoiceNumber string, PaymentDate date, PaymentStatusCode string"))
    released = com.releaseHeldEuLines(held, payments).collect()
    assert len(released) == 1
    assert released[0]["CommissionPeriod"] == "2024-05"
    assert released[0]["CommissionAmount"] == Decimal("90.00")
    assert released[0]["CountryCode"] == "DE"


def test_apac_commission_fiscal_period_fx_reject_and_team_split(spark, saleLineRows, commissionPlanRows, fxRateRows, fiscalCalendarRows):
    plans = com.legacyCommissionPlans(spark.createDataFrame(commissionPlanRows))
    fx = com.legacyFxRates(spark.createDataFrame(fxRateRows))
    cal = com.legacyFiscalCalendar(spark.createDataFrame(fiscalCalendarRows))
    apac = _lines(spark, saleLineRows, "APAC")
    assert com.countMissingCalendarDays(apac, cal) == 0
    matched, rejected = com.computeApacCommission(com.joinCommissionPlans(apac, plans, "APAC"), cal, fx, True)
    m = {r["SaleLineId"]: r for r in matched.collect()}
    r = {r["SaleLineId"]: r for r in rejected.collect()}
    assert list(m) == ["AP-1"] and list(r) == ["AP-2"]
    assert r["AP-2"]["RejectReasonCode"] == "FX_RATE_MISSING"
    # GST-exclusive 1100 - 100 = 1000; newest AVERAGE AUD->SGD rate 0.9 -> 900; 10% -> 90 * 60% split = 54
    assert m["AP-1"]["GstExclusiveAmount"] == Decimal("1000.00")
    assert m["AP-1"]["ConversionRate"] == Decimal("0.90000000")
    assert m["AP-1"]["PlanCurrencyAmount"] == Decimal("900.00")
    assert m["AP-1"]["CommissionAmount"] == Decimal("54.00")
    assert m["AP-1"]["CommissionPeriod"] == "2024-P04" and m["AP-1"]["CommissionFiscalWeek"] == 14
    # invoice month 03 vs period 04 -> boundary line
    assert m["AP-1"]["IsPeriodBoundaryLine"] is True
    assert com.countPeriodBoundaryLines(matched) == 1
    noSplit, _ = com.computeApacCommission(com.joinCommissionPlans(apac, plans, "APAC"), cal, fx, False)
    assert noSplit.first()["CommissionAmount"] == Decimal("90.00")


def test_apac_missing_calendar_blocks(spark, saleLineRows, fiscalCalendarRows):
    apac = _lines(spark, saleLineRows, "APAC")
    cal = com.legacyFiscalCalendar(spark.createDataFrame(fiscalCalendarRows[:0] or fiscalCalendarRows).where("1 = 0"))
    assert com.countMissingCalendarDays(apac, cal) == 1


def test_posting_rows_keyed_per_region(spark, saleLineRows, commissionPlanRows):
    plans = com.legacyCommissionPlans(spark.createDataFrame(commissionPlanRows))
    work = com.computeNaCommission(com.joinCommissionPlans(_lines(spark, saleLineRows, "NA"), plans, "NA"), None, 50)
    posted = com.postingRows(work, "NA", "2024-03", BATCH, 1001)
    rows = {r["SaleLineId"]: r for r in posted.collect()}
    assert rows["NA-1"]["CommissionableAmount"] == Decimal("1080.00")
    assert rows["NA-1"]["PostedCommissionPeriod"] == "2024-03" and rows["NA-1"]["BatchId"] == BATCH
    assert rows["NA-1"]["RegionCode"] == "NA" and rows["NA-1"]["PackageExecutionId"] == 1001
