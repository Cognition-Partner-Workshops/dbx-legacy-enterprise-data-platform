import os
import sys
from datetime import date
from decimal import Decimal

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "fakes"))
sys.path.insert(0, os.path.join(HERE, "..", "src"))

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("wwi_11_sales_tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    yield session
    session.stop()


def money(value):
    return Decimal(str(value)).quantize(Decimal("0.01"))


@pytest.fixture
def saleLineRows():
    """Rows in the conformed silver.stg_sale_line shape (22_stg_tables_sales.sql) plus the
    generator-only columns the packages need."""
    return [
        # NA lines: rep 10 has a plan; rep 11 has none (unplanned).
        dict(SaleLineBusinessKey="NA-1", SaleBusinessKey="INV-NA-1", InvoiceDate=date(2024, 3, 5), SalespersonPersonId="10",
             CustomerId="C1", StockItemId="S1", TerritoryCode="T-NA", CountryCode="US", TransactionCurrencyCode="USD",
             Quantity=Decimal("2"), NetLineAmount=money(1000), GrossLineAmount=money(1080), TaxAmount=money(80),
             TaxRatePercent=money(8), LineProfitAmount=money(300), RegionCode="NA", BatchId=7, LineTypeCode="STD"),
        dict(SaleLineBusinessKey="NA-2", SaleBusinessKey="INV-NA-2", InvoiceDate=date(2024, 3, 6), SalespersonPersonId="10",
             CustomerId="C2", StockItemId="S1", TerritoryCode="T-NA", CountryCode="US", TransactionCurrencyCode="USD",
             Quantity=Decimal("1"), NetLineAmount=money(100), GrossLineAmount=money(108), TaxAmount=money(8),
             TaxRatePercent=money(8), LineProfitAmount=money(30), RegionCode="NA", BatchId=7, LineTypeCode="STD"),
        dict(SaleLineBusinessKey="NA-3", SaleBusinessKey="INV-NA-3", InvoiceDate=date(2024, 3, 7), SalespersonPersonId="10",
             CustomerId="C1", StockItemId="S1", TerritoryCode="T-NA", CountryCode="US", TransactionCurrencyCode="USD",
             Quantity=Decimal("1"), NetLineAmount=money(50), GrossLineAmount=money(54), TaxAmount=money(4),
             TaxRatePercent=money(8), LineProfitAmount=money(10), RegionCode="NA", BatchId=7, LineTypeCode="SAMPLE"),
        dict(SaleLineBusinessKey="NA-4", SaleBusinessKey="INV-NA-4", InvoiceDate=date(2024, 3, 7), SalespersonPersonId="11",
             CustomerId="C1", StockItemId="S1", TerritoryCode="T-NA", CountryCode="CA", TransactionCurrencyCode="CAD",
             Quantity=Decimal("1"), NetLineAmount=money(50), GrossLineAmount=money(54), TaxAmount=money(4),
             TaxRatePercent=money(8), LineProfitAmount=money(10), RegionCode="NA", BatchId=7, LineTypeCode="STD"),
        # EU lines: DE is cash basis, FR accrues; FR line has no NetAmount so VAT is backed out.
        dict(SaleLineBusinessKey="EU-1", SaleBusinessKey="INV-EU-1", InvoiceDate=date(2024, 3, 10), SalespersonPersonId="20",
             CustomerId="C3", StockItemId="S2", TerritoryCode="T-EU", CountryCode="DE", TransactionCurrencyCode="EUR",
             Quantity=Decimal("1"), NetLineAmount=money(1000), GrossLineAmount=money(1190), TaxAmount=money(190),
             TaxRatePercent=money(19), LineProfitAmount=money(100), RegionCode="EU", BatchId=7, LineTypeCode="STD"),
        dict(SaleLineBusinessKey="EU-2", SaleBusinessKey="INV-EU-2", InvoiceDate=date(2024, 3, 11), SalespersonPersonId="20",
             CustomerId="C4", StockItemId="S2", TerritoryCode="T-EU", CountryCode="FR", TransactionCurrencyCode="GBP",
             Quantity=Decimal("1"), NetLineAmount=None, GrossLineAmount=money(1200), TaxAmount=money(200),
             TaxRatePercent=money(20), LineProfitAmount=money(100), RegionCode="EU", BatchId=7, LineTypeCode="STD"),
        # APAC lines: AUD has an AVERAGE rate into the plan currency, NZD has none (reject).
        dict(SaleLineBusinessKey="AP-1", SaleBusinessKey="INV-AP-1", InvoiceDate=date(2024, 3, 30), SalespersonPersonId="30",
             CustomerId="C5", StockItemId="S3", TerritoryCode="T-AP", CountryCode="AU", TransactionCurrencyCode="AUD",
             Quantity=Decimal("1"), NetLineAmount=money(1000), GrossLineAmount=money(1100), TaxAmount=money(100),
             TaxRatePercent=money(10), LineProfitAmount=money(100), RegionCode="APAC", BatchId=7, LineTypeCode="STD"),
        dict(SaleLineBusinessKey="AP-2", SaleBusinessKey="INV-AP-2", InvoiceDate=date(2024, 3, 30), SalespersonPersonId="30",
             CustomerId="C6", StockItemId="S3", TerritoryCode="T-AP", CountryCode="NZ", TransactionCurrencyCode="NZD",
             Quantity=Decimal("1"), NetLineAmount=money(500), GrossLineAmount=money(575), TaxAmount=money(75),
             TaxRatePercent=money(15), LineProfitAmount=money(50), RegionCode="APAC", BatchId=7, LineTypeCode="STD"),
        # previous batch: must be excluded by the BatchId filter
        dict(SaleLineBusinessKey="NA-OLD", SaleBusinessKey="INV-NA-OLD", InvoiceDate=date(2024, 2, 5), SalespersonPersonId="10",
             CustomerId="C1", StockItemId="S1", TerritoryCode="T-NA", CountryCode="US", TransactionCurrencyCode="USD",
             Quantity=Decimal("1"), NetLineAmount=money(999), GrossLineAmount=money(1078.92), TaxAmount=money(79.92),
             TaxRatePercent=money(8), LineProfitAmount=money(1), RegionCode="NA", BatchId=6, LineTypeCode="STD"),
    ]


@pytest.fixture
def commissionPlanRows():
    return [
        dict(SalespersonPersonId="10", RegionCode="NA", PlanCode="NA-STD", BaseRatePercent=money(5),
             AcceleratorRatePercent=money(2), AcceleratorThresholdAmount=money(500), StatutoryCapAmount=None,
             PlanCurrencyCode="USD", TeamSplitPercent=None, EffectiveFrom=date(2024, 1, 1), EffectiveTo=None),
        dict(SalespersonPersonId="20", RegionCode="EU", PlanCode="EU-STD", BaseRatePercent=money(10),
             AcceleratorRatePercent=None, AcceleratorThresholdAmount=None, StatutoryCapAmount=money(90),
             PlanCurrencyCode="EUR", TeamSplitPercent=None, EffectiveFrom=date(2024, 1, 1), EffectiveTo=date(2024, 12, 31)),
        dict(SalespersonPersonId="30", RegionCode="APAC", PlanCode="AP-STD", BaseRatePercent=money(10),
             AcceleratorRatePercent=None, AcceleratorThresholdAmount=None, StatutoryCapAmount=None,
             PlanCurrencyCode="SGD", TeamSplitPercent=money(60), EffectiveFrom=date(2024, 1, 1), EffectiveTo=None),
    ]


@pytest.fixture
def fxRateRows():
    return [
        dict(FromCurrencyCode="GBP", ToCurrencyCode="EUR", RateDate=date(2024, 3, 31), RateTypeCode="AVERAGE", ConversionRate=Decimal("1.20000000")),
        dict(FromCurrencyCode="GBP", ToCurrencyCode="EUR", RateDate=date(2024, 3, 31), RateTypeCode="CLOSING", ConversionRate=Decimal("1.25000000")),
        dict(FromCurrencyCode="AUD", ToCurrencyCode="SGD", RateDate=date(2024, 2, 29), RateTypeCode="AVERAGE", ConversionRate=Decimal("0.80000000")),
        dict(FromCurrencyCode="AUD", ToCurrencyCode="SGD", RateDate=date(2024, 3, 31), RateTypeCode="AVERAGE", ConversionRate=Decimal("0.90000000")),
    ]


@pytest.fixture
def fiscalCalendarRows():
    return [
        dict(CalendarDate=date(2024, 3, 30), FiscalPeriod445="2024-P04", FiscalYear445=2024, FiscalWeek445=14),
        dict(CalendarDate=date(2024, 3, 31), FiscalPeriod445="2024-P04", FiscalYear445=2024, FiscalWeek445=14),
    ]
