"""Hand-built inputs for the gold reporting tests (workstream 7).

Facts follow the CONVENTIONS.md gold contract (workstream 6 does not exist on
this branch); dims follow the silver contract; bronze tables carry the OLTP
column names verbatim.  Every expected value in test_gold_reporting_*.py is
computed by hand from the rows below - keep them small.
"""
from __future__ import annotations

import datetime as dt
import os
import tempfile
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import overwriteTable
from sales_lakehouse.gold import aggregates, reporting, sales_ops

AS_OF = dt.date(2024, 3, 15)
D = dt.date


def dec(v: str | int | float) -> Decimal:
    return Decimal(str(v))


def _naFiscalPeriods() -> list[tuple]:
    """NA 4-4-5: FY2024 starts 2024-01-01, FY2023 starts 364 days earlier (2023-01-02)."""
    rows = []
    for fy, start in ((2023, D(2023, 1, 2)), (2024, D(2024, 1, 1))):
        cur = start
        for p, weeks in enumerate((4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5), start=1):
            end = cur + dt.timedelta(days=7 * weeks - 1)
            rows.append(("NA445", fy, p, cur, end))
            cur = end + dt.timedelta(days=1)
    return rows


SALE_COLUMNS = (
    "sale_key BIGINT, invoice_date_key DATE, invoice_number STRING, invoice_line_number INT, order_number STRING, "
    "customer_key INT, stock_item_key INT, salesperson_key INT, sales_territory_key INT, sales_channel_key INT, "
    "customer_segment_key INT, promotion_key INT, region_code STRING, transaction_currency_code STRING, "
    "fx_rate_to_reporting DECIMAL(18,6), quantity_base_uom DECIMAL(18,3), quantity DECIMAL(18,3), "
    "gross_amount DECIMAL(18,2), line_discount_amount DECIMAL(18,2), net_amount DECIMAL(18,2), tax_amount DECIMAL(18,2), "
    "freight_amount DECIMAL(18,2), cost_of_sale_amount DECIMAL(18,2), gross_margin_amount DECIMAL(18,2), "
    "net_amount_reporting DECIMAL(18,2), total_excluding_tax DECIMAL(18,2), tax_regime_code STRING, correction_type_code STRING"
)


def _sale(key, date, inv, line, order, cust, item, sp, terr, ch, seg, promo, region, ccy, fx, qty, gross, disc, net, tax,
          cost, margin, netRep, regime="STD", corr=None, freight=0):
    return (key, date, inv, line, order, cust, item, sp, terr, ch, seg, promo, region, ccy, dec(fx), dec(qty), dec(qty),
            dec(gross), dec(disc), dec(net), dec(tax), dec(freight), dec(cost), dec(margin), dec(netRep), dec(net), regime, corr)


SALES = [
    # NA / USD ------------------------------------------------------------------------------------------------
    _sale(1, D(2024, 2, 5), "INV-NA-1", 1, "ORD-NA-1", 1, 10, 1, 1, 1, 1, 0, "NA", "USD", 1, 10, 1000, 0, 1000, 80, 600, 400, 1000),
    _sale(2, D(2024, 2, 5), "INV-NA-1", 2, "ORD-NA-1", 1, 11, 1, 1, 1, 1, 7, "NA", "USD", 1, 5, 500, 50, 450, 36, 300, 150, 450),
    _sale(3, D(2024, 2, 20), "INV-NA-2", 1, "ORD-NA-2", 2, 10, 1, 1, 2, 1, 0, "NA", "USD", 1, 2, 200, 0, 200, 16, 120, 80, 200),  # PILOT channel
    _sale(4, D(2024, 3, 10), "INV-NA-3", 1, "ORD-NA-3", 2, 10, 1, 1, 1, 1, 0, "NA", "USD", 1, 20, 2000, 0, 2000, 160, 1200, 800, 2000),  # house account
    _sale(5, D(2023, 2, 6), "INV-NA-0", 1, "ORD-NA-0", 1, 10, 1, 1, 1, 1, 0, "NA", "USD", 1, 8, 800, 0, 800, 64, 500, 300, 800),  # 364 days before sale 1
    _sale(6, D(2024, 2, 5), "INV-NA-1R", 1, "ORD-NA-1", 1, 10, 1, 1, 1, 1, 0, "NA", "USD", 1, -1, -100, 0, -100, -8, -60, -40, -100, corr="REV"),
    _sale(7, D(2024, 2, 10), "INV-NA-4", 1, "ORD-NA-4", 1, 10, 4, 1, 1, 1, 0, "NA", "USD", 1, 1, 100, 0, 100, 8, 60, 40, 100),  # rep without plan
    # EU / EUR (+GBP, CHF lines) ---------------------------------------------------------------------------------
    _sale(8, D(2024, 2, 12), "INV-EU-1", 1, "ORD-EU-1", 3, 20, 2, 2, 3, 2, 0, "EU", "EUR", 1.1, 10, 1000, 0, 1000, 190, 700, 300, 1100),
    _sale(9, D(2024, 2, 14), "INV-EU-2", 1, "ORD-EU-2", 4, 20, 2, 2, 5, 2, 0, "EU", "EUR", 1.1, 4, 400, 0, 400, 76, 250, 150, 440, regime="RC"),
    _sale(10, D(2016, 6, 1), "INV-EU-0", 1, "ORD-EU-0", 6, 21, 2, 2, 3, 2, 0, "EU", "EUR", 1.1, 1, 100, 0, 100, 19, 50, 50, 110),  # retention expired customer
    _sale(11, D(2024, 2, 20), "INV-EU-4", 1, "ORD-EU-4", 3, 20, 2, 2, 3, 2, 0, "EU", "EUR", 1.1, 100, 10000, 0, 10000, 1900, 7000, 3000, 11000),  # hits statutory cap
    _sale(12, D(2024, 2, 22), "INV-EU-5", 1, "ORD-EU-5", 3, 20, 2, 2, 3, 2, 0, "EU", "GBP", 1.27, 2, 200, 0, 200, 40, 140, 60, 254),  # GBP -> EUR month average 1.15
    _sale(13, D(2024, 3, 5), "INV-EU-6", 1, "ORD-EU-6", 3, 20, 2, 2, 3, 2, 0, "EU", "CHF", 1.13, 3, 300, 0, 300, 0, 200, 100, 339),  # no CHF->EUR rate
    # APAC / AUD (+NZD) -------------------------------------------------------------------------------------------
    _sale(14, D(2024, 2, 8), "INV-AP-1", 1, "ORD-AP-1", 5, 30, 3, 3, 4, 3, 0, "APAC", "AUD", 0.65, 10, 1000, 0, 1000, 100, 600, 400, 650, regime="GST"),
    _sale(15, D(2024, 2, 27), "INV-AP-2", 1, "ORD-AP-2", 5, 30, 3, 3, 4, 3, 0, "APAC", "AUD", 0.65, 3, 300, 0, 300, 0, 200, 100, 195, regime="GSTFREE"),
    _sale(16, D(2024, 3, 1), "INV-AP-3", 1, "ORD-AP-3", 5, 31, 3, 3, 4, 3, 0, "APAC", "NZD", 0.6, 5, 500, 0, 500, 50, 300, 200, 300, regime="GST"),
]

MARGIN_COLUMNS = (
    "invoice_date_key DATE, invoice_number STRING, invoice_line_number INT, customer_key INT, stock_item_key INT, "
    "product_category_key INT, salesperson_key INT, sales_territory_key INT, sales_channel_key INT, region_code STRING, "
    "quantity_base_uom DECIMAL(18,3), net_amount DECIMAL(18,2), net_amount_reporting DECIMAL(18,2), fx_rate_to_reporting DECIMAL(18,6), "
    "cost_of_sale_amount DECIMAL(18,2), standard_cost_amount DECIMAL(18,2), freight_cost_amount DECIMAL(18,2), "
    "rebate_accrual_amount DECIMAL(18,2), gross_margin_amount DECIMAL(18,2), gross_margin_reporting DECIMAL(18,2)"
)


def _margin(date, inv, line, cust, item, cat, sp, terr, ch, region, qty, net, netRep, fx, cost, std, freight, rebate, margin, marginRep):
    return (date, inv, line, cust, item, cat, sp, terr, ch, region, dec(qty), dec(net), dec(netRep), dec(fx), dec(cost), dec(std),
            dec(freight), dec(rebate), dec(margin), dec(marginRep))


MARGINS = [
    _margin(D(2024, 2, 5), "INV-NA-1", 1, 1, 10, 1, 1, 1, 1, "NA", 10, 1000, 1000, 1, 600, 580, 10, 5, 400, 400),
    _margin(D(2024, 2, 5), "INV-NA-1", 2, 1, 11, 1, 1, 1, 1, "NA", 5, 450, 450, 1, 300, 290, 5, 0, 150, 150),
    _margin(D(2024, 2, 20), "INV-NA-2", 1, 2, 10, 1, 1, 1, 2, "NA", 2, 200, 200, 1, 120, 120, 0, 0, 80, 80),  # PILOT
    _margin(D(2024, 3, 10), "INV-NA-3", 1, 2, 10, 1, 1, 1, 1, "NA", 20, 2000, 2000, 1, 1200, 1200, 0, 0, 800, 800),  # house
    _margin(D(2024, 2, 10), "INV-NA-4", 1, 1, 10, 1, 4, 1, 1, "NA", 1, 100, 100, 1, 60, 60, 0, 0, 40, 40),  # no plan
    _margin(D(2024, 2, 12), "INV-EU-1", 1, 3, 20, 2, 2, 2, 3, "EU", 10, 1000, 1100, 1.1, 700, 700, 0, 0, 300, 330),
    _margin(D(2024, 2, 8), "INV-AP-1", 1, 5, 30, 3, 3, 3, 4, "APAC", 10, 1000, 650, 0.65, 600, 600, 0, 0, 400, 260),
]

FULFILMENT_COLUMNS = (
    "order_fulfilment_key BIGINT, order_number STRING, order_line_number INT, invoice_number STRING, despatch_note_number STRING, "
    "region_code STRING, customer_key INT, sales_territory_key INT, warehouse_site_key INT, order_date_key DATE, "
    "allocation_date_key DATE, pick_date_key DATE, pack_date_key DATE, despatch_date_key DATE, delivery_date_key DATE, "
    "invoice_date_key DATE, cash_applied_date_key DATE, order_to_pick_lag_days INT, pick_to_despatch_lag_days INT, "
    "despatch_to_delivery_lag_days INT, delivery_to_invoice_lag_days INT, invoice_to_cash_lag_days INT, order_to_cash_cycle_days INT, "
    "service_target_days INT, quantity_ordered DECIMAL(18,3), quantity_despatched DECIMAL(18,3), quantity_invoiced DECIMAL(18,3), "
    "order_value_reporting DECIMAL(18,2), invoiced_value_reporting DECIMAL(18,2), cash_applied_reporting DECIMAL(18,2), "
    "pipeline_status_code STRING, open_milestone_count INT, pick_sla_breach_flag BOOLEAN, delivery_sla_breach_flag BOOLEAN, "
    "perfect_order_flag BOOLEAN, cancelled_flag BOOLEAN, cycle_complete_flag BOOLEAN, last_milestone_update TIMESTAMP"
)


def _fulfilment(key, order, inv, region, cust, terr, orderDate, cycleDays, target, value, cash, complete):
    d = orderDate
    return (key, order, 1, inv, f"DN-{key}", region, cust, terr, 1, d, d + dt.timedelta(days=1), d + dt.timedelta(days=2),
            d + dt.timedelta(days=2), d + dt.timedelta(days=3), d + dt.timedelta(days=6), d + dt.timedelta(days=7),
            d + dt.timedelta(days=cycleDays) if complete else None, 2, 1, 3, 1, cycleDays - 7 if complete else None,
            cycleDays if complete else None, target, dec(10), dec(10), dec(10), dec(value), dec(value), dec(cash),
            "COMPLETE" if complete else "INVOICED", 0 if complete else 1, False, False, complete, False, complete,
            dt.datetime(2024, 3, 1, 12, 0, 0))


FULFILMENTS = [
    _fulfilment(1, "ORD-NA-1", "INV-NA-1", "NA", 1, 1, D(2024, 1, 10), 20, 25, 1450, 1450, True),
    _fulfilment(2, "ORD-NA-3", "INV-NA-3", "NA", 2, 1, D(2024, 2, 1), 40, 25, 2000, 2000, True),
    _fulfilment(3, "ORD-EU-6", "INV-EU-6", "EU", 3, 2, D(2024, 3, 1), 0, 25, 339, 0, False),
]


def seedGoldInputs(spark: SparkSession, cfg: PipelineConfig) -> None:
    def write(layer: str, table: str, rows: list, schema: str) -> None:
        overwriteTable(spark.createDataFrame(rows, schema), cfg.fqn(layer, table))

    write("gold", "fact_sale", SALES, SALE_COLUMNS)
    write("gold", "fact_sales_margin", MARGINS, MARGIN_COLUMNS)
    write("gold", "fact_order_fulfilment", FULFILMENTS, FULFILMENT_COLUMNS)
    write(
        "gold", "fact_order",
        [
            (1, D(2024, 2, 3), "ORD-AP-1", 3, 3, 4, 5, "APAC", dec(800), dec(520)),
            (2, D(2024, 2, 26), "ORD-AP-2", 3, 3, 4, 5, "APAC", dec(200), dec(130)),
            (3, D(2024, 2, 4), "ORD-NA-1", 1, 1, 1, 1, "NA", dec(1450), dec(1450)),
        ],
        "order_key BIGINT, order_date_key DATE, order_number STRING, salesperson_key INT, sales_territory_key INT, "
        "sales_channel_key INT, customer_key INT, region_code STRING, net_order_amount DECIMAL(18,2), net_order_amount_reporting DECIMAL(18,2)",
    )
    write(
        "gold", "fact_credit_note",
        [(1, D(2024, 2, 20), 3, 2, 2, "EU", "INV-EU-1", dec(100), dec(1.1))],
        "credit_note_key BIGINT, credit_note_date_key DATE, customer_key INT, salesperson_key INT, sales_territory_key INT, "
        "region_code STRING, original_invoice_number STRING, credit_excluding_tax DECIMAL(18,2), fx_rate_to_reporting DECIMAL(18,6)",
    )
    write(
        "gold", "fact_payment",
        [
            (1, D(2024, 2, 15), "INV-AP-1", 5, "APAC", dec(1100), dec(715), "CLEARED", 7),
            (2, D(2024, 3, 5), "INV-AP-2", 5, "APAC", dec(300), dec(195), "PENDING", 7),
            (3, D(2024, 3, 6), "INV-AP-3", 5, "APAC", dec(550), dec(330), "CLEARED", 5),
            (4, D(2024, 2, 28), "INV-NA-1", 1, "NA", dec(1566), dec(1566), "CLEARED", 23),
        ],
        "payment_key BIGINT, payment_date_key DATE, invoice_number STRING, customer_key INT, region_code STRING, "
        "allocated_amount DECIMAL(18,2), allocated_amount_reporting DECIMAL(18,2), payment_status_code STRING, days_to_pay INT",
    )
    write(
        "silver", "dim_customer",
        [
            (1, 1001, "Acme Novelties", "C-1", "Novelty Shop", 1, 1, "NA", "buyer@acme.example", 7, dec(10000), True, None, False, True),
            (2, 1002, "Bigbox Inc", "C-2", "Supermarket", 1, 1, "NA", "po@bigbox.example", 7, dec(50000), False, None, True, True),
            (3, 1003, "Berlin GmbH", "C-3", "Novelty Shop", 2, 2, "EU", "kauf@berlin.example", 8, dec(20000), True, None, False, True),
            (4, 1004, "Paris SA", "C-4", "Gift Store", 2, 2, "EU", "achat@paris.example", 8, dec(5000), False, None, False, True),
            (5, 1005, "Sydney Pty", "C-5", "Novelty Shop", 3, 3, "APAC", "buy@sydney.example", 9, dec(8000), None, None, False, True),
            (6, 1006, "Old Roma Srl", "C-6", "Gift Store", 2, 2, "EU", "old@roma.example", 8, dec(1000), True, None, False, True),
        ],
        "customer_key INT, wwi_customer_id INT, customer STRING, source_customer_reference STRING, category STRING, "
        "customer_segment_key INT, sales_territory_key INT, region_code STRING, primary_contact_email STRING, "
        "account_manager_employee_key INT, credit_limit_amount DECIMAL(18,2), marketing_consent_flag BOOLEAN, "
        "erasure_requested_on DATE, is_house_account BOOLEAN, is_current_row BOOLEAN",
    )
    write(
        "silver", "dim_sales_territory",
        [(1, 10, "US East", "NA"), (2, 20, "Germany", "EU"), (3, 30, "Australia", "APAC")],
        "sales_territory_key INT, wwi_sales_territory_id INT, sales_territory STRING, region_code STRING",
    )
    write(
        "silver", "dim_sales_channel",
        [
            (1, "DIRECT", "Direct Sales", "ACTIVE", True, None, "NA"),
            (2, "PILOTWEB", "Pilot Web Store", "PILOT", False, None, "NA"),
            (3, "DIRECT", "Direct Sales", "ACTIVE", True, None, "EU"),
            (4, "PARTNER", "Marketplace", "ACTIVE", True, "APAC-MKTPL", "APAC"),
            (5, "PARTNER", "EU Reseller", "ACTIVE", True, "EU-RESELL", "EU"),
        ],
        "sales_channel_key INT, sales_channel_code STRING, sales_channel STRING, channel_status STRING, "
        "is_commissionable BOOLEAN, partner_name STRING, region_code STRING",
    )
    write(
        "silver", "dim_salesperson",
        [(1, 101, "Ana North", "NA"), (2, 102, "Emil Euro", "EU"), (3, 103, "Aroha Pacific", "APAC"), (4, 104, "Nate Noplan", "NA")],
        "salesperson_key INT, wwi_person_id INT, salesperson STRING, region_code STRING",
    )
    write(
        "silver", "dim_stock_item",
        [
            (10, 110, "Chocolate frogs", "WWI", "S", 1, 1, False, dt.datetime(2020, 1, 1), True),
            (11, 111, "Novelty glasses", "WWI", "M", 1, 1, False, dt.datetime(2020, 1, 1), True),
            (20, 120, "Toy robot", "Tailspin", "L", 2, 2, False, dt.datetime(2020, 1, 1), True),
            (21, 121, "Toy car", "Tailspin", "S", 2, 2, True, dt.datetime(2020, 1, 1), True),
            (30, 130, "Shipping carton", "WWI", "XL", 3, 3, False, dt.datetime(2020, 1, 1), True),
            (31, 131, "Air cushion", "WWI", "M", 3, 3, False, dt.datetime(2024, 1, 1), True),
        ],
        "stock_item_key INT, wwi_stock_item_id INT, stock_item STRING, brand STRING, size STRING, product_category_key INT, "
        "primary_supplier_key INT, is_discontinued BOOLEAN, valid_from TIMESTAMP, is_current_row BOOLEAN",
    )
    write("silver", "dim_product_category", [(1, "Novelty"), (2, "Toys"), (3, "Packaging")], "product_category_key INT, product_category STRING")
    write("silver", "dim_fiscal_calendar", _naFiscalPeriods(), "calendar_code STRING, fiscal_year INT, fiscal_period INT, period_start DATE, period_end DATE")
    write(
        "silver", "ref_fx_rate",
        [
            ("EUR", "USD", "AVERAGE", D(2024, 2, 1), dec(1.10)),
            ("EUR", "USD", "AVERAGE", D(2024, 3, 1), dec(1.08)),
            ("AUD", "USD", "AVERAGE", D(2024, 2, 1), dec(0.65)),
            ("GBP", "EUR", "AVERAGE", D(2024, 2, 1), dec(1.15)),
            ("GBP", "EUR", "DAILY", D(2024, 2, 22), dec(1.17)),
        ],
        "from_currency_code STRING, to_currency_code STRING, rate_type_code STRING, effective_date DATE, conversion_rate DECIMAL(18,6)",
    )
    write("silver", "ref_sales_budget", [(1, D(2024, 2, 1), dec(3000))], "sales_territory_key INT, budget_month DATE, budget_amount_reporting DECIMAL(18,2)")
    write("silver", "ref_commission_statutory_cap", [("EU-NET", dec(300))], sales_ops.STATUTORY_CAP_SCHEMA)
    write(
        "bronze", "sqlserver_sales_commission_plans",
        [
            (1, "NA-MARGIN", "NA", "INVOICEDMARGIN", dec(80), dec(5), dec(100), dec(8), dec(120), dec(10), dec(2), dec(20), D(2023, 1, 1), None),
            (2, "EU-NET", "EU", "NETREVENUE", dec(90), dec(5), dec(110), dec(6), dec(130), dec(7), None, None, D(2023, 1, 1), None),
            (3, "APAC-CASH", "APAC", "COLLECTEDCASH", dec(100), dec(4), dec(120), dec(5), dec(150), dec(6), None, None, D(2023, 1, 1), None),
        ],
        "CommissionPlanID INT, PlanCode STRING, RegionCode STRING, CommissionBasis STRING, Band1UpperPercent DECIMAL(5,2), "
        "Band1RatePercent DECIMAL(5,2), Band2UpperPercent DECIMAL(5,2), Band2RatePercent DECIMAL(5,2), Band3UpperPercent DECIMAL(5,2), "
        "Band3RatePercent DECIMAL(5,2), AcceleratorPercent DECIMAL(5,2), MinimumMarginPercent DECIMAL(5,2), EffectiveFromDate DATE, EffectiveToDate DATE",
    )
    write(
        "bronze", "sqlserver_application_sales_team_members",
        [
            (1, 1, 101, "REP", 1, dec(100), D(2023, 1, 1), None, True),
            (2, 2, 102, "REP", 2, dec(100), D(2023, 1, 1), None, True),
            (3, 3, 103, "REP", 3, dec(50), D(2023, 1, 1), None, True),
            (4, 1, 104, "SUPPORT", None, dec(100), D(2023, 1, 1), None, True),
        ],
        "SalesTeamMemberID BIGINT, SalesTeamID INT, PersonID INT, RoleCode STRING, CommissionPlanID INT, QuotaSharePercent DECIMAL(5,2), "
        "ValidFrom DATE, ValidTo DATE, IsPrimaryAssignment BOOLEAN",
    )
    write(
        "bronze", "sqlserver_sales_sales_quotas",
        [
            (1, 10, 101, "NA445", "FY2024-P02", D(2024, 1, 29), D(2024, 2, 25), dec(2000), "USD"),
            (2, 10, 101, "NA445", "FY2024-P03", D(2024, 2, 26), D(2024, 3, 31), dec(500), "USD"),
            (3, 20, 102, "EUCAL", "2024-02", D(2024, 2, 1), D(2024, 2, 29), dec(10000), "EUR"),
            (4, 30, 103, "NA445", "FY2024-P02", D(2024, 1, 29), D(2024, 2, 25), dec(800), "AUD"),
            (5, 99, 101, "NA445", "FY2024-P04", D(2024, 4, 1), D(2024, 4, 28), dec(100), "USD"),  # unknown territory
            (6, 10, 104, "NA445", "FY2024-P02", D(2024, 1, 29), D(2024, 2, 25), dec(0), "USD"),  # zero quota
        ],
        "SalesQuotaID INT, SalesTerritoryID INT, SalespersonPersonID INT, FiscalCalendarCode STRING, FiscalPeriodLabel STRING, "
        "PeriodStartDate DATE, PeriodEndDate DATE, QuotaAmount DECIMAL(18,2), QuotaCurrencyCode STRING",
    )


_GOLD_RUN: dict[str, str] = {}


@pytest.fixture(scope="session")
def goldRun(spark, cfg) -> dict[str, str]:
    """Seed inputs and run the three gold modules once for the whole test session.

    The fixture is imported into several test modules, which makes pytest
    register (and run) it once per module; the module-level cache keeps the
    seed + run to a single execution per Spark session.
    """
    if not _GOLD_RUN:
        seedGoldInputs(spark, cfg)
        aggregates.run(spark, cfg, asOfDate=AS_OF)
        reporting.run(spark, cfg)
        feedDir = os.path.join(tempfile.mkdtemp(prefix="partner_feed_"), "out")
        sales_ops.run(spark, cfg, outDir=feedDir, asOfDate=AS_OF)
        _GOLD_RUN["feedDir"] = feedDir
    return dict(_GOLD_RUN)


def rowsOf(spark: SparkSession, cfg: PipelineConfig, layer: str, table: str, where: str = "1=1", orderBy: str | None = None):
    sql = f"SELECT * FROM {cfg.fqn(layer, table)} WHERE {where}"
    if orderBy:
        sql += f" ORDER BY {orderBy}"
    return [r.asDict() for r in spark.sql(sql).collect()]


def one(spark: SparkSession, cfg: PipelineConfig, layer: str, table: str, where: str) -> dict:
    rows = rowsOf(spark, cfg, layer, table, where)
    assert len(rows) == 1, f"{table} WHERE {where}: expected 1 row, got {len(rows)}"
    return rows[0]
