from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from logistics_returns.aggregate import (
    buildLaneDeliveryPerformance,
    buildWeeklyDeliveryPerformance,
    isoWeekStart,
    refreshWindowStart,
)

FACT_SCHEMA = (
    "despatch_note_number string, despatch_date_key date, delivery_confirmed_date_key date, promised_delivery_date_key date, "
    "carrier_key int, warehouse_site_key int, sales_territory_key int, region_code string, service_level_code string, "
    "package_count int, customs_hold_days int, delivery_attempt_count int, damaged_flag boolean, shipment_status_code string, "
    "despatch_to_delivery_lag_days int, pick_to_despatch_lag_days int, total_weight_kg decimal(18,3), chargeable_weight_kg decimal(18,3), "
    "freight_charge_reporting decimal(19,4), freight_charge decimal(19,4), fuel_surcharge decimal(19,4), duty_and_clearance_amount decimal(19,4)"
)


def _fact(spark: SparkSession, rows):
    return spark.createDataFrame(rows, FACT_SCHEMA)


def _row(dn, region, delivered, promised, **kw):
    base = dict(
        despatch_note_number=dn, despatch_date_key=date(2024, 3, 6), delivery_confirmed_date_key=delivered, promised_delivery_date_key=promised,
        carrier_key=1, warehouse_site_key=2, sales_territory_key=-1, region_code=region, service_level_code=None, package_count=2,
        customs_hold_days=0, delivery_attempt_count=1, damaged_flag=False, shipment_status_code="DELIVERED" if delivered else "IN_TRANSIT",
        despatch_to_delivery_lag_days=2 if delivered else None, pick_to_despatch_lag_days=1, total_weight_kg=Decimal("10.000"),
        chargeable_weight_kg=Decimal("10.000"), freight_charge_reporting=Decimal("100.0000"), freight_charge=Decimal("100.0000"),
        fuel_surcharge=Decimal("0"), duty_and_clearance_amount=Decimal("0"),
    )  # fmt: skip
    base.update(kw)
    return tuple(base[c.split()[0]] for c in FACT_SCHEMA.split(", "))


def test_week_start_matches_tsql_sunday_weeks(spark):
    df = spark.createDataFrame([(date(2024, 3, 6),), (date(2024, 3, 10),), (date(2024, 3, 9),)], "d date")
    out = [r[0] for r in df.select(isoWeekStart(F.col("d"))).collect()]
    assert out == [date(2024, 3, 3), date(2024, 3, 10), date(2024, 3, 3)]


def test_weekly_summary_regional_sla_and_eu_service_credit_cap(spark):
    rows = []
    # EU: 4 delivered, 1 on time -> 25 % vs 98 target -> gap 73 pts * 2 % = 146 % -> capped at 10 % of freight (400 -> 40.00)
    rows += [_row(f"EU-{i}", "EU", date(2024, 3, 12), date(2024, 3, 10)) for i in range(3)]
    rows += [_row("EU-3", "EU", date(2024, 3, 9), date(2024, 3, 10), damaged_flag=True)]
    # EU second grain (carrier 9): 50 on-time of 50 -> no breach; one lost consignment
    rows += [_row(f"EU9-{i}", "EU", date(2024, 3, 9), date(2024, 3, 10), carrier_key=9) for i in range(2)]
    rows += [_row("EU9-L", "EU", None, date(2024, 3, 10), carrier_key=9, shipment_status_code="LOST")]
    # NA: strict on-time, 1 of 2 -> 50 % < 96 -> breach but NO service credit outside the EU
    rows += [_row("NA-0", "NA", date(2024, 3, 12), date(2024, 3, 10)), _row("NA-1", "NA", date(2024, 3, 10), date(2024, 3, 10))]
    # APAC: customs hold grace -> delivered 2 days late with 3 hold days counts as on time (100 % vs 92)
    rows += [_row("AP-0", "APAC", date(2024, 3, 12), date(2024, 3, 10), customs_hold_days=3)]
    # outside the refresh window -> excluded
    rows += [_row("OLD", "NA", date(2024, 1, 5), date(2024, 1, 4), despatch_date_key=date(2024, 1, 3))]
    out = buildWeeklyDeliveryPerformance(_fact(spark, rows), date(2024, 2, 1), 42)
    byKey = {(r["region_code"], r["carrier_key"]): r.asDict() for r in out.collect()}
    assert len(byKey) == 4 and all(r["iso_week_start_date"] == date(2024, 3, 3) for r in byKey.values())
    eu = byKey[("EU", 1)]
    assert eu["consignment_count"] == 4 and eu["package_count"] == 8 and eu["delivered_count"] == 4 and eu["on_time_count"] == 1
    assert eu["late_count"] == 3 and eu["damaged_count"] == 1 and eu["service_level_code"] == "STD"
    assert eu["on_time_percent"] == Decimal("25.0000") and eu["damage_rate_percent"] == Decimal("25.0000")
    assert eu["sla_target_percent"] == Decimal("98.0000") and eu["sla_breach_flag"] is True
    assert eu["service_credit_reporting"] == Decimal("40.00") and eu["cost_per_consignment"] == Decimal("100.0000")
    assert eu["cost_per_chargeable_kg"] == Decimal("10.0000") and eu["refresh_batch_id"] == 42
    eu9 = byKey[("EU", 9)]
    assert eu9["lost_count"] == 1 and eu9["delivered_count"] == 2 and eu9["on_time_percent"] == Decimal("100.0000")
    assert eu9["sla_breach_flag"] is False and eu9["service_credit_reporting"] == Decimal("0.00")
    na = byKey[("NA", 1)]
    assert (
        na["on_time_percent"] == Decimal("50.0000")
        and na["sla_breach_flag"] is True
        and na["service_credit_reporting"] == Decimal("0.00")
    )
    apac = byKey[("APAC", 1)]
    assert apac["on_time_count"] == 1 and apac["sla_target_percent"] == Decimal("92.0000") and apac["sla_breach_flag"] is False
    assert apac["average_customs_hold_days"] == Decimal("3.00")
    # a small EU miss stays below the cap: 97 % vs 98 -> 1 pt * 2 % = 2 % of freight
    small = [_row(f"S-{i}", "EU", date(2024, 3, 9), date(2024, 3, 10), carrier_key=5) for i in range(97)]
    small += [_row(f"SL-{i}", "EU", date(2024, 3, 11), date(2024, 3, 10), carrier_key=5) for i in range(3)]
    smallOut = buildWeeklyDeliveryPerformance(_fact(spark, small), None, 1).collect()[0]
    assert smallOut["on_time_percent"] == Decimal("97.0000") and smallOut["service_credit_reporting"] == Decimal("200.00")


def test_lane_tiers_and_thin_sample_reject(spark):
    rows = [_row(f"L-{i}", "EU", date(2024, 3, 9), date(2024, 3, 10)) for i in range(5)] + [
        _row("T-0", "NA", date(2024, 3, 9), date(2024, 3, 10))
    ]
    rows += [_row("X", "EU", None, date(2024, 3, 10))]
    lanes = spark.createDataFrame(
        [(f"L-{i}", "DHL", "DE", "FR") for i in range(5)] + [("T-0", "UPS", "US", "US")],
        "despatch_note_number string, carrier_code string, origin_country_iso_code string, destination_country_iso_code string",
    )
    kept, thin = buildLaneDeliveryPerformance(_fact(spark, rows), lanes, date(2024, 3, 1), date(2024, 3, 31))
    keptRow = kept.collect()
    assert len(keptRow) == 1 and keptRow[0]["carrier_code"] == "DHL" and keptRow[0]["shipment_count"] == 5
    assert keptRow[0]["service_tier_code"] == "PLATINUM" and keptRow[0]["is_cross_border_lane"] is True
    assert keptRow[0]["freight_per_kg"] == Decimal("10.00")
    thinRow = thin.collect()
    assert len(thinRow) == 1 and thinRow[0]["carrier_code"] == "UPS" and thinRow[0]["is_cross_border_lane"] is False
    assert thinRow[0]["reject_reason"].startswith("Aggregate row failed")


def test_refresh_window():
    assert refreshWindowStart(date(2024, 3, 15), 6, False) == date(2024, 2, 2)
    assert refreshWindowStart(date(2024, 3, 15), 6, True) is None
