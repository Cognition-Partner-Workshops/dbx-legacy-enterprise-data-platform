from datetime import date, datetime, timedelta

import maintenance_lib as mnt


def test_retention_rules_follow_legacy_case():
    assert mnt.retentionDaysFor("bronze", "raw_fin_ledger", 90) == 2555
    assert mnt.retentionDaysFor("bronze", "raw_partner_orders", 90) == 365
    assert mnt.retentionDaysFor("silver", "stg_partner_orders", 90) == 365
    assert mnt.retentionDaysFor("silver", "work_customer_delta", 90) == 14
    assert mnt.retentionDaysFor("silver", "stg_customer", 90) == 90
    assert mnt.retentionDaysFor("silver", "err_customer", 45) == 45


def test_delta_name_mapping():
    assert mnt.deltaName("raw", "FinLedger") == ("bronze", "raw_fin_ledger")
    assert mnt.deltaName("stg", "Customer") == ("silver", "stg_customer")
    assert mnt.deltaName("Fact", "Sale") == ("gold", "fact_sale")
    assert mnt.deltaName("Dimension", "Stock Item") == ("gold", "dim_stock_item")
    assert mnt.deltaName("Integration", "IndexMaintenancePlan") == ("silver", "int_index_maintenance_plan")


def test_staging_candidates():
    assert mnt.isStagingPurgeCandidate("bronze", "raw_customer")
    assert mnt.isStagingPurgeCandidate("silver", "err_customer")
    assert mnt.isStagingPurgeCandidate("silver", "work_plan")
    assert not mnt.isStagingPurgeCandidate("silver", "ref_country")
    assert not mnt.isStagingPurgeCandidate("gold", "fact_sale")


def test_purge_column_and_predicate():
    cols = ["CustomerId", "BatchId", "LoadedAtUtc"]
    assert mnt.choosePurgeColumn(cols) == "LoadedAtUtc"
    assert mnt.choosePurgeColumn(cols, "loadedatutc") == "LoadedAtUtc"
    assert mnt.choosePurgeColumn(["BusinessDate", "X"]) == "BusinessDate"
    assert mnt.choosePurgeColumn(["A", "B"]) is None
    cutoff = date(2024, 1, 31)
    assert mnt.purgePredicate("c", "LoadedAtUtc", True, cutoff) == "`LoadedAtUtc` < TIMESTAMP'2024-01-31 00:00:00'"
    byBatch = mnt.purgePredicate("c", None, True, cutoff)
    assert "c.etl.batch" in byBatch and "BusinessDate < DATE'2024-01-31'" in byBatch
    assert mnt.purgePredicate("c", None, False, cutoff) is None


def test_cutoff_and_months():
    assert mnt.cutoffDate(date(2024, 3, 1), 90) == date(2023, 12, 2)
    assert mnt.monthsFromDays(400) == 13
    assert mnt.monthsFromDays(180) == 6
    assert mnt.monthsFromDays(90) == 3
    assert mnt.monthsFromDays(5) == 1


def test_vacuum_and_retention_parsing():
    assert mnt.vacuumStatement("c.s.t", None) == "VACUUM `c`.`s`.`t`"
    assert mnt.vacuumStatement("c.s.t", 240) == "VACUUM `c`.`s`.`t` RETAIN 240 HOURS"
    assert mnt.deletedFileRetentionHours({}) == 168
    assert mnt.deletedFileRetentionHours({"delta.deletedFileRetentionDuration": "interval 30 days"}) == 720
    assert mnt.deletedFileRetentionHours({"delta.deletedFileRetentionDuration": "interval 12 hours"}) == 12
    assert mnt.deletedFileRetentionHours({"delta.deletedFileRetentionDuration": "garbage"}) == 168


def test_fragmentation_and_optimize_plan():
    assert mnt.fragmentationPercent(1, 10**9) == 0.0
    assert mnt.fragmentationPercent(100, 100 * 128 * 1024 * 1024) == 0.0
    assert mnt.fragmentationPercent(100, 100 * 64 * 1024 * 1024) == 50.0
    small = {"numFiles": 50, "sizeInBytes": 50 * 1024, "clusteringColumns": []}
    assert mnt.planOptimizeAction("gold", "fact_sale", ["BatchId"], small, 10, 30)["PlannedAction"] == "NONE"
    fragmented = {"numFiles": 100, "sizeInBytes": 100 * 64 * 1024 * 1024, "clusteringColumns": []}
    plan = mnt.planOptimizeAction("gold", "fact_sale", ["SaleKey", "InvoiceDateKey", "DeliveryDateKey", "BatchId"],
                                  fragmented, 10, 30)
    assert plan["PlannedAction"] == "REBUILD"
    assert plan["ZorderColumns"] == ["InvoiceDateKey", "DeliveryDateKey", "BatchId"]
    assert mnt.optimizeStatement("c.gold.fact_sale", "REBUILD", plan["ZorderColumns"]) == \
        "OPTIMIZE `c`.`gold`.`fact_sale` ZORDER BY (`InvoiceDateKey`, `DeliveryDateKey`, `BatchId`)"
    mild = {"numFiles": 100, "sizeInBytes": 100 * 110 * 1024 * 1024, "clusteringColumns": []}
    plan = mnt.planOptimizeAction("silver", "stg_customer", ["BatchId"], mild, 10, 30)
    assert plan["PlannedAction"] == "REORGANIZE" and plan["ZorderColumns"] == []
    assert mnt.optimizeStatement("c.silver.stg_customer", "REORGANIZE", []) == "OPTIMIZE `c`.`silver`.`stg_customer`"
    clustered = dict(fragmented, clusteringColumns=["InvoiceDateKey"])
    plan = mnt.planOptimizeAction("gold", "fact_sale", ["InvoiceDateKey"], clustered, 10, 30)
    assert plan["IndexTypeCode"] == "LIQUID" and plan["ZorderColumns"] == []


def test_zorder_keys_per_layer():
    assert mnt.zorderColumnsFor("gold", "dim_customer", ["CustomerKey", "WWICustomerID", "ValidFrom"]) == \
        ["WWICustomerID", "ValidFrom"]
    assert mnt.zorderColumnsFor("bronze", "raw_orders", ["OrderId", "BatchId", "BusinessDate"]) == ["BatchId", "BusinessDate"]
    assert mnt.zorderColumnsFor("silver", "ref_country", ["CountryCode"]) == []


def test_deadline():
    start = datetime(2024, 1, 1, 22, 0)
    assert not mnt.deadlineReached(start, 180, now=start + timedelta(minutes=179))
    assert mnt.deadlineReached(start, 180, now=start + timedelta(minutes=180))


def test_statistics_rules():
    assert mnt.statisticsRefreshMode("gold", "dim_customer") == "FULLSCAN"
    assert mnt.statisticsRefreshMode("gold", "fact_sale") == "SAMPLE"
    assert mnt.analyzeStatement("c.gold.dim_customer", "FULLSCAN") == \
        "ANALYZE TABLE `c`.`gold`.`dim_customer` COMPUTE STATISTICS FOR ALL COLUMNS"
    assert mnt.analyzeStatement("c.gold.fact_sale", "SAMPLE") == "ANALYZE TABLE `c`.`gold`.`fact_sale` COMPUTE STATISTICS"
    t0 = datetime(2024, 1, 1)
    history = [
        {"timestamp": t0 + timedelta(days=2), "operation": "MERGE",
         "operationMetrics": {"numTargetRowsInserted": "100", "numTargetRowsUpdated": "50"}},
        {"timestamp": t0 + timedelta(days=1), "operation": "OPTIMIZE", "operationMetrics": {"numOutputRows": "9999"}},
        {"timestamp": t0, "operation": "WRITE", "operationMetrics": {"numOutputRows": "1000"}},
    ]
    assert mnt.modifiedRowsSince(history, None) == 1150
    assert mnt.modifiedRowsSince(history, t0) == 150


def test_archive_paths_and_settlement():
    asOf = datetime(2024, 3, 5, 12, 0)
    assert mnt.archiveFolder("/Volumes/wwi_dev/bronze/landing/", asOf) == "/Volumes/wwi_dev/bronze/landing/archive/2024/03"
    assert mnt.processedFolder("/Volumes/wwi_dev/bronze/landing") == "/Volumes/wwi_dev/bronze/landing/inbound/processed"
    sevenHoursAgo = int((asOf - timedelta(hours=7) - datetime(1970, 1, 1)).total_seconds() * 1000)
    oneHourAgo = int((asOf - timedelta(hours=1) - datetime(1970, 1, 1)).total_seconds() * 1000)
    assert mnt.fileIsSettled(sevenHoursAgo, asOf, 6)
    assert not mnt.fileIsSettled(oneHourAgo, asOf, 6)


def test_volume_evaluation():
    gb = 1024 ** 3
    ok = mnt.evaluateVolume(usedBytes=200 * gb, budgetGb=1000, projectedBytes=10 * gb, minimumFreePercent=15)
    assert ok["SeverityCode"] == "OK" and not ok["HasShortfall"] and ok["FreePercent"] == 80
    warn = mnt.evaluateVolume(usedBytes=900 * gb, budgetGb=1000, projectedBytes=10 * gb, minimumFreePercent=15)
    assert warn["SeverityCode"] == "WARNING" and warn["HasShortfall"]
    crit = mnt.evaluateVolume(usedBytes=950 * gb, budgetGb=1000, projectedBytes=10 * gb, minimumFreePercent=15)
    assert crit["SeverityCode"] == "CRITICAL" and mnt.preflightCheckStatus("CRITICAL") == "FAILED"
    growth = mnt.evaluateVolume(usedBytes=700 * gb, budgetGb=1000, projectedBytes=400 * gb, minimumFreePercent=15)
    assert growth["HasShortfall"] and growth["SeverityCode"] == "OK" and growth["HeadroomGb"] == -100
    assert mnt.preflightStatus(0, 100) == "OK"
    assert mnt.preflightStatus(2, 12) == "WARNING"
    assert mnt.preflightStatus(1, 4) == "CRITICAL"
    assert mnt.projectedGrowthBytes([100, 200, None]) == int(150 * 1.33)
    assert mnt.projectedGrowthBytes([]) == 0


def test_configuration_checks():
    assert mnt.requiredKeyStatus(None) == "MISSING"
    assert mnt.requiredKeyStatus("  ") == "EMPTY"
    assert mnt.requiredKeyStatus("x") == "OK"
    asOf = date(2024, 6, 15)
    assert mnt.periodPlausibility("EU", "CAL", "202406", "EU_STD", asOf) == "OK"
    assert mnt.periodPlausibility("EU", "CAL", "202406", None, asOf) == "SUSPECT"
    assert mnt.periodPlausibility("APAC", "CAL", "202406", None, asOf) == "SUSPECT"
    assert mnt.periodPlausibility("APAC", "445", "202406", None, asOf) == "OK"
    assert mnt.periodPlausibility("NA", "CAL", "202403", None, asOf) == "SUSPECT"
    assert mnt.periodPlausibility("NA", "CAL", "202404", None, asOf) == "OK"
    assert mnt.periodPlausibility("NA", "CAL", None, None, asOf) == "MISSING"
    assert mnt.fxPlausibility(0) == "MISSING" and mnt.fxPlausibility(3) == "OK"
    results = [{"CheckStatus": "OK"}, {"CheckStatus": "MISSING"}, {"CheckStatus": "EMPTY"}, {"CheckStatus": "SUSPECT"}]
    assert mnt.countDefects(results) == (2, 1)
    assert mnt.configurationStatus(2, 1) == "DEFECTIVE"
    assert mnt.configurationStatus(0, 1) == "SUSPECT"
    assert mnt.configurationStatus(0, 0) == "OK"


def test_misc_helpers():
    assert mnt.toBool("true") and mnt.toBool("1") and not mnt.toBool("False") and not mnt.toBool(None)
    assert mnt.toInt("12", 0) == 12 and mnt.toInt("x", 7) == 7
    assert mnt.splitCsv(" a, b ,,c ") == ["a", "b", "c"]
    assert mnt.sqlString("O'Brien") == "'O''Brien'" and mnt.sqlString(None) == "NULL"
    assert mnt.quoted("wwi_dev.etl.batch") == "`wwi_dev`.`etl`.`batch`"
