"""Publication logic for AGG_Publish_ReportingLayer (Integration.usp_PublishReportingLayer)."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import Row, SparkSession

from agg_common import LEGACY_OBJECT_NAMES, tableExists
from rpt_views import REPORT_SOURCES, REPORT_VIEWS, VIEW_BUILDERS, resolvePublicationList

PUBLISH_SCOPE_CODE = "DW"
PUBLISH_STATE_OBJECT = "Report.PublishState"


@dataclass
class PublishRuleResult:
    ruleName: str
    passed: bool
    detail: str


@dataclass
class StalenessResult:
    objectName: str
    watermarkTo: Optional[dt.datetime]
    stalenessHours: float
    isStale: bool


@dataclass
class PublicationPlan:
    groupCode: str
    views: List[str]
    source: str                       # "int_reporting_publication" | "configuration" | "default"
    stampRows: List[Row] = field(default_factory=list)


def stalenessHours(watermarkTo: Optional[dt.datetime], now: dt.datetime) -> float:
    if watermarkTo is None:
        return float("inf")
    return (now - watermarkTo).total_seconds() / 3600.0


def evaluateStaleness(watermarks: Dict[str, Optional[dt.datetime]], now: dt.datetime,
                      maxStalenessHours: float) -> List[StalenessResult]:
    """Check Aggregate Staleness / Quarantine Stale Publications: every
    ``Aggregate.*`` watermark older than ``maxStalenessHours`` is stale."""
    results = []
    for objectName, wm in watermarks.items():
        hours = stalenessHours(wm, now)
        results.append(StalenessResult(objectName, wm, hours, hours > maxStalenessHours))
    return results


def requiredAggregatesFor(views: Sequence[str]) -> List[str]:
    names: List[str] = []
    for v in views:
        for src in REPORT_SOURCES.get(v, []):
            if src not in names:
                names.append(src)
    return names


def readPublicationPlan(spark: SparkSession, t: Dict[str, str], groupCode: str,
                        configuredList: Optional[str]) -> PublicationPlan:
    """Integration.ReportingPublication (silver.int_reporting_publication) wins
    when present; otherwise the etl.Configuration list; otherwise every view."""
    pubTable = t["int_reporting_publication"]
    if tableExists(spark, pubTable):
        rows = spark.sql(
            f"SELECT PublishSequence, TargetObjectName, SourceObjectName FROM {pubTable} "
            f"WHERE PublicationGroupCode = '{groupCode}' AND IsEnabled = TRUE ORDER BY PublishSequence"
        ).collect()
        if rows:
            names = ",".join(_legacyTargetName(r["TargetObjectName"]) for r in rows)
            return PublicationPlan(groupCode, resolvePublicationList(names), "int_reporting_publication", rows)
    if configuredList:
        return PublicationPlan(groupCode, resolvePublicationList(configuredList), "configuration")
    return PublicationPlan(groupCode, resolvePublicationList(""), "default")


def _legacyTargetName(targetObjectName: str) -> str:
    name = targetObjectName.strip().strip("[]")
    if name.startswith("Report."):
        return name
    if name.startswith("vw_"):
        return f"Report.{name}"
    return name


def evaluatePublishRules(spark: SparkSession, t: Dict[str, str], expectedDate: dt.date,
                         asOfDate: dt.date) -> List[PublishRuleResult]:
    """The three publish rules of Integration.usp_PublishReportingLayer."""
    results: List[PublishRuleResult] = []

    def exists(sql: str) -> bool:
        return spark.sql(f"SELECT COUNT(*) AS c FROM ({sql}) q").collect()[0]["c"] > 0

    dailySales = t["agg_daily_sales_summary"]
    hasSales = tableExists(spark, dailySales) and exists(
        f"SELECT 1 FROM {dailySales} WHERE sales_date = DATE'{expectedDate}' LIMIT 1")
    results.append(PublishRuleResult(
        "DailySalesPresent", hasSales,
        f"{LEGACY_OBJECT_NAMES['agg_daily_sales_summary']} has rows for {expectedDate}"))

    inventory = t["agg_daily_inventory_health"]
    hasInventory = tableExists(spark, inventory) and exists(
        f"SELECT 1 FROM {inventory} WHERE snapshot_date = DATE'{expectedDate}' LIMIT 1")
    results.append(PublishRuleResult(
        "DailyInventoryPresent", hasInventory,
        f"{LEGACY_OBJECT_NAMES['agg_daily_inventory_health']} has rows for {expectedDate}"))

    c360 = t["agg_customer_360"]
    privacyBreach = tableExists(spark, c360) and exists(
        f"SELECT 1 FROM {c360} WHERE region_code = 'EU' AND NOT COALESCE(anonymised_flag, FALSE) "
        f"AND retention_expiry_date < DATE'{asOfDate}' LIMIT 1")
    results.append(PublishRuleResult(
        "EuRetentionAnonymised", not privacyBreach,
        "no EU customer past retention expiry remains un-anonymised"))
    return results


def publishDecision(failedRules: int, forcePublish: bool) -> Tuple[bool, str]:
    if failedRules == 0:
        return True, "OK"
    if forcePublish:
        return True, "FORCED"
    return False, "BLOCKED"


def ensurePublishStateTable(spark: SparkSession, table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            publish_scope_code STRING NOT NULL,
            published_batch_id BIGINT,
            published_datetime TIMESTAMP,
            publish_status_code STRING,
            failed_rule_count INT,
            published_object_count INT,
            publication_group_code STRING,
            package_execution_id BIGINT
        ) USING DELTA
    """)


def updatePublishState(spark: SparkSession, table: str, batchId: int, statusCode: str, failedRules: int,
                       publishedObjectCount: int, groupCode: str, packageExecutionId: Optional[int]) -> int:
    """Report.PublishState flip: one row per scope, updated in place (MERGE)."""
    ensurePublishStateTable(spark, table)
    execId = "NULL" if packageExecutionId is None else str(int(packageExecutionId))
    spark.sql(f"""
        MERGE INTO {table} AS s
        USING (SELECT '{PUBLISH_SCOPE_CODE}' AS publish_scope_code) AS n
        ON s.publish_scope_code = n.publish_scope_code
        WHEN MATCHED THEN UPDATE SET
            published_batch_id = {int(batchId)}, published_datetime = current_timestamp(),
            publish_status_code = '{statusCode}', failed_rule_count = {int(failedRules)},
            published_object_count = {int(publishedObjectCount)}, publication_group_code = '{groupCode}',
            package_execution_id = {execId}
        WHEN NOT MATCHED THEN INSERT
            (publish_scope_code, published_batch_id, published_datetime, publish_status_code, failed_rule_count,
             published_object_count, publication_group_code, package_execution_id)
        VALUES ('{PUBLISH_SCOPE_CODE}', {int(batchId)}, current_timestamp(), '{statusCode}', {int(failedRules)},
                {int(publishedObjectCount)}, '{groupCode}', {execId})
    """)
    return 1


def stampPublicationMetadata(spark: SparkSession, pubTable: str, groupCode: str,
                             packageExecutionId: Optional[int]) -> int:
    if not tableExists(spark, pubTable):
        return 0
    execId = "NULL" if packageExecutionId is None else str(int(packageExecutionId))
    spark.sql(f"UPDATE {pubTable} SET LastPublishedAt = current_timestamp(), "
              f"LastPublishedByExecutionId = {execId} "
              f"WHERE PublicationGroupCode = '{groupCode}' AND IsEnabled = TRUE")
    return spark.sql(f"SELECT COUNT(*) AS c FROM {pubTable} WHERE PublicationGroupCode = '{groupCode}' "
                     f"AND IsEnabled = TRUE").collect()[0]["c"]


def legacyReportName(viewName: str) -> str:
    for legacy, rpt in REPORT_VIEWS.items():
        if rpt == viewName:
            return legacy
    raise KeyError(viewName)


def viewSelectSql(viewName: str, t: Dict[str, str]) -> str:
    return VIEW_BUILDERS[viewName](t)
