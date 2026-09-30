"""Package dispatch used by the thin notebooks: package name -> library entry point."""

from __future__ import annotations

from datetime import date
from typing import Callable, Dict, Tuple

from pyspark.sql import SparkSession

from customer_engagement import aggregates, customer360, extracts, facts, staging
from customer_engagement.config import CeConfig, resolveAsOfDate

Runner = Callable[[SparkSession, CeConfig, date], Tuple[int, int]]

RUNNERS: Dict[str, Runner] = {
    "EXT_SQL_LoyaltyLedger": lambda spark, cfg, asOf: extracts.runExtLoyaltyLedger(spark, cfg),
    "EXT_SQL_WebSessions": lambda spark, cfg, asOf: extracts.runExtWebSessions(spark, cfg),
    "STG_Load_LoyaltyLedger": lambda spark, cfg, asOf: staging.runStgLoyaltyLedger(spark, cfg),
    "STG_Load_WebSession": lambda spark, cfg, asOf: staging.runStgWebSession(spark, cfg),
    "FACT_Load_LoyaltyPoints": lambda spark, cfg, asOf: facts.runFactLoyaltyPoints(spark, cfg),
    "FACT_Load_WebSession": lambda spark, cfg, asOf: facts.runFactWebSession(spark, cfg),
    "C360_Build_CustomerProfile": customer360.runBuildCustomerProfile,
    "C360_Build_LoyaltyOverlay": customer360.runBuildLoyaltyOverlay,
    "C360_Build_RollingMetrics": customer360.runBuildRollingMetrics,
    "C360_Build_ChurnFlags": customer360.runBuildChurnFlags,
    "C360_Publish_Segments": customer360.runPublishSegments,
    "AGG_Refresh_Customer360": lambda spark, cfg, asOf: aggregates.runAggRefreshCustomer360(spark, cfg, asOf),
    "AGG_Refresh_CustomerRolling12Month": lambda spark, cfg, asOf: aggregates.runAggRefreshCustomerRolling12Month(spark, cfg, asOf),
}


def runPackage(spark: SparkSession, cfg: CeConfig, packageName: str) -> Dict[str, object]:
    if packageName not in RUNNERS:
        raise ValueError(f"unknown package {packageName!r}; expected one of {sorted(RUNNERS)}")
    asOf = resolveAsOfDate(spark, cfg)
    inserted, rejected = RUNNERS[packageName](spark, cfg, asOf)
    return {"package": packageName, "as_of_date": asOf.isoformat(), "rows_inserted": inserted, "rows_rejected": rejected}
