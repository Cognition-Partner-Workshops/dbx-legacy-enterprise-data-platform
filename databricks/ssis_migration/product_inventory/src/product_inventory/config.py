"""Runtime configuration for the product_inventory bundle.

All table names are resolved through `PipelineConfig.fqn` so the same code runs on
Databricks (three-level Unity Catalog names) and in local pytest sessions.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from typing import Mapping, Optional

BRANCH = "ssis_product_inventory"
ACTOR = "devin:ssis_product_inventory"
HARNESS_VERSION = "ssis-migration-v1"
FAR_FUTURE = datetime(9999, 12, 31, 23, 59, 59)


def farFutureLit():
    """Far-future timestamp as a server-side literal (Python datetime literals overflow Arrow at year 9999)."""
    from pyspark.sql import functions as F

    return F.to_timestamp(F.lit(FAR_FUTURE.strftime("%Y-%m-%d %H:%M:%S")))


def parseBool(value: str) -> bool:
    return str(value).strip().lower() in ("1", "true", "t", "yes", "y")


def parseOptionalDate(value: str) -> Optional[date]:
    text = (value or "").strip()
    if not text or text.lower() in ("auto", "none", "null"):
        return None
    return date.fromisoformat(text[:10])


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = "otterorders_migration"
    schema: str = "ssis_product_inventory"
    evidenceSchema: str = "evidence"
    oltpCatalog: str = "wwi_legacy_oltp"
    stagingCatalog: str = "wwi_legacy_staging"
    dwCatalog: str = "wwi_legacy_dw"
    oracleCatalog: str = "wwi_legacy_oracle"
    sqlServerConnection: str = "wwi_legacy_sqlserver"
    dwDatabase: str = "WideWorldImportersDW"
    stagingDatabase: str = "WideWorldImporters_Staging"
    batchId: int = 0
    businessDate: Optional[date] = None
    reloadFullHistory: bool = True
    gitSha: str = "unknown"
    # package parameters (defaults are the .dtsx defaults)
    positionWindowDays: int = 90
    countToleranceUnits: int = 2
    countToleranceValue: float = 50.0
    coverDays: int = 21
    suppressChillerSuggestions: bool = False
    inTransitAgeAlertDays: int = 10
    staleTransitDays: int = 14
    timingWindowMinutes: int = 30
    siteScope: str = "ALL"
    retentionDays: int = 400
    stockOutThreshold: int = 0
    stockItemLookbackMinutes: int = 240
    extra: Mapping[str, str] = field(default_factory=dict)

    def fqn(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"

    def evidenceFqn(self, table: str) -> str:
        return f"{self.catalog}.{self.evidenceSchema}.{table}"

    def oltp(self, schema: str, table: str) -> str:
        return f"{self.oltpCatalog}.{schema}.{table}"

    def staging(self, schema: str, table: str) -> str:
        return f"{self.stagingCatalog}.{schema}.{table}"

    def dw(self, schema: str, table: str) -> str:
        return f"{self.dwCatalog}.{schema}.`{table}`"

    def oracle(self, schema: str, table: str) -> str:
        return f"{self.oracleCatalog}.{schema}.{table}"

    def withBusinessDate(self, businessDate: date) -> "PipelineConfig":
        return replace(self, businessDate=businessDate)


def defaultBatchId(now: Optional[datetime] = None) -> int:
    current = now or datetime.now(timezone.utc)
    return int(current.strftime("%Y%m%d%H"))


def configFromParams(params: Mapping[str, str]) -> PipelineConfig:
    """Build a config from job/notebook widget values (all strings)."""

    def get(name: str, default: str) -> str:
        value = params.get(name)
        return default if value is None or str(value).strip() == "" else str(value)

    batchText = get("batch_id", "")
    return PipelineConfig(
        catalog=get("catalog", "otterorders_migration"),
        schema=get("schema", "ssis_product_inventory"),
        batchId=int(batchText) if batchText else defaultBatchId(),
        businessDate=parseOptionalDate(get("business_date", "")),
        reloadFullHistory=parseBool(get("reload_full_history", "true")),
        gitSha=get("git_sha", "unknown"),
        positionWindowDays=int(get("position_window_days", "90")),
        countToleranceUnits=int(get("count_tolerance_units", "2")),
        countToleranceValue=float(get("count_tolerance_value", "50")),
        coverDays=int(get("cover_days", "21")),
        suppressChillerSuggestions=parseBool(get("suppress_chiller_suggestions", "false")),
        inTransitAgeAlertDays=int(get("in_transit_age_alert_days", "10")),
        timingWindowMinutes=int(get("timing_window_minutes", "30")),
        siteScope=get("site_scope", "ALL"),
        retentionDays=int(get("retention_days", "400")),
        stockOutThreshold=int(get("stock_out_threshold", "0")),
        extra=dict(params),
    )
