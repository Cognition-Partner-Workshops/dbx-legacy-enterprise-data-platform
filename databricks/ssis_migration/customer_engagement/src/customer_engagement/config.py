"""Runtime configuration shared by every package task (catalog/schema/source names, run metadata)."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GROUP_SLUG = "customer_engagement"
BRANCH = "ssis_customer_engagement"
ACTOR = "devin:ssis_customer_engagement"
HARNESS_VERSION = "ssis-migration-v1"


@dataclass(frozen=True)
class CeConfig:
    catalog: str = "otterorders_migration"
    schema: str = "ssis_customer_engagement"
    evidenceSchema: str = "evidence"
    oltpCatalog: str = "wwi_legacy_oltp"
    stagingCatalog: str = "wwi_legacy_staging"
    dwCatalog: str = "wwi_legacy_dw"
    # "legacy_raw" reads the staging packages' declared source (wwi_legacy_staging.raw.*);
    # "bronze" reads the bronze tables written by the EXT_* tasks in this schema.
    stagingSourceMode: str = "legacy_raw"
    # "auto" resolves to MAX(Fact.Sale.[Invoice Date Key]) so the rolling windows anchored on
    # SYSDATETIME() in SSIS are anchored on the frozen legacy baseline instead.
    asOfDate: str = "auto"
    windowStart: str = "auto"
    windowEnd: str = "auto"
    defaultRegion: str = "NA"
    batchId: int = 0
    runId: str = ""
    gitSha: str = ""
    extra: dict = field(default_factory=dict)

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def evidenceTable(self, name: str) -> str:
        return f"{self.catalog}.{self.evidenceSchema}.{name}"

    def oltp(self, schema: str, name: str) -> str:
        return f"{self.oltpCatalog}.{schema}.{name}"

    def staging(self, schema: str, name: str) -> str:
        return f"{self.stagingCatalog}.{schema}.{name}"

    def dw(self, schema: str, name: str) -> str:
        return f"{self.dwCatalog}.{schema}.`{name}`"

    def withAsOf(self, asOf: date) -> "CeConfig":
        return replace(self, asOfDate=asOf.isoformat())


def resolveAsOfDate(spark: SparkSession, cfg: CeConfig) -> date:
    """Resolve the ``as of`` anchor date (explicit ISO date or the latest legacy invoice date)."""
    if cfg.asOfDate and cfg.asOfDate != "auto":
        return date.fromisoformat(cfg.asOfDate)
    row = spark.table(cfg.dw("fact", "Sale")).agg(F.max("Invoice Date Key").alias("d")).first()
    resolved: Optional[date] = row["d"] if row else None
    return resolved or date.today()
