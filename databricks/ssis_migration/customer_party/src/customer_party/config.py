"""Runtime configuration for the customer_party bundle.

Names are resolved through `PipelineConfig` so the same library runs on the
workspace (three-level Unity Catalog names) and on local Spark in pytest
(two-level `spark_catalog` names).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

GROUP_SLUG = "customer_party"
BRANCH_TAG = "ssis_customer_party"
ACTOR = "devin:ssis_customer_party"
HARNESS_VERSION = "ssis-migration-v1"

LEGACY_OLTP = "wwi_legacy_oltp"
LEGACY_STAGING = "wwi_legacy_staging"
LEGACY_DW = "wwi_legacy_dw"
LEGACY_ORACLE = "wwi_legacy_oracle"

HIGH_DATE = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
LOW_DATE = datetime(1900, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class PipelineConfig:
    """Where the group reads from and writes to."""

    catalog: str = "otterorders_migration"
    schema: str = "ssis_customer_party"
    evidenceSchema: str = "evidence"
    batchId: int = 1
    reloadFullHistory: bool = False
    lookbackMinutes: int = 120
    gitSha: str = "unknown"
    isLocal: bool = False
    tablePrefix: str = ""
    extraTags: dict[str, str] = field(default_factory=dict)

    def fqn(self, table: str) -> str:
        """Fully qualified name of a table in the group's landing schema."""
        name = f"{self.tablePrefix}{table}"
        if self.isLocal:
            return f"{self.schema}.{name}"
        return f"{self.catalog}.{self.schema}.{name}"

    def evidenceFqn(self, table: str) -> str:
        if self.isLocal:
            return f"{self.evidenceSchema}.{table}"
        return f"{self.catalog}.{self.evidenceSchema}.{table}"

    def legacy(self, catalog: str, schema: str, table: str) -> str:
        """Three-level federated name for a legacy object (schema/table may contain spaces)."""
        return f"{catalog}.{schema}.`{table}`"


def utcNow() -> datetime:
    return datetime.now(tz=timezone.utc)
