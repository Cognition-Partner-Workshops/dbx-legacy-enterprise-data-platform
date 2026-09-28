"""Pipeline configuration and Unity Catalog name resolution."""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

LAYER_SCHEMAS: dict[str, str] = {
    "bronze": "sales_bronze",
    "silver": "sales_silver",
    "gold": "sales_gold",
    "quality": "sales_quality",
}

REGION_CODES: tuple[str, ...] = ("NA", "EU", "APAC")
REPORTING_CURRENCY: str = "USD"


@dataclass(frozen=True)
class PipelineConfig:
    """Everything a layer needs to know about where it reads and writes.

    ``catalog`` is None locally (two-level names on ``spark_catalog``) and the
    Unity Catalog name on Databricks.
    """

    catalog: str | None = None
    mockDataRoot: str = "mock_data/output"
    batchId: int = field(default_factory=lambda: int(time.time()))
    reportingCurrency: str = REPORTING_CURRENCY
    schemaOverrides: dict[str, str] = field(default_factory=dict)

    def schema(self, layer: str) -> str:
        name = self.schemaOverrides.get(layer, LAYER_SCHEMAS[layer])
        return f"{self.catalog}.{name}" if self.catalog else name

    def fqn(self, layer: str, table: str) -> str:
        return f"{self.schema(layer)}.{table}"

    def sourcePath(self, system: str, schema: str, table: str) -> str:
        return os.path.join(self.mockDataRoot, system, schema, f"{table}.csv")

    @staticmethod
    def fromEnv() -> "PipelineConfig":
        """Build from env vars / Databricks widgets-friendly settings."""
        catalog = os.environ.get("SALES_LAKEHOUSE_CATALOG") or None
        root = os.environ.get("SALES_LAKEHOUSE_MOCK_ROOT", "mock_data/output")
        batchRaw = os.environ.get("SALES_LAKEHOUSE_BATCH_ID")
        batchId = int(batchRaw) if batchRaw else int(time.time())
        return PipelineConfig(catalog=catalog, mockDataRoot=root, batchId=batchId)
