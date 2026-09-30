"""Runtime configuration shared by every task in the bundle."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

OWNED_PACKAGES = {
    "orchestration": [
        "Master_Daily_ETL",
        "Master_Hourly_Incremental",
        "Master_Intraday_Inventory",
        "Master_Customer_Sync",
        "Master_File_Ingestion",
        "Master_Finance_Close",
        "Master_Month_End",
        "Master_Weekly_Maintenance",
        "Master_Weekly_Reference_Load",
    ],
    "quality": [
        "DQ_Rule_Engine",
        "DQ_Threshold_Gate",
        "DQ_Referential_Screen",
        "DQ_Reject_Reprocess",
        "DQ_File_Screen",
        "ING_FILE_QuarantineMalformed",
    ],
    "error_handling": [
        "ERR_Handle_PackageFailure",
        "ERR_Notify_Operations",
        "ERR_Quarantine_BadFiles",
        "ERR_Reconcile_RowCounts",
        "ERR_Retry_FailedSteps",
        "ERR_Route_RejectedRows",
    ],
    "maintenance": [
        "MNT_Archive_ProcessedFiles",
        "MNT_Check_DiskSpace",
        "MNT_Purge_ControlHistory",
        "MNT_Purge_StagingHistory",
        "MNT_Rebuild_Indexes",
        "MNT_Update_Statistics",
        "MNT_Validate_Configuration",
    ],
}

ALL_OWNED_PACKAGES = [p for group in OWNED_PACKAGES.values() for p in group]

GROUP_SLUG = "platform_control"
BRANCH = "ssis_platform_control"
ACTOR = "devin:ssis_platform_control"
HARNESS_VERSION = "ssis-migration-v1"


@dataclass
class PlatformConfig:
    """Where the control tables live and where the legacy baseline is read from.

    ``isLocal`` switches to a two-level (schema.table) namespace so the same code runs on
    a local Delta-enabled SparkSession in pytest.
    """

    catalog: str = "otterorders_migration"
    schema: str = "ssis_platform_control"
    evidenceSchema: str = "evidence"
    legacyStagingCatalog: str = "wwi_legacy_staging"
    legacyDwCatalog: str = "wwi_legacy_dw"
    environmentCode: str = "DEV"
    volumeName: str = "landing"
    gitSha: str = "unknown"
    isLocal: bool = False
    siblingDispatchOnMissing: str = "skip"
    extra: dict = field(default_factory=dict)

    @classmethod
    def fromEnv(cls, **overrides) -> "PlatformConfig":
        values = dict(
            catalog=os.environ.get("PC_CATALOG", "otterorders_migration"),
            schema=os.environ.get("PC_SCHEMA", "ssis_platform_control"),
            evidenceSchema=os.environ.get("PC_EVIDENCE_SCHEMA", "evidence"),
            legacyStagingCatalog=os.environ.get("PC_LEGACY_STAGING_CATALOG", "wwi_legacy_staging"),
            legacyDwCatalog=os.environ.get("PC_LEGACY_DW_CATALOG", "wwi_legacy_dw"),
            environmentCode=os.environ.get("PC_ENVIRONMENT_CODE", "DEV"),
            gitSha=os.environ.get("PC_GIT_SHA", "unknown"),
            siblingDispatchOnMissing=os.environ.get("PC_SIBLING_DISPATCH_ON_MISSING", "skip"),
        )
        values.update(overrides)
        return cls(**values)

    @property
    def schemaFqn(self) -> str:
        return self.schema if self.isLocal else f"{self.catalog}.{self.schema}"

    def table(self, name: str) -> str:
        """Fully qualified name of one of our Delta tables."""
        return f"{self.schemaFqn}.{name}"

    def evidenceTable(self, name: str = "recon_results") -> str:
        if self.isLocal:
            return f"{self.evidenceSchema}.{name}"
        return f"{self.catalog}.{self.evidenceSchema}.{name}"

    def legacyStaging(self, schemaName: str, tableName: str) -> str:
        return f"{self.legacyStagingCatalog}.{schemaName}.`{tableName}`"

    def legacyDw(self, schemaName: str, tableName: str) -> str:
        return f"{self.legacyDwCatalog}.{schemaName}.`{tableName}`"

    @property
    def volumeRoot(self) -> str:
        if self.isLocal:
            return self.extra.get("volumeRoot", "/tmp/platform_control_volume")
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volumeName}"
