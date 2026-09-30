"""Names, constants and the per-run context shared by every package module.

Python identifiers are camelCase (org convention); table and column names are snake_case.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession

GROUP_SLUG = "logistics_returns"
BRANCH = "ssis_logistics_returns"
ACTOR = "devin:ssis_logistics_returns"
HARNESS_VERSION = "ssis-migration-v1"
UNIT_TYPE = "ssis_package"

DEFAULT_CATALOG = "otterorders_migration"
DEFAULT_SCHEMA = "ssis_logistics_returns"
EVIDENCE_SCHEMA = "evidence"
LANDING_VOLUME = "landing"

LEGACY_OLTP = "wwi_legacy_oltp"
LEGACY_STAGING = "wwi_legacy_staging"
LEGACY_DW = "wwi_legacy_dw"
LEGACY_SQLSERVER_CONNECTION = "wwi_legacy_sqlserver"
LEGACY_OLTP_DATABASE = "WideWorldImporters"
LEGACY_DW_DATABASE = "WideWorldImportersDW"
LEGACY_STAGING_DATABASE = "WideWorldImporters_Staging"

SOURCE_SYSTEM_OLTP = "WWI_OLTP"
SOURCE_SYSTEM_CARRIER = "CARRIER_FL"

# Reserved dimension members used by the legacy Integration.usp_LoadFact* procedures.
UNKNOWN_MEMBER_KEY = 0
LB_TO_KG = 0.45359237


# Landing tables (snake_case; legacy object names kept recognisable).
class Tables:
    bronzeSqlShipment = "bronze_sql_shipment"
    bronzeSqlShipmentLine = "bronze_sql_shipment_line"
    bronzeSqlReturnLine = "bronze_sql_return_line"
    bronzeSqlCreditNote = "bronze_sql_credit_note"
    bronzeFileCarrierScan = "bronze_file_carrier_scan"
    silverShipment = "silver_shipment"
    silverShipmentLine = "silver_shipment_line"
    silverReturn = "silver_return"
    silverCreditNote = "silver_credit_note"
    goldFactShipment = "gold_fact_shipment"
    goldFactOrderFulfilment = "gold_fact_order_fulfilment"
    goldFactReturn = "gold_fact_return"
    goldFactSaleReversal = "gold_fact_sale_reversal"
    goldFactCreditNote = "gold_fact_credit_note"
    goldCreditNoteApprovalHold = "gold_credit_note_approval_hold"
    goldFactSaleRestatement = "gold_fact_sale_restatement"
    goldAggDeliveryPerformanceSummary = "gold_agg_delivery_performance_summary"
    goldAggDeliveryPerformanceLane = "gold_agg_delivery_performance_lane"
    goldAggDeliveryPerformanceThinSample = "gold_agg_delivery_performance_thin_sample"
    errRejectedConstraintViolation = "err_rejected_constraint_violation"
    errRejectedFileRow = "err_rejected_file_row"
    errRejectedShipment = "err_rejected_shipment"
    errRejectedLookupFailure = "err_rejected_lookup_failure"
    errRejectedFact = "err_rejected_fact"
    ctlWatermark = "ctl_watermark"
    ctlRowCountAudit = "ctl_row_count_audit"
    ctlFileIngestionLog = "ctl_file_ingestion_log"
    ctlFileControlTotal = "ctl_file_control_total"
    ctlReconResults = "ctl_recon_results"


@dataclass
class RunContext:
    """Everything a package needs to know about where it runs and which batch it belongs to."""

    spark: SparkSession
    catalog: str = DEFAULT_CATALOG
    schema: str = DEFAULT_SCHEMA
    batchId: int = 0
    packageExecutionId: int = 0
    gitSha: str = "unknown"
    reloadFullHistory: bool = False
    seedFromLegacyRaw: bool = True
    legacyOltp: str = LEGACY_OLTP
    legacyStaging: str = LEGACY_STAGING
    legacyDw: str = LEGACY_DW
    startedAtUtc: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

    def __post_init__(self) -> None:
        if not self.batchId:
            self.batchId = int(self.startedAtUtc.strftime("%Y%m%d")) * 100 + 1
        if not self.packageExecutionId:
            self.packageExecutionId = int(self.startedAtUtc.timestamp())

    def table(self, name: str) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{name}`"

    def volumePath(self, *parts: str) -> str:
        return "/".join(["/Volumes", self.catalog, self.schema, LANDING_VOLUME, *parts])

    def legacy(self, catalog: str, schema: str, table: str) -> str:
        return f"`{catalog}`.`{schema}`.`{table}`"


def parseBool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}
