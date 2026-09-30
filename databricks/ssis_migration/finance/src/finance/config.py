"""Runtime configuration shared by every finance package."""

from dataclasses import dataclass, field
from datetime import date


@dataclass(frozen=True)
class FinanceConfig:
    catalog: str = "otterorders_migration"
    schema: str = "ssis_finance"
    evidenceSchema: str = "evidence"
    oracleCatalog: str = "wwi_legacy_oracle"
    stagingCatalog: str = "wwi_legacy_staging"
    dwCatalog: str = "wwi_legacy_dw"
    accountingPeriod: str = "2024-12"
    businessDate: date = date(2024, 12, 31)
    gitSha: str = "unknown"
    # SSIS package parameters that gate business rules
    allowUnbalancedJournals: bool = False
    failOnMissingRate: bool = False
    includeDisputed: bool = False
    ledgerScope: str = "ALL"
    extra: dict = field(default_factory=dict)

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def evidenceTable(self, name: str) -> str:
        return f"{self.catalog}.{self.evidenceSchema}.{name}"

    def oracle(self, schema: str, name: str) -> str:
        return f"{self.oracleCatalog}.{schema}.{name}"

    def staging(self, schema: str, name: str) -> str:
        return f"{self.stagingCatalog}.{schema}.{name}"

    def dw(self, schema: str, name: str) -> str:
        return f"{self.dwCatalog}.{schema}.{name}"

    @property
    def revaluationDate(self) -> date:
        return self.businessDate


def configFromParams(params: dict) -> FinanceConfig:
    """Build a config from notebook widgets / job parameters (all strings)."""

    def flag(key: str, default: bool) -> bool:
        raw = params.get(key)
        if raw in (None, ""):
            return default
        return str(raw).strip().lower() in ("1", "true", "y", "yes")

    bd = params.get("business_date") or ""
    businessDate = date.fromisoformat(bd) if bd else date.today()
    return FinanceConfig(
        catalog=params.get("catalog") or "otterorders_migration",
        schema=params.get("schema") or "ssis_finance",
        evidenceSchema=params.get("evidence_schema") or "evidence",
        accountingPeriod=params.get("accounting_period") or "2024-12",
        businessDate=businessDate,
        gitSha=params.get("git_sha") or "unknown",
        allowUnbalancedJournals=flag("allow_unbalanced_journals", False),
        failOnMissingRate=flag("fail_on_missing_rate", False),
        includeDisputed=flag("include_disputed", False),
        ledgerScope=params.get("ledger_scope") or "ALL",
    )
