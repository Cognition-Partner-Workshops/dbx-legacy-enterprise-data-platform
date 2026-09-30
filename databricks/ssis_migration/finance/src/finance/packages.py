"""Package registry (name exactly as in docs/inventories/ssis-packages.csv) and the job-task runner."""

from collections.abc import Callable
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from finance import aggregates, close, extracts, facts, staging
from finance.config import FinanceConfig, configFromParams
from finance.io import PackageResult, ensureControlTables, logBatch, nextBatchId

Runner = Callable[[SparkSession, FinanceConfig, int], PackageResult]

PACKAGES: dict[str, Runner] = {
    # 01_oracle_extract (Master_Daily_ETL)
    "EXT_ORA_ApInvoiceHdr": extracts.runInvoiceHeaders,
    "EXT_ORA_ApInvoiceLine": extracts.runInvoiceLines,
    "EXT_ORA_ApPayment": extracts.runPayments,
    "EXT_ORA_ApPaymentApply": extracts.runPaymentApplies,
    "EXT_ORA_ApAging": extracts.runApAging,
    "EXT_ORA_GlJournalLine": extracts.runGlJournalLines,
    "EXT_ORA_CostCenter": extracts.runCostCenters,
    # 04_staging / 05_data_quality (Master_Daily_ETL)
    "STG_Load_ApInvoice": staging.runStgApInvoice,
    "STG_Load_GlJournal": staging.runStgGlJournal,
    "STG_Load_Payment": staging.runStgPayment,
    "STG_Load_CostCenter": staging.runStgCostCenter,
    "STG_Work_PaymentMatch": staging.runPaymentMatch,
    "DQ_Payment_Screen": staging.runDqPaymentScreen,
    # 06_reference_data (Master_Weekly_Reference_Load)
    "REF_Load_CostCenter": facts.runRefCostCenter,
    # 08_facts (Master_Daily_ETL)
    "FACT_Load_GLPosting": facts.runFactGlPosting,
    "FACT_Load_Payment": facts.runFactPayment,
    "FACT_Load_SupplierPayment": facts.runFactSupplierPayment,
    # 10_finance (Master_Finance_Close)
    "FIN_Currency_Revaluation": close.runCurrencyRevaluation,
    "FIN_Load_GlPostings": close.runFinGlPostings,
    "FIN_Load_ApAging": close.runFinApAging,
    "FIN_Load_CostAllocation": close.runFinCostAllocation,
    "FIN_Load_WithholdingTax": close.runFinWithholdingTax,
    "FIN_Reconcile_SubledgerToGl": close.runFinReconcile,
    "FIN_Close_PeriodLock": close.runFinPeriodLock,
    # 09_aggregates (Master_Month_End)
    "AGG_Refresh_FinanceCloseSummary": aggregates.runAggFinanceCloseSummary,
}


def parsePackageList(raw: str) -> list[str]:
    names = [p.strip() for p in raw.replace("\n", ",").split(",") if p.strip()]
    unknown = [n for n in names if n not in PACKAGES]
    if unknown:
        raise ValueError(f"unknown finance package(s): {unknown}")
    return names


def runPackages(spark: SparkSession, params: dict, packageNames: list[str]) -> list[PackageResult]:
    cfg = configFromParams(params)
    ensureControlTables(spark, cfg)
    results: list[PackageResult] = []
    for name in packageNames:
        batchId = nextBatchId(spark, cfg)
        startedAt = datetime.now(timezone.utc)
        try:
            result = PACKAGES[name](spark, cfg, batchId)
        except Exception as exc:
            logBatch(
                spark,
                cfg,
                batchId,
                startedAt,
                PackageResult(name, status="FAILED", notes={"error": str(exc)[:4000]}),
            )
            raise
        logBatch(spark, cfg, batchId, startedAt, result)
        print(
            f"[{name}] batch={batchId} status={result.status} read={result.rowsRead} written={result.rowsWritten} rejected={result.rowsRejected} notes={result.notes}"
        )
        results.append(result)
    return results
