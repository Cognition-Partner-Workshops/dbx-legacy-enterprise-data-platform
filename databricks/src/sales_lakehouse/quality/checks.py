"""Post-load data-quality checks - the lakehouse replacement for the legacy ``err.*`` /
``dq.*`` reconciliation procedures.

Every check is *reported*, never enforced: rows that failed a layer rule are already in
``sales_quality.rejected_rows``; this module only writes one summary row per check to
``sales_quality.check_results`` (``check_code, table, status, observed, expected, batch_id,
run_ts``). Checks are batch-scoped wherever the table carries a batch column.

Check families
--------------
``RECON_BRONZE``  rows_loaded + rows_rejected == rows_read per bronze table (bronze load log)
``RECON_SILVER``  silver rows + quarantined rows == bronze rows for the transaction tables (WARN: the
                  silver merge keeps prior batches and soft-deletes, so a drift is reported, not failed)
``RECON_GOLD``    fact rows + quarantined rows == silver rows for the line-grain facts (WARN, same reason)
``RI_*``          every fact FK resolves to its dimension; ``-1`` unknown members are counted
``DUP_KEY``       business / surrogate keys are unique
``AMOUNT_*``      NULL and negative amount guards on the facts
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.quality import REJECTED_ROWS_TABLE
from sales_lakehouse.common.tables import tableExists

CHECK_RESULTS_TABLE = "check_results"
LOAD_LOG_TABLE = "load_log"
CHECK_RESULTS_SCHEMA = (
    "check_code string, table string, status string, observed bigint, expected bigint, detail string, "
    "batch_id bigint, run_ts timestamp"
)

STATUS_PASS = "PASS"
STATUS_WARN = "WARN"
STATUS_FAIL = "FAIL"
STATUS_SKIPPED = "SKIPPED"

UNKNOWN_KEY = -1

# bronze source -> silver table (transaction grain; silver row + quarantined row per bronze row)
SILVER_RECON: tuple[tuple[str, str], ...] = (
    ("sqlserver_sales_orders", "order"),
    ("sqlserver_sales_order_lines", "order_line"),
    ("sqlserver_sales_invoices", "sale"),
    ("sqlserver_sales_invoice_lines", "sale_line"),
)
# silver source -> gold fact (line grain)
GOLD_RECON: tuple[tuple[str, str], ...] = (
    ("sale_line", "fact_sale"),
    ("order_line", "fact_order"),
)
FACT_TABLES: tuple[str, ...] = (
    "fact_sale",
    "fact_order",
    "fact_payment",
    "fact_sales_margin",
    "fact_return",
    "fact_credit_note",
    "fact_order_fulfilment",
)
# fact FK column -> (silver dimension, dimension key column)
DIMENSION_KEYS: dict[str, tuple[str, str]] = {
    "customer_key": ("dim_customer", "customer_key"),
    "bill_to_customer_key": ("dim_customer", "customer_key"),
    "salesperson_key": ("dim_salesperson", "salesperson_key"),
    "sales_channel_key": ("dim_sales_channel", "sales_channel_key"),
    "sales_territory_key": ("dim_sales_territory", "sales_territory_key"),
    "buying_group_key": ("dim_buying_group", "buying_group_key"),
}
SURROGATE_KEYS: dict[str, str] = {
    "fact_sale": "sale_key",
    "fact_order": "order_key",
    "fact_payment": "payment_key",
    "fact_sales_margin": "sales_margin_key",
    "fact_return": "return_key",
    "fact_credit_note": "credit_note_key",
    "fact_order_fulfilment": "order_fulfilment_key",
}
SILVER_BUSINESS_KEYS: tuple[str, ...] = ("order", "order_line", "sale", "sale_line", "payment", "payment_allocation", "customer")
# fact -> amount columns that must never be NULL
NOT_NULL_AMOUNTS: dict[str, tuple[str, ...]] = {
    "fact_sale": ("quantity", "unit_price", "net_amount", "tax_amount", "total_including_tax", "fx_rate_to_reporting"),
    "fact_order": ("quantity", "unit_price", "net_amount"),
    "fact_payment": ("payment_amount", "allocated_amount"),
}
# fact -> (amount columns, filter for rows where a negative value is unexpected)
NON_NEGATIVE_AMOUNTS: dict[str, tuple[tuple[str, ...], str | None]] = {
    "fact_sale": (("quantity", "unit_price", "net_amount", "total_including_tax"), "is_credit_note"),
    "fact_order": (("quantity", "unit_price", "net_amount"), None),
    "fact_payment": (("payment_amount",), "is_reversal"),
}


@dataclass(frozen=True)
class CheckResult:
    checkCode: str
    table: str
    status: str
    observed: int | None
    expected: int | None
    detail: str = ""


@dataclass
class CheckSummary:
    batchId: int
    results: list[CheckResult] = field(default_factory=list)

    @property
    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == STATUS_FAIL]

    @property
    def ok(self) -> bool:
        return not self.failed

    def describe(self) -> str:
        counts = {s: sum(r.status == s for r in self.results) for s in (STATUS_PASS, STATUS_WARN, STATUS_FAIL, STATUS_SKIPPED)}
        return ", ".join(f"{n} {s.lower()}" for s, n in counts.items())

    def table(self) -> str:
        rows = [("check_code", "table", "status", "observed", "expected", "detail")] + [
            (r.checkCode, r.table, r.status, _fmt(r.observed), _fmt(r.expected), r.detail) for r in self.results
        ]
        widths = [max(len(row[i]) for row in rows) for i in range(6)]
        return "\n".join("  ".join(c.ljust(widths[i]) for i, c in enumerate(row)) for row in rows)


# --------------------------------------------------------------------------- helpers
def _fmt(value: int | None) -> str:
    return "" if value is None else str(value)


def _read(spark: SparkSession, cfg: PipelineConfig, layer: str, table: str) -> DataFrame | None:
    fqn = cfg.fqn(layer, table)
    return spark.table(fqn) if tableExists(spark, fqn) else None


def _forBatch(df: DataFrame, cfg: PipelineConfig, batchCols: Sequence[str] = ("batch_id", "_batch_id")) -> DataFrame:
    for c in batchCols:
        if c in df.columns:
            return df.filter(F.col(c) == cfg.batchId)
    return df


def _rejectedCount(
    spark: SparkSession, cfg: PipelineConfig, sourceTables: Sequence[str], ruleCodes: Sequence[str] | None = None
) -> int:
    rejected = _read(spark, cfg, "quality", REJECTED_ROWS_TABLE)
    if rejected is None:
        return 0
    df = rejected.filter(F.col("batch_id") == cfg.batchId)
    patterns = [F.lower(F.col("source_table")).endswith(t.lower()) for t in sourceTables]
    cond = patterns[0]
    for p in patterns[1:]:
        cond = cond | p
    df = df.filter(cond)
    if ruleCodes:
        df = df.filter(F.col("rule_code").isin(*ruleCodes))
    return df.count()


def _result(
    code: str, table: str, observed: int | None, expected: int | None, failStatus: str = STATUS_FAIL, detail: str = ""
) -> CheckResult:
    status = STATUS_PASS if observed == expected else failStatus
    return CheckResult(code, table, status, observed, expected, detail)


def _skipped(code: str, table: str, detail: str) -> CheckResult:
    return CheckResult(code, table, STATUS_SKIPPED, None, None, detail)


# --------------------------------------------------------------------------- check families
def reconcileBronze(spark: SparkSession, cfg: PipelineConfig) -> list[CheckResult]:
    log = _read(spark, cfg, "quality", LOAD_LOG_TABLE)
    if log is None:
        return [_skipped("RECON_BRONZE", cfg.fqn("quality", LOAD_LOG_TABLE), "bronze load log not found")]
    rows = (
        log.filter(F.col("batch_id") == cfg.batchId)
        .filter(F.col("status") != "MISSING_SOURCE")
        .select("source_object", "rows_read", "rows_loaded", "rows_rejected")
        .collect()
    )
    if not rows:
        return [_skipped("RECON_BRONZE", cfg.schema("bronze"), f"no bronze loads logged for batch {cfg.batchId}")]
    return [
        _result("RECON_BRONZE", r["source_object"], (r["rows_loaded"] or 0) + (r["rows_rejected"] or 0), r["rows_read"] or 0)
        for r in rows
    ]


def reconcileSilver(spark: SparkSession, cfg: PipelineConfig) -> list[CheckResult]:
    out: list[CheckResult] = []
    for bronzeTable, silverTable in SILVER_RECON:
        bronze = _read(spark, cfg, "bronze", bronzeTable)
        silver = _read(spark, cfg, "silver", silverTable)
        if bronze is None or silver is None:
            out.append(_skipped("RECON_SILVER", cfg.fqn("silver", silverTable), "source or target table missing"))
            continue
        source = _forBatch(bronze, cfg).count()
        loaded = _forBatch(silver, cfg).count()
        rejected = _rejectedCount(spark, cfg, [bronzeTable, silverTable])
        detail = f"source={bronzeTable} loaded={loaded} rejected={rejected}"
        out.append(
            _result("RECON_SILVER", cfg.fqn("silver", silverTable), loaded + rejected, source, failStatus=STATUS_WARN, detail=detail)
        )
    return out


def reconcileGold(spark: SparkSession, cfg: PipelineConfig) -> list[CheckResult]:
    out: list[CheckResult] = []
    for silverTable, factTable in GOLD_RECON:
        silver = _read(spark, cfg, "silver", silverTable)
        fact = _read(spark, cfg, "gold", factTable)
        if silver is None or fact is None:
            out.append(_skipped("RECON_GOLD", cfg.fqn("gold", factTable), "source or target table missing"))
            continue
        source = _forBatch(silver, cfg).count()
        loaded = _forBatch(fact, cfg).count()
        rejected = _rejectedCount(spark, cfg, [factTable, silverTable])
        detail = f"source={silverTable} loaded={loaded} rejected={rejected}"
        out.append(_result("RECON_GOLD", cfg.fqn("gold", factTable), loaded + rejected, source, failStatus=STATUS_WARN, detail=detail))
    return out


def referentialIntegrity(spark: SparkSession, cfg: PipelineConfig) -> list[CheckResult]:
    out: list[CheckResult] = []
    dims: dict[str, DataFrame | None] = {}
    for factTable in FACT_TABLES:
        fact = _read(spark, cfg, "gold", factTable)
        if fact is None:
            out.append(_skipped("RI_FACT_DIM", cfg.fqn("gold", factTable), "fact table missing"))
            continue
        fact = _forBatch(fact, cfg)
        fqn = cfg.fqn("gold", factTable)
        for keyCol, (dimTable, dimKey) in DIMENSION_KEYS.items():
            if keyCol not in fact.columns:
                continue
            nulls = fact.filter(F.col(keyCol).isNull()).count()
            out.append(_result("RI_KEY_NOT_NULL", fqn, nulls, 0, detail=keyCol))
            unknown = fact.filter(F.col(keyCol) == UNKNOWN_KEY).count()
            out.append(_result("RI_UNKNOWN_MEMBER", fqn, unknown, 0, failStatus=STATUS_WARN, detail=f"{keyCol} = -1"))
            if dimTable not in dims:
                dims[dimTable] = _read(spark, cfg, "silver", dimTable)
            dim = dims[dimTable]
            if dim is None or dimKey not in dim.columns:
                out.append(_skipped("RI_FACT_DIM", fqn, f"{keyCol}: dimension {dimTable}.{dimKey} missing"))
                continue
            orphans = (
                fact.filter(F.col(keyCol).isNotNull() & (F.col(keyCol) != UNKNOWN_KEY))
                .select(F.col(keyCol).alias("_k"))
                .join(dim.select(F.col(dimKey).alias("_k")).distinct(), "_k", "left_anti")
                .count()
            )
            out.append(_result("RI_FACT_DIM", fqn, orphans, 0, detail=f"{keyCol} -> {dimTable}.{dimKey}"))
    return out


def _duplicates(df: DataFrame, keyCol: str) -> int:
    return df.filter(F.col(keyCol).isNotNull()).groupBy(keyCol).count().filter(F.col("count") > 1).count()


def duplicateKeys(spark: SparkSession, cfg: PipelineConfig) -> list[CheckResult]:
    out: list[CheckResult] = []
    for table in SILVER_BUSINESS_KEYS:
        df = _read(spark, cfg, "silver", table)
        keyCol = f"{table}_business_key"
        if df is None or keyCol not in df.columns:
            out.append(_skipped("DUP_KEY", cfg.fqn("silver", table), f"{keyCol} unavailable"))
            continue
        scope = df.filter(F.col("is_current")) if "is_current" in df.columns else df
        out.append(_result("DUP_KEY", cfg.fqn("silver", table), _duplicates(scope, keyCol), 0, detail=keyCol))
    for table, keyCol in SURROGATE_KEYS.items():
        df = _read(spark, cfg, "gold", table)
        if df is None or keyCol not in df.columns:
            out.append(_skipped("DUP_KEY", cfg.fqn("gold", table), f"{keyCol} unavailable"))
            continue
        out.append(_result("DUP_KEY", cfg.fqn("gold", table), _duplicates(df, keyCol), 0, detail=keyCol))
    return out


def amountGuards(spark: SparkSession, cfg: PipelineConfig) -> list[CheckResult]:
    out: list[CheckResult] = []
    for table, cols in NOT_NULL_AMOUNTS.items():
        df = _read(spark, cfg, "gold", table)
        if df is None:
            out.append(_skipped("AMOUNT_NOT_NULL", cfg.fqn("gold", table), "fact table missing"))
            continue
        df = _forBatch(df, cfg)
        for c in cols:
            if c not in df.columns:
                continue
            out.append(_result("AMOUNT_NOT_NULL", cfg.fqn("gold", table), df.filter(F.col(c).isNull()).count(), 0, detail=c))
    for table, (cols, exemptFlag) in NON_NEGATIVE_AMOUNTS.items():
        df = _read(spark, cfg, "gold", table)
        if df is None:
            continue
        df = _forBatch(df, cfg)
        if exemptFlag and exemptFlag in df.columns:
            df = df.filter(~F.coalesce(F.col(exemptFlag), F.lit(False)))
        for c in cols:
            if c not in df.columns:
                continue
            negatives = df.filter(F.col(c) < 0).count()
            out.append(_result("AMOUNT_NON_NEGATIVE", cfg.fqn("gold", table), negatives, 0, failStatus=STATUS_WARN, detail=c))
    return out


# --------------------------------------------------------------------------- entry point
def runChecks(spark: SparkSession, cfg: PipelineConfig) -> CheckSummary:
    summary = CheckSummary(cfg.batchId)
    for family in (reconcileBronze, reconcileSilver, reconcileGold, referentialIntegrity, duplicateKeys, amountGuards):
        summary.results.extend(family(spark, cfg))
    return summary


def writeResults(spark: SparkSession, cfg: PipelineConfig, summary: CheckSummary) -> None:
    rows = [(r.checkCode, r.table, r.status, r.observed, r.expected, r.detail, cfg.batchId) for r in summary.results]
    schema = CHECK_RESULTS_SCHEMA.replace(", run_ts timestamp", "")
    df = spark.createDataFrame(rows, schema).withColumn("run_ts", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(cfg.fqn("quality", CHECK_RESULTS_TABLE))


def run(spark: SparkSession, cfg: PipelineConfig) -> CheckSummary:
    """Run every check family for ``cfg.batchId`` and append the results to ``sales_quality.check_results``."""
    summary = runChecks(spark, cfg)
    writeResults(spark, cfg, summary)
    return summary
