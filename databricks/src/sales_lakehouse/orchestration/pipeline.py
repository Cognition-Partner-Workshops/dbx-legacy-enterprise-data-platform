"""End-to-end orchestrator for the sales lakehouse.

Replaces the SSIS master packages (``Master_Daily_ETL`` / ``Master_Hourly_Incremental`` /
``Master_Month_End``): every layer entry point is run in dependency order inside one
Spark session so the local Delta/metastore state is shared by all stages.

    python -m sales_lakehouse.orchestration.pipeline --mock-root /tmp/mock_out --stages all
    python -m sales_lakehouse.orchestration.pipeline --stages gold_facts,gold_aggregates

Stage names (canonical order) are in ``STAGE_NAMES``; ``runAll`` always executes the
requested subset in that order regardless of how it was spelled on the command line.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import SparkSession

from sales_lakehouse.common.config import REGION_CODES, PipelineConfig

log = logging.getLogger(__name__)

STAGE_ALL = "all"
MOCK_ROOT_ENV = "SALES_LAKEHOUSE_MOCK_ROOT"
WAREHOUSE_DIR_ENV = "SALES_LAKEHOUSE_WAREHOUSE_DIR"
PARTNER_FEED_SUBDIR = ("outbound", "partner_feed")


@dataclass(frozen=True)
class Stage:
    """One orchestrated step: ``run`` receives the session, the config and the run options."""

    name: str
    description: str
    run: Callable[[SparkSession, PipelineConfig, RunOptions], object]
    dependsOn: tuple[str, ...] = ()
    optional: bool = False  # not part of ``all``


@dataclass(frozen=True)
class RunOptions:
    mockScale: str = "small"
    mockSeed: int | None = None
    partnerFeedDir: str | None = None
    closeMonth: dt.date | None = None  # month_end stage: calendar month to freeze (default: previous month)
    failFast: bool = True


@dataclass
class StageResult:
    stage: str
    status: str  # OK | FAILED | SKIPPED
    seconds: float
    message: str = ""


@dataclass
class PipelineRun:
    results: list[StageResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(r.status != "FAILED" for r in self.results)

    def summary(self) -> str:
        counts = {s: sum(r.status == s for r in self.results) for s in ("OK", "FAILED", "SKIPPED")}
        return ", ".join(f"{n} {s.lower()}" for s, n in counts.items() if n)

    def table(self) -> str:
        width = max([len("stage")] + [len(r.stage) for r in self.results])
        lines = [f"{'stage'.ljust(width)}  status   seconds  message"]
        for r in self.results:
            lines.append(f"{r.stage.ljust(width)}  {r.status.ljust(7)}  {r.seconds:7.1f}  {r.message}")
        return "\n".join(lines)


# --------------------------------------------------------------------------- stage bodies
def _mock(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> str:
    from sales_lakehouse.mock_data.generate import DEFAULT_SEED, generate

    manifest = generate(Path(cfg.mockDataRoot), seed=opts.mockSeed or DEFAULT_SEED, scale=opts.mockScale)
    return f"{len(manifest['tables'])} tables, {len(manifest['edgeCases'])} edge cases -> {cfg.mockDataRoot}"


def _bronze(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> str:
    from sales_lakehouse.bronze import ingest

    results = ingest.run(spark, cfg)
    loaded = sum(r.rowsLoaded for r in results)
    rejected = sum(r.rowsRejected for r in results)
    missing = sum(r.status == "MISSING_SOURCE" for r in results)
    return f"{len(results)} tables, {loaded} rows loaded, {rejected} rejected, {missing} missing sources"


def _silverReference(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> None:
    from sales_lakehouse.silver import reference

    reference.run(spark, cfg)


def _silverParty(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> None:
    from sales_lakehouse.silver import party_resolution

    party_resolution.run(spark, cfg)


def _silverCustomers(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> None:
    from sales_lakehouse.silver import customers

    customers.run(spark, cfg)


def _silverDimensions(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> None:
    from sales_lakehouse.silver import dimensions

    dimensions.run(spark, cfg)


def _silverTransactions(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> None:
    from sales_lakehouse.silver import transactions

    transactions.run(spark, cfg)


def _goldFacts(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> None:
    from sales_lakehouse.gold import facts

    facts.run(spark, cfg)


def _goldAggregates(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> None:
    from sales_lakehouse.gold import aggregates

    aggregates.run(spark, cfg)


def _goldReporting(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> None:
    from sales_lakehouse.gold import reporting

    reporting.run(spark, cfg)


def partnerFeedDir(cfg: PipelineConfig, opts: RunOptions) -> str:
    return opts.partnerFeedDir or os.path.join(cfg.mockDataRoot, *PARTNER_FEED_SUBDIR)


def _salesOps(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> str:
    from sales_lakehouse.gold import sales_ops

    target = partnerFeedDir(cfg, opts)
    sales_ops.run(spark, cfg, outDir=target)
    return f"quota / commission refreshed, partner feed -> {target}"


def previousCalendarMonth(today: dt.date | None = None) -> dt.date:
    first = (today or dt.date.today()).replace(day=1)
    return (first - dt.timedelta(days=1)).replace(day=1)


def _monthEnd(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> str:
    from sales_lakehouse.gold import aggregates

    month = opts.closeMonth or previousCalendarMonth()
    for region in REGION_CODES:
        aggregates.closeMonthlyPeriod(spark, cfg, month, region)
    return f"agg_monthly_sales period_closed_flag set for {month.isoformat()} in {', '.join(REGION_CODES)}"


def _quality(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> str:
    from sales_lakehouse.quality import checks

    summary = checks.run(spark, cfg)
    return summary.describe()


def _validation(spark: SparkSession, cfg: PipelineConfig, opts: RunOptions) -> str:
    from sales_lakehouse.quality import validate_mock

    report = validate_mock.validate(spark, cfg, partnerFeedDir(cfg, opts))
    print(report.table())
    if not report.ok:
        raise RuntimeError("mock validation failed:\n" + report.table())
    return report.describe()


STAGES: tuple[Stage, ...] = (
    Stage("mock", "generate the mock source CSVs under cfg.mockDataRoot (optional)", _mock, optional=True),
    Stage("bronze", "bronze.ingest: land every registered CSV", _bronze),
    Stage(
        "silver_reference",
        "silver.reference: ref_fx_rate / ref_tax_rate_* / dim_fiscal_calendar / dim_date",
        _silverReference,
        ("bronze",),
    ),
    Stage("silver_party", "silver.party_resolution", _silverParty, ("bronze",)),
    Stage("silver_customers", "silver.customers: customer + dim_customer (SCD2)", _silverCustomers, ("silver_party",)),
    Stage(
        "silver_dimensions",
        "silver.dimensions: channel / territory / salesperson / buying group",
        _silverDimensions,
        ("silver_customers",),
    ),
    Stage(
        "silver_transactions",
        "silver.transactions: order / sale / payment / allocation / hold / amendment / backorder / quote",
        _silverTransactions,
        ("silver_reference", "silver_dimensions"),
    ),
    Stage(
        "gold_facts",
        "gold.facts: sale, order, payment, return, credit note, margin, snapshots, fulfilment",
        _goldFacts,
        ("silver_transactions",),
    ),
    Stage("gold_aggregates", "gold.aggregates: agg_* tables", _goldAggregates, ("gold_facts",)),
    Stage("gold_reporting", "gold.reporting: Report.vw_* replacement views", _goldReporting, ("gold_aggregates",)),
    Stage("sales_ops", "gold.sales_ops: quota attainment, commission, partner feed export", _salesOps, ("gold_aggregates",)),
    Stage("month_end", "aggregates.closeMonthlyPeriod per region (Master_Month_End)", _monthEnd, ("gold_aggregates",), optional=True),
    Stage(
        "quality",
        "quality.checks: reconciliation / RI / duplicate / amount guards -> sales_quality.check_results",
        _quality,
        ("gold_aggregates",),
    ),
    Stage("validation", "quality.validate_mock: gold vs mock manifest expectations", _validation, ("sales_ops", "quality")),
)
STAGE_NAMES: tuple[str, ...] = tuple(s.name for s in STAGES)
_BY_NAME: dict[str, Stage] = {s.name: s for s in STAGES}


def defaultStages() -> tuple[str, ...]:
    return tuple(s.name for s in STAGES if not s.optional)


def parseStages(spec: str | Iterable[str] | None) -> tuple[str, ...]:
    """``"all"`` / comma list / iterable -> canonical-order tuple; ``+name`` adds an optional stage to ``all``."""
    if spec is None:
        return defaultStages()
    tokens = [t.strip() for t in (spec.split(",") if isinstance(spec, str) else spec)]
    tokens = [t for t in tokens if t]
    if not tokens:
        return defaultStages()
    selected: set[str] = set()
    for token in tokens:
        if token == STAGE_ALL:
            selected.update(defaultStages())
            continue
        name = token[1:] if token.startswith("+") else token
        if name not in _BY_NAME:
            raise ValueError(f"unknown stage {token!r}; valid stages: {', '.join(STAGE_NAMES)}")
        if token.startswith("+"):
            selected.update(defaultStages())
        selected.add(name)
    return tuple(n for n in STAGE_NAMES if n in selected)


def orderStages(names: Sequence[str]) -> tuple[str, ...]:
    """Canonical (dependency) order of ``names``; raises on an unknown stage."""
    return parseStages(list(names))


def runAll(
    spark: SparkSession,
    cfg: PipelineConfig,
    stages: str | Iterable[str] | None = None,
    options: RunOptions | None = None,
) -> PipelineRun:
    """Run the selected stages in dependency order inside the given session.

    A failed stage marks every later stage that (transitively) depends on it as
    SKIPPED; with ``options.failFast`` (default) nothing else runs either.
    """
    from sales_lakehouse.common.spark import ensureSchemas

    opts = options or RunOptions()
    selected = parseStages(stages)
    ensureSchemas(spark, cfg)
    run = PipelineRun()
    failed: set[str] = set()
    for name in selected:
        stage = _BY_NAME[name]
        blocked = [d for d in stage.dependsOn if d in failed]
        if blocked or (opts.failFast and failed):
            run.results.append(StageResult(name, "SKIPPED", 0.0, f"blocked by {', '.join(blocked) or 'earlier failure'}"))
            failed.add(name)
            continue
        log.info("stage %s: %s", name, stage.description)
        started = time.monotonic()
        try:
            message = stage.run(spark, cfg, opts)
        except Exception as exc:  # noqa: BLE001 - every stage failure is reported in the run table
            log.exception("stage %s failed", name)
            run.results.append(StageResult(name, "FAILED", time.monotonic() - started, f"{type(exc).__name__}: {exc}"))
            failed.add(name)
            continue
        run.results.append(StageResult(name, "OK", time.monotonic() - started, str(message or "")))
    return run


# --------------------------------------------------------------------------- CLI
def parseArgs(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m sales_lakehouse.orchestration.pipeline", description=__doc__)
    parser.add_argument("--mock-root", default=os.environ.get(MOCK_ROOT_ENV), help=f"mock CSV root (default ${MOCK_ROOT_ENV})")
    parser.add_argument("--catalog", default=os.environ.get("SALES_LAKEHOUSE_CATALOG") or None)
    parser.add_argument("--batch-id", type=int, default=None)
    parser.add_argument(
        "--stages",
        default=STAGE_ALL,
        help=f"'all', a comma list of {', '.join(STAGE_NAMES)}, or '+mock' / '+month_end' to add an optional stage to 'all'",
    )
    parser.add_argument("--close-month", type=dt.date.fromisoformat, default=None, help="month_end stage: YYYY-MM-01 to freeze")
    parser.add_argument("--scale", default="small", help="mock scale when the mock stage runs")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--partner-feed-dir", default=None)
    parser.add_argument(
        "--warehouse-dir", default=os.environ.get(WAREHOUSE_DIR_ENV), help="persistent local warehouse + metastore directory"
    )
    parser.add_argument("--keep-going", action="store_true", help="run independent stages after a failure")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parseArgs(argv)
    if not args.mock_root:
        print(f"--mock-root (or ${MOCK_ROOT_ENV}) is required", file=sys.stderr)
        return 2
    from sales_lakehouse.common.spark import getSpark

    cfg = PipelineConfig(catalog=args.catalog, mockDataRoot=args.mock_root, **({"batchId": args.batch_id} if args.batch_id else {}))
    if args.warehouse_dir:
        os.environ[WAREHOUSE_DIR_ENV] = args.warehouse_dir
    spark = getSpark("sales_lakehouse_pipeline")
    opts = RunOptions(
        mockScale=args.scale,
        mockSeed=args.seed,
        partnerFeedDir=args.partner_feed_dir,
        closeMonth=args.close_month,
        failFast=not args.keep_going,
    )
    run = runAll(spark, cfg, stages=args.stages, options=opts)
    print(f"\nbatch_id={cfg.batchId} catalog={cfg.catalog or 'spark_catalog'} mock_root={cfg.mockDataRoot}")
    print(run.table())
    return 0 if run.ok else 1


if __name__ == "__main__":
    sys.exit(main())
