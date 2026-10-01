# Databricks sales lakehouse

Databricks/Delta Lake replacement for the WideWorldImporters sales pipeline
(SQL Server OLTP + Oracle ERP/MDM -> staging -> DW -> aggregates -> reporting, orchestrated by the
`Master_Daily_ETL` / `Master_Hourly_Incremental` / `Master_Month_End` SSIS packages).

Runs end-to-end on deterministic mock data with no production connectivity. See `CONVENTIONS.md`
for the layer contract and `../docs/migration/sales-etl-mapping.md` for the legacy -> lakehouse
artifact map, the deliberate behavioural reproductions, the open questions and the integration
changelog.

```
src/sales_lakehouse/
  common/         PipelineConfig (cfg.fqn), getSpark, Delta helpers, quarantine
  mock_data/      deterministic source generator (70 CSV tables + manifest.json)
  bronze/         DDL-derived registry, CSV -> sales_bronze Delta loader, watermarks, load_log
  silver/         reference (FX/tax/fiscal), rules/, party_resolution, customers, dimensions, transactions
  gold/           fact_*, aggregates, reporting views, sales_ops (quota, commission), partner_feed
  quality/        checks.py (post-load DQ -> sales_quality.check_results), validate_mock.py
  orchestration/  pipeline.py: runAll(spark, cfg, stages) + CLI
notebooks/        thin serverless notebook wrappers (00_mock_data .. 95_validation)
resources/        bundle jobs (per-layer jobs + sales_lakehouse_pipeline_job.yml end-to-end)
tests/            pytest (PySpark + delta-spark, local[2])
```

## Local run-book

Requirements: Python 3.10+, Java 17, network access to a Maven mirror for the Delta jars
(`SALES_LAKEHOUSE_MAVEN_MIRROR` overrides the default
`https://maven-central.storage-download.googleapis.com/maven2/`).

```bash
cd databricks
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest ruff
export PYTHONPATH=src

# 1. mock sources (70 tables / ~87k rows / 31 tagged edge cases at --scale small)
python -m sales_lakehouse.mock_data.generate --scale small --out /tmp/mock_out

# 2. the whole pipeline in ONE process (mock -> bronze -> silver -> gold -> aggregates -> views
#    -> quota/commission -> partner feed -> quality checks -> validation against the manifest)
PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell" \
python -m sales_lakehouse.orchestration.pipeline \
  --mock-root /tmp/mock_out \
  --stages all \
  --warehouse-dir /tmp/lh_wh \
  --partner-feed-dir /tmp/partner_feed
```

`--stages` accepts `all`, a comma list of stage names, or `+mock` / `+month_end` to add the optional
stages. Stage names (dependency order): `mock`, `bronze`, `silver_reference`, `silver_party`,
`silver_customers`, `silver_dimensions`, `silver_transactions`, `gold_facts`, `gold_aggregates`,
`gold_reporting`, `sales_ops`, `month_end`, `quality`, `validation`. A failed stage blocks its
dependants; `--keep-going` still runs independent stages. The CLI prints a per-stage table and the
validation pass/fail table and exits non-zero on any failure.

Other CLI options: `--catalog` (blank = two-level `sales_<layer>.<table>` names, the local default),
`--batch-id` (default epoch seconds; reuse it to re-run later stages against the same batch),
`--scale` / `--seed` (mock stage), `--close-month YYYY-MM-01` (month_end stage).

Local Spark state is per process unless `--warehouse-dir` (or `SALES_LAKEHOUSE_WAREHOUSE_DIR`) is
set: `getSpark` then puts the warehouse **and** a Derby metastore under that directory so a later
process (a second `--stages` invocation, a probe, the validation module) sees the same tables.
Delete the directory to start clean.

Environment variables read by `PipelineConfig.fromEnv` / `getSpark`:

| Variable | Meaning |
|---|---|
| `SALES_LAKEHOUSE_MOCK_ROOT` | mock CSV root (default `mock_data/output`) |
| `SALES_LAKEHOUSE_CATALOG` | Unity Catalog catalog; blank locally |
| `SALES_LAKEHOUSE_BATCH_ID` | numeric batch id shared by all layers |
| `SALES_LAKEHOUSE_WAREHOUSE_DIR` | persistent local warehouse + Derby metastore |
| `SALES_LAKEHOUSE_MAVEN_MIRROR` | Maven repo for the Delta jars |

### Checks

```bash
ruff check .                                   # from databricks/ (ruff.toml: line 140, E,F,I,B,UP)
python -m pytest tests -q -p no:cacheprovider  # full suite, ~30-40 min on a 4-core box
PYSPARK_SUBMIT_ARGS="--driver-memory 6g pyspark-shell" \
python -m pytest tests/test_silver_transactions_run.py -q   # the heavy WS4 end-to-end file needs a bigger driver heap
databricks bundle validate --strict -t dev
```

Re-running a single layer locally: `--stages silver_transactions --batch-id <same id> --warehouse-dir /tmp/lh_wh`.
Quarantined rows are in `sales_quality.rejected_rows` (`rule_code`, `source_table`, `row_json`,
`batch_id`); post-load check results in `sales_quality.check_results`; load audit in
`sales_quality.load_log`; bronze watermarks in `sales_bronze._watermark`.

## Databricks run-book

The bundle (`databricks.yml`) targets serverless notebook tasks only - no cluster specs. Every notebook
bootstraps `sys.path` relative to its own location (`../../src`) and takes the widgets `catalog`,
`mock_data_root`, `batch_id` (plus stage-specific ones).

```bash
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
# one-off mock data into the volume (creates schema + volume)
databricks bundle run generate_mock_data -t dev
# the whole pipeline, one batch id shared by every task (default {{job.run_id}})
databricks bundle run sales_lakehouse_pipeline -t dev
# month-end close of the previous month (or --params close_month=2025-06-01)
databricks bundle run sales_lakehouse_month_end -t dev
```

Jobs:

| Job (`resources/`) | Purpose |
|---|---|
| `sales_lakehouse_pipeline` (`sales_lakehouse_pipeline_job.yml`) | end-to-end replacement for `Master_Daily_ETL` / `Master_Hourly_Incremental`: `ingest_bronze` -> `silver_reference`, `silver_party_resolution` -> `silver_customers` -> `silver_dimensions` -> `silver_transactions` -> `fact_sale`, `fact_order` -> `fact_payment`, `fact_return`, `fact_credit_note` -> `fact_sales_margin`, `fact_daily_snapshots`, `fact_order_fulfilment` -> `gold_aggregates` -> `gold_reporting_views`, `gold_sales_ops` -> `quality_checks` -> `validate_against_mock`. Parameters `batch_id` (default `{{job.run_id}}`), `partner_feed_dir` (blank = `<mock_data_root>/outbound/partner_feed`). Daily 02:00 UTC schedule, paused. |
| `sales_lakehouse_month_end` (same file) | `Master_Month_End`: `run_pipeline.py` with `stages=gold_aggregates,gold_reporting,month_end`; parameter `close_month`. Monthly schedule, paused. |
| `generate_mock_data`, `bronze_ingest`, `silver_reference`, `silver_dimensions`, `silver_transactions`, `gold_facts`, `gold_reporting` | per-layer jobs added by the workstreams, kept for re-running one layer; not scheduled. |

`notebooks/90_orchestration/run_pipeline.py` is the same orchestrator as the CLI (widgets `catalog`,
`mock_data_root`, `batch_id`, `stages`, `partner_feed_dir`, `close_month`, `keep_going`) and can run any
subset of stages in one task; `notebooks/95_validation/validate_against_mock.py` re-runs the manifest
validation read-only and fails the task on any failed check.

Serverless notes: `DataFrame.cache()` / `persist()` are not supported - use
`common.spark.cacheIfSupported` / `unpersistQuietly`; `spark.sparkContext`, `spark._jvm` and RDD APIs do
not exist on Spark Connect. All batch ids are numeric (`{{job.run_id}}`), `int()`-parsed by the notebooks.
