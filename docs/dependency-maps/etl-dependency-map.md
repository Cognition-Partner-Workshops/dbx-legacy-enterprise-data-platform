# ETL dependency map

What executes what, and what reads what the other wrote. Every number here is
produced by `validation/checks/extract_package_dependency_graph.py`, which
parses the `.dtsx` files in `ssis/` and the object declarations in
`config/estate-catalog.yaml`. Regenerate it with:

```bash
python3 validation/checks/extract_package_dependency_graph.py --json
python3 validation/checks/extract_package_dependency_graph.py --mermaid > /tmp/graph.mmd
python3 validation/checks/extract_package_dependency_graph.py --dot > /tmp/graph.dot
```

This is static analysis of files. No package has been executed and no
dependency below has been observed at runtime.

## The numbers

| | |
| --- | --- |
| Packages | 204 |
| Master packages | 9 |
| Execute-package edges | 327 |
| Packages reachable from a master | 194 |
| Packages no master executes | 1 |
| Longest execute chain | 2 (`Master_Customer_Sync` -> `EXT_ORA_CustomerMaster`) |
| Execute cycles | 0 |
| Data-flow edges (writer -> reader of the same object) | 1587 |
| Data-flow cycles | 3 |

327 execute edges across 204 packages with a longest chain of 2 says everything
about the orchestration style: it is flat. The masters call children directly
and no child calls another child. Ordering *within* a master is expressed by
precedence constraints between Execute Package Tasks, not by nesting, so the
real sequencing is invisible to a reader looking only at the call graph.

## The masters

| Master | Executes | Agent job | Cadence |
| --- | --- | --- | --- |
| `Master_Daily_ETL` | 192 | WWI - Daily ETL | daily 01:30 |
| `Master_Hourly_Incremental` | 26 | WWI - Hourly Incremental | hourly 05:00-23:00 |
| `Master_Customer_Sync` | 24 | WWI - Customer Master Sync | daily 00:15 |
| `Master_Month_End` | 19 | WWI - Month End | daily 04:00, calendar-gated, disabled |
| `Master_Intraday_Inventory` | 16 | WWI - Intraday Inventory Refresh | every 20 min |
| `Master_Weekly_Reference_Load` | 16 | WWI - Weekly Reference Refresh | Sunday 03:00 |
| `Master_File_Ingestion` | 13 | WWI - File Ingestion | every 15 min |
| `Master_Finance_Close` | 11 | WWI - Finance Close | monthly day 1, disabled |
| `Master_Weekly_Maintenance` | 10 | WWI - Weekly Maintenance | Saturday 22:00 |

Those add up to more than 204 because packages are executed by more than one
master - the reference loads run both nightly and weekly, the customer extracts
run in both `Master_Daily_ETL` and `Master_Customer_Sync` - which is one of the
estate's more expensive habits.

## Layer order

The daily run walks the layers in this order, and every master is a subset of
it:

```
1  extract     01_oracle_extract, 02_sqlserver_extract, 03_file_ingestion   -> raw.*
2  stage       04_staging                                                   -> stg.*, work.*
3  screen      05_data_quality                                              -> err.*, quarantine
4  reference   06_reference_data                                            -> ref.*, Dimension.*
5  dimension   07_dimensions                                                -> Dimension.*
6  fact        08_facts                                                     -> Fact.*
7  aggregate   09_aggregates                                                -> Aggregate.*
8  domain      10_finance .. 14_customer_360                                -> Fact.*, Aggregate.*
9  publish     Report views, Aggregate publishes
10 maintain    99_maintenance, 15_error_handling
```

The hard ordering constraints are the obvious ones - a dimension cannot load
before its staging table, a fact cannot load before its dimensions, an
aggregate cannot rebuild before its fact - and they are enforced by precedence
constraints inside the masters plus the declarations in `etl.PackageDependency`.
Nothing reconciles those two sources of truth, which is why the dependency
extractor exists.

## Data-flow dependencies

The data graph is derived from what each package declares it reads and writes.
1587 edges over 204 packages: on average every package is downstream of eight
others. The dense clusters are exactly where you would expect them -
`stg.Customer` has many writers and many readers, and every dimension load
feeds every fact load that looks it up.

Three data-flow cycles exist. None of them is an execute cycle, and all three
are broken in practice by running the two halves in different phases:

| Cycle | Packages | Why it closes |
| --- | --- | --- |
| Customer 360 | `AGG_Refresh_Customer360`, `C360_Publish_Segments` | the published segments are read back on the next rebuild |
| Data quality | 16 packages across `05_data_quality` and `15_error_handling` | screens write rejects, the reject handlers write back into the screened tables |
| Referential screen | `DQ_Referential_Screen`, `DQ_Reject_Reprocess` | reprocessed rejects re-enter the screen |

A cycle broken by scheduling rather than by structure is a cycle that becomes a
loop the moment someone runs the two halves in the same window. This is a
finding, not a design.

## Orphans and disagreements

The extractor reports these as warnings, and they are real properties of the
merged estate rather than defects in the check:

- `ERR_Retry_FailedSteps` is executed by no master. It can only run by hand or
  from an Agent job step.
- `DQ_File_Screen`, `ERR_Quarantine_BadFiles` and `ERR_Retry_FailedSteps` are
  declared in the catalog with `parent: Master_Daily_ETL`, but
  `Master_Daily_ETL` contains no Execute Package Task that references them. The
  declared parentage and the actual XML disagree.

194 of the 204 packages are reachable from a master. The ten that are not are
the nine masters themselves - nothing executes a master except SQL Agent - and
`ERR_Retry_FailedSteps`.

## Object-level dependencies

For the source-to-target view - which package writes which table, and which
tables nobody writes or nobody reads - use:

```bash
python3 validation/checks/check_source_to_target_coverage.py
python3 validation/checks/check_orphan_objects.py
```

The current findings from both are catalogued in
`docs/known-unvalidated-items.md`.

## Reading the generated graph

`--mermaid` emits the execute graph only; the data graph at 1587 edges is not
readable as a diagram and is better queried from the JSON. `--dot` output
renders with `dot -Tsvg`, but the daily master's 192 children make a hairball,
so filter to one master before rendering.
