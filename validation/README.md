# Validation

Two layers of checks live here, plus a set of queries that are written for a
live estate and have never been pointed at one.

| Path | What it is | Needs a server? |
| --- | --- | --- |
| `validation/static/run_all_checks.py` | well-formedness and convention checks over the checked-in files | no |
| `validation/checks/` | deeper structural analysis: coverage, orphans, the package graph, control-framework wiring | no |
| `validation/static/run_negative_fixtures.py` | proves the static checks fail on deliberately malformed copies of real artifacts | no |
| `validation/runtime/` | SQL an operator would run *after* a deployment, to see whether the estate actually behaves | yes - and it has not been run |

Everything under `validation/static` and `validation/checks` is offline. No
check opens a connection, loads an Oracle client, invokes `dtexec`, reads a
credential, or touches a file share. `python3` and `pyyaml` are the only
requirements.

## Running them

```bash
python3 validation/static/run_all_checks.py                       # whole tree
python3 validation/static/run_all_checks.py --path validation --path docs
python3 validation/checks/run_deep_checks.py                      # all four deep checks
python3 validation/checks/check_source_to_target_coverage.py --json
python3 validation/checks/extract_package_dependency_graph.py --mermaid
python3 validation/static/run_negative_fixtures.py                # negative fixtures
```

`run_negative_fixtures.py` copies a real project, package, connection manager
or procedure into a scratch tree, breaks exactly one thing in the copy, and
asserts the matching static check fires - and that it stays quiet on the
unbroken copy. The repository is never written to. It is what stops the static
suite from passing everything by passing nothing.

Each deep check exits non-zero when it finds an error, `0` when it finds only
warnings, and non-zero for either under `--strict`. `--json` emits findings,
counters and detail lists for use in a report.

## The deep checks

### `check_source_to_target_coverage.py`

Joins the 204 package declarations in `config/estate-catalog.yaml` to the
object inventory and asks, for every object, who writes it and who reads it.
Errors on references that resolve to nothing; warns on the coverage gaps that
are part of the legacy picture - Oracle tables nobody extracts, base
WideWorldImporters tables outside the estate inventory, staging tables written
and never read.

### `check_orphan_objects.py`

Walks the tables and views actually created by SQL under `sqlserver/staging`,
`sqlserver/warehouse` and `sqlserver/views`, and classifies each as
`no-writer`, `no-reader` or `isolated` based on every reference in every SQL
file and package in the tree.

### `extract_package_dependency_graph.py`

Reads the `<PackageName>` elements out of the package XML to get the real
execute graph - not the catalog's `parent` field - and derives a second,
data-level graph from the object declarations. Reports strongly connected
components, execute-edges that point at a missing file, packages no master
reaches, and the longest execute chain. `--dot`, `--mermaid` and `--graph-json`
emit the graph itself.

### `check_control_framework_integration.py`

Asserts that every load is visible in the `etl` control schema: package start
and end logged, errors logged, batches opened and closed by the masters,
watermarks read and written by incremental loads, and every `etl.usp_*` the
estate calls actually created under `sqlserver/control/procedures`.

### `check_control_object_columns.py`

Every column an `INSERT` or an `EXEC` names on a control table or routine has
to exist on it.

## What these checks can establish

- The XML parses, the names follow the conventions, nothing is defined twice.
- Every object a package names either exists in the catalog, is created by a
  SQL file in this repository, or belongs to the Microsoft sample.
- The execute graph terminates: no package can execute itself, directly or
  through a chain of children.
- Every `Execute Package Task` names a `.dtsx` that is present in `ssis/`.
- Every load package contains the text of the control-framework calls the
  runbooks and the operational views depend on.
- No credential literal and no forbidden target-platform artifact has been
  committed.

## What these checks cannot establish

They are text and graph analysis over files. In particular they do **not**
establish that:

- any PL/SQL package, T-SQL procedure, view or DDL script compiles;
- any `.dtsx` opens in SSDT, validates its metadata, or runs under `dtexec`;
- a column mapping inside a data flow is correct, or that the columns it names
  exist - only object-level declarations are read, never column lineage;
- a call that appears in a package is on a reachable path, has correct
  parameters, or would ever fire;
- a table populated by dynamic SQL is populated at all: a name composed at
  runtime is invisible here, so some "orphans" may not be orphans;
- an object read by something outside this repository - a report, a hand-run
  script, a downstream extract - is unused, however isolated it looks here;
- the tax, FX, rounding and fiscal-calendar rules produce correct numbers;
- row counts reconcile, watermarks advance, or a batch completes.

Anything in the second list belongs to `validation/runtime`, and to
`docs/known-unvalidated-items.md`, which is the estate's standing ledger of
what has not been proven.

## Findings the deep checks currently report

`python3 validation/checks/run_deep_checks.py` currently comes back clean, as
does `run_all_checks.py`. That is a statement about structure only - see "What
these checks cannot establish" above, and `docs/known-unvalidated-items.md`
for the estate's standing ledger of what has not been proven.
