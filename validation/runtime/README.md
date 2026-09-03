# Runtime validation

The queries in this directory are the ones an operator would run against a
deployed estate to find out whether it behaves. **None of them has been run.**
There is no Oracle instance, no SQL Server instance, no SSIS catalog and no
file share attached to this repository, so everything here is written from the
object definitions in the tree and is unproven in exactly the way the rest of
the estate is.

They are deliberately read-only: `SELECT` only, no DDL, no writes, no jobs
started or stopped. They can be run against a production instance without
changing anything, which is why they exist as files rather than as procedures.

| File | Question it answers | Run against |
| --- | --- | --- |
| `validation/runtime/01_control_framework_health.sql` | Are batches completing, and is the control schema consistent with itself? | staging and warehouse |
| `validation/runtime/02_row_count_reconciliation.sql` | Do the row counts logged at each hop agree? | staging and warehouse |
| `validation/runtime/03_dimension_fact_integrity.sql` | Are the surrogate keys, SCD2 chains and fact/dimension joins intact? | warehouse |
| `validation/runtime/04_regional_divergence.sql` | Do the three regions behave differently in the ways the design intends? | warehouse |

## How to run them

```bash
sqlcmd -S "$SQLSERVER_HOST,$SQLSERVER_PORT" -d WideWorldImporters_Staging -b -I \
       -i validation/runtime/01_control_framework_health.sql
```

The control framework is deployed into both the staging database and the
warehouse (see `deployment/README.md`), so the control queries have to be run
twice, once against each, and the two answers compared by eye. That duplication
is a property of the estate, not of these scripts.

Credentials come from the environment variables named in `config/README.md`.
No value appears in any file here.

## Interpreting the results

There is no pass/fail threshold baked into these queries and no automation
around them. Each returns rows that a human reads. A clean result is an empty
result set for the exception queries and a plausible one for the summary
queries; what counts as plausible depends on the environment, which is why the
tolerances live in `etl.Configuration` rather than here.

Anything these queries would answer is, until they are run, listed in
`docs/known-unvalidated-items.md`.
