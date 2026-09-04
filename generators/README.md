# Synthetic estate data generator (`wwigen`)

Fills the whole legacy estate - the Oracle ERP schemas, the SQL Server OLTP
databases and the file landing zone - with coherent, referentially consistent
fictional data. It writes **files only**. Nothing in this directory opens a
database connection, and nothing here needs one in order to run.

```
python3 generators/generate.py --scale small          # everything, small mode
python3 generators/generate.py --list-tables          # the registry, 76 objects
python3 generators/generate.py --self-check           # determinism proof
```

Python 3 standard library only - no third-party dependency is added.

## What it produces

```
generators/output/<scale>/
  data/oracle/WWI_MDM/CUST_MASTER.dat         pipe-delimited extract per table
  data/sqlserver/Sales/Orders.dat
  data/file/landing/partner_sales_eu.csv      the landing-zone feeds, as they arrive
  loaders/oracle/WWI_MDM/*.ctl                SQL*Loader control file per table
  loaders/oracle/Load-Oracle.ps1              sqlldr driver, foreign-key order
  loaders/sqlserver/formats/*.fmt             bcp format file per landed table
  loaders/sqlserver/Load-SqlServer.ps1        client-side bcp driver
  loaders/sqlserver/unlanded.txt              extracts with no landing table
  loaders/landing/Stage-LandingZone.ps1       feeds -> $WWI_LANDING_ROOT
  loaders/landing/feeds.csv                   feed -> landing path, per the yaml
  manifest.json                               table, file, rows, bytes, sha256, seed, scale
  .markers/<table>.json                       per-table resume markers
```

`generators/output/` is git-ignored; generated bulk data is never committed. A
tiny inspection slice is checked in under `generators/sample/` instead - the
first few rows of a handful of tables, one control file and one format file.

## Scale modes

Row counts for all three modes live in one place: `generators/config/scales.json`.
Change them there, not in code. `config_version` in that file is part of the run
signature, so bumping it invalidates resume markers from an earlier shape.

| driver | `small` | `medium` | `large` |
| --- | --- | --- | --- |
| customers | 400 | 12,000 | 120,000 |
| suppliers | 60 | 1,200 | 12,000 |
| products | 800 | 6,000 | 60,000 |
| orders | 3,000 | 220,000 | 5,200,000 |
| order lines (derived) | ~7,350 | ~539,000 | ~12,740,000 |
| invoice lines (derived) | ~7,060 | ~517,000 | ~12,230,000 |
| purchase orders | 900 | 60,000 | 1,050,000 |
| AP payments | 1,200 | 90,000 | 2,100,000 |
| inventory movements | 4,000 | 260,000 | 5,200,000 |
| shipments | 1,500 | 120,000 | 2,400,000 |
| web sessions | 1,800 | 150,000 | 3,000,000 |
| **estate total** | ~106,000 | ~7,000,000 | 40,000,000+ |

The `small` total above is the row count of a full `small` run. The `medium` and
`large` totals are arithmetic from the configured drivers, not measurements.

**Runtime is an order-of-magnitude design target, not a measured result.** Only
`small` has been exercised. Generation is single-threaded, streamed and CPU-bound
on hashing, so the expectation is roughly linear in rows: `small` in seconds,
`medium` in minutes, `large` in hours, with output on the order of a few GB at
`large`. Nothing at `medium` or `large` scale has been run.

Memory does not scale with row count: rows are streamed from generator functions
and flushed every `--chunk-rows` (default 50,000), so peak memory is a function of
the chunk size. Pushing past 30M rows is a matter of editing `scales.json`; the
data shape does not change.

## Determinism

The run is fully determined by `(seed, scale, config_version)`. The default seed
is in `scales.json`. Every value is derived by `wwigen.rng`, which position-
addresses a BLAKE2b digest of `(seed, table identity, ordinal, field name)` -
there is no shared mutable RNG state, no clock, no process id, no `hash()`, and
no iteration over a `set`. Because derivation is positional, any table can be
generated on its own and still agree with every other table about who customer
`CUS-0100237` is.

```
python3 generators/generate.py --self-check
```

generates the same four-table slice twice into scratch directories and compares
the SHA-256 of each file.

## Selective and resumable runs

```
python3 generators/generate.py --scale medium --system oracle
python3 generators/generate.py --scale medium --group sqlserver_sales
python3 generators/generate.py --scale medium --only sqlserver.Sales.Orders --only Sales.OrderLines
python3 generators/generate.py --scale large --resume
```

`--resume` skips any table whose marker records a completed run with the same
signature *and* the same column contract, so an interrupted `large` run continues
where it stopped. A table whose columns changed is regenerated even if its marker
says complete.

## Coherence and the deliberate defects

The same customer, supplier, product and order exist on both sides of the estate,
joined by the crosswalk codes the `ref.*` tables expect: an ERP code
(`CUS-0100237`), an OLTP integer id, and a partner code in the file feeds. A
configurable proportion is deliberately wrong, so the DQ and reconciliation
packages have something real to find:

| crosswalk state | what it looks like |
| --- | --- |
| `CLEAN` | all three identifiers agree |
| `MISSING_XREF` | OLTP account was never mastered in the ERP |
| `RETIRED_TARGET` | crosswalk points at a de-activated ERP party |
| `STALE_CODE` | OLTP carries a superseded ERP code |
| `DUPLICATE_XREF` | two ERP parties claim the same OLTP id |

Also present, at rates set in the `quality` block of `scales.json`: Pareto-skewed
customer and product activity with a handful of dominant accounts; seasonality,
weekday, month-end and quarter-end shape; several years of history with SCD Type 2
attribute changes (customer moves, reclassification, category reassignment, price
and cost changes, salesperson reassignment); late-arriving and out-of-order rows;
exact duplicates and name/address near-duplicates; corrections and restatements;
and malformed rows in the file feeds (bad dates, bad decimals, unknown codes,
overflowing values, Latin-1 bytes in a UTF-8 feed, embedded delimiters, wrong
field counts, null and padded keys, negative quantities).

Every intentionally malformed feed row is also emitted to
`file/landing/quarantine_rejects.dat` with the reject classification it should be
filed under, so a load can be reconciled against what the generator meant to break.

Regional divergence is real, not cosmetic: sales tax with destination sourcing in
NA, VAT with reverse charge in the EU, GST with truncation rather than rounding in
APAC; USD/EUR/AUD reporting with daily, triangulated and month-average FX; January,
April and July fiscal years; different address, postal, phone, consent and
return-reason code sets per region.

## Loading it, once databases exist

Nothing below has been executed. These are the artefacts the generator emits and
the commands they are written to be run with.

Oracle, every schema, in foreign-key order:

```powershell
$env:ORACLE_HOST = '...'; $env:ORACLE_PORT = '1521'; $env:ORACLE_SERVICE = '...'
# WWI_MDM_SECRET WWI_PROC_SECRET WWI_FIN_SECRET WWI_REF_SECRET WWI_AUDIT_SECRET
# come from the secret store; each schema is loaded as its own owner.
.\generators\output\small\loaders\oracle\Load-Oracle.ps1 -ListOnly
.\generators\output\small\loaders\oracle\Load-Oracle.ps1
```

SQL Server, into the staging database, with **client-side bcp**:

```powershell
$env:SQLSERVER_HOST = '...'; $env:SQLSERVER_PORT = '1433'
$env:SQLSERVER_USER = '...'; $env:SQLSERVER_STAGING_DB = '...'
# SQLSERVER_PASSWORD comes from the secret store.
.\generators\output\small\loaders\sqlserver\Load-SqlServer.ps1 -ListOnly
.\generators\output\small\loaders\sqlserver\Load-SqlServer.ps1
```

The instance is not the machine the extracts are written on, so a server-side
`BULK INSERT` can never see them: bcp reads each file locally and sends the rows
over the client connection. Each format file maps the extract's fields onto the
landing table's own column ordinals, so the columns the extract does not carry
keep their `DEFAULT`. Rejected rows go to `loaders/sqlserver/errors/<table>.err`
rather than failing the batch.

An extract with no landing table in `sqlserver/staging` gets no loader; it is
listed in `loaders/sqlserver/unlanded.txt`. Landing it would mean adding a raw
table, which is a schema change rather than a generator change.

The file feeds are placed under the landing zone, whose layout, filename
patterns and encodings are `config/landing-zone.yaml`:

```powershell
$env:WWI_LANDING_ROOT = 'C:\WWI\DEV'
.\generators\output\small\loaders\landing\Stage-LandingZone.ps1 -ListOnly
.\generators\output\small\loaders\landing\Stage-LandingZone.ps1
```

No credential value appears anywhere in this directory or in anything it
generates - only the environment variable names above.
